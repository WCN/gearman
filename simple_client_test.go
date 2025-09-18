package gearman

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSimpleClient creates a SimpleClient with a mocked underlying Client
func mockSimpleClient() *SimpleClient {
	client := mockClient()
	// Mark as started to pass state validation
	client.connLock.Lock()
	client.started = true
	client.connLock.Unlock()

	return &SimpleClient{
		client: client,
		config: ClientConfig{
			Network: "tcp4",
			Address: "test:4730",
			Timeout: 100 * time.Millisecond, // Short timeout for tests
		},
	}
}

func TestSimpleClient_StateValidation(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(*SimpleClient)
		expectedError string
	}{
		{
			name: "client not started",
			setup: func(sc *SimpleClient) {
				client := mockClient()
				client.started = false // Override default for this test
				sc.client = client
			},
			expectedError: "client has not been started - call Start(ctx) first",
		},
		{
			name: "client closed",
			setup: func(sc *SimpleClient) {
				// Create a client and close it
				client := mockClient()
				client.Close()
				sc.client = client
			},
			expectedError: "client is closed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &SimpleClient{
				config: DefaultConfig("localhost:4730"),
			}
			tt.setup(sc)

			result, err := sc.Call(context.Background(), "test", []byte("data"))

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedError)
			assert.Nil(t, result)
		})
	}
}

func TestSimpleClient_Start(t *testing.T) {
	sc := mockSimpleClient()
	ctx := context.Background()

	sc.Start(ctx)

	// Verify client is marked as started
	assert.True(t, sc.client.started)
}

func TestSimpleClient_Close(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(*SimpleClient)
		expectedError bool
	}{
		{
			name:  "close normally",
			setup: func(sc *SimpleClient) {},
		},
		{
			name:  "close multiple times",
			setup: func(sc *SimpleClient) { sc.client.Close() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := mockSimpleClient()
			tt.setup(sc)

			err := sc.Close()
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.True(t, sc.client.closed)
		})
	}
}

func TestSimpleClient_CloseIdempotent(t *testing.T) {
	sc := mockSimpleClient()

	err1 := sc.Close()
	assert.NoError(t, err1)

	err2 := sc.Close()
	assert.NoError(t, err2)

	err3 := sc.Close()
	assert.NoError(t, err3)
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig("localhost:4730")

	assert.Equal(t, "tcp4", config.Network)
	assert.Equal(t, "localhost:4730", config.Address)
	assert.Equal(t, 60*time.Second, config.Timeout)
}

func TestNewSimpleClient(t *testing.T) {
	tests := []struct {
		name          string
		config        ClientConfig
		expectedError bool
	}{
		{
			name: "invalid address",
			config: ClientConfig{
				Network: "tcp4",
				Address: "invalid:address:port:too:many:parts",
				Timeout: 30 * time.Second,
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewSimpleClient(tt.config)
			require.NotNil(t, client)
			err := client.Start(context.Background())

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.config, client.config)
				assert.NotNil(t, client.client)
			}
		})
	}
}

func TestBufferWriteCloser(t *testing.T) {
	buf := &bytes.Buffer{}
	bwc := &bufferWriteCloser{buf}

	// Test Write
	n, err := bwc.Write([]byte("test data"))
	assert.NoError(t, err)
	assert.Equal(t, 9, n)
	assert.Equal(t, "test data", buf.String())

	// Test Close
	err = bwc.Close()
	assert.NoError(t, err)
}
