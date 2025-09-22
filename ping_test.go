package gearman

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wcn/gearman/v2/packet"
)

func TestClientPing(t *testing.T) {
	c := mockClient()

	ctx := context.Background()

	// Test ping in a goroutine since it will block waiting for response
	var pingErr error
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		pingErr = c.Ping(ctx)
	}()

	// Give the ping request time to be registered
	time.Sleep(10 * time.Millisecond)

	c.pingLock.RLock()
	assert.Equal(t, 1, len(c.pings), "Expected one ping request to be registered")

	var token string
	for token = range c.pings {
		break
	}
	c.pingLock.RUnlock()

	// Simulate ECHO_RES response
	c.packets <- &packet.Packet{
		Type:      packet.EchoRes,
		Arguments: [][]byte{[]byte(token)},
	}

	wg.Wait()
	require.NoError(t, pingErr, "Ping should succeed")

	c.pingLock.RLock()
	assert.Equal(t, 0, len(c.pings), "Ping request should be cleaned up after response")
	c.pingLock.RUnlock()
}

func TestClientPingTimeout(t *testing.T) {
	c := mockClient()

	// very short timeout + we don't send response packets = expected timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	err := c.Ping(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded", "Should be a timeout error")

	c.pingLock.RLock()
	assert.Equal(t, 0, len(c.pings), "Ping request should be cleaned up after timeout")
	c.pingLock.RUnlock()
}

func TestClientPingSimple(t *testing.T) {
	c := mockClient()

	var pingErr error
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		pingErr = c.Ping(context.Background())
	}()

	// Give the ping request time to be registered
	time.Sleep(10 * time.Millisecond)

	c.pingLock.RLock()
	assert.Equal(t, 1, len(c.pings), "Expected one ping request to be registered")

	// determine the token used in ECHO_REQ so we can generate an ECHO_RES
	// that will be routed back to the caller
	var token string
	for token = range c.pings {
		break
	}
	c.pingLock.RUnlock()

	c.packets <- &packet.Packet{
		Type:      packet.EchoRes,
		Arguments: [][]byte{[]byte(token)},
	}

	wg.Wait()
	assert.NoError(t, pingErr, "Ping should succeed")
}

func TestSimpleClientPing(t *testing.T) {
	sc := mockSimpleClient()

	var pingErr error
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		pingErr = sc.Ping(context.Background())
	}()

	// Give the ping request time to be registered
	time.Sleep(10 * time.Millisecond)

	sc.client.pingLock.RLock()
	assert.Equal(t, 1, len(sc.client.pings), "Expected one ping request to be registered")

	var token string
	for token = range sc.client.pings {
		break
	}
	sc.client.pingLock.RUnlock()

	// Simulate ECHO_RES response
	sc.client.packets <- &packet.Packet{
		Type:      packet.EchoRes,
		Arguments: [][]byte{[]byte(token)},
	}

	wg.Wait()
	require.NoError(t, pingErr, "SimpleClient.Ping should succeed")

	sc.client.pingLock.RLock()
	assert.Equal(t, 0, len(sc.client.pings), "Ping request should be cleaned up after response")
	sc.client.pingLock.RUnlock()
}

func TestSimpleClientPingStateValidation(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(*SimpleClient)
		expectedError string
	}{
		{
			name: "client not started",
			setup: func(sc *SimpleClient) {
				client := mockClient()
				client.connLock.Lock()
				client.started = false
				client.connLock.Unlock()
				sc.client = client
			},
			expectedError: "client has not been started - call Start(ctx) first",
		},
		{
			name: "client closed",
			setup: func(sc *SimpleClient) {
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

			err := sc.Ping(context.Background())

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedError)
		})
	}
}
