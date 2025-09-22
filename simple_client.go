// Package gearman provides a simple, context-aware Gearman client implementation.
package gearman

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/wcn/gearman/v2/job"
)

// SimpleClient provides a simplified, context-aware Gearman client interface.
type SimpleClient struct {
	client *Client // Wraps the original client
	config ClientConfig
}

// ClientConfig holds configuration for the simple client.
type ClientConfig struct {
	Network string
	Address string
	Timeout time.Duration
}

// DefaultConfig returns a sensible default configuration.
func DefaultConfig(address string) ClientConfig {
	return ClientConfig{
		Network: "tcp4",
		Address: address,
		Timeout: 60 * time.Second,
	}
}

// NewSimpleClient creates a new simple Gearman client.
// You must call Start(ctx) before using the client in order to open the
// connection to gearmand. client.Close() should be called to close the
// connection unless you expect to make subsequent calls.
func NewSimpleClient(config ClientConfig) *SimpleClient {
	client := newClientWithoutStart(config.Network, config.Address)

	return &SimpleClient{
		client: client,
		config: config,
	}
}

// Call performs a synchronous RPC call to the specified Gearman function.
func (c *SimpleClient) Call(ctx context.Context, function string, payload []byte) ([]byte, error) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && c.config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.Timeout)
		defer cancel()
	}

	dataBuffer := &bytes.Buffer{}
	warningsBuffer := &bytes.Buffer{}
	dataCloser := &bufferWriteCloser{dataBuffer}
	warningsCloser := &bufferWriteCloser{warningsBuffer}

	gearmanJob, err := c.client.SubmitWithTimeout(function, payload, dataCloser, warningsCloser, c.config.Timeout)
	if err != nil {
		return nil, fmt.Errorf("gearman cannot submit job: %w", err)
	}

	done := make(chan job.State, 1)
	go func() {
		done <- gearmanJob.Run()
	}()

	select {
	case state := <-done:
		switch state {
		case job.Completed:
			return dataBuffer.Bytes(), nil
		case job.Failed:
			return nil, fmt.Errorf("gearman job failed")
		default:
			return nil, fmt.Errorf("gearman job finished with unexpected state: %v", state)
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// bufferWriteCloser wraps a bytes.Buffer to implement io.WriteCloser
type bufferWriteCloser struct {
	*bytes.Buffer
}

func (b *bufferWriteCloser) Close() error {
	return nil
}

// Start connects to server and starts the goroutines for packet processing.
// This must be called before using the client for any operations.
func (c *SimpleClient) Start(ctx context.Context) error {
	return c.client.Start(ctx)
}

// Close closes the client connection and cleanly shuts down all background processing.
func (c *SimpleClient) Close() error {
	return c.client.Close()
}

// Ping sends an ECHO_REQ packet to the Gearman server and waits for the corresponding ECHO_RES.
// It generates its own content for the echo request and returns an error if the ping fails or times out.
// The ping uses the configured timeout if the context doesn't have a deadline.
func (c *SimpleClient) Ping(ctx context.Context) error {

	timeout := c.config.Timeout
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	// Call the underlying client ping
	return c.client.Ping(ctx)
}
