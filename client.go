// Package gearman provides a thread-safe Gearman client implementation in Go.
//
// Gearman is a job queue system that allows you to distribute work across multiple
// machines or processes. This package provides a client that can submit jobs to
// a Gearman server and handle the responses.
//
// The client supports both foreground and background jobs, and provides methods
// for submitting jobs and receiving data and warnings from job execution.
//
// Example usage:
//
//	client, err := gearman.NewClient("tcp4", "localhost:4730")
//	if err != nil {
//		panic(err)
//	}
//	defer client.Close()
//
//	job, err := client.Submit("reverse", []byte("hello world!"), nil, nil)
//	if err != nil {
//		panic(err)
//	}
//
//	state := job.Run()
//	fmt.Println("Job completed with state:", state)
package gearman

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	slogctx "github.com/veqryn/slog-context"
	"github.com/wcn/gearman/v2/job"
	"github.com/wcn/gearman/v2/packet"
	"github.com/wcn/gearman/v2/scanner"
)

// noOpCloser is like an io.NopCloser, but for an io.Writer.
type noOpCloser struct {
	w io.Writer
}

func (c *noOpCloser) Write(data []byte) (n int, err error) {
	return c.w.Write(data)
}

func (c *noOpCloser) Close() error {
	return nil
}

var discard = &noOpCloser{w: io.Discard}

type partialJob struct {
	// data is used to write data back to the caller's provided io.Writer
	data io.WriteCloser
	// warnings is used to write warning messages back to the caller's provided io.Writer
	warnings io.WriteCloser
}

// Client is a thread-safe Gearman client that can submit jobs to a Gearman server.
// The client maintains a connection to the server and handles packet routing
// to the appropriate jobs.
type Client struct {
	network string
	address string
	// conn is the connection to the gearman server
	conn net.Conn
	// packets is the stream of incoming gearman packets from the server
	packets chan *packet.Packet
	// jobs is a router for sending packets to the correct job to interpret
	jobs map[string]chan *packet.Packet
	// partialJobs
	partialJobs chan *partialJob
	newJobs     chan *job.Job
	jobLock     sync.RWMutex
	// pings tracks ping identifiers so we can route them back to the caller
	pings    map[string]chan struct{}
	pingLock sync.RWMutex
	// connection state
	connLock sync.RWMutex
	connErr  error // tracks the last connection error
	closed   bool  // tracks if the client has been closed
	started  bool  // prevents multiple Start() calls
	// goroutine management (added in v2, backward compatible)
	ctx    context.Context
	cancel context.CancelFunc
}

// Start connects to server and starts the goroutines for packet processing.
// When the provided context is cancelled, the client will automatically close,
func (c *Client) Start(ctx context.Context) error {
	c.connLock.Lock()
	defer c.connLock.Unlock()

	if c.started {
		return nil
	}

	conn, err := net.Dial(c.network, c.address)
	if err != nil {
		return fmt.Errorf("error while establishing a connection to gearman: %s", err)
	}
	c.conn = conn

	c.ctx, c.cancel = context.WithCancel(ctx)
	c.started = true

	go c.read(c.ctx, scanner.New(c.conn))
	go c.routePackets(c.ctx)

	// Monitor context cancellation and auto-close
	go func() {
		<-ctx.Done()
		c.Close()
	}()
	return nil
}

// Close terminates the connection to the server and cleans up resources.
func (c *Client) Close() error {
	c.connLock.Lock()
	defer c.connLock.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	if c.cancel != nil {
		c.cancel()
	}

	// TODO: figure out when to close packet chan
	return c.conn.Close()
}

func (c *Client) isReady() error {
	c.connLock.RLock()
	if c.closed {
		c.connLock.RUnlock()
		return fmt.Errorf("client is closed")
	}
	if c.connErr != nil {
		c.connLock.RUnlock()
		return fmt.Errorf("connection error: %w", c.connErr)
	}
	if !c.started {
		c.connLock.RUnlock()
		return fmt.Errorf("client has not been started - call Start(ctx) first")
	}
	c.connLock.RUnlock()

	return nil
}

func (c *Client) submit(fn string, payload []byte, data, warnings io.WriteCloser, t packet.Type, timeout time.Duration) (*job.Job, error) {
	if err := c.isReady(); err != nil {
		return nil, err
	}

	// create and marshal the gearman packet
	pack := &packet.Packet{
		Code:      packet.Req,
		Type:      t,
		Arguments: [][]byte{[]byte(fn), {}, payload},
	}
	buf, err := pack.MarshalBinary()
	if err != nil {
		return nil, err
	}

	// write the packet to the gearman server
	if _, err := io.Copy(c.conn, bytes.NewBuffer(buf)); err != nil {
		// Mark connection as failed
		c.connLock.Lock()
		c.connErr = err
		c.connLock.Unlock()
		return nil, fmt.Errorf("failed to send packet: %w", err)
	}

	// block while the client waits for confirmation that a job has been created
	select {
	case c.partialJobs <- &partialJob{data: data, warnings: warnings}:
		// Job submitted successfully
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for job confirmation")
	}

	select {
	case j := <-c.newJobs:
		return j, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for job creation")
	}
}

// SubmitWithTimeout sends a new job to the server with the specified function and payload and timeout.
// You must provide two WriteClosers for data and warnings to be written to.
func (c *Client) SubmitWithTimeout(fn string, payload []byte, data, warnings io.WriteCloser, timeout time.Duration) (*job.Job, error) {
	return c.submit(fn, payload, data, warnings, packet.SubmitJob, timeout)
}

// Submit sends a new job to the server with the specified function and payload. You must provide
// two WriteClosers for data and warnings to be written to.
func (c *Client) Submit(fn string, payload []byte, data, warnings io.WriteCloser) (*job.Job, error) {
	return c.submit(fn, payload, data, warnings, packet.SubmitJob, 30*time.Second)
}

// SubmitBackground submits a background job. There is no access to data, warnings, or completion
// state.
func (c *Client) SubmitBackground(fn string, payload []byte) error {
	_, err := c.submit(fn, payload, discard, discard, packet.SubmitJobBg, 30*time.Second)
	return err
}

// addJob adds the reference to a job and its packet stream to the internal map of packet streams.
func (c *Client) addJob(handle string, packets chan *packet.Packet) {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	c.jobs[handle] = packets
}

// getJob returns the reference to channel for a specific job based off of its handle.
func (c *Client) getJob(handle string) chan *packet.Packet {
	c.jobLock.RLock()
	defer c.jobLock.RUnlock()
	return c.jobs[handle]
}

// deleteJob removes a job's packet stream from the internal map of ongoing jobs.
func (c *Client) deleteJob(handle string) {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	delete(c.jobs, handle)
}

// pingCounter is used to ensure uniqueness when multiple pings happen in the same nanosecond
var pingCounter int64

// generatePingToken creates a unique token for ping requests with "echo" prefix
// Uses PID + timestamp + counter to ensure uniqueness without error handling
func generatePingToken() string {
	pid := os.Getpid()
	now := time.Now().UnixNano()
	counter := atomic.AddInt64(&pingCounter, 1)
	return fmt.Sprintf("echo%d_%d_%d", pid, now, counter)
}

// Ping sends an ECHO_REQ packet to the server and waits for the corresponding ECHO_RES.
// It returns the echoed data or an error if the ping fails or times out.
func (c *Client) Ping(ctx context.Context) error {
	if err := c.isReady(); err != nil {
		return err
	}

	token := generatePingToken()
	responseChan := make(chan struct{}, 1)
	c.pingLock.Lock()
	c.pings[token] = responseChan
	c.pingLock.Unlock()

	pack := &packet.Packet{
		Code:      packet.Req,
		Type:      packet.EchoReq,
		Arguments: [][]byte{[]byte(token)},
	}

	buf, err := pack.MarshalBinary()
	if err != nil {
		c.pingLock.Lock()
		delete(c.pings, token)
		c.pingLock.Unlock()
		return fmt.Errorf("failed to marshal ECHO_REQ packet: %w", err)
	}

	_, err = io.Copy(c.conn, bytes.NewBuffer(buf))
	if err != nil {
		c.pingLock.Lock()
		delete(c.pings, token)
		c.pingLock.Unlock()

		// Mark connection as failed
		c.connLock.Lock()
		c.connErr = err
		c.connLock.Unlock()
		return fmt.Errorf("failed to send ECHO_REQ packet: %w", err)
	}

	select {
	case <-responseChan:
		return nil
	case <-ctx.Done():
		c.pingLock.Lock()
		delete(c.pings, token)
		c.pingLock.Unlock()
		return ctx.Err()
	}
}

// read attempts to read incoming packets from the gearman server to route them to the job
// they are intended for.
func (c *Client) read(ctx context.Context, scanner *bufio.Scanner) {
	logger := slogctx.FromCtx(ctx)

	defer func() {
		if r := recover(); r != nil {
			logger.Error("Gearman client panic recovered in read loop", slog.Any("panic", r))
		}
	}()

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			logger.Debug("Read loop cancelled")
			return
		default:
		}

		pack := &packet.Packet{}
		if err := pack.UnmarshalBinary(scanner.Bytes()); err != nil {
			logger.Warn("Error parsing packet", slog.Any("error", err))
			continue // Skip this packet and continue reading
		}

		// Use non-blocking send to prevent deadlock if channel is full
		select {
		case c.packets <- pack:
			// Packet sent successfully
		case <-ctx.Done():
			logger.Debug("Read loop cancelled while sending packet")
			return
		default:
			logger.Warn("Packet channel full, dropping packet")
		}
	}
	if scanner.Err() != nil {
		errMsg := scanner.Err().Error()
		if strings.Contains(errMsg, "use of closed network connection") {
			logger.Debug("Connection closed normally")
		} else {
			logger.Warn("Error scanning packets", slog.Any("error", scanner.Err()))
			// Mark connection as failed
			c.connLock.Lock()
			c.connErr = scanner.Err()
			c.connLock.Unlock()
			c.failPendingJobs(ctx, scanner.Err())
		}
	}
}

// failPendingJobs fails all pending jobs when a scanner error occurs
func (c *Client) failPendingJobs(ctx context.Context, err error) {
	logger := slogctx.FromCtx(ctx)
	c.jobLock.Lock()
	defer c.jobLock.Unlock()

	for handle, packets := range c.jobs {
		logger.Warn("Failing job due to scanner error",
			slog.String("handle", handle),
			slog.Any("error", err))
		failPacket := &packet.Packet{
			Type:      packet.WorkFail,
			Arguments: [][]byte{[]byte(handle)},
		}

		select {
		case packets <- failPacket:
		default:
			logger.Warn("Could not send fail packet to job", slog.String("handle", handle))
		}
	}
}

// routePackets forwards incoming packets to the correct job.
func (c *Client) routePackets(ctx context.Context) {
	logger := slogctx.FromCtx(ctx)

	// operate on every packet that has been read
	for {
		select {
		case pack := <-c.packets:
			switch pack.Type {
			case packet.EchoRes:
				if len(pack.Arguments) == 0 {
					logger.Warn("ECHO_RES packet received with no data")
					continue
				}

				token := string(pack.Arguments[0])

				// Look for a matching ping request
				c.pingLock.Lock()
				pingResponseChan, exists := c.pings[token]
				if exists {
					delete(c.pings, token)
				}
				c.pingLock.Unlock()

				if exists {
					// Send response back to waiting ping
					select {
					case pingResponseChan <- struct{}{}:
					case <-ctx.Done():
						return
					}
				} else {
					logger.Warn("Received ECHO_RES for unknown token", slog.String("token", token))
				}
				continue
			}

			if len(pack.Arguments) == 0 {
				logger.Warn("Packet read with no handle")
				continue
			}

			handle := string(pack.Arguments[0])
			switch pack.Type {
			case packet.JobCreated:
				// create a new channel to send packets for this job
				packets := make(chan *packet.Packet)
				// optimistically hope that the last job submitted is the same one that just started
				pj := <-c.partialJobs
				// hook up the job to its packet stream
				j := job.NewWithContext(ctx, handle, pj.data, pj.warnings, packets)
				// add the packet stream to the internal routing map
				c.addJob(handle, packets)
				// finally unblock the Submit() fn call
				c.newJobs <- j

				go func() {
					defer close(packets)
					defer c.deleteJob(handle)
					j.Run()
				}()
			default:
				// send the packet to the right job
				pktStream := c.getJob(handle)
				if pktStream != nil {
					pktStream <- pack
				} else {
					logger.Warn("Packet read with unknown handle", slog.String("handle", handle))
				}
			}
		case <-ctx.Done():
			logger.Debug("Route packets loop cancelled")
			return
		}
	}
}

// newClientWithoutStart creates a client but doesn't start background goroutines.
// This is used internally by SimpleClient.
func newClientWithoutStart(network, address string) *Client {
	return &Client{
		network:     network,
		address:     address,
		packets:     make(chan *packet.Packet),
		newJobs:     make(chan *job.Job),
		partialJobs: make(chan *partialJob),
		jobs:        make(map[string]chan *packet.Packet),
		pings:       make(map[string]chan struct{}),
	}
}

// NewClient returns a new Gearman client pointing at the specified server.
// For v2 compatibility, this automatically starts the background goroutines.
func NewClient(network, address string) (*Client, error) {
	c := newClientWithoutStart(network, address)

	// Auto-start with background context for v2 compatibility
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := slogctx.With(context.Background(), logger)

	err := c.Start(ctx)
	if err != nil {
		return nil, err
	}

	return c, nil
}
