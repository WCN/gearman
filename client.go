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
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/adamoleeo/gearman/v2/job"
	"github.com/adamoleeo/gearman/v2/packet"
	"github.com/adamoleeo/gearman/v2/scanner"
)

// noOpCloser is like an ioutil.NopCloser, but for an io.Writer.
type noOpCloser struct {
	w io.Writer
}

func (c noOpCloser) Write(data []byte) (n int, err error) {
	return c.w.Write(data)
}

func (c noOpCloser) Close() error {
	return nil
}

var discard = noOpCloser{w: ioutil.Discard}

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
	// conn is the connection to the gearman server
	conn io.WriteCloser
	// packets is the stream of incoming gearman packets from the server
	packets chan *packet.Packet
	// jobs is a router for sending packets to the correct job to interpret
	jobs map[string]chan *packet.Packet
	// partialJobs
	partialJobs chan *partialJob
	newJobs     chan *job.Job
	jobLock     sync.RWMutex
	// connection state
	connLock sync.RWMutex
	connErr  error // tracks the last connection error
	closed   bool  // tracks if the client has been closed
}

// Close terminates the connection to the server and cleans up resources.
func (c *Client) Close() error {
	c.connLock.Lock()
	c.closed = true
	c.connLock.Unlock()
	// TODO: figure out when to close packet chan
	return c.conn.Close()
}

func (c *Client) submit(fn string, payload []byte, data, warnings io.WriteCloser, t packet.Type) (*job.Job, error) {
	// Check if the client has been closed or has a connection error
	c.connLock.RLock()
	if c.closed {
		c.connLock.RUnlock()
		return nil, fmt.Errorf("client is closed")
	}
	if c.connErr != nil {
		c.connLock.RUnlock()
		return nil, fmt.Errorf("connection error: %w", c.connErr)
	}
	c.connLock.RUnlock()

	// create and marshal the gearman packet
	pack := &packet.Packet{
		Code:      packet.Req,
		Type:      t,
		Arguments: [][]byte{[]byte(fn), []byte{}, payload},
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
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("timeout waiting for job confirmation")
	}

	select {
	case job := <-c.newJobs:
		return job, nil
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("timeout waiting for job creation")
	}
}

// Submit sends a new job to the server with the specified function and payload. You must provide
// two WriteClosers for data and warnings to be written to.
func (c *Client) Submit(fn string, payload []byte, data, warnings io.WriteCloser) (*job.Job, error) {
	return c.submit(fn, payload, data, warnings, packet.SubmitJob)
}

// SubmitBackground submits a background job. There is no access to data, warnings, or completion
// state.
func (c *Client) SubmitBackground(fn string, payload []byte) error {
	_, err := c.submit(fn, payload, discard, discard, packet.SubmitJobBg)
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

// read attempts to read incoming packets from the gearman server to route them to the job
// they are intended for.
func (c *Client) read(scanner *bufio.Scanner) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "GEARMAN PANIC: recovered from panic in read loop: %v\n", r)
		}
	}()

	for scanner.Scan() {
		pack := &packet.Packet{}
		if err := pack.UnmarshalBinary(scanner.Bytes()); err != nil {
			fmt.Fprintf(os.Stderr, "GEARMAN WARNING: error parsing packet! %v\n", err)
			continue // Skip this packet and continue reading
		} else {
			if pack == nil {
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: received nil packet, skipping\n")
				continue
			}

			// Use non-blocking send to prevent deadlock if channel is full
			select {
			case c.packets <- pack:
				// Packet sent successfully
			default:
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: packet channel full, dropping packet\n")
			}
		}
	}
	if scanner.Err() != nil {
		errMsg := scanner.Err().Error()
		if strings.Contains(errMsg, "use of closed network connection") {
			// fmt.Fprintf(os.Stderr, "DEBUG: Connection closed normally\n")
		} else {
			fmt.Fprintf(os.Stderr, "GEARMAN WARNING: error scanning! %v\n", scanner.Err())
			// Mark connection as failed
			c.connLock.Lock()
			c.connErr = scanner.Err()
			c.connLock.Unlock()
			c.failPendingJobs(scanner.Err())
		}
	}
}

// failPendingJobs fails all pending jobs when a scanner error occurs
func (c *Client) failPendingJobs(err error) {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()

	for handle, packets := range c.jobs {
		fmt.Fprintf(os.Stderr, "GEARMAN WARNING: failing job %s due to scanner error: %v\n", handle, err)
		failPacket := &packet.Packet{
			Type:      packet.WorkFail,
			Arguments: [][]byte{[]byte(handle)},
		}

		select {
		case packets <- failPacket:
		default:
			fmt.Fprintf(os.Stderr, "GEARMAN WARNING: could not send fail packet to job %s\n", handle)
		}
	}
}

// routePackets forwards incoming packets to the correct job.
func (c *Client) routePackets() {
	// operate on every packet that has been read
	for pack := range c.packets {
		if len(pack.Arguments) == 0 {
			fmt.Fprintln(os.Stderr, "GEARMAN WARNING: packet read with no handle!")
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
			j := job.New(handle, pj.data, pj.warnings, packets)
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
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: packet read with handle of '%s', "+
					"no reference found in client.!\n", handle)
			}
		}
	}
}

// NewClient returns a new Gearman client pointing at the specified server
func NewClient(network, addr string) (*Client, error) {
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, fmt.Errorf("Error while establishing a connection to gearman: %s", err)
	}

	c := &Client{
		conn:        conn,
		packets:     make(chan *packet.Packet),
		newJobs:     make(chan *job.Job),
		partialJobs: make(chan *partialJob),
		jobs:        make(map[string]chan *packet.Packet),
	}
	go c.read(scanner.New(conn))
	go c.routePackets()

	return c, nil
}
