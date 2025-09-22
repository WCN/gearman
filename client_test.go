package gearman

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wcn/gearman/v2/job"
	"github.com/wcn/gearman/v2/packet"

	"github.com/stretchr/testify/assert"
)

type bufferCloser struct {
	bytes.Buffer
}

func (buf *bufferCloser) Close() error {
	return nil
}

// Implement net.Conn interface
func (buf *bufferCloser) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
}

func (buf *bufferCloser) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4730}
}

func (buf *bufferCloser) SetDeadline(t time.Time) error {
	return nil
}

func (buf *bufferCloser) SetReadDeadline(t time.Time) error {
	return nil
}

func (buf *bufferCloser) SetWriteDeadline(t time.Time) error {
	return nil
}

func mockClient() *Client {
	c := &Client{
		//network: "tcp4",
		//address: "localhost:4730",
		conn:    &bufferCloser{},
		packets: make(chan *packet.Packet),
		// Add buffers to prevent blocking in test cases
		newJobs:     make(chan *job.Job, 10),
		jobs:        make(map[string]chan *packet.Packet, 10),
		partialJobs: make(chan *partialJob, 10),
		pings:       make(map[string]chan struct{}),
		started:     true, // Default to started for most tests
	}
	go c.routePackets(context.Background())
	return c
}

func TestSubmit(t *testing.T) {
	c := mockClient()

	buf := c.conn.(*bufferCloser)
	expected := job.New("the_handle", nil, nil, make(chan *packet.Packet))
	c.newJobs <- expected
	j, err := c.Submit("my_function", []byte("my data"), nil, nil)
	assert.Nil(t, err)
	assert.Equal(t, j, expected)
	expectedPacket := &packet.Packet{
		Code:      []byte{0x0, 0x52, 0x45, 0x51}, // \0REQ
		Type:      packet.SubmitJob,
		Arguments: [][]byte{[]byte("my_function"), {}, []byte("my data")},
	}
	b, err := expectedPacket.MarshalBinary()
	assert.Nil(t, err)
	assert.Equal(t, buf.Bytes(), b)
}

func handlePacket(handle string, kind int, arguments [][]byte) *packet.Packet {
	if arguments == nil {
		arguments = [][]byte{}
	}
	arguments = append([][]byte{[]byte(handle)}, arguments...)
	return &packet.Packet{
		Type:      packet.Type(kind),
		Arguments: arguments,
	}
}

func TestJobCreated(t *testing.T) {
	c := mockClient()
	c.partialJobs <- &partialJob{nil, nil}
	wg := sync.WaitGroup{}
	wg.Add(1)
	var j *job.Job
	var packets chan *packet.Packet
	go func() {
		defer wg.Done()
		j = <-c.newJobs
		packets = c.getJob("5")
		assert.Equal(t, j.Handle(), "5")
	}()
	c.packets <- handlePacket("5", packet.JobCreated, nil)
	wg.Wait()
	c.packets <- handlePacket("5", packet.WorkComplete, nil)
	j.Run()
	<-packets // Wait until packet channel is closed, so we know that we've deleted the job
	assert.Nil(t, c.jobs["5"])
}

func TestRoutePackets(t *testing.T) {
	c := mockClient()
	c.jobs = map[string]chan *packet.Packet{
		"0": make(chan *packet.Packet, 10),
		"1": make(chan *packet.Packet, 10),
		"2": make(chan *packet.Packet, 10),
		"3": make(chan *packet.Packet, 10),
		"4": make(chan *packet.Packet, 10),
	}

	packetChans := []chan *packet.Packet{
		c.jobs["0"], c.jobs["1"], c.jobs["2"], c.jobs["3"], c.jobs["4"],
	}

	packets := []*packet.Packet{
		handlePacket("0", packet.WorkFail, nil),
		handlePacket("1", packet.WorkFail, nil),
		handlePacket("2", packet.WorkFail, nil),
		handlePacket("3", packet.WorkFail, nil),
		handlePacket("4", packet.WorkFail, nil),
	}
	for _, pack := range packets {
		c.packets <- pack
	}
	for i := 0; i < 5; i++ {
		pack := <-packetChans[i]
		assert.Equal(t, pack, packets[i])
	}
}
