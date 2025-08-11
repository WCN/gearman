// Package packet provides structures to marshal binary data to and from binary data
// according to the Gearman protocol specification.
//
// The Gearman protocol is a binary protocol where packets consist of a header
// (magic code, type, and data length) followed by optional data. This package
// provides types and functions for creating, parsing, and manipulating these packets.
//
// For the complete protocol specification, see: http://gearman.org/protocol/
package packet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

type packetCode []byte

var (
	// Req is the code for a Request packet
	Req = packetCode([]byte{0, byte('R'), byte('E'), byte('Q')})
	// Res is the code for a Response packet
	Res = packetCode([]byte{0, byte('R'), byte('E'), byte('S')})
)

const (
	// Maximum packet size to prevent memory exhaustion attacks
	maxPacketSize = 1 * 1024 * 1024 // 1MB
)

// Packet contains a Gearman packet according to the protocol specification.
// Each packet has a code (either REQ or RES), a type, and optional arguments.
// See http://gearman.org/protocol/ for the complete specification.
type Packet struct {
	// The Code for the packet: either \0REQ or \0RES
	Code packetCode
	// The Type of the packet, e.g. WorkStatus
	Type Type
	// The Arguments of the packet
	Arguments [][]byte
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface
func (packet *Packet) UnmarshalBinary(data []byte) error {
	// 4 bytes each for magic, type, data length
	if len(data) < 12 {
		return errors.New("all gearman packets must be at least 12 bytes or more")
	}

	if len(data) > maxPacketSize {
		return fmt.Errorf("packet too large: %d bytes (maximum %d allowed)", len(data), maxPacketSize)
	}

	if bytes.Equal(data[0:4], Req) {
		packet.Code = Req
	} else if bytes.Equal(data[0:4], Res) {
		packet.Code = Res
	} else {
		return fmt.Errorf("unrecognized magic packet code %#v", data[0:4])
	}

	// determine the kind of packet
	kind := uint32(0)
	if err := binary.Read(bytes.NewBuffer(data[4:8]), binary.BigEndian, &kind); err != nil {
		return fmt.Errorf("cannot read packet type: %s", err)
	}
	packet.Type = Type(kind)

	length := uint32(0)
	if err := binary.Read(bytes.NewBuffer(data[8:12]), binary.BigEndian, &length); err != nil {
		return fmt.Errorf("cannot read packet data_length: %s", err)
	}

	packet.Arguments = [][]byte{}
	if length == 0 {
		return nil // no data/arguments
	}

	if length > maxPacketSize {
		return fmt.Errorf("packet length too large: %d bytes (maximum %d allowed)", length, maxPacketSize)
	}

	expectedDataLength := 12 + int(length)
	if len(data) != expectedDataLength {
		return fmt.Errorf("packet data length mismatch: expected %d bytes, got %d", expectedDataLength, len(data))
	}

	if len(data) <= 12 {
		return fmt.Errorf("packet has declared length %d but no data after header", length)
	}

	packet.Arguments = bytes.Split(data[12:expectedDataLength], []byte{0})

	// Validate that we don't have an excessive number of arguments
	if len(packet.Arguments) > 1000 {
		return fmt.Errorf("too many arguments in packet: %d (maximum 1000 allowed)", len(packet.Arguments))
	}

	return nil
}

// MarshalBinary implements the encoding.BinaryMarshaler interface
func (packet *Packet) MarshalBinary() ([]byte, error) {
	// form a buffer with the packet's magic code
	buf := bytes.NewBuffer(packet.Code)

	// write the packet type
	if err := binary.Write(buf, binary.BigEndian, uint32(packet.Type)); err != nil {
		return nil, fmt.Errorf("cannot write packet type: %w", err)
	}

	// finish the header with the size of the packet
	size := len(packet.Arguments) - 1 // One for each null-byte separator
	for _, argument := range packet.Arguments {
		size += len(argument)
	}
	size = int(math.Max(0, float64(size)))

	// write the size of the packet
	if err := binary.Write(buf, binary.BigEndian, uint32(size)); err != nil {
		return nil, fmt.Errorf("cannot write packet length: %w", err)
	}

	// write all arguments provided
	for i, arg := range packet.Arguments {
		buf.Write(arg)

		// null deliminate every argument but the last
		if i != len(packet.Arguments)-1 {
			buf.WriteByte(0)
		}
	}
	return buf.Bytes(), nil
}
