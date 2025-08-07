package scanner

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// needMoreData returns the requisite values to get the bufio.Scanner to send more data to
// the splitter.
// http://pkg.golang.org/pkg/bufio/#SplitFunc
func needMoreData() (int, []byte, error) { return 0, nil, nil }

const (
	headerSize    = 12
	maxPacketSize = 1 * 1024 * 1024 // 1MB: avoid memory exhaustion attacks
)

// New returns a new Scanner that parses a Reader as the Gearman protocol.
// See: http://gearman.org/protocol/
func New(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)

	buf := make([]byte, maxPacketSize)
	scanner.Buffer(buf, maxPacketSize)

	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if len(data) < headerSize {
			return needMoreData()
		}

		// Must have 4 byte magic, 4 byte type, 4 byte data size
		if len(data) < 12 {
			return 0, nil, fmt.Errorf("packet too short: %d bytes (minimum 12 required)", len(data))
		}

		var size uint32
		if err := binary.Read(bytes.NewBuffer(data[8:12]), binary.BigEndian, &size); err != nil {
			return 0, nil, fmt.Errorf("failed to read packet size: %w", err)
		}

		if size > maxPacketSize {
			return 0, nil, fmt.Errorf("packet too large: %d bytes (maximum %d allowed)", size, maxPacketSize)
		}

		totalSize := headerSize + int(size)
		if len(data) < totalSize {
			return needMoreData()
		}

		// Validate that we're not exceeding our buffer limits
		if totalSize > maxPacketSize {
			return 0, nil, fmt.Errorf("total packet size too large: %d bytes (maximum %d allowed)", totalSize, maxPacketSize)
		}

		// bufio.Scanner reuses these bytes, so make sure we copy them.
		packet := make([]byte, totalSize)
		copy(packet, data[0:totalSize])
		return totalSize, packet, nil
	})
	return scanner
}
