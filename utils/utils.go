// Package utils provides utility types and functions for the Gearman client.
//
// This package includes helper types like Buffer that implement common interfaces
// used throughout the Gearman client implementation.
package utils

import (
	"bytes"
)

// Buffer is a WriteCloser that wraps a bytes.Buffer.
// It provides a convenient way to collect data in memory while implementing
// the io.WriteCloser interface.
type Buffer struct {
	bytes.Buffer
}

// Close is a no-op method that satisfies the io.Closer interface.
func (b Buffer) Close() error {
	return nil
}

// NewBuffer initializes and returns a new empty Buffer.
func NewBuffer() *Buffer {
	return &Buffer{}
}
