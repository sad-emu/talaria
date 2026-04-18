package connector

// Package connector defines the interfaces for talaria pickup and dropoff
// connectors.  Implementations (e.g. local disk, SFTP) will live in
// sub-packages of this package.

import "context"

// Source is a pickup connector: it reads data items for talaria to transport.
type Source interface {
	// Name returns a human-readable identifier for this source.
	Name() string
	// Read blocks until a data item is available or ctx is done.
	// Returns the item bytes and an opaque delivery handle.  The handle
	// must be passed to Ack after the item has been delivered successfully.
	Read(ctx context.Context) (data []byte, handle any, err error)
	// Ack signals that the item identified by handle was delivered
	// successfully and can be removed/archived from the source.
	Ack(ctx context.Context, handle any) error
	// Close releases resources held by the source.
	Close() error
}

// Sink is a dropoff connector: it writes data items delivered by talaria.
type Sink interface {
	// Name returns a human-readable identifier for this sink.
	Name() string
	// Write persists data to the sink.
	Write(ctx context.Context, data []byte) error
	// Close releases resources held by the sink.
	Close() error
}
