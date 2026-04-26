package persistence

import (
	"context"
	"fmt"
)

type Backend string

const (
	BackendSQLite Backend = "sqlite"
	BackendMySQL  Backend = "mysql" // future
	BackendMongo  Backend = "mongo" // future
)

type Config struct {
	Backend    Backend
	SQLitePath string
}

type TransferClaim struct {
	TransferID       string
	CustomerID       string
	OwnerPeer        string
	FileUUID         string
	Connector        string
	NextOffset       int64
	LastSeenUnixNano int64
	LeaseUntilUnixNs int64
	UpdatedUnixNano  int64
}

type ChunkAck struct {
	AckID             string
	DataRespID        string
	RequestID         string
	TransferID        string
	FileUUID          string
	NodeName          string
	Offset            int64
	Length            int64
	DataHash          string
	Status            string
	Message           string
	TimestampUnixNano int64
}

type HodosProgress struct {
	HodosName         string
	ItemKey           string
	SinkKey           string
	Status            string
	Message           string
	UpdatedUnixNano   int64
	CompletedUnixNano int64
}

type TransferStore interface {
	UpsertClaim(ctx context.Context, c TransferClaim) error
	GetClaimByTransferID(ctx context.Context, transferID string) (*TransferClaim, error)
	UpdateProgress(ctx context.Context, transferID string, nextOffset int64, nowUnixNano int64) error
	InsertChunkAck(ctx context.Context, a ChunkAck) error
	ExpireClaimsBefore(ctx context.Context, cutoffUnixNano int64) (int64, error)
	DeleteClaim(ctx context.Context, transferID string) error
	UpsertHodosProgress(ctx context.Context, p HodosProgress) error
	GetHodosProgress(ctx context.Context, hodosName string, itemKey string) (*HodosProgress, error)
	DeleteHodosProgress(ctx context.Context, hodosName string, itemKey string) error
	Close() error
}

func OpenTransferStore(ctx context.Context, cfg Config) (TransferStore, error) {
	switch cfg.Backend {
	case BackendSQLite:
		return OpenSQLiteTransferStore(ctx, cfg.SQLitePath)
	case BackendMySQL, BackendMongo:
		return nil, fmt.Errorf("persistence: backend %q not implemented yet", cfg.Backend)
	default:
		return nil, fmt.Errorf("persistence: unsupported backend %q", cfg.Backend)
	}
}
