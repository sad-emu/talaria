package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "modernc.org/sqlite"
)

type SQLiteTransferStore struct {
	db *sql.DB
}

func OpenSQLiteTransferStore(ctx context.Context, path string) (*SQLiteTransferStore, error) {
	if path == "" {
		return nil, errors.New("persistence: sqlite path is required")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("persistence: open sqlite: %w", err)
	}

	s := &SQLiteTransferStore{db: db}
	if err := s.initSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLiteTransferStore) initSchema(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS transfer_claims (
  transfer_id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  owner_peer TEXT NOT NULL,
  file_uuid TEXT NOT NULL,
  connector TEXT NOT NULL,
  next_offset INTEGER NOT NULL,
  last_seen_ns INTEGER NOT NULL,
  lease_until_ns INTEGER NOT NULL,
  updated_ns INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transfer_claims_file ON transfer_claims(file_uuid);
CREATE INDEX IF NOT EXISTS idx_transfer_claims_lease ON transfer_claims(lease_until_ns);

CREATE TABLE IF NOT EXISTS chunk_acks (
  ack_id TEXT PRIMARY KEY,
  data_resp_id TEXT NOT NULL,
  req_id TEXT,
  transfer_id TEXT NOT NULL,
  file_uuid TEXT NOT NULL,
  node_name TEXT NOT NULL,
  offset INTEGER NOT NULL,
  length INTEGER NOT NULL,
  data_hash TEXT,
  status TEXT NOT NULL,
  message TEXT,
  ts_ns INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_chunk_acks_transfer ON chunk_acks(transfer_id);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("persistence: init schema: %w", err)
	}
	return nil
}

func (s *SQLiteTransferStore) UpsertClaim(ctx context.Context, c TransferClaim) error {
	const q = `
INSERT INTO transfer_claims (
  transfer_id, customer_id, owner_peer, file_uuid, connector,
  next_offset, last_seen_ns, lease_until_ns, updated_ns
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(transfer_id) DO UPDATE SET
  customer_id=excluded.customer_id,
  owner_peer=excluded.owner_peer,
  file_uuid=excluded.file_uuid,
  connector=excluded.connector,
  next_offset=excluded.next_offset,
  last_seen_ns=excluded.last_seen_ns,
  lease_until_ns=excluded.lease_until_ns,
  updated_ns=excluded.updated_ns;
`
	_, err := s.db.ExecContext(ctx, q,
		c.TransferID, c.CustomerID, c.OwnerPeer, c.FileUUID, c.Connector,
		c.NextOffset, c.LastSeenUnixNano, c.LeaseUntilUnixNs, c.UpdatedUnixNano,
	)
	if err != nil {
		return fmt.Errorf("persistence: upsert claim: %w", err)
	}
	return nil
}

func (s *SQLiteTransferStore) GetClaimByTransferID(ctx context.Context, transferID string) (*TransferClaim, error) {
	const q = `
SELECT transfer_id, customer_id, owner_peer, file_uuid, connector,
       next_offset, last_seen_ns, lease_until_ns, updated_ns
FROM transfer_claims
WHERE transfer_id = ?;
`
	var c TransferClaim
	err := s.db.QueryRowContext(ctx, q, transferID).Scan(
		&c.TransferID, &c.CustomerID, &c.OwnerPeer, &c.FileUUID, &c.Connector,
		&c.NextOffset, &c.LastSeenUnixNano, &c.LeaseUntilUnixNs, &c.UpdatedUnixNano,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("persistence: get claim: %w", err)
	}
	return &c, nil
}

func (s *SQLiteTransferStore) UpdateProgress(ctx context.Context, transferID string, nextOffset int64, nowUnixNano int64) error {
	const q = `
UPDATE transfer_claims
SET next_offset = ?, last_seen_ns = ?, updated_ns = ?
WHERE transfer_id = ?;
`
	_, err := s.db.ExecContext(ctx, q, nextOffset, nowUnixNano, nowUnixNano, transferID)
	if err != nil {
		return fmt.Errorf("persistence: update progress: %w", err)
	}
	return nil
}

func (s *SQLiteTransferStore) InsertChunkAck(ctx context.Context, a ChunkAck) error {
	const q = `
INSERT INTO chunk_acks (
  ack_id, data_resp_id, req_id, transfer_id, file_uuid, node_name,
  offset, length, data_hash, status, message, ts_ns
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`
	_, err := s.db.ExecContext(ctx, q,
		a.AckID, a.DataRespID, a.RequestID, a.TransferID, a.FileUUID, a.NodeName,
		a.Offset, a.Length, a.DataHash, a.Status, a.Message, a.TimestampUnixNano,
	)
	if err != nil {
		return fmt.Errorf("persistence: insert chunk ack: %w", err)
	}
	return nil
}

func (s *SQLiteTransferStore) ExpireClaimsBefore(ctx context.Context, cutoffUnixNano int64) (int64, error) {
	const q = `DELETE FROM transfer_claims WHERE lease_until_ns < ?;`
	res, err := s.db.ExecContext(ctx, q, cutoffUnixNano)
	if err != nil {
		return 0, fmt.Errorf("persistence: expire claims: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (s *SQLiteTransferStore) DeleteClaim(ctx context.Context, transferID string) error {
	const q = `DELETE FROM transfer_claims WHERE transfer_id = ?;`
	_, err := s.db.ExecContext(ctx, q, transferID)
	if err != nil {
		return fmt.Errorf("persistence: delete claim: %w", err)
	}
	return nil
}

func (s *SQLiteTransferStore) Close() error {
	return s.db.Close()
}
