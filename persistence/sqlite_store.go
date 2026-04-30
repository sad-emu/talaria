package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

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

CREATE TABLE IF NOT EXISTS hodos_progress (
	hodos_name TEXT NOT NULL,
	item_key TEXT NOT NULL,
	sink_key TEXT NOT NULL,
	status TEXT NOT NULL,
	message TEXT NOT NULL,
	started_ns INTEGER NOT NULL DEFAULT 0,
	updated_ns INTEGER NOT NULL,
	completed_ns INTEGER NOT NULL,
	duration_ns INTEGER NOT NULL DEFAULT 0,
	size_bytes INTEGER NOT NULL DEFAULT 0,
	source_type TEXT NOT NULL DEFAULT '',
	source_details TEXT NOT NULL DEFAULT '',
	destination_type TEXT NOT NULL DEFAULT '',
	destination_detail TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (hodos_name, item_key)
);

CREATE INDEX IF NOT EXISTS idx_hodos_progress_status ON hodos_progress(hodos_name, status);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("persistence: init schema: %w", err)
	}
	if err := s.ensureHodosProgressColumns(ctx); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteTransferStore) ensureHodosProgressColumns(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "PRAGMA table_info(hodos_progress);")
	if err != nil {
		return fmt.Errorf("persistence: hodos_progress schema inspect: %w", err)
	}
	defer rows.Close()

	cols := map[string]struct{}{}
	for rows.Next() {
		var cid int
		var name string
		var dataType string
		var notNull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &dflt, &pk); err != nil {
			return fmt.Errorf("persistence: hodos_progress schema scan: %w", err)
		}
		cols[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("persistence: hodos_progress schema rows: %w", err)
	}

	needed := []struct {
		name string
		ddl  string
	}{
		{name: "started_ns", ddl: "ALTER TABLE hodos_progress ADD COLUMN started_ns INTEGER NOT NULL DEFAULT 0;"},
		{name: "duration_ns", ddl: "ALTER TABLE hodos_progress ADD COLUMN duration_ns INTEGER NOT NULL DEFAULT 0;"},
		{name: "size_bytes", ddl: "ALTER TABLE hodos_progress ADD COLUMN size_bytes INTEGER NOT NULL DEFAULT 0;"},
		{name: "source_type", ddl: "ALTER TABLE hodos_progress ADD COLUMN source_type TEXT NOT NULL DEFAULT '';"},
		{name: "source_details", ddl: "ALTER TABLE hodos_progress ADD COLUMN source_details TEXT NOT NULL DEFAULT '';"},
		{name: "destination_type", ddl: "ALTER TABLE hodos_progress ADD COLUMN destination_type TEXT NOT NULL DEFAULT '';"},
		{name: "destination_detail", ddl: "ALTER TABLE hodos_progress ADD COLUMN destination_detail TEXT NOT NULL DEFAULT '';"},
	}

	for _, col := range needed {
		if _, ok := cols[col.name]; ok {
			continue
		}
		if _, err := s.db.ExecContext(ctx, col.ddl); err != nil {
			return fmt.Errorf("persistence: add hodos_progress column %s: %w", col.name, err)
		}
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

func (s *SQLiteTransferStore) UpsertHodosProgress(ctx context.Context, p HodosProgress) error {
	const q = `
INSERT INTO hodos_progress (
  hodos_name, item_key, sink_key, status, message, started_ns, updated_ns, completed_ns,
  duration_ns, size_bytes, source_type, source_details, destination_type, destination_detail
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(hodos_name, item_key) DO UPDATE SET
  sink_key=excluded.sink_key,
  status=excluded.status,
  message=excluded.message,
  started_ns=excluded.started_ns,
  updated_ns=excluded.updated_ns,
  completed_ns=excluded.completed_ns,
  duration_ns=excluded.duration_ns,
  size_bytes=excluded.size_bytes,
  source_type=excluded.source_type,
  source_details=excluded.source_details,
  destination_type=excluded.destination_type,
  destination_detail=excluded.destination_detail;
`
	_, err := s.db.ExecContext(ctx, q,
		p.HodosName,
		p.ItemKey,
		p.SinkKey,
		p.Status,
		p.Message,
		p.StartedUnixNano,
		p.UpdatedUnixNano,
		p.CompletedUnixNano,
		p.DurationUnixNano,
		p.SizeBytes,
		p.SourceType,
		p.SourceDetails,
		p.DestinationType,
		p.DestinationDetail,
	)
	if err != nil {
		return fmt.Errorf("persistence: upsert hodos progress: %w", err)
	}
	return nil
}

func (s *SQLiteTransferStore) GetHodosProgress(ctx context.Context, hodosName string, itemKey string) (*HodosProgress, error) {
	const q = `
SELECT hodos_name, item_key, sink_key, status, message, started_ns, updated_ns, completed_ns,
       duration_ns, size_bytes, source_type, source_details, destination_type, destination_detail
FROM hodos_progress
WHERE hodos_name = ? AND item_key = ?;
`
	var p HodosProgress
	err := s.db.QueryRowContext(ctx, q, hodosName, itemKey).Scan(
		&p.HodosName,
		&p.ItemKey,
		&p.SinkKey,
		&p.Status,
		&p.Message,
		&p.StartedUnixNano,
		&p.UpdatedUnixNano,
		&p.CompletedUnixNano,
		&p.DurationUnixNano,
		&p.SizeBytes,
		&p.SourceType,
		&p.SourceDetails,
		&p.DestinationType,
		&p.DestinationDetail,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("persistence: get hodos progress: %w", err)
	}
	return &p, nil
}

func (s *SQLiteTransferStore) ListHodosProgress(ctx context.Context, hodosName string, limit int, offset int) ([]HodosProgress, error) {
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	const q = `
SELECT hodos_name, item_key, sink_key, status, message, started_ns, updated_ns, completed_ns,
       duration_ns, size_bytes, source_type, source_details, destination_type, destination_detail
FROM hodos_progress
WHERE hodos_name = ?
ORDER BY updated_ns DESC, item_key ASC
LIMIT ? OFFSET ?;
`
	rows, err := s.db.QueryContext(ctx, q, hodosName, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("persistence: list hodos progress: %w", err)
	}
	defer rows.Close()

	out := make([]HodosProgress, 0, limit)
	for rows.Next() {
		var p HodosProgress
		if err := rows.Scan(
			&p.HodosName,
			&p.ItemKey,
			&p.SinkKey,
			&p.Status,
			&p.Message,
			&p.StartedUnixNano,
			&p.UpdatedUnixNano,
			&p.CompletedUnixNano,
			&p.DurationUnixNano,
			&p.SizeBytes,
			&p.SourceType,
			&p.SourceDetails,
			&p.DestinationType,
			&p.DestinationDetail,
		); err != nil {
			return nil, fmt.Errorf("persistence: list hodos progress scan: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("persistence: list hodos progress rows: %w", err)
	}

	return out, nil
}

func (s *SQLiteTransferStore) ListHodosProgressSummaries(ctx context.Context) ([]HodosProgressSummary, error) {
	const q = `
SELECT
	hodos_name,
	COUNT(*) AS total,
	SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS in_progress,
	SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
	SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
	MAX(updated_ns) AS last_updated_ns
FROM hodos_progress
GROUP BY hodos_name
ORDER BY hodos_name ASC;
`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("persistence: list hodos progress summaries: %w", err)
	}
	defer rows.Close()

	out := []HodosProgressSummary{}
	for rows.Next() {
		var s HodosProgressSummary
		if err := rows.Scan(
			&s.HodosName,
			&s.Total,
			&s.InProgress,
			&s.Completed,
			&s.Failed,
			&s.LastUpdatedUnixNs,
		); err != nil {
			return nil, fmt.Errorf("persistence: list hodos progress summaries scan: %w", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("persistence: list hodos progress summaries rows: %w", err)
	}

	return out, nil
}

func (s *SQLiteTransferStore) DeleteHodosProgress(ctx context.Context, hodosName string, itemKey string) error {
	const q = `DELETE FROM hodos_progress WHERE hodos_name = ? AND item_key = ?;`
	_, err := s.db.ExecContext(ctx, q, hodosName, itemKey)
	if err != nil {
		return fmt.Errorf("persistence: delete hodos progress: %w", err)
	}
	return nil
}

func (s *SQLiteTransferStore) Close() error {
	return s.db.Close()
}
