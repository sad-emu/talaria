package persistence

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) TransferStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := OpenTransferStore(context.Background(), Config{
		Backend:    BackendSQLite,
		SQLitePath: path,
	})
	if err != nil {
		t.Fatalf("OpenTransferStore() error = %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func testClaim(transferID string) TransferClaim {
	now := time.Now().UnixNano()
	return TransferClaim{
		TransferID:       transferID,
		CustomerID:       "customer-x",
		OwnerPeer:        "CN=instance-b,O=customer-x",
		FileUUID:         "file-uuid-1",
		Connector:        "feed_a",
		NextOffset:       0,
		LastSeenUnixNano: now,
		LeaseUntilUnixNs: now + int64(2*time.Hour),
		UpdatedUnixNano:  now,
	}
}

func TestSQLiteStore_UpsertAndGetClaim(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	claim := testClaim("xfer-1")
	if err := store.UpsertClaim(ctx, claim); err != nil {
		t.Fatalf("UpsertClaim() error = %v", err)
	}

	got, err := store.GetClaimByTransferID(ctx, "xfer-1")
	if err != nil {
		t.Fatalf("GetClaimByTransferID() error = %v", err)
	}
	if got == nil {
		t.Fatal("GetClaimByTransferID() returned nil, want claim")
	}
	if got.TransferID != claim.TransferID {
		t.Errorf("TransferID = %q, want %q", got.TransferID, claim.TransferID)
	}
	if got.CustomerID != claim.CustomerID {
		t.Errorf("CustomerID = %q, want %q", got.CustomerID, claim.CustomerID)
	}
	if got.FileUUID != claim.FileUUID {
		t.Errorf("FileUUID = %q, want %q", got.FileUUID, claim.FileUUID)
	}
	if got.Connector != claim.Connector {
		t.Errorf("Connector = %q, want %q", got.Connector, claim.Connector)
	}
	if got.NextOffset != claim.NextOffset {
		t.Errorf("NextOffset = %d, want %d", got.NextOffset, claim.NextOffset)
	}
}

func TestSQLiteStore_GetClaim_NotFound(t *testing.T) {
	store := openTestStore(t)

	got, err := store.GetClaimByTransferID(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("GetClaimByTransferID() error = %v", err)
	}
	if got != nil {
		t.Fatalf("GetClaimByTransferID() = %+v, want nil", got)
	}
}

func TestSQLiteStore_UpsertClaim_UpdatesExisting(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	claim := testClaim("xfer-2")
	if err := store.UpsertClaim(ctx, claim); err != nil {
		t.Fatalf("UpsertClaim() first error = %v", err)
	}

	claim.NextOffset = 4096
	claim.OwnerPeer = "CN=instance-c,O=customer-x"
	if err := store.UpsertClaim(ctx, claim); err != nil {
		t.Fatalf("UpsertClaim() update error = %v", err)
	}

	got, err := store.GetClaimByTransferID(ctx, "xfer-2")
	if err != nil {
		t.Fatalf("GetClaimByTransferID() error = %v", err)
	}
	if got.NextOffset != 4096 {
		t.Errorf("NextOffset = %d, want 4096", got.NextOffset)
	}
	if got.OwnerPeer != "CN=instance-c,O=customer-x" {
		t.Errorf("OwnerPeer = %q, want updated value", got.OwnerPeer)
	}
}

func TestSQLiteStore_UpdateProgress(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	if err := store.UpsertClaim(ctx, testClaim("xfer-3")); err != nil {
		t.Fatalf("UpsertClaim() error = %v", err)
	}

	now := time.Now().UnixNano()
	if err := store.UpdateProgress(ctx, "xfer-3", 8192, now); err != nil {
		t.Fatalf("UpdateProgress() error = %v", err)
	}

	got, err := store.GetClaimByTransferID(ctx, "xfer-3")
	if err != nil {
		t.Fatalf("GetClaimByTransferID() error = %v", err)
	}
	if got.NextOffset != 8192 {
		t.Errorf("NextOffset = %d, want 8192", got.NextOffset)
	}
	if got.LastSeenUnixNano != now {
		t.Errorf("LastSeenUnixNano = %d, want %d", got.LastSeenUnixNano, now)
	}
}

func TestSQLiteStore_InsertChunkAck(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	ack := ChunkAck{
		AckID:             "ack-1",
		DataRespID:        "resp-1",
		RequestID:         "req-1",
		TransferID:        "xfer-4",
		FileUUID:          "file-uuid-1",
		NodeName:          "instance-b",
		Offset:            0,
		Length:            4096,
		DataHash:          "abc123",
		Status:            "RECEIVED",
		Message:           "ok",
		TimestampUnixNano: time.Now().UnixNano(),
	}
	if err := store.InsertChunkAck(ctx, ack); err != nil {
		t.Fatalf("InsertChunkAck() error = %v", err)
	}
}

func TestSQLiteStore_InsertChunkAck_DuplicateAckID(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	ack := ChunkAck{
		AckID:             "ack-dup",
		DataRespID:        "resp-1",
		TransferID:        "xfer-5",
		FileUUID:          "file-uuid-1",
		NodeName:          "instance-b",
		Status:            "RECEIVED",
		TimestampUnixNano: time.Now().UnixNano(),
	}
	if err := store.InsertChunkAck(ctx, ack); err != nil {
		t.Fatalf("InsertChunkAck() first error = %v", err)
	}
	if err := store.InsertChunkAck(ctx, ack); err == nil {
		t.Fatal("InsertChunkAck() duplicate: expected error, got nil")
	}
}

func TestSQLiteStore_ExpireClaimsBefore(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	now := time.Now().UnixNano()

	expiredClaim := testClaim("xfer-expired")
	expiredClaim.LeaseUntilUnixNs = now - int64(time.Second)
	if err := store.UpsertClaim(ctx, expiredClaim); err != nil {
		t.Fatalf("UpsertClaim() expired error = %v", err)
	}

	activeClaim := testClaim("xfer-active")
	activeClaim.LeaseUntilUnixNs = now + int64(2*time.Hour)
	if err := store.UpsertClaim(ctx, activeClaim); err != nil {
		t.Fatalf("UpsertClaim() active error = %v", err)
	}

	n, err := store.ExpireClaimsBefore(ctx, now)
	if err != nil {
		t.Fatalf("ExpireClaimsBefore() error = %v", err)
	}
	if n != 1 {
		t.Errorf("ExpireClaimsBefore() = %d rows, want 1", n)
	}

	got, err := store.GetClaimByTransferID(ctx, "xfer-expired")
	if err != nil {
		t.Fatalf("GetClaimByTransferID() error = %v", err)
	}
	if got != nil {
		t.Fatal("expected expired claim to be deleted, still present")
	}

	got, err = store.GetClaimByTransferID(ctx, "xfer-active")
	if err != nil {
		t.Fatalf("GetClaimByTransferID() active error = %v", err)
	}
	if got == nil {
		t.Fatal("expected active claim to remain, got nil")
	}
}

func TestSQLiteStore_DeleteClaim(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	if err := store.UpsertClaim(ctx, testClaim("xfer-del")); err != nil {
		t.Fatalf("UpsertClaim() error = %v", err)
	}
	if err := store.DeleteClaim(ctx, "xfer-del"); err != nil {
		t.Fatalf("DeleteClaim() error = %v", err)
	}

	got, err := store.GetClaimByTransferID(ctx, "xfer-del")
	if err != nil {
		t.Fatalf("GetClaimByTransferID() error = %v", err)
	}
	if got != nil {
		t.Fatal("expected claim to be deleted, still present")
	}
}

func TestSQLiteStore_DeleteClaim_NonExistent(t *testing.T) {
	store := openTestStore(t)
	// Should not error on deleting a claim that does not exist.
	if err := store.DeleteClaim(context.Background(), "nonexistent"); err != nil {
		t.Fatalf("DeleteClaim() nonexistent error = %v", err)
	}
}

func TestSQLiteStore_HodosProgress_UpsertGetDelete(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UnixNano()

	rec := HodosProgress{
		HodosName:         "local-to-s3",
		ItemKey:           "/tmp/input/a.txt",
		SinkKey:           "uploads/a.txt",
		Status:            "completed",
		Message:           "ok",
		UpdatedUnixNano:   now,
		CompletedUnixNano: now,
	}
	if err := store.UpsertHodosProgress(ctx, rec); err != nil {
		t.Fatalf("UpsertHodosProgress() error = %v", err)
	}

	got, err := store.GetHodosProgress(ctx, rec.HodosName, rec.ItemKey)
	if err != nil {
		t.Fatalf("GetHodosProgress() error = %v", err)
	}
	if got == nil {
		t.Fatal("GetHodosProgress() returned nil")
	}
	if got.Status != "completed" || got.SinkKey != rec.SinkKey {
		t.Fatalf("GetHodosProgress() = %+v, want status=completed sink=%q", got, rec.SinkKey)
	}

	rec.Status = "in_progress"
	rec.CompletedUnixNano = 0
	if err := store.UpsertHodosProgress(ctx, rec); err != nil {
		t.Fatalf("UpsertHodosProgress() update error = %v", err)
	}
	got, err = store.GetHodosProgress(ctx, rec.HodosName, rec.ItemKey)
	if err != nil {
		t.Fatalf("GetHodosProgress() update error = %v", err)
	}
	if got.Status != "in_progress" {
		t.Fatalf("updated status = %q, want in_progress", got.Status)
	}

	if err := store.DeleteHodosProgress(ctx, rec.HodosName, rec.ItemKey); err != nil {
		t.Fatalf("DeleteHodosProgress() error = %v", err)
	}
	got, err = store.GetHodosProgress(ctx, rec.HodosName, rec.ItemKey)
	if err != nil {
		t.Fatalf("GetHodosProgress() after delete error = %v", err)
	}
	if got != nil {
		t.Fatal("expected deleted Hodos progress to be nil")
	}
}

func TestSQLiteStore_ListHodosProgress(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	records := []HodosProgress{
		{HodosName: "flow-a", ItemKey: "/tmp/in/a.txt", SinkKey: "uploads/a.txt", Status: "completed", UpdatedUnixNano: 20, CompletedUnixNano: 20},
		{HodosName: "flow-a", ItemKey: "/tmp/in/b.txt", SinkKey: "uploads/b.txt", Status: "in_progress", UpdatedUnixNano: 30},
		{HodosName: "flow-b", ItemKey: "/tmp/in/c.txt", SinkKey: "uploads/c.txt", Status: "failed", UpdatedUnixNano: 10},
	}
	for _, rec := range records {
		if err := store.UpsertHodosProgress(ctx, rec); err != nil {
			t.Fatalf("UpsertHodosProgress() error = %v", err)
		}
	}

	list, err := store.ListHodosProgress(ctx, "flow-a", 10, 0)
	if err != nil {
		t.Fatalf("ListHodosProgress() error = %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("len(list) = %d, want 2", len(list))
	}
	if list[0].ItemKey != "/tmp/in/b.txt" {
		t.Fatalf("first item key = %q, want /tmp/in/b.txt", list[0].ItemKey)
	}
	if list[1].ItemKey != "/tmp/in/a.txt" {
		t.Fatalf("second item key = %q, want /tmp/in/a.txt", list[1].ItemKey)
	}
}

func TestSQLiteStore_ListHodosProgressSummaries(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	rows := []HodosProgress{
		{HodosName: "flow-a", ItemKey: "a", SinkKey: "sa", Status: "completed", UpdatedUnixNano: 10, CompletedUnixNano: 10},
		{HodosName: "flow-a", ItemKey: "b", SinkKey: "sb", Status: "in_progress", UpdatedUnixNano: 20},
		{HodosName: "flow-a", ItemKey: "c", SinkKey: "sc", Status: "failed", UpdatedUnixNano: 30},
		{HodosName: "flow-b", ItemKey: "d", SinkKey: "sd", Status: "completed", UpdatedUnixNano: 15, CompletedUnixNano: 15},
	}
	for _, rec := range rows {
		if err := store.UpsertHodosProgress(ctx, rec); err != nil {
			t.Fatalf("UpsertHodosProgress() error = %v", err)
		}
	}

	summaries, err := store.ListHodosProgressSummaries(ctx)
	if err != nil {
		t.Fatalf("ListHodosProgressSummaries() error = %v", err)
	}
	if len(summaries) != 2 {
		t.Fatalf("len(summaries) = %d, want 2", len(summaries))
	}
	if summaries[0].HodosName != "flow-a" {
		t.Fatalf("first summary hodos = %q, want flow-a", summaries[0].HodosName)
	}
	if summaries[0].Total != 3 || summaries[0].InProgress != 1 || summaries[0].Completed != 1 || summaries[0].Failed != 1 {
		t.Fatalf("flow-a summary = %+v, want total=3 in_progress=1 completed=1 failed=1", summaries[0])
	}
	if summaries[0].LastUpdatedUnixNs != 30 {
		t.Fatalf("flow-a LastUpdatedUnixNs = %d, want 30", summaries[0].LastUpdatedUnixNs)
	}
}
