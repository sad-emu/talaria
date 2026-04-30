package hodos

import (
	"context"
	"testing"
	"time"

	"talaria/config"
	"talaria/persistence"
)

func TestBuildS3Key_WithPrefixAndNestedPath(t *testing.T) {
	key := buildS3Key("/tmp/in", "/tmp/in/a/b/file.txt", "uploads")
	if key != "uploads/a/b/file.txt" {
		t.Fatalf("key = %q, want %q", key, "uploads/a/b/file.txt")
	}
}

func TestBuildS3Key_NoPrefix(t *testing.T) {
	key := buildS3Key("/tmp/in", "/tmp/in/file.txt", "")
	if key != "file.txt" {
		t.Fatalf("key = %q, want %q", key, "file.txt")
	}
}

func TestBuildS3Key_PathOutsideSourceFallsBackToBase(t *testing.T) {
	key := buildS3Key("/tmp/in", "/other/path/file.txt", "uploads")
	if key != "uploads/file.txt" {
		t.Fatalf("key = %q, want %q", key, "uploads/file.txt")
	}
}

type fakeSource struct {
	reads []struct {
		data   []byte
		handle any
	}
	readIdx int
	ackCnt  int
}

func (f *fakeSource) Name() string { return "fake-source" }
func (f *fakeSource) Read(ctx context.Context) ([]byte, any, error) {
	if f.readIdx >= len(f.reads) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	r := f.reads[f.readIdx]
	f.readIdx++
	return r.data, r.handle, nil
}
func (f *fakeSource) Ack(context.Context, any) error { f.ackCnt++; return nil }
func (f *fakeSource) Close() error                   { return nil }

type fakeKeyWriter struct {
	keys  []string
	data  [][]byte
	err   error
	delay time.Duration
}

func (f *fakeKeyWriter) WriteToKey(ctx context.Context, key string, data []byte) error {
	if f.err != nil {
		return f.err
	}
	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(f.delay):
		}
	}
	f.keys = append(f.keys, key)
	f.data = append(f.data, append([]byte(nil), data...))
	return nil
}
func (f *fakeKeyWriter) Close() error { return nil }

type fakeStore struct {
	records           map[string]persistence.HodosProgress
	inProgressUpserts map[string]int
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		records:           map[string]persistence.HodosProgress{},
		inProgressUpserts: map[string]int{},
	}
}

func (f *fakeStore) UpsertClaim(context.Context, persistence.TransferClaim) error { return nil }
func (f *fakeStore) GetClaimByTransferID(context.Context, string) (*persistence.TransferClaim, error) {
	return nil, nil
}
func (f *fakeStore) UpdateProgress(context.Context, string, int64, int64) error { return nil }
func (f *fakeStore) InsertChunkAck(context.Context, persistence.ChunkAck) error { return nil }
func (f *fakeStore) ExpireClaimsBefore(context.Context, int64) (int64, error)   { return 0, nil }
func (f *fakeStore) DeleteClaim(context.Context, string) error                  { return nil }
func (f *fakeStore) Close() error                                               { return nil }
func (f *fakeStore) UpsertHodosProgress(_ context.Context, p persistence.HodosProgress) error {
	key := p.HodosName + "|" + p.ItemKey
	f.records[key] = p
	if p.Status == "in_progress" {
		f.inProgressUpserts[key]++
	}
	return nil
}
func (f *fakeStore) GetHodosProgress(_ context.Context, hodosName string, itemKey string) (*persistence.HodosProgress, error) {
	p, ok := f.records[hodosName+"|"+itemKey]
	if !ok {
		return nil, nil
	}
	cp := p
	return &cp, nil
}
func (f *fakeStore) ListHodosProgress(_ context.Context, hodosName string, limit int, offset int) ([]persistence.HodosProgress, error) {
	if limit <= 0 {
		limit = 100
	}
	out := make([]persistence.HodosProgress, 0, limit)
	for _, p := range f.records {
		if p.HodosName == hodosName {
			out = append(out, p)
		}
	}
	if offset >= len(out) {
		return []persistence.HodosProgress{}, nil
	}
	end := offset + limit
	if end > len(out) {
		end = len(out)
	}
	return out[offset:end], nil
}
func (f *fakeStore) ListHodosProgressSummaries(_ context.Context) ([]persistence.HodosProgressSummary, error) {
	return []persistence.HodosProgressSummary{}, nil
}
func (f *fakeStore) DeleteHodosProgress(_ context.Context, hodosName string, itemKey string) error {
	delete(f.records, hodosName+"|"+itemKey)
	return nil
}

func TestRunner_StoresCompletedProgress(t *testing.T) {
	store := newFakeStore()
	source := &fakeSource{reads: []struct {
		data   []byte
		handle any
	}{{data: []byte("payload"), handle: "/tmp/in/a.txt"}}}
	sink := &fakeKeyWriter{}

	r := &runner{
		cfg: config.HodosConfig{
			Name:    "flow-a",
			Dropoff: config.HodosEndpointConfig{S3: &config.HodosS3Config{KeyPrefix: "uploads"}},
		},
		store:     store,
		source:    source,
		sourceDir: "/tmp/in",
		sinkType:  "s3",
		s3Sink:    sink,
		s3Prefix:  "uploads",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n, err := r.runOnce(ctx)
	if err != nil {
		t.Fatalf("runOnce() error = %v", err)
	}
	if n != 1 {
		t.Fatalf("processed = %d, want 1", n)
	}
	p, err := store.GetHodosProgress(context.Background(), "flow-a", "/tmp/in/a.txt")
	if err != nil || p == nil {
		t.Fatalf("expected stored progress, err=%v progress=%v", err, p)
	}
	if p.Status != "completed" {
		t.Fatalf("status = %q, want completed", p.Status)
	}
	if p.SizeBytes != int64(len("payload")) {
		t.Fatalf("size bytes = %d, want %d", p.SizeBytes, len("payload"))
	}
	if p.SourceType != "local" || p.DestinationType != "s3" {
		t.Fatalf("source/destination types = %q/%q, want local/s3", p.SourceType, p.DestinationType)
	}
	if p.StartedUnixNano <= 0 || p.DurationUnixNano < 0 {
		t.Fatalf("unexpected timing fields: started=%d duration=%d", p.StartedUnixNano, p.DurationUnixNano)
	}
	if len(sink.keys) != 1 || sink.keys[0] != "uploads/a.txt" {
		t.Fatalf("sink keys = %#v", sink.keys)
	}
}

func TestRunner_SkipsCompletedAfterRestart(t *testing.T) {
	store := newFakeStore()
	store.records["flow-a|/tmp/in/a.txt"] = persistence.HodosProgress{
		HodosName:         "flow-a",
		ItemKey:           "/tmp/in/a.txt",
		SinkKey:           "uploads/a.txt",
		Status:            "completed",
		CompletedUnixNano: 1,
	}
	source := &fakeSource{reads: []struct {
		data   []byte
		handle any
	}{{data: []byte("payload"), handle: "/tmp/in/a.txt"}}}
	sink := &fakeKeyWriter{}

	r := &runner{
		cfg: config.HodosConfig{
			Name:    "flow-a",
			Dropoff: config.HodosEndpointConfig{S3: &config.HodosS3Config{KeyPrefix: "uploads"}},
		},
		store:     store,
		source:    source,
		sourceDir: "/tmp/in",
		sinkType:  "s3",
		s3Sink:    sink,
		s3Prefix:  "uploads",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n, err := r.runOnce(ctx)
	if err != nil {
		t.Fatalf("runOnce() error = %v", err)
	}
	if n != 0 {
		t.Fatalf("processed = %d, want 0 because item was already completed", n)
	}
	if len(sink.keys) != 0 {
		t.Fatalf("sink should not be invoked, got keys %#v", sink.keys)
	}
}

func TestRunner_StoresFailedProgressOnWriteError(t *testing.T) {
	store := newFakeStore()
	source := &fakeSource{reads: []struct {
		data   []byte
		handle any
	}{{data: []byte("payload"), handle: "/tmp/in/a.txt"}}}
	sink := &fakeKeyWriter{err: context.DeadlineExceeded}

	r := &runner{
		cfg: config.HodosConfig{
			Name:    "flow-a",
			Dropoff: config.HodosEndpointConfig{S3: &config.HodosS3Config{KeyPrefix: "uploads"}},
		},
		store:     store,
		source:    source,
		sourceDir: "/tmp/in",
		sinkType:  "s3",
		s3Sink:    sink,
		s3Prefix:  "uploads",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := r.runOnce(ctx)
	if err == nil {
		t.Fatalf("expected runOnce() error")
	}
	p, err := store.GetHodosProgress(context.Background(), "flow-a", "/tmp/in/a.txt")
	if err != nil || p == nil {
		t.Fatalf("expected stored failure progress, err=%v progress=%v", err, p)
	}
	if p.Status != "failed" {
		t.Fatalf("status = %q, want failed", p.Status)
	}
	if p.Message == "" {
		t.Fatalf("failed progress message should not be empty")
	}
	if p.SizeBytes != int64(len("payload")) {
		t.Fatalf("size bytes = %d, want %d", p.SizeBytes, len("payload"))
	}
	if p.SourceType != "local" || p.DestinationType != "s3" {
		t.Fatalf("source/destination types = %q/%q, want local/s3", p.SourceType, p.DestinationType)
	}
}

func TestRunner_RefreshesInProgressDuringWrite(t *testing.T) {
	store := newFakeStore()
	source := &fakeSource{reads: []struct {
		data   []byte
		handle any
	}{{data: []byte("payload"), handle: "/tmp/in/a.txt"}}}
	sink := &fakeKeyWriter{delay: 1300 * time.Millisecond}

	r := &runner{
		cfg: config.HodosConfig{
			Name:    "flow-a",
			Dropoff: config.HodosEndpointConfig{S3: &config.HodosS3Config{KeyPrefix: "uploads"}},
		},
		store:     store,
		source:    source,
		sourceDir: "/tmp/in",
		sinkType:  "s3",
		s3Sink:    sink,
		s3Prefix:  "uploads",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := r.runOnce(ctx); err != nil {
		t.Fatalf("runOnce() error = %v", err)
	}

	key := "flow-a|/tmp/in/a.txt"
	if store.inProgressUpserts[key] < 2 {
		t.Fatalf("in-progress upserts = %d, want at least 2", store.inProgressUpserts[key])
	}
}
