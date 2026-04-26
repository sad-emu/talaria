package hodos

import (
	"context"
	"testing"

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
	reads  []struct {
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
func (f *fakeSource) Close() error                  { return nil }

type fakeKeyWriter struct {
	keys []string
	data [][]byte
}

func (f *fakeKeyWriter) WriteToKey(_ context.Context, key string, data []byte) error {
	f.keys = append(f.keys, key)
	f.data = append(f.data, append([]byte(nil), data...))
	return nil
}
func (f *fakeKeyWriter) Close() error { return nil }

type fakeStore struct {
	records map[string]persistence.HodosProgress
}

func newFakeStore() *fakeStore {
	return &fakeStore{records: map[string]persistence.HodosProgress{}}
}

func (f *fakeStore) UpsertClaim(context.Context, persistence.TransferClaim) error { return nil }
func (f *fakeStore) GetClaimByTransferID(context.Context, string) (*persistence.TransferClaim, error) {
	return nil, nil
}
func (f *fakeStore) UpdateProgress(context.Context, string, int64, int64) error { return nil }
func (f *fakeStore) InsertChunkAck(context.Context, persistence.ChunkAck) error  { return nil }
func (f *fakeStore) ExpireClaimsBefore(context.Context, int64) (int64, error)    { return 0, nil }
func (f *fakeStore) DeleteClaim(context.Context, string) error                    { return nil }
func (f *fakeStore) Close() error                                                 { return nil }
func (f *fakeStore) UpsertHodosProgress(_ context.Context, p persistence.HodosProgress) error {
	f.records[p.HodosName+"|"+p.ItemKey] = p
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
			Name: "flow-a",
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
			Name: "flow-a",
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
