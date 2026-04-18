package connector_test

import (
	"context"
	"errors"
	"testing"

	"talaria/connector"
)

// ---------------------------------------------------------------------------
// Compile-time interface satisfaction checks
// ---------------------------------------------------------------------------

// mockSource implements connector.Source for testing.
type mockSource struct {
	name   string
	data   []byte
	readFn func(ctx context.Context) ([]byte, any, error)
}

func (m *mockSource) Name() string { return m.name }
func (m *mockSource) Read(ctx context.Context) ([]byte, any, error) {
	if m.readFn != nil {
		return m.readFn(ctx)
	}
	return m.data, "handle", nil
}
func (m *mockSource) Ack(_ context.Context, _ any) error { return nil }
func (m *mockSource) Close() error                       { return nil }

// mockSink implements connector.Sink for testing.
type mockSink struct {
	name    string
	written [][]byte
}

func (m *mockSink) Name() string { return m.name }
func (m *mockSink) Write(_ context.Context, data []byte) error {
	m.written = append(m.written, data)
	return nil
}
func (m *mockSink) Close() error { return nil }

// Ensure the mock types satisfy the interfaces at compile time.
var _ connector.Source = (*mockSource)(nil)
var _ connector.Sink = (*mockSink)(nil)

// ---------------------------------------------------------------------------
// Behavioural tests against the interfaces
// ---------------------------------------------------------------------------

func TestSource_Name(t *testing.T) {
	s := &mockSource{name: "disk-source"}
	if got := s.Name(); got != "disk-source" {
		t.Errorf("Name() = %q, want disk-source", got)
	}
}

func TestSource_Read(t *testing.T) {
	want := []byte("hello")
	s := &mockSource{data: want}
	data, handle, err := s.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(data) != string(want) {
		t.Errorf("data = %q, want %q", data, want)
	}
	if handle == nil {
		t.Error("handle should not be nil")
	}
}

func TestSource_Read_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &mockSource{
		readFn: func(ctx context.Context) ([]byte, any, error) {
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			default:
				return []byte("data"), "h", nil
			}
		},
	}
	_, _, err := s.Read(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestSink_Name(t *testing.T) {
	s := &mockSink{name: "sftp-sink"}
	if got := s.Name(); got != "sftp-sink" {
		t.Errorf("Name() = %q, want sftp-sink", got)
	}
}

func TestSink_Write(t *testing.T) {
	s := &mockSink{}
	data := []byte("payload")
	if err := s.Write(context.Background(), data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(s.written) != 1 || string(s.written[0]) != string(data) {
		t.Errorf("written = %v, want [%q]", s.written, data)
	}
}

func TestSink_Write_Multiple(t *testing.T) {
	s := &mockSink{}
	for i := 0; i < 3; i++ {
		s.Write(context.Background(), []byte("msg"))
	}
	if len(s.written) != 3 {
		t.Errorf("expected 3 writes, got %d", len(s.written))
	}
}
