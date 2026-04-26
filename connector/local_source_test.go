package connector

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewLocalSource_Valid(t *testing.T) {
	dir := t.TempDir()
	s, err := NewLocalSource(LocalSourceConfig{Path: dir})
	if err != nil {
		t.Fatalf("NewLocalSource() error = %v", err)
	}
	if s.Name() != "local" {
		t.Fatalf("Name() = %q, want %q", s.Name(), "local")
	}
}

func TestNewLocalSource_InvalidPath(t *testing.T) {
	_, err := NewLocalSource(LocalSourceConfig{Path: "/does/not/exist"})
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

func TestNewLocalSource_PathMustBeDirectory(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := NewLocalSource(LocalSourceConfig{Path: file})
	if err == nil {
		t.Fatal("expected error for non-directory path")
	}
}

func TestLocalSource_ReadAndAck_DeleteByDefault(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "one.txt")
	if err := os.WriteFile(f, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := NewLocalSource(LocalSourceConfig{Path: dir})
	if err != nil {
		t.Fatal(err)
	}

	data, handle, err := s.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("Read() data = %q, want hello", string(data))
	}
	if err := s.Ack(context.Background(), handle); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if _, err := os.Stat(f); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected file to be deleted, stat err = %v", err)
	}
}

func TestLocalSource_KeepFiles(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "one.txt")
	if err := os.WriteFile(f, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := NewLocalSource(LocalSourceConfig{Path: dir, KeepFiles: true})
	if err != nil {
		t.Fatal(err)
	}

	_, handle, err := s.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if err := s.Ack(context.Background(), handle); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if _, err := os.Stat(f); err != nil {
		t.Fatalf("expected file to remain, stat err = %v", err)
	}
}

func TestLocalSource_RecurseFalse_SkipsNested(t *testing.T) {
	dir := t.TempDir()
	top := filepath.Join(dir, "top.txt")
	nestedDir := filepath.Join(dir, "nested")
	nested := filepath.Join(nestedDir, "nested.txt")
	if err := os.MkdirAll(nestedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(top, []byte("top"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nested, []byte("nested"), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := NewLocalSource(LocalSourceConfig{Path: dir, Recurse: false, KeepFiles: true})
	if err != nil {
		t.Fatal(err)
	}

	data, handle, err := s.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(data) != "top" {
		t.Fatalf("got %q, want top", string(data))
	}
	if got := handle.(string); got != top {
		t.Fatalf("handle = %q, want %q", got, top)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, _, err = s.Read(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestLocalSource_RecurseTrue_ReadsNested(t *testing.T) {
	dir := t.TempDir()
	nestedDir := filepath.Join(dir, "nested")
	nested := filepath.Join(nestedDir, "nested.txt")
	if err := os.MkdirAll(nestedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nested, []byte("nested"), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := NewLocalSource(LocalSourceConfig{Path: dir, Recurse: true, KeepFiles: true})
	if err != nil {
		t.Fatal(err)
	}

	data, handle, err := s.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(data) != "nested" {
		t.Fatalf("got %q, want nested", string(data))
	}
	if got := handle.(string); got != nested {
		t.Fatalf("handle = %q, want %q", got, nested)
	}
}

func TestLocalSource_AckInvalidHandle(t *testing.T) {
	dir := t.TempDir()
	s, err := NewLocalSource(LocalSourceConfig{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Ack(context.Background(), 123); err == nil {
		t.Fatal("expected error for invalid handle type")
	}
}

func TestLocalSource_Close(t *testing.T) {
	dir := t.TempDir()
	s, err := NewLocalSource(LocalSourceConfig{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	_, _, err = s.Read(context.Background())
	if err == nil {
		t.Fatal("expected Read() to fail after Close()")
	}
}
