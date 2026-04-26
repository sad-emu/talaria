package hodos

import "testing"

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
