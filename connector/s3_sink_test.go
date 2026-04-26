package connector

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type mockS3Client struct {
	headCalls int
	putCalls  int

	headErr error
	putErr  error

	lastPutBucket string
	lastPutKey    string
	lastPutBody   []byte
}

func (m *mockS3Client) HeadObject(_ context.Context, _ *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.headCalls++
	if m.headErr != nil {
		return nil, m.headErr
	}
	return &s3.HeadObjectOutput{}, nil
}

func (m *mockS3Client) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.putCalls++
	if in.Bucket != nil {
		m.lastPutBucket = *in.Bucket
	}
	if in.Key != nil {
		m.lastPutKey = *in.Key
	}
	if in.Body != nil {
		b, _ := io.ReadAll(in.Body)
		m.lastPutBody = append([]byte(nil), b...)
	}
	if m.putErr != nil {
		return nil, m.putErr
	}
	return &s3.PutObjectOutput{}, nil
}

func baseS3Cfg() S3SinkConfig {
	return S3SinkConfig{
		Bucket:          "my-bucket",
		ObjectKey:       "uploads/file.bin",
		Region:          "us-east-1",
		AccessKeyID:     "AKIA_TEST",
		SecretAccessKey: "SECRET_TEST",
	}
}

func TestValidateS3Config_RequiredFields(t *testing.T) {
	cfg := baseS3Cfg()
	if err := validateS3Config(cfg); err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}

	cases := []struct {
		name string
		mut  func(*S3SinkConfig)
	}{
		{name: "missing bucket", mut: func(c *S3SinkConfig) { c.Bucket = "" }},
		{name: "missing object key", mut: func(c *S3SinkConfig) { c.ObjectKey = "" }},
		{name: "missing region", mut: func(c *S3SinkConfig) { c.Region = "" }},
		{name: "missing access key", mut: func(c *S3SinkConfig) { c.AccessKeyID = "" }},
		{name: "missing secret", mut: func(c *S3SinkConfig) { c.SecretAccessKey = "" }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := baseS3Cfg()
			tc.mut(&c)
			if err := validateS3Config(c); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestS3Sink_Name_Default(t *testing.T) {
	s := newS3SinkWithClient(baseS3Cfg(), &mockS3Client{})
	if got := s.Name(); got != "s3" {
		t.Fatalf("Name() = %q, want s3", got)
	}
}

func TestS3Sink_Write_OverwriteDisabled_ExistingObject(t *testing.T) {
	cfg := baseS3Cfg()
	cfg.OverwriteExisting = false

	mock := &mockS3Client{}
	s := newS3SinkWithClient(cfg, mock)

	err := s.Write(context.Background(), []byte("payload"))
	if err == nil {
		t.Fatalf("expected error when object exists")
	}
	if mock.headCalls != 1 {
		t.Fatalf("headCalls = %d, want 1", mock.headCalls)
	}
	if mock.putCalls != 0 {
		t.Fatalf("putCalls = %d, want 0", mock.putCalls)
	}
}

func TestS3Sink_Write_OverwriteDisabled_NotFound_AllowsPut(t *testing.T) {
	cfg := baseS3Cfg()
	cfg.OverwriteExisting = false

	mock := &mockS3Client{headErr: &smithy.GenericAPIError{Code: "NotFound", Message: "not found"}}
	s := newS3SinkWithClient(cfg, mock)

	payload := []byte("payload")
	if err := s.Write(context.Background(), payload); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if mock.headCalls != 1 {
		t.Fatalf("headCalls = %d, want 1", mock.headCalls)
	}
	if mock.putCalls != 1 {
		t.Fatalf("putCalls = %d, want 1", mock.putCalls)
	}
	if mock.lastPutBucket != cfg.Bucket || mock.lastPutKey != cfg.ObjectKey {
		t.Fatalf("put target = %s/%s, want %s/%s", mock.lastPutBucket, mock.lastPutKey, cfg.Bucket, cfg.ObjectKey)
	}
	if !bytes.Equal(mock.lastPutBody, payload) {
		t.Fatalf("put body mismatch")
	}
}

func TestS3Sink_Write_OverwriteEnabled_SkipsHead(t *testing.T) {
	cfg := baseS3Cfg()
	cfg.OverwriteExisting = true

	mock := &mockS3Client{}
	s := newS3SinkWithClient(cfg, mock)

	if err := s.Write(context.Background(), []byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if mock.headCalls != 0 {
		t.Fatalf("headCalls = %d, want 0", mock.headCalls)
	}
	if mock.putCalls != 1 {
		t.Fatalf("putCalls = %d, want 1", mock.putCalls)
	}
}

func TestS3Sink_Write_HeadUnexpectedError(t *testing.T) {
	cfg := baseS3Cfg()
	cfg.OverwriteExisting = false

	mock := &mockS3Client{headErr: errors.New("network error")}
	s := newS3SinkWithClient(cfg, mock)

	if err := s.Write(context.Background(), []byte("payload")); err == nil {
		t.Fatalf("expected error")
	}
	if mock.putCalls != 0 {
		t.Fatalf("put should not be called on head error")
	}
}

func TestS3Sink_Write_PutError(t *testing.T) {
	cfg := baseS3Cfg()
	cfg.OverwriteExisting = true

	mock := &mockS3Client{putErr: errors.New("put failed")}
	s := newS3SinkWithClient(cfg, mock)

	if err := s.Write(context.Background(), []byte("payload")); err == nil {
		t.Fatalf("expected put error")
	}
	if mock.putCalls != 1 {
		t.Fatalf("putCalls = %d, want 1", mock.putCalls)
	}
}

func TestValidateS3Config_AllowsKeyPrefixWithoutObjectKey(t *testing.T) {
	cfg := baseS3Cfg()
	cfg.ObjectKey = ""
	cfg.KeyPrefix = "uploads"
	if err := validateS3Config(cfg); err != nil {
		t.Fatalf("expected valid config with KeyPrefix only, got: %v", err)
	}
}

func TestS3Sink_Write_RequiresObjectKey(t *testing.T) {
	cfg := baseS3Cfg()
	cfg.ObjectKey = ""
	cfg.KeyPrefix = "uploads"

	s := newS3SinkWithClient(cfg, &mockS3Client{})
	if err := s.Write(context.Background(), []byte("payload")); err == nil {
		t.Fatalf("expected Write() to fail when ObjectKey is empty")
	}
}

func TestS3Sink_WriteToKey_UsesSuppliedKey(t *testing.T) {
	cfg := baseS3Cfg()
	cfg.ObjectKey = ""
	cfg.KeyPrefix = "uploads"
	cfg.OverwriteExisting = true

	mock := &mockS3Client{}
	s := newS3SinkWithClient(cfg, mock)

	if err := s.WriteToKey(context.Background(), "uploads/a.bin", []byte("payload")); err != nil {
		t.Fatalf("WriteToKey() error = %v", err)
	}
	if mock.putCalls != 1 {
		t.Fatalf("putCalls = %d, want 1", mock.putCalls)
	}
	if mock.lastPutKey != "uploads/a.bin" {
		t.Fatalf("lastPutKey = %q, want %q", mock.lastPutKey, "uploads/a.bin")
	}
}

func TestS3Sink_Close(t *testing.T) {
	s := newS3SinkWithClient(baseS3Cfg(), &mockS3Client{})
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestIsS3NotFound(t *testing.T) {
	if !isS3NotFound(&smithy.GenericAPIError{Code: "NotFound", Message: "x"}) {
		t.Fatalf("expected true for NotFound")
	}
	if !isS3NotFound(&smithy.GenericAPIError{Code: "NoSuchKey", Message: "x"}) {
		t.Fatalf("expected true for NoSuchKey")
	}
	if isS3NotFound(errors.New("x")) {
		t.Fatalf("expected false for generic error")
	}
}
