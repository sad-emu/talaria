package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

const (
	defaultMultipartChunkSizeBytes = int64(50 * 1024 * 1024)
	minimumMultipartChunkSizeBytes = int64(5 * 1024 * 1024)
)

// S3SinkConfig defines how to connect and write to an S3 bucket.
type S3SinkConfig struct {
	Name string

	Bucket string
	// ObjectKey is the destination object path in S3.
	ObjectKey string
	// KeyPrefix is used when object keys are generated per item by a caller.
	KeyPrefix string
	// MultipartChunkSizeBytes controls S3 multipart part size. Defaults to 50MB.
	MultipartChunkSizeBytes int64

	Region            string
	Endpoint          string
	UsePathStyle      bool
	OverwriteExisting bool

	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

type s3Client interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type uploadFunc func(ctx context.Context, bucket string, key string, data []byte) error

// S3Sink is a dropoff connector that writes payloads to S3.
type S3Sink struct {
	cfg      S3SinkConfig
	client   s3Client
	uploadTo uploadFunc
}

var _ Sink = (*S3Sink)(nil)

// NewS3Sink creates an S3 sink using static credentials from cfg.
func NewS3Sink(cfg S3SinkConfig) (*S3Sink, error) {
	if err := validateS3Config(cfg); err != nil {
		return nil, err
	}

	provider := credentials.NewStaticCredentialsProvider(
		cfg.AccessKeyID,
		cfg.SecretAccessKey,
		cfg.SessionToken,
	)

	awsCfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(provider),
	)
	if err != nil {
		return nil, fmt.Errorf("s3 sink: load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.UsePathStyle
		if strings.TrimSpace(cfg.Endpoint) != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	partSize := cfg.MultipartChunkSizeBytes
	if partSize <= 0 {
		partSize = defaultMultipartChunkSizeBytes
	}
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = partSize
	})

	return newS3SinkWithClientAndUploader(cfg, client, func(ctx context.Context, bucket string, key string, data []byte) error {
		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		return err
	}), nil
}

func newS3SinkWithClient(cfg S3SinkConfig, client s3Client) *S3Sink {
	return newS3SinkWithClientAndUploader(cfg, client, func(ctx context.Context, bucket string, key string, data []byte) error {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(data),
			ContentLength: aws.Int64(int64(len(data))),
		})
		return err
	})
}

func newS3SinkWithClientAndUploader(cfg S3SinkConfig, client s3Client, upload uploadFunc) *S3Sink {
	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		name = "s3"
	}
	cfg.Name = name
	if cfg.MultipartChunkSizeBytes <= 0 {
		cfg.MultipartChunkSizeBytes = defaultMultipartChunkSizeBytes
	}
	return &S3Sink{cfg: cfg, client: client, uploadTo: upload}
}

func (s *S3Sink) Name() string {
	return s.cfg.Name
}

func (s *S3Sink) Write(ctx context.Context, data []byte) error {
	if strings.TrimSpace(s.cfg.ObjectKey) == "" {
		return fmt.Errorf("s3 sink: ObjectKey is required for Write(); use WriteToKey() for dynamic keys")
	}
	return s.WriteToKey(ctx, s.cfg.ObjectKey, data)
}

// WriteToKey writes payload data to a caller-supplied object key.
func (s *S3Sink) WriteToKey(ctx context.Context, key string, data []byte) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("s3 sink: key is required")
	}

	if !s.cfg.OverwriteExisting {
		_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(s.cfg.Bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			return fmt.Errorf("s3 sink: object already exists at s3://%s/%s", s.cfg.Bucket, key)
		}
		if !isS3NotFound(err) {
			return fmt.Errorf("s3 sink: head object s3://%s/%s: %w", s.cfg.Bucket, key, err)
		}
	}

	if err := s.uploadTo(ctx, s.cfg.Bucket, key, data); err != nil {
		return fmt.Errorf("s3 sink: upload object s3://%s/%s: %w", s.cfg.Bucket, key, err)
	}

	return nil
}

func (s *S3Sink) Close() error {
	return nil
}

func validateS3Config(cfg S3SinkConfig) error {
	if strings.TrimSpace(cfg.Bucket) == "" {
		return fmt.Errorf("s3 sink: Bucket is required")
	}
	if strings.TrimSpace(cfg.ObjectKey) == "" && strings.TrimSpace(cfg.KeyPrefix) == "" {
		return fmt.Errorf("s3 sink: ObjectKey or KeyPrefix is required")
	}
	if strings.TrimSpace(cfg.Region) == "" {
		return fmt.Errorf("s3 sink: Region is required")
	}
	if strings.TrimSpace(cfg.AccessKeyID) == "" {
		return fmt.Errorf("s3 sink: AccessKeyID is required")
	}
	if strings.TrimSpace(cfg.SecretAccessKey) == "" {
		return fmt.Errorf("s3 sink: SecretAccessKey is required")
	}
	if cfg.MultipartChunkSizeBytes <= 0 {
		cfg.MultipartChunkSizeBytes = defaultMultipartChunkSizeBytes
	}
	if cfg.MultipartChunkSizeBytes < minimumMultipartChunkSizeBytes {
		return fmt.Errorf("s3 sink: MultipartChunkSizeBytes must be at least %d", minimumMultipartChunkSizeBytes)
	}
	return nil
}

func isS3NotFound(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := strings.ToLower(strings.TrimSpace(apiErr.ErrorCode()))
		if code == "notfound" || code == "nosuchkey" || code == "404" {
			return true
		}
	}
	return false
}
