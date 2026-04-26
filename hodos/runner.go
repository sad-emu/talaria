package hodos

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"talaria/config"
	"talaria/connector"
)

const runOnceIdleTimeout = 200 * time.Millisecond

type keyWriter interface {
	WriteToKey(ctx context.Context, key string, data []byte) error
	Close() error
}

type runner struct {
	cfg config.HodosConfig

	source    connector.Source
	sourceDir string

	sinkType string
	s3Sink   keyWriter
	s3Prefix string
}

// RunConfigured executes configured hodos flows.
// For now, local->s3 is fully supported; talaria endpoints are reserved.
func RunConfigured(ctx context.Context, cfgs []config.HodosConfig) (int, error) {
	total := 0
	for i, hc := range cfgs {
		if !hc.EnabledValue() {
			continue
		}
		r, err := newRunner(hc)
		if err != nil {
			return total, fmt.Errorf("hodos[%d] %q: %w", i, hc.Name, err)
		}

		var n int
		if hc.RunOnceValue() {
			n, err = r.runOnce(ctx)
		} else {
			n, err = r.runContinuous(ctx)
		}
		_ = r.close()
		if err != nil {
			return total, fmt.Errorf("hodos[%d] %q: %w", i, hc.Name, err)
		}
		total += n
	}
	return total, nil
}

func newRunner(hc config.HodosConfig) (*runner, error) {
	pickupType := strings.ToLower(strings.TrimSpace(hc.Pickup.Type))
	dropoffType := strings.ToLower(strings.TrimSpace(hc.Dropoff.Type))

	if pickupType != "local" || dropoffType != "s3" {
		return nil, fmt.Errorf("currently only local pickup -> s3 dropoff is supported (got %s -> %s)", pickupType, dropoffType)
	}
	if hc.Pickup.Local == nil || hc.Dropoff.S3 == nil {
		return nil, fmt.Errorf("local pickup and s3 dropoff config blocks are required")
	}

	src, err := connector.NewLocalSource(connector.LocalSourceConfig{
		Name:      hc.Name + "-pickup-local",
		Path:      hc.Pickup.Local.Path,
		Recurse:   hc.Pickup.Local.Recurse,
		KeepFiles: hc.Pickup.Local.KeepFiles,
	})
	if err != nil {
		return nil, err
	}

	s3Cfg := connector.S3SinkConfig{
		Name:              hc.Name + "-dropoff-s3",
		Bucket:            hc.Dropoff.S3.Bucket,
		ObjectKey:         hc.Dropoff.S3.ObjectKey,
		KeyPrefix:         hc.Dropoff.S3.KeyPrefix,
		Region:            hc.Dropoff.S3.Region,
		Endpoint:          hc.Dropoff.S3.Endpoint,
		UsePathStyle:      hc.Dropoff.S3.UsePathStyle,
		OverwriteExisting: hc.Dropoff.S3.OverwriteExisting,
		AccessKeyID:       hc.Dropoff.S3.AccessKeyID,
		SecretAccessKey:   hc.Dropoff.S3.SecretAccessKey,
		SessionToken:      hc.Dropoff.S3.SessionToken,
	}
	sink, err := connector.NewS3Sink(s3Cfg)
	if err != nil {
		_ = src.Close()
		return nil, err
	}

	return &runner{
		cfg:       hc,
		source:    src,
		sourceDir: hc.Pickup.Local.Path,
		sinkType:  dropoffType,
		s3Sink:    sink,
		s3Prefix:  hc.Dropoff.S3.KeyPrefix,
	}, nil
}

func (r *runner) runOnce(ctx context.Context) (int, error) {
	count := 0
	for {
		readCtx, cancel := context.WithTimeout(ctx, runOnceIdleTimeout)
		data, handle, err := r.source.Read(readCtx)
		cancel()
		if err != nil {
			if err == context.DeadlineExceeded {
				return count, nil
			}
			if err == context.Canceled {
				return count, ctx.Err()
			}
			return count, err
		}
		if err := r.write(ctx, handle, data); err != nil {
			return count, err
		}
		if err := r.source.Ack(ctx, handle); err != nil {
			return count, err
		}
		count++
	}
}

func (r *runner) runContinuous(ctx context.Context) (int, error) {
	count := 0
	for {
		data, handle, err := r.source.Read(ctx)
		if err != nil {
			if err == context.Canceled {
				return count, ctx.Err()
			}
			return count, err
		}
		if err := r.write(ctx, handle, data); err != nil {
			return count, err
		}
		if err := r.source.Ack(ctx, handle); err != nil {
			return count, err
		}
		count++
	}
}

func (r *runner) write(ctx context.Context, handle any, data []byte) error {
	if r.sinkType != "s3" {
		return fmt.Errorf("unsupported sink type %q", r.sinkType)
	}

	path, _ := handle.(string)
	key := strings.TrimSpace(r.cfg.Dropoff.S3.ObjectKey)
	if key == "" {
		key = buildS3Key(r.sourceDir, path, r.s3Prefix)
	}
	return r.s3Sink.WriteToKey(ctx, key, data)
}

func (r *runner) close() error {
	var firstErr error
	if r.source != nil {
		if err := r.source.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if r.s3Sink != nil {
		if err := r.s3Sink.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func buildS3Key(sourceDir, absPath, prefix string) string {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	sourceDir = strings.TrimSpace(sourceDir)
	absPath = strings.TrimSpace(absPath)

	rel := ""
	if sourceDir != "" && absPath != "" {
		if r, err := filepath.Rel(sourceDir, absPath); err == nil {
			rel = r
		}
	}
	if rel == "" || rel == "." || strings.HasPrefix(rel, "..") {
		rel = filepath.Base(absPath)
	}
	rel = filepath.ToSlash(rel)
	rel = strings.TrimLeft(rel, "/")

	if prefix == "" {
		return rel
	}
	if rel == "" {
		return prefix
	}
	return prefix + "/" + rel
}
