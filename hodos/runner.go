package hodos

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"talaria/config"
	"talaria/connector"
	"talaria/persistence"
	"talaria/utils"
)

const runOnceIdleTimeout = 200 * time.Millisecond
const inProgressRefreshInterval = 500 * time.Millisecond

type keyWriter interface {
	WriteToKey(ctx context.Context, key string, data []byte) error
	Close() error
}

type runner struct {
	cfg   config.HodosConfig
	store persistence.TransferStore

	source    connector.Source
	sourceDir string
	keepFiles bool

	sinkType string
	s3Sink   keyWriter
	s3Prefix string

	processedCount int64
	skippedCount   int64
	failedCount    int64
}

// RunConfigured executes configured hodos flows.
// For now, local->s3 is fully supported; talaria endpoints are reserved.
func RunConfigured(ctx context.Context, cfgs []config.HodosConfig, store persistence.TransferStore) (int, error) {
	total := 0
	for i, hc := range cfgs {
		if !hc.EnabledValue() {
			continue
		}
		r, err := newRunner(hc, store)
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

func newRunner(hc config.HodosConfig, store persistence.TransferStore) (*runner, error) {
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
		Name:                    hc.Name + "-dropoff-s3",
		Bucket:                  hc.Dropoff.S3.Bucket,
		ObjectKey:               hc.Dropoff.S3.ObjectKey,
		KeyPrefix:               hc.Dropoff.S3.KeyPrefix,
		MultipartChunkSizeBytes: int64(hc.Dropoff.S3.MultipartChunkSizeMB) * 1024 * 1024,
		Region:                  hc.Dropoff.S3.Region,
		Endpoint:                hc.Dropoff.S3.Endpoint,
		UsePathStyle:            hc.Dropoff.S3.UsePathStyle,
		OverwriteExisting:       hc.Dropoff.S3.OverwriteExisting,
		AccessKeyID:             hc.Dropoff.S3.AccessKeyID,
		SecretAccessKey:         hc.Dropoff.S3.SecretAccessKey,
		SessionToken:            hc.Dropoff.S3.SessionToken,
	}
	sink, err := connector.NewS3Sink(s3Cfg)
	if err != nil {
		_ = src.Close()
		return nil, err
	}

	return &runner{
		cfg:       hc,
		store:     store,
		source:    src,
		sourceDir: hc.Pickup.Local.Path,
		keepFiles: hc.Pickup.Local.KeepFiles,
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
		skipped, err := r.handleExistingCompletion(ctx, handle)
		if err != nil {
			return count, err
		}
		if skipped {
			r.skippedCount++
			r.logProgress("skipped", handle, "already completed")
			continue
		}
		if err := r.write(ctx, handle, data); err != nil {
			r.failedCount++
			_ = r.markFailed(ctx, handle, fmt.Sprintf("write failed: %v", err))
			r.logProgress("failed", handle, err.Error())
			return count, err
		}
		if err := r.markCompleted(ctx, handle); err != nil {
			return count, err
		}
		if err := r.source.Ack(ctx, handle); err != nil {
			r.failedCount++
			_ = r.markFailed(ctx, handle, fmt.Sprintf("ack failed: %v", err))
			r.logProgress("failed", handle, err.Error())
			return count, err
		}
		count++
		r.processedCount++
		r.logProgress("completed", handle, "")
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
		skipped, err := r.handleExistingCompletion(ctx, handle)
		if err != nil {
			return count, err
		}
		if skipped {
			r.skippedCount++
			r.logProgress("skipped", handle, "already completed")
			continue
		}
		if err := r.write(ctx, handle, data); err != nil {
			r.failedCount++
			_ = r.markFailed(ctx, handle, fmt.Sprintf("write failed: %v", err))
			r.logProgress("failed", handle, err.Error())
			return count, err
		}
		if err := r.markCompleted(ctx, handle); err != nil {
			return count, err
		}
		if err := r.source.Ack(ctx, handle); err != nil {
			r.failedCount++
			_ = r.markFailed(ctx, handle, fmt.Sprintf("ack failed: %v", err))
			r.logProgress("failed", handle, err.Error())
			return count, err
		}
		count++
		r.processedCount++
		r.logProgress("completed", handle, "")
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

	itemKey := r.itemKey(handle)
	utils.Infof("hodos file start name=%q file=%q sink_key=%q bytes=%d", r.cfg.Name, itemKey, key, len(data))
	utils.Debugf("hodos chunk start name=%q chunk=1/1 upload_id=%q item=%q bytes=%d overall_processed=%d overall_skipped=%d overall_failed=%d",
		r.cfg.Name, key, itemKey, len(data), r.processedCount, r.skippedCount, r.failedCount)

	if err := r.markInProgress(ctx, handle, key, int64(len(data))); err != nil {
		return err
	}

	refreshCtx, stopRefresh := context.WithCancel(ctx)
	defer stopRefresh()
	refreshDone := make(chan struct{})
	go func() {
		defer close(refreshDone)
		r.refreshInProgressLoop(refreshCtx, handle, key)
	}()

	if err := r.s3Sink.WriteToKey(ctx, key, data); err != nil {
		stopRefresh()
		<-refreshDone
		return err
	}
	stopRefresh()
	<-refreshDone

	utils.Debugf("hodos chunk finish name=%q chunk=1/1 upload_id=%q item=%q bytes=%d overall_processed=%d overall_skipped=%d overall_failed=%d",
		r.cfg.Name, key, itemKey, len(data), r.processedCount, r.skippedCount, r.failedCount)
	utils.Infof("hodos file finish name=%q file=%q sink_key=%q bytes=%d", r.cfg.Name, itemKey, key, len(data))
	return nil
}

func (r *runner) refreshInProgressLoop(ctx context.Context, handle any, sinkKey string) {
	if r.store == nil {
		return
	}
	ticker := time.NewTicker(inProgressRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.touchInProgress(ctx, handle, sinkKey); err != nil {
				utils.Errorf("hodos progress refresh failed name=%q item=%q: %v", r.cfg.Name, r.itemKey(handle), err)
			}
		}
	}
}

func (r *runner) touchInProgress(ctx context.Context, handle any, sinkKey string) error {
	if r.store == nil {
		return nil
	}
	itemKey := r.itemKey(handle)
	if itemKey == "" {
		return nil
	}
	p, err := r.store.GetHodosProgress(ctx, r.cfg.Name, itemKey)
	if err != nil {
		return err
	}
	if p == nil || !strings.EqualFold(p.Status, "in_progress") {
		return nil
	}
	now := time.Now().UnixNano()
	started := p.StartedUnixNano
	if started <= 0 {
		started = now
	}
	p.SinkKey = sinkKey
	p.StartedUnixNano = started
	p.UpdatedUnixNano = now
	p.DurationUnixNano = now - started
	if p.DurationUnixNano < 0 {
		p.DurationUnixNano = 0
	}
	return r.store.UpsertHodosProgress(ctx, *p)
}

func (r *runner) handleExistingCompletion(ctx context.Context, handle any) (bool, error) {
	if r.store == nil {
		return false, nil
	}
	itemKey := r.itemKey(handle)
	if itemKey == "" {
		return false, nil
	}
	p, err := r.store.GetHodosProgress(ctx, r.cfg.Name, itemKey)
	if err != nil {
		return false, err
	}
	if p == nil || p.Status != "completed" {
		return false, nil
	}
	if !r.keepFiles {
		if err := r.source.Ack(ctx, handle); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (r *runner) markInProgress(ctx context.Context, handle any, sinkKey string, sizeBytes int64) error {
	if r.store == nil {
		return nil
	}
	itemKey := r.itemKey(handle)
	if itemKey == "" {
		return nil
	}
	now := time.Now().UnixNano()
	sourceType, sourceDetails := r.transferSource(itemKey)
	destinationType, destinationDetails := r.transferDestination(sinkKey)
	return r.store.UpsertHodosProgress(ctx, persistence.HodosProgress{
		HodosName:         r.cfg.Name,
		ItemKey:           itemKey,
		SinkKey:           sinkKey,
		Status:            "in_progress",
		StartedUnixNano:   now,
		UpdatedUnixNano:   now,
		SizeBytes:         sizeBytes,
		SourceType:        sourceType,
		SourceDetails:     sourceDetails,
		DestinationType:   destinationType,
		DestinationDetail: destinationDetails,
	})
}

func (r *runner) markCompleted(ctx context.Context, handle any) error {
	if r.store == nil {
		return nil
	}
	itemKey := r.itemKey(handle)
	if itemKey == "" {
		return nil
	}
	sinkKey := strings.TrimSpace(r.cfg.Dropoff.S3.ObjectKey)
	if sinkKey == "" {
		sinkKey = buildS3Key(r.sourceDir, itemKey, r.s3Prefix)
	}
	now := time.Now().UnixNano()
	prev, err := r.store.GetHodosProgress(ctx, r.cfg.Name, itemKey)
	if err != nil {
		return err
	}
	started := now
	sizeBytes := int64(0)
	sourceType := "local"
	sourceDetails := itemKey
	destinationType := "s3"
	destinationDetails := fmt.Sprintf("bucket=%s key=%s", r.cfg.Dropoff.S3.Bucket, sinkKey)
	if prev != nil {
		if prev.StartedUnixNano > 0 {
			started = prev.StartedUnixNano
		}
		sizeBytes = prev.SizeBytes
		if strings.TrimSpace(prev.SourceType) != "" {
			sourceType = prev.SourceType
		}
		if strings.TrimSpace(prev.SourceDetails) != "" {
			sourceDetails = prev.SourceDetails
		}
		if strings.TrimSpace(prev.DestinationType) != "" {
			destinationType = prev.DestinationType
		}
		if strings.TrimSpace(prev.DestinationDetail) != "" {
			destinationDetails = prev.DestinationDetail
		}
	}
	return r.store.UpsertHodosProgress(ctx, persistence.HodosProgress{
		HodosName:         r.cfg.Name,
		ItemKey:           itemKey,
		SinkKey:           sinkKey,
		Status:            "completed",
		Message:           "",
		StartedUnixNano:   started,
		UpdatedUnixNano:   now,
		CompletedUnixNano: now,
		DurationUnixNano:  now - started,
		SizeBytes:         sizeBytes,
		SourceType:        sourceType,
		SourceDetails:     sourceDetails,
		DestinationType:   destinationType,
		DestinationDetail: destinationDetails,
	})
}

func (r *runner) markFailed(ctx context.Context, handle any, message string) error {
	if r.store == nil {
		return nil
	}
	itemKey := r.itemKey(handle)
	if itemKey == "" {
		return nil
	}
	sinkKey := strings.TrimSpace(r.cfg.Dropoff.S3.ObjectKey)
	if sinkKey == "" {
		sinkKey = buildS3Key(r.sourceDir, itemKey, r.s3Prefix)
	}
	now := time.Now().UnixNano()
	prev, err := r.store.GetHodosProgress(ctx, r.cfg.Name, itemKey)
	if err != nil {
		return err
	}
	started := now
	sizeBytes := int64(0)
	sourceType := "local"
	sourceDetails := itemKey
	destinationType := "s3"
	destinationDetails := fmt.Sprintf("bucket=%s key=%s", r.cfg.Dropoff.S3.Bucket, sinkKey)
	if prev != nil {
		if prev.StartedUnixNano > 0 {
			started = prev.StartedUnixNano
		}
		sizeBytes = prev.SizeBytes
		if strings.TrimSpace(prev.SourceType) != "" {
			sourceType = prev.SourceType
		}
		if strings.TrimSpace(prev.SourceDetails) != "" {
			sourceDetails = prev.SourceDetails
		}
		if strings.TrimSpace(prev.DestinationType) != "" {
			destinationType = prev.DestinationType
		}
		if strings.TrimSpace(prev.DestinationDetail) != "" {
			destinationDetails = prev.DestinationDetail
		}
	}
	return r.store.UpsertHodosProgress(ctx, persistence.HodosProgress{
		HodosName:         r.cfg.Name,
		ItemKey:           itemKey,
		SinkKey:           sinkKey,
		Status:            "failed",
		Message:           strings.TrimSpace(message),
		StartedUnixNano:   started,
		UpdatedUnixNano:   now,
		DurationUnixNano:  now - started,
		SizeBytes:         sizeBytes,
		SourceType:        sourceType,
		SourceDetails:     sourceDetails,
		DestinationType:   destinationType,
		DestinationDetail: destinationDetails,
	})
}

func (r *runner) transferSource(itemKey string) (string, string) {
	base := strings.TrimSpace(r.sourceDir)
	if base == "" {
		return "local", fmt.Sprintf("path=%s", itemKey)
	}
	return "local", fmt.Sprintf("base=%s path=%s", base, itemKey)
}

func (r *runner) transferDestination(sinkKey string) (string, string) {
	bucket := strings.TrimSpace(r.cfg.Dropoff.S3.Bucket)
	key := strings.TrimSpace(sinkKey)
	region := strings.TrimSpace(r.cfg.Dropoff.S3.Region)
	if region == "" {
		return "s3", fmt.Sprintf("bucket=%s key=%s", bucket, key)
	}
	return "s3", fmt.Sprintf("bucket=%s key=%s region=%s", bucket, key, region)
}

func (r *runner) logProgress(status string, handle any, message string) {
	itemKey := r.itemKey(handle)
	if itemKey == "" {
		itemKey = "unknown"
	}
	if strings.TrimSpace(message) == "" {
		utils.Infof("hodos progress name=%q status=%s item=%q processed=%d skipped=%d failed=%d", r.cfg.Name, status, itemKey, r.processedCount, r.skippedCount, r.failedCount)
		return
	}
	if strings.EqualFold(status, "failed") {
		utils.Errorf("hodos progress name=%q status=%s item=%q message=%q processed=%d skipped=%d failed=%d", r.cfg.Name, status, itemKey, message, r.processedCount, r.skippedCount, r.failedCount)
		return
	}
	utils.Infof("hodos progress name=%q status=%s item=%q message=%q processed=%d skipped=%d failed=%d", r.cfg.Name, status, itemKey, message, r.processedCount, r.skippedCount, r.failedCount)
}

func (r *runner) itemKey(handle any) string {
	path, ok := handle.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(path)
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
