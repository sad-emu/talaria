package connector

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const localSourcePollInterval = 500 * time.Millisecond

// LocalSourceConfig configures a local disk pickup connector.
type LocalSourceConfig struct {
	Name             string
	Path             string
	Recurse          bool
	KeepFiles        bool
	FilenameContains string
	IgnoreDotFiles   bool
	PickupDelay      time.Duration
}

// LocalSource reads files from a directory on disk.
//
// Read() returns file contents and uses the absolute file path as the handle.
// Ack() deletes files by default; if KeepFiles is true, Ack is a no-op.
type LocalSource struct {
	cfg   LocalSourceConfig
	mu    sync.Mutex
	queue []string
	seen  map[string]struct{}
	open  bool
}

var _ Source = (*LocalSource)(nil)

func NewLocalSource(cfg LocalSourceConfig) (*LocalSource, error) {
	if cfg.Name == "" {
		cfg.Name = "local"
	}
	if cfg.Path == "" {
		return nil, fmt.Errorf("local source: Path is required")
	}
	abs, err := filepath.Abs(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("local source: resolve path %q: %w", cfg.Path, err)
	}
	st, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("local source: stat %q: %w", abs, err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("local source: path %q is not a directory", abs)
	}

	return &LocalSource{
		cfg: LocalSourceConfig{
			Name:             cfg.Name,
			Path:             abs,
			Recurse:          cfg.Recurse,
			KeepFiles:        cfg.KeepFiles,
			FilenameContains: strings.TrimSpace(cfg.FilenameContains),
			IgnoreDotFiles:   cfg.IgnoreDotFiles,
			PickupDelay:      cfg.PickupDelay,
		},
		seen: make(map[string]struct{}),
		open: true,
	}, nil
}

func (s *LocalSource) Name() string { return s.cfg.Name }

func (s *LocalSource) Read(ctx context.Context) ([]byte, any, error) {
	for {
		s.mu.Lock()
		if !s.open {
			s.mu.Unlock()
			return nil, nil, errors.New("local source: closed")
		}
		if len(s.queue) == 0 {
			if err := s.enqueueFilesLocked(); err != nil {
				s.mu.Unlock()
				return nil, nil, err
			}
		}
		if len(s.queue) > 0 {
			path := s.queue[0]
			s.queue = s.queue[1:]
			s.mu.Unlock()

			data, err := os.ReadFile(path)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, nil, fmt.Errorf("local source: read %q: %w", path, err)
			}
			return data, path, nil
		}
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(localSourcePollInterval):
		}
	}
}

func (s *LocalSource) Ack(_ context.Context, handle any) error {
	if s.cfg.KeepFiles {
		return nil
	}
	path, ok := handle.(string)
	if !ok || path == "" {
		return fmt.Errorf("local source: invalid handle type %T", handle)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("local source: remove %q: %w", path, err)
	}

	s.mu.Lock()
	delete(s.seen, path)
	s.mu.Unlock()
	return nil
}

func (s *LocalSource) Close() error {
	s.mu.Lock()
	s.open = false
	s.mu.Unlock()
	return nil
}

func (s *LocalSource) enqueueFilesLocked() error {
	now := time.Now()
	entries, err := os.ReadDir(s.cfg.Path)
	if err != nil {
		return fmt.Errorf("local source: read dir %q: %w", s.cfg.Path, err)
	}
	if !s.cfg.Recurse {
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			if s.cfg.IgnoreDotFiles && strings.HasPrefix(e.Name(), ".") {
				continue
			}
			if s.cfg.FilenameContains != "" && !strings.Contains(e.Name(), s.cfg.FilenameContains) {
				continue
			}
			full := filepath.Join(s.cfg.Path, e.Name())
			if _, ok := s.seen[full]; ok {
				continue
			}
			if s.cfg.PickupDelay > 0 {
				info, statErr := e.Info()
				if statErr != nil {
					return fmt.Errorf("local source: stat %q: %w", full, statErr)
				}
				if now.Sub(info.ModTime()) < s.cfg.PickupDelay {
					continue
				}
			}
			s.queue = append(s.queue, full)
			s.seen[full] = struct{}{}
		}
		return nil
	}

	for _, e := range entries {
		full := filepath.Join(s.cfg.Path, e.Name())
		if e.IsDir() {
			if s.cfg.IgnoreDotFiles && strings.HasPrefix(e.Name(), ".") {
				continue
			}
			err := filepath.WalkDir(full, func(path string, d fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if d.IsDir() {
					if s.cfg.IgnoreDotFiles && strings.HasPrefix(d.Name(), ".") {
						return filepath.SkipDir
					}
					return nil
				}
				if s.cfg.IgnoreDotFiles && strings.HasPrefix(d.Name(), ".") {
					return nil
				}
				if s.cfg.FilenameContains != "" && !strings.Contains(d.Name(), s.cfg.FilenameContains) {
					return nil
				}
				if _, ok := s.seen[path]; ok {
					return nil
				}
				if s.cfg.PickupDelay > 0 {
					info, statErr := d.Info()
					if statErr != nil {
						return statErr
					}
					if now.Sub(info.ModTime()) < s.cfg.PickupDelay {
						return nil
					}
				}
				s.queue = append(s.queue, path)
				s.seen[path] = struct{}{}
				return nil
			})
			if err != nil {
				return fmt.Errorf("local source: walk %q: %w", full, err)
			}
			continue
		}
		if _, ok := s.seen[full]; ok {
			continue
		}
		if s.cfg.IgnoreDotFiles && strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if s.cfg.FilenameContains != "" && !strings.Contains(e.Name(), s.cfg.FilenameContains) {
			continue
		}
		if s.cfg.PickupDelay > 0 {
			info, statErr := e.Info()
			if statErr != nil {
				return fmt.Errorf("local source: stat %q: %w", full, statErr)
			}
			if now.Sub(info.ModTime()) < s.cfg.PickupDelay {
				continue
			}
		}
		s.queue = append(s.queue, full)
		s.seen[full] = struct{}{}
	}
	return nil
}
