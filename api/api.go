package api

// Package api provides a lightweight HTTP API server for talaria.
// It is a stub in the initial prototype.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"

	"talaria/persistence"
	"talaria/utils"
)

type hodosProgressReader interface {
	ListHodosProgressSummaries(ctx context.Context) ([]persistence.HodosProgressSummary, error)
}

// Server serves the talaria HTTP API.
type Server struct {
	listenAddr string
	mux        *http.ServeMux
	progress   hodosProgressReader
	httpServer *http.Server
}

// NewServer creates an API server bound to listenAddr (host:port).
func NewServer(listenAddr string) *Server {
	return NewServerWithProgress(listenAddr, nil)
}

// NewServerWithProgress creates an API server with an optional hodos progress reader.
func NewServerWithProgress(listenAddr string, progress hodosProgressReader) *Server {
	s := &Server{
		listenAddr: listenAddr,
		mux:        http.NewServeMux(),
		progress:   progress,
	}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/api/v1/status", s.handleStatus)
	s.mux.HandleFunc("/api/v1/hodos/progress", s.handleHodosProgress)
}

// Start begins listening.  Returns an error if the listener cannot bind.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("api: listen: %w", err)
	}
	return s.Serve(ln)
}

// StartBackground binds the listener and serves requests in a goroutine.
func (s *Server) StartBackground() error {
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("api: listen: %w", err)
	}
	go func() {
		if err := s.Serve(ln); err != nil && err != http.ErrServerClosed {
			utils.Errorf("API server stopped: %v", err)
		}
	}()
	return nil
}

// Shutdown gracefully stops the API server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("api: shutdown: %w", err)
	}
	return nil
}

// Serve begins serving on an already-open listener.
func (s *Server) Serve(ln net.Listener) error {
	s.httpServer = &http.Server{
		Handler: s.mux,
	}
	utils.Infof("API server listening on %s", s.listenAddr)
	err := s.httpServer.Serve(ln)
	if err != nil {
		return fmt.Errorf("api: %w", err)
	}
	return nil
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleHodosProgress(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	if s.progress == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"summaries": []persistence.HodosProgressSummary{}})
		return
	}
	summaries, err := s.progress.ListHodosProgressSummaries(r.Context())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to load hodos progress"})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"summaries": summaries})
}
