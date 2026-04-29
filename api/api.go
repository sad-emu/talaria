package api

// Package api provides a lightweight HTTP API server for talaria.
// It is a stub in the initial prototype.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"talaria/persistence"
)

type hodosProgressReader interface {
	ListHodosProgressSummaries(ctx context.Context) ([]persistence.HodosProgressSummary, error)
}

// Server serves the talaria HTTP API.
type Server struct {
	listenAddr string
	mux        *http.ServeMux
	progress   hodosProgressReader
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
	log.Printf("API server listening on %s", s.listenAddr)
	srv := &http.Server{
		Addr:    s.listenAddr,
		Handler: s.mux,
	}
	return fmt.Errorf("api: %w", srv.ListenAndServe())
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
