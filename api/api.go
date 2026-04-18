package api

// Package api provides a lightweight HTTP API server for talaria.
// It is a stub in the initial prototype.

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

// Server serves the talaria HTTP API.
type Server struct {
	listenAddr string
	mux        *http.ServeMux
}

// NewServer creates an API server bound to listenAddr (host:port).
func NewServer(listenAddr string) *Server {
	s := &Server{
		listenAddr: listenAddr,
		mux:        http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/api/v1/status", s.handleStatus)
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
