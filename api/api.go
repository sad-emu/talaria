package api

// Package api provides a lightweight HTTP API server for talaria.
// It is a stub in the initial prototype.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"talaria/persistence"
	"talaria/utils"
)

type hodosProgressReader interface {
	ListHodosProgressSummaries(ctx context.Context) ([]persistence.HodosProgressSummary, error)
	ListHodosProgress(ctx context.Context, hodosName string, limit int, offset int) ([]persistence.HodosProgress, error)
}

type hodosOverviewRow struct {
	Name              string `json:"name"`
	TotalFiles        int64  `json:"total_files"`
	TransferredFiles  int64  `json:"transferred_files"`
	TransferringFiles int64  `json:"transferring_files"`
	FailedFiles       int64  `json:"failed_files"`
	LastUpdatedUnixNs int64  `json:"last_updated_unix_ns"`
}

type transferEndpoint struct {
	Type    string `json:"type"`
	Details string `json:"details"`
}

type hodosTransferRow struct {
	HodosName       string           `json:"hodos_name"`
	ItemKey         string           `json:"item_key"`
	SinkKey         string           `json:"sink_key"`
	Status          string           `json:"status"`
	Message         string           `json:"message"`
	DateUnixNs      int64            `json:"date_unix_ns"`
	Date            string           `json:"date"`
	UpdatedUnixNs   int64            `json:"updated_unix_ns"`
	CompletedUnixNs int64            `json:"completed_unix_ns"`
	DurationUnixNs  int64            `json:"duration_unix_ns"`
	DurationMs      int64            `json:"duration_ms"`
	SizeBytes       int64            `json:"size_bytes"`
	Source          transferEndpoint `json:"source"`
	Destination     transferEndpoint `json:"destination"`
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
	s.mux.HandleFunc("/api/v1/hodos/overview", s.handleHodosOverview)
	s.mux.HandleFunc("/api/v1/hodos/transfers", s.handleHodosTransfers)
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

func (s *Server) handleHodosOverview(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	if s.progress == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"hodos": []hodosOverviewRow{}})
		return
	}
	summaries, err := s.progress.ListHodosProgressSummaries(r.Context())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to load hodos overview"})
		return
	}
	rows := make([]hodosOverviewRow, 0, len(summaries))
	for _, summary := range summaries {
		rows = append(rows, hodosOverviewRow{
			Name:              summary.HodosName,
			TotalFiles:        summary.Total,
			TransferredFiles:  summary.Completed,
			TransferringFiles: summary.InProgress,
			FailedFiles:       summary.Failed,
			LastUpdatedUnixNs: summary.LastUpdatedUnixNs,
		})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"hodos": rows})
}

func (s *Server) handleHodosTransfers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	if s.progress == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"transfers": []hodosTransferRow{}})
		return
	}

	hodosName := strings.TrimSpace(r.URL.Query().Get("hodos"))
	if hodosName == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "query parameter 'hodos' is required"})
		return
	}

	limit := 100
	offset := 0
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "query parameter 'limit' must be a positive integer"})
			return
		}
		limit = parsed
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("offset")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "query parameter 'offset' must be a non-negative integer"})
			return
		}
		offset = parsed
	}

	rows, err := s.progress.ListHodosProgress(r.Context(), hodosName, limit, offset)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to load hodos transfers"})
		return
	}

	transfers := make([]hodosTransferRow, 0, len(rows))
	for _, row := range rows {
		transfers = append(transfers, mapTransferRow(row))
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"hodos":     hodosName,
		"limit":     limit,
		"offset":    offset,
		"transfers": transfers,
	})
}

func mapTransferRow(p persistence.HodosProgress) hodosTransferRow {
	dateUnixNs := p.StartedUnixNano
	if dateUnixNs <= 0 {
		dateUnixNs = p.UpdatedUnixNano
	}
	date := ""
	if dateUnixNs > 0 {
		date = time.Unix(0, dateUnixNs).UTC().Format(time.RFC3339Nano)
	}

	durationNs := p.DurationUnixNano
	if durationNs <= 0 && p.StartedUnixNano > 0 {
		end := p.UpdatedUnixNano
		if p.CompletedUnixNano > 0 {
			end = p.CompletedUnixNano
		}
		if end > p.StartedUnixNano {
			durationNs = end - p.StartedUnixNano
		}
	}
	if strings.EqualFold(strings.TrimSpace(p.Status), "in_progress") && p.StartedUnixNano > 0 {
		liveNs := time.Now().UnixNano() - p.StartedUnixNano
		if liveNs > durationNs {
			durationNs = liveNs
		}
	}

	return hodosTransferRow{
		HodosName:       p.HodosName,
		ItemKey:         p.ItemKey,
		SinkKey:         p.SinkKey,
		Status:          p.Status,
		Message:         p.Message,
		DateUnixNs:      dateUnixNs,
		Date:            date,
		UpdatedUnixNs:   p.UpdatedUnixNano,
		CompletedUnixNs: p.CompletedUnixNano,
		DurationUnixNs:  durationNs,
		DurationMs:      durationNs / int64(time.Millisecond),
		SizeBytes:       p.SizeBytes,
		Source: transferEndpoint{
			Type:    p.SourceType,
			Details: p.SourceDetails,
		},
		Destination: transferEndpoint{
			Type:    p.DestinationType,
			Details: p.DestinationDetail,
		},
	}
}
