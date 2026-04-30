package api

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"talaria/persistence"
)

type fakeProgressReader struct {
	summaries []persistence.HodosProgressSummary
	rows      []persistence.HodosProgress
	err       error
}

func (f *fakeProgressReader) ListHodosProgressSummaries(context.Context) ([]persistence.HodosProgressSummary, error) {
	if f.err != nil {
		return nil, f.err
	}
	return append([]persistence.HodosProgressSummary(nil), f.summaries...), nil
}

func (f *fakeProgressReader) ListHodosProgress(_ context.Context, hodosName string, limit int, offset int) ([]persistence.HodosProgress, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make([]persistence.HodosProgress, 0, len(f.rows))
	for _, row := range f.rows {
		if row.HodosName == hodosName {
			out = append(out, row)
		}
	}
	if offset >= len(out) {
		return []persistence.HodosProgress{}, nil
	}
	if limit <= 0 {
		limit = 100
	}
	end := offset + limit
	if end > len(out) {
		end = len(out)
	}
	return out[offset:end], nil
}

func TestHandleStatus_OK(t *testing.T) {
	s := NewServer("127.0.0.1:0")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	w := httptest.NewRecorder()

	s.handleStatus(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("body[status] = %q, want ok", body["status"])
	}
}

func TestNewServer_RegistersStatusRoute(t *testing.T) {
	s := NewServer("127.0.0.1:0")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status route = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestNewServer_UnknownRoute_Returns404(t *testing.T) {
	s := NewServer("127.0.0.1:0")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/unknown", nil)
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("unknown route = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestHandleHodosOverview_WithoutReader_ReturnsEmptyList(t *testing.T) {
	s := NewServer("127.0.0.1:0")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/hodos/overview", nil)
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var body struct {
		Hodos []map[string]any `json:"hodos"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Hodos) != 0 {
		t.Fatalf("hodos len = %d, want 0", len(body.Hodos))
	}
}

func TestHandleHodosOverview_WithReader_ReturnsSummaries(t *testing.T) {
	reader := &fakeProgressReader{summaries: []persistence.HodosProgressSummary{{
		HodosName:  "local-to-s3",
		Total:      4,
		InProgress: 1,
		Completed:  2,
		Failed:     1,
	}}}
	s := NewServerWithProgress("127.0.0.1:0", reader)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/hodos/overview", nil)
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var body struct {
		Hodos []struct {
			Name              string `json:"name"`
			TransferredFiles  int64  `json:"transferred_files"`
			TransferringFiles int64  `json:"transferring_files"`
			FailedFiles       int64  `json:"failed_files"`
		} `json:"hodos"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Hodos) != 1 {
		t.Fatalf("hodos len = %d, want 1", len(body.Hodos))
	}
	if body.Hodos[0].Name != "local-to-s3" {
		t.Fatalf("hodos name = %q, want local-to-s3", body.Hodos[0].Name)
	}
	if body.Hodos[0].TransferredFiles != 2 || body.Hodos[0].TransferringFiles != 1 || body.Hodos[0].FailedFiles != 1 {
		t.Fatalf("unexpected counts: %+v", body.Hodos[0])
	}
}

func TestHandleHodosTransfers_RequiresHodosQuery(t *testing.T) {
	s := NewServerWithProgress("127.0.0.1:0", &fakeProgressReader{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/hodos/transfers", nil)
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleHodosTransfers_ReturnsDetailedRows(t *testing.T) {
	reader := &fakeProgressReader{rows: []persistence.HodosProgress{{
		HodosName:         "local-to-s3",
		ItemKey:           "/tmp/in/a.mp4",
		SinkKey:           "uploads/a.mp4",
		Status:            "completed",
		Message:           "",
		StartedUnixNano:   1000,
		UpdatedUnixNano:   4000,
		CompletedUnixNano: 5000,
		DurationUnixNano:  4000,
		SizeBytes:         12345,
		SourceType:        "local",
		SourceDetails:     "base=/tmp/in path=/tmp/in/a.mp4",
		DestinationType:   "s3",
		DestinationDetail: "bucket=my-bucket key=uploads/a.mp4 region=eu-west-2",
	}}}
	s := NewServerWithProgress("127.0.0.1:0", reader)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/hodos/transfers?hodos=local-to-s3&limit=10&offset=0", nil)
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var body struct {
		Hodos     string `json:"hodos"`
		Transfers []struct {
			HodosName  string `json:"hodos_name"`
			SizeBytes  int64  `json:"size_bytes"`
			DurationNs int64  `json:"duration_unix_ns"`
			Source     struct {
				Type string `json:"type"`
			} `json:"source"`
			Destination struct {
				Type string `json:"type"`
			} `json:"destination"`
		} `json:"transfers"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Hodos != "local-to-s3" {
		t.Fatalf("hodos = %q, want local-to-s3", body.Hodos)
	}
	if len(body.Transfers) != 1 {
		t.Fatalf("transfers len = %d, want 1", len(body.Transfers))
	}
	if body.Transfers[0].HodosName != "local-to-s3" {
		t.Fatalf("transfer hodos_name = %q, want local-to-s3", body.Transfers[0].HodosName)
	}
	if body.Transfers[0].SizeBytes != 12345 || body.Transfers[0].DurationNs != 4000 {
		t.Fatalf("unexpected transfer metrics: %+v", body.Transfers[0])
	}
	if body.Transfers[0].Source.Type != "local" || body.Transfers[0].Destination.Type != "s3" {
		t.Fatalf("unexpected source/destination: %+v", body.Transfers[0])
	}
}

func TestMapTransferRow_InProgressDurationIsLive(t *testing.T) {
	start := time.Now().Add(-2 * time.Second).UnixNano()
	row := mapTransferRow(persistence.HodosProgress{
		HodosName:       "local-to-s3",
		ItemKey:         "/tmp/in/a.mp4",
		Status:          "in_progress",
		StartedUnixNano: start,
		UpdatedUnixNano: start,
	})
	if row.DurationUnixNs < int64(time.Second) {
		t.Fatalf("DurationUnixNs = %d, expected live value >= 1s", row.DurationUnixNs)
	}
}

func TestServer_StartBackground_ServesAndShutsDown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	s := NewServer(addr)
	if err := s.StartBackground(); err != nil {
		t.Fatalf("StartBackground() error = %v", err)
	}
	t.Cleanup(func() {
		_ = s.Shutdown(context.Background())
	})

	resp, err := http.Get("http://" + addr + "/api/v1/status")
	if err != nil {
		t.Fatalf("GET /api/v1/status error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	resp, err = http.Get("http://" + addr + "/api/v1/status")
	if err == nil {
		resp.Body.Close()
		t.Fatalf("expected request after shutdown to fail")
	}
}
