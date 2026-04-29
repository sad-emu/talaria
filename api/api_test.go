package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"talaria/persistence"
)

type fakeProgressReader struct {
	summaries []persistence.HodosProgressSummary
	err       error
}

func (f *fakeProgressReader) ListHodosProgressSummaries(context.Context) ([]persistence.HodosProgressSummary, error) {
	if f.err != nil {
		return nil, f.err
	}
	return append([]persistence.HodosProgressSummary(nil), f.summaries...), nil
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

func TestHandleHodosProgress_WithoutReader_ReturnsEmptySummaries(t *testing.T) {
	s := NewServer("127.0.0.1:0")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/hodos/progress", nil)
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var body struct {
		Summaries []persistence.HodosProgressSummary `json:"summaries"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Summaries) != 0 {
		t.Fatalf("summaries len = %d, want 0", len(body.Summaries))
	}
}

func TestHandleHodosProgress_WithReader_ReturnsSummaries(t *testing.T) {
	reader := &fakeProgressReader{summaries: []persistence.HodosProgressSummary{{
		HodosName:  "local-to-s3",
		Total:      4,
		InProgress: 1,
		Completed:  2,
		Failed:     1,
	}}}
	s := NewServerWithProgress("127.0.0.1:0", reader)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/hodos/progress", nil)
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var body struct {
		Summaries []persistence.HodosProgressSummary `json:"summaries"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Summaries) != 1 {
		t.Fatalf("summaries len = %d, want 1", len(body.Summaries))
	}
	if body.Summaries[0].HodosName != "local-to-s3" {
		t.Fatalf("hodos name = %q, want local-to-s3", body.Summaries[0].HodosName)
	}
}
