package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

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
