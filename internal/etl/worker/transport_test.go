package worker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMasterClientAttachesToken(t *testing.T) {
	var gotToken, gotAuth, gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("X-API-Token")
		gotAuth = r.Header.Get("Authorization")
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	c := NewMasterClient(srv.URL, TransportConfig{
		APIToken: "worker-secret-token",
		Timeout:  time.Second,
	})
	resp, err := c.PostJSON(context.Background(), "/api/v2/workers/w1/heartbeat", nil, true)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if gotToken != "worker-secret-token" {
		t.Fatalf("X-API-Token = %q", gotToken)
	}
	if gotAuth != "Bearer worker-secret-token" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotMethod != http.MethodPost || !strings.HasSuffix(gotPath, "/heartbeat") {
		t.Fatalf("request = %s %s", gotMethod, gotPath)
	}
}

func TestMasterClientRejectsUnauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Token") != "good" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"registered"}`)
	}))
	defer srv.Close()

	bad := NewMasterClient(srv.URL, TransportConfig{APIToken: "bad", Timeout: time.Second})
	resp, err := bad.PostJSON(context.Background(), "/api/v2/workers", []byte(`{"id":"w"}`), true)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	good := NewMasterClient(srv.URL, TransportConfig{APIToken: "good", Timeout: time.Second})
	resp2, err := good.PostJSON(context.Background(), "/api/v2/workers", []byte(`{"id":"w"}`), true)
	if err != nil {
		t.Fatalf("good post: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("good status = %d", resp2.StatusCode)
	}
}

func TestMasterClientRetries5xx(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if hits < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer srv.Close()

	c := NewMasterClient(srv.URL, TransportConfig{Timeout: time.Second, MaxRetries: 3})
	resp, err := c.PostJSON(context.Background(), "/api/v2/workers/x/heartbeat", nil, true)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if hits != 3 {
		t.Fatalf("hits = %d, want 3", hits)
	}
}
