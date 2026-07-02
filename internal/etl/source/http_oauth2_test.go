package source

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestHTTPOAuth2ClientCredentials(t *testing.T) {
	var tokenHits, dataHits int32
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&tokenHits, 1)
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Errorf("token content-type = %q", r.Header.Get("Content-Type"))
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		if r.PostForm.Get("grant_type") != "client_credentials" {
			t.Errorf("grant_type = %q", r.PostForm.Get("grant_type"))
		}
		if r.PostForm.Get("client_id") != "myclient" {
			t.Errorf("client_id = %q", r.PostForm.Get("client_id"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"abc123","expires_in":3600,"token_type":"Bearer"}`))
	})
	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dataHits, 1)
		if got := r.Header.Get("Authorization"); got != "Bearer abc123" {
			t.Errorf("Authorization = %q, want Bearer abc123", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"k": "v"}}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	src, err := NewHTTPSource(map[string]any{
		"url":                  srv.URL + "/data",
		"auth_type":            "oauth2_client_credentials",
		"oauth2_token_url":     srv.URL + "/token",
		"oauth2_client_id":     "myclient",
		"oauth2_client_secret": "mysecret",
	})
	if err != nil {
		t.Fatalf("NewHTTPSource: %v", err)
	}
	reader, err := src.Open(context.Background(), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	recs, err := reader.ReadBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	if tokenHits != 1 {
		t.Fatalf("token hits = %d, want 1", tokenHits)
	}
	if dataHits != 1 {
		t.Fatalf("data hits = %d, want 1", dataHits)
	}
}

func TestHTTPOAuth2TokenCachedAcrossPages(t *testing.T) {
	var tokenHits, dataHits int32
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&tokenHits, 1)
		_, _ = w.Write([]byte(`{"access_token":"abc123","expires_in":3600}`))
	})
	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&dataHits, 1)
		// Return one item on page 1, empty on page 2 (terminates).
		if n == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"k": "v"}}})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	src, err := NewHTTPSource(map[string]any{
		"url":               srv.URL + "/data",
		"auth_type":         "oauth2_client_credentials",
		"oauth2_token_url":  srv.URL + "/token",
		"oauth2_client_id":  "c",
		"oauth2_client_secret": "s",
		"pagination":        "page",
		"page_param":        "page",
		"size_param":        "size",
		"page_size":         10,
	})
	if err != nil {
		t.Fatalf("NewHTTPSource: %v", err)
	}
	reader, err := src.Open(context.Background(), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Read enough to trigger two page fetches.
	for i := 0; i < 5; i++ {
		_, err := reader.Read(context.Background())
		if err != nil {
			break
		}
	}
	if tokenHits != 1 {
		t.Fatalf("token hits = %d, want 1 (cached)", tokenHits)
	}
}

func TestHTTPOAuth2ValidationRequiresFields(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]any
	}{
		{"missing token_url", map[string]any{"url": "http://x", "auth_type": "oauth2_client_credentials", "oauth2_client_id": "c", "oauth2_client_secret": "s"}},
		{"missing client_id", map[string]any{"url": "http://x", "auth_type": "oauth2_client_credentials", "oauth2_token_url": "http://t", "oauth2_client_secret": "s"}},
		{"missing client_secret", map[string]any{"url": "http://x", "auth_type": "oauth2_client_credentials", "oauth2_token_url": "http://t", "oauth2_client_id": "c"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := NewHTTPSource(c.config); err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
		})
	}
}

func TestHTTPOAuth2TokenFieldOverride(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		// Use a non-standard token field name
		_, _ = w.Write([]byte(`{"tok":"xyz","expires_in":3600}`))
	})
	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Token xyz" {
			t.Errorf("Authorization = %q, want 'Token xyz'", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"k": "v"}}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	src, err := NewHTTPSource(map[string]any{
		"url":                  srv.URL + "/data",
		"auth_type":            "oauth2_client_credentials",
		"oauth2_token_url":     srv.URL + "/token",
		"oauth2_client_id":     "c",
		"oauth2_client_secret": "s",
		"oauth2_token_field":   "tok",
		"oauth2_header_format": "Token %s",
	})
	if err != nil {
		t.Fatalf("NewHTTPSource: %v", err)
	}
	reader, err := src.Open(context.Background(), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := reader.ReadBatch(context.Background(), 10); err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
}
