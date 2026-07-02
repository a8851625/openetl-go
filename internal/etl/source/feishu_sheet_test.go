package source

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFeishuSheetConfigValidation(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]any
	}{
		{"missing app_id", map[string]any{"app_secret": "s", "spreadsheet_token": "t", "sheet_range": "A1:B2"}},
		{"missing app_secret", map[string]any{"app_id": "i", "spreadsheet_token": "t", "sheet_range": "A1:B2"}},
		{"missing spreadsheet_token", map[string]any{"app_id": "i", "app_secret": "s", "sheet_range": "A1:B2"}},
		{"missing range and sheet_id", map[string]any{"app_id": "i", "app_secret": "s", "spreadsheet_token": "t"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := NewFeishuSheetSource(c.config); err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
		})
	}
}

func TestFeishuSheetTokenAndFetch(t *testing.T) {
	var tokenHits, sheetHits int
	mux := http.NewServeMux()
	mux.HandleFunc("/open-apis/auth/v3/tenant_access_token/internal", func(w http.ResponseWriter, r *http.Request) {
		tokenHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"msg":"ok","tenant_access_token":"t-abc","expire":7200}`))
	})
	mux.HandleFunc("/open-apis/sheets/v3/spreadsheets/", func(w http.ResponseWriter, r *http.Request) {
		sheetHits++
		if got := r.Header.Get("Authorization"); got != "Bearer t-abc" {
			t.Errorf("auth header = %q, want Bearer t-abc", got)
		}
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"code": 0,
			"msg":  "ok",
			"data": map[string]any{
				"value_range": map[string]any{
					"values": [][]any{
						{"id", "name"},
						{float64(1), "Alice"},
						{float64(2), "Bob"},
					},
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	src, err := NewFeishuSheetSource(map[string]any{
		"app_id":            "x",
		"app_secret":        "y",
		"spreadsheet_token": "tok",
		"sheet_range":       "Sheet1!A1:B3",
		"base_url":          srv.URL,
	})
	if err != nil {
		t.Fatalf("NewFeishuSheetSource: %v", err)
	}
	reader, err := src.Open(context.Background(), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	recs, err := reader.ReadBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if recs[0].Data["id"] != float64(1) || recs[0].Data["name"] != "Alice" {
		t.Fatalf("rec0 = %#v", recs[0].Data)
	}
	if tokenHits != 1 {
		t.Fatalf("token hits = %d, want 1", tokenHits)
	}
	if sheetHits != 1 {
		t.Fatalf("sheet hits = %d, want 1", sheetHits)
	}
}

func TestFeishuSheetTokenCached(t *testing.T) {
	var tokenHits int
	mux := http.NewServeMux()
	mux.HandleFunc("/open-apis/auth/v3/tenant_access_token/internal", func(w http.ResponseWriter, r *http.Request) {
		tokenHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"msg":"ok","tenant_access_token":"t-abc","expire":7200}`))
	})
	sheetHits := 0
	mux.HandleFunc("/open-apis/sheets/v3/spreadsheets/", func(w http.ResponseWriter, r *http.Request) {
		sheetHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"code":0,"msg":"ok","data":{"value_range":{"values":[["a"],["b"]]}}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	src, err := NewFeishuSheetSource(map[string]any{
		"app_id":            "x",
		"app_secret":        "y",
		"spreadsheet_token": "tok",
		"sheet_range":       "A1:B2",
		"base_url":          srv.URL,
	})
	if err != nil {
		t.Fatalf("NewFeishuSheetSource: %v", err)
	}
	// Two Open() calls within expiry should reuse the cached token
	if _, err := src.Open(context.Background(), nil); err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	if _, err := src.Open(context.Background(), nil); err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	if tokenHits != 1 {
		t.Fatalf("token hits = %d, want 1 (cached)", tokenHits)
	}
	if sheetHits != 2 {
		t.Fatalf("sheet hits = %d, want 2", sheetHits)
	}
}

func TestFeishuSheetTokenFetchFailure(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/open-apis/auth/v3/tenant_access_token/internal", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":99991663,"msg":"invalid app_id"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	src, err := NewFeishuSheetSource(map[string]any{
		"app_id":            "x",
		"app_secret":        "y",
		"spreadsheet_token": "tok",
		"sheet_range":       "A1:B2",
		"base_url":          srv.URL,
	})
	if err != nil {
		t.Fatalf("NewFeishuSheetSource: %v", err)
	}
	if _, err := src.Open(context.Background(), nil); err == nil {
		t.Fatalf("expected token fetch failure")
	}
}
