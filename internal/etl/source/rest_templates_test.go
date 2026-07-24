package source

import (
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func TestRestTemplatesRegistered(t *testing.T) {
	for _, name := range []string{"rest_source", "salesforce", "github", "hubspot", "stripe", "notion"} {
		if !registry.HasSource(name) {
			t.Errorf("source %q not registered", name)
		}
	}
}

func TestSalesforceTemplateDefaults(t *testing.T) {
	cfg, err := BuildRestTemplateConfig("salesforce", map[string]any{
		"client_id":     "cid",
		"client_secret": "csec",
		"object":        "Contact",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if cfg["auth"] != "oauth2_client_credentials" {
		t.Errorf("auth=%v", cfg["auth"])
	}
	if cfg["pagination"] != "offset" {
		t.Errorf("pagination=%v", cfg["pagination"])
	}
	if cfg["result_key"] != "records" {
		t.Errorf("result_key=%v", cfg["result_key"])
	}
	url, _ := cfg["url"].(string)
	if !strings.Contains(url, "Contact") {
		t.Errorf("url should contain object Contact: %s", url)
	}
	// 可实例化
	src, err := NewSalesforceSource(map[string]any{
		"client_id":     "cid",
		"client_secret": "csec",
		"object":        "Contact",
	})
	if err != nil {
		t.Fatalf("NewSalesforceSource: %v", err)
	}
	if src.Name() != "salesforce" {
		// name 可能被 config 覆盖；默认应为 salesforce
		if rs, ok := src.(*RestSource); ok && rs.name != "salesforce" {
			t.Errorf("name=%q", rs.name)
		}
	}
}

func TestGitHubTemplateDefaultsAndOverride(t *testing.T) {
	cfg, err := BuildRestTemplateConfig("github", map[string]any{
		"repo":  "octocat/Hello-World",
		"token": "ghp_x",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if cfg["auth"] != "bearer" {
		t.Errorf("auth=%v", cfg["auth"])
	}
	if cfg["cursor_header"] != "Link" {
		t.Errorf("cursor_header=%v", cfg["cursor_header"])
	}
	url, _ := cfg["url"].(string)
	if !strings.Contains(url, "api.github.com/repos/octocat/Hello-World/issues") {
		t.Errorf("url=%s", url)
	}

	// 自定义 resource + base_url 覆盖
	cfg2, err := BuildRestTemplateConfig("github", map[string]any{
		"repo":     "o/r",
		"resource": "commits",
		"base_url": "https://github.example.com/api/v3",
		"token":    "t",
	})
	if err != nil {
		t.Fatalf("build2: %v", err)
	}
	url2, _ := cfg2["url"].(string)
	if !strings.Contains(url2, "github.example.com") || !strings.Contains(url2, "/commits") {
		t.Errorf("url2=%s", url2)
	}

	src, err := NewGitHubSource(map[string]any{
		"repo":  "octocat/Hello-World",
		"token": "ghp_x",
	})
	if err != nil {
		t.Fatalf("NewGitHubSource: %v", err)
	}
	_ = src
}

func TestHubSpotTemplateDefaults(t *testing.T) {
	// API key 模式
	cfg, err := BuildRestTemplateConfig("hubspot", map[string]any{
		"object":        "deals",
		"api_key_value": "hub-key",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if cfg["auth"] != "api_key" {
		t.Errorf("auth=%v, want api_key", cfg["auth"])
	}
	if cfg["result_key"] != "results" {
		t.Errorf("result_key=%v", cfg["result_key"])
	}
	url, _ := cfg["url"].(string)
	if !strings.Contains(url, "/crm/v3/objects/deals") {
		t.Errorf("url=%s", url)
	}

	// Private app token → bearer
	cfg2, err := BuildRestTemplateConfig("hubspot", map[string]any{
		"object": "contacts",
		"token":  "pat-xxx",
	})
	if err != nil {
		t.Fatalf("build2: %v", err)
	}
	if cfg2["auth"] != "bearer" {
		t.Errorf("auth=%v, want bearer", cfg2["auth"])
	}

	src, err := NewHubSpotSource(map[string]any{
		"api_key_value": "hub-key",
	})
	if err != nil {
		t.Fatalf("NewHubSpotSource: %v", err)
	}
	_ = src
}

func TestStripeTemplateDefaults(t *testing.T) {
	cfg, err := BuildRestTemplateConfig("stripe", map[string]any{
		"resource": "customers",
		"token":    "sk_test_x",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if cfg["auth"] != "bearer" {
		t.Errorf("auth=%v", cfg["auth"])
	}
	if cfg["result_key"] != "data" {
		t.Errorf("result_key=%v", cfg["result_key"])
	}
	if cfg["cursor_field"] != "__last_id__" {
		t.Errorf("cursor_field=%v", cfg["cursor_field"])
	}
	url, _ := cfg["url"].(string)
	if !strings.Contains(url, "api.stripe.com/v1/customers") {
		t.Errorf("url=%s", url)
	}

	// secret_key 别名
	cfg2, err := BuildRestTemplateConfig("stripe", map[string]any{
		"secret_key": "sk_live",
	})
	if err != nil {
		t.Fatalf("build2: %v", err)
	}
	if cfg2["token"] != "sk_live" {
		t.Errorf("token=%v", cfg2["token"])
	}

	src, err := NewStripeSource(map[string]any{"token": "sk_test_x"})
	if err != nil {
		t.Fatalf("NewStripeSource: %v", err)
	}
	_ = src
}

func TestNotionTemplateDefaults(t *testing.T) {
	cfg, err := BuildRestTemplateConfig("notion", map[string]any{
		"database_id": "db-123",
		"token":       "secret_x",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if cfg["auth"] != "bearer" {
		t.Errorf("auth=%v", cfg["auth"])
	}
	if cfg["pagination"] != "page_token" {
		t.Errorf("pagination=%v", cfg["pagination"])
	}
	if cfg["method"] != "POST" {
		t.Errorf("method=%v", cfg["method"])
	}
	url, _ := cfg["url"].(string)
	if !strings.Contains(url, "/v1/databases/db-123/query") {
		t.Errorf("url=%s", url)
	}
	headers, _ := cfg["headers"].(map[string]any)
	if headers["Notion-Version"] == "" {
		t.Errorf("missing Notion-Version header: %#v", headers)
	}

	// 自定义 page_size 覆盖 body
	cfg2, err := BuildRestTemplateConfig("notion", map[string]any{
		"database_id": "db-1",
		"token":       "t",
		"page_size":   50,
	})
	if err != nil {
		t.Fatalf("build2: %v", err)
	}
	body, _ := cfg2["body"].(string)
	if !strings.Contains(body, "50") {
		t.Errorf("body=%s", body)
	}

	if _, err := NewNotionSource(map[string]any{"token": "t"}); err == nil {
		t.Fatal("expected error without database_id")
	}
	src, err := NewNotionSource(map[string]any{
		"database_id": "db-123",
		"token":       "secret_x",
	})
	if err != nil {
		t.Fatalf("NewNotionSource: %v", err)
	}
	_ = src
}

func TestTemplateConfigOverrideURL(t *testing.T) {
	cfg, err := BuildRestTemplateConfig("stripe", map[string]any{
		"url":   "https://example.com/custom",
		"token": "sk",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if cfg["url"] != "https://example.com/custom" {
		t.Errorf("url override failed: %v", cfg["url"])
	}
}

func TestRegistryBuildTemplates(t *testing.T) {
	cases := []struct {
		typ    string
		config map[string]any
	}{
		{"rest_source", map[string]any{"url": "http://example.com"}},
		{"salesforce", map[string]any{"client_id": "c", "client_secret": "s"}},
		{"github", map[string]any{"repo": "a/b", "token": "t"}},
		{"hubspot", map[string]any{"api_key_value": "k"}},
		{"stripe", map[string]any{"token": "sk"}},
		{"notion", map[string]any{"database_id": "d", "token": "t"}},
	}
	for _, tc := range cases {
		src, err := registry.BuildSource(tc.typ, tc.config)
		if err != nil {
			t.Errorf("BuildSource(%s): %v", tc.typ, err)
			continue
		}
		if src == nil {
			t.Errorf("BuildSource(%s) returned nil", tc.typ)
		}
	}
}
