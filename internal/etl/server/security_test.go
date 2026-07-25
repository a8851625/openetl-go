package server

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseCORSOriginsProfileDefaults(t *testing.T) {
	dev, err := parseCORSOrigins("", false)
	if err != nil {
		t.Fatalf("development defaults: %v", err)
	}
	if len(dev) != 1 || dev[0] != "*" {
		t.Fatalf("development origins = %#v, want wildcard", dev)
	}

	prod, err := parseCORSOrigins("", true)
	if err != nil {
		t.Fatalf("production defaults: %v", err)
	}
	if len(prod) != 0 {
		t.Fatalf("production origins = %#v, want empty same-origin allow-list", prod)
	}

	if _, err := parseCORSOrigins("*", true); err == nil || !strings.Contains(err.Error(), "wildcard") {
		t.Fatalf("production wildcard error = %v", err)
	}

	origins, err := parseCORSOrigins(" https://console.example, http://localhost:5173 ", true)
	if err != nil {
		t.Fatalf("explicit origins: %v", err)
	}
	if len(origins) != 2 || origins[0] != "https://console.example" || origins[1] != "http://localhost:5173" {
		t.Fatalf("explicit origins = %#v", origins)
	}
}

func TestCORSAllowListAndSecurityHeaders(t *testing.T) {
	s := &Server{
		corsOrigins:    []string{"https://console.example"},
		runtimeProfile: RuntimeProfileConfig{Name: RuntimeProfileProduction},
	}
	called := false
	h := s.securityHeadersMiddleware(s.corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	})))

	req := httptest.NewRequest(http.MethodGet, "https://api.example/api/v2/pipelines", nil)
	req.Header.Set("Origin", "https://console.example")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if !called || res.Code != http.StatusNoContent {
		t.Fatalf("allowed request called=%v status=%d", called, res.Code)
	}
	if got := res.Header().Get("Access-Control-Allow-Origin"); got != "https://console.example" {
		t.Fatalf("allow-origin = %q", got)
	}
	if got := res.Header().Get("Vary"); got != "Origin" {
		t.Fatalf("vary = %q, want Origin", got)
	}
	for key, want := range map[string]string{
		"X-Content-Type-Options":    "nosniff",
		"X-Frame-Options":           "DENY",
		"Referrer-Policy":           "no-referrer",
		"Permissions-Policy":        "geolocation=(), microphone=(), camera=()",
		"Strict-Transport-Security": "max-age=31536000; includeSubDomains",
	} {
		if got := res.Header().Get(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}

func TestCORSRejectsDisallowedPreflight(t *testing.T) {
	s := &Server{corsOrigins: []string{"https://console.example"}}
	called := false
	h := s.securityHeadersMiddleware(s.corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})))
	req := httptest.NewRequest(http.MethodOptions, "http://api.example/api/v2/pipelines", nil)
	req.Header.Set("Origin", "https://evil.example")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusForbidden || called {
		t.Fatalf("disallowed preflight status=%d called=%v", res.Code, called)
	}
	if got := res.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("disallowed allow-origin = %q", got)
	}

	// A non-preflight request is rejected too; otherwise a simple cross-origin
	// POST could still mutate state even though the browser hides its response.
	getReq := httptest.NewRequest(http.MethodGet, "http://api.example/api/v2/pipelines", nil)
	getReq.Header.Set("Origin", "https://evil.example")
	getRes := httptest.NewRecorder()
	h.ServeHTTP(getRes, getReq)
	if getRes.Code != http.StatusForbidden {
		t.Fatalf("disallowed GET status=%d, want 403", getRes.Code)
	}
}

func TestSameHostOriginRemainsAllowedWithExplicitCrossOriginList(t *testing.T) {
	s := &Server{corsOrigins: []string{"https://other-console.example"}}
	called := false
	h := s.corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodOptions, "http://console.example/api/v2/pipelines", nil)
	req.Host = "console.example"
	req.Header.Set("Origin", "http://console.example")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if called || res.Code != http.StatusNoContent {
		t.Fatalf("same-host preflight called=%v status=%d", called, res.Code)
	}
	if got := res.Header().Get("Access-Control-Allow-Origin"); got != "http://console.example" {
		t.Fatalf("same-host allow-origin=%q", got)
	}
}

func TestClientIPOnlyTrustsForwardedHeadersFromConfiguredProxy(t *testing.T) {
	_, trusted, err := net.ParseCIDR("10.0.0.0/8")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{trustedProxyCIDRs: []*net.IPNet{trusted}}

	untrusted := httptest.NewRequest(http.MethodGet, "http://api.example/", nil)
	untrusted.RemoteAddr = "192.0.2.10:1234"
	untrusted.Header.Set("X-Forwarded-For", "203.0.113.7")
	if got := s.clientIP(untrusted); got != "192.0.2.10" {
		t.Fatalf("untrusted client IP = %q, want immediate peer", got)
	}

	trustedReq := httptest.NewRequest(http.MethodGet, "http://api.example/", nil)
	trustedReq.RemoteAddr = "10.2.3.4:5678"
	trustedReq.Header.Set("X-Forwarded-For", "203.0.113.8, 10.2.3.4")
	if got := s.clientIP(trustedReq); got != "203.0.113.8" {
		t.Fatalf("trusted client IP = %q, want forwarded client", got)
	}

	trustedReq.Header.Del("X-Forwarded-For")
	trustedReq.Header.Set("X-Real-IP", "203.0.113.9")
	if got := s.clientIP(trustedReq); got != "203.0.113.9" {
		t.Fatalf("trusted real IP = %q, want X-Real-IP", got)
	}
}

func TestAuthFailureHasChallengeAndSecurityHeaders(t *testing.T) {
	s := &Server{
		apiToken:       "expected-token",
		runtimeProfile: RuntimeProfileConfig{Name: RuntimeProfileProduction},
	}
	h := s.securityHeadersMiddleware(s.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})))
	req := httptest.NewRequest(http.MethodGet, "https://api.example/api/v2/pipelines", nil)
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d, want 401", res.Code)
	}
	if got := res.Header().Get("WWW-Authenticate"); got != `Bearer realm="openetl-etl-api"` {
		t.Fatalf("WWW-Authenticate=%q", got)
	}
	if got := res.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("security header missing on auth failure: %q", got)
	}
}
