package worker

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// TransportConfig configures the authenticated HTTP client used by a
// distributed worker to talk to the master (PR-D1.1).
type TransportConfig struct {
	// APIToken is sent as X-API-Token (and Bearer Authorization) on every
	// register/heartbeat/poll/report/deregister request.
	APIToken string
	// Timeout bounds each request. Zero defaults to 10s.
	Timeout time.Duration
	// TLSInsecureSkipVerify is only for local e2e with self-signed certs.
	// Production must leave this false and present a verifiable trust chain.
	TLSInsecureSkipVerify bool
	// TLSServerName overrides the SNI/hostname used for certificate verification.
	TLSServerName string
	// MaxRetries is the number of additional attempts after the first failure
	// for idempotent register/heartbeat/deregister. Poll/report use 0 by default
	// so ownership semantics stay simple.
	MaxRetries int
}

// MasterClient is a unified, timeout-bound HTTP client that always attaches
// the configured API token. Production deployments must use HTTPS + token.
type MasterClient struct {
	baseURL string
	token   string
	client  *http.Client
	retries int
}

// NewMasterClient builds a MasterClient from TransportConfig. baseURL is the
// master API root (e.g. https://master:8001). Empty token is allowed only for
// insecure-dev / unit tests; production profile rejects empty tokens at boot.
func NewMasterClient(baseURL string, cfg TransportConfig) *MasterClient {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.TLSInsecureSkipVerify || cfg.TLSServerName != "" {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: cfg.TLSInsecureSkipVerify, //nolint:gosec // explicit opt-in for local e2e only
			ServerName:         cfg.TLSServerName,
		}
	}
	retries := cfg.MaxRetries
	if retries < 0 {
		retries = 0
	}
	return &MasterClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   cfg.APIToken,
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		retries: retries,
	}
}

// TransportConfigFromEnv reads worker transport settings from environment.
// Used by the worker role bootstrap so flags/env stay consistent with master.
func TransportConfigFromEnv() TransportConfig {
	cfg := TransportConfig{
		APIToken: strings.TrimSpace(os.Getenv("ETL_API_TOKEN")),
		Timeout:  10 * time.Second,
		MaxRetries: 2,
	}
	if v := strings.TrimSpace(os.Getenv("ETL_WORKER_HTTP_TIMEOUT")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.Timeout = d
		}
	}
	if strings.EqualFold(os.Getenv("ETL_WORKER_TLS_INSECURE"), "true") ||
		os.Getenv("ETL_WORKER_TLS_INSECURE") == "1" {
		cfg.TLSInsecureSkipVerify = true
	}
	cfg.TLSServerName = strings.TrimSpace(os.Getenv("ETL_WORKER_TLS_SERVER_NAME"))
	return cfg
}

func (c *MasterClient) do(ctx context.Context, method, path string, body []byte, retryable bool) (*http.Response, error) {
	if c == nil {
		return nil, fmt.Errorf("master client is nil")
	}
	url := c.baseURL + path
	attempts := 1
	if retryable {
		attempts += c.retries
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			// Bounded linear backoff: 200ms, 400ms, ...
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(i) * 200 * time.Millisecond):
			}
		}
		var rdr io.Reader
		if body != nil {
			rdr = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, rdr)
		if err != nil {
			return nil, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if c.token != "" {
			req.Header.Set("X-API-Token", c.token)
			req.Header.Set("Authorization", "Bearer "+c.token)
		}
		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		// Retry only on 5xx for idempotent calls; 4xx is diagnostic and final.
		if retryable && resp.StatusCode >= 500 {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("master %s %s returned %d", method, path, resp.StatusCode)
			continue
		}
		return resp, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("master request failed")
	}
	return nil, lastErr
}

// PostJSON issues a POST with optional body and retries on transport/5xx errors
// when retryable is true.
func (c *MasterClient) PostJSON(ctx context.Context, path string, body []byte, retryable bool) (*http.Response, error) {
	return c.do(ctx, http.MethodPost, path, body, retryable)
}

// Delete issues a DELETE with retries.
func (c *MasterClient) Delete(ctx context.Context, path string) (*http.Response, error) {
	return c.do(ctx, http.MethodDelete, path, nil, true)
}
