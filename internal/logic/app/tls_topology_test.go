package app

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizeETLTargetUsesHTTPSWhenTLSIsConfigured(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		tls       bool
		want      string
		wantError string
	}{
		{name: "default http", raw: ":8001", want: "http://localhost:8001"},
		{name: "tls default", raw: ":8001", tls: true, want: "https://localhost:8001"},
		{name: "explicit host", raw: "etl.internal:9443", tls: true, want: "https://etl.internal:9443"},
		{name: "scheme is invalid for bind address", raw: "http://etl.internal:8001", tls: true, wantError: "without an HTTP scheme"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeETLTarget(tc.raw, tc.tls)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("error=%v, want %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeETLTarget: %v", err)
			}
			if got.String() != tc.want {
				t.Fatalf("target=%q, want %q", got.String(), tc.want)
			}
		})
	}
}

func TestETLProxyTransportTrustsConfiguredCertificateWithoutSkippingVerification(t *testing.T) {
	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()
	cert := backend.Certificate()
	if cert == nil {
		t.Fatal("test TLS server did not expose a certificate")
	}
	serverName := ""
	if len(cert.DNSNames) > 0 {
		serverName = cert.DNSNames[0]
	}
	if serverName == "" {
		t.Fatal("test TLS certificate has no DNS name")
	}

	certPath := filepath.Join(t.TempDir(), "backend.crt")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	transport, err := newETLProxyTransport(certPath, serverName)
	if err != nil {
		t.Fatalf("newETLProxyTransport: %v", err)
	}
	client := backend.Client()
	client.Transport = transport
	resp, err := client.Get(backend.URL)
	if err != nil {
		t.Fatalf("TLS proxy transport request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	if cfg, ok := transport.(*http.Transport); !ok {
		t.Fatalf("transport type=%T, want *http.Transport", transport)
	} else if cfg.TLSClientConfig == nil || cfg.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("TLS transport disabled certificate verification")
	}

	wrongNameTransport, err := newETLProxyTransport(certPath, "wrong-name.invalid")
	if err != nil {
		t.Fatalf("wrong-name transport setup: %v", err)
	}
	wrongNameClient := backend.Client()
	wrongNameClient.Transport = wrongNameTransport
	if resp, err := wrongNameClient.Get(backend.URL); err == nil {
		resp.Body.Close()
		t.Fatal("TLS request unexpectedly accepted a certificate for the wrong server name")
	}
}

func TestConfigureHTTPSTopologyRejectsPartialCertificatePair(t *testing.T) {
	t.Setenv("ETL_TLS_CERT", filepath.Join(t.TempDir(), "tls.crt"))
	t.Setenv("ETL_TLS_KEY", "")
	if err := ConfigureHTTPSTopology(t.Context()); err == nil || !strings.Contains(err.Error(), "configured together") {
		t.Fatalf("partial TLS pair error=%v", err)
	}
}
