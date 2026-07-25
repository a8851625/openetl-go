package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/net/ghttp"
)

// ConfigureHTTPSTopology makes the embedded UI and ETL API use one explicit
// transport topology. When ETL_TLS_CERT/ETL_TLS_KEY are present, GoFrame
// terminates TLS on the UI port and the reverse proxy speaks HTTPS to the ETL
// API. Without the pair, development keeps the historical HTTP/HTTP path.
// A partial certificate pair is rejected instead of silently creating a mixed
// HTTP/HTTPS deployment.
func ConfigureHTTPSTopology(ctx context.Context) error {
	certFile, keyFile := tlsFiles(ctx)
	if (certFile == "") != (keyFile == "") {
		return fmt.Errorf("ETL_TLS_CERT and ETL_TLS_KEY must be configured together")
	}
	if certFile != "" {
		if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
			return fmt.Errorf("load UI/API TLS certificate/key: %w", err)
		}
		// Bind the same configured UI address as HTTPS and clear the HTTP
		// address explicitly. This remains fail-closed even if a deployment
		// supplies an old `server.httpsAddr` override in its config.
		uiAddress := strings.TrimSpace(g.Cfg().MustGet(ctx, "server.address", ":8000").String())
		g.Server().EnableHTTPS(certFile, keyFile)
		g.Server().SetHTTPSAddr(uiAddress)
		g.Server().SetAddr("")
	}
	if _, err := newETLReverseProxy(ctx); err != nil {
		return fmt.Errorf("configure ETL reverse proxy: %w", err)
	}
	return nil
}

func tlsFiles(ctx context.Context) (string, string) {
	cert := strings.TrimSpace(os.Getenv("ETL_TLS_CERT"))
	if cert == "" {
		cert = strings.TrimSpace(g.Cfg().MustGet(ctx, "etl.tls.cert", "").String())
	}
	key := strings.TrimSpace(os.Getenv("ETL_TLS_KEY"))
	if key == "" {
		key = strings.TrimSpace(g.Cfg().MustGet(ctx, "etl.tls.key", "").String())
	}
	return cert, key
}

func tlsServerName(ctx context.Context) string {
	if value := strings.TrimSpace(os.Getenv("ETL_TLS_SERVER_NAME")); value != "" {
		return value
	}
	return strings.TrimSpace(g.Cfg().MustGet(ctx, "etl.tls.serverName", "").String())
}

func etlAddress(ctx context.Context) string {
	return strings.TrimSpace(g.Cfg().MustGet(ctx, "etl.address", ":8001").String())
}

type etlProxyConfig struct {
	target    *url.URL
	transport http.RoundTripper
}

func buildETLProxyConfig(ctx context.Context) (*etlProxyConfig, error) {
	certFile, keyFile := tlsFiles(ctx)
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("ETL_TLS_CERT and ETL_TLS_KEY must be configured together")
	}
	tlsEnabled := certFile != "" && keyFile != ""
	target, err := normalizeETLTarget(etlAddress(ctx), tlsEnabled)
	if err != nil {
		return nil, err
	}
	transport, err := newETLProxyTransport(certFile, tlsServerName(ctx))
	if err != nil {
		return nil, err
	}
	return &etlProxyConfig{target: target, transport: transport}, nil
}

func normalizeETLTarget(raw string, tlsEnabled bool) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = ":8001"
	}
	if strings.Contains(raw, "://") {
		return nil, fmt.Errorf("etl.address %q must be a bind address without an HTTP scheme", raw)
	}

	host := "localhost"
	port := ""
	if strings.HasPrefix(raw, ":") {
		port = strings.TrimPrefix(raw, ":")
	} else if _, err := strconv.Atoi(raw); err == nil {
		port = raw
	} else {
		parsedHost, parsedPort, err := net.SplitHostPort(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid etl.address %q: %w", raw, err)
		}
		if parsedHost != "" && parsedHost != "0.0.0.0" && parsedHost != "::" {
			host = parsedHost
		}
		port = parsedPort
	}
	if port == "" {
		return nil, fmt.Errorf("invalid etl.address %q: port is required", raw)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return nil, fmt.Errorf("invalid etl.address %q: port must be between 1 and 65535", raw)
	}
	hostPort := net.JoinHostPort(host, port)
	scheme := "http"
	if tlsEnabled {
		scheme = "https"
	}
	target, err := url.Parse(scheme + "://" + hostPort)
	if err != nil || target.Host == "" {
		return nil, fmt.Errorf("invalid etl.address %q", raw)
	}
	return target, nil
}

func newETLProxyTransport(certFile, serverName string) (http.RoundTripper, error) {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		if certFile != "" {
			return nil, fmt.Errorf("default HTTP transport cannot be cloned for verified ETL TLS")
		}
		return http.DefaultTransport, nil
	}
	transport := base.Clone()
	// etl.address is the listener in this same process; it must never be
	// redirected through HTTP_PROXY/HTTPS_PROXY from the host environment.
	transport.Proxy = nil
	if certFile == "" {
		return transport, nil
	}
	pemBytes, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("read ETL TLS certificate %q: %w", certFile, err)
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("ETL TLS certificate %q contains no readable certificate", certFile)
	}
	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: strings.TrimSpace(serverName),
	}
	return transport, nil
}

func newETLReverseProxy(ctx context.Context) (*httputil.ReverseProxy, error) {
	config, err := buildETLProxyConfig(ctx)
	if err != nil {
		return nil, err
	}
	proxy := httputil.NewSingleHostReverseProxy(config.target)
	proxy.Transport = config.transport
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		w.WriteHeader(http.StatusBadGateway)
	}
	return proxy, nil
}

func (a *sApp) frontendSecurityHeaders(r *ghttp.Request) {
	r.Response.Header().Set("X-Content-Type-Options", "nosniff")
	r.Response.Header().Set("X-Frame-Options", "DENY")
	r.Response.Header().Set("Referrer-Policy", "no-referrer")
	r.Response.Header().Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
	certFile, keyFile := tlsFiles(r.Context())
	if certFile != "" && keyFile != "" {
		r.Response.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
	}
	r.Middleware.Next()
}
