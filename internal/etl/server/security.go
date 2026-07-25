package server

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
)

func parseCORSOrigins(raw string, production bool) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if production {
			return nil, nil
		}
		return []string{"*"}, nil
	}
	var origins []string
	for _, item := range strings.Split(raw, ",") {
		origin := strings.TrimSpace(item)
		if origin == "" {
			continue
		}
		if origin == "*" {
			if production {
				return nil, fmt.Errorf("wildcard CORS origin is not allowed in production")
			}
			origins = append(origins, origin)
			continue
		}
		u, err := url.Parse(origin)
		if err != nil || u.Scheme == "" || u.Host == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("invalid CORS origin %q", origin)
		}
		origins = append(origins, origin)
	}
	return origins, nil
}

func parseTrustedProxyCIDRs(raw string) ([]*net.IPNet, error) {
	var result []*net.IPNet
	for _, item := range strings.Split(strings.TrimSpace(raw), ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if !strings.Contains(item, "/") {
			ip := net.ParseIP(item)
			if ip == nil {
				return nil, fmt.Errorf("invalid trusted proxy IP %q", item)
			}
			bits := 128
			if ip.To4() != nil {
				bits = 32
				ip = ip.To4()
			}
			result = append(result, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
			continue
		}
		_, network, err := net.ParseCIDR(item)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy CIDR %q: %w", item, err)
		}
		result = append(result, network)
	}
	return result, nil
}

func (s *Server) originAllowed(origin string) bool {
	if origin == "" {
		return true
	}
	for _, allowed := range s.corsOrigins {
		if allowed == "*" || allowed == origin {
			return true
		}
	}
	return false
}

// sameOrigin reports whether an Origin header points at the host through
// which this request arrived. The API is commonly reached through the
// GoFrame :8000 reverse proxy, so comparing the incoming Host is more useful
// than comparing the ETL listener's private :8001 address. The scheme is not
// compared here because TLS may terminate at that trusted front-end.
func sameOrigin(r *http.Request, origin string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || r == nil {
		return false
	}
	host := strings.TrimSpace(r.Host)
	if host == "" && r.URL != nil {
		host = strings.TrimSpace(r.URL.Host)
	}
	return strings.EqualFold(strings.TrimSuffix(u.Host, "."), strings.TrimSuffix(host, "."))
}

func (s *Server) isTrustedProxy(ip net.IP) bool {
	for _, network := range s.trustedProxyCIDRs {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

// clientIP trusts forwarded headers only when the immediate peer is in the
// configured trusted-proxy set. Untrusted clients cannot spoof audit/rate
// limit identity with X-Forwarded-For.
func (s *Server) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		host = strings.TrimSpace(r.RemoteAddr)
	}
	peer := net.ParseIP(host)
	if peer != nil && s.isTrustedProxy(peer) {
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			candidate := strings.TrimSpace(strings.Split(forwarded, ",")[0])
			if net.ParseIP(candidate) != nil {
				return candidate
			}
		}
		if realIP := strings.TrimSpace(r.Header.Get("X-Real-IP")); net.ParseIP(realIP) != nil {
			return realIP
		}
	}
	if host == "" {
		return "unknown"
	}
	return host
}

func (s *Server) securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		if s.runtimeProfile.Name == RuntimeProfileProduction {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		next.ServeHTTP(w, r)
	})
}
