package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

const (
	RuntimeProfileDevelopment = "development"
	RuntimeProfileProduction  = "production"
)

type RuntimeProfileConfig struct {
	Name                string
	InsecureDevelopment bool
	Role                string
	APIToken            string
	TLSCert             string
	TLSKey              string
}

// ValidateRuntimeProfile resolves the runtime profile and enforces the
// production fail-closed gate before pipeline restore or HTTP startup.
func ValidateRuntimeProfile(ctx context.Context, role string) (RuntimeProfileConfig, error) {
	profile, err := normalizeRuntimeProfile(configString(ctx, "ETL_PROFILE", "etl.profile", RuntimeProfileDevelopment))
	if err != nil {
		return RuntimeProfileConfig{}, err
	}
	if role == "" {
		role = configString(ctx, "ETL_ROLE", "etl.role", "standalone")
	}
	cfg := RuntimeProfileConfig{
		Name:                profile,
		InsecureDevelopment: configBool(ctx, "ETL_INSECURE_DEV", "etl.insecureDevelopment", false),
		Role:                role,
		APIToken:            configString(ctx, "ETL_API_TOKEN", "etl.apiToken", ""),
		TLSCert:             configString(ctx, "ETL_TLS_CERT", "etl.tls.cert", ""),
		TLSKey:              configString(ctx, "ETL_TLS_KEY", "etl.tls.key", ""),
	}
	if cfg.Name != RuntimeProfileProduction {
		return cfg, nil
	}
	if cfg.InsecureDevelopment {
		g.Log().Warningf(ctx, "ETL_PROFILE=production is running with ETL_INSECURE_DEV=true; production secret/TLS gates are bypassed for explicit development use only")
		return cfg, nil
	}
	if role == "worker" {
		return cfg, fmt.Errorf("production worker profile is blocked until PR-D1 authenticated transport is delivered; use ETL_INSECURE_DEV=true only for explicit development runs")
	}

	var problems []string
	if isPlaceholderSecret(cfg.APIToken) || len(cfg.APIToken) < 16 {
		problems = append(problems, "ETL_API_TOKEN must be a non-placeholder token of at least 16 characters")
	}
	cipher, cipherErr := storage.NewSpecCipherFromEnv()
	if cipherErr != nil {
		problems = append(problems, cipherErr.Error())
	} else if !cipher.Enabled() {
		problems = append(problems, "ETL_SPEC_ENCRYPTION_KEY must be a base64-encoded 32-byte key")
	}
	if cfg.TLSCert == "" || cfg.TLSKey == "" {
		problems = append(problems, "ETL_TLS_CERT and ETL_TLS_KEY are required for the production profile")
	} else if _, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey); err != nil {
		problems = append(problems, fmt.Sprintf("load ETL TLS certificate/key: %v", err))
	}
	if !configBool(ctx, "ETL_AUDIT_ENABLED", "etl.audit.enabled", true) {
		problems = append(problems, "ETL_AUDIT_ENABLED must remain true in the production profile")
	}

	storageType := strings.ToLower(configString(ctx, "ETL_STORAGE_TYPE", "etl.storage.type", "sqlite"))
	if storageType == "mysql" || storageType == "postgresql" {
		configKey := "etl.storage.mysql.dsn"
		if storageType == "postgresql" {
			configKey = "etl.storage.postgresql.dsn"
		}
		dsn := configString(ctx, "ETL_STORAGE_DSN", configKey, "")
		if strings.TrimSpace(dsn) == "" || containsPlaceholder(dsn) {
			problems = append(problems, "ETL_STORAGE_DSN must be set and must not contain placeholder credentials")
		}
	}
	redisAddr := configString(ctx, "ETL_STATE_REDIS_ADDR", "etl.state.redis.addr", "")
	if redisAddr != "" {
		redisPassword := configString(ctx, "ETL_STATE_REDIS_PASSWORD", "etl.state.redis.password", "")
		if isPlaceholderSecret(redisPassword) {
			problems = append(problems, "ETL_STATE_REDIS_PASSWORD must be set and must not use a placeholder when Redis state is enabled")
		}
	}

	if len(problems) > 0 {
		return cfg, fmt.Errorf("production profile validation failed: %s; set ETL_INSECURE_DEV=true only for explicit development bypass", strings.Join(problems, "; "))
	}
	return cfg, nil
}

func normalizeRuntimeProfile(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "dev", "development":
		return RuntimeProfileDevelopment, nil
	case "prod", "production":
		return RuntimeProfileProduction, nil
	default:
		return "", fmt.Errorf("invalid ETL_PROFILE %q: must be development or production", raw)
	}
}

func isPlaceholderSecret(value string) bool {
	v := strings.ToLower(strings.TrimSpace(value))
	if v == "" {
		return true
	}
	switch v {
	case "change-me", "changeme", "change_me", "password", "secret", "token", "replace-me", "replace_me":
		return true
	}
	return strings.Contains(v, "change-me") || strings.Contains(v, "changeme")
}

func containsPlaceholder(value string) bool {
	v := strings.ToLower(value)
	return strings.Contains(v, "change-me") || strings.Contains(v, "changeme") || strings.Contains(v, "replace-me")
}
