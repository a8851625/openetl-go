package cmd

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gogf/gf/v2/frame/g"
)

type runtimeFlags struct {
	config       string
	dataDir      string
	logDir       string
	pluginsDir   string
	schemasDir   string
	specsDir     string
	host         string
	port         string
	etlAPIHost   string
	etlAPIPort   string
	storageType  string
	storageDSN   string
	sqlitePath   string
	apiToken     string
	tlsCert      string
	tlsKey       string
	role         string
	masterURL    string
	workerID     string
	workerSlots  string
	auditEnabled string
	loggerFormat string
	printHelp    bool
	seen         map[string]bool
}

func applyRuntimeFlags() (*runtimeFlags, error) {
	opts, err := parseRuntimeFlags(os.Args[1:], os.Stderr)
	if err != nil {
		return nil, err
	}
	if opts.printHelp {
		os.Exit(0)
	}

	if opts.config != "" {
		if err := setConfigFile(opts.config); err != nil {
			return nil, err
		}
	}

	applyRuntimeEnvOverrides(opts.seen)
	if err := applyRuntimeFlagOverrides(opts); err != nil {
		return nil, err
	}
	return opts, nil
}

func parseRuntimeFlags(args []string, output io.Writer) (*runtimeFlags, error) {
	opts := &runtimeFlags{seen: map[string]bool{}}
	fs := flag.NewFlagSet("openetl-go", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.Usage = func() {
		fmt.Fprint(output, runtimeHelpText())
	}

	fs.StringVar(&opts.config, "config", "", "config file path")
	fs.StringVar(&opts.dataDir, "data-dir", "", "local data directory")
	fs.StringVar(&opts.logDir, "log-dir", "", "log directory")
	fs.StringVar(&opts.pluginsDir, "plugins-dir", "", "plugin WASM directory")
	fs.StringVar(&opts.schemasDir, "schemas-dir", "", "schema registry directory")
	fs.StringVar(&opts.specsDir, "specs-dir", "", "pipeline specs directory")
	fs.StringVar(&opts.host, "host", "", "GoFrame HTTP host")
	fs.StringVar(&opts.port, "port", "", "GoFrame HTTP port")
	fs.StringVar(&opts.etlAPIHost, "etl-api-host", "", "ETL API host")
	fs.StringVar(&opts.etlAPIPort, "etl-api-port", "", "ETL API port")
	fs.StringVar(&opts.storageType, "storage", "", "storage backend: sqlite|mysql|postgresql")
	fs.StringVar(&opts.storageDSN, "storage-dsn", "", "MySQL/PostgreSQL storage DSN")
	fs.StringVar(&opts.sqlitePath, "sqlite-path", "", "SQLite metadata database path")
	fs.StringVar(&opts.apiToken, "api-token", "", "ETL API token")
	fs.StringVar(&opts.tlsCert, "tls-cert", "", "ETL API TLS certificate file")
	fs.StringVar(&opts.tlsKey, "tls-key", "", "ETL API TLS key file")
	fs.StringVar(&opts.role, "role", "", "runtime role: standalone|master|worker")
	fs.StringVar(&opts.masterURL, "master-url", "", "master API URL for worker role")
	fs.StringVar(&opts.workerID, "worker-id", "", "worker identifier")
	fs.StringVar(&opts.workerSlots, "worker-slots", "", "worker shard slots")
	fs.StringVar(&opts.auditEnabled, "audit-enabled", "", "enable SQL-backed audit logging: true|false")
	fs.StringVar(&opts.loggerFormat, "logger-format", "", "logger format: text|json")
	fs.BoolVar(&opts.printHelp, "help", false, "show help")
	fs.BoolVar(&opts.printHelp, "h", false, "show help")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	fs.Visit(func(f *flag.Flag) {
		opts.seen[f.Name] = true
	})
	return opts, nil
}

func setConfigFile(configPath string) error {
	abs, err := filepath.Abs(configPath)
	if err != nil {
		return fmt.Errorf("resolve --config: %w", err)
	}
	adapter := g.Cfg().GetAdapter()
	if setter, ok := adapter.(interface{ SetFileName(string) }); ok {
		setter.SetFileName(abs)
		return nil
	}
	return fmt.Errorf("active config adapter does not support SetFileName")
}

func applyRuntimeEnvOverrides(flagSeen map[string]bool) {
	envString(flagSeen, "data-dir", "ETL_DATA_DIR", applyDataDir)
	envString(flagSeen, "log-dir", "ETL_LOG_DIR", func(v string) { mustSetConfig("logger.path", v) })
	envString(flagSeen, "plugins-dir", "ETL_PLUGINS_DIR", func(v string) { mustSetConfig("etl.pluginsDir", v) })
	envString(flagSeen, "schemas-dir", "ETL_SCHEMAS_DIR", func(v string) { mustSetConfig("etl.schemasDir", v) })
	envString(flagSeen, "specs-dir", "ETL_SPECS_DIR", func(v string) { mustSetConfig("etl.specsDir", v) })
	envString(flagSeen, "host", "ETL_HTTP_HOST", func(v string) { mustSetConfig("server.address", joinHostPort(v, configPort("server.address", "8000"))) })
	envString(flagSeen, "port", "ETL_HTTP_PORT", func(v string) { mustSetConfig("server.address", joinHostPort(configHost("server.address"), v)) })
	envString(flagSeen, "etl-api-host", "ETL_API_HOST", func(v string) { mustSetConfig("etl.address", joinHostPort(v, configPort("etl.address", "8001"))) })
	envString(flagSeen, "etl-api-port", "ETL_API_PORT", func(v string) { mustSetConfig("etl.address", joinHostPort(configHost("etl.address"), v)) })
	envString(flagSeen, "storage", "ETL_STORAGE_TYPE", func(v string) { mustSetConfig("etl.storage.type", v) })
	envString(flagSeen, "storage-dsn", "ETL_STORAGE_DSN", applyStorageDSN)
	envString(flagSeen, "sqlite-path", "ETL_SQLITE_PATH", func(v string) { mustSetConfig("etl.storage.sqlite.path", v) })
	envString(flagSeen, "role", "ETL_ROLE", func(v string) { mustSetConfig("etl.role", v) })
	envString(flagSeen, "master-url", "ETL_MASTER_URL", func(v string) { mustSetConfig("etl.masterURL", v) })
	envString(flagSeen, "worker-id", "ETL_WORKER_ID", func(v string) { mustSetConfig("etl.workerID", v) })
	envString(flagSeen, "worker-slots", "ETL_WORKER_SLOTS", func(v string) { mustSetConfig("etl.workerSlots", atoiOrString(v)) })
	envString(flagSeen, "api-token", "ETL_API_TOKEN", func(v string) { mustSetConfig("etl.apiToken", v) })
	envString(flagSeen, "tls-cert", "ETL_TLS_CERT", func(v string) { mustSetConfig("etl.tls.cert", v) })
	envString(flagSeen, "tls-key", "ETL_TLS_KEY", func(v string) { mustSetConfig("etl.tls.key", v) })
	envString(flagSeen, "audit-enabled", "ETL_AUDIT_ENABLED", func(v string) { mustSetConfig("etl.audit.enabled", atobOrString(v)) })
	envString(flagSeen, "logger-format", "LOGGER_FORMAT", func(v string) { mustSetConfig("logger.format", v) })
}

func applyRuntimeFlagOverrides(opts *runtimeFlags) error {
	if opts.dataDir != "" {
		applyDataDir(opts.dataDir)
		_ = os.Setenv("ETL_DATA_DIR", opts.dataDir)
		_ = os.Setenv("ETL_PLUGINS_DIR", filepath.Join(opts.dataDir, "plugins"))
		_ = os.Setenv("ETL_SCHEMAS_DIR", filepath.Join(opts.dataDir, "schemas"))
	}
	setStringFlag(opts, "log-dir", opts.logDir, "logger.path", "ETL_LOG_DIR")
	setStringFlag(opts, "plugins-dir", opts.pluginsDir, "etl.pluginsDir", "ETL_PLUGINS_DIR")
	setStringFlag(opts, "schemas-dir", opts.schemasDir, "etl.schemasDir", "ETL_SCHEMAS_DIR")
	setStringFlag(opts, "specs-dir", opts.specsDir, "etl.specsDir", "ETL_SPECS_DIR")
	setStringFlag(opts, "storage", opts.storageType, "etl.storage.type", "ETL_STORAGE_TYPE")
	if opts.storageDSN != "" {
		applyStorageDSN(opts.storageDSN)
		_ = os.Setenv("ETL_STORAGE_DSN", opts.storageDSN)
	}
	setStringFlag(opts, "sqlite-path", opts.sqlitePath, "etl.storage.sqlite.path", "ETL_SQLITE_PATH")
	setStringFlag(opts, "api-token", opts.apiToken, "etl.apiToken", "ETL_API_TOKEN")
	setStringFlag(opts, "tls-cert", opts.tlsCert, "etl.tls.cert", "ETL_TLS_CERT")
	setStringFlag(opts, "tls-key", opts.tlsKey, "etl.tls.key", "ETL_TLS_KEY")
	setStringFlag(opts, "role", opts.role, "etl.role", "ETL_ROLE")
	setStringFlag(opts, "master-url", opts.masterURL, "etl.masterURL", "ETL_MASTER_URL")
	setStringFlag(opts, "worker-id", opts.workerID, "etl.workerID", "ETL_WORKER_ID")
	if opts.auditEnabled != "" {
		mustSetConfig("etl.audit.enabled", atobOrString(opts.auditEnabled))
		_ = os.Setenv("ETL_AUDIT_ENABLED", opts.auditEnabled)
	}
	setStringFlag(opts, "logger-format", opts.loggerFormat, "logger.format", "LOGGER_FORMAT")
	if opts.workerSlots != "" {
		mustSetConfig("etl.workerSlots", atoiOrString(opts.workerSlots))
		_ = os.Setenv("ETL_WORKER_SLOTS", opts.workerSlots)
	}
	if opts.host != "" || opts.port != "" {
		host := opts.host
		if host == "" {
			host = configHost("server.address")
		}
		port := opts.port
		if port == "" {
			port = configPort("server.address", "8000")
		}
		mustSetConfig("server.address", joinHostPort(host, port))
	}
	if opts.etlAPIHost != "" || opts.etlAPIPort != "" {
		host := opts.etlAPIHost
		if host == "" {
			host = configHost("etl.address")
		}
		port := opts.etlAPIPort
		if port == "" {
			port = configPort("etl.address", "8001")
		}
		mustSetConfig("etl.address", joinHostPort(host, port))
	}
	return validateRuntimeFlags(opts)
}

func validateRuntimeFlags(opts *runtimeFlags) error {
	role := opts.role
	if role == "" {
		role = g.Cfg().MustGet(context.Background(), "etl.role", "standalone").String()
	}
	if role != "standalone" && role != "master" && role != "worker" {
		return fmt.Errorf("invalid --role %q: must be standalone, master, or worker", role)
	}
	if opts.storageType != "" && opts.storageType != "sqlite" && opts.storageType != "mysql" && opts.storageType != "postgresql" {
		return fmt.Errorf("invalid --storage %q: must be sqlite, mysql, or postgresql", opts.storageType)
	}
	if opts.workerSlots != "" {
		n, err := strconv.Atoi(opts.workerSlots)
		if err != nil || n <= 0 {
			return fmt.Errorf("invalid --worker-slots %q: must be a positive integer", opts.workerSlots)
		}
	}
	if opts.auditEnabled != "" {
		if _, err := strconv.ParseBool(opts.auditEnabled); err != nil {
			return fmt.Errorf("invalid --audit-enabled %q: must be true or false", opts.auditEnabled)
		}
	}
	for _, item := range []struct {
		name  string
		value string
	}{
		{"--port", opts.port},
		{"--etl-api-port", opts.etlAPIPort},
	} {
		if item.value == "" {
			continue
		}
		n, err := strconv.Atoi(item.value)
		if err != nil || n <= 0 || n > 65535 {
			return fmt.Errorf("invalid %s %q: must be a TCP port", item.name, item.value)
		}
	}
	return nil
}

func applyDataDir(dir string) {
	mustSetConfig("etl.dataDir", dir)
	mustSetConfig("etl.checkpointDir", filepath.Join(dir, "checkpoint"))
	mustSetConfig("etl.dlqDir", filepath.Join(dir, "dlq"))
	mustSetConfig("etl.storage.sqlite.path", filepath.Join(dir, "etl.db"))
	mustSetConfig("etl.pluginsDir", filepath.Join(dir, "plugins"))
	mustSetConfig("etl.schemasDir", filepath.Join(dir, "schemas"))
}

func applyStorageDSN(dsn string) {
	storageType := g.Cfg().MustGet(context.Background(), "etl.storage.type", "").String()
	switch storageType {
	case "postgresql":
		mustSetConfig("etl.storage.postgresql.dsn", dsn)
	default:
		mustSetConfig("etl.storage.mysql.dsn", dsn)
	}
}

func envString(flagSeen map[string]bool, flagName, envName string, apply func(string)) {
	if flagSeen[flagName] {
		return
	}
	if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
		apply(v)
	}
}

func setStringFlag(opts *runtimeFlags, flagName, value, key, envName string) {
	if !opts.seen[flagName] || value == "" {
		return
	}
	mustSetConfig(key, value)
	if envName != "" {
		_ = os.Setenv(envName, value)
	}
}

func mustSetConfig(key string, value any) {
	if setter, ok := g.Cfg().GetAdapter().(interface {
		Set(pattern string, value any) error
	}); ok {
		if err := setter.Set(key, value); err != nil {
			panic(fmt.Sprintf("set config %s: %v", key, err))
		}
		return
	}
	panic("active config adapter does not support Set")
}

func configHost(key string) string {
	host, _, err := splitAddress(g.Cfg().MustGet(context.Background(), key, "").String())
	if err != nil {
		return ""
	}
	return host
}

func configPort(key, def string) string {
	_, port, err := splitAddress(g.Cfg().MustGet(context.Background(), key, "").String())
	if err != nil || port == "" {
		return def
	}
	return port
}

func splitAddress(addr string) (string, string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", "", nil
	}
	host, port, err := net.SplitHostPort(addr)
	if err == nil {
		return host, port, nil
	}
	if strings.HasPrefix(addr, ":") {
		return "", strings.TrimPrefix(addr, ":"), nil
	}
	if !strings.Contains(addr, ":") {
		return addr, "", nil
	}
	return "", "", err
}

func joinHostPort(host, port string) string {
	if port == "" {
		return host
	}
	if host == "" {
		return ":" + port
	}
	return net.JoinHostPort(host, port)
}

func atoiOrString(v string) any {
	if n, err := strconv.Atoi(v); err == nil {
		return n
	}
	return v
}

func atobOrString(v string) any {
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	return v
}

func logRuntimeSummary() {
	ctx := context.Background()
	storageType := g.Cfg().MustGet(ctx, "etl.storage.type", "sqlite").String()
	role := g.Cfg().MustGet(ctx, "etl.role", "standalone").String()
	summary := map[string]any{
		"server_address": g.Cfg().MustGet(ctx, "server.address", ":8000").String(),
		"etl_address":    g.Cfg().MustGet(ctx, "etl.address", ":8001").String(),
		"role":           role,
		"storage":        storageType,
		"specs_dir":      g.Cfg().MustGet(ctx, "etl.specsDir", "./pipes").String(),
		"checkpoint_dir": g.Cfg().MustGet(ctx, "etl.checkpointDir", "./data/checkpoint").String(),
		"dlq_dir":        g.Cfg().MustGet(ctx, "etl.dlqDir", "./data/dlq").String(),
		"log_dir":        g.Cfg().MustGet(ctx, "logger.path", "./logs").String(),
		"plugins_dir":    g.Cfg().MustGet(ctx, "etl.pluginsDir", "./data/plugins").String(),
		"tls_enabled":    g.Cfg().MustGet(ctx, "etl.tls.cert", "").String() != "" && g.Cfg().MustGet(ctx, "etl.tls.key", "").String() != "",
		"api_auth":       g.Cfg().MustGet(ctx, "etl.apiToken", "").String() != "",
	}
	g.Log().Infof(ctx, "Runtime config: %+v", summary)
}

func runtimeHelpText() string {
	return `OpenETL-Go runtime

Usage:
  openetl-go [flags]

Configuration priority:
  CLI flags > environment variables > config.yaml > built-in defaults

Flags:
  --config PATH              Config file path.
  --data-dir DIR             Data directory; derives checkpoint, DLQ, SQLite, plugins, schemas.
  --log-dir DIR              Log directory. Env: ETL_LOG_DIR
  --plugins-dir DIR          Plugin WASM directory. Env: ETL_PLUGINS_DIR
  --schemas-dir DIR          Schema registry directory. Env: ETL_SCHEMAS_DIR
  --specs-dir DIR            Pipeline YAML specs directory. Env: ETL_SPECS_DIR
  --host HOST                Web/UI bind host. Env: ETL_HTTP_HOST
  --port PORT                Web/UI bind port. Env: ETL_HTTP_PORT
  --etl-api-host HOST        ETL API bind host. Env: ETL_API_HOST
  --etl-api-port PORT        ETL API bind port. Env: ETL_API_PORT
  --storage TYPE             Storage backend: sqlite, mysql, postgresql. Env: ETL_STORAGE_TYPE
  --storage-dsn DSN          MySQL/PostgreSQL storage DSN. Env: ETL_STORAGE_DSN
  --sqlite-path PATH         SQLite metadata DB path. Env: ETL_SQLITE_PATH
  --api-token TOKEN          ETL API token. Env: ETL_API_TOKEN. Sensitive.
  --tls-cert PATH            ETL API TLS certificate. Env: ETL_TLS_CERT
  --tls-key PATH             ETL API TLS key. Env: ETL_TLS_KEY. Sensitive path.
  --role ROLE                standalone, master, or worker. Env: ETL_ROLE
  --master-url URL           Master API URL for worker role. Env: ETL_MASTER_URL
  --worker-id ID             Worker identifier. Env: ETL_WORKER_ID
  --worker-slots N           Worker shard slots. Env: ETL_WORKER_SLOTS
  --audit-enabled BOOL       Enable SQL-backed audit logging. Env: ETL_AUDIT_ENABLED
  --logger-format FORMAT     text or json. Env: LOGGER_FORMAT
  --help, -h                 Show this help.

Examples:
  openetl-go --config /etc/openetl/config.yaml --port 8080 --etl-api-port 8081
  openetl-go --data-dir /var/lib/openetl --specs-dir /etc/openetl/pipes
  openetl-go --role master --storage mysql --storage-dsn 'user:pass@tcp(db:3306)/etl?parseTime=true'
  openetl-go --role worker --master-url http://openetl-master:8001 --worker-id worker-a
`
}
