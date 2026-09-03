//go:build e2e

package harness

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
)

// Dependency container images and credentials mirror docker-compose.dev.yml
// (mysql-source + clickhouse) so migrated path tests assert the same stack
// the shell scripts certified.
const (
	MySQLImage      = "docker.io/library/mysql:8.0"
	ClickHouseImage = "docker.io/clickhouse/clickhouse-server:24.3" // non-alpine: alpine entrypoint hangs after stop/start on CI runners

	MySQLRootPassword = "root123456"
	MySQLSyncUser     = "sync_user"
	MySQLSyncPassword = "sync_password_123"
	CHUser            = "default"
	CHPassword        = "dzh123456"

	// PodmanHint documents the rootless podman setup for local runs
	// (T1.3 acceptance 3); CI runners use the Docker API directly.
	PodmanHint = "for podman rootless: export DOCKER_HOST=unix://<podman machine API socket> " +
		"(`podman machine inspect` -> ConnectionInfo.PodmanSocket) and TESTCONTAINERS_RYUK_DISABLED=true"
)

// PollUntil runs fn every `every` until it returns true or the timeout lapses.
func PollUntil(ctx context.Context, timeout, every time.Duration, describe string, fn func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(every):
		}
	}
	return fmt.Errorf("timeout after %s waiting for %s", timeout, describe)
}

func runtimeHint(err error) string {
	msg := err.Error()
	if strings.Contains(msg, "Cannot connect to the Docker daemon") ||
		strings.Contains(msg, "docker daemon") ||
		strings.Contains(msg, "no such file or directory") && strings.Contains(msg, ".sock") {
		return PodmanHint
	}
	return ""
}

// ---------------------------------------------------------------------------
// MySQL (binlog-enabled source + batch target, shared across the suite)
// ---------------------------------------------------------------------------

// MySQLInstance is the shared, lazily started MySQL 8.0 container with
// ROW binlog + GTID enabled (mirrors docker-compose.dev.yml mysql-source).
type MySQLInstance struct {
	Container testcontainers.Container
	Host      string
	Port      int
	ctx       context.Context
}

var (
	mysqlOnce sync.Once
	mysqlInst *MySQLInstance
	mysqlErr  error
)

// MySQL starts (once per process) and returns the shared MySQL container.
// On any failure the error carries the podman hint so local skips are
// actionable.
func MySQL(ctx context.Context) (*MySQLInstance, error) {
	mysqlOnce.Do(func() {
		req := testcontainers.ContainerRequest{
			Image: MySQLImage,
			Env: map[string]string{
				"MYSQL_ROOT_PASSWORD": MySQLRootPassword,
				"MYSQL_DATABASE":      "dzh3136_go",
				"MYSQL_USER":          MySQLSyncUser,
				"MYSQL_PASSWORD":      MySQLSyncPassword,
				"TZ":                  "Asia/Shanghai",
			},
			Cmd: []string{
				"--server-id=1",
				"--log-bin=mysql-bin",
				"--binlog-format=ROW",
				"--binlog-row-image=FULL",
				"--gtid-mode=ON",
				"--enforce-gtid-consistency=ON",
				"--binlog-expire-logs-seconds=604800",
				"--default-authentication-plugin=mysql_native_password",
			},
			ExposedPorts: []string{"3306/tcp"},
		}
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			mysqlErr = fmt.Errorf("start %s: %w %s", MySQLImage, err, runtimeHint(err))
			return
		}
		inst := &MySQLInstance{Container: c, ctx: ctx}
		// Probe over TCP as sync_user: during the entrypoint's init phase a
		// temporary server runs with skip-networking, so socket-based pings
		// can succeed before the real server (and the MySQL-created user)
		// exist — grants issued in that window fail with exit 1.
		if err := PollUntil(ctx, 3*time.Minute, 2*time.Second, "mysql ready (tcp, sync_user)", func() bool {
			code, _, err := c.Exec(ctx, []string{"mysql", "-u" + MySQLSyncUser, "-p" + MySQLSyncPassword, "-h127.0.0.1", "-e", "SELECT 1"})
			return err == nil && code == 0
		}); err != nil {
			mysqlErr = err
			return
		}
		host, err := c.Host(ctx)
		if err != nil {
			mysqlErr = err
			return
		}
		mport, err := c.MappedPort(ctx, nat.Port("3306/tcp"))
		if err != nil {
			mysqlErr = err
			return
		}
		inst.Host = host
		inst.Port = mport.Int()
		// Mirror testdata/mysql/init/01-init.sql grants so per-test databases
		// work for the sync_user CDC/batch identity.
		if err := inst.Exec("GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '" +
			MySQLSyncUser + "'@'%'; FLUSH PRIVILEGES;"); err != nil {
			mysqlErr = fmt.Errorf("grant replication privileges: %w", err)
			return
		}
		mysqlInst = inst
	})
	return mysqlInst, mysqlErr
}

// execOutput carries demultiplexed exec streams.
type execOutput struct {
	stdout string
	stderr string
}

// SetContainerPaused freezes (pause=true) or resumes (pause=false) a
// container via the container CLI's cgroup freezer (docker pause / podman
// pause). This simulates an outage WITHOUT restarting the process: a CH
// process restart crashes on GitHub runners (DB::CgroupsMemoryUsageObserver
// cannot read the runner cgroup), so stop/start cannot be used there.
func SetContainerPaused(ctx context.Context, c testcontainers.Container, paused bool) error {
	cli := os.Getenv("CONTAINER_CLI")
	if cli == "" {
		cli = "docker"
		if _, err := exec.LookPath(cli); err != nil {
			cli = "podman"
		}
	}
	if _, err := exec.LookPath(cli); err != nil {
		return fmt.Errorf("container CLI %q not found (need docker or podman)", cli)
	}
	sub := "unpause"
	if paused {
		sub = "pause"
	}
	out, err := exec.CommandContext(ctx, cli, sub, c.GetContainerID()).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w (%s)", cli, sub, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// execResult runs argv inside the container and returns demultiplexed output
// + exit code. Container exec attach streams are 8-byte-framed
// (stdout/stderr multiplexed), so they must go through stdcopy — reading the
// reader raw yields frame headers interleaved with the payload.
func execResult(ctx context.Context, c testcontainers.Container, argv []string) (execOutput, int, error) {
	code, r, err := c.Exec(ctx, argv)
	if err != nil {
		return execOutput{}, code, err
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, r); err != nil {
		// Non-multiplexed (tty) stream fallback: read raw into stdout.
		var raw bytes.Buffer
		if _, rerr := io.Copy(&raw, r); rerr != nil {
			return execOutput{}, code, rerr
		}
		return execOutput{stdout: raw.String()}, code, nil
	}
	return execOutput{stdout: stdout.String(), stderr: stderr.String()}, code, nil
}

// Exec runs one SQL statement (or ;-joined batch) as root inside the container.
func (m *MySQLInstance) Exec(sql string) error {
	out, code, err := execResult(m.ctx, m.Container, []string{"mysql", "-uroot", "-p" + MySQLRootPassword, "-e", sql})
	if err != nil {
		return fmt.Errorf("mysql exec: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("mysql exec exit %d: stdout=%q stderr=%q", code, strings.TrimSpace(out.stdout), strings.TrimSpace(out.stderr))
	}
	return nil
}

// QueryValue runs the statement and returns the first cell, mirroring the
// shell helper `mysql -N -B` (result on stdout, warnings on stderr).
func (m *MySQLInstance) QueryValue(sql string) (string, error) {
	out, code, err := execResult(m.ctx, m.Container, []string{"mysql", "-N", "-B", "-uroot", "-p" + MySQLRootPassword, "-e", sql})
	if err != nil {
		return "", fmt.Errorf("mysql query: %w", err)
	}
	if code != 0 {
		return "", fmt.Errorf("mysql query exit %d: stdout=%q stderr=%q", code, strings.TrimSpace(out.stdout), strings.TrimSpace(out.stderr))
	}
	return strings.TrimSpace(out.stdout), nil
}

// WaitValue polls QueryValue until it equals want (shell wait_mysql_value).
func (m *MySQLInstance) WaitValue(sql, want string, timeout time.Duration) error {
	var got string
	err := PollUntil(m.ctx, timeout, time.Second, "sql result "+sql, func() bool {
		v, err := m.QueryValue(sql)
		if err != nil {
			return false
		}
		got = v
		return v == want
	})
	if err != nil {
		return fmt.Errorf("timeout waiting for SQL result: %s (got=%q want=%q)", sql, got, want)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ClickHouse (ReplacingMergeTree sink, shared across the suite)
// ---------------------------------------------------------------------------

// ClickHouseInstance is the shared ClickHouse 24.3 container.
type ClickHouseInstance struct {
	Container  testcontainers.Container
	Host       string
	NativePort int // 9000 native protocol (sink writes)
	HTTPPort   int // 8123 HTTP (health ping)
	ctx        context.Context
}

var (
	chOnce sync.Once
	chInst *ClickHouseInstance
	chErr  error
)

// ClickHouse starts (once per process) and returns the shared container.
func ClickHouse(ctx context.Context) (*ClickHouseInstance, error) {
	chOnce.Do(func() {
		req := testcontainers.ContainerRequest{
			Image: ClickHouseImage,
			Env: map[string]string{
				"CLICKHOUSE_USER":                      CHUser,
				"CLICKHOUSE_PASSWORD":                  CHPassword,
				"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
				"TZ":                                   "Asia/Shanghai",
			},
			ExposedPorts: []string{"8123/tcp", "9000/tcp"},
		}
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			chErr = fmt.Errorf("start %s: %w %s", ClickHouseImage, err, runtimeHint(err))
			return
		}
		inst := &ClickHouseInstance{Container: c, ctx: ctx}
		host, err := c.Host(ctx)
		if err != nil {
			chErr = err
			return
		}
		httpPort, err := c.MappedPort(ctx, nat.Port("8123/tcp"))
		if err != nil {
			chErr = err
			return
		}
		nativePort, err := c.MappedPort(ctx, nat.Port("9000/tcp"))
		if err != nil {
			chErr = err
			return
		}
		inst.Host = host
		inst.HTTPPort = httpPort.Int()
		inst.NativePort = nativePort.Int()
		url := fmt.Sprintf("http://%s:%d/ping", hostForHTTP(host), inst.HTTPPort)
		if err := PollUntil(ctx, 3*time.Minute, time.Second, "clickhouse ping", func() bool {
			resp, err := http.Get(url) //nolint:gosec // fixed test endpoint
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			return resp.StatusCode == http.StatusOK
		}); err != nil {
			chErr = err
			return
		}
		chInst = inst
	})
	return chInst, chErr
}

// hostForHTTP maps the docker host to a loopback-safe hostname for HTTP pings.
func hostForHTTP(host string) string {
	if host == "" {
		return "127.0.0.1"
	}
	return host
}

// TailErrLog returns the tail of the server error log (the container
// stdout only shows entrypoint messages; the real server error goes to
// /var/log/clickhouse-server/clickhouse-server.err.log).
func (c *ClickHouseInstance) TailErrLog() string {
	out, _, err := execResult(c.ctx, c.Container, []string{"sh", "-c", "tail -80 /var/log/clickhouse-server/clickhouse-server.err.log 2>/dev/null || true"})
	if err != nil {
		return "<errlog unavailable: " + err.Error() + ">"
	}
	if strings.TrimSpace(out.stdout) == "" && strings.TrimSpace(out.stderr) == "" {
		return "<errlog empty>"
	}
	return strings.TrimSpace(out.stdout + "\n" + out.stderr)
}

// Exec runs one clickhouse-client query inside the container.
func (c *ClickHouseInstance) Exec(sql string) error {
	out, code, err := execResult(c.ctx, c.Container, []string{"clickhouse-client", "--password", CHPassword, "--query", sql})
	if err != nil {
		return fmt.Errorf("clickhouse exec: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("clickhouse exec exit %d: %s%s", code, strings.TrimSpace(out.stdout), strings.TrimSpace(out.stderr))
	}
	return nil
}

// QueryValue runs the query and returns the trimmed first cell.
func (c *ClickHouseInstance) QueryValue(sql string) (string, error) {
	out, code, err := execResult(c.ctx, c.Container, []string{"clickhouse-client", "--password", CHPassword, "--query", sql})
	if err != nil {
		return "", fmt.Errorf("clickhouse query: %w", err)
	}
	if code != 0 {
		return "", fmt.Errorf("clickhouse query exit %d: %s%s", code, strings.TrimSpace(out.stdout), strings.TrimSpace(out.stderr))
	}
	return strings.TrimSpace(out.stdout), nil
}

// WaitValue polls QueryValue until it equals want (shell wait_ch_value).
func (c *ClickHouseInstance) WaitValue(sql, want string, timeout time.Duration) error {
	var got string
	err := PollUntil(c.ctx, timeout, time.Second, "query result "+sql, func() bool {
		v, err := c.QueryValue(sql)
		if err != nil {
			return false
		}
		got = v
		return v == want
	})
	if err != nil {
		return fmt.Errorf("query did not reach expected value: %s (got=%q want=%q)", sql, got, want)
	}
	return nil
}

// Ping reports whether the ClickHouse HTTP endpoint currently answers.
func (c *ClickHouseInstance) Ping() bool {
	url := fmt.Sprintf("http://%s:%d/ping", hostForHTTP(c.Host), c.HTTPPort)
	resp, err := http.Get(url) //nolint:gosec // fixed test endpoint
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// ---------------------------------------------------------------------------
// Shared lifecycle
// ---------------------------------------------------------------------------

// CloseAll terminates every shared dependency container started by this
// process. Called from TestMain; with TESTCONTAINERS_RYUK_DISABLED=true the
// harness owns cleanup (no reaper container is spawned).
func CloseAll(ctx context.Context) {
	if mysqlInst != nil && mysqlInst.Container != nil {
		_ = mysqlInst.Container.Terminate(ctx)
		mysqlInst = nil
	}
	if chInst != nil && chInst.Container != nil {
		_ = chInst.Container.Terminate(ctx)
		chInst = nil
	}
}
