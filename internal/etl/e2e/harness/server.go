//go:build e2e

package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// RepoRoot walks up from the current directory until it finds go.mod.
func RepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found above %s", dir)
		}
		dir = parent
	}
}

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

// BuildBinary compiles the repo root module once per process and returns the
// binary path. Subprocess mode (not container mode) is deliberate: crash
// recovery acceptance needs a real process lifecycle (SIGKILL/restart), and
// dropping the image build removes the `go mod download` blocker that stalled
// BUG-1/2/6 (plan.md route B).
func BuildBinary(ctx context.Context) (string, error) {
	buildOnce.Do(func() {
		root, err := RepoRoot()
		if err != nil {
			buildErr = err
			return
		}
		outDir, err := os.MkdirTemp("", "openetl-e2e-bin-")
		if err != nil {
			buildErr = err
			return
		}
		bin := filepath.Join(outDir, "openetl-go")
		cmd := exec.CommandContext(ctx, "go", "build", "-o", bin, ".")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			buildErr = fmt.Errorf("go build: %v: %s", err, stderr.String())
			return
		}
		binPath = bin
	})
	return binPath, buildErr
}

// Server is one tested openetl-go subprocess bound to an isolated data/specs
// directory and two loopback ports (GoFrame HTTP + ETL API).
type Server struct {
	BinPath  string
	WorkDir  string // clean cwd so the repo's manifest/config is not picked up
	DataDir  string
	SpecsDir string
	HTTPPort int
	APIPort  int
	logPath  string

	cmd *exec.Cmd
	log *os.File
}

// NewServer allocates an isolated workspace (clean cwd, fresh data dir, fresh
// specs dir) and two free loopback ports. ns only labels the temp directory.
func NewServer(ns string) (*Server, error) {
	bin, err := BuildBinary(context.Background())
	if err != nil {
		return nil, err
	}
	work, err := os.MkdirTemp("", "e2e-"+ns+"-")
	if err != nil {
		return nil, err
	}
	data := filepath.Join(work, "data")
	specs := filepath.Join(work, "pipes")
	if err := os.MkdirAll(data, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(specs, 0o755); err != nil {
		return nil, err
	}
	httpPort, err := freeTCPPort()
	if err != nil {
		return nil, err
	}
	apiPort, err := freeTCPPort()
	if err != nil {
		return nil, err
	}
	return &Server{
		BinPath:  bin,
		WorkDir:  work,
		DataDir:  data,
		SpecsDir: specs,
		HTTPPort: httpPort,
		APIPort:  apiPort,
		logPath:  filepath.Join(work, "server.log"),
	}, nil
}

func freeTCPPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// Start launches the binary and blocks until /api/v2/health answers 200.
func (s *Server) Start(ctx context.Context) error {
	log, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	s.log = log
	cmd := exec.Command(s.BinPath,
		"--data-dir", s.DataDir,
		"--specs-dir", s.SpecsDir,
		"--port", strconv.Itoa(s.HTTPPort),
		"--etl-api-port", strconv.Itoa(s.APIPort),
		"--audit-enabled", "false",
	)
	// Clean cwd: the binary must not silently read the repo's
	// manifest/config/config.yaml — flags above are the full contract.
	cmd.Dir = s.WorkDir
	cmd.Env = os.Environ()
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		log.Close()
		return fmt.Errorf("start %s: %w", s.BinPath, err)
	}
	s.cmd = cmd
	if err := s.waitHealthy(ctx, 90*time.Second); err != nil {
		return err
	}
	return nil
}

func (s *Server) waitHealthy(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.cmd != nil && s.cmd.ProcessState != nil {
			return fmt.Errorf("server exited early; log tail:\n%s", s.LogTail())
		}
		resp, err := http.Get(s.APIURL("/api/v2/health")) //nolint:gosec // fixed test endpoint
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("server not healthy after %s; log tail:\n%s", timeout, s.LogTail())
}

// LogTail returns the last 4KiB of the server log for failure diagnostics.
func (s *Server) LogTail() string {
	data, err := os.ReadFile(s.logPath)
	if err != nil {
		return ""
	}
	const tail = 4096
	if len(data) > tail {
		data = data[len(data)-tail:]
	}
	return string(data)
}

// APIURL builds a URL against the ETL API port.
func (s *Server) APIURL(path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", s.APIPort, path)
}

// Get performs GET APIURL(path) and returns the body; non-2xx is an error.
func (s *Server) Get(path string) ([]byte, error) {
	resp, err := http.Get(s.APIURL(path)) //nolint:gosec // fixed test endpoint
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, string(body))
	}
	return body, nil
}

// Post performs POST APIURL(path) and returns the body; non-2xx is an error.
func (s *Server) Post(path string) ([]byte, error) {
	resp, err := http.Post(s.APIURL(path), "application/json", nil) //nolint:gosec // fixed test endpoint
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, fmt.Errorf("POST %s: status %d: %s", path, resp.StatusCode, string(body))
	}
	return body, nil
}

// WaitPipelineRunning polls GET /api/v2/pipelines until the named pipeline
// reports status "running" (shell wait_pipeline_running). The endpoint may
// return a bare array or a wrapped object ("pipelines"/"data").
func (s *Server) WaitPipelineRunning(name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		body, err := s.Get("/api/v2/pipelines")
		if err == nil {
			list := pipelinesFromBody(body)
			for _, item := range list {
				if n, _ := item["name"].(string); n == name {
					if st, _ := item["status"].(string); st == "running" {
						return nil
					}
				}
			}
			last = string(body)
		} else {
			last = err.Error()
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("pipeline %q not running after %s; last body: %s", name, timeout, last)
}

func pipelinesFromBody(body []byte) []map[string]any {
	var arr []map[string]any
	if json.Unmarshal(body, &arr) == nil && arr != nil {
		return arr
	}
	var wrap struct {
		Pipelines []map[string]any `json:"pipelines"`
		Data      []map[string]any `json:"data"`
	}
	if json.Unmarshal(body, &wrap) == nil {
		if wrap.Pipelines != nil {
			return wrap.Pipelines
		}
		return wrap.Data
	}
	return nil
}

// Kill sends SIGKILL and reaps the process (shell `$CLI kill`).
func (s *Server) Kill() error {
	if s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	_ = s.cmd.Process.Kill()
	_ = s.cmd.Wait()
	s.cmd = nil
	if s.log != nil {
		_ = s.log.Close()
		s.log = nil
	}
	return nil
}

// Restart starts the process again with identical data/specs/ports so the
// run resumes from the persisted checkpoint.
func (s *Server) Restart(ctx context.Context) error {
	return s.Start(ctx)
}

// Close stops the process gracefully (SIGTERM, then SIGKILL after 5s) and
// closes the log. Safe to call multiple times; intended for t.Cleanup.
func (s *Server) Close() error {
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() {
			_ = s.cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = s.cmd.Process.Kill()
			<-done
		}
		s.cmd = nil
	}
	if s.log != nil {
		_ = s.log.Close()
		s.log = nil
	}
	return nil
}
