package app

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fatih/color"
	"github.com/gogf/gf/v2/os/glog"
)

// TestConfigureStructuredLoggingWritesFileAndStdout verifies that the JSON
// logging handler persists structured logs to the rotating file under
// config.Path (regression for SV-6: the previous implementation wrote only to
// os.Stdout, dropping all logs when LOGGER_FORMAT=json was set and the
// container restarted).
func TestConfigureStructuredLoggingWritesFileAndStdout(t *testing.T) {
	t.Setenv("LOGGER_FORMAT", "json")

	// Capture stdout. doFinalPrint prints via fmt.Fprint(color.Output,...)
	// which is github.com/fatih/color's Output var (defaults to stdout).
	// Redirect both os.Stdout and color.Output for the duration of the test.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	origStdout := os.Stdout
	os.Stdout = w
	color.Output = w
	defer func() { os.Stdout = origStdout }()

	// Build an isolated logger writing into a temp dir.
	tmpDir := t.TempDir()
	logger := glog.New()
	logger.SetPath(tmpDir)
	logger.SetStdoutPrint(true)
	// Force the same JSON handler the production code installs.
	ConfigureStructuredLogging()

	ctx := context.Background()
	logger.Infof(ctx, "hello world %d", 42)

	// Flush the pipe reader.
	_ = w.Close()
	scanner := bufio.NewScanner(r)
	stdoutJSON := map[string]any{}
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, `"msg"`) {
			_ = json.Unmarshal([]byte(line), &stdoutJSON)
			break
		}
	}

	// Verify file output: find the daily log file under tmpDir.
	matches, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Glob log files: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("no log file created under %s — JSON handler bypassed glog file output", tmpDir)
	}
	fileBytes, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("ReadFile %s: %v", matches[0], err)
	}
	fileJSON := map[string]any{}
	if err := json.Unmarshal(fileBytes, &fileJSON); err != nil {
		t.Fatalf("log file is not valid JSON: %v (content=%q)", err, string(fileBytes))
	}

	if got := fileJSON["msg"]; got != "hello world 42" {
		t.Fatalf("file msg = %v, want %q", got, "hello world 42")
	}
	if got := fileJSON["level"]; got != "info" {
		t.Fatalf("file level = %v, want info", got)
	}
	if got := stdoutJSON["msg"]; got != "hello world 42" {
		t.Fatalf("stdout msg = %v, want %q (stdout must also receive JSON)", got, "hello world 42")
	}
}
