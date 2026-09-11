//go:build linux || darwin

// process-measure records a child process's resource usage without depending on
// differences between GNU time and BusyBox time's ru_maxrss unit conversion.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"time"
)

type measurement struct {
	ElapsedSeconds float64 `json:"elapsed_seconds"`
	PeakRSSBytes   int64   `json:"peak_rss_bytes"`
	NativeMaxRSS   int64   `json:"native_max_rss"`
	NativeRSSUnit  string  `json:"native_rss_unit"`
	Method         string  `json:"method"`
	Platform       string  `json:"platform"`
	ExitCode       int     `json:"exit_code"`
	Error          string  `json:"error,omitempty"`
}

func measure(command []string, stdout, stderr io.Writer) measurement {
	r := measurement{Method: "wait4.rusage.ru_maxrss", Platform: runtime.GOOS, ExitCode: 125}
	if len(command) == 0 {
		r.Error = "a child command is required"
		return r
	}
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, stdout, stderr
	started := time.Now()
	err := cmd.Run()
	r.ElapsedSeconds = time.Since(started).Seconds()
	if err != nil {
		r.Error = err.Error()
	}
	if cmd.ProcessState == nil {
		return r
	}
	r.ExitCode = cmd.ProcessState.ExitCode()
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		r.ExitCode = 128 + int(status.Signal())
	}
	usage := cmd.ProcessState.SysUsage().(*syscall.Rusage)
	r.NativeMaxRSS = usage.Maxrss
	r.PeakRSSBytes, r.NativeRSSUnit = usage.Maxrss, "bytes"
	if runtime.GOOS == "linux" {
		r.PeakRSSBytes, r.NativeRSSUnit = usage.Maxrss*1024, "KiB"
	}
	return r
}

func main() {
	output := flag.String("output", "", "resource JSON path (required)")
	flag.Parse()
	if *output == "" || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: process-measure --output file.json -- command [args...]")
		os.Exit(2)
	}
	r := measure(flag.Args(), os.Stdout, os.Stderr)
	f, err := os.OpenFile(*output, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err == nil {
		err = json.NewEncoder(f).Encode(r)
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(125)
	}
	os.Exit(r.ExitCode)
}
