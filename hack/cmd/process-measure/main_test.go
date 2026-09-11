//go:build linux || darwin

package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func TestChildMemoryProbe(t *testing.T) {
	if os.Getenv("OPENETL_MEASURE_TEST_CHILD") != "1" {
		return
	}
	allocation := make([]byte, 16<<20)
	for i := range allocation {
		allocation[i] = byte(i)
	}
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		os.Exit(3)
	}
	fmt.Println(usage.Maxrss)
	runtime.KeepAlive(allocation)
	os.Exit(0)
}

func TestKernelPeakRSSUnitMatchesChildSelfReport(t *testing.T) {
	t.Setenv("OPENETL_MEASURE_TEST_CHILD", "1")
	var output bytes.Buffer
	r := measure([]string{os.Args[0], "-test.run=^TestChildMemoryProbe$"}, &output, io.Discard)
	if r.ExitCode != 0 || r.Error != "" {
		t.Fatalf("child measurement failed: %+v", r)
	}
	native, err := strconv.ParseInt(strings.TrimSpace(output.String()), 10, 64)
	if err != nil || native <= 0 {
		t.Fatalf("invalid self-report %q: %v", output.String(), err)
	}
	expectedBytes := native
	if runtime.GOOS == "linux" {
		expectedBytes *= 1024
	}
	// Small exit/printing allocations may raise the final peak. A page/KB
	// conversion error (the BusyBox observation was ~4x) must fail this check.
	if r.PeakRSSBytes < expectedBytes*8/10 || r.PeakRSSBytes > expectedBytes*13/10 {
		t.Fatalf("wait4 bytes %d disagree with child bytes %d", r.PeakRSSBytes, expectedBytes)
	}
}

func TestChildFailureAndMissingExecutableRemainFailures(t *testing.T) {
	r := measure([]string{"/bin/sh", "-c", "exit 7"}, io.Discard, io.Discard)
	if r.ExitCode != 7 || r.Error == "" || r.ElapsedSeconds <= 0 {
		t.Fatalf("child exit status lost: %+v", r)
	}
	r = measure([]string{"/nonexistent/openetl-measure-child"}, io.Discard, io.Discard)
	if r.ExitCode != 125 || r.Error == "" {
		t.Fatalf("exec failure lost: %+v", r)
	}
}
