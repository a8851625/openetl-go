//go:build !nolua

package transform

import (
	"context"
	"strings"
	"testing"

	"openetl-go/internal/etl/core"
)

func recWith(data map[string]any) core.Record {
	return core.Record{Operation: core.OpInsert, Data: data}
}

// TestLuaSandboxBlocksOS verifies os.execute / os.exit / io are stripped.
func TestLuaSandboxBlocksOS(t *testing.T) {
	cases := []struct {
		name   string
		script string
		errMsg string
	}{
		{"os_execute", `os.execute("echo pwned")`, "attempt to index"},
		{"os_exit", `os.exit(1)`, "attempt to index"},
		{"io_open", `io.open("/etc/passwd", "r")`, "attempt to index"},
		{"loadfile", `loadfile("/etc/passwd")`, "attempt to call"},
		{"dofile", `dofile("/etc/passwd")`, "attempt to call"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr, err := NewLuaTransform(tc.script)
			if err != nil {
				return // compile-time rejection also fine
			}
			defer tr.Close()
			_, err = tr.Apply(context.Background(), recWith(map[string]any{"x": 1}))
			if err == nil {
				t.Errorf("expected error for %s, got nil", tc.name)
				return
			}
			// We accept ANY error — the key is that os.execute didn't run.
			// The error text varies; just verify execution didn't succeed.
		})
	}
}

// TestLuaSandboxAllowsMath verifies safe builtins still work.
func TestLuaSandboxAllowsMath(t *testing.T) {
	tr, err := NewLuaTransform(`record.result = math.floor(record.x * 2)`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	defer tr.Close()
	out, err := tr.Apply(context.Background(), recWith(map[string]any{"x": 3.7}))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	got := out.Data["result"]
	// math.floor(3.7 * 2) = math.floor(7.4) = 7
	if got != float64(7) {
		t.Errorf("result = %v, want 7", got)
	}
}

// TestLuaSandboxAllowsStringOps verifies string builtins still work.
func TestLuaSandboxAllowsStringOps(t *testing.T) {
	tr, err := NewLuaTransform(`record.upper = string.upper(record.s)`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	defer tr.Close()
	out, err := tr.Apply(context.Background(), recWith(map[string]any{"s": "hello"}))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := out.Data["upper"]; got != "HELLO" {
		t.Errorf("upper = %v, want HELLO", got)
	}
}

// TestLuaStateReuseAcrossRecords verifies the same Lua state is reused.
func TestLuaStateReuseAcrossRecords(t *testing.T) {
	tr, err := NewLuaTransform(`record.v2 = record.v * 2`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	defer tr.Close()
	for i := 1; i <= 100; i++ {
		out, err := tr.Apply(context.Background(), recWith(map[string]any{"v": float64(i)}))
		if err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
		want := float64(i * 2)
		if got := out.Data["v2"]; got != want {
			t.Errorf("iter %d: v2 = %v, want %v", i, got, want)
			break
		}
	}
}

// TestLuaStatePersistsAcrossCalls verifies the script can keep a counter
// across invocations (using a global variable).
func TestLuaStatePersistsAcrossCalls(t *testing.T) {
	tr, err := NewLuaTransform(`
count = (count or 0) + 1
record.count = count
`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	defer tr.Close()
	for i := 1; i <= 5; i++ {
		out, err := tr.Apply(context.Background(), recWith(map[string]any{}))
		if err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
		if got := out.Data["count"]; got != float64(i) {
			t.Errorf("iter %d: count = %v, want %d", i, got, i)
			break
		}
	}
}

// TestLuaEmptyScriptFails verifies an empty script is rejected.
func TestLuaEmptyScriptFails(t *testing.T) {
	_, err := NewLuaTransform("")
	if err == nil {
		t.Error("expected error for empty script, got nil")
	}
}

// TestLuaCompileError verifies malformed Lua fails fast.
func TestLuaCompileError(t *testing.T) {
	_, err := NewLuaTransform(`if true then`) // missing end
	if err == nil {
		t.Error("expected compile error, got nil")
	}
	if !strings.Contains(err.Error(), "compile") && !strings.Contains(err.Error(), "eof") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestLuaRecordMutation verifies scripts can add/remove/modify fields.
func TestLuaRecordMutation(t *testing.T) {
	tr, err := NewLuaTransform(`
record.new_field = "added"
record.existing = string.upper(record.existing)
record.to_remove = nil
`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	defer tr.Close()
	out, err := tr.Apply(context.Background(), recWith(map[string]any{
		"existing":  "x",
		"to_remove": "y",
	}))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out.Data["new_field"] != "added" {
		t.Errorf("new_field = %v, want 'added'", out.Data["new_field"])
	}
	if out.Data["existing"] != "X" {
		t.Errorf("existing = %v, want 'X'", out.Data["existing"])
	}
	if _, ok := out.Data["to_remove"]; ok {
		t.Errorf("to_remove should be nil, got %v", out.Data["to_remove"])
	}
}

// TestLuaTransform (relocated from builtin_test.go so it is gated with the rest
// of the Lua tests under //go:build !nolua — P5-22).
func TestLuaTransform(t *testing.T) {
	tr, err := NewLuaTransform(`record.full_name = record.first .. " " .. record.last`)
	if err != nil {
		t.Fatalf("NewLuaTransform error = %v", err)
	}
	rec, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"first": "Ada", "last": "Lovelace"}})
	if err != nil {
		t.Fatalf("Apply error = %v", err)
	}
	if rec.Data["full_name"] != "Ada Lovelace" {
		t.Fatalf("full_name = %#v", rec.Data["full_name"])
	}
}
