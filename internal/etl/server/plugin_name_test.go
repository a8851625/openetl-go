package server

import "testing"

// TestValidPluginName guards TF-3: plugin names are joined into filesystem
// paths (tmpDir/pluginsDir + name + ".wasm"), so path-traversal input must be
// rejected before it can escape the target directory.
func TestValidPluginName(t *testing.T) {
	cases := []struct {
		name string
		in   string
		wantErr bool
	}{
		{"empty", "", true},
		{"simple", "my-plugin", false},
		{"alnum underscore dot", "etl.transform_v2.1", false},
		{"traversal slash", "../../etc/passwd", true},
		{"leading dotdot", "../secret", true},
		{"absolute unix", "/etc/x", true},
		{"absolute windows", `C:\windows\x`, true},
		{"dotdot literal", "..", true},
		{"dotdot mid", "a/../b", true},
		{"space", "my plugin", true},
		{"shell meta", "x; rm -rf /", true},
		{"backtick", "a`b", true},
		{"too long", string(make([]byte, 129)), true}, // 129 zero-bytes, also invalid char
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := validPluginName(c.in)
			if c.wantErr && err == nil {
				t.Errorf("validPluginName(%q) = nil, want error", c.in)
			}
			if !c.wantErr && err != nil {
				t.Errorf("validPluginName(%q) = %v, want nil", c.in, err)
			}
		})
	}
}
