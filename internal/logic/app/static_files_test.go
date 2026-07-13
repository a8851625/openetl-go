package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStaticAssetPathNormalizesSPARoutes(t *testing.T) {
	if got := staticAssetPath("/"); got != "/index.html" {
		t.Fatalf("root path = %q, want /index.html", got)
	}
	if got := staticAssetPath("/pipelines/../assets/app.js"); got != "/assets/app.js" {
		t.Fatalf("cleaned path = %q, want /assets/app.js", got)
	}
}

func TestReadStaticAssetPrefersDiskResource(t *testing.T) {
	origWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	tmp := t.TempDir()
	t.Cleanup(func() { _ = os.Chdir(origWD) })
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join("resource", "public", "assets"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join("resource", "public", "assets", "app.js"), []byte("from-disk"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	content, ok := readStaticAsset("/assets/app.js")
	if !ok {
		t.Fatalf("readStaticAsset returned ok=false")
	}
	if string(content) != "from-disk" {
		t.Fatalf("content = %q, want disk content", string(content))
	}
}
