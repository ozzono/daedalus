package activities

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeMirroredSource creates the worktree-side source dir with one regular
// file (the given mode) and one subdirectory carrying a nested file — the
// flat-files-by-convention shape mirrorToHost is specified against.
func writeMirroredSource(t *testing.T, worktreePath, relDir, content string, mode os.FileMode) string {
	t.Helper()
	srcDir := filepath.Join(worktreePath, relDir)
	if err := os.MkdirAll(filepath.Join(srcDir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "file-a.md"), []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "sub", "nested.md"), []byte("nested\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return srcDir
}

// TestMirrorToHostCopiesFiles pins the copy-on-first-see contract: each
// regular file directly under the source dir lands in the mirror under the
// same name, with content and mode preserved; subdirectories are skipped
// (bug files and dumps are flat by convention); the mirror dir is created
// if missing.
func TestMirrorToHostCopiesFiles(t *testing.T) {
	wt := t.TempDir()
	mirror := filepath.Join(t.TempDir(), "mirror") // not pre-created
	writeMirroredSource(t, wt, "backlog/bugs", "bug one\n", 0o600)

	mirrorToHost(context.Background(), wt, "backlog/bugs", mirror)

	data, err := os.ReadFile(filepath.Join(mirror, "file-a.md"))
	if err != nil {
		t.Fatalf("read mirrored file: %v", err)
	}
	if string(data) != "bug one\n" {
		t.Errorf("mirrored content = %q, want %q", data, "bug one\n")
	}
	info, err := os.Stat(filepath.Join(mirror, "file-a.md"))
	if err != nil {
		t.Fatalf("stat mirrored file: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("mirrored mode = %v, want the source's 0o600", got)
	}
	if _, err := os.Stat(filepath.Join(mirror, "sub")); !os.IsNotExist(err) {
		t.Errorf("mirror carries the subdirectory: %v", err)
	}
}

// TestMirrorToHostNewerWins pins the re-mirror decision: a worktree copy
// strictly newer than the mirror overwrites it (files are written and
// edited across rounds), while an older or same-mtime worktree copy leaves
// the existing mirror file untouched.
func TestMirrorToHostNewerWins(t *testing.T) {
	wt := t.TempDir()
	mirror := t.TempDir()
	srcDir := writeMirroredSource(t, wt, "backlog/bugs", "v1\n", 0o644)
	src := filepath.Join(srcDir, "file-a.md")
	dst := filepath.Join(mirror, "file-a.md")

	mirrorToHost(context.Background(), wt, "backlog/bugs", mirror)
	if data, err := os.ReadFile(dst); err != nil || string(data) != "v1\n" {
		t.Fatalf("first mirror: dst = %q (%v), want v1", data, err)
	}

	// Strictly newer: overwritten.
	newer := time.Now().Add(2 * time.Hour)
	if err := os.WriteFile(src, []byte("v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(src, newer, newer); err != nil {
		t.Fatal(err)
	}
	mirrorToHost(context.Background(), wt, "backlog/bugs", mirror)
	if data, err := os.ReadFile(dst); err != nil || string(data) != "v2\n" {
		t.Fatalf("newer worktree copy: dst = %q (%v), want v2", data, err)
	}

	// Older: the mirror copy survives. Write v3 but stamp it in the past,
	// before the mirror file's own mtime.
	older := time.Now().Add(-2 * time.Hour)
	if err := os.WriteFile(src, []byte("v3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(src, older, older); err != nil {
		t.Fatal(err)
	}
	mirrorToHost(context.Background(), wt, "backlog/bugs", mirror)
	if data, err := os.ReadFile(dst); err != nil || string(data) != "v2\n" {
		t.Errorf("older worktree copy: dst = %q (%v), want the mirror kept at v2", data, err)
	}

	// Same mtime as the mirror copy: also kept — only a strictly newer
	// worktree copy wins, so an equal stamp never triggers a rewrite.
	dstInfo, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, []byte("v4\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(src, dstInfo.ModTime(), dstInfo.ModTime()); err != nil {
		t.Fatal(err)
	}
	mirrorToHost(context.Background(), wt, "backlog/bugs", mirror)
	if data, err := os.ReadFile(dst); err != nil || string(data) != "v2\n" {
		t.Errorf("same-mtime worktree copy: dst = %q (%v), want the mirror kept at v2", data, err)
	}
}

// TestMirrorToHostNoopWhenOff pins the off switches: an empty relDir or an
// empty mirrorDir is a silent no-op (absent env means off), and a missing
// source dir — nothing filed yet — is silent too, leaving the mirror dir
// uncreated rather than materializing empty host directories.
func TestMirrorToHostNoopWhenOff(t *testing.T) {
	wt := t.TempDir()
	mirror := filepath.Join(t.TempDir(), "mirror")

	mirrorToHost(context.Background(), wt, "", mirror)
	mirrorToHost(context.Background(), wt, "backlog/bugs", "")
	mirrorToHost(context.Background(), wt, "backlog/bugs", mirror) // source dir never created

	if _, err := os.Stat(mirror); !os.IsNotExist(err) {
		t.Errorf("mirror dir exists after no-op runs: %v", err)
	}
}
