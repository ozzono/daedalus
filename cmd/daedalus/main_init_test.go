package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
)

// TestMainInitProfiles pins the profiled init end to end in a subprocess:
// each profile argument — and both together, which help.go documents as the
// full example — writes config-example.yaml as the base configuration plus
// that profile's slices, with the write receipt on stdout and nothing on
// stderr.
func TestMainInitProfiles(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"init", "prompt"}, config.ExampleYAMLFor(false, true)},
		{[]string{"init", "slim"}, config.ExampleYAMLFor(true, false)},
		{[]string{"init", "prompt", "slim"}, config.ExampleYAML},
	} {
		dir := t.TempDir()
		stdout, stderr, code := runMainIn(t, dir, c.args...)
		if code != 0 || stderr != "" {
			t.Fatalf("daedalus %v = (exit %d) stderr %q, want a clean run", c.args, code, stderr)
		}
		if want := "wrote config-example.yaml — copy to config.yaml and edit\n"; stdout != want {
			t.Errorf("daedalus %v stdout = %q, want %q", c.args, stdout, want)
		}
		data, err := os.ReadFile(filepath.Join(dir, "config-example.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if got := string(data); got != c.want {
			i := firstDiff(got, c.want)
			t.Errorf("daedalus %v wrote %d bytes, want the %d of config.ExampleYAMLFor (first difference at byte %d)", c.args, len(got), len(c.want), i)
		}
	}
}

// firstDiff returns the index of the first byte two strings differ at (their
// shared length when one is a prefix of the other), for failure messages.
func firstDiff(a, b string) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// TestMainInitRegenerates pins the overwrite asymmetry: a profiled init
// replaces whatever an earlier init wrote — the one init form that
// overwrites — while a bare init afterward still refuses, leaving the
// regenerated file intact.
func TestMainInitRegenerates(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "config-example.yaml")
	if err := os.WriteFile(stale, []byte("stale\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, stderr, code := runMainIn(t, dir, "init", "prompt"); code != 0 || stderr != "" {
		t.Fatalf("daedalus init prompt over an existing file = (exit %d) stderr %q, want a clean run", code, stderr)
	}
	data, err := os.ReadFile(stale)
	if err != nil {
		t.Fatal(err)
	}
	if want := config.ExampleYAMLFor(false, true); string(data) != want {
		t.Errorf("profiled init left %d bytes, want it replaced by the %d of the prompt profile", len(data), len(want))
	}

	stdout, stderr, code := runMainIn(t, dir, "init")
	if code != 1 {
		t.Errorf("bare init after a profiled one exit code = %d, want 1", code)
	}
	if stdout != "" {
		t.Errorf("bare init stdout = %q, want empty", stdout)
	}
	if !strings.HasPrefix(stderr, "init failed: ") || !strings.Contains(stderr, "already exists") {
		t.Errorf("bare init stderr = %q, want the init failure naming the existing file", stderr)
	}
	data, err = os.ReadFile(stale)
	if err != nil {
		t.Fatal(err)
	}
	if want := config.ExampleYAMLFor(false, true); string(data) != want {
		t.Error("the refused bare init should leave the profiled file untouched")
	}
}

// TestMainInitProfileUsage pins the profile arguments' usage failures:
// an unknown or repeated profile is the standard usage failure — diagnostic,
// blank line, usage text, exit 1 — with no file written.
func TestMainInitProfileUsage(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{
			args: []string{"init", "frobnicate"},
			want: fmt.Sprintf("unknown init profile %q (available: %s)", "frobnicate", strings.Join(initProfiles, ", ")),
		},
		{
			args: []string{"init", "prompt", "prompt"},
			want: fmt.Sprintf("init got profile %q twice", "prompt"),
		},
		{
			args: []string{"init", "slim", "slim"},
			want: fmt.Sprintf("init got profile %q twice", "slim"),
		},
	} {
		dir := t.TempDir()
		stdout, stderr, code := runMainIn(t, dir, c.args...)
		if code != 1 {
			t.Errorf("daedalus %v exit code = %d, want 1", c.args, code)
		}
		if stdout != "" {
			t.Errorf("daedalus %v stdout = %q, want empty", c.args, stdout)
		}
		if !strings.HasPrefix(stderr, c.want+"\n\n") || !strings.HasSuffix(stderr, usage) {
			t.Errorf("daedalus %v stderr = %q, want %q then the usage text", c.args, stderr, c.want)
		}
		if _, err := os.Stat(filepath.Join(dir, "config-example.yaml")); !os.IsNotExist(err) {
			t.Errorf("daedalus %v should write nothing, found config-example.yaml (stat err %v)", c.args, err)
		}
	}
}
