package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestConfigCommand pins `daedalus config` against an explicit -c: the
// resolved path in the "# config:" header, then the file's own pre-defaults
// view as YAML — set values verbatim (durations as duration strings, keys
// unredacted), absent fields empty rather than filled with defaults.
func TestConfigCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	doc := "agent: pi\ntests_timeout: 45m\nanthropic:\n  key: sk-secret\n"
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runMainIn(t, "", "-c", path, "config")
	if code != 0 {
		t.Fatalf("daedalus config exit code = %d, want 0 (stderr: %q)", code, stderr)
	}
	if stderr != "" {
		t.Errorf("daedalus config stderr = %q, want empty", stderr)
	}
	if want := "# config: " + path + "\n"; !strings.HasPrefix(stdout, want) {
		t.Errorf("stdout should start with %q, got %q", want, stdout)
	}
	for _, want := range []string{
		"agent: pi\n",            // set value verbatim
		"tests_timeout: 45m0s\n", // duration string, not nanoseconds
		"key: sk-secret\n",       // keys print unredacted
		`branch_prefix: ""`,      // absent fields stay empty — no defaults injected
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout is missing %q:\n%s", want, stdout)
		}
	}
}

// TestConfigCommandResolvesDefaultSource pins the no-flag invocation: the
// command resolves the usual source chain itself (here ./config.yaml) and
// names the winner in the "# config:" header.
func TestConfigCommandResolvesDefaultSource(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	workdir := t.TempDir()
	if err := os.WriteFile(filepath.Join(workdir, "config.yaml"), []byte("agent: pi\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(workdir)

	stdout, stderr, code := runMainIn(t, "", "config")
	if code != 0 {
		t.Fatalf("daedalus config exit code = %d, want 0 (stderr: %q)", code, stderr)
	}
	if want := "# config: " + filepath.Join(workdir, "config.yaml") + "\n"; !strings.HasPrefix(stdout, want) {
		t.Errorf("stdout should start with %q, got %q", want, stdout)
	}
	if !strings.Contains(stdout, "agent: pi\n") {
		t.Errorf("stdout should render the resolved config's values, got %q", stdout)
	}
}

// TestConfigCommandUsageFail pins the argument rejection: anything after the
// subcommand is a usage error — message, blank line, usage text, exit 1.
func TestConfigCommandUsageFail(t *testing.T) {
	stdout, stderr, code := runMainIn(t, "", "-c", validConfig(t), "config", "extra")
	if code != 1 {
		t.Errorf("daedalus config extra exit code = %d, want 1", code)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
	if !strings.HasPrefix(stderr, "config takes no arguments\n\n") {
		t.Errorf("stderr should start with the argument diagnostic followed by a blank line, got %q", stderr)
	}
	if !strings.HasSuffix(stderr, usage) {
		t.Errorf("stderr should end with the usage text, got %q", stderr)
	}
}

// TestConfigCommandBadConfig pins the load-failure path: a config that fails
// read, parse, or validation produces the "load config:" diagnostic and
// exit 1 — that error is the command's answer for a broken config, not a
// crash — with no usage text.
func TestConfigCommandBadConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		doc     string
		wantErr string
	}{
		{"unknown agent", "agent: cursor\n", "unknown agent"},
		{"invalid yaml", "temporal: [broken", "parse config"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.doc), 0o644); err != nil {
				t.Fatal(err)
			}
			stdout, stderr, code := runMainIn(t, "", "-c", path, "config")
			if code != 1 {
				t.Errorf("exit code = %d, want 1", code)
			}
			if stdout != "" {
				t.Errorf("stdout = %q, want empty", stdout)
			}
			if !strings.HasPrefix(stderr, "load config: ") {
				t.Errorf("stderr should start with the load config diagnostic, got %q", stderr)
			}
			if !strings.Contains(stderr, tc.wantErr) {
				t.Errorf("stderr should contain %q, got %q", tc.wantErr, stderr)
			}
			if strings.Contains(stderr, "Usage:") {
				t.Errorf("stderr should not contain the usage text, got %q", stderr)
			}
		})
	}

	stdout, stderr, code := runMainIn(t, "", "-c", filepath.Join(t.TempDir(), "absent.yaml"), "config")
	if code != 1 {
		t.Errorf("daedalus config (missing file) exit code = %d, want 1", code)
	}
	if stdout != "" || !strings.HasPrefix(stderr, "load config: ") {
		t.Errorf("stdout %q stderr %q, want empty stdout and the load config diagnostic", stdout, stderr)
	}
}
