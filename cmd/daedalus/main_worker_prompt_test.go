package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMainWorkerForegroundPromptOverride pins the prompt-override fail-fast
// in `worker foreground`: a config naming an override directory that fails
// validation exits 1 with the override diagnostic — before runWorker, so a
// bad override can never get as far as a registered poller. The config names
// the directory relative to itself while the child's working directory is
// this package's dir, which holds no such folder: the stray-stem diagnostic
// proves the directory resolved against the config file's directory, not
// against the process's working directory.
func TestMainWorkerForegroundPromptOverride(t *testing.T) {
	dir := t.TempDir()
	overrides := filepath.Join(dir, "overrides")
	if err := os.MkdirAll(overrides, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(overrides, "dev-session.md"), []byte("no such prompt"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfg, []byte("agent: claude\nprompt: overrides\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runMainIn(t, "", "-c", cfg, "worker", "foreground")
	if code != 1 {
		t.Errorf("worker foreground with a bad override exit code = %d, want 1", code)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
	const want = "worker failed: prompt overrides: dev-session.md does not name a prompt"
	if !strings.HasPrefix(stderr, want) {
		t.Errorf("stderr = %q, want the override diagnostic (proving validation ran before any poller work), got it starting %q", stderr, want)
	}
}
