package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestWireGateRunWiring pins the launch gate end to end, in a subprocess: a
// `run` whose effective agent cannot take the config's only serving endpoint
// fails at launch naming the agent and the wire, before any submission side
// effect — the gate sits ahead of the repo-path resolution a passing launch
// would die on instead. The --cli override is the effective agent at the
// gate, and an agent the served wire can take is never refused.
func TestWireGateRunWiring(t *testing.T) {
	missingRepo := filepath.Join(t.TempDir(), "no-such-repo")

	t.Run("an openai-only config refuses the default agent", func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(cfg, []byte("agent: claude\nopenai:\n  url: https://o.example/v1\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		stdout, stderr, code := runMainIn(t, "", "-c", cfg, "run", missingRepo, "42", "fix the parser")
		if code != 1 {
			t.Errorf("daedalus run (openai-only config, agent claude) exit code = %d, want 1", code)
		}
		if stdout != "" {
			t.Errorf("stdout = %q, want empty", stdout)
		}
		if !strings.Contains(stderr, `run failed: agent "claude"`) || !strings.Contains(stderr, "OpenAI-wire") {
			t.Errorf("stderr should be the wire gate's refusal naming the agent and the wire, got %q", stderr)
		}
		if strings.Contains(stderr, "repo path") {
			t.Errorf("stderr = %q, want the gate to fire before any submission side effect", stderr)
		}
	})

	t.Run("--cli override is the effective agent at the gate", func(t *testing.T) {
		// An anthropic-only config serves the configured claude — but the
		// -cli opencode override cannot take that wire, and the gate answers
		// to the override.
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(cfg, []byte("agent: claude\nanthropic:\n  url: https://a.example\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		_, stderr, code := runMainIn(t, "", "-c", cfg, "-cli", "opencode", "run", missingRepo, "42", "fix the parser")
		if code != 1 {
			t.Errorf("daedalus run -cli opencode (anthropic-only config) exit code = %d, want 1", code)
		}
		if !strings.Contains(stderr, `run failed: agent "opencode"`) {
			t.Errorf("stderr should be the wire gate's refusal naming the overridden agent, got %q", stderr)
		}
	})

	t.Run("an agent the served wire takes is never refused", func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(cfg, []byte("agent: opencode\nopenai:\n  url: https://o.example/v1\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		_, stderr, code := runMainIn(t, "", "-c", cfg, "run", missingRepo, "42", "fix the parser")
		if code != 1 {
			t.Errorf("daedalus run (openai-only config, agent opencode) exit code = %d, want 1", code)
		}
		if !strings.HasPrefix(stderr, "run failed: repo path ") {
			t.Errorf("stderr should be the repo-path failure the served launch reaches, got %q", stderr)
		}
		if strings.Contains(stderr, "cannot be served") {
			t.Errorf("stderr = %q, want no wire-gate refusal for a served agent", stderr)
		}
	})
}
