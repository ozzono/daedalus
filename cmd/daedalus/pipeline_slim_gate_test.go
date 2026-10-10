package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/shrink"
)

// hugeInput is a task input whose biased-high estimate dwarfs its strict
// count (a single 40000-character run of 'a' counts 5000 o200k_base tokens
// but estimates ceil(40000/4) = 10000), so it separates the two arms of the
// gate's max.
const hugeInputLen = 40000

func hugeInput() string { return strings.Repeat("a", hugeInputLen) }

// TestSlimInputGateShipsShrunkText pins the gate's pass path: an
// under-cap input comes back as the shrunken text — never the original —
// since the shrunken text is what the workflow embeds.
func TestSlimInputGateShipsShrunkText(t *testing.T) {
	in := "In order to fix the parser, utilize the harness. Do not skip the suite."
	got, err := slimInputGate(in, config.Config{})
	if err != nil {
		t.Fatalf("slimInputGate: %v", err)
	}
	if got != shrink.Task(in) {
		t.Errorf("slimInputGate = %q, want the shrunk text %q", got, shrink.Task(in))
	}
	if got == in {
		t.Error("slimInputGate returned the original input, want the shrunk text the workflow embeds")
	}
}

// TestSlimInputGateRefusesOverCap pins the fail path: over the default cap
// the run never starts, and the error carries the count, the cap that
// refused it, and the split-the-task guidance.
func TestSlimInputGateRefusesOverCap(t *testing.T) {
	_, err := slimInputGate(hugeInput(), config.Config{})
	if err == nil {
		t.Fatal("slimInputGate(40000 'a's) = nil error, want the default-cap refusal")
	}
	for _, want := range []string{"slim input too large", "4098", "split the work"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("slimInputGate error %q should name %q", err, want)
		}
	}
}

// TestSlimInputGateHonorsConfiguredCap pins the config knob: a raised
// ceiling admits what the default refuses, a lowered one refuses ordinary
// input, and the error names the configured ceiling.
func TestSlimInputGateHonorsConfiguredCap(t *testing.T) {
	raised := config.Config{}
	raised.Slim.MaxInputTokens = 1 << 20
	if _, err := slimInputGate(hugeInput(), raised); err != nil {
		t.Errorf("slimInputGate with a raised cap: %v", err)
	}

	lowered := config.Config{}
	lowered.Slim.MaxInputTokens = 8
	_, err := slimInputGate(strings.Repeat("utilize the parser. ", 50), lowered)
	if err == nil {
		t.Fatal("slimInputGate with an 8-token cap = nil error, want a refusal")
	}
	if !strings.Contains(err.Error(), "8-token slim cap") {
		t.Errorf("slimInputGate error %q should name the configured cap", err)
	}
}

// TestSlimInputGateUsesBiasedHighEstimate pins the max: the gate answers to
// the higher of the strict o200k_base count and the chars/4 estimate, so it
// cannot under-count against the arithmetic that governs the wire budget.
// The 40000-'a' input counts 5000 tokens — under this cap — but estimates
// 10000, and the refusal must say 10000.
func TestSlimInputGateUsesBiasedHighEstimate(t *testing.T) {
	cfg := config.Config{}
	cfg.Slim.MaxInputTokens = 6000
	_, err := slimInputGate(hugeInput(), cfg)
	if err == nil {
		t.Fatal("slimInputGate(count 5000, estimate 10000, cap 6000) = nil error, want the estimate to refuse it")
	}
	if !strings.Contains(err.Error(), "10000 tokens") {
		t.Errorf("slimInputGate error %q should carry the estimate, not the strict count", err)
	}
}

// TestSlimInputGateCapIsInclusive pins the boundary: an input whose gate
// number equals the cap passes (32 'a's: count 4, estimate 8, cap 8); one
// more character pushes the estimate to 9 and refuses.
func TestSlimInputGateCapIsInclusive(t *testing.T) {
	cfg := config.Config{}
	cfg.Slim.MaxInputTokens = 8

	if _, err := slimInputGate(strings.Repeat("a", 32), cfg); err != nil {
		t.Errorf("slimInputGate(32 'a's, cap 8): %v", err)
	}
	if _, err := slimInputGate(strings.Repeat("a", 33), cfg); err == nil {
		t.Error("slimInputGate(33 'a's, cap 8) = nil error, want the estimate-9 refusal")
	}
}

// TestSlimGateRunWiring pins the dispatch wiring end to end, in a
// subprocess: a `run` resolving to the slim flow gates its task input
// before any submission side effect — the over-cap diagnostic surfaces
// ahead of the repo-path resolution that would otherwise fail first — both
// through slim.enabled's reroute and through an explicit -w slim, while a
// non-slim flow's run is never gated.
func TestSlimGateRunWiring(t *testing.T) {
	huge := hugeInput()
	missingRepo := filepath.Join(t.TempDir(), "no-such-repo")

	t.Run("slim.enabled reroute gates before any side effect", func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(cfg, []byte("agent: claude\nslim:\n  enabled: true\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		// A nonexistent repo path: if the gate failed to fire (or fired
		// late), the run would die on "repo path ... is not a directory"
		// inside startPipelineFolders instead.
		stdout, stderr, code := runMainIn(t, "", "-c", cfg, "run", missingRepo, "42", huge)
		if code != 1 {
			t.Errorf("daedalus run (over-cap slim input) exit code = %d, want 1", code)
		}
		if stdout != "" {
			t.Errorf("stdout = %q, want empty", stdout)
		}
		if !strings.HasPrefix(stderr, "slim input too large: ") {
			t.Errorf("stderr should start with the slim gate's diagnostic, got %q", stderr)
		}
		if strings.Contains(stderr, "repo path") {
			t.Errorf("stderr = %q, want the gate to fire before any submission side effect", stderr)
		}
	})

	t.Run("explicit -w slim gates without slim.enabled", func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(cfg, []byte("agent: claude\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		stdout, stderr, code := runMainIn(t, "", "-c", cfg, "-w", "slim", "run", missingRepo, "42", huge)
		if code != 1 {
			t.Errorf("daedalus run -w slim (over-cap input) exit code = %d, want 1", code)
		}
		if stdout != "" {
			t.Errorf("stdout = %q, want empty", stdout)
		}
		if !strings.HasPrefix(stderr, "slim input too large: ") {
			t.Errorf("stderr should start with the slim gate's diagnostic, got %q", stderr)
		}
	})

	t.Run("non-slim flow is never gated", func(t *testing.T) {
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(cfg, []byte("agent: claude\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		_, stderr, code := runMainIn(t, "", "-c", cfg, "run", missingRepo, "42", huge)
		if code != 1 {
			t.Errorf("daedalus run (over-cap input, default flow) exit code = %d, want 1", code)
		}
		if !strings.HasPrefix(stderr, "run failed: repo path ") {
			t.Errorf("stderr should be the repo-path failure the ungated run reaches, got %q", stderr)
		}
		if strings.Contains(stderr, "slim input too large") {
			t.Errorf("stderr = %q, want no slim gate on the default flow", stderr)
		}
	})
}
