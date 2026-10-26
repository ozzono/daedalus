package activities

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
	"gopkg.in/yaml.v3"
)

// handsEnv builds a round env slice from K=V pairs, the shape
// stageHandsProvider reads (the round's explicit env, not the
// process's).
func handsEnv(kv ...string) []string {
	return kv
}

// stagedHandsConfigPath returns the round-scoped config path a stage
// writes into a worktree.
func stagedHandsConfigPath(wt string) string {
	return filepath.Join(wt, handsScratchDir, ".config", "hands", "config.yaml")
}

// stagedHandsFile mirrors the staged config's schema — the strict one
// hands loads (unknown fields are load errors), so every assertion
// decodes only fields the stager may write.
type stagedHandsFile struct {
	Provider struct {
		Type            string `yaml:"type"`
		URL             string `yaml:"url"`
		Key             string `yaml:"key"`
		Model           string `yaml:"model"`
		TimeoutMS       int    `yaml:"timeout_ms"`
		ContextTokens   int    `yaml:"context_tokens"`
		MaxOutputTokens int    `yaml:"max_output_tokens"`
	} `yaml:"provider"`
	Context struct {
		Import []string `yaml:"import"`
	} `yaml:"context"`
	Run struct {
		BudgetUSDCents int     `yaml:"budget_usd_cents"`
		WarnFrac       float64 `yaml:"warn_frac"`
	} `yaml:"run"`
}

// readStagedHandsConfig parses the round-scoped config a stage wrote,
// failing the test when the file is missing or unparseable, and returns
// the raw bytes alongside — the thinking knob and a url's absence are
// pinned on the bytes, where an omitted key and an empty one are still
// distinct.
func readStagedHandsConfig(t *testing.T, wt string) (stagedHandsFile, string) {
	t.Helper()
	data, err := os.ReadFile(stagedHandsConfigPath(wt))
	if err != nil {
		t.Fatalf("read staged hands config: %v", err)
	}
	var f stagedHandsFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		t.Fatalf("parse staged hands config: %v", err)
	}
	return f, string(data)
}

// assertHandsScratchExcluded pins the git invisibility contract: the
// staged config and the hands context dir are ignored via info/exclude
// exactly once per dir, git agrees, and the tree stays clean — a staged
// key or a hands session store can never reach a review diff or a
// preserved branch (the opencode scratch invariant, one dir richer).
func assertHandsScratchExcluded(t *testing.T, wt string) {
	t.Helper()
	excl := gitInfoExclude(t, wt)
	exData, err := os.ReadFile(excl)
	if err != nil {
		t.Fatalf("read exclude: %v", err)
	}
	for _, dir := range []string{handsScratchDir, ".hands"} {
		if got := strings.Count(string(exData), dir+"/\n"); got != 1 {
			t.Errorf("exclude file carries %s %d times, want exactly once: %q", dir, got, exData)
		}
	}
	for _, p := range []string{
		filepath.Join(handsScratchDir, ".config", "hands", "config.yaml"),
		filepath.Join(".hands", "context.md"),
	} {
		if out, err := exec.Command("git", "-C", wt, "check-ignore", "-q", p).CombinedOutput(); err != nil {
			t.Errorf("git check-ignore %s failed: %v: %s", p, err, out)
		}
	}
	if out, err := exec.Command("git", "-C", wt, "status", "--porcelain").CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != "" {
		t.Errorf("git status after the stage = %q (%v), want a clean tree", out, err)
	}
}

// TestStageHandsProviderUnstageableShapes pins the fail-before-launch
// guards: with no provider section exported at all, or an anthropic
// section carrying no model, there is nothing hands could dial — the
// round must fail here, not launch into hands's own public default. An
// openai section missing either the url or the model is the same posture
// (a half-specified section must not let the round dial a cloud default),
// and every failure shape stages nothing.
func TestStageHandsProviderUnstageableShapes(t *testing.T) {
	for _, c := range []struct {
		name    string
		env     []string
		wantErr string
	}{
		{"no exports at all", nil, "no provider section exported"},
		{"anthropic url and key without a model", handsEnv(
			"ANTHROPIC_BASE_URL=https://relay.example",
			"ANTHROPIC_API_KEY=sk-a",
		), "no provider section exported"},
		{"url without a model", handsEnv("OPENAI_BASE_URL=https://llm.example/v1"), "half-specified"},
		{"model without a url", handsEnv("OPENAI_MODEL=glm"), "half-specified"},
		{"key without url or model", handsEnv("OPENAI_API_KEY=sk-ambient"), "half-specified"},
	} {
		t.Run(c.name, func(t *testing.T) {
			wt := gitRepo(t)
			err := stageHandsProvider(c.env, wt)
			if err == nil {
				t.Fatalf("stageHandsProvider(%v) = nil error, want the %q guard", c.env, c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("error = %q, want the %q guard message", err, c.wantErr)
			}
			if _, statErr := os.Stat(stagedHandsConfigPath(wt)); !os.IsNotExist(statErr) {
				t.Errorf("staged config stat err = %v, want nothing staged", statErr)
			}
		})
	}
}

// TestStageHandsProviderStagesOpenaiConfig pins the openai-wire bridge:
// any openai export selects the openai wire and stages a provider-only
// config into the worktree scratch — with the /v1 tail stripped so hands
// (which appends /v1/chat/completions itself) dials the same URL the
// SDK-convention agents dial from the shared section, the key landing
// literally (hands shows no env-template expansion — the exposure is the
// 0600 git-excluded scratch file, never a review diff), and hands's own
// `config init` defaults for the numeric knobs.
func TestStageHandsProviderStagesOpenaiConfig(t *testing.T) {
	t.Run("keyed backend stages the literal key and hands's own defaults", func(t *testing.T) {
		wt := gitRepo(t)
		if err := stageHandsProvider(handsEnv(
			"OPENAI_BASE_URL=https://llm.example/v1",
			"OPENAI_API_KEY=sk-secret-do-not-stage",
			"OPENAI_MODEL=glm",
		), wt); err != nil {
			t.Fatalf("stageHandsProvider: %v", err)
		}
		f, raw := readStagedHandsConfig(t, wt)
		if f.Provider.Type != "openai" {
			t.Errorf("provider.type = %q, want openai", f.Provider.Type)
		}
		if f.Provider.URL != "https://llm.example" {
			t.Errorf("provider.url = %q, want the /v1 tail stripped", f.Provider.URL)
		}
		if f.Provider.Key != "sk-secret-do-not-stage" {
			t.Errorf("provider.key = %q, want the literal value — hands has no env-template expansion", f.Provider.Key)
		}
		if strings.Contains(raw, "${OPENAI_API_KEY}") {
			t.Errorf("staged config carries the pi-style env template hands cannot expand: %s", raw)
		}
		if f.Provider.Model != "glm" {
			t.Errorf("provider.model = %q, want the round's model", f.Provider.Model)
		}
		if f.Provider.TimeoutMS != handsDefaultTimeoutMS {
			t.Errorf("timeout_ms = %d, want hands's own default %d", f.Provider.TimeoutMS, handsDefaultTimeoutMS)
		}
		if f.Provider.ContextTokens != handsDefaultContextTokens {
			t.Errorf("context_tokens = %d, want hands's own default %d", f.Provider.ContextTokens, handsDefaultContextTokens)
		}
		if f.Provider.MaxOutputTokens != handsDefaultMaxOutTokens {
			t.Errorf("max_output_tokens = %d, want hands's own default %d", f.Provider.MaxOutputTokens, handsDefaultMaxOutTokens)
		}
		if len(f.Context.Import) != 0 {
			t.Errorf("context.import = %v, want the empty list hands's schema expects", f.Context.Import)
		}
		if f.Run.BudgetUSDCents != 0 {
			t.Errorf("budget_usd_cents = %d, want 0 — cost accounting stays with the caller's usage figures", f.Run.BudgetUSDCents)
		}
		if f.Run.WarnFrac != 0.70 {
			t.Errorf("warn_frac = %v, want hands's own default 0.70", f.Run.WarnFrac)
		}
		if strings.Contains(raw, "thinking") {
			t.Errorf("staged config carries a thinking line without the knob exported: %s", raw)
		}
		// The top-level shape is exactly the three blocks hands's strict
		// schema knows — one unknown key is a load error, so the key set
		// itself is the contract.
		var top map[string]any
		if err := yaml.Unmarshal([]byte(raw), &top); err != nil {
			t.Fatalf("reparse staged config: %v", err)
		}
		if keys := keysOf(top); !slices.Equal(slices.Sorted(slices.Values(keys)), []string{"context", "provider", "run"}) {
			t.Errorf("staged top-level keys = %v, want exactly [context provider run]", keys)
		}
		assertHandsScratchExcluded(t, wt)
	})

	// OPENAI_API_BASE rides the same value as OPENAI_BASE_URL for the
	// other agents' bridges; the hands stage honors it as the fallback so
	// a failover or ambient environment exporting only the older name
	// still bridges.
	t.Run("OPENAI_API_BASE fallback", func(t *testing.T) {
		wt := gitRepo(t)
		if err := stageHandsProvider(handsEnv(
			"OPENAI_API_BASE=http://localhost:4000/v1",
			"OPENAI_MODEL=glm",
		), wt); err != nil {
			t.Fatalf("stageHandsProvider: %v", err)
		}
		f, _ := readStagedHandsConfig(t, wt)
		if f.Provider.URL != "http://localhost:4000" {
			t.Errorf("provider.url = %q, want OPENAI_API_BASE's value with the /v1 tail stripped", f.Provider.URL)
		}
	})

	t.Run("OPENAI_BASE_URL wins over OPENAI_API_BASE", func(t *testing.T) {
		wt := gitRepo(t)
		if err := stageHandsProvider(handsEnv(
			"OPENAI_BASE_URL=https://primary.example/v1",
			"OPENAI_API_BASE=https://stale.example/v1",
			"OPENAI_MODEL=glm",
		), wt); err != nil {
			t.Fatalf("stageHandsProvider: %v", err)
		}
		f, _ := readStagedHandsConfig(t, wt)
		if f.Provider.URL != "https://primary.example" {
			t.Errorf("provider.url = %q, want OPENAI_BASE_URL's value", f.Provider.URL)
		}
	})

	t.Run("a /v1-less url stages verbatim", func(t *testing.T) {
		wt := gitRepo(t)
		if err := stageHandsProvider(handsEnv(
			"OPENAI_BASE_URL=http://localhost:11434",
			"OPENAI_MODEL=qwen2.5-coder:3b",
		), wt); err != nil {
			t.Fatalf("stageHandsProvider: %v", err)
		}
		f, _ := readStagedHandsConfig(t, wt)
		if f.Provider.URL != "http://localhost:11434" {
			t.Errorf("provider.url = %q, want it staged verbatim", f.Provider.URL)
		}
	})

	t.Run("a trailing slash before the /v1 tail still strips", func(t *testing.T) {
		wt := gitRepo(t)
		if err := stageHandsProvider(handsEnv(
			"OPENAI_BASE_URL=https://llm.example/v1/",
			"OPENAI_MODEL=glm",
		), wt); err != nil {
			t.Fatalf("stageHandsProvider: %v", err)
		}
		f, _ := readStagedHandsConfig(t, wt)
		if f.Provider.URL != "https://llm.example" {
			t.Errorf("provider.url = %q, want https://llm.example", f.Provider.URL)
		}
	})

	// The url lands quoted like its sibling fields, staging the
	// env-supplied value as a literal YAML scalar: the truncation hazard
	// is the whitespace-preceded '#' (binary-probed 2026-10-25 —
	// https://llm.example/v1 #frag loads as https://llm.example/v1) and
	// other structure-significant characters, while a bare mid-word '#'
	// does not truncate. The /v1 strip is a suffix match and cannot fire
	// once a fragment trails the /v1, so the staged value carries the
	// tail verbatim.
	t.Run("a fragment-bearing url stages quoted", func(t *testing.T) {
		wt := gitRepo(t)
		if err := stageHandsProvider(handsEnv(
			"OPENAI_BASE_URL=https://llm.example/v1#frag",
			"OPENAI_MODEL=glm",
		), wt); err != nil {
			t.Fatalf("stageHandsProvider: %v", err)
		}
		f, raw := readStagedHandsConfig(t, wt)
		if f.Provider.URL != "https://llm.example/v1#frag" {
			t.Errorf("provider.url = %q, want the fragment kept verbatim", f.Provider.URL)
		}
		if !strings.Contains(raw, `url: "https://llm.example/v1#frag"`) {
			t.Errorf("staged config = %q, want the url value quoted — the literal-scalar contract, same as the sibling key and model fields", raw)
		}
	})

	// The decode-discriminating shape: a whitespace-preceded '#'. An
	// unquoted writer emits a plain scalar whose ' #' starts a comment —
	// hands's loader loads it as https://llm.example/v1, fragment gone
	// (binary-probed 2026-10-25) — so the decoded assertion alone fails
	// on a revert, while the quoted writer passes both halves. The value
	// ends in '#frag', so the /v1 suffix strip cannot fire here either.
	t.Run("a whitespace-preceded fragment stages quoted and decodes whole", func(t *testing.T) {
		wt := gitRepo(t)
		if err := stageHandsProvider(handsEnv(
			"OPENAI_BASE_URL=https://llm.example/v1 #frag",
			"OPENAI_MODEL=glm",
		), wt); err != nil {
			t.Fatalf("stageHandsProvider: %v", err)
		}
		f, raw := readStagedHandsConfig(t, wt)
		if f.Provider.URL != "https://llm.example/v1 #frag" {
			t.Errorf("provider.url = %q, want the fragment kept whole through the quoted scalar", f.Provider.URL)
		}
		if !strings.Contains(raw, `url: "https://llm.example/v1 #frag"`) {
			t.Errorf("staged config = %q, want the url value quoted", raw)
		}
	})

	// The strip composes on the URL only — a slashed model id must stage
	// verbatim or hands would select a model that does not exist.
	t.Run("slashed model id survives", func(t *testing.T) {
		wt := gitRepo(t)
		if err := stageHandsProvider(handsEnv(
			"OPENAI_BASE_URL=https://llm.example/v1",
			"OPENAI_MODEL=hf/org/Qwen3-4B",
		), wt); err != nil {
			t.Fatalf("stageHandsProvider: %v", err)
		}
		f, _ := readStagedHandsConfig(t, wt)
		if f.Provider.Model != "hf/org/Qwen3-4B" {
			t.Errorf("provider.model = %q, want the full slashed model id", f.Provider.Model)
		}
	})

	// The staged-config-agent convention: an openai export wins over an
	// exported anthropic section — the anthropic values must not leak
	// into the staged file.
	t.Run("the openai section wins over an exported anthropic one", func(t *testing.T) {
		wt := gitRepo(t)
		if err := stageHandsProvider(handsEnv(
			"OPENAI_BASE_URL=https://open.example/v1",
			"OPENAI_MODEL=glm",
			"ANTHROPIC_BASE_URL=https://anth.example",
			"ANTHROPIC_API_KEY=sk-a",
			"ANTHROPIC_MODEL=claude-sonnet",
		), wt); err != nil {
			t.Fatalf("stageHandsProvider: %v", err)
		}
		f, _ := readStagedHandsConfig(t, wt)
		if f.Provider.Type != "openai" || f.Provider.URL != "https://open.example" || f.Provider.Model != "glm" {
			t.Errorf("staged provider = %+v, want the openai family's values", f.Provider)
		}
		if f.Provider.Key == "sk-a" {
			t.Errorf("provider.key = %q, want the openai section's key (or none), never the anthropic one", f.Provider.Key)
		}
	})
}

// TestStageHandsProviderStagesAnthropicConfig pins the anthropic-wire
// fallback: with no openai export, an ANTHROPIC_MODEL serves — hands
// speaks the anthropic wire itself, so the url stages verbatim (no /v1
// strip) and is optional (its absence means hands's own public default,
// the claude key-alone posture).
func TestStageHandsProviderStagesAnthropicConfig(t *testing.T) {
	t.Run("model and key with a url", func(t *testing.T) {
		wt := gitRepo(t)
		if err := stageHandsProvider(handsEnv(
			"ANTHROPIC_BASE_URL=https://relay.example/v1",
			"ANTHROPIC_API_KEY=sk-a",
			"ANTHROPIC_MODEL=claude-sonnet",
		), wt); err != nil {
			t.Fatalf("stageHandsProvider: %v", err)
		}
		f, _ := readStagedHandsConfig(t, wt)
		if f.Provider.Type != "anthropic" {
			t.Errorf("provider.type = %q, want anthropic", f.Provider.Type)
		}
		if f.Provider.URL != "https://relay.example/v1" {
			t.Errorf("provider.url = %q, want the anthropic url staged verbatim — no /v1 strip on this wire", f.Provider.URL)
		}
		if f.Provider.Key != "sk-a" {
			t.Errorf("provider.key = %q, want the literal value", f.Provider.Key)
		}
		if f.Provider.Model != "claude-sonnet" {
			t.Errorf("provider.model = %q, want the round's model", f.Provider.Model)
		}
	})

	t.Run("model alone stages no url", func(t *testing.T) {
		wt := gitRepo(t)
		if err := stageHandsProvider(handsEnv(
			"ANTHROPIC_API_KEY=sk-a",
			"ANTHROPIC_MODEL=claude-sonnet",
		), wt); err != nil {
			t.Fatalf("stageHandsProvider: %v", err)
		}
		f, raw := readStagedHandsConfig(t, wt)
		if f.Provider.Type != "anthropic" || f.Provider.Key != "sk-a" || f.Provider.Model != "claude-sonnet" {
			t.Errorf("staged provider = %+v, want the anthropic family's values", f.Provider)
		}
		if strings.Contains(raw, "url:") {
			t.Errorf("staged config carries a url without an export: %s", raw)
		}
	})
}

// TestStageHandsProviderStagesKnobs pins the numeric and thinking
// channels: the timeout/window/cap exports land on the staged config's
// own fields, an unusable export (zero, negative, garbage) stages
// hands's written default instead — zero and unset are indistinguishable
// to a yaml decode and a literal zero would be an instant timeout, a
// zero window, an always-warning threshold — and only the exact "off"
// thinking value stages a thinking line.
func TestStageHandsProviderStagesKnobs(t *testing.T) {
	base := handsEnv("OPENAI_BASE_URL=https://llm.example/v1", "OPENAI_MODEL=glm")

	t.Run("explicit exports stage", func(t *testing.T) {
		wt := gitRepo(t)
		if err := stageHandsProvider(append(slices.Clone(base),
			"API_TIMEOUT_MS=60000",
			config.ContextTokensEnv+"=131072",
			config.MaxOutputTokensEnv+"=8192",
			config.WireThinkingEnv+"=off",
		), wt); err != nil {
			t.Fatalf("stageHandsProvider: %v", err)
		}
		f, raw := readStagedHandsConfig(t, wt)
		if f.Provider.TimeoutMS != 60000 {
			t.Errorf("timeout_ms = %d, want the API_TIMEOUT_MS export", f.Provider.TimeoutMS)
		}
		if f.Provider.ContextTokens != 131072 {
			t.Errorf("context_tokens = %d, want the window export", f.Provider.ContextTokens)
		}
		if f.Provider.MaxOutputTokens != 8192 {
			t.Errorf("max_output_tokens = %d, want the cap export", f.Provider.MaxOutputTokens)
		}
		if !strings.Contains(raw, "thinking: off\n") {
			t.Errorf("staged config = %q, want the thinking: off line", raw)
		}
	})

	// Zero is the config's documented unset default and a garbage value
	// is an ambient accident; neither is a budget.
	for _, c := range []struct{ name, timeout, window, cap string }{
		{"zero exports", "0", "0", "0"},
		{"negative exports", "-1", "-1", "-1"},
		{"garbage exports", "not-a-number", "", "junk"},
	} {
		t.Run(c.name, func(t *testing.T) {
			wt := gitRepo(t)
			env := append(slices.Clone(base),
				"API_TIMEOUT_MS="+c.timeout,
				config.ContextTokensEnv+"="+c.window,
				config.MaxOutputTokensEnv+"="+c.cap,
			)
			if err := stageHandsProvider(env, wt); err != nil {
				t.Fatalf("stageHandsProvider: %v", err)
			}
			f, raw := readStagedHandsConfig(t, wt)
			if f.Provider.TimeoutMS != handsDefaultTimeoutMS || f.Provider.ContextTokens != handsDefaultContextTokens || f.Provider.MaxOutputTokens != handsDefaultMaxOutTokens {
				t.Errorf("staged knobs = %d/%d/%d, want hands's defaults %d/%d/%d",
					f.Provider.TimeoutMS, f.Provider.ContextTokens, f.Provider.MaxOutputTokens,
					handsDefaultTimeoutMS, handsDefaultContextTokens, handsDefaultMaxOutTokens)
			}
			if strings.Contains(raw, "thinking") {
				t.Errorf("staged config carries a thinking line without an off export: %s", raw)
			}
		})
	}

	t.Run("a thinking value other than off stages no line", func(t *testing.T) {
		wt := gitRepo(t)
		if err := stageHandsProvider(append(slices.Clone(base), config.WireThinkingEnv+"=on"), wt); err != nil {
			t.Fatalf("stageHandsProvider: %v", err)
		}
		_, raw := readStagedHandsConfig(t, wt)
		if strings.Contains(raw, "thinking") {
			t.Errorf("staged config carries a thinking line for a non-off export: %s", raw)
		}
	})
}

// TestHandsJailMounts pins the state-dir resolution behind the hands jail
// mount: the worktree scratch's .config/hands maps read-write onto the
// real ~/.config/hands (so the child reads the staged provider and its
// session store lands back in the scratch), both sides are created before
// spawn, a colon in either path is rejected before it can split the
// --rw-map spec, and an unresolvable home fails the round here.
func TestHandsJailMounts(t *testing.T) {
	t.Run("scratch config dir mounted onto the home path", func(t *testing.T) {
		home := fakeHome(t)
		wt := t.TempDir()
		args, err := handsJailMounts(wt)
		if err != nil {
			t.Fatalf("handsJailMounts: %v", err)
		}
		src := filepath.Join(wt, handsScratchDir, ".config", "hands")
		dst := filepath.Join(home, ".config", "hands")
		want := []string{"--rw-map", src + ":" + dst}
		if !slices.Equal(args, want) {
			t.Errorf("handsJailMounts() = %v, want %v", args, want)
		}
		for _, dir := range []string{src, dst} {
			if info, err := os.Stat(dir); err != nil || !info.IsDir() {
				t.Errorf("mount dir %s stat err = %v, want it created before spawn", dir, err)
			}
		}
	})

	t.Run("colon in a path rejected", func(t *testing.T) {
		fakeHome(t)
		wt := filepath.Join(t.TempDir(), "weird:wt")
		if _, err := handsJailMounts(wt); err == nil || !strings.Contains(err.Error(), "must not contain") {
			t.Errorf("handsJailMounts() err = %v, want the colon-guard rejection", err)
		}
	})

	t.Run("unresolvable home fails", func(t *testing.T) {
		t.Setenv("HOME", "")
		if _, err := handsJailMounts(t.TempDir()); err == nil || !strings.Contains(err.Error(), "resolve home directory") {
			t.Errorf("handsJailMounts() err = %v, want a resolution failure", err)
		}
	})
}

// TestHandsTranscriptExists pins the staleness check: it shells out to
// `hands export <id>` with HOME pointed at the worktree scratch (the
// mounted store's host path), reads only the exit code, and any shape
// that cannot confirm the session — a missing binary, a nonzero exit —
// reports stale, because the resume would fail the same way.
func TestHandsTranscriptExists(t *testing.T) {
	wt := t.TempDir()
	scratch := handsStateHome(wt)

	t.Run("missing hands binary reports stale", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		if handsTranscriptExists(wt, "sess-1") {
			t.Error("handsTranscriptExists without a hands binary = true, want false (stale)")
		}
	})

	t.Run("exit 0 is live and reads through the scratch home", func(t *testing.T) {
		log := newStubLog(t)
		stubBin(t, "hands", "echo \"ENV:HOME=$HOME\" >> \"$STUB_LOG\"\nexit 0")
		if !handsTranscriptExists(wt, "sess-9") {
			t.Error("handsTranscriptExists with a healthy export = false, want true (live)")
		}
		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("hands called %d times, want 1", len(calls))
		}
		if want := []string{"export", "sess-9"}; !slices.Equal(calls[0].Args, want) {
			t.Errorf("hands args = %v, want %v", calls[0].Args, want)
		}
		if got := calls[0].Env["HOME"]; got != scratch {
			t.Errorf("export HOME = %q, want the worktree scratch %q — the store the jail mounts", got, scratch)
		}
	})

	t.Run("nonzero exit reports stale", func(t *testing.T) {
		newStubLog(t)
		stubBin(t, "hands", "exit 1")
		if handsTranscriptExists(wt, "sess-1") {
			t.Error("handsTranscriptExists with a failing export = true, want false (stale)")
		}
	})
}

// TestHandsSessionID pins the session-id extraction from a round's
// captured stdout: the last started event's payload.session wins (one
// round mints exactly one), and a stream with no parsable started event —
// garbage lines, a round killed before hands wrote anything — reports
// nothing to record.
func TestHandsSessionID(t *testing.T) {
	started := func(id string) string {
		return `{"round_id":"r","seq":0,"kind":"started","payload":{"session":"` + id + `"}}`
	}
	for _, c := range []struct {
		name   string
		stdout string
		want   string
		ok     bool
	}{
		{"the started event's session", started("s-1"), "s-1", true},
		{"the last started event wins", started("s-1") + "\n" + started("s-2"), "s-2", true},
		{"garbage lines are skipped", "hands: launching round\n" + started("s-3"), "s-3", true},
		{"blank lines are skipped", "\n" + started("s-4") + "\n\n", "s-4", true},
		{"a started event without a session is ignored", `{"kind":"started","payload":{}}`, "", false},
		{"other kinds never decide", `{"kind":"usage","payload":{"session":"forged"}}`, "", false},
		{"no started event", `{"kind":"usage","payload":{}}`, "", false},
		{"an empty stream", "", "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, ok := handsSessionID(c.stdout)
			if got != c.want || ok != c.ok {
				t.Errorf("handsSessionID = %q, %v; want %q, %v", got, ok, c.want, c.ok)
			}
		})
	}
}

// TestParseHandsStream pins the stream parser: the started event carries
// the conversation session, the terminal delivered/partial event's
// delivery summary is the round's visible text (the forced deliverable,
// not the narration before it), the last non-empty summary wins, and
// thinking stays empty — hands emits no thinking events.
func TestParseHandsStream(t *testing.T) {
	t.Run("a delivered round", func(t *testing.T) {
		out := `{"round_id":"r","seq":0,"kind":"started","payload":{"session":"s-1"}}` + "\n" +
			`{"round_id":"r","seq":1,"kind":"tool_call","payload":{}}` + "\n" +
			`{"round_id":"r","seq":2,"kind":"delivered","payload":{"payload":{"summary":"the deliverable","files_changed":["a.go"]}}}`
		thinking, text, session := parseHandsStream(out)
		if thinking != "" || text != "the deliverable" || session != "s-1" {
			t.Errorf("parseHandsStream = %q, %q, %q; want empty thinking, the delivery summary, and the session", thinking, text, session)
		}
	})

	t.Run("a partial delivery is text too", func(t *testing.T) {
		out := `{"kind":"started","payload":{"session":"s-2"}}` + "\n" +
			`{"kind":"partial","payload":{"payload":{"summary":"a partial answer"}}}`
		_, text, session := parseHandsStream(out)
		if text != "a partial answer" || session != "s-2" {
			t.Errorf("parseHandsStream = %q, %q; want the partial summary and the session", text, session)
		}
	})

	t.Run("a later delivery wins over an earlier one", func(t *testing.T) {
		out := `{"kind":"partial","payload":{"payload":{"summary":"first"}}}` + "\n" +
			`{"kind":"delivered","payload":{"payload":{"summary":"second"}}}`
		if _, text, _ := parseHandsStream(out); text != "second" {
			t.Errorf("parseHandsStream text = %q, want the terminal delivery", text)
		}
	})

	t.Run("an empty delivery summary keeps the earlier text", func(t *testing.T) {
		out := `{"kind":"partial","payload":{"payload":{"summary":"kept"}}}` + "\n" +
			`{"kind":"delivered","payload":{"payload":{"summary":""}}}`
		if _, text, _ := parseHandsStream(out); text != "kept" {
			t.Errorf("parseHandsStream text = %q, want the earlier non-empty summary", text)
		}
	})

	t.Run("no delivery is empty text", func(t *testing.T) {
		out := `{"kind":"started","payload":{"session":"s-3"}}` + "\n" +
			`{"kind":"failed","payload":{"class":"retriable","reason":"boom"}}`
		thinking, text, session := parseHandsStream(out)
		if thinking != "" || text != "" || session != "s-3" {
			t.Errorf("parseHandsStream = %q, %q, %q; want only the session", thinking, text, session)
		}
	})

	t.Run("garbage lines are skipped", func(t *testing.T) {
		out := "hands round r-1\n" +
			`{"kind":"delivered","payload":{"payload":{"summary":"done"}}}` + "\n" +
			"{truncated"
		if _, text, _ := parseHandsStream(out); text != "done" {
			t.Errorf("parseHandsStream text = %q, want the delivered summary", text)
		}
	})
}

// TestParseHandsUsage pins the usage parser: usage rides one event per
// provider request, so the round's raw figure is the sum across them —
// the same total claude's single result event carries — and every other
// kind (whose payload carries no usage keys) and every unparsable line
// contributes nothing.
func TestParseHandsUsage(t *testing.T) {
	out := `{"kind":"started","payload":{"session":"s"}}` + "\n" +
		`{"kind":"usage","payload":{"input_tokens":10,"output_tokens":2,"cache_read_tokens":1,"cache_write_tokens":0,"cost_dollars":0.01}}` + "\n" +
		`{"kind":"usage","payload":{"input_tokens":5,"output_tokens":3,"cache_read_tokens":0,"cache_write_tokens":4,"cost_dollars":0.02}}`
	usage := parseHandsUsage(out)
	if usage.InputTokens != 15 || usage.OutputTokens != 5 || usage.CacheReadTokens != 1 || usage.CacheWriteTokens != 4 || usage.CostUSD != 0.03 {
		t.Errorf("parseHandsUsage = %+v, want the sum across both usage events", usage)
	}

	if got := parseHandsUsage(`{"kind":"delivered","payload":{"payload":{"summary":"done"}}}`); got != (Usage{}) {
		t.Errorf("parseHandsUsage without usage events = %+v, want the zero Usage", got)
	}
	if got := parseHandsUsage("hands round r-1"); got != (Usage{}) {
		t.Errorf("parseHandsUsage of plain text = %+v, want the zero Usage", got)
	}
}

// TestHandsRoundStagesProvider pins the runJailedRound wiring end to end:
// a hands round launches ai-jail with the scratch-to-home rw-map and the
// typed headless set (`round --format json`), staged from the round env
// before launch; a resumed round's precomposed argv replaces the round
// subcommand with resume and still stages; and both unbridgeable
// provider shapes fail the round before ai-jail is ever invoked.
func TestHandsRoundStagesProvider(t *testing.T) {
	t.Run("openai-served round carries the rw-map and the typed headless set", func(t *testing.T) {
		scrubBugFilingEnv(t)
		home := fakeHome(t)
		t.Setenv("DAEDALUS_AGENT", "hands")
		t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
		t.Setenv("OPENAI_API_KEY", "sk-local")
		t.Setenv("OPENAI_MODEL", "glm-selfhost")
		wt := gitRepo(t)
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
			WorktreePath: wt,
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedClaudeActivity: %v", err)
		}

		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		src := filepath.Join(wt, handsScratchDir, ".config", "hands")
		dst := filepath.Join(home, ".config", "hands")
		assertArgs(t, calls[0].Args, slices.Concat(
			[]string{"--worktree",
				"--network",
				"--no-save-config",
				"--mask",
				".claude/settings.json",
				"--mask",
				".claude/settings.local.json",
				"--mask",
				homeClaudeMask(t),
				"--rw-map",
				src + ":" + dst,
				"--",
				"hands",
				"round",
				"--format",
				"json",
			},
		), "ai-jail")
		// No --env rides: hands reads no provider env var — the staged
		// file is the whole bridge.
		if contains(calls[0].Args, "--env") {
			t.Errorf("ai-jail args %v carry an --env passthrough, want none for hands", calls[0].Args)
		}
		// The bytes the jailed hands would read carry the bridge, with
		// the /v1 tail stripped.
		f, _ := readStagedHandsConfig(t, wt)
		if f.Provider.Type != "openai" || f.Provider.URL != "https://selfhost.example" || f.Provider.Key != "sk-local" || f.Provider.Model != "glm-selfhost" {
			t.Errorf("staged provider = %+v, want the round's openai wiring", f.Provider)
		}
		assertHandsScratchExcluded(t, wt)
	})

	// The resumed shape arrives with the whole post-`--` argv precomposed
	// (headless nil), so resume replaces the round subcommand and the
	// format flags trail — the resumed stream stays typed like a fresh
	// one — and the provider staging runs on this path too.
	t.Run("resumed round replaces the round subcommand and still stages", func(t *testing.T) {
		scrubBugFilingEnv(t)
		home := fakeHome(t)
		t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
		t.Setenv("OPENAI_MODEL", "glm-selfhost")
		wt := gitRepo(t)
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := runJailedRound(context.Background(), os.Environ(), "", "hands", wt, "fix the bug",
			agentResumeArgs("hands", "sess-hands-1", nil)...); err != nil {
			t.Fatalf("runJailedRound: %v", err)
		}

		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		src := filepath.Join(wt, handsScratchDir, ".config", "hands")
		dst := filepath.Join(home, ".config", "hands")
		assertArgs(t, calls[0].Args, slices.Concat(
			[]string{"--worktree",
				"--network",
				"--no-save-config",
				"--mask",
				".claude/settings.json",
				"--mask",
				".claude/settings.local.json",
				"--mask",
				homeClaudeMask(t),
				"--rw-map",
				src + ":" + dst,
				"--",
				"hands",
				"resume",
				"sess-hands-1",
				"--format",
				"json",
			},
		), "ai-jail")
		f, _ := readStagedHandsConfig(t, wt)
		if f.Provider.Type != "openai" || f.Provider.URL != "https://selfhost.example" {
			t.Errorf("staged provider = %+v, want the resumed round staged too", f.Provider)
		}
	})

	t.Run("ambient key without url or model fails before launch", func(t *testing.T) {
		scrubBugFilingEnv(t)
		fakeHome(t)
		t.Setenv("DAEDALUS_AGENT", "hands")
		t.Setenv("OPENAI_API_KEY", "sk-ambient")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		_, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		})
		if err == nil {
			t.Fatal("RunJailedClaudeActivity = nil error, want the half-specified failure")
		}
		if !strings.Contains(err.Error(), "half-specified") {
			t.Errorf("error = %q, want the hands half-specified guard message", err)
		}
		if _, statErr := os.Stat(log); !os.IsNotExist(statErr) {
			calls := readCalls(t, log)
			t.Errorf("ai-jail called %d times, want the round failed before launch (%v)", len(calls), calls)
		}
	})

	t.Run("no provider exports fail before launch", func(t *testing.T) {
		scrubBugFilingEnv(t)
		fakeHome(t)
		t.Setenv("DAEDALUS_AGENT", "hands")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		_, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		})
		if err == nil {
			t.Fatal("RunJailedClaudeActivity = nil error, want the no-provider failure")
		}
		if !strings.Contains(err.Error(), "no provider section exported") {
			t.Errorf("error = %q, want the hands no-provider guard message", err)
		}
		if _, statErr := os.Stat(log); !os.IsNotExist(statErr) {
			calls := readCalls(t, log)
			t.Errorf("ai-jail called %d times, want the round failed before launch (%v)", len(calls), calls)
		}
	})
}

// TestHandsRoundRecordsKilledSessionAndResumes pins the hands tracking
// loop end to end: a round killed at its first provider failure still
// leaves its started event behind, the id is recorded from the round's
// own stdout (no transcript face exists for the watchers), the staleness
// check shells `hands export` with HOME pointed at the scratch store, and
// the retry resumes that conversation — `resume <id>` replacing the round
// subcommand, the format flags trailing.
func TestHandsRoundRecordsKilledSessionAndResumes(t *testing.T) {
	scrubBugFilingEnv(t)
	home := fakeHome(t)
	t.Setenv("DAEDALUS_AGENT", "hands")
	t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
	t.Setenv("OPENAI_API_KEY", "sk-local")
	t.Setenv("OPENAI_MODEL", "glm-selfhost")
	wt := gitRepo(t)
	scratch := filepath.Join(wt, handsScratchDir)

	// The staleness stub writes its own log: a separate STUB_LOG from the
	// ai-jail one, so the export calls read apart from the jail calls.
	handsLog := newStubLog(t)
	stubBin(t, "hands", "echo \"ENV:HOME=$HOME\" >> \"$STUB_LOG\"\nexit 0")
	log := newStubLog(t)
	// Round one: hands mints its session before any model turn, then the
	// round dies to a signal.
	stubBin(t, "ai-jail",
		`printf '%s\n' '{"round_id":"r-1","seq":0,"kind":"started","payload":{"session":"sess-hands-1"}}' '{"round_id":"r-1","seq":1,"kind":"usage","payload":{"input_tokens":10,"output_tokens":2,"cost_dollars":0.01}}'`+"\nkill -9 $$")

	if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "implement",
		Role:         RoleDev,
	}); !errors.Is(err, ErrAgentKilled) {
		t.Fatalf("first round error = %v, want ErrAgentKilled", err)
	}
	id, stale := recordedAgentSession(context.Background(), "hands", wt, RoleDev)
	if id != "sess-hands-1" || stale {
		t.Fatalf("recorded session = %q (stale=%v), want sess-hands-1 live — the killed round's conversation must be resumable", id, stale)
	}
	for _, call := range readCalls(t, handsLog) {
		if slices.Equal(call.Args, []string{"export", "sess-hands-1"}) {
			if got := call.Env["HOME"]; got != scratch {
				t.Errorf("export HOME = %q, want the worktree scratch %q", got, scratch)
			}
			break
		}
	}

	// Round two: the retry, with no workflow-provided id.
	stubBin(t, "ai-jail",
		`printf '%s\n' '{"round_id":"r-2","seq":0,"kind":"started","payload":{"session":"sess-hands-2"}}' '{"round_id":"r-2","seq":1,"kind":"delivered","payload":{"payload":{"summary":"resumed and delivered","files_changed":["a.go"]}}}' '{"round_id":"r-2","seq":2,"kind":"usage","payload":{"input_tokens":100,"output_tokens":20,"cost_dollars":0.42}}'; exit 0`)
	res, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "continue the work",
		Role:         RoleDev,
	})
	if err != nil {
		t.Fatalf("retry round: %v", err)
	}
	if res.SessionID != "sess-hands-2" {
		t.Errorf("res.SessionID = %q, want sess-hands-2", res.SessionID)
	}
	if res.Text != "resumed and delivered" {
		t.Errorf("res.Text = %q, want the delivery summary", res.Text)
	}
	if res.Usage.CostUSD != 0.42 || res.Usage.InputTokens != 100 {
		t.Errorf("res.Usage = %+v, want the summed usage figures", res.Usage)
	}

	src := filepath.Join(wt, handsScratchDir, ".config", "hands")
	dst := filepath.Join(home, ".config", "hands")
	calls := readCalls(t, log)
	if len(calls) != 2 {
		t.Fatalf("ai-jail called %d times, want 2 (killed round, retry)", len(calls))
	}
	assertArgs(t, calls[0].Args, slices.Concat(
		[]string{"--worktree", "--network", "--no-save-config",
			"--mask", ".claude/settings.json",
			"--mask", ".claude/settings.local.json",
			"--mask", homeClaudeMask(t),
			"--rw-map", src + ":" + dst,
			"--", "hands", "round", "--format", "json"},
	), "killed round")
	assertArgs(t, calls[1].Args, slices.Concat(
		[]string{"--worktree", "--network", "--no-save-config",
			"--mask", ".claude/settings.json",
			"--mask", ".claude/settings.local.json",
			"--mask", homeClaudeMask(t),
			"--rw-map", src + ":" + dst,
			"--", "hands", "resume", "sess-hands-1", "--format", "json"},
	), "retry round")
}
