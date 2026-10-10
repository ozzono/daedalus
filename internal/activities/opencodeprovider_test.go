package activities

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
)

// ocEnv builds a round env slice from K=V pairs, the shape
// stageOpencodeProvider reads (the round's explicit env, not the
// process's).
func ocEnv(kv ...string) []string {
	return kv
}

// stagedOpencodeConfigPath returns the round-scoped config path a stage
// writes into a worktree.
func stagedOpencodeConfigPath(wt string) string {
	return filepath.Join(wt, opencodeScratchDir, "opencode.json")
}

// readStagedOpencodeConfig parses the round-scoped config a stage wrote,
// failing the test when the file is missing or unparseable — every
// staging assertion starts from the bytes opencode would actually read at
// config load.
func readStagedOpencodeConfig(t *testing.T, path string) map[string]json.RawMessage {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read staged opencode.json: %v", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		t.Fatalf("parse staged opencode.json: %v", err)
	}
	return top
}

// stagedOpencodeProviderRaw returns the daedalus-openai entry as raw JSON —
// the bytes opencode would read — for assertions a typed decode cannot make
// (a key an omitempty tag dropped is indistinguishable from a zero value
// once decoded).
func stagedOpencodeProviderRaw(t *testing.T, wt string) map[string]json.RawMessage {
	t.Helper()
	top := readStagedOpencodeConfig(t, stagedOpencodeConfigPath(wt))
	if len(top) != 1 || top["provider"] == nil {
		t.Fatalf("staged config keys = %v, want exactly the provider object and nothing else", keysOf(top))
	}
	var providers map[string]json.RawMessage
	if err := json.Unmarshal(top["provider"], &providers); err != nil {
		t.Fatalf("parse staged providers: %v", err)
	}
	if len(providers) != 1 || providers[opencodeProviderID] == nil {
		t.Fatalf("staged providers = %v, want exactly one %s entry", keysOf(providers), opencodeProviderID)
	}
	var entry map[string]json.RawMessage
	if err := json.Unmarshal(providers[opencodeProviderID], &entry); err != nil {
		t.Fatalf("parse staged %s entry: %v", opencodeProviderID, err)
	}
	return entry
}

// stagedOpencodeModelRaw returns the single staged model entry's id and raw
// JSON — the id is the map key opencode's model lookup resolves -m against,
// so it must carry the model name verbatim.
func stagedOpencodeModelRaw(t *testing.T, wt string) (string, map[string]json.RawMessage) {
	t.Helper()
	entry := stagedOpencodeProviderRaw(t, wt)
	var models map[string]json.RawMessage
	if err := json.Unmarshal(entry["models"], &models); err != nil {
		t.Fatalf("parse staged models: %v", err)
	}
	if len(models) != 1 {
		t.Fatalf("staged models = %v, want exactly one entry", keysOf(models))
	}
	var ids []string
	for id := range models {
		ids = append(ids, id)
	}
	id := ids[0]
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(models[id], &decoded); err != nil {
		t.Fatalf("parse staged model %s: %v", id, err)
	}
	return id, decoded
}

// stagedOpencodeOptions decodes the staged provider's options object and
// additionally returns its raw keys, so a test can tell an omitted apiKey
// from an empty one.
func stagedOpencodeOptions(t *testing.T, wt string) (baseURL, apiKey string, rawKeys []string) {
	t.Helper()
	entry := stagedOpencodeProviderRaw(t, wt)
	var opts map[string]json.RawMessage
	if err := json.Unmarshal(entry["options"], &opts); err != nil {
		t.Fatalf("parse staged options: %v", err)
	}
	if err := json.Unmarshal(opts["baseURL"], &baseURL); err != nil {
		t.Fatalf("parse staged baseURL: %v", err)
	}
	if raw, ok := opts["apiKey"]; ok {
		if err := json.Unmarshal(raw, &apiKey); err != nil {
			t.Fatalf("parse staged apiKey: %v", err)
		}
	}
	for k := range opts {
		rawKeys = append(rawKeys, k)
	}
	return baseURL, apiKey, rawKeys
}

// TestStageOpencodeProviderNoOpenaiVars pins the pass-through shape: a
// round env with none of the openai exports stages nothing — no scratch
// dir, no config file, not even the info/exclude entry the staging would
// add — and adds no -m flag, leaving opencode on whatever config it
// resolves itself (the round dials the host's own opencode.json, exactly
// as before the bridge).
func TestStageOpencodeProviderNoOpenaiVars(t *testing.T) {
	wt := gitRepo(t)
	path, args, err := stageOpencodeProvider(ocEnv("PATH=/bin", "ANTHROPIC_API_KEY=sk-a"), wt)
	if err != nil {
		t.Fatalf("stageOpencodeProvider: %v", err)
	}
	if path != "" || args != nil {
		t.Errorf("stageOpencodeProvider = %q, %v; want an empty path and nil args", path, args)
	}
	if _, err := os.Stat(filepath.Join(wt, opencodeScratchDir)); !os.IsNotExist(err) {
		t.Errorf("scratch dir stat err = %v, want nothing staged", err)
	}
}

// TestStageOpencodeProviderUnbridgeableShapes pins the fail-before-launch
// guard: a key or model without a URL cannot be bridged (opencode reads no
// OPENAI_* var natively, so the round would silently dial whatever the
// host's own opencode.json holds), and a URL without a model has nothing
// to select — all three shapes fail the round and stage nothing.
func TestStageOpencodeProviderUnbridgeableShapes(t *testing.T) {
	for _, c := range []struct {
		name    string
		env     []string
		wantErr string
	}{
		{"key without url", ocEnv("OPENAI_API_KEY=sk-ambient"), "without a base URL"},
		{"model without url", ocEnv("OPENAI_MODEL=qwen"), "without a base URL"},
		{"url without model", ocEnv("OPENAI_BASE_URL=http://localhost:4000"), "without openai.model"},
	} {
		t.Run(c.name, func(t *testing.T) {
			wt := gitRepo(t)
			path, args, err := stageOpencodeProvider(c.env, wt)
			if err == nil {
				t.Fatalf("stageOpencodeProvider(%v) = %q, %v; want an error", c.env, path, args)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("error = %q, want the %q guard message", err, c.wantErr)
			}
			if path != "" || args != nil {
				t.Errorf("result = %q, %v; want nothing returned on the failure path", path, args)
			}
			if _, statErr := os.Stat(stagedOpencodeConfigPath(wt)); !os.IsNotExist(statErr) {
				t.Errorf("staged config stat err = %v, want nothing staged", statErr)
			}
		})
	}
}

// TestStageOpencodeProviderStagesConfig pins the happy bridge: a
// url+model round writes a round-scoped config holding exactly one
// daedalus-openai provider entry pointing at the round's endpoint, and
// returns the -m flag selecting its staged model. With a key the entry
// stages the {env:OPENAI_API_KEY} template — the literal key value must
// never land on disk; without one the apiKey key is omitted entirely so a
// keyless backend gets no auth header.
func TestStageOpencodeProviderStagesConfig(t *testing.T) {
	t.Run("keyed backend", func(t *testing.T) {
		wt := gitRepo(t)
		path, args, err := stageOpencodeProvider(ocEnv(
			"OPENAI_BASE_URL=https://llm.example/v1",
			"OPENAI_API_KEY=sk-secret-do-not-stage",
			"OPENAI_MODEL=glm",
		), wt)
		if err != nil {
			t.Fatalf("stageOpencodeProvider: %v", err)
		}
		if want := stagedOpencodeConfigPath(wt); path != want {
			t.Errorf("path = %q, want %q", path, want)
		}
		wantArgs := []string{"-m", opencodeProviderID + "/glm"}
		if !slices.Equal(args, wantArgs) {
			t.Errorf("args = %v, want %v", args, wantArgs)
		}
		baseURL, apiKey, _ := stagedOpencodeOptions(t, wt)
		if baseURL != "https://llm.example/v1" {
			t.Errorf("baseURL = %q, want the round's endpoint", baseURL)
		}
		if apiKey != "{env:OPENAI_API_KEY}" {
			t.Errorf("apiKey = %q, want the env template — the literal key value never lands on disk", apiKey)
		}
		entry := stagedOpencodeProviderRaw(t, wt)
		var npm string
		if err := json.Unmarshal(entry["npm"], &npm); err != nil {
			t.Fatalf("parse staged npm: %v", err)
		}
		if npm != "@ai-sdk/openai-compatible" {
			t.Errorf("npm = %q, want the openai-compatible wire package", npm)
		}
		id, model := stagedOpencodeModelRaw(t, wt)
		if id != "glm" {
			t.Errorf("staged model id = %q, want the round's model", id)
		}
		var name string
		if err := json.Unmarshal(model["name"], &name); err != nil {
			t.Fatalf("parse staged model name: %v", err)
		}
		if name != "glm" {
			t.Errorf("staged model name = %q, want the round's model", name)
		}
		if _, ok := model["limit"]; ok {
			t.Errorf("staged model carries a limit, want none without token exports")
		}
		// The key's value must not appear anywhere in the staged bytes.
		data, err := os.ReadFile(stagedOpencodeConfigPath(wt))
		if err != nil {
			t.Fatalf("reread staged config: %v", err)
		}
		if strings.Contains(string(data), "sk-secret-do-not-stage") {
			t.Errorf("staged config leaks the key value: %s", data)
		}
		// The scratch dir stays out of git's sight: excluded via
		// info/exclude exactly once, git agrees, and the tree stays clean.
		excl := gitInfoExclude(t, wt)
		exData, err := os.ReadFile(excl)
		if err != nil {
			t.Fatalf("read exclude: %v", err)
		}
		if got := strings.Count(string(exData), opencodeScratchDir+"/\n"); got != 1 {
			t.Errorf("exclude file carries the dir %d times, want exactly once: %q", got, exData)
		}
		if out, err := exec.Command("git", "-C", wt, "check-ignore", "-q",
			filepath.Join(opencodeScratchDir, "opencode.json")).CombinedOutput(); err != nil {
			t.Errorf("git check-ignore the staged config failed: %v: %s", err, out)
		}
		if out, err := exec.Command("git", "-C", wt, "status", "--porcelain").CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != "" {
			t.Errorf("git status after the stage = %q (%v), want a clean tree", out, err)
		}
	})

	// The load-bearing half of the keyless shape is the raw bytes:
	// APIKey's omitempty tag is what drops the key for a keyless backend,
	// and a decode-back (apiKey == "") cannot tell an omitted key from a
	// staged empty one.
	t.Run("keyless backend omits apiKey entirely", func(t *testing.T) {
		wt := gitRepo(t)
		if _, _, err := stageOpencodeProvider(ocEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1",
			"OPENAI_MODEL=qwen2.5-coder:3b",
		), wt); err != nil {
			t.Fatalf("stageOpencodeProvider: %v", err)
		}
		baseURL, apiKey, rawKeys := stagedOpencodeOptions(t, wt)
		if baseURL != "http://localhost:11434/v1" {
			t.Errorf("baseURL = %q, want the round's endpoint", baseURL)
		}
		if slices.Contains(rawKeys, "apiKey") {
			t.Errorf("staged options keys = %v, want no apiKey key at all for a keyless backend", rawKeys)
		}
		if apiKey != "" {
			t.Errorf("apiKey = %q, want empty", apiKey)
		}
	})

	// OPENAI_API_BASE rides the same value as OPENAI_BASE_URL for the
	// other agents' bridges (litellm versions disagree on which var they
	// honor); the opencode stage honors it as the fallback, so a failover
	// or ambient environment exporting only the older name still bridges.
	t.Run("OPENAI_API_BASE fallback", func(t *testing.T) {
		wt := gitRepo(t)
		if _, args, err := stageOpencodeProvider(ocEnv(
			"OPENAI_API_BASE=http://localhost:4000/v1",
			"OPENAI_MODEL=glm",
		), wt); err != nil {
			t.Fatalf("stageOpencodeProvider: %v", err)
		} else if want := []string{"-m", opencodeProviderID + "/glm"}; !slices.Equal(args, want) {
			t.Errorf("args = %v, want %v", args, want)
		}
		baseURL, _, _ := stagedOpencodeOptions(t, wt)
		if baseURL != "http://localhost:4000/v1" {
			t.Errorf("baseURL = %q, want OPENAI_API_BASE's value", baseURL)
		}
	})

	t.Run("OPENAI_BASE_URL wins over OPENAI_API_BASE", func(t *testing.T) {
		wt := gitRepo(t)
		if _, _, err := stageOpencodeProvider(ocEnv(
			"OPENAI_BASE_URL=https://primary.example/v1",
			"OPENAI_API_BASE=https://stale.example/v1",
			"OPENAI_MODEL=glm",
		), wt); err != nil {
			t.Fatalf("stageOpencodeProvider: %v", err)
		}
		baseURL, _, _ := stagedOpencodeOptions(t, wt)
		if baseURL != "https://primary.example/v1" {
			t.Errorf("baseURL = %q, want OPENAI_BASE_URL's value", baseURL)
		}
	})

	// opencode splits -m's operand on the FIRST slash into provider/model,
	// so a model id carrying slashes must stage and select verbatim —
	// truncating at the first slash would select a model that does not
	// exist on the staged provider.
	t.Run("slashed model id survives", func(t *testing.T) {
		wt := gitRepo(t)
		_, args, err := stageOpencodeProvider(ocEnv(
			"OPENAI_BASE_URL=https://llm.example/v1",
			"OPENAI_MODEL=hf/org/Qwen3-4B",
		), wt)
		if err != nil {
			t.Fatalf("stageOpencodeProvider: %v", err)
		}
		wantArgs := []string{"-m", opencodeProviderID + "/hf/org/Qwen3-4B"}
		if !slices.Equal(args, wantArgs) {
			t.Errorf("args = %v, want %v", args, wantArgs)
		}
		id, _ := stagedOpencodeModelRaw(t, wt)
		if id != "hf/org/Qwen3-4B" {
			t.Errorf("staged model id = %q, want the full slashed model id", id)
		}
	})
}

// TestStageOpencodeProviderStagesLimits pins the token-budget channel: the
// config's context window and completion cap exports land as limit.context
// and limit.output on the staged model entry, and a non-positive or
// garbage export stages no limit at all — opencode's own defaults stand,
// per the agnostic rule.
func TestStageOpencodeProviderStagesLimits(t *testing.T) {
	assertLimit := func(t *testing.T, wt string, wantContext, wantOutput int, wantKeys []string) {
		t.Helper()
		_, model := stagedOpencodeModelRaw(t, wt)
		limitRaw, ok := model["limit"]
		if !ok {
			if wantKeys != nil {
				t.Fatalf("staged model carries no limit, want keys %v", wantKeys)
			}
			return
		}
		if wantKeys == nil {
			t.Fatalf("staged model carries a limit (%s), want none", limitRaw)
		}
		var limit map[string]json.RawMessage
		if err := json.Unmarshal(limitRaw, &limit); err != nil {
			t.Fatalf("parse staged limit: %v", err)
		}
		gotKeys := keysOf(limit)
		slices.Sort(gotKeys)
		if !slices.Equal(gotKeys, wantKeys) {
			t.Errorf("staged limit keys = %v, want %v", gotKeys, wantKeys)
		}
		if wantContext != 0 {
			var n int
			if err := json.Unmarshal(limit["context"], &n); err != nil || n != wantContext {
				t.Errorf("limit.context = %s (%v), want %d", limit["context"], err, wantContext)
			}
		}
		if wantOutput != 0 {
			var n int
			if err := json.Unmarshal(limit["output"], &n); err != nil || n != wantOutput {
				t.Errorf("limit.output = %s (%v), want %d", limit["output"], err, wantOutput)
			}
		}
	}

	t.Run("context window stages limit.context", func(t *testing.T) {
		wt := gitRepo(t)
		if _, _, err := stageOpencodeProvider(ocEnv(
			"OPENAI_BASE_URL=https://llm.example/v1",
			"OPENAI_MODEL=glm",
			config.ContextTokensEnv+"=131072",
		), wt); err != nil {
			t.Fatalf("stageOpencodeProvider: %v", err)
		}
		assertLimit(t, wt, 131072, 0, []string{"context"})
	})

	t.Run("completion cap stages limit.output", func(t *testing.T) {
		wt := gitRepo(t)
		if _, _, err := stageOpencodeProvider(ocEnv(
			"OPENAI_BASE_URL=https://llm.example/v1",
			"OPENAI_MODEL=glm",
			config.MaxOutputTokensEnv+"=8192",
		), wt); err != nil {
			t.Fatalf("stageOpencodeProvider: %v", err)
		}
		assertLimit(t, wt, 0, 8192, []string{"output"})
	})

	t.Run("both exports stage both sides", func(t *testing.T) {
		wt := gitRepo(t)
		if _, _, err := stageOpencodeProvider(ocEnv(
			"OPENAI_BASE_URL=https://llm.example/v1",
			"OPENAI_MODEL=glm",
			config.ContextTokensEnv+"=131072",
			config.MaxOutputTokensEnv+"=8192",
		), wt); err != nil {
			t.Fatalf("stageOpencodeProvider: %v", err)
		}
		assertLimit(t, wt, 131072, 8192, []string{"context", "output"})
	})

	// Zero is the config's documented unset default and a garbage value is
	// an ambient accident; neither is a budget.
	for _, c := range []struct{ name, context, output string }{
		{"zero exports", "0", "0"},
		{"negative exports", "-1", "-1"},
		{"garbage exports", "not-a-number", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			wt := gitRepo(t)
			env := ocEnv(
				"OPENAI_BASE_URL=https://llm.example/v1",
				"OPENAI_MODEL=glm",
				config.ContextTokensEnv+"="+c.context,
			)
			if c.output != "" {
				env = append(env, config.MaxOutputTokensEnv+"="+c.output)
			}
			if _, _, err := stageOpencodeProvider(env, wt); err != nil {
				t.Fatalf("stageOpencodeProvider: %v", err)
			}
			assertLimit(t, wt, 0, 0, nil)
		})
	}
}

// TestOpencodeRoundStagesProvider pins the runJailedRound wiring end to
// end: an opencode round served by the openai section launches ai-jail
// with the state-dir bridge, the OPENCODE_CONFIG passthrough riding the
// staged config path in its environment, and the staged -m appended after
// the headless set (where the run subcommand defines it); a round with no
// openai exports launches with neither the passthrough nor -m, the argv
// byte-identical to the unbridged shape; a resumed round's precomposed
// argv still gains the -m after its run -s tokens; and a half-specified
// section fails the round before launch.
func TestOpencodeRoundStagesProvider(t *testing.T) {
	t.Run("openai-served round carries the staged config and -m", func(t *testing.T) {
		scrubBugFilingEnv(t)
		ocMount := opencodeTestEnv(t)
		t.Setenv("DAEDALUS_AGENT", "opencode")
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
		staged := stagedOpencodeConfigPath(wt)
		assertArgs(t, calls[0].Args, slices.Concat(
			[]string{"--worktree",
				"--network",
				"--no-save-config",
				"--mask",
				".claude/settings.json",
				"--mask",
				".claude/settings.local.json",
				"--mask",
				homeClaudeMask(t)},
			ocMount,
			[]string{"--env",
				"OPENCODE_CONFIG",
				"--",
				"opencode",
				"run",
				"--auto",
				"--standalone",
				"-m",
				opencodeProviderID + "/glm-selfhost"},
		), "ai-jail")
		// The passthrough flag is only half the bridge: --env copies the
		// value from ai-jail's own environment, so the round env must carry
		// the staged path — a dropped setEnvVar here would leave the flag
		// copying an unset var and the round silently resolving its own
		// config again.
		if got := calls[0].Env["OPENCODE_CONFIG"]; got != staged {
			t.Errorf("round env OPENCODE_CONFIG = %q, want the staged config path %q", got, staged)
		}
		// The bytes the jailed opencode would read carry the bridge.
		baseURL, apiKey, _ := stagedOpencodeOptions(t, wt)
		if baseURL != "https://selfhost.example/v1" {
			t.Errorf("staged baseURL = %q, want the round's endpoint", baseURL)
		}
		if apiKey != "{env:OPENAI_API_KEY}" {
			t.Errorf("staged apiKey = %q, want the env template", apiKey)
		}
	})

	t.Run("plain round launches with no passthrough and no -m", func(t *testing.T) {
		scrubBugFilingEnv(t)
		ocMount := opencodeTestEnv(t)
		t.Setenv("DAEDALUS_AGENT", "opencode")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
			WorktreePath: gitRepo(t),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedClaudeActivity: %v", err)
		}

		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		assertArgs(t, calls[0].Args, slices.Concat(
			[]string{"--worktree",
				"--network",
				"--no-save-config",
				"--mask",
				".claude/settings.json",
				"--mask",
				".claude/settings.local.json",
				"--mask",
				homeClaudeMask(t)},
			ocMount,
			[]string{"--", "opencode", "run", "--auto", "--standalone"},
		), "ai-jail")
	})

	// The resumed shape arrives with the whole post-`--` argv precomposed
	// (headless nil), so the held -m must follow the run -s tokens — the
	// same slot codex's overrides take in its exec resume shape.
	t.Run("resumed round appends -m after the run -s tokens", func(t *testing.T) {
		scrubBugFilingEnv(t)
		ocMount := opencodeTestEnv(t)
		t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
		t.Setenv("OPENAI_MODEL", "glm-selfhost")
		wt := gitRepo(t)
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := runJailedRound(context.Background(), os.Environ(), "", "opencode", wt, "fix the bug",
			agentResumeArgs("opencode", "oc-1", nil)...); err != nil {
			t.Fatalf("runJailedRound: %v", err)
		}

		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		assertArgs(t, calls[0].Args, slices.Concat(
			[]string{"--worktree",
				"--network",
				"--no-save-config",
				"--mask",
				".claude/settings.json",
				"--mask",
				".claude/settings.local.json",
				"--mask",
				homeClaudeMask(t)},
			ocMount,
			[]string{"--env",
				"OPENCODE_CONFIG",
				"--",
				"opencode",
				"run",
				"-s",
				"oc-1",
				"--auto",
				"--standalone",
				"-m",
				opencodeProviderID + "/glm-selfhost"},
		), "ai-jail")
	})

	t.Run("ambient key without url fails before launch", func(t *testing.T) {
		scrubBugFilingEnv(t)
		opencodeTestEnv(t)
		t.Setenv("DAEDALUS_AGENT", "opencode")
		t.Setenv("OPENAI_API_KEY", "sk-ambient")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		_, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		})
		if err == nil {
			t.Fatal("RunJailedClaudeActivity = nil error, want the unbridgeable-shape failure")
		}
		if !strings.Contains(err.Error(), "without a base URL") {
			t.Errorf("error = %q, want the opencode no-base-URL guard message", err)
		}
		if _, statErr := os.Stat(log); !os.IsNotExist(statErr) {
			calls := readCalls(t, log)
			t.Errorf("ai-jail called %d times, want the round failed before launch (%v)", len(calls), calls)
		}
	})

	t.Run("url without model fails before launch", func(t *testing.T) {
		scrubBugFilingEnv(t)
		opencodeTestEnv(t)
		t.Setenv("DAEDALUS_AGENT", "opencode")
		t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		_, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		})
		if err == nil {
			t.Fatal("RunJailedClaudeActivity = nil error, want the unbridgeable-shape failure")
		}
		if !strings.Contains(err.Error(), "without openai.model") {
			t.Errorf("error = %q, want the opencode no-model guard message", err)
		}
		if _, statErr := os.Stat(log); !os.IsNotExist(statErr) {
			calls := readCalls(t, log)
			t.Errorf("ai-jail called %d times, want the round failed before launch (%v)", len(calls), calls)
		}
	})
}
