package activities

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
)

// piTestHome redirects the host home the staging writes through
// (~/.pi/agent/models.json) at a fresh temp dir and returns it — a test
// must never touch the real ~/.pi of whatever host runs the suite.
func piTestHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	return home
}

// piEnv builds a round env slice from K=V pairs, the shape stagePiProvider
// reads (the round's explicit env, not the process's).
func piEnv(kv ...string) []string {
	return kv
}

// piModelsPath returns the staged provider config path under a test home.
func piModelsPath(home string) string {
	return filepath.Join(home, ".pi", "agent", "models.json")
}

// readStagedPiModels parses the models.json a stage wrote, failing the test
// when the file is missing or unparseable — every staging assertion starts
// from the file pi would actually read at round startup.
func readStagedPiModels(t *testing.T, home string) map[string]json.RawMessage {
	t.Helper()
	data, err := os.ReadFile(piModelsPath(home))
	if err != nil {
		t.Fatalf("read staged models.json: %v", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		t.Fatalf("parse staged models.json: %v", err)
	}
	return top
}

// stagedPiProvider decodes the daedalus-openai entry out of a staged
// models.json, keyed the way pi's provider lookup keys it.
func stagedPiProvider(t *testing.T, home string) piProviderConfig {
	t.Helper()
	top := readStagedPiModels(t, home)
	raw, ok := top["providers"]
	if !ok {
		t.Fatal("staged models.json has no providers object")
	}
	var providers map[string]piProviderConfig
	if err := json.Unmarshal(raw, &providers); err != nil {
		t.Fatalf("parse staged providers: %v", err)
	}
	entry, ok := providers[piProviderID]
	if !ok {
		t.Fatalf("staged providers %v carry no %s entry", keysOf(providers), piProviderID)
	}
	return entry
}

// TestStagePiProviderNoOpenaiVars pins the pass-through shape: a round env
// with none of the openai exports stages nothing (no models.json is even
// created) and adds no --model flag, leaving pi on its own config.
func TestStagePiProviderNoOpenaiVars(t *testing.T) {
	home := piTestHome(t)
	args, err := stagePiProvider(piEnv("PATH=/bin", "ANTHROPIC_API_KEY=sk-a"))
	if err != nil {
		t.Fatalf("stagePiProvider: %v", err)
	}
	if args != nil {
		t.Errorf("stagePiProvider args = %v, want nil", args)
	}
	if _, err := os.Stat(piModelsPath(home)); !os.IsNotExist(err) {
		t.Errorf("models.json stat err = %v, want no file staged", err)
	}
}

// TestStagePiProviderUnbridgeableShapes pins the fail-before-launch guard:
// a key or model without a URL cannot be bridged (pi has no base-URL env
// channel, so the round would silently dial api.openai.com on its built-in
// openai provider), and a URL without a model has nothing to select — all
// three shapes fail the round and stage nothing.
func TestStagePiProviderUnbridgeableShapes(t *testing.T) {
	for _, c := range []struct {
		name string
		env  []string
	}{
		{"key without url", piEnv("OPENAI_API_KEY=sk-ambient")},
		{"model without url", piEnv("OPENAI_MODEL=qwen")},
		{"url without model", piEnv("OPENAI_BASE_URL=http://localhost:4000")},
	} {
		t.Run(c.name, func(t *testing.T) {
			home := piTestHome(t)
			args, err := stagePiProvider(c.env)
			if err == nil {
				t.Fatalf("stagePiProvider(%v) = %v, want an error", c.env, args)
			}
			if args != nil {
				t.Errorf("args = %v, want nil on the failure path", args)
			}
			if _, err := os.Stat(piModelsPath(home)); !os.IsNotExist(err) {
				t.Errorf("models.json stat err = %v, want nothing staged", err)
			}
		})
	}
}

// TestStagePiProviderStagesEntry pins the happy bridge: a url+model round
// stages a daedalus-openai provider entry pointing at the round's endpoint
// and returns the --model flag selecting it. Without a key the entry
// carries the documented "none" placeholder (keyless backends ignore the
// header); with one, the key is staged as the ${OPENAI_API_KEY} template —
// the literal key value must never land on disk.
func TestStagePiProviderStagesEntry(t *testing.T) {
	t.Run("keyless backend", func(t *testing.T) {
		home := piTestHome(t)
		args, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1",
			"OPENAI_MODEL=qwen2.5-coder:3b",
		))
		if err != nil {
			t.Fatalf("stagePiProvider: %v", err)
		}
		want := []string{"--model", piProviderID + "/qwen2.5-coder:3b"}
		if !slices.Equal(args, want) {
			t.Errorf("args = %v, want %v", args, want)
		}
		entry := stagedPiProvider(t, home)
		if entry.BaseURL != "http://localhost:11434/v1" {
			t.Errorf("baseUrl = %q, want the round's endpoint", entry.BaseURL)
		}
		if entry.API != "openai-completions" {
			t.Errorf("api = %q, want openai-completions", entry.API)
		}
		if entry.APIKey != "none" {
			t.Errorf("apiKey = %q, want the keyless placeholder", entry.APIKey)
		}
		if len(entry.Models) != 1 || entry.Models[0].ID != "qwen2.5-coder:3b" {
			t.Errorf("models = %+v, want one entry for the round's model", entry.Models)
		}
		if entry.Models[0].SamplingParams != nil {
			t.Errorf("samplingParams = %v, want none staged without sampler exports", entry.Models[0].SamplingParams)
		}
	})

	t.Run("key rides the template, never disk", func(t *testing.T) {
		home := piTestHome(t)
		args, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=https://llm.example/v1",
			"OPENAI_API_KEY=sk-secret-do-not-stage",
			"OPENAI_MODEL=glm",
		))
		if err != nil {
			t.Fatalf("stagePiProvider: %v", err)
		}
		if want := "--model " + piProviderID + "/glm"; !slices.Equal(args, []string{"--model", piProviderID + "/glm"}) {
			t.Errorf("args = %v, want [%s]", args, want)
		}
		entry := stagedPiProvider(t, home)
		if entry.APIKey != "${OPENAI_API_KEY}" {
			t.Errorf("apiKey = %q, want the env template", entry.APIKey)
		}
		data, err := os.ReadFile(piModelsPath(home))
		if err != nil {
			t.Fatalf("reread staged models.json: %v", err)
		}
		if strings.Contains(string(data), "sk-secret-do-not-stage") {
			t.Error("staged models.json embeds the literal key value")
		}
	})
}

// TestStagePiProviderAPIBaseFallback pins the var precedence: an env
// carrying only OPENAI_API_BASE (litellm's channel — AgentEnv exports both,
// failover sets both) is bridged at that URL, not rejected as URL-less.
func TestStagePiProviderAPIBaseFallback(t *testing.T) {
	home := piTestHome(t)
	args, err := stagePiProvider(piEnv(
		"OPENAI_API_BASE=http://localhost:4000",
		"OPENAI_MODEL=glm-backup",
	))
	if err != nil {
		t.Fatalf("stagePiProvider: %v", err)
	}
	if !slices.Equal(args, []string{"--model", piProviderID + "/glm-backup"}) {
		t.Errorf("args = %v, want the staged model flag", args)
	}
	if entry := stagedPiProvider(t, home); entry.BaseURL != "http://localhost:4000" {
		t.Errorf("baseUrl = %q, want the OPENAI_API_BASE value", entry.BaseURL)
	}
}

// TestStagePiProviderModelMetadata pins the staged model's metadata: the
// context window and completion cap ride their env channels into
// contextWindow/maxTokens (a fresh host has no other source for pi's
// context arithmetic), and a non-positive or malformed export leaves the
// field out rather than staging a zero.
func TestStagePiProviderModelMetadata(t *testing.T) {
	t.Run("valid exports stage both sides", func(t *testing.T) {
		home := piTestHome(t)
		if _, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1",
			"OPENAI_MODEL=qwen",
			config.ContextTokensEnv+"=131072",
			config.MaxOutputTokensEnv+"=16384",
		)); err != nil {
			t.Fatalf("stagePiProvider: %v", err)
		}
		model := stagedPiProvider(t, home).Models[0]
		if model.ContextWindow != 131072 {
			t.Errorf("contextWindow = %d, want 131072", model.ContextWindow)
		}
		if model.MaxTokens != 16384 {
			t.Errorf("maxTokens = %d, want 16384", model.MaxTokens)
		}
	})

	t.Run("malformed or zero exports stage nothing", func(t *testing.T) {
		home := piTestHome(t)
		if _, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1",
			"OPENAI_MODEL=qwen",
			config.ContextTokensEnv+"=abc",
			config.MaxOutputTokensEnv+"=0",
		)); err != nil {
			t.Fatalf("stagePiProvider: %v", err)
		}
		model := stagedPiProvider(t, home).Models[0]
		if model.ContextWindow != 0 || model.MaxTokens != 0 {
			t.Errorf("model = %+v, want both metadata fields omitted", model)
		}
	})
}

// TestStagePiProviderSamplingParams pins the sampler bridge: DAEDALUS_*
// exports land as samplingParams on the staged model, and a non-finite
// export (the stale-ambient DAEDALUS_TOP_P=nan class) is dropped like any
// malformed var instead of blowing up the staged JSON — while the finite
// siblings in the same env still stage.
func TestStagePiProviderSamplingParams(t *testing.T) {
	t.Run("finite exports stage as samplingParams", func(t *testing.T) {
		home := piTestHome(t)
		if _, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1",
			"OPENAI_MODEL=qwen",
			config.TopPEnv+"=0.9",
			config.TopKEnv+"=40",
			config.MinPEnv+"=0.05",
			config.TemperatureEnv+"=0.2",
			config.PresencePenaltyEnv+"=0.5",
			config.RepetitionPenaltyEnv+"=1.1",
		)); err != nil {
			t.Fatalf("stagePiProvider: %v", err)
		}
		params := stagedPiProvider(t, home).Models[0].SamplingParams
		for k, want := range map[string]float64{
			"top_p": 0.9, "top_k": 40, "min_p": 0.05, "temperature": 0.2,
			"presence_penalty": 0.5, "repetition_penalty": 1.1,
		} {
			got, ok := params[k].(float64)
			if !ok || got != want {
				t.Errorf("samplingParams[%s] = %v, want %v", k, params[k], want)
			}
		}
	})

	t.Run("non-finite export dropped, finite siblings kept", func(t *testing.T) {
		home := piTestHome(t)
		if _, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1",
			"OPENAI_MODEL=qwen",
			config.TopPEnv+"=nan",
			config.TopKEnv+"=40",
		)); err != nil {
			t.Fatalf("stagePiProvider: %v", err)
		}
		params := stagedPiProvider(t, home).Models[0].SamplingParams
		if _, ok := params["top_p"]; ok {
			t.Errorf("samplingParams top_p = %v, want the non-finite export dropped", params["top_p"])
		}
		if got, ok := params["top_k"].(float64); !ok || got != 40 {
			t.Errorf("samplingParams top_k = %v, want 40 kept", params["top_k"])
		}
	})
}

// TestStagePiProviderSlashModel pins the --model reference shape: the id is
// appended whole after the provider prefix, because pi splits
// provider/model on the FIRST slash — a huggingface-style model ref must
// survive intact.
func TestStagePiProviderSlashModel(t *testing.T) {
	piTestHome(t)
	args, err := stagePiProvider(piEnv(
		"OPENAI_BASE_URL=http://localhost:8000/v1",
		"OPENAI_MODEL=unsloth/Qwen3-4B-GGUF/Q4_K_M",
	))
	if err != nil {
		t.Fatalf("stagePiProvider: %v", err)
	}
	want := "--model " + piProviderID + "/unsloth/Qwen3-4B-GGUF/Q4_K_M"
	if !slices.Equal(args, []string{"--model", piProviderID + "/unsloth/Qwen3-4B-GGUF/Q4_K_M"}) {
		t.Errorf("args = %v, want [%s]", args, want)
	}
}

// TestSamplerFromEnvRejectsNonFinite pins the non-finite guard on the
// sampler env channel directly: NaN and infinities parse as floats but can
// never survive into a staged file (pi's json.Marshal rejects them; aider's
// YAML would render a junk float), so they are dropped like any malformed
// var — while a finite value still parses through.
func TestSamplerFromEnvRejectsNonFinite(t *testing.T) {
	for _, c := range []struct {
		value string
		want  bool
	}{
		{"0.9", true},
		{"nan", false},
		{"NaN", false},
		{"inf", false},
		{"-inf", false},
		{"garbage", false},
		{"", false},
	} {
		t.Run(c.value, func(t *testing.T) {
			s := samplerFromEnv(piEnv(config.TopPEnv + "=" + c.value))
			if got := s.TopP != nil; got != c.want {
				t.Errorf("samplerFromEnv TOP_P=%q TopP parsed = %v, want %v", c.value, got, c.want)
			}
			if s.TopP != nil && *s.TopP != 0.9 {
				t.Errorf("TopP = %v, want the parsed value", *s.TopP)
			}
		})
	}
}

// TestMergePiModelsEntryFreshHost pins the fresh-host case the bridge
// targets: no models.json exists at all, and the first staged round creates
// it (an assignment into a nil map would panic every openai-served pi
// round on exactly this host).
func TestMergePiModelsEntryFreshHost(t *testing.T) {
	home := piTestHome(t)
	entry := piProviderConfig{
		Name:    "Daedalus (openai section)",
		BaseURL: "http://localhost:11434/v1",
		API:     "openai-completions",
		APIKey:  "none",
		Models:  []piModelConfig{{ID: "qwen"}},
	}
	if err := mergePiModelsEntry(entry); err != nil {
		t.Fatalf("mergePiModelsEntry: %v", err)
	}
	if got := stagedPiProvider(t, home); got.BaseURL != entry.BaseURL || len(got.Models) != 1 || got.Models[0].ID != "qwen" {
		t.Errorf("staged entry = %+v, want the merged entry", got)
	}
}

// TestMergePiModelsEntryPreservesForeignContent pins the round-trip
// contract: everything daedalus does not own in models.json — pi's own
// top-level fields and other providers' entries, including fields these
// structs do not model — survives a stage with its values intact (the file
// is re-indented, but no data is dropped), and a re-stage replaces only the
// daedalus entry (upsert, never duplicate).
func TestMergePiModelsEntryPreservesForeignContent(t *testing.T) {
	home := piTestHome(t)
	path := piModelsPath(home)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"providers":{"other":{"baseUrl":"http://elsewhere:8000/v1","nested":{"deep":[1,2]}}},"theme":"dark"}`), 0o644); err != nil {
		t.Fatalf("seed models.json: %v", err)
	}

	write := func(model string) {
		t.Helper()
		entry := piProviderConfig{
			Name: "Daedalus (openai section)", BaseURL: "http://localhost:11434/v1",
			API: "openai-completions", APIKey: "none",
			Models: []piModelConfig{{ID: model}},
		}
		if err := mergePiModelsEntry(entry); err != nil {
			t.Fatalf("mergePiModelsEntry: %v", err)
		}
	}

	write("qwen")
	top := readStagedPiModels(t, home)
	if string(top["theme"]) != `"dark"` {
		t.Errorf("top-level theme = %s, want it preserved", top["theme"])
	}
	var providers map[string]json.RawMessage
	if err := json.Unmarshal(top["providers"], &providers); err != nil {
		t.Fatalf("parse providers: %v", err)
	}
	var other map[string]any
	if err := json.Unmarshal(providers["other"], &other); err != nil {
		t.Fatalf("parse other provider entry: %v", err)
	}
	wantOther := map[string]any{
		"baseUrl": "http://elsewhere:8000/v1",
		"nested":  map[string]any{"deep": []any{1.0, 2.0}},
	}
	if !reflect.DeepEqual(other, wantOther) {
		t.Errorf("other provider entry = %s, want it preserved verbatim", providers["other"])
	}

	write("glm")
	providers = map[string]json.RawMessage{}
	top = readStagedPiModels(t, home)
	if err := json.Unmarshal(top["providers"], &providers); err != nil {
		t.Fatalf("parse providers after re-stage: %v", err)
	}
	if len(providers) != 2 {
		t.Errorf("providers = %v, want exactly the other entry plus one daedalus entry", keysOf(providers))
	}
	var mine piProviderConfig
	if err := json.Unmarshal(providers[piProviderID], &mine); err != nil {
		t.Fatalf("parse daedalus entry: %v", err)
	}
	if len(mine.Models) != 1 || mine.Models[0].ID != "glm" {
		t.Errorf("daedalus entry models = %+v, want the re-stage to replace the old one", mine.Models)
	}
}

// TestMergePiModelsEntryRejectsJSONC pins the loud-failure case: a
// models.json pi would accept but Go cannot parse (JSON comments) fails the
// stage instead of silently clobbering the file it could not round-trip —
// and the file is left untouched for the operator to fix.
func TestMergePiModelsEntryRejectsJSONC(t *testing.T) {
	home := piTestHome(t)
	path := piModelsPath(home)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seed := "{\n// pi tolerates this, Go does not\n\"providers\": {}\n}"
	if err := os.WriteFile(path, []byte(seed), 0o644); err != nil {
		t.Fatalf("seed models.json: %v", err)
	}
	entry := piProviderConfig{
		Name: "Daedalus (openai section)", BaseURL: "http://localhost:11434/v1",
		API: "openai-completions", APIKey: "none",
		Models: []piModelConfig{{ID: "qwen"}},
	}
	if err := mergePiModelsEntry(entry); err == nil {
		t.Fatal("mergePiModelsEntry on a JSONC file = nil error, want a loud failure")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reread models.json: %v", err)
	}
	if string(data) != seed {
		t.Error("a rejected merge modified the unparseable models.json; it must stay untouched")
	}
}

// TestPiRoundStagesProvider pins the runJailedRound wiring end to end: a
// pi round served by the openai section launches ai-jail with the staged
// --model flag, the staged config is on the host disk the jail's pi preset
// mounts, and a pi round with no openai exports launches with no --model
// at all.
func TestPiRoundStagesProvider(t *testing.T) {
	t.Run("openai-served round carries the staged model flag", func(t *testing.T) {
		home := piTestHome(t)
		t.Setenv("DAEDALUS_AGENT", "pi")
		t.Setenv("OPENAI_BASE_URL", "http://localhost:11434/v1")
		t.Setenv("OPENAI_API_KEY", "sk-local")
		t.Setenv("OPENAI_MODEL", "qwen2.5-coder:3b")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedClaudeActivity: %v", err)
		}

		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		args := calls[0].Args
		pi := slices.Index(args, "pi")
		if pi < 0 {
			t.Fatalf("args %v never name the pi CLI", args)
		}
		model := slices.Index(args, "--model")
		if model < 0 || model < pi {
			t.Fatalf("args %v carry no --model after the pi selection", args)
		}
		if got := args[model+1]; got != piProviderID+"/qwen2.5-coder:3b" {
			t.Errorf("--model = %q, want the staged provider reference", got)
		}
		entry := stagedPiProvider(t, home)
		if entry.BaseURL != "http://localhost:11434/v1" {
			t.Errorf("staged baseUrl = %q, want the round's endpoint", entry.BaseURL)
		}
	})

	t.Run("plain pi round launches with no model flag", func(t *testing.T) {
		home := piTestHome(t)
		t.Setenv("DAEDALUS_AGENT", "pi")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedClaudeActivity: %v", err)
		}

		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		if slices.Contains(calls[0].Args, "--model") {
			t.Errorf("args %v carry a --model flag without any openai export", calls[0].Args)
		}
		if _, err := os.Stat(piModelsPath(home)); !os.IsNotExist(err) {
			t.Errorf("models.json stat err = %v, want nothing staged", err)
		}
	})

	t.Run("ambient key without url fails before launch", func(t *testing.T) {
		piTestHome(t)
		t.Setenv("DAEDALUS_AGENT", "pi")
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
		// Name the guard in the failure: a regression that launches anyway
		// must be distinguishable from one that fails for another reason.
		if !strings.Contains(err.Error(), "without a base URL") {
			t.Errorf("error = %q, want the pi no-base-URL guard message", err)
		}
		// No call is the point: the stub log is never even created.
		if _, statErr := os.Stat(log); !os.IsNotExist(statErr) {
			calls := readCalls(t, log)
			t.Errorf("ai-jail called %d times, want the round failed before launch (%v)", len(calls), calls)
		}
	})
}

// piSettingsPath returns the staged retry-timeout path under a test home.
func piSettingsPath(home string) string {
	return filepath.Join(home, ".pi", "agent", "settings.json")
}

// TestStagePiProviderStagesRetryTimeout pins the timeout bridge: a positive
// API_TIMEOUT_MS export — the same ceiling every other agent gets as the
// exported var — lands as retry.provider.timeoutMs in the host's
// settings.json (pi's only request-timeout channel), and models.json is
// still staged alongside it.
func TestStagePiProviderStagesRetryTimeout(t *testing.T) {
	home := piTestHome(t)
	if _, err := stagePiProvider(piEnv(
		"OPENAI_BASE_URL=http://localhost:11434/v1",
		"OPENAI_MODEL=qwen",
		"API_TIMEOUT_MS=900000",
	)); err != nil {
		t.Fatalf("stagePiProvider: %v", err)
	}
	data, err := os.ReadFile(piSettingsPath(home))
	if err != nil {
		t.Fatalf("read staged settings.json: %v", err)
	}
	var top struct {
		Retry struct {
			Provider struct {
				TimeoutMS int `json:"timeoutMs"`
			} `json:"provider"`
		} `json:"retry"`
	}
	if err := json.Unmarshal(data, &top); err != nil {
		t.Fatalf("parse staged settings.json: %v", err)
	}
	if top.Retry.Provider.TimeoutMS != 900000 {
		t.Errorf("retry.provider.timeoutMs = %d, want 900000", top.Retry.Provider.TimeoutMS)
	}
	if entry := stagedPiProvider(t, home); entry.BaseURL != "http://localhost:11434/v1" {
		t.Errorf("staged baseUrl = %q, want the models.json bridge unaffected", entry.BaseURL)
	}
}

// TestStagePiProviderNoTimeoutExportStagesNoSettings pins the gate: the
// timeout merge derives from the exported var, so an absent, malformed, or
// non-positive value leaves pi's own 5-minute default standing — no
// settings.json is created at all.
func TestStagePiProviderNoTimeoutExportStagesNoSettings(t *testing.T) {
	for _, c := range []struct {
		name string
		env  []string
	}{
		{"absent export", piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1", "OPENAI_MODEL=qwen")},
		{"malformed export", piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1", "OPENAI_MODEL=qwen",
			"API_TIMEOUT_MS=abc")},
		{"zero export", piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1", "OPENAI_MODEL=qwen",
			"API_TIMEOUT_MS=0")},
		{"negative export", piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1", "OPENAI_MODEL=qwen",
			"API_TIMEOUT_MS=-5")},
	} {
		t.Run(c.name, func(t *testing.T) {
			home := piTestHome(t)
			if _, err := stagePiProvider(c.env); err != nil {
				t.Fatalf("stagePiProvider: %v", err)
			}
			if _, err := os.Stat(piSettingsPath(home)); !os.IsNotExist(err) {
				t.Errorf("settings.json stat err = %v, want nothing staged", err)
			}
		})
	}
}

// TestMergePiRetryTimeoutPreservesUserSettings pins the merge contract:
// daedalus owns exactly retry.provider.timeoutMs — the host user's other
// top-level settings, the rest of their retry block, and the rest of their
// provider block survive verbatim, and a re-stage upserts the one key
// instead of accumulating duplicates.
func TestMergePiRetryTimeoutPreservesUserSettings(t *testing.T) {
	home := piTestHome(t)
	path := piSettingsPath(home)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"theme":"dark","retry":{"attempts":5,"provider":{"apiKey":"sk-user","timeoutMs":300000}}}`), 0o644); err != nil {
		t.Fatalf("seed settings.json: %v", err)
	}
	if err := mergePiRetryTimeout(900000); err != nil {
		t.Fatalf("mergePiRetryTimeout: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read merged settings.json: %v", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		t.Fatalf("parse merged settings.json: %v", err)
	}
	if string(top["theme"]) != `"dark"` {
		t.Errorf("theme = %s, want it preserved", top["theme"])
	}
	var retry map[string]json.RawMessage
	if err := json.Unmarshal(top["retry"], &retry); err != nil {
		t.Fatalf("parse retry: %v", err)
	}
	if string(retry["attempts"]) != `5` {
		t.Errorf("retry.attempts = %s, want it preserved", retry["attempts"])
	}
	var provider map[string]json.RawMessage
	if err := json.Unmarshal(retry["provider"], &provider); err != nil {
		t.Fatalf("parse retry.provider: %v", err)
	}
	if string(provider["apiKey"]) != `"sk-user"` {
		t.Errorf("retry.provider.apiKey = %s, want it preserved", provider["apiKey"])
	}
	if string(provider["timeoutMs"]) != `900000` {
		t.Errorf("retry.provider.timeoutMs = %s, want the daedalus value to win", provider["timeoutMs"])
	}

	// A re-stage replaces the one owned key in place.
	if err := mergePiRetryTimeout(600000); err != nil {
		t.Fatalf("re-mergePiRetryTimeout: %v", err)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("reread merged settings.json: %v", err)
	}
	for _, key := range []string{`"apiKey"`, `"attempts"`, `"theme"`} {
		if n := strings.Count(string(data), key); n != 1 {
			t.Errorf("merged settings.json carries %q %d times, want exactly 1", key, n)
		}
	}
	var recheck struct {
		Retry struct {
			Provider struct {
				TimeoutMS int `json:"timeoutMs"`
			} `json:"provider"`
		} `json:"retry"`
	}
	if err := json.Unmarshal(data, &recheck); err != nil {
		t.Fatalf("parse re-merged settings.json: %v", err)
	}
	if recheck.Retry.Provider.TimeoutMS != 600000 {
		t.Errorf("retry.provider.timeoutMs = %d, want the re-stage value", recheck.Retry.Provider.TimeoutMS)
	}
}

// TestMergePiRetryTimeoutFreshHost pins the no-file case: a host with no
// settings.json at all gets one created carrying just the staged timeout —
// the merge must not depend on a pre-existing file.
func TestMergePiRetryTimeoutFreshHost(t *testing.T) {
	home := piTestHome(t)
	if err := mergePiRetryTimeout(900000); err != nil {
		t.Fatalf("mergePiRetryTimeout: %v", err)
	}
	data, err := os.ReadFile(piSettingsPath(home))
	if err != nil {
		t.Fatalf("read staged settings.json: %v", err)
	}
	if !strings.Contains(string(data), `"timeoutMs": 900000`) {
		t.Errorf("staged settings.json = %s, want the timeout key", data)
	}
}

// TestMergePiRetryTimeoutRejectsJSONC pins the loud-failure case: settings.json
// is parsed by pi with plain JSON.parse (unlike models.json), so a file with
// comments is already broken for pi itself — the merge fails instead of
// clobbering it, and the file stays untouched for the operator to fix.
func TestMergePiRetryTimeoutRejectsJSONC(t *testing.T) {
	home := piTestHome(t)
	path := piSettingsPath(home)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seed := "{\n// pi does not tolerate this either\n\"retry\": {}\n}"
	if err := os.WriteFile(path, []byte(seed), 0o644); err != nil {
		t.Fatalf("seed settings.json: %v", err)
	}
	if err := mergePiRetryTimeout(900000); err == nil {
		t.Fatal("mergePiRetryTimeout on a JSONC file = nil error, want a loud failure")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reread settings.json: %v", err)
	}
	if string(data) != seed {
		t.Error("a rejected merge modified the unparseable settings.json; it must stay untouched")
	}
}

// TestMergePiNullShapes pins the null tolerance pi's own JSON.parse accepts:
// a whole-file null, a null retry block, and a null provider block in
// settings.json — and their models.json counterparts — carry no user data,
// so each merge treats it as empty and stages normally instead of panicking
// on an assignment into the nil map the null unmarshals to. Dropping any of
// the merge functions' nil guards panics these tests.
func TestMergePiNullShapes(t *testing.T) {
	entry := piProviderConfig{
		Name: "Daedalus (openai section)", BaseURL: "http://localhost:11434/v1",
		API: "openai-completions", APIKey: "none",
		Models: []piModelConfig{{ID: "qwen"}},
	}
	assertStagedTimeout := func(t *testing.T, home string) {
		t.Helper()
		data, err := os.ReadFile(piSettingsPath(home))
		if err != nil {
			t.Fatalf("read staged settings.json: %v", err)
		}
		if !strings.Contains(string(data), `"timeoutMs": 900000`) {
			t.Errorf("staged settings.json = %s, want the timeout key", data)
		}
	}

	t.Run("settings.json whole-file null", func(t *testing.T) {
		home := piTestHome(t)
		path := piSettingsPath(home)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(`null`), 0o644); err != nil {
			t.Fatalf("seed settings.json: %v", err)
		}
		if err := mergePiRetryTimeout(900000); err != nil {
			t.Fatalf("mergePiRetryTimeout: %v", err)
		}
		assertStagedTimeout(t, home)
	})

	t.Run("settings.json null retry block", func(t *testing.T) {
		home := piTestHome(t)
		path := piSettingsPath(home)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(`{"retry": null}`), 0o644); err != nil {
			t.Fatalf("seed settings.json: %v", err)
		}
		if err := mergePiRetryTimeout(900000); err != nil {
			t.Fatalf("mergePiRetryTimeout: %v", err)
		}
		assertStagedTimeout(t, home)
	})

	t.Run("settings.json null provider block", func(t *testing.T) {
		home := piTestHome(t)
		path := piSettingsPath(home)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(`{"retry": {"provider": null}}`), 0o644); err != nil {
			t.Fatalf("seed settings.json: %v", err)
		}
		if err := mergePiRetryTimeout(900000); err != nil {
			t.Fatalf("mergePiRetryTimeout: %v", err)
		}
		assertStagedTimeout(t, home)
	})

	t.Run("models.json whole-file null", func(t *testing.T) {
		home := piTestHome(t)
		path := piModelsPath(home)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(`null`), 0o644); err != nil {
			t.Fatalf("seed models.json: %v", err)
		}
		if err := mergePiModelsEntry(entry); err != nil {
			t.Fatalf("mergePiModelsEntry: %v", err)
		}
		if got := stagedPiProvider(t, home); got.BaseURL != entry.BaseURL {
			t.Errorf("staged entry = %+v, want the merged entry", got)
		}
	})

	t.Run("models.json null providers preserves sibling", func(t *testing.T) {
		home := piTestHome(t)
		path := piModelsPath(home)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(`{"providers": null,"theme":"dark"}`), 0o644); err != nil {
			t.Fatalf("seed models.json: %v", err)
		}
		if err := mergePiModelsEntry(entry); err != nil {
			t.Fatalf("mergePiModelsEntry: %v", err)
		}
		// Null-merge-as-empty must clobber nothing: the null providers
		// object is replaced by the staged entry, the sibling key survives.
		top := readStagedPiModels(t, home)
		if string(top["theme"]) != `"dark"` {
			t.Errorf("theme = %s, want it preserved", top["theme"])
		}
		if got := stagedPiProvider(t, home); got.BaseURL != entry.BaseURL {
			t.Errorf("staged entry = %+v, want the merged entry", got)
		}
	})
}

// TestStagePiProviderToolRelayURL pins the slim relay's staging win: when
// the worker runs the tool-call relay (DAEDALUS_TOOL_RELAY_URL exported),
// the staged pi provider dials the relay's loopback URL instead of the
// openai upstream directly — same key and model, so the only observable
// change is baseUrl. The model flag stays the round's own model: the relay
// forwards it untouched.
func TestStagePiProviderToolRelayURL(t *testing.T) {
	home := piTestHome(t)
	args, err := stagePiProvider(piEnv(
		"OPENAI_BASE_URL=http://localhost:11434/v1",
		"OPENAI_MODEL=qwen2.5-coder:3b",
		config.ToolRelayURLEnv+"=http://127.0.0.1:41238/v1",
	))
	if err != nil {
		t.Fatalf("stagePiProvider: %v", err)
	}
	if !slices.Equal(args, []string{"--model", piProviderID + "/qwen2.5-coder:3b"}) {
		t.Errorf("args = %v, want the round's own model flag", args)
	}
	entry := stagedPiProvider(t, home)
	if entry.BaseURL != "http://127.0.0.1:41238/v1" {
		t.Errorf("baseUrl = %q, want the relay's staged URL (not the upstream)", entry.BaseURL)
	}
}
