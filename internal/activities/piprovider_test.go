package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
)

// piTestHome redirects the host home every staging write goes through
// (~/.pi/agent/models.json and settings.json, plus the ~/.daedalus lock
// file the read-modify-writes serialize on) at a fresh temp dir and returns
// it — a test must never touch the real ~/.pi or ~/.daedalus of whatever
// host runs the suite. It also empties the slim relay's env var: a suite
// launched by a relay-configured worker inherits DAEDALUS_TOOL_RELAY_URL
// through os.Environ(), and the staging prefers it over OPENAI_BASE_URL on
// a plain round — the relay-wins contract is pinned by
// TestStagePiProviderToolRelayURL through an explicit round env, so here
// the var must be absent, not ambient
// (backlog/bugs/pi-staging-tests-inherit-ambient-toolrelay-url.md).
func piTestHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv(config.ToolRelayURLEnv, "")
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

// piSettingsPath returns the staged retry-timeout path under a test home.
func piSettingsPath(home string) string {
	return filepath.Join(home, ".pi", "agent", "settings.json")
}

// piStagedIDPrefix is the documented round-unique id shape the staging
// writes ("daedalus-openai-<token>", config-example.yaml) — pinned as a
// literal so a renamed constant cannot silently walk away from the
// operator-facing docs.
const piStagedIDPrefix = "daedalus-openai-"

// piTestEntry builds the minimal provider entry the merge tests stage under
// a hand-picked id.
func piTestEntry(model string) piProviderConfig {
	return piProviderConfig{
		Name: "Daedalus (openai section)", BaseURL: "http://localhost:11434/v1",
		API: "openai-completions", APIKey: "none",
		Models: []piModelConfig{{ID: model}},
	}
}

// assertRoundUniqueID pins the staged id shape: the documented prefix plus
// a 16-lowercase-hex token. pi splits provider/model on the FIRST slash, so
// the token must carry none, and the hex alphabet keeps the whole id
// argv-safe.
func assertRoundUniqueID(t *testing.T, id string) {
	t.Helper()
	token, ok := strings.CutPrefix(id, piStagedIDPrefix)
	if !ok {
		t.Fatalf("provider id %q lacks the %s<token> shape", id, piStagedIDPrefix)
	}
	if len(token) != 16 {
		t.Fatalf("round token in %q = %d chars, want 16 hex chars", id, len(token))
	}
	for _, r := range token {
		if !strings.ContainsRune("0123456789abcdef", r) {
			t.Fatalf("round token in %q carries %q, want lowercase hex only", id, string(r))
		}
	}
}

// roundIDFromArgs extracts the staged provider id from the --model flag
// stagePiProvider returned: everything before the FIRST slash, the same
// split pi's findExactModelReference does (huggingface-style model refs
// survive it).
func roundIDFromArgs(t *testing.T, args []string) string {
	t.Helper()
	if len(args) != 2 || args[0] != "--model" {
		t.Fatalf("args = %v, want exactly [--model <id>/<model>]", args)
	}
	id, _, ok := strings.Cut(args[1], "/")
	if !ok {
		t.Fatalf("--model value %q carries no provider/model slash", args[1])
	}
	return id
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

// stagedPiProviders decodes the providers object out of a staged
// models.json, keyed the way pi's provider lookup keys it.
func stagedPiProviders(t *testing.T, home string) map[string]piProviderConfig {
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
	return providers
}

// stagedDaedalusEntry locates the single daedalus-staged entry in a staged
// models.json — the one id carrying the round-unique prefix — and returns
// it with its id. Tests staging exactly one round assert through it; tests
// staging several address each entry by the id stagePiProvider returned.
func stagedDaedalusEntry(t *testing.T, home string) (string, piProviderConfig) {
	t.Helper()
	providers := stagedPiProviders(t, home)
	var (
		id    string
		found int
	)
	for key := range providers {
		if strings.HasPrefix(key, piStagedIDPrefix) {
			found++
			id = key
		}
	}
	if found != 1 {
		t.Fatalf("staged providers %v carry %d %s<token> entries, want 1", keysOf(providers), found, piStagedIDPrefix)
	}
	assertRoundUniqueID(t, id)
	return id, providers[id]
}

// stagedDaedalusModelRaw returns the single daedalus-staged model entry as
// raw JSON — the bytes pi would read — for assertions a typed decode cannot
// make (a key an omitempty tag dropped is indistinguishable from a zero
// value once decoded).
func stagedDaedalusModelRaw(t *testing.T, home string) map[string]json.RawMessage {
	t.Helper()
	id, _ := stagedDaedalusEntry(t, home)
	top := readStagedPiModels(t, home)
	var providers map[string]json.RawMessage
	if err := json.Unmarshal(top["providers"], &providers); err != nil {
		t.Fatalf("parse staged providers: %v", err)
	}
	var entry map[string]json.RawMessage
	if err := json.Unmarshal(providers[id], &entry); err != nil {
		t.Fatalf("parse staged %s entry: %v", id, err)
	}
	var models []map[string]json.RawMessage
	if err := json.Unmarshal(entry["models"], &models); err != nil {
		t.Fatalf("parse staged models: %v", err)
	}
	if len(models) != 1 {
		t.Fatalf("staged models = %d entries, want 1", len(models))
	}
	return models[0]
}

// TestStagePiProviderStagesStoreCompat pins the compat override: every
// staged entry carries compat.supportsStore=false, in both bridge shapes —
// pi's URL-inferred openai compat otherwise hardcodes store:false into
// every chat-completions body, which strict OpenAI-compatible validators
// reject with 422 extra_forbidden. The raw-JSON half is the load-bearing
// assertion: SupportsStore is a *bool precisely so the false survives
// omitempty — a plain bool would drop the key from the staged file while a
// decode-back still reads false either way, so only the bytes pi reads can
// catch that regression.
func TestStagePiProviderStagesStoreCompat(t *testing.T) {
	assertStoreOff := func(t *testing.T, home string) {
		t.Helper()
		model := stagedDaedalusModelRaw(t, home)
		compatRaw, ok := model["compat"]
		if !ok {
			t.Fatal("staged model carries no compat object, want supportsStore staged")
		}
		var compat map[string]json.RawMessage
		if err := json.Unmarshal(compatRaw, &compat); err != nil {
			t.Fatalf("parse staged compat: %v", err)
		}
		if len(compat) != 1 {
			t.Errorf("compat = %s, want exactly the one staged override", compatRaw)
		}
		if string(compat["supportsStore"]) != "false" {
			t.Errorf("compat.supportsStore = %s, want the literal false in the staged bytes", compat["supportsStore"])
		}
		_, entry := stagedDaedalusEntry(t, home)
		decoded := entry.Models[0].Compat
		if decoded == nil || decoded.SupportsStore == nil || *decoded.SupportsStore {
			t.Errorf("decoded compat = %+v, want supportsStore=false", decoded)
		}
	}

	t.Run("keyless backend", func(t *testing.T) {
		home := piTestHome(t)
		args, drop, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1",
			"OPENAI_MODEL=qwen2.5-coder:3b",
		))
		if err != nil {
			t.Fatalf("stagePiProvider: %v", err)
		}
		assertRoundUniqueID(t, roundIDFromArgs(t, args))
		if drop == nil {
			t.Error("cleanup = nil, want one per staged entry")
		}
		assertStoreOff(t, home)
	})

	// The keyed shape also carries metadata and sampler exports, pinning
	// that compat stages alongside every other conditional branch of the
	// entry, never instead of them.
	t.Run("keyed backend with metadata and samplers", func(t *testing.T) {
		home := piTestHome(t)
		if _, _, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=https://llm.example/v1",
			"OPENAI_API_KEY=sk-local",
			"OPENAI_MODEL=glm",
			config.ContextTokensEnv+"=131072",
			config.TemperatureEnv+"=0.2",
		)); err != nil {
			t.Fatalf("stagePiProvider: %v", err)
		}
		assertStoreOff(t, home)
	})
}

// TestStagePiProviderNoOpenaiVars pins the pass-through shape: a round env
// with none of the openai exports stages nothing (no models.json is even
// created), adds no --model flag, and hands back no cleanup — pi follows
// its own config untouched.
func TestStagePiProviderNoOpenaiVars(t *testing.T) {
	home := piTestHome(t)
	args, drop, err := stagePiProvider(piEnv("PATH=/bin", "ANTHROPIC_API_KEY=sk-a"))
	if err != nil {
		t.Fatalf("stagePiProvider: %v", err)
	}
	if args != nil {
		t.Errorf("stagePiProvider args = %v, want nil", args)
	}
	if drop != nil {
		t.Error("cleanup = non-nil, want nil when nothing was staged")
	}
	if _, err := os.Stat(piModelsPath(home)); !os.IsNotExist(err) {
		t.Errorf("models.json stat err = %v, want no file staged", err)
	}
}

// TestStagePiProviderUnbridgeableShapes pins the fail-before-launch guard:
// a key or model without a URL cannot be bridged (pi has no base-URL env
// channel, so the round would silently dial api.openai.com on its built-in
// openai provider), and a URL without a model has nothing to select — all
// three shapes fail the round and stage nothing, so there is nothing to
// clean up either.
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
			args, drop, err := stagePiProvider(c.env)
			if err == nil {
				t.Fatalf("stagePiProvider(%v) = %v, want an error", c.env, args)
			}
			if args != nil {
				t.Errorf("args = %v, want nil on the failure path", args)
			}
			if drop != nil {
				t.Error("cleanup = non-nil, want nil — nothing was staged to clean up")
			}
			if _, err := os.Stat(piModelsPath(home)); !os.IsNotExist(err) {
				t.Errorf("models.json stat err = %v, want nothing staged", err)
			}
		})
	}
}

// TestStagePiProviderStagesEntry pins the happy bridge: a url+model round
// stages its entry under the round-unique id its own --model flag selects,
// and the returned cleanup removes exactly that entry again. Without a key
// the entry carries the documented "none" placeholder (keyless backends
// ignore the header); with one, the key is staged as the ${OPENAI_API_KEY}
// template — the literal key value must never land on disk.
func TestStagePiProviderStagesEntry(t *testing.T) {
	t.Run("keyless backend", func(t *testing.T) {
		home := piTestHome(t)
		args, drop, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1",
			"OPENAI_MODEL=qwen2.5-coder:3b",
		))
		if err != nil {
			t.Fatalf("stagePiProvider: %v", err)
		}
		if drop == nil {
			t.Fatal("cleanup = nil, want one per staged entry")
		}
		id := roundIDFromArgs(t, args)
		assertRoundUniqueID(t, id)
		providers := stagedPiProviders(t, home)
		if len(providers) != 1 {
			t.Fatalf("staged providers %v, want exactly the round's own entry", keysOf(providers))
		}
		entry, ok := providers[id]
		if !ok {
			t.Fatalf("staged providers %v carry no entry under the flag's id %q", keysOf(providers), id)
		}
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

		// The round-end cleanup drops exactly this entry.
		if err := drop(); err != nil {
			t.Fatalf("drop: %v", err)
		}
		if left := stagedPiProviders(t, home); len(left) != 0 {
			t.Errorf("providers after cleanup = %v, want the staged entry removed", keysOf(left))
		}
	})

	t.Run("key rides the template, never disk", func(t *testing.T) {
		home := piTestHome(t)
		args, _, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=https://llm.example/v1",
			"OPENAI_API_KEY=sk-secret-do-not-stage",
			"OPENAI_MODEL=glm",
		))
		if err != nil {
			t.Fatalf("stagePiProvider: %v", err)
		}
		id, entry := stagedDaedalusEntry(t, home)
		if id != roundIDFromArgs(t, args) {
			t.Errorf("staged entry %q does not match the flag's id %q", id, roundIDFromArgs(t, args))
		}
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
	args, _, err := stagePiProvider(piEnv(
		"OPENAI_API_BASE=http://localhost:4000",
		"OPENAI_MODEL=glm-backup",
	))
	if err != nil {
		t.Fatalf("stagePiProvider: %v", err)
	}
	id := roundIDFromArgs(t, args)
	assertRoundUniqueID(t, id)
	if _, entry := stagedDaedalusEntry(t, home); entry.BaseURL != "http://localhost:4000" {
		t.Errorf("baseUrl = %q, want the OPENAI_API_BASE value", entry.BaseURL)
	}
	if _, ref, _ := strings.Cut(args[1], "/"); ref != "glm-backup" {
		t.Errorf("--model value = %q, want the staged model selected", args[1])
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
		if _, _, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1",
			"OPENAI_MODEL=qwen",
			config.ContextTokensEnv+"=131072",
			config.MaxOutputTokensEnv+"=16384",
		)); err != nil {
			t.Fatalf("stagePiProvider: %v", err)
		}
		_, entry := stagedDaedalusEntry(t, home)
		model := entry.Models[0]
		if model.ContextWindow != 131072 {
			t.Errorf("contextWindow = %d, want 131072", model.ContextWindow)
		}
		if model.MaxTokens != 16384 {
			t.Errorf("maxTokens = %d, want 16384", model.MaxTokens)
		}
	})

	t.Run("malformed or zero exports stage nothing", func(t *testing.T) {
		home := piTestHome(t)
		if _, _, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1",
			"OPENAI_MODEL=qwen",
			config.ContextTokensEnv+"=abc",
			config.MaxOutputTokensEnv+"=0",
		)); err != nil {
			t.Fatalf("stagePiProvider: %v", err)
		}
		_, entry := stagedDaedalusEntry(t, home)
		model := entry.Models[0]
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
		if _, _, err := stagePiProvider(piEnv(
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
		_, entry := stagedDaedalusEntry(t, home)
		params := entry.Models[0].SamplingParams
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
		if _, _, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1",
			"OPENAI_MODEL=qwen",
			config.TopPEnv+"=nan",
			config.TopKEnv+"=40",
		)); err != nil {
			t.Fatalf("stagePiProvider: %v", err)
		}
		_, entry := stagedDaedalusEntry(t, home)
		params := entry.Models[0].SamplingParams
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
// survive intact (and the round token must contribute no slash of its own).
func TestStagePiProviderSlashModel(t *testing.T) {
	piTestHome(t)
	args, _, err := stagePiProvider(piEnv(
		"OPENAI_BASE_URL=http://localhost:8000/v1",
		"OPENAI_MODEL=unsloth/Qwen3-4B-GGUF/Q4_K_M",
	))
	if err != nil {
		t.Fatalf("stagePiProvider: %v", err)
	}
	id := roundIDFromArgs(t, args)
	assertRoundUniqueID(t, id)
	want := "--model " + id + "/unsloth/Qwen3-4B-GGUF/Q4_K_M"
	if !slices.Equal(args, []string{"--model", id + "/unsloth/Qwen3-4B-GGUF/Q4_K_M"}) {
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

// TestStagePiProviderRoundUniqueIDs reproduces the two-runs-in-one-worker
// shape the race fix targets, sequentially: each staging takes a distinct
// round-unique id, both entries coexist (neither staging renamed over the
// other), each flag selects its own entry, and a cleanup removes only its
// own — the surviving round's entry is exactly what it staged.
func TestStagePiProviderRoundUniqueIDs(t *testing.T) {
	home := piTestHome(t)
	stage := func(model string) (string, func() error) {
		t.Helper()
		args, drop, err := stagePiProvider(piEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1",
			"OPENAI_MODEL="+model,
		))
		if err != nil {
			t.Fatalf("stagePiProvider(%s): %v", model, err)
		}
		if drop == nil {
			t.Fatalf("cleanup = nil for the %s staging", model)
		}
		return roundIDFromArgs(t, args), drop
	}
	id1, drop1 := stage("qwen")
	id2, drop2 := stage("glm")
	if id1 == id2 {
		t.Fatalf("two rounds staged the same provider id %q — either can overwrite the other before its pi starts", id1)
	}
	providers := stagedPiProviders(t, home)
	if len(providers) != 2 {
		t.Fatalf("staged providers %v, want both rounds' entries", keysOf(providers))
	}
	if got := providers[id1].Models[0].ID; got != "qwen" {
		t.Errorf("entry %q carries model %q, want the first round's own", id1, got)
	}
	if got := providers[id2].Models[0].ID; got != "glm" {
		t.Errorf("entry %q carries model %q, want the second round's own", id2, got)
	}

	// Dropping the first round leaves the second's entry untouched.
	if err := drop1(); err != nil {
		t.Fatalf("drop the first round's entry: %v", err)
	}
	providers = stagedPiProviders(t, home)
	if _, ok := providers[id1]; ok {
		t.Errorf("provider %q survived its own cleanup", id1)
	}
	if got := providers[id2].Models[0].ID; got != "glm" {
		t.Errorf("the second round's entry did not survive the first's cleanup: %+v", providers[id2])
	}
	if err := drop2(); err != nil {
		t.Fatalf("drop the second round's entry: %v", err)
	}
}

// TestStagePiProviderConcurrentRounds pins the serialization the race fix
// adds: stagings racing into one shared models.json — the
// two-workers-on-one-host shape — must all land. The read-modify-write runs
// under lockPiAgentConfig, so no staging's rename may discard another's
// entry; a lost entry here is a round silently dialing someone else's
// provider (the 2026-10-10 wa-termo/bb-eloparse cross-read).
func TestStagePiProviderConcurrentRounds(t *testing.T) {
	home := piTestHome(t)
	const rounds = 8
	type staged struct {
		args []string
		drop func() error
		err  error
	}
	results := make(chan staged, rounds)
	for i := 0; i < rounds; i++ {
		go func(model string) {
			// No t.* here: t.Fatalf from a worker goroutine would Goexit
			// only the worker and hang the collector below on a regression
			// — every check runs after the results are collected.
			args, drop, err := stagePiProvider(piEnv(
				"OPENAI_BASE_URL=http://localhost:11434/v1",
				"OPENAI_MODEL="+model,
			))
			results <- staged{args: args, drop: drop, err: err}
		}(fmt.Sprintf("model-%d", i))
	}
	byRef := map[string]string{}
	drops := map[string]func() error{}
	for range rounds {
		r := <-results
		if r.err != nil {
			t.Fatalf("concurrent stagePiProvider: %v", r.err)
		}
		id := roundIDFromArgs(t, r.args)
		assertRoundUniqueID(t, id)
		if _, dup := drops[id]; dup {
			t.Fatalf("two rounds staged the same provider id %q", id)
		}
		_, ref, _ := strings.Cut(r.args[1], "/")
		byRef[id] = ref
		drops[id] = r.drop
	}
	providers := stagedPiProviders(t, home)
	if len(providers) != rounds {
		t.Fatalf("staged providers %v, want all %d concurrent rounds' entries", keysOf(providers), rounds)
	}
	for id, ref := range byRef {
		entry, ok := providers[id]
		if !ok {
			t.Errorf("provider %q lost — a concurrent staging renamed over it", id)
			continue
		}
		if len(entry.Models) != 1 || entry.Models[0].ID != ref {
			t.Errorf("provider %q carries models %+v, want the racing round's own %q", id, entry.Models, ref)
		}
	}
	for id, drop := range drops {
		if err := drop(); err != nil {
			t.Errorf("drop %q: %v", id, err)
		}
	}
	if left := stagedPiProviders(t, home); len(left) != 0 {
		t.Errorf("providers after every cleanup = %v, want each round's removal to take only its own", keysOf(left))
	}
}

// TestMergePiModelsEntryFreshHost pins the fresh-host case the bridge
// targets: no models.json exists at all, and the first staged round creates
// it (an assignment into a nil map would panic every openai-served pi
// round on exactly this host).
func TestMergePiModelsEntryFreshHost(t *testing.T) {
	home := piTestHome(t)
	const id = "daedalus-openai-0123456789abcdef"
	entry := piTestEntry("qwen")
	if err := mergePiModelsEntry(id, entry); err != nil {
		t.Fatalf("mergePiModelsEntry: %v", err)
	}
	providers := stagedPiProviders(t, home)
	got, ok := providers[id]
	if !ok {
		t.Fatalf("staged providers %v carry no %s entry", keysOf(providers), id)
	}
	if got.BaseURL != entry.BaseURL || len(got.Models) != 1 || got.Models[0].ID != "qwen" {
		t.Errorf("staged entry = %+v, want the merged entry", got)
	}
}

// TestMergePiModelsEntryPreservesForeignContent pins the round-trip
// contract: everything daedalus does not own in models.json — pi's own
// top-level fields and other providers' entries, including fields these
// structs do not model — survives a stage with its values intact (the file
// is re-indented, but no data is dropped). A re-stage replaces only the id
// it was handed (per-id upsert), and a second round's stage adds its own
// entry without touching the first's.
func TestMergePiModelsEntryPreservesForeignContent(t *testing.T) {
	home := piTestHome(t)
	path := piModelsPath(home)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"providers":{"other":{"baseUrl":"http://elsewhere:8000/v1","nested":{"deep":[1,2]}}},"theme":"dark"}`), 0o644); err != nil {
		t.Fatalf("seed models.json: %v", err)
	}
	const (
		mine1 = "daedalus-openai-0123456789abcdef"
		mine2 = "daedalus-openai-fedcba9876543210"
	)
	write := func(id, model string) {
		t.Helper()
		if err := mergePiModelsEntry(id, piTestEntry(model)); err != nil {
			t.Fatalf("mergePiModelsEntry(%s): %v", id, err)
		}
	}

	write(mine1, "qwen")
	top := readStagedPiModels(t, home)
	if string(top["theme"]) != `"dark"` {
		t.Errorf("top-level theme = %s, want it preserved", top["theme"])
	}
	var otherRaw map[string]json.RawMessage
	if err := json.Unmarshal(top["providers"], &otherRaw); err != nil {
		t.Fatalf("parse providers: %v", err)
	}
	var other map[string]any
	if err := json.Unmarshal(otherRaw["other"], &other); err != nil {
		t.Fatalf("parse other provider entry: %v", err)
	}
	wantOther := map[string]any{
		"baseUrl": "http://elsewhere:8000/v1",
		"nested":  map[string]any{"deep": []any{1.0, 2.0}},
	}
	if !reflect.DeepEqual(other, wantOther) {
		t.Errorf("other provider entry = %s, want it preserved verbatim", otherRaw["other"])
	}

	// A same-id re-stage replaces that one entry in place — never a
	// duplicate, never a touch on the foreign content.
	write(mine1, "glm")
	providers := stagedPiProviders(t, home)
	if len(providers) != 2 {
		t.Errorf("providers = %v, want exactly the other entry plus one daedalus entry", keysOf(providers))
	}
	if got := providers[mine1].Models[0].ID; got != "glm" {
		t.Errorf("daedalus entry models = %+v, want the re-stage to replace the old one", providers[mine1].Models)
	}

	// A second round's stage adds its own entry and leaves the first's
	// exactly as it staged it.
	write(mine2, "qwen3")
	providers = stagedPiProviders(t, home)
	if len(providers) != 3 {
		t.Errorf("providers = %v, want the other entry plus both rounds'", keysOf(providers))
	}
	if got := providers[mine1].Models[0].ID; got != "glm" {
		t.Errorf("first round's entry = %+v, want it untouched by the second round's stage", providers[mine1].Models)
	}
	if got := providers[mine2].Models[0].ID; got != "qwen3" {
		t.Errorf("second round's entry = %+v, want its own model", providers[mine2].Models)
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
	if err := mergePiModelsEntry("daedalus-openai-0123456789abcdef", piTestEntry("qwen")); err == nil {
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

// TestRemovePiProviderEntry pins the round-end cleanup contract:
// removePiProviderEntry deletes exactly the id it was handed — a concurrent
// round's entry and the operator's own survive it — and every shape with
// nothing to remove is a no-op that rewrites nothing, so the cleanup can
// never damage a file it did not stage into.
func TestRemovePiProviderEntry(t *testing.T) {
	const (
		round1 = "daedalus-openai-0123456789abcdef"
		round2 = "daedalus-openai-fedcba9876543210"
	)

	t.Run("removes only its own round id", func(t *testing.T) {
		home := piTestHome(t)
		path := piModelsPath(home)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(`{"providers":{"other":{"baseUrl":"http://elsewhere:8000/v1"}},"theme":"dark"}`), 0o644); err != nil {
			t.Fatalf("seed models.json: %v", err)
		}
		if err := mergePiModelsEntry(round1, piTestEntry("qwen")); err != nil {
			t.Fatalf("mergePiModelsEntry(%s): %v", round1, err)
		}
		if err := mergePiModelsEntry(round2, piTestEntry("glm")); err != nil {
			t.Fatalf("mergePiModelsEntry(%s): %v", round2, err)
		}
		if err := removePiProviderEntry(round1); err != nil {
			t.Fatalf("removePiProviderEntry: %v", err)
		}
		top := readStagedPiModels(t, home)
		if string(top["theme"]) != `"dark"` {
			t.Errorf("theme = %s, want it preserved", top["theme"])
		}
		providers := stagedPiProviders(t, home)
		if _, ok := providers[round1]; ok {
			t.Errorf("provider %q survived its own removal", round1)
		}
		if _, ok := providers["other"]; !ok {
			t.Errorf("providers %v lost the operator's own entry to a round's removal", keysOf(providers))
		}
		if got := providers[round2].Models[0].ID; got != "glm" {
			t.Errorf("the concurrent round's entry = %+v, want it undamaged", providers[round2].Models)
		}
	})

	t.Run("missing file is a no-op", func(t *testing.T) {
		home := piTestHome(t)
		if err := removePiProviderEntry(round1); err != nil {
			t.Errorf("removePiProviderEntry with no models.json: %v", err)
		}
		if _, err := os.Stat(piModelsPath(home)); !os.IsNotExist(err) {
			t.Errorf("models.json stat err = %v, want the no-op to create nothing", err)
		}
	})

	t.Run("absent id is a no-op that rewrites nothing", func(t *testing.T) {
		home := piTestHome(t)
		path := piModelsPath(home)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		seed := `{"providers":{"other":{"baseUrl":"http://elsewhere:8000/v1"}},"theme":"dark"}`
		if err := os.WriteFile(path, []byte(seed), 0o644); err != nil {
			t.Fatalf("seed models.json: %v", err)
		}
		if err := removePiProviderEntry(round1); err != nil {
			t.Errorf("removePiProviderEntry of an absent id: %v", err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reread models.json: %v", err)
		}
		if string(data) != seed {
			t.Error("an absent-id removal rewrote models.json; a no-op must not touch the file")
		}
	})

	t.Run("whole-file null is a no-op", func(t *testing.T) {
		home := piTestHome(t)
		path := piModelsPath(home)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(`null`), 0o644); err != nil {
			t.Fatalf("seed models.json: %v", err)
		}
		if err := removePiProviderEntry(round1); err != nil {
			t.Errorf("removePiProviderEntry on a whole-file null: %v", err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reread models.json: %v", err)
		}
		if string(data) != `null` {
			t.Error("a removal over a whole-file null rewrote it; there was nothing to remove")
		}
	})

	t.Run("null providers is a no-op", func(t *testing.T) {
		home := piTestHome(t)
		path := piModelsPath(home)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		seed := `{"providers": null,"theme":"dark"}`
		if err := os.WriteFile(path, []byte(seed), 0o644); err != nil {
			t.Fatalf("seed models.json: %v", err)
		}
		if err := removePiProviderEntry(round1); err != nil {
			t.Errorf("removePiProviderEntry on a null providers object: %v", err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reread models.json: %v", err)
		}
		if string(data) != seed {
			t.Error("a removal over a null providers object rewrote the file; there was nothing to remove")
		}
	})

	t.Run("JSONC fails loudly and stays untouched", func(t *testing.T) {
		home := piTestHome(t)
		path := piModelsPath(home)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		seed := "{\n// pi tolerates this, Go does not\n\"providers\": {}\n}"
		if err := os.WriteFile(path, []byte(seed), 0o644); err != nil {
			t.Fatalf("seed models.json: %v", err)
		}
		if err := removePiProviderEntry(round1); err == nil {
			t.Fatal("removePiProviderEntry on a JSONC file = nil error, want a loud failure")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reread models.json: %v", err)
		}
		if string(data) != seed {
			t.Error("a rejected removal modified the unparseable models.json; it must stay untouched")
		}
	})
}

// TestLockPiAgentConfig pins the serialization primitive the staged-config
// read-modify-writes share: the lock file lives under the test home's
// .daedalus (never the host's, and never inside pi's config dir), and while
// it is held no second open file description — the same shape an
// in-process concurrent round holds — can take it, until the holder
// releases.
func TestLockPiAgentConfig(t *testing.T) {
	home := piTestHome(t)
	lockPath := filepath.Join(home, ".daedalus", "pi-stage.lock")
	unlock, err := lockPiAgentConfig()
	if err != nil {
		t.Fatalf("lockPiAgentConfig: %v", err)
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("lock file %s: %v", lockPath, err)
	}

	// Held: an independent open file description must not be able to take
	// the lock — a flock grant here means the concurrent stagings serialize
	// on nothing. Probed with a non-blocking flock so the exclusion is
	// asserted deterministically, with no timing window to flake through.
	probe, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open the lock file for the exclusion probe: %v", err)
	}
	defer probe.Close()
	if err := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		syscall.Flock(int(probe.Fd()), syscall.LOCK_UN)
		t.Fatal("a second open file description took the lock while the first was held — the flock excludes nothing")
	} else if !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("exclusion probe: %v", err)
	}

	unlock()

	// Released: the same probe gets through, and a fresh lockPiAgentConfig
	// acquires and releases cleanly.
	if err := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("probe flock still blocked after the release: %v", err)
	}
	syscall.Flock(int(probe.Fd()), syscall.LOCK_UN)
	second, err := lockPiAgentConfig()
	if err != nil {
		t.Fatalf("second lockPiAgentConfig after the release: %v", err)
	}
	second()
}

// TestPiRoundStagesProvider pins the runJailedRound wiring end to end: a
// pi round served by the openai section launches ai-jail with the staged
// --model flag selecting an entry that really is in the host models.json
// the jail's pi preset mounts while the round runs, the round's end prunes
// that entry again, and a staging that fails after the entry landed (the
// timeout merge) prunes too — a round that never launches leaves nothing
// behind. A pi round with no openai exports launches with no --model at
// all.
func TestPiRoundStagesProvider(t *testing.T) {
	t.Run("openai-served round launches on its own entry and prunes it at round end", func(t *testing.T) {
		home := piTestHome(t)
		t.Setenv("DAEDALUS_AGENT", "pi")
		t.Setenv("OPENAI_BASE_URL", "http://localhost:11434/v1")
		t.Setenv("OPENAI_API_KEY", "sk-local")
		t.Setenv("OPENAI_MODEL", "qwen2.5-coder:3b")
		log := newStubLog(t)
		// The stub snapshots models.json mid-round — the bytes pi's pi
		// preset would hand the round at its startup read.
		capture := filepath.Join(t.TempDir(), "midround-models.json")
		stubBin(t, "ai-jail", fmt.Sprintf(`if [ -f "$HOME/.pi/agent/models.json" ]; then cp "$HOME/.pi/agent/models.json" %q; fi
exit 0`, capture))

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
		flag := args[model+1]
		id, _, ok := strings.Cut(flag, "/")
		if !ok {
			t.Fatalf("--model value %q carries no provider/model slash", flag)
		}
		assertRoundUniqueID(t, id)
		if _, ref, _ := strings.Cut(flag, "/"); ref != "qwen2.5-coder:3b" {
			t.Errorf("--model = %q, want the round's own model selected", flag)
		}

		// Mid-round: the entry under the flag's id is the one pi reads,
		// pointing at the round's endpoint.
		mid, err := os.ReadFile(capture)
		if err != nil {
			t.Fatalf("the round launched with no models.json staged (%v) — flag %q would select nothing", err, flag)
		}
		var midTop map[string]json.RawMessage
		if err := json.Unmarshal(mid, &midTop); err != nil {
			t.Fatalf("parse the mid-round models.json: %v", err)
		}
		var midProviders map[string]piProviderConfig
		if err := json.Unmarshal(midTop["providers"], &midProviders); err != nil {
			t.Fatalf("parse the mid-round providers: %v", err)
		}
		entry, ok := midProviders[id]
		if !ok {
			t.Fatalf("mid-round providers %v carry no entry under the flag's id %q", keysOf(midProviders), id)
		}
		if entry.BaseURL != "http://localhost:11434/v1" {
			t.Errorf("mid-round baseUrl = %q, want the round's endpoint", entry.BaseURL)
		}

		// Round end: nothing daedalus-owned is left in the operator's file.
		for key := range stagedPiProviders(t, home) {
			if strings.HasPrefix(key, piStagedIDPrefix) {
				t.Errorf("provider %q survived the round that staged it", key)
			}
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

	t.Run("staging failure after the entry lands prunes it before launch", func(t *testing.T) {
		home := piTestHome(t)
		path := piSettingsPath(home)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		// settings.json unparseable for the Go merge: the entry stages
		// fine, then the timeout merge fails the round.
		if err := os.WriteFile(path, []byte("{\n// pi does not tolerate this either\n\"retry\": {}\n}"), 0o644); err != nil {
			t.Fatalf("seed settings.json: %v", err)
		}
		t.Setenv("DAEDALUS_AGENT", "pi")
		t.Setenv("OPENAI_BASE_URL", "http://localhost:11434/v1")
		t.Setenv("OPENAI_API_KEY", "sk-local")
		t.Setenv("OPENAI_MODEL", "qwen2.5-coder:3b")
		t.Setenv("API_TIMEOUT_MS", "900000")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		_, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		})
		if err == nil {
			t.Fatal("RunJailedClaudeActivity = nil error, want the timeout-merge failure")
		}
		if !strings.Contains(err.Error(), "stage pi provider timeout") {
			t.Errorf("error = %q, want the pi timeout staging failure", err)
		}
		// The entry landed before the failure; the round's end dropped it
		// again — a round that never launches leaks nothing into the file
		// the next round reads.
		for key := range stagedPiProviders(t, home) {
			if strings.HasPrefix(key, piStagedIDPrefix) {
				t.Errorf("provider %q leaked from a round that never launched", key)
			}
		}
		if _, statErr := os.Stat(log); !os.IsNotExist(statErr) {
			calls := readCalls(t, log)
			t.Errorf("ai-jail called %d times, want the round failed before launch (%v)", len(calls), calls)
		}
	})

	t.Run("failed cleanup never fails the round", func(t *testing.T) {
		home := piTestHome(t)
		t.Setenv("DAEDALUS_AGENT", "pi")
		t.Setenv("OPENAI_BASE_URL", "http://localhost:11434/v1")
		t.Setenv("OPENAI_API_KEY", "sk-local")
		t.Setenv("OPENAI_MODEL", "qwen2.5-coder:3b")
		newStubLog(t)
		// The round itself corrupts models.json to JSONC after staging —
		// so the round-end cleanup cannot parse the file it must remove
		// its entry from. (That the cleanup runs at all is pinned by the
		// prune subtest above; this pins the failure half of the same
		// defer.)
		seed := `{"providers": {}} /* pi tolerates this, Go does not */`
		stubBin(t, "ai-jail", fmt.Sprintf(`printf '%%s' '%s' > "$HOME/.pi/agent/models.json"
exit 0`, seed))

		// A failed cleanup is logged, never surfaced as a round failure —
		// the round already ran, and the leftover is inert.
		if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedClaudeActivity = %v, want the cleanup failure logged and swallowed", err)
		}

		// And it must never rewrite what it cannot round-trip: the
		// unparseable file stays exactly as the round left it, for the
		// operator to fix.
		data, err := os.ReadFile(piModelsPath(home))
		if err != nil {
			t.Fatalf("reread models.json: %v", err)
		}
		if string(data) != seed {
			t.Errorf("a failed cleanup rewrote the unparseable models.json to %q, want %q untouched", data, seed)
		}
	})
}

// TestStagePiProviderStagesRetryTimeout pins the timeout bridge: a positive
// API_TIMEOUT_MS export — the same ceiling every other agent gets as the
// exported var — lands as retry.provider.timeoutMs in the host's
// settings.json (pi's only request-timeout channel), and models.json is
// still staged alongside it.
func TestStagePiProviderStagesRetryTimeout(t *testing.T) {
	home := piTestHome(t)
	if _, _, err := stagePiProvider(piEnv(
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
	if _, entry := stagedDaedalusEntry(t, home); entry.BaseURL != "http://localhost:11434/v1" {
		t.Errorf("staged baseUrl = %q, want the models.json bridge unaffected", entry.BaseURL)
	}
}

// TestStagePiProviderNoTimeoutExportStagesNoSettings pins the gate: the
// timeout merge derives from the exported var, so an absent, malformed, or
// non-positive value leaves pi's own 5-minute default standing — no
// settings.json is created at all. The entry itself stages in every shape,
// so the cleanup exists each time.
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
			_, drop, err := stagePiProvider(c.env)
			if err != nil {
				t.Fatalf("stagePiProvider: %v", err)
			}
			if drop == nil {
				t.Error("cleanup = nil, want one per staged entry")
			}
			if _, err := os.Stat(piSettingsPath(home)); !os.IsNotExist(err) {
				t.Errorf("settings.json stat err = %v, want nothing staged", err)
			}
		})
	}
}

// TestStagePiProviderCleanupRidesTimeoutFailure pins the leak guard on the
// post-staging error path: when the timeout merge fails after the entry is
// already on disk, the returned cleanup is non-nil and removes the entry —
// a stagePiProvider error return must never leave an entry for a round
// that never launches.
func TestStagePiProviderCleanupRidesTimeoutFailure(t *testing.T) {
	home := piTestHome(t)
	path := piSettingsPath(home)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte("{\n// unparseable for the Go merge\n\"retry\": {}\n}"), 0o644); err != nil {
		t.Fatalf("seed settings.json: %v", err)
	}
	args, drop, err := stagePiProvider(piEnv(
		"OPENAI_BASE_URL=http://localhost:11434/v1",
		"OPENAI_MODEL=qwen",
		"API_TIMEOUT_MS=900000",
	))
	if err == nil {
		t.Fatal("stagePiProvider = nil error, want the timeout-merge failure")
	}
	if !strings.Contains(err.Error(), "stage pi provider timeout") {
		t.Errorf("error = %q, want the pi timeout staging failure", err)
	}
	if args != nil {
		t.Errorf("args = %v, want nil on the failure path", args)
	}
	if drop == nil {
		t.Fatal("cleanup = nil on the post-staging failure, want it to ride the error return")
	}
	// The staged entry landed before the failure...
	id, entry := stagedDaedalusEntry(t, home)
	if entry.Models[0].ID != "qwen" {
		t.Errorf("staged entry = %+v, want the entry the failed round staged", entry.Models)
	}
	// ...and its cleanup hands it back.
	if err := drop(); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, ok := stagedPiProviders(t, home)[id]; ok {
		t.Errorf("provider %q survived its own cleanup", id)
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
	const id = "daedalus-openai-0123456789abcdef"
	entry := piTestEntry("qwen")
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
		if err := mergePiModelsEntry(id, entry); err != nil {
			t.Fatalf("mergePiModelsEntry: %v", err)
		}
		if _, got := stagedDaedalusEntry(t, home); got.BaseURL != entry.BaseURL {
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
		if err := mergePiModelsEntry(id, entry); err != nil {
			t.Fatalf("mergePiModelsEntry: %v", err)
		}
		// Null-merge-as-empty must clobber nothing: the null providers
		// object is replaced by the staged entry, the sibling key survives.
		top := readStagedPiModels(t, home)
		if string(top["theme"]) != `"dark"` {
			t.Errorf("theme = %s, want it preserved", top["theme"])
		}
		if _, got := stagedDaedalusEntry(t, home); got.BaseURL != entry.BaseURL {
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
	args, _, err := stagePiProvider(piEnv(
		"OPENAI_BASE_URL=http://localhost:11434/v1",
		"OPENAI_MODEL=qwen2.5-coder:3b",
		config.ToolRelayURLEnv+"=http://127.0.0.1:41238/v1",
	))
	if err != nil {
		t.Fatalf("stagePiProvider: %v", err)
	}
	id := roundIDFromArgs(t, args)
	assertRoundUniqueID(t, id)
	if _, ref, _ := strings.Cut(args[1], "/"); ref != "qwen2.5-coder:3b" {
		t.Errorf("--model value = %q, want the round's own model", args[1])
	}
	stagedID, entry := stagedDaedalusEntry(t, home)
	if stagedID != id {
		t.Fatalf("staged entry %q does not match the flag's id %q", stagedID, id)
	}
	if entry.BaseURL != "http://127.0.0.1:41238/v1" {
		t.Errorf("baseUrl = %q, want the relay's staged URL (not the upstream)", entry.BaseURL)
	}
}
