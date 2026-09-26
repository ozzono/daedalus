package activities

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ozzono/daedalus/internal/config"
)

// piProviderID is the custom pi provider daedalus stages into the host's
// ~/.pi/agent/models.json for jailed rounds served by the config's openai
// section; "--model <id>/<model>" selects it. pi honors OPENAI_API_KEY for
// its built-in openai provider but has no base-URL env channel at all
// (source-verified 2026-09-26: env-api-keys.ts maps openai → OPENAI_API_KEY
// and nothing reads OPENAI_BASE_URL/OPENAI_API_BASE — the wa-termo 401 that
// reached api.openai.com carrying the right key), so the env vars alone
// cannot route a pi round anywhere but the cloud default. A models.json
// provider entry is pi's only custom-endpoint mechanism, and the jail's pi
// preset mounts ~/.pi read-write, so the host file is visible to the round.
const piProviderID = "daedalus-openai"

// piModelConfig is the model definition daedalus stages for the openai
// section's model. Field names follow pi's ProviderConfigSchema/
// ModelDefinitionSchema (model-config.ts, source-verified 2026-09-26):
// contextWindow drives pi's context arithmetic, maxTokens is the completion
// ceiling the wire request falls back to, and samplingParams merge as
// top-level OpenAI-completions request params.
type piModelConfig struct {
	ID             string         `json:"id"`
	ContextWindow  int            `json:"contextWindow,omitempty"`
	MaxTokens      int            `json:"maxTokens,omitempty"`
	SamplingParams map[string]any `json:"samplingParams,omitempty"`
}

// piProviderConfig is the daedalus-owned provider entry. apiKey is staged as
// an "${OPENAI_API_KEY}" template — pi resolves it from the round env at
// request time, so the key value itself never lands on disk. With no key
// exported, the literal "none" keeps the provider authenticated for keyless
// backends (llama.cpp and friends ignore the header), mirroring the
// placeholder the config example documents.
type piProviderConfig struct {
	Name    string          `json:"name"`
	BaseURL string          `json:"baseUrl"`
	API     string          `json:"api"`
	APIKey  string          `json:"apiKey,omitempty"`
	Models  []piModelConfig `json:"models"`
}

// stagePiProvider bridges the config's openai section into pi's own provider
// config for one jailed round, derived from the round's provider env — so
// failover rounds re-derive the entry for free when the fallback is
// openai-type (whose OPENAI_* overrides carry the fallback's endpoint, see
// provider_failover.go; an anthropic-type fallback has no pi channel at
// all — no ANTHROPIC_BASE_URL exists in pi — and the round stays pinned to
// the primary's staged entry, see backlog/bugs/pi-anthropic-fallback-
// unserveable.md). It returns the
// --model args selecting the staged entry; pi rereads models.json at
// startup. With none of the openai vars set, nothing is staged and no flag
// is added — pi follows its own config untouched. With a key or model but
// no URL, the round fails before launch: that shape cannot be bridged, and
// launching it would ship the ambient key to api.openai.com on pi's
// built-in openai provider — the exact silent-dial trap this staging
// exists to close. The guard keys on the exported env vars, not on the
// config section: a foreground run inherits the invoking shell, so an
// ambient OPENAI_API_KEY with no URL triggers it too (see
// backlog/bugs/pi-ambient-openai-key-fails-rounds.md).
// ponytail: the models.json merge is read-modify-write
// with no lock, so two concurrent jailed rounds on one host
// (max_concurrent_agent_runs > 1) can interleave and the last rename wins —
// each round launches immediately after its own write, so the worst case is
// a round reading the other's endpoint variant (primary vs failover, both
// operator-configured), never a missing or default one.
func stagePiProvider(env []string) ([]string, error) {
	url := envLookup(env, "OPENAI_BASE_URL")
	if url == "" {
		url = envLookup(env, "OPENAI_API_BASE")
	}
	key := envLookup(env, "OPENAI_API_KEY")
	model := envLookup(env, "OPENAI_MODEL")
	if url == "" && key == "" && model == "" {
		return nil, nil
	}
	if url == "" {
		return nil, fmt.Errorf("pi round: OPENAI_API_KEY/OPENAI_MODEL exported without a base URL — pi has no base-URL env channel, so the round would silently dial api.openai.com; set openai.url (and openai.model), or unset the ambient OPENAI_* exports")
	}
	if model == "" {
		return nil, fmt.Errorf("pi round: openai.url exported without openai.model — pi cannot select a model to bridge the section; set openai.model")
	}
	entry := piProviderConfig{
		Name:    "Daedalus (openai section)",
		BaseURL: url,
		// openai-completions is pi's OpenAI-compatible chat-completions
		// wire — what litellm/ollama/vLLM self-hosted endpoints serve.
		API:    "openai-completions",
		APIKey: "${OPENAI_API_KEY}",
	}
	if key == "" {
		entry.APIKey = "none"
	}
	modelEntry := piModelConfig{ID: model}
	if n, err := strconv.Atoi(envLookup(env, config.ContextTokensEnv)); err == nil && n > 0 {
		modelEntry.ContextWindow = n
	}
	if n, err := strconv.Atoi(envLookup(env, config.MaxOutputTokensEnv)); err == nil && n > 0 {
		modelEntry.MaxTokens = n
	}
	if s := samplerFromEnv(env); anySampler(s) {
		params := map[string]any{}
		if s.TopP != nil {
			params["top_p"] = *s.TopP
		}
		if s.PresencePenalty != nil {
			params["presence_penalty"] = *s.PresencePenalty
		}
		if s.TopK != nil {
			params["top_k"] = *s.TopK
		}
		if s.MinP != nil {
			params["min_p"] = *s.MinP
		}
		if s.RepetitionPenalty != nil {
			params["repetition_penalty"] = *s.RepetitionPenalty
		}
		modelEntry.SamplingParams = params
	}
	entry.Models = []piModelConfig{modelEntry}
	if err := mergePiModelsEntry(entry); err != nil {
		return nil, fmt.Errorf("stage pi provider entry: %w", err)
	}
	// provider/model splits on the FIRST slash (pi's findExactModelReference
	// Match), so model ids containing slashes (huggingface-style refs) survive.
	return []string{"--model", piProviderID + "/" + model}, nil
}

// anySampler reports whether any knob in s was set — an all-nil sampler
// means the staged model omits samplingParams entirely, so the backend's
// own sampler values stand (never a sent default overriding them).
func anySampler(s aiderSampler) bool {
	return s.TopP != nil || s.PresencePenalty != nil || s.TopK != nil ||
		s.MinP != nil || s.RepetitionPenalty != nil
}

// mergePiModelsEntry upserts entry into the host's ~/.pi/agent/models.json
// without disturbing anything else in the file: the top level and the
// providers map are handled as raw JSON, so unknown top-level fields and
// other providers' entries (including fields this repo's structs do not
// model) round-trip byte-for-byte. A file pi would accept but Go cannot
// parse (JSONC comments) fails the round loudly rather than risk clobbering
// it — pi strips comments, Go's decoder does not.
func mergePiModelsEntry(entry piProviderConfig) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	path := filepath.Join(home, ".pi", "agent", "models.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	// Initialize unconditionally: a fresh host has no models.json at all
	// (exactly the self-hosted deployment this bridge targets), and an
	// assignment into a nil map would panic every openai-served pi round.
	top := map[string]json.RawMessage{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &top); err != nil {
			return fmt.Errorf("parse existing %s (pi also accepts JSON comments, which this merge cannot round-trip — move them out): %w", path, err)
		}
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	providers := map[string]json.RawMessage{}
	if prev, ok := top["providers"]; ok {
		if err := json.Unmarshal(prev, &providers); err != nil {
			return fmt.Errorf("parse providers object in %s: %w", path, err)
		}
	}
	providers[piProviderID] = raw
	top["providers"], err = json.Marshal(providers)
	if err != nil {
		return err
	}
	out, err := json.MarshalIndent(top, "", "  ")
	if err != nil {
		return err
	}
	// Write-then-rename keeps a mid-write crash from leaving pi a truncated
	// config; the original mode is preserved for an existing file.
	mode := os.FileMode(0o644)
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode().Perm()
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".models.json-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(out); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
