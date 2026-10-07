package activities

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ozzono/daedalus/internal/config"
)

// opencodeProviderID is the custom opencode provider daedalus stages for
// jailed rounds served by the config's openai section; "-m <id>/<model>"
// selects it on `opencode run`. Same daedalus-owned convention as pi's and
// codex's ids: an operator's own config has no reason to collide with it.
const opencodeProviderID = "daedalus-openai"

// opencodeScratchDir is the worktree scratch dir holding the staged
// round-scoped config; excludeAgentArtifacts keeps it out of git's sight
// (same pattern as .daedalus-aider/).
const opencodeScratchDir = ".daedalus-opencode"

// opencodeStagedConfig is the whole round-scoped config file. opencode
// merges it over its global config when OPENCODE_CONFIG points at it, so
// the file carries exactly the staged provider and nothing else — unlike
// pi's bridge no host file is ever read or rewritten, which also sidesteps
// the config-dir jail mount (opencode's global config dir reaches a jailed
// round through no existing mount).
type opencodeStagedConfig struct {
	Provider map[string]opencodeProviderConfig `json:"provider"`
}

// opencodeProviderConfig is the daedalus-owned provider entry, in the shape
// the host's own custom providers use (verified against opencode 1.18.31
// and a working host opencode.json, 2026-10-06): the @ai-sdk/openai-compatible
// npm package speaks the OpenAI-compatible chat-completions wire — what
// litellm/ollama/vLLM/Mistral self-hosted endpoints serve.
type opencodeProviderConfig struct {
	NPM     string                         `json:"npm"`
	Options opencodeProviderOptionsConfig  `json:"options"`
	Models  map[string]opencodeModelConfig `json:"models"`
}

// opencodeProviderOptionsConfig carries the endpoint and the key template.
type opencodeProviderOptionsConfig struct {
	BaseURL string `json:"baseURL"`
	// APIKey is staged as the "{env:OPENAI_API_KEY}" template — opencode
	// resolves it from the round env at config load (binary-verified,
	// 2026-10-06), so the key value never lands on disk (pi's
	// "${OPENAI_API_KEY}" pattern). Omitted entirely for a keyless backend,
	// which then gets no auth header (codex's env_key posture).
	APIKey string `json:"apiKey,omitempty"`
}

// opencodeModelConfig is the staged model entry; Name is the display name
// opencode's own entries carry. Limit is set only when the config declares
// a context window or completion cap (ContextTokensEnv / MaxOutputTokensEnv)
// — absent both it is omitted and opencode's own defaults stand, per the
// agnostic rule.
type opencodeModelConfig struct {
	Name  string                    `json:"name"`
	Limit *opencodeModelLimitConfig `json:"limit,omitempty"`
}

type opencodeModelLimitConfig struct {
	Context int `json:"context,omitempty"`
	Output  int `json:"output,omitempty"`
}

// stageOpencodeProvider bridges the config's openai section into opencode
// for one jailed round, derived from the round's provider env like pi's and
// codex's staging — so openai-type failover rounds and reviewer-endpoint
// rounds re-derive for free. It writes the round-scoped config file into
// the worktree scratch dir and returns its path ("" when none of the
// openai vars is exported — stage nothing, no flag: the round dials
// whatever the host's own opencode.json holds, exactly as before this
// bridge) plus the -m args selecting the staged provider. The bridge exists
// because opencode is dispatched with no provider wiring at all — without
// it the section is silently dead config for opencode rounds (verified live
// 2026-10-06: an openai-section round dialed the host's hand-configured zai
// provider instead). The sampler and timeout exports have no verified
// opencode route, so the backend's own defaults stand, per the agnostic
// rule. With a key or model but no URL, or a URL but no model, the round
// fails before launch: the section cannot be bridged half-specified, and a
// silently-unbridged round would misdirect spend (same posture as
// stagePiProvider and stageCodexProvider).
// ponytail: the staged shapes are probe-derived from opencode 1.18.31's
// documented provider schema and the host's own working opencode.json
// (2026-10-06) but never exercised against a live round — this sandbox has
// no opencode binary. Likewise unprobed: the -m flag composing with the
// resumed `run -s <id>` tokens, the keyless apiKey omission, the
// {env:...} template resolving from the jailed round's env (the same
// env-allowlist channel pi's ${OPENAI_API_KEY} template rides), and —
// first time in this file — two --env flags on one ai-jail invocation
// (XDG_DATA_HOME when the operator set it, plus OPENCODE_CONFIG): whether
// ai-jail's --env tolerates repetition or is last-wins is unknown, and
// last-wins would silently drop one var — losing XDG_DATA_HOME
// reintroduces the exact auth/session-loss bug opencodeJailMounts fixed,
// losing OPENCODE_CONFIG silently reverts the round to its jail-resolved
// config. Probe on the first live openai-served opencode round, alongside
// opencodeJailMounts's own owed probe.
func stageOpencodeProvider(env []string, worktreePath string) (string, []string, error) {
	url := envLookup(env, "OPENAI_BASE_URL")
	if url == "" {
		url = envLookup(env, "OPENAI_API_BASE")
	}
	key := envLookup(env, "OPENAI_API_KEY")
	model := envLookup(env, "OPENAI_MODEL")
	if url == "" && key == "" && model == "" {
		return "", nil, nil
	}
	if url == "" {
		return "", nil, fmt.Errorf("opencode round: OPENAI_API_KEY/OPENAI_MODEL exported without a base URL — the section cannot be bridged and the round would silently dial whatever the host's own opencode.json holds; set openai.url (and openai.model), or unset the ambient OPENAI_* exports")
	}
	if model == "" {
		return "", nil, fmt.Errorf("opencode round: openai.url exported without openai.model — opencode would dial the bridged endpoint with its own default model; set openai.model")
	}
	modelEntry := opencodeModelConfig{Name: model}
	lim := opencodeModelLimitConfig{}
	if n, err := strconv.Atoi(envLookup(env, config.ContextTokensEnv)); err == nil && n > 0 {
		lim.Context = n
	}
	if n, err := strconv.Atoi(envLookup(env, config.MaxOutputTokensEnv)); err == nil && n > 0 {
		lim.Output = n
	}
	if lim.Context > 0 || lim.Output > 0 {
		modelEntry.Limit = &lim
	}
	entry := opencodeProviderConfig{
		NPM:     "@ai-sdk/openai-compatible",
		Options: opencodeProviderOptionsConfig{BaseURL: url},
		Models:  map[string]opencodeModelConfig{model: modelEntry},
	}
	if key != "" {
		entry.Options.APIKey = "{env:OPENAI_API_KEY}"
	}
	data, err := json.MarshalIndent(opencodeStagedConfig{
		Provider: map[string]opencodeProviderConfig{opencodeProviderID: entry},
	}, "", "  ")
	if err != nil {
		return "", nil, err
	}
	if err := excludeAgentArtifacts(worktreePath, opencodeScratchDir); err != nil {
		return "", nil, fmt.Errorf("prepare opencode scratch dir: %w", err)
	}
	path := filepath.Join(worktreePath, opencodeScratchDir, "opencode.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", nil, err
	}
	// provider/model splits on the FIRST slash, so model ids containing
	// slashes (huggingface-style refs) survive — and the id itself carries
	// none.
	return path, []string{"-m", opencodeProviderID + "/" + model}, nil
}
