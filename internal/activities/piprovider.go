package activities

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/ozzono/daedalus/internal/config"
)

// piProviderPrefix is the prefix of the per-round provider ids daedalus
// stages into the host's ~/.pi/agent/models.json for jailed rounds served
// by the config's openai section; "--model <id>/<model>" selects one. pi
// honors OPENAI_API_KEY for its built-in openai provider but has no
// base-URL env channel at all (source-verified 2026-09-26: env-api-keys.ts
// maps openai → OPENAI_API_KEY and nothing reads OPENAI_BASE_URL/OPENAI_API_BASE
// — the wa-termo 401 that reached api.openai.com carrying the right key),
// so the env vars alone cannot route a pi round anywhere but the cloud
// default. A models.json provider entry is pi's only custom-endpoint
// mechanism, and the jail's pi preset mounts ~/.pi read-write, so the host
// file is visible to the round.
//
// The prefix is completed per round with a random token
// (daedalus-openai-<hex>): every round stages and selects its own entry,
// so a concurrent round — another worker on the host, or another run in
// the same worker — can never select, overwrite, or be overwritten by it.
// A single shared id cannot be saved by locking alone: the 2026-10-10
// wa-termo/bb-eloparse overlap had the second worker's staging land in the
// file between the first's staging and its pi startup, and the first round
// silently dialed the second's provider — the entry must be unaddressable
// to everyone but the round that staged it.
const piProviderPrefix = "daedalus-openai"

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
	Compat         *piModelCompat `json:"compat,omitempty"`
}

// piModelCompat carries the per-model compat overrides pi merges over its
// detected provider compat (getCompat(): model.compat.* ?? detected.*, task-
// verified against the installed 0.87.1 release 2026-10-06). daedalus stages
// exactly one: supportsStore=false. pi's openai-completions provider infers
// compat from the base URL — any URL outside its known-provider list counts
// as standard OpenAI and gets supportsStore, so buildParams hardcodes
// `store: false` into every chat-completions body — which strict
// OpenAI-compatible validators (Mistral) reject with 422
// extra_forbidden. Staging the override makes pi omit the param, always
// equivalent-or-better: store defaults to false on OpenAI itself. SupportsStore
// is a *bool because omitempty on a plain bool would drop the very false the
// override exists to send.
type piModelCompat struct {
	SupportsStore *bool `json:"supportsStore,omitempty"`
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
// unserveable.md). It returns the --model args selecting the staged entry —
// pi rereads models.json at startup and never mid-process — plus a cleanup
// that removes the entry again, for the round's end: a staged entry is
// addressable only through its own --model flag, so nothing else ever reads
// it, and pruning keeps the round-unique ids from accumulating in the
// operator's file. Pruning cannot break the chain's next round: a resumed
// round restages before launch and pi applies an explicit --model
// unconditionally over the model recorded in the session (source-verified
// 2026-10-10, main.ts buildSessionOptions: the session-recorded model is
// consulted only when no --model was given), so the predecessor's pruned id
// is never looked up. The cleanup is non-nil whenever an entry was staged,
// including on an error return that follows the staging (the timeout merge
// below), and nil when nothing was staged. With none of the openai vars
// set, nothing is staged and no flag is added — pi follows its own config
// untouched. The slim relay URL (ToolRelayURLEnv, when the worker started
// the tool-call relay) wins only on a plain round: an explicit endpoint
// override — an openai-type fallback (fallbackEnv) or a reviewer.url
// (reviewerEnv) — scrubs the var, because the relay's upstream is pinned to
// the primary's openai.url and must not swallow overridden endpoints. With
// a key or model but no URL, the round fails before launch: that shape
// cannot be bridged, and launching it would ship the ambient key to
// api.openai.com on pi's built-in openai provider — the exact silent-dial
// trap this staging exists to close. The guard keys on the exported env
// vars, not on the config section: a foreground run inherits the invoking
// shell, so an ambient OPENAI_API_KEY with no URL triggers it too (see
// backlog/bugs/pi-ambient-openai-key-fails-rounds.md).
//
// Concurrency: the entry is staged under a round-unique id (piProviderPrefix
// plus a crypto-random token, unique across workers and runs by
// construction), and every models.json/settings.json read-modify-write runs
// under lockPiAgentConfig — so two concurrent rounds each find exactly
// their own entry at pi startup, no matter how their stagings interleave.
// ponytail: a worker killed outright between staging and the round's end
// leaks that one entry — inert (nothing selects it, and its apiKey is the
// ${OPENAI_API_KEY} template or "none", never a live key) and bounded at
// one per hard kill; a restart re-stages fresh.
func stagePiProvider(env []string) ([]string, func() error, error) {
	url := envLookup(env, "OPENAI_BASE_URL")
	if url == "" {
		url = envLookup(env, "OPENAI_API_BASE")
	}
	key := envLookup(env, "OPENAI_API_KEY")
	model := envLookup(env, "OPENAI_MODEL")
	if url == "" && key == "" && model == "" {
		return nil, nil, nil
	}
	if url == "" {
		return nil, nil, fmt.Errorf("pi round: OPENAI_API_KEY/OPENAI_MODEL exported without a base URL — pi has no base-URL env channel, so the round would silently dial api.openai.com; set openai.url (and openai.model), or unset the ambient OPENAI_* exports")
	}
	if model == "" {
		return nil, nil, fmt.Errorf("pi round: openai.url exported without openai.model — pi cannot select a model to bridge the section; set openai.model")
	}
	token, err := piRoundToken()
	if err != nil {
		return nil, nil, err
	}
	id := piProviderPrefix + "-" + token
	// The slim relay: when the worker runs the tool-call relay
	// (internal/toolrelay, started when slim.parser_model is set), pi dials
	// the staged loopback URL instead of the upstream directly — same key
	// and model, upstream path prefix preserved — and the relay lifts
	// text-encoded tool calls into the native tool_calls wire pi executes.
	baseURL := url
	if relay := envLookup(env, config.ToolRelayURLEnv); relay != "" {
		baseURL = relay
	}
	entry := piProviderConfig{
		Name:    "Daedalus (openai section)",
		BaseURL: baseURL,
		// openai-completions is pi's OpenAI-compatible chat-completions
		// wire — what litellm/ollama/vLLM self-hosted endpoints serve.
		API:    "openai-completions",
		APIKey: "${OPENAI_API_KEY}",
	}
	if key == "" {
		entry.APIKey = "none"
	}
	modelEntry := piModelConfig{ID: model}
	// Staged unconditionally (see piModelCompat): no backend wants the param.
	storeOff := false
	modelEntry.Compat = &piModelCompat{SupportsStore: &storeOff}
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
		if s.Temperature != nil {
			params["temperature"] = *s.Temperature
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
	if err := mergePiModelsEntry(id, entry); err != nil {
		return nil, nil, fmt.Errorf("stage pi provider entry: %w", err)
	}
	// From here on an entry is on disk, so every exit must hand back the
	// cleanup — including the timeout-merge failure below — or the entry
	// would outlive the round that can never launch.
	drop := func() error { return removePiProviderEntry(id) }
	// pi folds a provider request at its own 5-minute default
	// (retry.provider.timeoutMs, which defaults to httpIdleTimeoutMs) and
	// reads no timeout env var, so API_TIMEOUT_MS alone never reaches it —
	// at slow-model pace a legitimate reviewer turn outlives the default,
	// pi's agent-level retries burn, and the round exits verdict-less. The
	// bridge stages the same ceiling other agents get from the export into
	// pi's own channel, so one config value bounds every CLI. Derived from
	// the exported var (the serving section's timeout, per the AgentEnv
	// invariant that a ceiling is always exported), so an absent or
	// unparseable value leaves pi's default standing.
	if n, err := strconv.Atoi(envLookup(env, "API_TIMEOUT_MS")); err == nil && n > 0 {
		if err := mergePiRetryTimeout(n); err != nil {
			return nil, drop, fmt.Errorf("stage pi provider timeout: %w", err)
		}
	}
	// provider/model splits on the FIRST slash (pi's findExactModelReference
	// Match), so model ids containing slashes (huggingface-style refs) survive
	// — the round-unique suffix contains none.
	return []string{"--model", id + "/" + model}, drop, nil
}

// piRoundToken returns 16 hex chars of crypto-random entropy — the per-round
// suffix completing piProviderPrefix into an id no concurrent round can hold
// (2^64 space; a worker stages at most one entry per round). An entropy
// failure fails the round loudly rather than stage an id another round might
// already hold — the whole race fix rests on the ids being distinct.
func piRoundToken() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("stage pi provider entry: entropy for the round-unique id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// anySampler reports whether any knob in s was set — an all-nil sampler
// means the staged model omits samplingParams entirely, so the backend's
// own sampler values stand (never a sent default overriding them).
func anySampler(s aiderSampler) bool {
	return s.TopP != nil || s.PresencePenalty != nil || s.TopK != nil ||
		s.MinP != nil || s.RepetitionPenalty != nil
}

// mergePiModelsEntry upserts entry into the host's ~/.pi/agent/models.json
// under the round-unique provider id, without disturbing anything else in
// the file: the top level and the providers map are handled as raw JSON, so
// unknown top-level fields and other providers' entries — operator-owned
// ones and other rounds' staged ones alike, including fields this repo's
// structs do not model — round-trip byte-for-byte. The whole
// read-modify-write holds lockPiAgentConfig, so a concurrent round's merge
// re-reads this one's entry instead of renaming over it. A file pi would
// accept but Go cannot parse (JSONC comments) fails the round loudly rather
// than risk clobbering it — pi strips comments, Go's decoder does not.
func mergePiModelsEntry(id string, entry piProviderConfig) error {
	unlock, err := lockPiAgentConfig()
	if err != nil {
		return err
	}
	defer unlock()
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
		// JSON null unmarshals into a map by setting it to nil, and pi's
		// JSON.parse accepts a whole-file null — merge it as empty rather
		// than panic on the assignment below.
		if top == nil {
			top = map[string]json.RawMessage{}
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
		if providers == nil {
			providers = map[string]json.RawMessage{}
		}
	}
	providers[id] = raw
	top["providers"], err = json.Marshal(providers)
	if err != nil {
		return err
	}
	return writePiAgentJSON("models.json", top)
}

// removePiProviderEntry deletes one round's staged provider id from the
// host's ~/.pi/agent/models.json — stagePiProvider's cleanup, called at the
// round's end. It is safe against every concurrent round by construction:
// the id is this round's alone, so the removal can never touch another
// round's entry, and the locked read-modify-write cannot drop entries a
// concurrent merge added after this read. Safe against the round's own pi
// because pi reads models.json only at startup — the process launched long
// before this runs. A missing file or an id already absent is a no-op (the
// file replaced or trimmed underneath us); a file pi would accept but Go
// cannot parse fails loudly, mirroring the merge — an unparseable file must
// never be rewritten by either direction.
func removePiProviderEntry(id string) error {
	unlock, err := lockPiAgentConfig()
	if err != nil {
		return err
	}
	defer unlock()
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	path := filepath.Join(home, ".pi", "agent", "models.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	top := map[string]json.RawMessage{}
	if err := json.Unmarshal(data, &top); err != nil {
		return fmt.Errorf("parse existing %s (pi also accepts JSON comments, which this removal cannot round-trip — move them out): %w", path, err)
	}
	if top == nil {
		// A whole-file null carries no providers at all — nothing staged to
		// remove (same shape as the merge's null handling).
		return nil
	}
	providers := map[string]json.RawMessage{}
	if prev, ok := top["providers"]; ok {
		if err := json.Unmarshal(prev, &providers); err != nil {
			return fmt.Errorf("parse providers object in %s: %w", path, err)
		}
		if providers == nil {
			return nil
		}
	}
	if _, ok := providers[id]; !ok {
		return nil
	}
	delete(providers, id)
	top["providers"], err = json.Marshal(providers)
	if err != nil {
		return err
	}
	return writePiAgentJSON("models.json", top)
}

// lockPiAgentConfig takes the host-wide exclusive lock serializing every
// daedalus read-modify-write of the shared ~/.pi/agent config files — the
// models.json upsert/removal and the settings.json timeout merge. It is
// flock on a dedicated lock file because the writes are atomic renames:
// locking models.json itself would key the lock to an inode the next rename
// replaces, and two writers would then hold "the lock" on different inodes.
// The lock file lives under ~/.daedalus (daedalus-owned space, like the
// session records) rather than pi's config dir, so pi only ever finds files
// it owns there. Workers are same-user by construction — they all bridge
// the same ~/.pi — so a per-user lock file covers every writer on the host,
// and the in-process concurrent rounds (max_concurrent_agent_runs > 1) hold
// separate open file descriptions, which flock excludes like any other
// process. Blocking: a holder's critical section is read, marshal, rename —
// microseconds — and the kernel drops the lock if the holder dies, so the
// only wait is a genuinely concurrent staging, never a stale lock; EINTR
// retries. The returned func releases; callers must not nest (a second
// flock on a fresh fd of the same file would block on itself).
func lockPiAgentConfig() (func(), error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(home, ".daedalus")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "pi-stage.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
		if err == nil {
			break
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		f.Close()
		return nil, err
	}
	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}

// mergePiRetryTimeout upserts retry.provider.timeoutMs into the host's
// ~/.pi/agent/settings.json — pi's only request-timeout channel (pi reads
// no timeout env var, and its models.json provider entries carry no
// timeoutMs it plumbs — source-verified 2026-09-27, references exist only
// in interactive-TUI code). daedalus owns exactly that one key, derived
// from the same config timeout other agents get as API_TIMEOUT_MS; the
// merge is idempotent and preserves every other setting, including the
// rest of any user retry block. The read-modify-write holds
// lockPiAgentConfig like the models.json merge. If pi later plumbs
// timeoutMs on provider entries, stage it there and retire this settings
// write.
// ponytail: the lock closes the torn-write/lost-update class, but the key
// itself is one shared setting — pi has no per-round timeout channel — so
// a round can still observe a concurrent round's timeoutMs value at its
// startup read (both operator-configured; the provider entry, which IS
// per-round, never collides).
func mergePiRetryTimeout(timeoutMS int) error {
	unlock, err := lockPiAgentConfig()
	if err != nil {
		return err
	}
	defer unlock()
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	path := filepath.Join(home, ".pi", "agent", "settings.json")
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	top := map[string]json.RawMessage{}
	if len(data) > 0 {
		// Unlike models.json (comment-stripped on pi's side), pi parses
		// settings.json with plain JSON.parse — a file with comments is
		// already broken for pi itself; failing the merge loudly rather
		// than clobbering it is the only correct posture either way.
		if err := json.Unmarshal(data, &top); err != nil {
			return fmt.Errorf("parse existing %s: %w", path, err)
		}
		// JSON null unmarshals into a map by setting it to nil, and pi's
		// JSON.parse accepts null shapes (whole-file null, "retry": null,
		// "provider": null) that carry no user data to clobber — merge
		// each as empty rather than panic on the assignments below.
		if top == nil {
			top = map[string]json.RawMessage{}
		}
	}
	retry := map[string]json.RawMessage{}
	if prev, ok := top["retry"]; ok {
		if err := json.Unmarshal(prev, &retry); err != nil {
			return fmt.Errorf("parse retry object in %s: %w", path, err)
		}
		if retry == nil {
			retry = map[string]json.RawMessage{}
		}
	}
	provider := map[string]json.RawMessage{}
	if prev, ok := retry["provider"]; ok {
		if err := json.Unmarshal(prev, &provider); err != nil {
			return fmt.Errorf("parse retry.provider object in %s: %w", path, err)
		}
		if provider == nil {
			provider = map[string]json.RawMessage{}
		}
	}
	provider["timeoutMs"], err = json.Marshal(timeoutMS)
	if err != nil {
		return err
	}
	retry["provider"], err = json.Marshal(provider)
	if err != nil {
		return err
	}
	top["retry"], err = json.Marshal(retry)
	if err != nil {
		return err
	}
	return writePiAgentJSON("settings.json", top)
}

// writePiAgentJSON atomically writes one file under the host's
// ~/.pi/agent/ as indented JSON: write-then-rename keeps a mid-write crash
// from leaving pi a truncated config, and the original mode is preserved
// for an existing file. Its callers complete a read-modify-write and hold
// lockPiAgentConfig around it — this function must never take that lock
// itself (it runs under it; a second flock on a fresh fd would block on
// itself).
func writePiAgentJSON(name string, top map[string]json.RawMessage) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	path := filepath.Join(home, ".pi", "agent", name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	out, err := json.MarshalIndent(top, "", "  ")
	if err != nil {
		return err
	}
	mode := os.FileMode(0o644)
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode().Perm()
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+name+"-*")
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
