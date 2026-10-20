// Package config loads Daedalus runtime configuration from a YAML file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Defaults applied when the corresponding config.yaml field is unset.
const (
	// DefaultTaskQueue routes this daedalus deployment's work. Distinct
	// projects or flows sharing one Temporal server each use their own
	// queue — the queue also scopes workflow IDs and worktree paths, so
	// nothing collides across queues.
	DefaultTaskQueue = "daedalus"
	// ReservedTestTaskQueue is the fleet-wide shared suite queue name
	// ("test"): the queue an unmodified config (shared_test_queue unset or
	// true) schedules its native test suites on and its workers poll, so
	// any deployment's worker can serve any deployment's suites. It is
	// rejected as temporal.task_queue — a main queue of that name would
	// receive suite tasks no pipeline schedules and lose its own routing —
	// and so is any name carrying the "-test" suffix, whose suite queue
	// (TestQueueFor) would collide with the stem deployment's.
	ReservedTestTaskQueue = "test"
	// DefaultTemporalHost is Temporal's own default frontend address.
	DefaultTemporalHost = "127.0.0.1:7233"
	// DefaultTemporalUIPort is the Temporal UI's own default port.
	DefaultTemporalUIPort = 8233
	// DefaultAgent is the jailed agent CLI used when config agent is unset.
	DefaultAgent = "claude"
	// DefaultBranchPrefix prefixes the preserved branch that carries a run's
	// approved work when config branch_prefix is unset.
	DefaultBranchPrefix = "daedalus"
	// DefaultAnthropicTimeoutMS bounds the jailed agent's API requests,
	// exported to the agent as API_TIMEOUT_MS. Generous by design: agent
	// rounds legitimately run long (whole-repo analyses, slow builds), and a
	// tight client-side ceiling kills rounds the pipeline would keep.
	DefaultAnthropicTimeoutMS = 3_000_000
	// DefaultTestsTimeout bounds one execution of a repo's native test suite
	// when config tests_timeout is unset. Wider than the shared activity
	// ceiling because test-command discovery and a cold build legitimately
	// overrun it.
	DefaultTestsTimeout = 30 * time.Minute
	// DefaultAgentRunTimeout bounds one jailed-agent round
	// (implementation or review) when config agent_run_timeout is unset.
	// Wider than the shared activity ceiling because agent rounds
	// legitimately run long (whole-repo analyses, slow builds).
	DefaultAgentRunTimeout = 45 * time.Minute
	// DefaultReviewTimeout bounds one jailed reviewer round
	// (RunJailedReviewerActivity) when config review_timeout is unset. It
	// preserves the historical behavior: the reviewers shared the fixed
	// 15-minute activity ceiling. A review cut off at this ceiling retries
	// with its conversation resumed, so consecutive windows accumulate.
	DefaultReviewTimeout = 15 * time.Minute
	// DefaultCleanupTimeout bounds one CleanupWorktreeActivity — committing
	// the aborted snapshot, unregistering the worktree, and removing its
	// directory — when config cleanup_timeout is unset. Wider than the
	// shared 15-minute activity ceiling: removing a large worktree (a
	// build tree can hold hundreds of thousands of files) is filesystem-
	// bound work that has burned the full 15 minutes in practice, and a
	// timed-out cleanup risks losing the aborted-work snapshot.
	DefaultCleanupTimeout = 30 * time.Minute
	// DefaultMaxConcurrentAgentRuns caps how many jailed-agent rounds run at
	// once on this worker when config max_concurrent_agent_runs is unset.
	// Concurrent cold sessions share one provider account's throughput, so
	// unbounded parallelism slows every run; queued rounds wait for a slot
	// (heartbeating while they do) and then run at full speed.
	DefaultMaxConcurrentAgentRuns = 2
	// DefaultMaxConcurrentTests caps how many native test-suite executions
	// run at once on this worker when config max_concurrent_tests is unset.
	// Suites are CPU-bound host work, so the cap is separate from the
	// provider-bound agent rounds; queued suites wait for a slot
	// (heartbeating while they do).
	DefaultMaxConcurrentTests = 2
	// DefaultFallbackType is the fallback provider's wire style when its
	// type field is unset.
	DefaultFallbackType = FallbackTypeAnthropic
	// DefaultBugDir is the effective bug_filing.dir when filing is enabled
	// without an explicit dir — the historical always-on path.
	DefaultBugDir = "backlog/bugs"
	// DefaultTestOutputDir is the effective test_output.dir when dumping is
	// enabled without an explicit dir — the .daedalus/ entrypoint
	// convention, like the .daedalus.yaml test declaration.
	DefaultTestOutputDir = ".daedalus/test-output"
	// DefaultMetricsUIPort is the metrics daemon's UI listener port when
	// metrics.port is unset — the dashboard's own decided default (htmx-ui
	// pt1 design, Q14/Q17): the daedalus dashboard and the GoLens mount.
	DefaultMetricsUIPort = 7777
	// DefaultMetricsRelayPort is the metrics daemon's relay listener port
	// when metrics.relay_port is unset: loopback-only, physically
	// unreachable off-host (Q17), one below the UI port by the same
	// day-one-knob precedent.
	DefaultMetricsRelayPort = 7778
)

// Fallback wire styles accepted by the config's fallback.type.
const (
	// FallbackTypeAnthropic is an Anthropic-compatible endpoint.
	FallbackTypeAnthropic = "anthropic"
	// FallbackTypeOpenAI is an OpenAI-compatible chat-completions endpoint.
	// Caveat: a jailed round's wire is chosen by the agent, not by daedalus
	// — an openai-style fallback serves only agents that themselves dial
	// OPENAI_BASE_URL (claude dials ANTHROPIC_*, so it cannot). pi and
	// aider dial whichever provider their selected model uses: pointing
	// such a model at the fallback works via the same env vars, pointing
	// it at the primary's vendor does not — the model choice is the
	// operator's.
	FallbackTypeOpenAI = "openai"
)

// fallbackTypes lists the accepted fallback wire styles.
var fallbackTypes = []string{FallbackTypeAnthropic, FallbackTypeOpenAI}

// ContextTokensEnv is the environment variable carrying the configured
// context window (anthropic.context_tokens) into jailed rounds. The name
// is claude's own (the var it reads for its compaction/budget math), and
// the aider staging in internal/activities consumes the same export in
// place of its hardcoded input-token constant. Jailed claude rounds are
// env-only — the worktree's .claude/settings.json env block is masked —
// so the process env is the only channel that reaches them.
const ContextTokensEnv = "CLAUDE_CODE_MAX_CONTEXT_TOKENS"

// Env vars carrying the config's per-round agent knobs into jailed rounds.
// Unlike ContextTokensEnv these are daedalus's own names — no jailed CLI
// reads them natively; they exist so the worker-exported environment
// stays the one channel config values travel to activities on (never
// workflow history or activity inputs). The jailed-round builder
// re-exports MaxOutputTokensEnv under claude's own var, the aider staging
// consumes it and the sampler vars, and agents without a route ignore
// them.
const (
	// MaxOutputTokensEnv carries anthropic.max_output_tokens.
	MaxOutputTokensEnv = "DAEDALUS_MAX_OUTPUT_TOKENS"
	// TopPEnv, TemperatureEnv, PresencePenaltyEnv, TopKEnv, MinPEnv, and
	// RepetitionPenaltyEnv carry the openai section's sampler knobs.
	TopPEnv              = "DAEDALUS_TOP_P"
	TemperatureEnv       = "DAEDALUS_TEMPERATURE"
	PresencePenaltyEnv   = "DAEDALUS_PRESENCE_PENALTY"
	TopKEnv              = "DAEDALUS_TOP_K"
	MinPEnv              = "DAEDALUS_MIN_P"
	RepetitionPenaltyEnv = "DAEDALUS_REPETITION_PENALTY"
	// BugDirEnv carries bug_filing.dir — the worktree-relative folder
	// unscoped bug files are filed under — to the prompt-render sites.
	// Exported only when bug_filing is enabled: absent means filing is off,
	// and the absence is load-bearing (the worker unsets it symmetrically
	// and the daemon-spawn scrub keeps a stale shell export out).
	BugDirEnv = "DAEDALUS_BUG_DIR"
	// BugMirrorEnv carries bug_filing.mirror — the host directory each
	// round's filed bug files are copied into after the round (see
	// ResolveMirror). Exported only when filing is enabled and a mirror
	// is configured: absent means the mirror is off, and the absence is
	// load-bearing (the worker unsets it symmetrically and the
	// daemon-spawn scrub keeps a stale shell export out).
	BugMirrorEnv = "DAEDALUS_BUG_MIRROR"
	// TestOutputMirrorEnv carries test_output.mirror — the host directory
	// each suite dump is copied into (see ResolveMirror). Exported only
	// when dumping is enabled and a mirror is configured, under the same
	// symmetric-unset discipline as BugMirrorEnv. It travels worker env
	// rather than the pipeline input that carries test_output.dir
	// because the mirror is a host path: a shared test queue may run a
	// suite on a foreign deployment's worker, and each deployment
	// mirrors onto its own host.
	TestOutputMirrorEnv = "DAEDALUS_TEST_OUTPUT_MIRROR"
	// ReviewerURLEnv, ReviewerKeyEnv, and ReviewerModelEnv carry the
	// reviewer section's endpoint (reviewer.url / reviewer.key /
	// reviewer.model) into the jailed-round builder, which pins reviewer
	// rounds' provider endpoint and model to them while implementing and
	// test rounds keep the serving provider's. Exported only when set —
	// absent means reviewers share the implementing agent's endpoint and
	// model exactly as before.
	ReviewerURLEnv   = "DAEDALUS_REVIEWER_BASE_URL"
	ReviewerKeyEnv   = "DAEDALUS_REVIEWER_API_KEY"
	ReviewerModelEnv = "DAEDALUS_REVIEWER_MODEL"
	// ReviewerContextTokensEnv carries the reviewer section's
	// context_tokens — reviewer rounds' own context window, staged like
	// the serving anthropic.context_tokens (ContextTokensEnv → pi's
	// ContextWindow, aider's staged metadata input side, opencode's
	// limit.context, claude's own var). Exported only when set — absent
	// means reviewer rounds keep the serving window.
	ReviewerContextTokensEnv = "DAEDALUS_REVIEWER_CONTEXT_TOKENS"
	// DevURLEnv, DevKeyEnv, DevModelEnv, DevContextTokensEnv, and
	// DevThinkingEnv carry the dev section's overrides (dev.url / key /
	// model / context_tokens / disable_thinking) into the jailed-round
	// builder, which pins the dev rounds — the implementing agent's
	// rounds: the slim flow's plan, parse, step, and fix rounds among
	// them — to them while review roles keep theirs. Exported only when
	// set — absent means dev rounds keep the serving provider's values
	// exactly as before.
	DevURLEnv           = "DAEDALUS_DEV_BASE_URL"
	DevKeyEnv           = "DAEDALUS_DEV_API_KEY"
	DevModelEnv         = "DAEDALUS_DEV_MODEL"
	DevContextTokensEnv = "DAEDALUS_DEV_CONTEXT_TOKENS"
	DevThinkingEnv      = "DAEDALUS_DEV_THINKING"
	// FinalReviewerURLEnv, FinalReviewerKeyEnv, FinalReviewerModelEnv,
	// FinalReviewerContextTokensEnv, and FinalReviewerThinkingEnv carry
	// the final_reviewer section's overrides into the jailed-round
	// builder for the terminal review — the last slim sub-task's review
	// loop, the rounds that see the cumulative diff of the whole run.
	// Each unset field falls back, per round, to the reviewer section's
	// (ReviewerURLEnv / ReviewerKeyEnv / ReviewerModelEnv /
	// ReviewerContextTokensEnv / ReviewerThinkingEnv), then to the
	// serving sections — resolved by reviewerEnv and wireThinkingEnv.
	// Exported only when set — absent means the final review stages from
	// the reviewer chain exactly as before.
	FinalReviewerURLEnv           = "DAEDALUS_FINAL_REVIEWER_BASE_URL"
	FinalReviewerKeyEnv           = "DAEDALUS_FINAL_REVIEWER_API_KEY"
	FinalReviewerModelEnv         = "DAEDALUS_FINAL_REVIEWER_MODEL"
	FinalReviewerContextTokensEnv = "DAEDALUS_FINAL_REVIEWER_CONTEXT_TOKENS"
	FinalReviewerThinkingEnv      = "DAEDALUS_FINAL_REVIEWER_THINKING"
	// OpenAIThinkingEnv and ReviewerThinkingEnv carry the openai and
	// reviewer sections' disable_thinking knobs to the jailed-round
	// builder, which resolves them per role into WireThinkingEnv — the
	// round-env var the provider stagings read (pi's staged samplingParams,
	// aider's extra_body) to stage the disable-thinking encoding onto the
	// wire request. Exported only when set ("off"; an explicit
	// disable_thinking: true) — absent means that role's rounds send no
	// thinking signal and the backend's own default stands. All three are
	// scrubbed from the daemon spawn (ProviderEnvVars) like every other
	// provider-derived value.
	OpenAIThinkingEnv   = "DAEDALUS_OPENAI_THINKING"
	ReviewerThinkingEnv = "DAEDALUS_REVIEWER_THINKING"
	WireThinkingEnv     = "DAEDALUS_WIRE_THINKING"
	// ToolRelayURLEnv carries the staged base URL of the worker's slim
	// tool-call relay (internal/toolrelay) — the relay's loopback address
	// plus the openai upstream's path prefix, so pi's request paths land
	// on the relay exactly as they would land on the upstream. Exported
	// only while the relay is running; absent means pi dials the upstream
	// directly, the pre-relay behavior. Unset symmetrically and scrubbed
	// from the daemon spawn (ProviderEnvVars) so a stale export can never
	// point a new worker's rounds at a dead relay.
	ToolRelayURLEnv = "DAEDALUS_TOOL_RELAY_URL"
	// VerdictRepairURLEnv, VerdictRepairModelEnv, and VerdictRepairKeyEnv
	// carry the verdict_repair section's endpoint (url / model / key) to
	// the review activity's decision-model repair pass, which classifies a
	// marker-less reviewer output into one of the four verdict markers.
	// Exported only when set — absent means the repair layer is off and
	// marker-less rounds keep the pre-repair behavior. The key is optional
	// (a local relay needs none); all three are scrubbed from the daemon
	// spawn (ProviderEnvVars) like every other provider-derived value.
	VerdictRepairURLEnv   = "DAEDALUS_VERDICT_REPAIR_URL"
	VerdictRepairModelEnv = "DAEDALUS_VERDICT_REPAIR_MODEL"
	VerdictRepairKeyEnv   = "DAEDALUS_VERDICT_REPAIR_KEY"
	// MetricsRelayAnthropicEnv and MetricsRelayOpenAIEnv carry the staged
	// base URLs of the metrics relay (internal/metricsrelay) for each
	// provider wire: the relay's loopback address plus the worker-scoped
	// path prefix (/relay/<worker-name>/<wire>) plus the upstream's own
	// path prefix, so a CLI appending its usual suffix to the staged URL
	// lands on the relay exactly where it would have landed on the
	// upstream. Exported only while the metrics daemon is running and the
	// worker verified and registered with it (metrics.enabled); absent
	// means rounds dial the provider directly, the pre-metrics behavior.
	// Unset symmetrically and scrubbed from the daemon spawn
	// (ProviderEnvVars) like every other provider-derived value; the
	// failover and reviewer endpoint overrides remove them from the round
	// env (see provider_failover.go) so a staged URL can never route a
	// round at an upstream it was not registered for.
	MetricsRelayAnthropicEnv = "DAEDALUS_METRICS_RELAY_ANTHROPIC_URL"
	MetricsRelayOpenAIEnv    = "DAEDALUS_METRICS_RELAY_OPENAI_URL"
	// MetricsRelayTokenEnv is the registration's secret restoration token:
	// minted by the worker, presented at registration (the relay stores it
	// and never returns it), and exported by the worker for staging —
	// per-round staging appends it to the synthetic attribution key, and
	// the relay requires a match before restoring any registered key —
	// without it any on-host process could spend a registered worker's
	// key with a fabricated task-shaped credential.
	MetricsRelayTokenEnv = "DAEDALUS_METRICS_RELAY_TOKEN"
)

// TemporalConfig describes the Temporal deployment daedalus talks to.
type TemporalConfig struct {
	// Host is the frontend host:port.
	Host string `yaml:"host"`
	// UIPort is the Temporal UI HTTP port. Informational only — daedalus
	// never connects to the UI, but shows it at worker startup.
	UIPort int `yaml:"ui_port"`
	// TaskQueue routes this deployment's workflows and activities. Distinct
	// projects or flows sharing one Temporal server use distinct queues.
	TaskQueue string `yaml:"task_queue"`
}

// AnthropicConfig configures the jailed agent's Anthropic backend. The
// values are injected into the agent's environment (ANTHROPIC_BASE_URL,
// ANTHROPIC_API_KEY, ANTHROPIC_MODEL, ANTHROPIC_DEFAULT_HAIKU_MODEL,
// API_TIMEOUT_MS); the key is required for the worker.
type AnthropicConfig struct {
	URL   string `yaml:"url"`
	Key   string `yaml:"key"`
	Model string `yaml:"model"`
	// HeartbeatModel is a small/fast model for tiny prompts (session-title
	// generation and the like), exported as ANTHROPIC_DEFAULT_HAIKU_MODEL —
	// the var the jailed claude reads for its small/fast model selection.
	// Unrelated to the workflow layer's quota heartbeats despite the name.
	// Empty means the agent's own default; `worker status` probes with it
	// (falling back to Model) when set.
	HeartbeatModel string `yaml:"heartbeat_model"`
	// TimeoutMS bounds the agent's API requests, exported as API_TIMEOUT_MS.
	// Zero (unset) defaults to DefaultAnthropicTimeoutMS — unlike the string
	// fields there is no "inherit the environment" escape hatch: the jailed
	// agent gets an explicit ceiling either way.
	TimeoutMS int `yaml:"timeout_ms"`
	// ContextTokens, when set, is the jailed agent's context window in
	// tokens — the one config knob overriding the agents' window
	// arithmetic. It exports as ContextTokensEnv, which jailed claude
	// honors for its compaction/budget math, and replaces the input side of
	// the model metadata staged for aider rounds (aider's completion cap is
	// unchanged — see MaxOutputTokens for that). openai-served opencode
	// rounds stage it as limit.context on the staged provider's model entry
	// (stageOpencodeProvider, selected with -m); amp and codex have
	// no wired lever and simply ignore it. The staged entry rides
	// OPENCODE_CONFIG, whose precedence against a repo-committed
	// opencode.json (project config, which upstream merge order puts
	// above it) was never probed — see the stageOpencodeProvider ponytail
	// note. Zero (unset) leaves every agent on its own
	// default.
	ContextTokens int `yaml:"context_tokens"`
	// MaxOutputTokens, when set, caps the jailed agent's completion size in
	// tokens. It exports as MaxOutputTokensEnv and is translated to each
	// agent's own lever by the jailed-round builder: claude's round env
	// gains CLAUDE_CODE_MAX_OUTPUT_TOKENS (the var claude 2.1.283's own
	// error text names for its request-level max_tokens override) and the
	// aider staging replaces both sides of the 8192 constant (the staged
	// metadata's max_output_tokens and extra_params.max_tokens).
	// Openai-served opencode rounds stage it as limit.output on the staged
	// provider's model entry; amp and codex have no wired route. Zero
	// (unset) keeps the aider constant and every other agent's API default.
	MaxOutputTokens int `yaml:"max_output_tokens"`
}

// FallbackConfig is a fully independent secondary provider: its url/key/
// model need have nothing in common with the primary (a different vendor's
// anthropic-compatible endpoint is fine). When enabled and a jailed round
// fails with the primary's API exhausted (429 quota), the worker retries
// the round once against these values and keeps using them until the
// primary's quota window resets. Values travel the same env-only channel
// as the primary's (DAEDALUS_FALLBACK_* — never workflow history, activity
// inputs, argv, or logs).
type FallbackConfig struct {
	Enabled bool `yaml:"enabled"`
	// Type selects the fallback's wire style: "anthropic" (the default —
	// an Anthropic-compatible endpoint) or "openai" (an OpenAI-compatible
	// chat-completions endpoint). It governs how the worker status probe
	// questions the fallback and which vars failover values travel on. A
	// jailed round's wire is chosen by the agent itself: an openai-style
	// fallback serves only agents that dial OPENAI_BASE_URL — claude dials
	// ANTHROPIC_* and cannot use it, while pi and aider dial whichever
	// provider their selected model uses (codex dials OpenAI wire formats
	// only, so an openai-style fallback is exactly what serves it — an
	// anthropic-style one has no codex channel). Empty loads as the default.
	Type  string `yaml:"type"`
	URL   string `yaml:"url"`
	Key   string `yaml:"key"`
	Model string `yaml:"model"`
	// HeartbeatModel mirrors AnthropicConfig.HeartbeatModel for fallback
	// rounds; empty falls back to Model, then to the agent's default.
	HeartbeatModel string `yaml:"heartbeat_model"`
}

// RoleOverrideConfig is one jailed-round role's optional provider override
// group: endpoint url, key, model id, context window, and wire-level
// thinking toggle, each independently settable — any subset is legal
// (model without url is the same-endpoint-different-brain split; a window
// alone resizes just that role's budget arithmetic). Every unset field
// falls back — the final reviewer's to the reviewer section's, then every
// role's to the serving sections — so an all-unset group stages
// byte-identically to the single-provider behavior. Values travel the
// env-only channel (never workflow history, activity inputs, or argv); see
// activities.reviewerEnv and devEnv for the application rules and
// wireThinkingEnv for the thinking resolution.
type RoleOverrideConfig struct {
	URL string `yaml:"url"`
	Key string `yaml:"key"`
	// Model, when set, is the model id the role's rounds request — from
	// the role's endpoint when url is set, else from the serving
	// endpoint. It overrides both wire families' model vars, so every
	// staging path (claude's env, aider's --model, the pi/codex/opencode
	// staged model entries) picks it up from the round env.
	Model string `yaml:"model"`
	// ContextTokens, when set, is the role's context window in tokens,
	// staged like the serving anthropic.context_tokens (ContextTokensEnv
	// → pi's ContextWindow, aider's staged metadata input side,
	// opencode's limit.context, claude's own var): only this role's
	// budget arithmetic changes — the serving window and the other roles'
	// staged entries are untouched. Zero (unset) keeps the serving window.
	ContextTokens int `yaml:"context_tokens"`
	// DisableThinking, when true, stages the disable-thinking encoding
	// onto the role's wire requests (see OpenAIConfig.DisableThinking).
	// Each role controls its own brain: an unset knob sends nothing, the
	// backend's own default standing — except the dev role, whose unset
	// knob falls back to the serving openai section's toggle (the dev
	// rounds' pre-role behavior). Backends that ignore the encoding make
	// it a silent no-op by design.
	DisableThinking bool `yaml:"disable_thinking"`
}

// VerdictRepairConfig points the decision-model repair pass at its
// classify-only endpoint: when a reviewer round's output carries no
// parseable verdict marker, the review activity asks this model to read the
// review and name the most likely verdict among the four markers. url is
// the /v1 base of a decision-model endpoint speaking the systemone
// contract (POST <url>/systemone — the OpenAI-compat chat paths reject
// decision-capable models, so a chat-completions URL cannot serve here);
// with Ollama serving tev1 that is http://localhost:11434/v1 and model
// tev1:latest. key is optional (a local relay needs none). Absent (the
// default) the repair layer is off: marker-less rounds keep the pre-repair
// behavior. Under slim mode the section is required (url and model), since
// slim's small models are the marker-less-exit generator and this layer is
// its recovery mechanism.
type VerdictRepairConfig struct {
	URL   string `yaml:"url"`
	Model string `yaml:"model"`
	Key   string `yaml:"key"`
}

// ThinkingDisabled reports an explicit thinking: false — the only setting
// under which daedalus sends an off signal to jailed rounds (translated to
// each agent's own lever in the jailed-round builder; see Config.Thinking).
// An absent key (nil) and an explicit true both leave every agent on its
// own default.
func (c Config) ThinkingDisabled() bool { return c.Thinking != nil && !*c.Thinking }

// StreamSetting reports the explicit stream toggle as "on"/"off"; ok is
// false when the key is absent (nil) — every agent then follows its own
// streaming default.
func (c Config) StreamSetting() (value string, ok bool) {
	if c.Stream == nil {
		return "", false
	}
	if *c.Stream {
		return "on", true
	}
	return "off", true
}

// Active reports whether the fallback is configured for use.
func (f FallbackConfig) Active() bool {
	return f.Enabled
}

// OpenAIConfig is injected into the jailed agent's environment as
// OPENAI_BASE_URL / OPENAI_API_KEY / OPENAI_MODEL (plus OPENAI_API_BASE —
// set from the same value as OPENAI_BASE_URL, because the litellm versions
// behind tooling like aider disagree on which var they honor). Entirely
// optional — available to tooling the agent runs. An aider round consumes
// the section directly: the worker selects aider's model with
// `--model openai/<model>` when it is set (aider's litellm layer only dials
// a custom endpoint for a provider-prefixed model name; see runJailedRound).
// A codex round consumes it through per-invocation `-c` overrides (codex
// reads no OPENAI_* var natively — see stageCodexProvider), so the section
// is the only way an openai endpoint serves codex.
type OpenAIConfig struct {
	URL   string `yaml:"url"`
	Key   string `yaml:"key"`
	Model string `yaml:"model"`
	// TimeoutMS bounds the jailed agent's API requests, exported as
	// API_TIMEOUT_MS. The export is baked into the worker's environment
	// once at startup, so the rule is: whenever url is set, this field's
	// value is the one exported — including mixed configs where the
	// anthropic section still serves claude rounds and this section exists
	// only for openai-dialing tooling, which is why an operator in that
	// setup sets it to a ceiling both consumers tolerate. Zero (unset)
	// falls back to anthropic.timeout_ms's value (which Load defaults), so
	// the jailed agent gets an explicit ceiling either way; a self-hosted
	// endpoint whose litellm applies only ~600s on its own should set it
	// explicitly.
	TimeoutMS int `yaml:"timeout_ms"`
	// Sampler knobs for rounds served by this section — sampler behavior is
	// a property of the backend behind url, so they live at provider level
	// and are shared by every agent that dials it. They export as the
	// DAEDALUS_* vars named in the env-var block above and are consumed
	// today only by the aider staging: temperature, top_p,
	// presence_penalty, and repetition_penalty ride the staged settings'
	// extra_params top level (litellm maps them natively), while top_k and
	// min_p ride extra_body — litellm's syntax for params it does not map.
	// pi's staging bridge sends them as top-level OpenAI-completions
	// request params; claude and amp are out of scope by decision;
	// opencode's provider.<id>.options may carry them but was never
	// probed. Zero
	// (unset) means the field is omitted from the request entirely —
	// absent, never sent as null/0, because a sent default would override
	// the backend's own sampler defaults (the exact bug class that
	// motivated these knobs). An explicit 0 in the file is therefore
	// indistinguishable from unset.
	TopP            float64 `yaml:"top_p"`
	Temperature     float64 `yaml:"temperature"`
	PresencePenalty float64 `yaml:"presence_penalty"`
	TopK            float64 `yaml:"top_k"`
	MinP            float64 `yaml:"min_p"`
	// RepetitionPenalty re-lands the empirically decisive anti-loop field
	// of the 2026-09-25 self-hosted round: its constants (with top_k and
	// min_p) were probe-verified live against the tenor litellm proxy but
	// lost before landing, and dropping this one would recreate the
	// output-loop bug the constants fixed — so it joins the decided list
	// as a fifth knob rather than being silently omitted.
	RepetitionPenalty float64 `yaml:"repetition_penalty"`
	// DisableThinking, when true, stages the disable-thinking encoding onto
	// this section's rounds' wire requests — chat_template_kwargs
	// {"enable_thinking": false}, the Qwen3-template convention, the
	// toggle's target brain family. It rides the same channel as the
	// sampler knobs (OpenAIThinkingEnv, consumed by the pi and aider
	// stagings; agents without a wire staging ignore it), and only true
	// ever sends a signal: false and unset both leave the backend's own
	// thinking default standing (never-send-a-default).
	// ponytail: the encoding is probe-owed — the serving endpoint is
	// unreachable from the round sandbox, and omlx-class backends silently
	// drop unknown fields, so a backend that cannot honor the key behaves
	// exactly as before; a probe pinning a different shape edits the one
	// staging spot per consuming agent.
	DisableThinking bool `yaml:"disable_thinking"`
}

// configured reports whether the section carries any operator-set value —
// the provider mixing rule's "both set" test. Validation runs after
// applyDefaults, which fills the timeout on every loaded config, so the
// comparison is against the defaulted zero section rather than the bare
// zero with the field excluded: an absent section and one that named only
// timeout_ms differ by that field alone, and excluding the field (the
// earlier shape) called the only-timeout section absent — slipping it
// past the mixing gate with its value sitting unused in favor of the
// provider section's.
// ponytail: zero-ness still cannot see an explicit timeout_ms equal to
// DefaultAnthropicTimeoutMS — it reads as absent, and the value only
// survives because it equals what the provider channel exports anyway;
// telling every explicit value apart needs tracking which fields the
// file actually named, not just their zero-ness.
func (a AnthropicConfig) configured() bool {
	return a != (AnthropicConfig{TimeoutMS: DefaultAnthropicTimeoutMS})
}

// configured is AnthropicConfig.configured's openai-side counterpart; no
// field of this section is defaulted before validation.
func (o OpenAIConfig) configured() bool { return o != (OpenAIConfig{}) }

// ProviderConfig is the wire-agnostic primary provider section: its type
// field — not the section name — selects the wire format, so one section
// fully determines how the model behind it is served. It is the single home
// for the per-model-class knobs a round needs: the endpoint surface
// (url/key/model), the request ceiling (timeout_ms), the honest-sizing pair
// (context_tokens/max_output_tokens — wire-agnostic by design, since the
// staging paths that consume sizing key on the exports, not on a section;
// that is the fix for sizing keys that used to be expressible only under
// anthropic:), and the sampler fields (an openai-wire property — request
// params only an OpenAI-compatible backend accepts).
//
// The legacy anthropic:/openai: sections keep working unchanged and
// indefinitely. The accepted mixes: a provider section alone, or one plus
// the *other* wire's legacy section (e.g. type: openai for a self-hosted
// model while jailed claude stays on anthropic:) — the incremental
// migration path. The same-wire legacy section alongside is a load error,
// never a silent override: two sources of truth for one channel is exactly
// the section-name confusion this section removes.
//
// Which jailed CLI consumes the section is still agent:'s decision (it
// stays top-level, as do the agent-native levers thinking/stream): staging
// keys off the exported env vars, and the type dispatches those onto the
// ANTHROPIC_*/OPENAI_* channels, so every existing consumer — the
// aider/pi/codex/opencode staging, the status probe, the slim tool relay —
// serves a provider-served round with no per-agent knowledge of the
// section.
type ProviderConfig struct {
	// Type selects the wire format and the env channel the values export
	// on: "anthropic" (ANTHROPIC_*) or "openai" (OPENAI_*). Required — a
	// missing or unknown value is a load error, never a silent default: a
	// defaulted wire would recreate the section-name ambiguity the section
	// replaces.
	Type string `yaml:"type"`
	// API base URL, key, and model, injected into the jailed agent's
	// environment on the channel Type selects, with the legacy sections'
	// unset-means-inherit rule: an empty field exports nothing.
	URL   string `yaml:"url"`
	Key   string `yaml:"key"`
	Model string `yaml:"model"`
	// HeartbeatModel is AnthropicConfig.HeartbeatModel's counterpart:
	// exported as ANTHROPIC_DEFAULT_HAIKU_MODEL and probed by `worker
	// status`. It is an anthropic-wire knob (claude's small/fast-model var
	// is its only consumer), so an openai-typed provider carrying it is a
	// load error — a knob on the wire without a consumer must fail loud,
	// never silently no-op. Empty means the agent's own default.
	HeartbeatModel string `yaml:"heartbeat_model"`
	// TimeoutMS bounds the jailed agent's API requests, exported as
	// API_TIMEOUT_MS — both wire styles need it. Zero (unset) defaults to
	// DefaultAnthropicTimeoutMS, so a ceiling is always exported.
	TimeoutMS int `yaml:"timeout_ms"`
	// ContextTokens and MaxOutputTokens are the sizing knobs, identical in
	// shape and channel to their AnthropicConfig counterparts — and
	// deliberately wire-agnostic: they export as ContextTokensEnv /
	// MaxOutputTokensEnv whichever type is set, reaching every staging path
	// that consumes sizing (claude's env re-export, aider's model
	// metadata, the pi and opencode staged model entries). Load rejects
	// negative values.
	//
	// Sizing caveat for pi rounds, from the 2026-10-10 RCA of three parked
	// wa-termo bootstraps: pi computes each request's max_tokens as the
	// staged contextWindow minus its prompt estimate minus a 4096-token
	// safety reserve, floored at 1 (pi-ai simple-options.js
	// clampMaxTokensToContext, release 0.87.1) — an understaged
	// context_tokens yields silent 1-token replies, so stage the window
	// honestly.
	ContextTokens int `yaml:"context_tokens"`
	// See ContextTokens.
	MaxOutputTokens int `yaml:"max_output_tokens"`
	// Sampler knobs for rounds served by this section — OpenAIConfig's
	// fields with the same absent-never-sent discipline (an explicit 0 is
	// indistinguishable from unset, so the backend's own sampler values
	// stand; NaN/Inf and negatives other than presence_penalty's legal
	// [-2, 2] range are load errors). An openai-wire property —
	// chat-completions request params — so an anthropic-typed provider
	// carrying any of them is a load error.
	TopP              float64 `yaml:"top_p"`
	Temperature       float64 `yaml:"temperature"`
	PresencePenalty   float64 `yaml:"presence_penalty"`
	TopK              float64 `yaml:"top_k"`
	MinP              float64 `yaml:"min_p"`
	RepetitionPenalty float64 `yaml:"repetition_penalty"`
	// DisableThinking is OpenAIConfig.DisableThinking's provider-section
	// counterpart: same encoding, same env channel and staging consumers
	// (OpenAIThinkingEnv, the pi and aider stagings), same
	// never-send-a-default discipline — only true sends a signal. An
	// openai-wire property like the sampler knobs above, so an
	// anthropic-typed provider carrying it is a load error.
	DisableThinking bool `yaml:"disable_thinking"`
}

// anthropicChannel projects the section onto the ANTHROPIC_* channel —
// every field the channel's exports read, nothing else.
func (p ProviderConfig) anthropicChannel() AnthropicConfig {
	return AnthropicConfig{
		URL:             p.URL,
		Key:             p.Key,
		Model:           p.Model,
		HeartbeatModel:  p.HeartbeatModel,
		TimeoutMS:       p.TimeoutMS,
		ContextTokens:   p.ContextTokens,
		MaxOutputTokens: p.MaxOutputTokens,
	}
}

// openaiChannel projects the section onto the OPENAI_* channel. The sizing
// pair is absent by construction: those exports are read off the
// anthropic-side struct, which ProviderChannels feeds for an openai-typed
// provider.
func (p ProviderConfig) openaiChannel() OpenAIConfig {
	return OpenAIConfig{
		URL:               p.URL,
		Key:               p.Key,
		Model:             p.Model,
		TimeoutMS:         p.TimeoutMS,
		TopP:              p.TopP,
		Temperature:       p.Temperature,
		PresencePenalty:   p.PresencePenalty,
		TopK:              p.TopK,
		MinP:              p.MinP,
		RepetitionPenalty: p.RepetitionPenalty,
		DisableThinking:   p.DisableThinking,
	}
}

// validate rejects the provider sections Load refuses, with Load's
// diagnostics (Config.validate wraps them with the section name). Call it
// on a defaults-applied section.
func (p ProviderConfig) validate() error {
	if p.Type == "" {
		return fmt.Errorf("type is required — %q or %q; it selects which env channel the section's values export on",
			FallbackTypeAnthropic, FallbackTypeOpenAI)
	}
	if !slices.Contains(fallbackTypes, p.Type) {
		return fmt.Errorf("unknown type %q (available: %s)", p.Type, strings.Join(fallbackTypes, ", "))
	}
	if p.ContextTokens < 0 {
		return fmt.Errorf("context_tokens: must not be negative")
	}
	if p.MaxOutputTokens < 0 {
		return fmt.Errorf("max_output_tokens: must not be negative")
	}
	if p.TimeoutMS < 0 {
		return fmt.Errorf("timeout_ms: must not be negative")
	}
	// The sampler checks mirror the openai section's (Config.validate's
	// loop): presence_penalty is exempt from the negative check because its
	// API range is [-2, 2], and NaN/Inf are rejected for all six — they
	// would render as non-float literals in the staged files, silently
	// dropping them.
	for _, s := range []struct {
		name  string
		v     float64
		negOK bool
	}{
		{"top_p", p.TopP, false},
		{"temperature", p.Temperature, false},
		{"presence_penalty", p.PresencePenalty, true},
		{"top_k", p.TopK, false},
		{"min_p", p.MinP, false},
		{"repetition_penalty", p.RepetitionPenalty, false},
	} {
		if math.IsNaN(s.v) || math.IsInf(s.v, 0) {
			return fmt.Errorf("%s: must be a finite number", s.name)
		}
		if !s.negOK && s.v < 0 {
			return fmt.Errorf("%s: must not be negative", s.name)
		}
	}
	if p.Type == FallbackTypeOpenAI && p.HeartbeatModel != "" {
		return fmt.Errorf("heartbeat_model: only the anthropic wire consumes it (claude's small/fast-model var) — remove it from an openai-typed provider")
	}
	if p.Type == FallbackTypeAnthropic &&
		(p.TopP != 0 || p.Temperature != 0 || p.PresencePenalty != 0 ||
			p.TopK != 0 || p.MinP != 0 || p.RepetitionPenalty != 0) {
		return fmt.Errorf("sampler knobs apply only to the openai wire (chat-completions request params; no anthropic-wire staging consumes them) — remove them from an anthropic-typed provider")
	}
	if p.Type == FallbackTypeAnthropic && p.DisableThinking {
		return fmt.Errorf("disable_thinking applies only to the openai wire (a chat-completions request param; no anthropic-wire staging consumes it) — remove it from an anthropic-typed provider")
	}
	return nil
}

// BugFilingConfig toggles where jailed rounds record out-of-scope bugs.
// Off (the section absent, or enabled: false — the default), no bug files
// are written: bugs surface in the round's reply or review comments only,
// alongside the project-own record duty the prompts carry regardless. On,
// the prompts additionally instruct the agent to file every out-of-scope
// bug as a file under dir — worktree-relative, resolved against the run's
// worktree root,
// so the files are ordinary committed content of the branch. Dir empty
// keeps the historical backlog/bugs path.
type BugFilingConfig struct {
	Enabled bool   `yaml:"enabled"`
	Dir     string `yaml:"dir"`
	// Mirror, when set, names a host directory each filed bug file lands
	// in, complementing the worktree-relative dir. The host dir is
	// bind-mounted read-write into each jailed round's sandbox at dir
	// (activities' --rw-map), so the agent's writes land on the host
	// directly and never ride the branch; without the mount, the
	// worker-side copy after the round remains as the fallback shape.
	// Absolute, or ~/… expanded against the worker's home (see
	// ResolveMirror); a relative value, the filesystem root, or a colon
	// is rejected at load. Empty keeps mirroring off.
	Mirror string `yaml:"mirror"`
}

// TestOutputConfig toggles dumping each native test suite's complete
// combined output into a worktree file. Off (the section absent, or
// enabled: false — the default), no file is written and the run behaves
// byte-identically to before. On, RunTestSuiteActivity writes the full
// output to <dir>/<timestamp>.log in the run's worktree and names the path
// to the jailed tester and reviewer — a worktree-relative dir puts the dump
// inside the ai-jail sandbox's only mounted tree, so the fix loop holding a
// transport-bounded tail can read the complete record (the task-log
// pointer is decorative to a jailed agent; /tmp/daedalus is not mounted).
// Dir empty keeps DefaultTestOutputDir. Unlike bug_filing's files, a dump
// is transient and never part of the deliverable: the activity keeps the
// dir out of `git status` via .git/info/exclude.
type TestOutputConfig struct {
	Enabled bool   `yaml:"enabled"`
	Dir     string `yaml:"dir"`
	// Mirror, when set, names a host directory each suite dump is copied
	// into after the suite runs — a host-side reflection that survives
	// worktree cleanup, which otherwise disposes of the dumps. Copied
	// worker-side (the dump is written by the activity, already
	// host-capable — no mount). Same accepted shapes and rejection rules
	// as BugFilingConfig.Mirror (see ResolveMirror); empty keeps mirroring
	// off.
	Mirror string `yaml:"mirror"`
}

// DependencyConfig governs what a run does when its `depends_on`
// dependency stops without approval. The default posture is still the
// refusal the gate has always had — the run fails before anything was
// started — because the skip flags all default false and an empty
// fallback_branch gives a release nowhere to land. opting into
// `fallback_branch` turns the gate from a wall into a switch: a dependency
// that stopped in a skip-listed state releases the waiting run onto that
// branch instead of failing it.
type DependencyConfig struct {
	// Enabled gates the whole section: false restores the unconditional
	// refusal whatever the other fields say. Default true; tri-state
	// (*bool) because applyDefaults cannot tell an explicit false from an
	// unset field — the same idiom as SharedTestQueue.
	Enabled *bool `yaml:"enabled"`
	// FallbackBranch names the branch a released run starts its worktree
	// from. Empty — the default — means the invocation branch, resolved
	// once at submit against the run's repo (a detached HEAD refuses with
	// the error naming this key as the fix). An explicitly set branch is
	// verified to exist at submit, so a typo fails the run before the
	// dependency gate instead of after it. The resolved name travels in
	// the workflow input, so a config edit mid-run cannot retarget an
	// in-flight chain.
	FallbackBranch string `yaml:"fallback_branch"`
	// SkipParked releases a run past a dependency that ended in a park
	// (preserved on its aborted branch, resumable with `daedalus
	// continue`). Default true — a parked dependency is the one stopped
	// state whose work survives, so its dependent is routinely started
	// rather than stranded; tri-state like Enabled for the same reason.
	SkipParked *bool `yaml:"skip_parked"`
	// SkipFailed releases past a dependency whose workflow failed. Default
	// false: a failed dependency produced nothing to build on.
	SkipFailed bool `yaml:"skip_failed"`
	// SkipStuck releases past a dependency that ran past its Temporal
	// timeouts and was killed (status "timed out"). Default false.
	SkipStuck bool `yaml:"skip_stuck"`
	// SkipCanceled releases past a dependency that was canceled or
	// terminated. Default false.
	SkipCanceled bool `yaml:"skip_canceled"`
}

// EnabledOrDefault reports the section's effective enabled: true unless an
// explicit false was loaded (the default — nil — is on).
func (d DependencyConfig) EnabledOrDefault() bool {
	return d.Enabled == nil || *d.Enabled
}

// SkipParkedOrDefault reports the effective skip_parked: true unless an
// explicit false was loaded (the default — nil — is on).
func (d DependencyConfig) SkipParkedOrDefault() bool {
	return d.SkipParked == nil || *d.SkipParked
}

// ReleasePosture reports whether the section can release a dependent run
// past a dependency that stopped without approval: enabled, with a
// fallback branch resolved. Both the submit preflight and the workflow
// gate key on it; which stopped states actually release is the skip flags'
// decision (workflows.dependencyReleases), and a paused or vanished
// dependency is outside every skip set.
func (d DependencyConfig) ReleasePosture() bool {
	return d.EnabledOrDefault() && d.FallbackBranch != ""
}

// SlimConfig is the slim mode section. Intentionally breaking reshape
// (2026-10-02): the historical top-level `slim: true/false` boolean became
// this section — existing configs migrate by renaming the value to
// slim.enabled.
type SlimConfig struct {
	// Enabled carries the historical slim boolean's semantics: the slot
	// clamp to 1, the DAEDALUS_SLIM=1 export (aider weak/editor pinning),
	// and the run flow's slim default (a defaulted -w reroutes to the slim
	// flow).
	Enabled bool `yaml:"enabled"`
	// ParserModel names the model the worker's tool-call relay
	// (internal/toolrelay) uses to normalize lifted arguments, resolved
	// against the openai section's upstream via ollama structured output.
	// Empty — the default — means the relay never starts and slim rounds
	// behave exactly as before the relay existed: text-encoded tool calls
	// from text-emitting models stay inert (pi executes only native
	// tool_calls).
	ParserModel string `yaml:"parser_model"`
	// MaxInputTokens is the slim input gate's ceiling: a `daedalus run` that
	// resolves to the slim flow refuses to start when its task input — the
	// shrunken text (internal/shrink) the run would embed in every round
	// prompt — counts more than this many tokens. The gate fires at submit,
	// before any workflow, worktree, or worker round, and the error tells
	// the operator to split the task. Zero (the key unset or empty) means
	// DefaultSlimMaxInputTokens; a negative value is a config load error.
	// It is a ceiling on input, not a model window — hence not
	// context_tokens — and matches the max_output_tokens precedent.
	MaxInputTokens int `yaml:"max_input_tokens"`
}

// DefaultSlimMaxInputTokens is the slim input gate's ceiling when
// slim.max_input_tokens is unset: just over the 4096-token prompt budget
// of the smallest staged self-hosted window this flow targets, so a task
// that fills it leaves the model no output budget at all (the pi
// max_tokens clamp: request max_tokens = contextWindow − prompt estimate −
// 4096 reserve).
const DefaultSlimMaxInputTokens = 4098

// MaxInputTokensOrDefault returns the configured slim input ceiling, or
// DefaultSlimMaxInputTokens when unset.
func (s SlimConfig) MaxInputTokensOrDefault() int {
	if s.MaxInputTokens > 0 {
		return s.MaxInputTokens
	}
	return DefaultSlimMaxInputTokens
}

// UnmarshalYAML migrates the historical top-level `slim: true/false`
// boolean the 2026-10-02 reshape renamed to slim.enabled: a scalar node
// decodes into Enabled, so an upgrading operator's config loads instead
// of failing the strict decode with a "cannot unmarshal !!bool into
// config.SlimConfig" type error that names no migration. The section
// shape decodes field by field, with the mapping's keys checked by hand —
// KnownFields strictness does not reach through a custom unmarshaler, so
// the hand check is what keeps a typo'd section key failing the load with
// the yaml-style "field … not found" error.
func (s *SlimConfig) UnmarshalYAML(value *yaml.Node) error {
	if value.Tag == "!!null" {
		return nil
	}
	if value.Kind == yaml.ScalarNode {
		return value.Decode(&s.Enabled)
	}
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: cannot unmarshal %s into config.SlimConfig", value.Line, value.ShortTag())
	}
	for i := 0; i+1 < len(value.Content); i += 2 {
		key, val := value.Content[i], value.Content[i+1]
		switch key.Value {
		case "enabled":
			if err := val.Decode(&s.Enabled); err != nil {
				return err
			}
		case "parser_model":
			if err := val.Decode(&s.ParserModel); err != nil {
				return err
			}
		case "max_input_tokens":
			if err := val.Decode(&s.MaxInputTokens); err != nil {
				return err
			}
		default:
			return fmt.Errorf("line %d: field %s not found in type config.SlimConfig", key.Line, key.Value)
		}
	}
	return nil
}

// MetricsConfig gates the metrics subsystem: the local LLM relay, its
// per-request usage ledger, and the `daedalus metrics` daemon that hosts
// them (plus the dashboard served on the same process — see the htmx-ui
// pt1 design). On by default (an unset enabled is on): the dashboard is
// the operator's window into runs, and the relay is what makes its usage
// numbers exist — a subsystem that required an opt-in per project was
// off more often than anyone meant it to be. The cost of on is named so
// the opt-out is a decision, not a forgotten default: with metrics on,
// every round's prompt transits the localhost relay process. Set
// `enabled: false` to turn the subsystem off for this project's rounds.
// When on, the project requires the host-global daemon (worker
// start/restart probes the relay listener and fails fast without it),
// and the worker stages the relay's URL into every round's provider
// environment so the relay reads each request's usage block —
// wire-shaped, not CLI-shaped: an anthropic stream always carries usage,
// an openai-wire stream only when the client asks for it
// (stream_options.include_usage), so a streamed openai-wire round can
// ledger zero tokens. A CLI dialing its provider directly (no staged URL
// override) bypasses the relay invisibly: coverage is per-CLI-wiring.
type MetricsConfig struct {
	// Enabled turns the subsystem on for this project's rounds — nil
	// (unset) is on; false opts out. When on, the worker boot verifies
	// the metrics daemon, registers this worker's upstream mapping with
	// it, and stages the relay URL into jailed rounds. omitempty is the
	// render contract (RenderYAML): an unset key renders absent —
	// marshaling the nil pointer would print `enabled: null`, a shape
	// no config line means. The repo's two other defaulted pointer keys
	// still have that wart (shared_test_queue, dependency.enabled;
	// backlog/bugs/config-render-defaulted-pointer-keys-emit-null.md) —
	// this field is fixed, not the family.
	Enabled *bool `yaml:"enabled,omitempty"`
	// Port is the daemon's UI listener (the dashboard mux). Zero (unset)
	// defaults to DefaultMetricsUIPort; equal to relay_port is a load
	// error — one port, one listener.
	Port int `yaml:"port"`
	// RelayPort is the daemon's relay listener, bound to 127.0.0.1 only —
	// the relay is unreachable off-host by construction (the UI mux may
	// serve the LAN; the relay never does). Zero (unset) defaults to
	// DefaultMetricsRelayPort.
	RelayPort int `yaml:"relay_port"`
}

// Config holds the runtime configuration for a Daedalus process, loaded
// from a YAML file (see config-example.yaml).
type Config struct {
	// Agent selects which jailed CLI runs the implementing and reviewer
	// agents: "claude" (Claude Code), "opencode", "amp", "pi", "aider"
	// (deprecated), or "codex" (see agents for the accepted values).
	Agent string `yaml:"agent"`
	// BranchPrefix names the preserved branch carrying a run's approved
	// work: <prefix>/issue-<id>-<unix timestamp>. It is deliberately
	// separate from temporal.task_queue — the queue routes workflows and
	// scopes worktree paths, while this is repo-facing branch naming.
	// `daedalus run --prefix` overrides it per run.
	BranchPrefix string `yaml:"branch_prefix"`
	// Authorship, when true, commits daedalus's own work — the approved
	// deliverable and the aborted-work snapshot — as author/committer
	// "daedalus <daedalus@local>". False (the default) leaves the commits
	// to the worker's git config, so they carry whoever runs the worker.
	Authorship bool `yaml:"authorship"`
	// WorkerID names this deployment's worker daemon for management: the
	// daemon-dir pid/log/config-record files are keyed by it, and
	// `daedalus worker restart <id>` addresses it from any directory. It is
	// deliberately separate from temporal.task_queue — two workers may
	// share one queue under different ids, each with its own config (e.g. a
	// rotated API key). Empty means the task queue names the worker, so a
	// single-queue deployment needs no id.
	WorkerID string `yaml:"worker_id"`
	// TestsTimeout bounds one execution of the repo's native test suite
	// (RunNativeTestsActivity): test-command discovery and the suite itself
	// share this budget. A time.ParseDuration string in YAML ("30m");
	// DefaultTestsTimeout when unset.
	TestsTimeout time.Duration `yaml:"tests_timeout"`
	// AgentRunTimeout bounds one jailed agent round — the implementation,
	// fix, and rebuild rounds (RunJailedClaudeActivity). A
	// time.ParseDuration string in YAML ("45m"); DefaultAgentRunTimeout
	// when unset.
	AgentRunTimeout time.Duration `yaml:"agent_run_timeout"`
	// ReviewTimeout bounds one jailed reviewer round
	// (RunJailedReviewerActivity) — the code and test reviewers are jailed
	// rounds like the implementer, and a whole-repo review legitimately
	// runs long. A review cut off at this ceiling retries with its
	// conversation resumed, so consecutive windows accumulate instead of
	// restarting. A time.ParseDuration string in YAML ("15m");
	// DefaultReviewTimeout when unset.
	ReviewTimeout time.Duration `yaml:"review_timeout"`
	// CleanupTimeout bounds one CleanupWorktreeActivity: committing the
	// aborted-work snapshot, unregistering the worktree, and removing its
	// directory. A time.ParseDuration string in YAML ("15m");
	// DefaultCleanupTimeout when unset.
	CleanupTimeout time.Duration `yaml:"cleanup_timeout"`
	// MaxConcurrentAgentRuns caps how many jailed-agent rounds run at once
	// on this worker; further rounds queue until a slot frees. Concurrent
	// cold agent sessions share one provider account, so unbounded
	// parallelism (many workflows on one task queue) slows every run.
	// DefaultMaxConcurrentAgentRuns when unset.
	MaxConcurrentAgentRuns int `yaml:"max_concurrent_agent_runs"`
	// MaxRequestsPerMinute, when positive, paces this deployment's
	// provider traffic through the worker's loopback relay
	// (internal/toolrelay): jailed pi rounds' chat-completions requests —
	// agent and reviewer alike — are forwarded only up to this many per
	// sliding one-minute window, a request beyond the cap waiting inside
	// the relay until the window frees. The point is a provider whose API
	// tier rate-limits by request count: staying under the cap avoids the
	// 429 quota-heartbeat crawl instead of absorbing it. Enforcement rides
	// the relay's staging, so it covers exactly the rounds the relay
	// serves (pi on the openai channel; a reviewer.url or openai-fallback
	// override bypasses it) — Load rejects the key with any other agent or
	// without an openai url, where it would be silently inert. Zero
	// (unset) means unthrottled.
	MaxRequestsPerMinute int `yaml:"max_requests_per_minute"`
	// Slim targets small self-hosted models (small context window, low max
	// output tokens) on the aider and pi agents: when enabled, the worker
	// runs a single jailed-agent round at a time (MaxConcurrentAgentRuns is
	// clamped to 1, so no two provider requests are ever in flight) and
	// exports DAEDALUS_SLIM=1, under which jailed aider rounds pin
	// AIDER_WEAK_MODEL/AIDER_EDITOR_MODEL to the effective AIDER_MODEL.
	// With parser_model also set (and the openai section configured), the
	// worker additionally starts the loopback tool-call relay
	// (internal/toolrelay), which lifts text-encoded tool calls into the
	// native tool_calls wire pi executes. Applies to the deployment, not
	// to an agent: valid regardless of `agent`, so a `run --cli aider`
	// override needs no config edit. The per-value limits (model ids,
	// context/output budgets) stay operator-side — see
	// config-example.yaml's slim entry for the per-agent prerequisites.
	Slim SlimConfig `yaml:"slim"`
	// Mid targets mid-tier remote models: capable endpoints on either wire
	// (OpenAI-compatible or anthropic) with large context windows, served
	// through the same provider sections as any other round. Mid adds no
	// endpoint fields of its own, no wire restriction, no concurrency clamp,
	// no flow reroute, and no per-agent branches — the deployment's existing
	// channel and agent wiring serve it as-is. What it changes is the review
	// verdict path: mid requires verdict_repair (url and model, validated at
	// load like slim's requirement) because marker-less reviewer rounds route
	// through the /v1/systemone decision contract. Flagship remains the
	// default: with neither this nor Slim.Enabled set, behavior is
	// byte-for-byte the pre-mid behavior. Mutually exclusive with
	// slim.enabled — one model tier per deployment; setting both is a load
	// error naming both keys.
	Mid bool `yaml:"mid"`
	// Thinking is the agents' thinking toggle, independent of Slim: nil
	// (the key absent, the default) and an explicit true mean daedalus
	// sends no thinking signal at all and every agent follows its own
	// default; only an explicit false makes the jailed-round builder
	// translate the off signal to each agent's own lever — pi argv gains
	// `--thinking off`, aider gains `--thinking-tokens 0`, claude's round
	// env gains MAX_THINKING_TOKENS=0 (0-disables verified in claude
	// 2.1.283's own code). opencode and amp have no verified off lever and
	// silently ignore the setting, per the agnostic-config rule. A pointer
	// because an absent key and an explicit false are otherwise the same
	// Go zero value.
	Thinking *bool `yaml:"thinking"`
	// Stream is the agents' response-streaming toggle, the tri-state shape
	// of Thinking: nil (the key absent, the default) leaves every agent on
	// its own streaming default; only an explicit true/false makes the
	// jailed-round builder translate the setting to each agent's own
	// lever — today only aider has one (--stream/--no-stream, its
	// documented option pair; unprobed on this host, which has no aider
	// binary — same verification class as --thinking-tokens). The other
	// agents have no verified streaming lever and silently ignore the
	// setting, per the agnostic-config rule.
	Stream *bool `yaml:"stream"`
	// MaxConcurrentTests caps how many native test-suite executions run at
	// once on this worker (RunTestSuiteActivity on the test task queue);
	// further suites queue until a slot frees. Suites are CPU-bound host
	// work, unlike the provider-bound agent rounds, so the cap is separate.
	// DefaultMaxConcurrentTests when unset.
	MaxConcurrentTests int `yaml:"max_concurrent_tests"`
	// SharedTestQueue routes this deployment's native test suites onto the
	// one Temporal-wide shared queue (ReservedTestTaskQueue), where any
	// deployment's worker may execute them — today's fleet-shared
	// behavior, and the effect of nil (the key absent) or true. Explicit
	// false gives the deployment its own derived suite queue
	// (TestQueueFor) that only workers started from this config poll, so
	// a stale worker of any other deployment can no longer serve — or
	// fail — its suites.
	SharedTestQueue *bool           `yaml:"shared_test_queue"`
	Temporal        TemporalConfig  `yaml:"temporal"`
	Anthropic       AnthropicConfig `yaml:"anthropic"`
	OpenAI          OpenAIConfig    `yaml:"openai"`
	// Provider is the wire-agnostic primary provider section (see
	// ProviderConfig). Nil — the key absent, the default — means the legacy
	// anthropic:/openai: sections stand alone, behavior byte-identical to
	// before the section existed.
	Provider *ProviderConfig `yaml:"provider"`
	// Fallback is the independent secondary provider failover uses when
	// the primary is API-exhausted. Inactive unless Enabled.
	Fallback FallbackConfig `yaml:"fallback"`
	// Dev is the dev rounds' own provider override group: the implementing
	// agent's rounds — the slim flow's plan, parse, step, and fix rounds
	// among them. Empty (the section absent) means dev rounds share the
	// serving sections' values exactly as before.
	Dev RoleOverrideConfig `yaml:"dev"`
	// Reviewer is the reviewer rounds' own provider override group; empty
	// (the section absent) means reviewers share the implementing agent's.
	Reviewer RoleOverrideConfig `yaml:"reviewer"`
	// FinalReviewer is the terminal review's own provider override group —
	// the last slim sub-task's review loop, the rounds that see the
	// cumulative diff of the whole run (the motivating case: a bigger
	// window or a different brain for exactly that review). Every unset
	// field falls back to the reviewer section's, then to the serving
	// sections; empty (the section absent) means the final review stages
	// from the reviewer chain.
	FinalReviewer RoleOverrideConfig `yaml:"final_reviewer"`
	// ReviewSkillList is the ordered list of extra skill reviews each
	// review-gated phase of the feature-dev flow appends after its default
	// reviewer approves: the default daedalus review ping-pong runs first,
	// then one ping-pong per entry, in order — the code phase routes each
	// skill's feedback through the implementing agent's session, the test
	// phase through the tester's (and a skill reviewing the suite may
	// verdict REBUILD like the default reviewer, routing an implementation
	// finding back through the dev cycle). Each entry is either "/name" —
	// resolved at round time to <name>/SKILL.md under the run worktree's
	// .claude/skills (project wins) or the worker's ~/.claude/skills — or
	// hand-written prompt text passed through verbatim. Every skill's
	// instructions render into the reviewer prompt under a fixed preamble
	// restating the verdict protocol and the harness-parsed-output
	// contract, so configured text can never reach a reviewer without
	// daedalus's communication rules above it (one render path in the
	// review template). The list is frozen into the run at submit (a
	// continued run keeps its attempt's list); empty — the default — runs
	// the historical default-only reviews.
	ReviewSkillList []string `yaml:"review_skill_list"`
	// VerdictRepair is the decision-model endpoint that repairs marker-less
	// review verdicts (see VerdictRepairConfig). Empty (the section absent)
	// means the repair layer is off.
	VerdictRepair VerdictRepairConfig `yaml:"verdict_repair"`
	// BugFiling is the out-of-scope-bug filing toggle; inactive unless
	// Enabled (see BugFilingConfig).
	BugFiling BugFilingConfig `yaml:"bug_filing"`
	// TestOutput is the suite-output dump toggle; inactive unless Enabled
	// (see TestOutputConfig).
	TestOutput TestOutputConfig `yaml:"test_output"`
	// Dependency governs the -dep gate's broken-chain answer: refuse (the
	// historical default) or release onto a fallback branch (see
	// DependencyConfig).
	Dependency DependencyConfig `yaml:"dependency"`
	// Metrics is the metrics-subsystem gate; on unless Enabled is
	// explicitly false (see MetricsConfig). Global daemon, per-project
	// switch: the daemon is host-global (one port pair, one process),
	// while this per-project section gates that project's relay staging
	// and the boot-time daemon check.
	Metrics MetricsConfig `yaml:"metrics"`
	// Prompt points at a directory of prompt-template overrides: every .md
	// file directly inside it whose file stem names a prompt (the
	// internal/template Prompts — the embedded prompts/*.md stems:
	// implement, implement_fix, continue, tests, tests_failed, tests_review,
	// review, rebuild, investigate, investigate_fix, refactor,
	// refactor_fix, bugfix, bugfix_fix, slim_plan, slim_step, slim_parse,
	// slim_parse_reask, slim_fix)
	// replaces that embedded prompt for this deployment; a stem matching no
	// prompt fails the start. The path may be absolute, ~/…, or relative to
	// this config file's directory. Resolved and validated once at worker
	// startup (missing directory, unreadable file, empty file, template
	// that does not parse, a data field the prompt does not take, a
	// {{template}} action, a {{define}}/{{block}} block (a define or
	// block body can never render in an override — the sole exception is
	// a define named exactly <prompt-name>.md; don't rely on it), a render
	// against the prompt's representative data that fails (a reference
	// that only breaks at render time — a nested access, or one whose
	// {{if}} guard an edit stripped; review renders further times with a
	// diff handoff attached and in the test-review framing), or a review
	// override missing the verdict
	// protocol in its rendered text (checked in the test-review framing,
	// the one shape where the fourth verdict renders) all fail the start);
	// rendered prompts
	// are recorded in workflow history, so replacement content is not
	// secret and may live on the same path as the config. Empty — the
	// default — renders every prompt byte-identically to the embedded one.
	// A string, not a name→path mapping, so Config stays ==-comparable; the
	// one directory keeps a deployment's replacements together. Note the
	// machine contracts a replacement must keep: review's verdict protocol
	// is validated at startup (all four verdict words, whole words, in the
	// rendered text), and slim_parse must keep instructing the
	// parse round to emit the raw SlimSubtask JSON array parseSlimPlan
	// reads (caveat, not validated); slim_plan's replacement carries no
	// such contract — it is pure generation, a prose plan with no JSON,
	// which the slim_parse round transcribes.
	Prompt string `yaml:"prompt"`
}

// BugFilingDir returns the effective worktree-relative folder unscoped bug
// files are filed under: DefaultBugDir when enabled without an explicit
// dir, "" when filing is off — the empty return is the off signal the env
// export and the prompt render sites key on.
func (c Config) BugFilingDir() string {
	if !c.BugFiling.Enabled {
		return ""
	}
	if c.BugFiling.Dir == "" {
		return DefaultBugDir
	}
	return c.BugFiling.Dir
}

// TestOutputDir returns the effective worktree-relative folder suite
// outputs are dumped under: DefaultTestOutputDir when enabled without an
// explicit dir, "" when dumping is off — the empty return is the off
// signal the pipeline input carries to the suite activity.
func (c Config) TestOutputDir() string {
	if !c.TestOutput.Enabled {
		return ""
	}
	if c.TestOutput.Dir == "" {
		return DefaultTestOutputDir
	}
	return c.TestOutput.Dir
}

// durationValue marshals a time.Duration the way config files express them:
// a canonical duration string ("30m0s"), never the raw nanosecond count a
// plain re-marshal would emit. Zero — the unset marker a pre-defaults view
// renders — prints as "0s", zero's own canonical string: a plain 0 would be
// an !!int scalar, which yaml.v3 refuses to decode into a time.Duration
// ("cannot unmarshal !!int `0`"), so the rendered file would not load.
type durationValue time.Duration

func (d durationValue) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}

// renderConfig mirrors Config field-for-field so a re-marshal prints every
// field in struct order with the durations carried by durationValue; the
// nested sections marshal as themselves.
type renderConfig struct {
	Agent                  string              `yaml:"agent"`
	BranchPrefix           string              `yaml:"branch_prefix"`
	Authorship             bool                `yaml:"authorship"`
	WorkerID               string              `yaml:"worker_id"`
	TestsTimeout           durationValue       `yaml:"tests_timeout"`
	AgentRunTimeout        durationValue       `yaml:"agent_run_timeout"`
	ReviewTimeout          durationValue       `yaml:"review_timeout"`
	CleanupTimeout         durationValue       `yaml:"cleanup_timeout"`
	MaxConcurrentAgentRuns int                 `yaml:"max_concurrent_agent_runs"`
	MaxRequestsPerMinute   int                 `yaml:"max_requests_per_minute"`
	MaxConcurrentTests     int                 `yaml:"max_concurrent_tests"`
	SharedTestQueue        *bool               `yaml:"shared_test_queue"`
	Slim                   SlimConfig          `yaml:"slim"`
	Mid                    bool                `yaml:"mid"`
	Temporal               TemporalConfig      `yaml:"temporal"`
	Anthropic              AnthropicConfig     `yaml:"anthropic"`
	OpenAI                 OpenAIConfig        `yaml:"openai"`
	Provider               *ProviderConfig     `yaml:"provider"`
	Fallback               FallbackConfig      `yaml:"fallback"`
	Dev                    RoleOverrideConfig  `yaml:"dev"`
	Reviewer               RoleOverrideConfig  `yaml:"reviewer"`
	FinalReviewer          RoleOverrideConfig  `yaml:"final_reviewer"`
	ReviewSkillList        []string            `yaml:"review_skill_list"`
	VerdictRepair          VerdictRepairConfig `yaml:"verdict_repair"`
	BugFiling              BugFilingConfig     `yaml:"bug_filing"`
	TestOutput             TestOutputConfig    `yaml:"test_output"`
	Dependency             DependencyConfig    `yaml:"dependency"`
	Metrics                MetricsConfig       `yaml:"metrics"`
	Prompt                 string              `yaml:"prompt"`
}

// RenderYAML renders the config as YAML covering every field of the struct,
// in struct order: set values verbatim, unset fields as their zero value
// ("what the file says", not what applyDefaults would fill in). Intended
// for a LoadRaw result — rendering a defaulted Config would misrepresent
// absent fields as set. Keys print unredacted by design: the command shows
// the operator their own file.
func (c Config) RenderYAML() (string, error) {
	out, err := yaml.Marshal(renderConfig{
		Agent:                  c.Agent,
		BranchPrefix:           c.BranchPrefix,
		Authorship:             c.Authorship,
		WorkerID:               c.WorkerID,
		TestsTimeout:           durationValue(c.TestsTimeout),
		AgentRunTimeout:        durationValue(c.AgentRunTimeout),
		ReviewTimeout:          durationValue(c.ReviewTimeout),
		CleanupTimeout:         durationValue(c.CleanupTimeout),
		MaxConcurrentAgentRuns: c.MaxConcurrentAgentRuns,
		MaxRequestsPerMinute:   c.MaxRequestsPerMinute,
		MaxConcurrentTests:     c.MaxConcurrentTests,
		SharedTestQueue:        c.SharedTestQueue,
		Slim:                   c.Slim,
		Mid:                    c.Mid,
		Temporal:               c.Temporal,
		Anthropic:              c.Anthropic,
		OpenAI:                 c.OpenAI,
		Provider:               c.Provider,
		Fallback:               c.Fallback,
		Dev:                    c.Dev,
		Reviewer:               c.Reviewer,
		FinalReviewer:          c.FinalReviewer,
		ReviewSkillList:        c.ReviewSkillList,
		VerdictRepair:          c.VerdictRepair,
		BugFiling:              c.BugFiling,
		TestOutput:             c.TestOutput,
		Dependency:             c.Dependency,
		Metrics:                c.Metrics,
		Prompt:                 c.Prompt,
	})
	return string(out), err
}

// agents lists the accepted config Agent values. amp authenticates through
// AMP_API_KEY in the worker's environment (runJailed passes it into the jail
// when set; amp's host login does not reach the jail) — the anthropic/openai
// config sections do not apply to it. pi and aider authenticate through the
// provider env vars the worker already exports (ANTHROPIC_API_KEY,
// OPENAI_API_KEY, ...) with three caveats: pi also reads ~/.pi/agent/auth.json,
// which the jail's pi preset bridges in read-write (probe-verified — see
// jailedAgentCLI) and which takes priority over the env vars for the same
// provider, so a stale host /login wins; pi honors the openai section's key
// env var but no base-URL env var, so openai-served pi rounds are bridged
// through a staged ~/.pi/agent/models.json provider entry instead
// (stagePiProvider — an openai section missing url or model fails the round
// before it can silently dial api.openai.com); and aider's dotenv load uses
// override=True, but the jail masks the repo's .env to empty, so inside
// jailed rounds the config-derived exports stand. codex authenticates like
// pi — host login state (~/.codex, bridged read-write by the jail) plus the
// provider env vars — but reads no OPENAI_* var natively and speaks only
// OpenAI wire formats, so an openai-served codex round is bridged through
// per-invocation `-c` config overrides instead (stageCodexProvider — the
// same missing-url/missing-model failure posture as pi's staging), and an
// anthropic-section round has no codex channel at all. aider is DEPRECATED
// (amp-parity): it keeps working as-is, accepts no new flags or fixes, and
// its gaps are accepted limitations — no id-addressable resume, openai-only
// model wiring (the anthropic half awaits a litellm probe), uv-tools-only
// install layout. Which wire a round dials is chosen by the agent's selected
// model, not by daedalus — see FallbackTypeOpenAI.
var agents = []string{"claude", "opencode", "amp", "pi", "aider", "codex"}

// Agents lists the agent names ValidateAgent accepts — the single source
// the dashboard's New-Task dropdown renders from, so its options can never
// drift from what the -cli flag and the config loader admit.
func Agents() []string { return slices.Clone(agents) }

// ValidateAgent rejects Agent values Load would refuse. The CLI's
// -cli/--cli flag overrides the config's agent and must fail up front,
// with the same error, rather than at worker startup.
func ValidateAgent(a string) error {
	if !slices.Contains(agents, a) {
		return fmt.Errorf("unknown agent %q (available: %s)", a, strings.Join(agents, ", "))
	}
	return nil
}

// SharesTestQueue reports the effective shared_test_queue value: true —
// fleet-shared suites, the historical routing — unless the config
// explicitly opts out with false.
func (c Config) SharesTestQueue() bool { return c.SharedTestQueue == nil || *c.SharedTestQueue }

// MetricsEnabled reports the effective metrics enabled value: true — the
// subsystem on — unless the config explicitly opts out with false. Same
// pointer idiom as SharesTestQueue.
func (c Config) MetricsEnabled() bool { return c.Metrics.Enabled == nil || *c.Metrics.Enabled }

// TestQueueFor derives the suite-execution queue of the deployment whose
// main task queue is queue: queue + "-test". A config with
// shared_test_queue: false schedules its native test suites on this
// derived queue and its workers poll it, so those suites are schedulable
// only by that deployment's own workers — restarting the deployment's
// daemon is then sufficient to change which binary executes its suites.
// Validation rejects a task_queue carrying the "-test" suffix, so a
// derived name can never collide with another deployment's main queue.
func TestQueueFor(queue string) string { return queue + "-test" }

// UIURL returns the Temporal UI address corresponding to Temporal.UIPort.
func (c Config) UIURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", c.Temporal.UIPort)
}

// WorkerName returns the name the worker daemon is managed under: the
// worker id when set, else the task queue. Callers key the daemon-dir
// files and record-driven restarts by it.
func (c Config) WorkerName() string {
	if c.WorkerID != "" {
		return c.WorkerID
	}
	return c.Temporal.TaskQueue
}

// ValidateWorkerID rejects ids that cannot key the daemon-dir file names
// (worker-<id>.pid/.log/.conf) or would smuggle a path: no path
// separators, whitespace, control characters, dot components, or a
// leading dash. Empty is valid — it means the task queue names the worker.
func ValidateWorkerID(id string) error {
	if id == "" {
		return nil
	}
	bad := func() error {
		return fmt.Errorf("worker id %q must be a plain file-name-safe token (letters, digits, dashes, underscores, dots inside)", id)
	}
	if strings.HasPrefix(id, "-") || id == "." || id == ".." || strings.ContainsAny(id, "/\\ \t\r\n") {
		return bad()
	}
	for _, r := range id {
		if r < 0x20 || r == 0x7f {
			return bad()
		}
	}
	return nil
}

// ProviderChannels resolves the config's two provider channels — the
// structs whose fields AgentEnv exports and worker tooling probes/dials. A
// legacy-only config resolves to its own sections verbatim. A provider
// section owns the channel its type names (validation has already rejected
// a same-wire legacy section, so ownership is unambiguous) while the other
// wire's legacy section passes through untouched, and its wire-agnostic
// knobs land where their exporters read them: an openai-typed provider's
// sizing pair rides the anthropic-side struct — the same channel those
// exports have always used, now fed from the section that may legally
// carry them.
func (c Config) ProviderChannels() (AnthropicConfig, OpenAIConfig) {
	if c.Provider == nil {
		return c.Anthropic, c.OpenAI
	}
	p := *c.Provider
	if p.Type == FallbackTypeAnthropic {
		return p.anthropicChannel(), c.OpenAI
	}
	anth := c.Anthropic
	// The sizing pair: the provider section wins when it sizes (validation
	// has rejected the both-set case, so this only ever overwrites a zero).
	if p.ContextTokens != 0 {
		anth.ContextTokens = p.ContextTokens
	}
	if p.MaxOutputTokens != 0 {
		anth.MaxOutputTokens = p.MaxOutputTokens
	}
	return anth, p.openaiChannel()
}

// AgentWireConflict reports whether this config's serving channels leave
// agent misdirected: a launch whose agent cannot take the only configured
// endpoint dials whatever the agent's own ambient/default wiring reaches —
// the silent misdirection the per-agent staging exists to prevent. The
// matrix is the repo's documented wiring, not the wire names: claude dials
// ANTHROPIC_* and is served by an anthropic channel with a url (custom base)
// or a key alone (the public base with that key); codex, opencode, and
// aider have no anthropic channel at all (openai-only wiring — aider's
// model wiring, opencode's staged-config bridge, codex's config overrides);
// pi is staged on the openai channel, and — reading no ANTHROPIC_BASE_URL
// and getting no anthropic-side staging — an anthropic channel's custom URL
// never reaches it, though a key with no url still does (the public base
// with the exported key); amp authenticates through AMP_API_KEY alone and
// is exempt. A conflict fires only when a serving endpoint EXISTS and no
// channel serves the agent; an all-unset config is the ambient posture and
// never fires. Deliberately not a validate() rule: an openai-url-only
// section under the default agent is a pinned-legal load — the env channel
// serves tooling and openai-wire agents — so the conflict is a property of
// the launch, checked before any worktree or round exists, on the start
// path and the continue path alike, never a silent degradation mid-run.
func (c Config) AgentWireConflict(agent string) error {
	anth, open := c.ProviderChannels()
	switch agent {
	case "amp":
		return nil
	case "claude":
		if open.URL != "" && anth.URL == "" && anth.Key == "" {
			return fmt.Errorf("agent %q cannot be served by this config: claude dials only the anthropic wire and the config's only serving endpoint is OpenAI-wire — set an anthropic url or key, or run an agent that speaks the OpenAI wire", agent)
		}
	case "pi", "codex", "opencode", "aider":
		if open.URL == "" && anth.URL != "" {
			return fmt.Errorf("agent %q cannot be served by this config: it is served only through the openai channel and the config's only serving endpoint is the anthropic wire's custom URL — pi reads no ANTHROPIC_BASE_URL (and daedalus stages no anthropic-side entry), and codex/opencode/aider have no anthropic channel at all; set an openai url or run an agent that speaks the anthropic wire", agent)
		}
	}
	return nil
}

// AgentEnv translates provider configuration into environment variables for
// the jailed agent process. Only explicitly-set values are exported — an
// unset field means "inherit whatever the worker's environment already
// provides", and empty URL/model fields are deliberately not defaulted (the
// defaults live in config-example.yaml as documentation, not in code). The
// worker exports these into its own environment so activities can pass them
// through — the values never travel through workflow history or activity
// inputs.
func (c Config) AgentEnv() []string {
	anth, open := c.ProviderChannels()
	var env []string
	add := func(key, value string) {
		if value != "" {
			env = append(env, key+"="+value)
		}
	}
	add("ANTHROPIC_BASE_URL", anth.URL)
	add("ANTHROPIC_API_KEY", anth.Key)
	add("ANTHROPIC_MODEL", anth.Model)
	add("ANTHROPIC_DEFAULT_HAIKU_MODEL", anth.HeartbeatModel)
	// The context window rides the same env-only channel; unset means the
	// agents' own defaults, so nothing is exported.
	if anth.ContextTokens > 0 {
		add(ContextTokensEnv, strconv.Itoa(anth.ContextTokens))
	}
	// The completion cap rides the same channel; the jailed-round builder
	// re-exports it under claude's own var and the aider staging consumes
	// it (see MaxOutputTokens).
	if anth.MaxOutputTokens > 0 {
		add(MaxOutputTokensEnv, strconv.Itoa(anth.MaxOutputTokens))
	}
	// Sampler knobs export only when set — absent means the field is
	// omitted from the staged request entirely (see OpenAIConfig).
	addFloat := func(key string, v float64) {
		if v != 0 {
			env = append(env, key+"="+strconv.FormatFloat(v, 'g', -1, 64))
		}
	}
	addFloat(TopPEnv, open.TopP)
	addFloat(TemperatureEnv, open.Temperature)
	addFloat(PresencePenaltyEnv, open.PresencePenalty)
	addFloat(TopKEnv, open.TopK)
	addFloat(MinPEnv, open.MinP)
	addFloat(RepetitionPenaltyEnv, open.RepetitionPenalty)
	add("OPENAI_BASE_URL", open.URL)
	add("OPENAI_API_KEY", open.Key)
	add("OPENAI_MODEL", open.Model)
	// Both URL vars carry the same value: aider's litellm layer standardizes
	// on OPENAI_API_BASE while other tooling reads OPENAI_BASE_URL, and
	// litellm versions disagree on which they honor — export both so either
	// resolution lands on the configured endpoint.
	add("OPENAI_API_BASE", open.URL)
	// API_TIMEOUT_MS comes from whichever channel is serving the jailed
	// round. With a provider section, that is the provider channel's
	// ceiling (applyDefaults always gives it one): the cross-wire legacy
	// timeout this ordering once resolved silently is rejected at load, so
	// the override below can only replace a value the operator never set.
	// Legacy-only configs keep the historical ordering: an openai section
	// with a timeout wins when its URL is set, and an openai section
	// without one falls back to anthropic's value (Load defaults it),
	// keeping the invariant that the jailed agent gets an explicit ceiling
	// either way — exporting "0" would zero the client's ceiling rather
	// than clear it, and exporting nothing would leave a mixed config's
	// claude rounds (anthropic-served, openai configured for tooling) with
	// no ceiling at all.
	timeoutMS := anth.TimeoutMS
	if open.URL != "" && open.TimeoutMS != 0 {
		timeoutMS = open.TimeoutMS
	}
	add("API_TIMEOUT_MS", strconv.Itoa(timeoutMS))
	if f := c.Fallback; f.Active() {
		// Type travels only as an override: failover treats every value
		// but "openai" as the anthropic default, so the default is not
		// exported (unset means "use the default", like every other field).
		if f.Type != DefaultFallbackType {
			add("DAEDALUS_FALLBACK_TYPE", f.Type)
		}
		add("DAEDALUS_FALLBACK_BASE_URL", f.URL)
		add("DAEDALUS_FALLBACK_API_KEY", f.Key)
		add("DAEDALUS_FALLBACK_MODEL", f.Model)
		add("DAEDALUS_FALLBACK_HEARTBEAT_MODEL", f.HeartbeatModel)
	}
	add(ReviewerURLEnv, c.Reviewer.URL)
	add(ReviewerKeyEnv, c.Reviewer.Key)
	add(VerdictRepairURLEnv, c.VerdictRepair.URL)
	add(VerdictRepairModelEnv, c.VerdictRepair.Model)
	add(VerdictRepairKeyEnv, c.VerdictRepair.Key)
	return env
}

// RoleOverrideEnv translates the per-role provider override groups (dev,
// reviewer.context_tokens, final_reviewer) into environment variables for
// the worker, following AgentEnv's rules: only explicitly-set values are
// exported — an unset field means that role's rounds resolve to the
// fallback (final reviewer → reviewer → serving; dev and reviewer →
// serving), and nothing is exported for it. The thinking knobs export
// only the off value ("off"; an explicit disable_thinking: true),
// matched strictly like DAEDALUS_THINKING — an ambient garbage value is
// never read as an off. The values never travel through workflow history
// or activity inputs.
func (c Config) RoleOverrideEnv() []string {
	var env []string
	add := func(key, value string) {
		if value != "" {
			env = append(env, key+"="+value)
		}
	}
	addWindow := func(key string, tokens int) {
		if tokens > 0 {
			env = append(env, key+"="+strconv.Itoa(tokens))
		}
	}
	addThinking := func(key string, off bool) {
		if off {
			env = append(env, key+"=off")
		}
	}
	add(DevURLEnv, c.Dev.URL)
	add(DevKeyEnv, c.Dev.Key)
	add(DevModelEnv, c.Dev.Model)
	addWindow(DevContextTokensEnv, c.Dev.ContextTokens)
	addThinking(DevThinkingEnv, c.Dev.DisableThinking)
	addWindow(ReviewerContextTokensEnv, c.Reviewer.ContextTokens)
	add(FinalReviewerURLEnv, c.FinalReviewer.URL)
	add(FinalReviewerKeyEnv, c.FinalReviewer.Key)
	add(FinalReviewerModelEnv, c.FinalReviewer.Model)
	addWindow(FinalReviewerContextTokensEnv, c.FinalReviewer.ContextTokens)
	addThinking(FinalReviewerThinkingEnv, c.FinalReviewer.DisableThinking)
	return env
}

// RoleOverrideEnvVars lists every variable RoleOverrideEnv may export —
// the symmetric-unset list in runWorker and the per-role half of the
// daemon-spawn scrub (ProviderEnvVars carries them too), so a stale
// shell export can never win over a config that says nothing.
func RoleOverrideEnvVars() []string {
	return []string{
		DevURLEnv,
		DevKeyEnv,
		DevModelEnv,
		DevContextTokensEnv,
		DevThinkingEnv,
		ReviewerContextTokensEnv,
		FinalReviewerURLEnv,
		FinalReviewerKeyEnv,
		FinalReviewerModelEnv,
		FinalReviewerContextTokensEnv,
		FinalReviewerThinkingEnv,
	}
}

// ProviderEnvVars lists every environment variable daedalus derives from
// the config's provider sections — the provider-derived half of the
// union in AllWorkerEnvVars, which is the actual spawn-scrub source
// (envWithoutWorkerVars in cmd/daedalus scrubs it at daemon spawn), so
// the recorded config file is the only way provider values reach the
// daemon: a stale export in the operator's terminal can never win over
// a rotated config. Foreground runs keep the inherit semantics (see
// AgentEnv).
func ProviderEnvVars() []string {
	return []string{
		"ANTHROPIC_BASE_URL",
		"ANTHROPIC_API_KEY",
		"ANTHROPIC_MODEL",
		"ANTHROPIC_DEFAULT_HAIKU_MODEL",
		"API_TIMEOUT_MS",
		ContextTokensEnv,
		MaxOutputTokensEnv,
		TopPEnv,
		TemperatureEnv,
		PresencePenaltyEnv,
		TopKEnv,
		MinPEnv,
		RepetitionPenaltyEnv,
		"OPENAI_BASE_URL",
		"OPENAI_API_KEY",
		"OPENAI_MODEL",
		"OPENAI_API_BASE",
		"DAEDALUS_FALLBACK_TYPE",
		"DAEDALUS_FALLBACK_BASE_URL",
		"DAEDALUS_FALLBACK_API_KEY",
		"DAEDALUS_FALLBACK_MODEL",
		"DAEDALUS_FALLBACK_HEARTBEAT_MODEL",
		ReviewerURLEnv,
		ReviewerKeyEnv,
		ReviewerModelEnv,
		ReviewerContextTokensEnv,
		DevURLEnv,
		DevKeyEnv,
		DevModelEnv,
		DevContextTokensEnv,
		DevThinkingEnv,
		FinalReviewerURLEnv,
		FinalReviewerKeyEnv,
		FinalReviewerModelEnv,
		FinalReviewerContextTokensEnv,
		FinalReviewerThinkingEnv,
		OpenAIThinkingEnv,
		ReviewerThinkingEnv,
		WireThinkingEnv,
		VerdictRepairURLEnv,
		VerdictRepairModelEnv,
		VerdictRepairKeyEnv,
		ToolRelayURLEnv,
		MetricsRelayAnthropicEnv,
		MetricsRelayOpenAIEnv,
		MetricsRelayTokenEnv,
		BugDirEnv,
		BugMirrorEnv,
		TestOutputMirrorEnv,
	}
}

// WorkerEnvVars lists the worker-ecosystem variables that must never
// inherit ambiently, beyond the config-derived set in ProviderEnvVars:
// the worker name, its concurrency clamps, the mode/agent switches its
// jailed rounds inherit through the os.Environ() passthrough, and the
// daemon marker (worker.go's daemonEnv, carried on the spawned daemon's
// env — never a shell's). runWorker re-exports every member from the
// recorded config EXCEPT OPENCODE_CONFIG: the opencode bridge stages it
// round-scoped into the round env, runWorker never sets it, and an
// operator's own export would ride the same passthrough into the pins
// that read the var back off the ai-jail call — so it scrubs with the
// rest and must never return ambient.
func WorkerEnvVars() []string {
	return []string{
		"DAEDALUS_WORKER_NAME",
		"DAEDALUS_MAX_CONCURRENT_AGENT_RUNS",
		"DAEDALUS_MAX_CONCURRENT_TESTS",
		"DAEDALUS_SLIM",
		"DAEDALUS_THINKING",
		"DAEDALUS_STREAM",
		"DAEDALUS_AGENT",
		"OPENCODE_CONFIG",
		"DAEDALUS_WORKER_DAEMON",
	}
}

// AllWorkerEnvVars is the full scrub set: ProviderEnvVars plus the
// worker's own exports. The single composition point — the daemon-spawn
// scrub (envWithoutWorkerVars in cmd/daedalus: the primary production
// consumer) and the test suites' TestMain scrubs and the export
// reconciliation scan consume this, never the two lists separately —
// so a future third list or a per-site filter cannot leave one package
// non-hermetic while the others stay clean.
func AllWorkerEnvVars() []string {
	return append(ProviderEnvVars(), WorkerEnvVars()...)
}

// roundEnvWorkerSideVars is the ProviderEnvVars subset the worker process
// consumes before the jail spawn, so it never needs to reach the round
// env (RoundEnvVars): the fallback section (provider_failover.go reads it
// to build failover envs and bench dry providers), the per-role provider
// endpoints — reviewer, dev, final-reviewer — and the verdict-repair
// endpoint (reviewerEnv, devEnv, and the repair pass read them
// worker-side; all carry credentials), the metrics-staging inputs
// (metricsRelayEnv reads them pre-spawn and stages their output onto the
// provider vars themselves), and the bug/mirror host paths (the prompt
// render and the post-round mirror copies consume them worker-side). A
// ProviderEnvVars member not listed here serves the jailed CLI directly
// and reaches the round env by default.
var roundEnvWorkerSideVars = map[string]bool{
	"DAEDALUS_FALLBACK_TYPE":            true,
	"DAEDALUS_FALLBACK_BASE_URL":        true,
	"DAEDALUS_FALLBACK_API_KEY":         true,
	"DAEDALUS_FALLBACK_MODEL":           true,
	"DAEDALUS_FALLBACK_HEARTBEAT_MODEL": true,
	ReviewerURLEnv:                      true,
	ReviewerKeyEnv:                      true,
	DevURLEnv:                           true,
	DevKeyEnv:                           true,
	FinalReviewerURLEnv:                 true,
	FinalReviewerKeyEnv:                 true,
	VerdictRepairURLEnv:                 true,
	VerdictRepairModelEnv:               true,
	VerdictRepairKeyEnv:                 true,
	MetricsRelayAnthropicEnv:            true,
	MetricsRelayOpenAIEnv:               true,
	MetricsRelayTokenEnv:                true,
	BugDirEnv:                           true,
	BugMirrorEnv:                        true,
	TestOutputMirrorEnv:                 true,
}

// RoundEnvVars is the jailed round env's allow-list: the only variables
// the sandbox may receive (roundEnvAllowlist in internal/activities
// strips everything else from the assembled round env right before the
// ai-jail spawn, so no other family's key, worker control knob, or
// ambient value rides in). Beyond the provider-derived set —
// ProviderEnvVars minus roundEnvWorkerSideVars — it carries the names the
// round builder itself stages (OPENCODE_CONFIG; claude's
// MAX_THINKING_TOKENS and CLAUDE_CODE_MAX_OUTPUT_TOKENS; the slim aider
// pins), the operator exports named agents consume in-jail (AIDER_MODEL,
// and AMP_API_KEY / CODEX_HOME / XDG_DATA_HOME, which the builder's
// --env passthrough copies from this same round env), and the ambient
// base every round needs: shell identity and locale, the proxy and TLS
// trust shape, and the go module-fetch knobs a jailed `go test` inherits.
// The base is the whole vouch list — an agent knob it does not name (the
// wider AIDER_*/CLAUDE_CODE_* families, NODE_EXTRA_CA_CERTS, …) stops at
// the sandbox door and joins this list only as a conscious addition.
// ponytail: the base's sufficiency on a deployment is proven only by
// rounds running there; a missing entry surfaces as a round-level tool
// failure, never silently.
func RoundEnvVars() []string {
	vars := make([]string, 0, len(ProviderEnvVars())+33)
	for _, name := range ProviderEnvVars() {
		if !roundEnvWorkerSideVars[name] {
			vars = append(vars, name)
		}
	}
	return append(vars,
		"OPENCODE_CONFIG",
		"MAX_THINKING_TOKENS",
		"CLAUDE_CODE_MAX_OUTPUT_TOKENS",
		"AIDER_MODEL",
		"AIDER_WEAK_MODEL",
		"AIDER_EDITOR_MODEL",
		"AMP_API_KEY",
		"CODEX_HOME",
		"XDG_DATA_HOME",
		"PATH", "HOME", "USER", "LOGNAME", "SHELL", "TERM", "LANG", "LC_ALL", "TMPDIR",
		"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
		"http_proxy", "https_proxy", "no_proxy",
		"SSL_CERT_FILE", "SSL_CERT_DIR",
		"GOPROXY", "GONOPROXY", "GOPRIVATE", "GOINSECURE", "GOSUMDB", "GOFLAGS", "GOTOOLCHAIN",
	)
}

// Load reads the YAML configuration at path and applies defaults for every
// unset field.
func Load(path string) (Config, error) {
	c, err := parse(path)
	if err != nil {
		return c, err
	}
	c.applyDefaults()
	if err := c.validate(path); err != nil {
		return c, err
	}
	// Slim serializes provider traffic: the agent limiter gates agent and
	// reviewer rounds alike (both hit the provider), so one slot means no
	// two LLM requests are ever in flight. Normalization is the single
	// source: the env export, the semaphore it sizes, and report's slot
	// display all just see 1. MaxConcurrentTests is deliberately untouched
	// — native suites dial no LLM. Kept after validation so an invalid
	// negative max_concurrent_agent_runs still errors. LoadRaw deliberately
	// skips this — the raw view must stay raw.
	if c.Slim.Enabled {
		c.MaxConcurrentAgentRuns = 1
	}
	return c, nil
}

// LoadRaw is Load's pre-defaults view: the same read, parse, and validation
// (identical diagnostics), but the returned Config holds the file's own
// values — fields absent from it stay at their zero value instead of the
// applied default. `daedalus config` renders this view, so the operator can
// tell what the file says from what the code injects.
func LoadRaw(path string) (Config, error) {
	c, err := parse(path)
	if err != nil {
		return c, err
	}
	// Validation sees the defaulted values (an unset fallback.type loads as
	// "anthropic", an unset agent as "claude"), so validate a copy — the
	// returned view must stay raw. The provider section is copied deep:
	// it is the one pointer field applyDefaults writes through, and the
	// struct copy alone would alias it, letting the defaults pass stamp
	// its timeout into the returned raw view.
	d := c
	if c.Provider != nil {
		p := *c.Provider
		d.Provider = &p
	}
	d.applyDefaults()
	return c, d.validate(path)
}

// parse reads and unmarshals the YAML configuration at path. Decoding is
// strict: a key the Config struct does not know fails the load instead of
// being silently dropped — a misplaced key (e.g. thinking: nested under
// openai:) must surface at load time, not as all-defaults behavior partway
// into a run.
func parse(path string) (Config, error) {
	var c Config
	data, err := os.ReadFile(path)
	if err != nil {
		return c, fmt.Errorf("read config %s: %w", path, err)
	}
	// A repeated key in any section is tolerated — the last value wins,
	// loudly — instead of failing the load the way yaml.v3's own
	// duplicate-key check would (a check the slim section's custom
	// unmarshaler bypassed anyway, so strictness there was never parity).
	// The pre-scan prunes the losers from the node tree; only when it
	// pruned something is the tree re-encoded for the strict decode — an
	// ordinary config still decodes from the file's own bytes, error line
	// numbers included. ponytail: a dup-carrying config decodes
	// re-encoded, so a second error in it reports re-encoded line
	// numbers; the alert lines keep the file's own.
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return c, fmt.Errorf("parse config %s: %w", path, err)
	}
	if dedupeKeys(&doc, path, "top level") {
		if data, err = yaml.Marshal(&doc); err != nil {
			return c, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	// An empty document (touch config.yaml) is no config at all: it loads
	// as all-defaults, as it always has — Decoder signals it as io.EOF,
	// not as a decode error.
	if err := dec.Decode(&c); err != nil && !errors.Is(err, io.EOF) {
		return c, fmt.Errorf("parse config %s: %w", path, err)
	}
	return c, nil
}

// dedupeKeys walks n's mapping nodes and deletes every repeated key's
// earlier occurrences, keeping the last value and printing one alert line
// per dropped occurrence to stderr (file, section, key, winning line).
// section names the mapping the walk sits in — "top level" at the root,
// else the key that introduced it. It reports whether anything was pruned.
// Alias nodes are skipped: their target was walked where it was defined.
func dedupeKeys(n *yaml.Node, path, section string) bool {
	pruned := false
	switch n.Kind {
	case yaml.DocumentNode:
		for _, c := range n.Content {
			pruned = dedupeKeys(c, path, section) || pruned
		}
	case yaml.MappingNode:
		// Keys sit at even Content indices with their values at the odd
		// ones that follow; the last occurrence of a key is the winner.
		last := make(map[string]int, len(n.Content)/2)
		for i := 0; i < len(n.Content); i += 2 {
			last[n.Content[i].Value] = i
		}
		keep := make([]*yaml.Node, 0, len(n.Content))
		for i := 0; i < len(n.Content); i += 2 {
			key, val := n.Content[i], n.Content[i+1]
			if j := last[key.Value]; j != i {
				fmt.Fprintf(os.Stderr, "config alert: duplicate key %q in section %q (%s:%d) — the last value wins\n",
					key.Value, section, path, n.Content[j].Line)
				pruned = true
				continue
			}
			keep = append(keep, key, val)
		}
		n.Content = keep
		for i := 0; i+1 < len(keep); i += 2 {
			pruned = dedupeKeys(keep[i+1], path, keep[i].Value) || pruned
		}
	case yaml.SequenceNode:
		for _, c := range n.Content {
			pruned = dedupeKeys(c, path, section) || pruned
		}
	}
	return pruned
}

// validate rejects the configs Load refuses, with Load's diagnostics. Call
// it on a defaults-applied Config (see Load).
func (c Config) validate(path string) error {
	if err := ValidateAgent(c.Agent); err != nil {
		return fmt.Errorf("config %s: agent: %w", path, err)
	}
	if err := ValidateBranchPrefix(c.BranchPrefix); err != nil {
		return fmt.Errorf("config %s: %w", path, err)
	}
	if err := ValidateWorkerID(c.WorkerID); err != nil {
		return fmt.Errorf("config %s: %w", path, err)
	}
	if c.Temporal.TaskQueue == ReservedTestTaskQueue {
		return fmt.Errorf("config %s: temporal.task_queue: %q is reserved for suite queues", path, c.Temporal.TaskQueue)
	}
	// A "-test" suffixed main queue would derive a suite queue colliding
	// with the stem name's plain deployment (foo-test's suites on foo's
	// queue, pollable by the wrong workers) — rejected up front instead.
	if stem, suffix := strings.CutSuffix(c.Temporal.TaskQueue, "-test"); suffix {
		return fmt.Errorf("config %s: temporal.task_queue: %q must not carry the \"-test\" suffix — suites for %q derive the queue %q, colliding with this deployment's own main queue",
			path, c.Temporal.TaskQueue, stem, TestQueueFor(stem))
	}
	if c.TestsTimeout < 0 {
		return fmt.Errorf("config %s: tests_timeout: must not be negative", path)
	}
	if c.AgentRunTimeout < 0 {
		return fmt.Errorf("config %s: agent_run_timeout: must not be negative", path)
	}
	if c.ReviewTimeout < 0 {
		return fmt.Errorf("config %s: review_timeout: must not be negative", path)
	}
	if c.CleanupTimeout < 0 {
		return fmt.Errorf("config %s: cleanup_timeout: must not be negative", path)
	}
	if c.MaxConcurrentAgentRuns < 0 {
		return fmt.Errorf("config %s: max_concurrent_agent_runs: must not be negative", path)
	}
	if c.MaxRequestsPerMinute < 0 {
		return fmt.Errorf("config %s: max_requests_per_minute: must not be negative", path)
	}
	// A set cap must be enforceable, never silently inert: the pacing lives
	// in the worker's loopback relay (internal/toolrelay), which forwards
	// the openai channel and is staged only into pi rounds. Any other
	// shape — no openai url to resolve the relay's upstream against, or an
	// agent whose rounds never read the relay's staged URL — would load a
	// knob that throttles nothing, so it fails at load naming the key.
	if c.MaxRequestsPerMinute > 0 {
		_, open := c.ProviderChannels()
		if open.URL == "" {
			return fmt.Errorf("config %s: max_requests_per_minute: no openai url to relay — the cap is enforced by the worker's loopback provider relay, which forwards the openai channel; set openai.url or remove the key", path)
		}
		if c.Agent != "pi" {
			return fmt.Errorf("config %s: max_requests_per_minute: only pi rounds are staged through the worker's provider relay — set agent: pi or remove the key", path)
		}
	}
	if c.MaxConcurrentTests < 0 {
		return fmt.Errorf("config %s: max_concurrent_tests: must not be negative", path)
	}
	if c.Slim.MaxInputTokens < 0 {
		return fmt.Errorf("config %s: slim.max_input_tokens: must not be negative", path)
	}
	if c.Metrics.Port < 0 || c.Metrics.RelayPort < 0 {
		return fmt.Errorf("config %s: metrics: port and relay_port must not be negative", path)
	}
	// validate runs after applyDefaults, so one side of this equality may
	// be the operator's explicit port and the other the default that
	// filled an unset field — naming only relay_port: 7777 collides with
	// the defaulted UI port without the config ever naming the UI port.
	// The message stays value-first for both shapes: whichever side is
	// wrong, the fix is a distinct value.
	if c.Metrics.Port > 0 && c.Metrics.Port == c.Metrics.RelayPort {
		return fmt.Errorf("config %s: metrics: port and relay_port collide on %d — the daemon serves two listeners (the dashboard and the loopback relay); an unset port takes its default, so give them distinct values", path, c.Metrics.Port)
	}
	// A blank review_skill_list entry would resolve to a skill-review round
	// with no instructions at all — rejected at load. Named skills are not
	// checked for existence here: resolution happens per round (worktree
	// skills may be created after submit) and fails the round loudly.
	for i, entry := range c.ReviewSkillList {
		if strings.TrimSpace(entry) == "" {
			return fmt.Errorf("config %s: review_skill_list: entry %d is empty — every entry is a /name or hand-written prompt text", path, i+1)
		}
	}
	if c.Anthropic.ContextTokens < 0 {
		return fmt.Errorf("config %s: anthropic.context_tokens: must not be negative", path)
	}
	// The per-role override groups stage the same window var the serving
	// section does, so a negative is the same export bug there: rejected
	// at load, every other subset shape legal (any-subset is the groups'
	// contract — validation adds no cross-field rules).
	for _, g := range []struct {
		name string
		toks int
	}{
		{"dev.context_tokens", c.Dev.ContextTokens},
		{"reviewer.context_tokens", c.Reviewer.ContextTokens},
		{"final_reviewer.context_tokens", c.FinalReviewer.ContextTokens},
	} {
		if g.toks < 0 {
			return fmt.Errorf("config %s: %s: must not be negative", path, g.name)
		}
	}
	if c.Anthropic.MaxOutputTokens < 0 {
		return fmt.Errorf("config %s: anthropic.max_output_tokens: must not be negative", path)
	}
	// The ceiling joins every other numeric knob in the negative check:
	// applyDefaults only fills zero, so a negative survives defaulting and
	// would export verbatim as API_TIMEOUT_MS=-5 — a negative ceiling on
	// the jailed agent's client rather than a load error.
	if c.Anthropic.TimeoutMS < 0 {
		return fmt.Errorf("config %s: anthropic.timeout_ms: must not be negative", path)
	}
	if c.OpenAI.TimeoutMS < 0 {
		return fmt.Errorf("config %s: openai.timeout_ms: must not be negative", path)
	}
	// presence_penalty is exempt from the negative check: its API range is
	// [-2, 2], so a negative value is a legal request, not a typo.
	// NaN/Inf are rejected for all five: they pass every comparison
	// against 0 and would render as non-float literals ("NaN.0") in the
	// staged aider settings, silently dropping the whole file.
	for _, p := range []struct {
		name  string
		v     float64
		negOK bool
	}{
		{"top_p", c.OpenAI.TopP, false},
		{"temperature", c.OpenAI.Temperature, false},
		{"presence_penalty", c.OpenAI.PresencePenalty, true},
		{"top_k", c.OpenAI.TopK, false},
		{"min_p", c.OpenAI.MinP, false},
		{"repetition_penalty", c.OpenAI.RepetitionPenalty, false},
	} {
		if math.IsNaN(p.v) || math.IsInf(p.v, 0) {
			return fmt.Errorf("config %s: openai.%s: must be a finite number", path, p.name)
		}
		if !p.negOK && p.v < 0 {
			return fmt.Errorf("config %s: openai.%s: must not be negative", path, p.name)
		}
	}
	// The provider section: its own field rules, then the mixing rules —
	// one channel, one source of truth. The provider section and the
	// same-wire legacy section are a load error (an override would silently
	// resurrect the mis-sectioned-keys confusion the section removes),
	// while the other wire's legacy section stays legal — the incremental
	// migration path. The sizing pair and the request ceiling are one
	// channel regardless of wire, so a cross-wire legacy section may not
	// size or time what the provider section also governs: AgentEnv's
	// legacy ordering (the openai channel's timeout wins when its URL is
	// set) predates type dispatch and would silently pick between two
	// values the file's own sections disagree on — precedence flipping
	// with which side the provider section sits.
	if p := c.Provider; p != nil {
		if err := p.validate(); err != nil {
			return fmt.Errorf("config %s: provider: %w", path, err)
		}
		if p.Type == FallbackTypeAnthropic {
			if c.Anthropic.configured() {
				return fmt.Errorf("config %s: provider (type anthropic) and the legacy anthropic: section are both set — both configure the anthropic channel, and the provider section is that section's replacement; keep one of the two (mixing across wires is legal)", path)
			}
			// The provider section always carries a post-default ceiling
			// (applyDefaults fills it set or not), so the cross-wire
			// openai side alone decides this clash.
			if c.OpenAI.TimeoutMS != 0 {
				return fmt.Errorf("config %s: provider.timeout_ms and openai.timeout_ms are both set — the request ceiling is one channel whichever wire serves; keep it in one place", path)
			}
		} else {
			if c.OpenAI.configured() {
				return fmt.Errorf("config %s: provider (type openai) and the legacy openai: section are both set — both configure the openai channel, and the provider section is that section's replacement; keep one of the two (mixing across wires is legal)", path)
			}
			if p.ContextTokens != 0 && c.Anthropic.ContextTokens != 0 {
				return fmt.Errorf("config %s: provider.context_tokens and anthropic.context_tokens are both set — sizing is one channel whichever wire serves; keep it in one place", path)
			}
			if p.MaxOutputTokens != 0 && c.Anthropic.MaxOutputTokens != 0 {
				return fmt.Errorf("config %s: provider.max_output_tokens and anthropic.max_output_tokens are both set — sizing is one channel whichever wire serves; keep it in one place", path)
			}
			// The same one-channel rule for the ceiling: the provider
			// side always carries a post-default value, so the legacy
			// side alone decides. The default-filled absent section reads
			// as unset, and an explicit value equal to the default is
			// accepted as the same — the provider channel would export
			// exactly that number either way.
			if t := c.Anthropic.TimeoutMS; t != 0 && t != DefaultAnthropicTimeoutMS {
				return fmt.Errorf("config %s: provider.timeout_ms and anthropic.timeout_ms are both set — the request ceiling is one channel whichever wire serves; keep it in one place", path)
			}
		}
	}
	if f := c.Fallback; f.Active() {
		if f.URL == "" || f.Key == "" || f.Model == "" {
			return fmt.Errorf("config %s: fallback: enabled fallback needs url, key, and model", path)
		}
	}
	// One model tier per deployment: slim and mid are disjoint serving
	// postures (a clamped single-round self-hosted lane vs. remote
	// mid-tier models on the deployment's own channels), not layers — a
	// both-set config has no defined meaning, so it fails at load naming
	// both keys instead of silently picking one.
	if c.Slim.Enabled && c.Mid {
		return fmt.Errorf("config %s: slim.enabled and mid are mutually exclusive — one model tier per deployment; keep one of the two", path)
	}
	// The verdict-repair decision model is required exactly when slim or mid
	// is on: slim's small models are the marker-less-exit generator, and mid
	// is defined by its verdicts routing through the decision contract — the
	// repair layer is both modes' recovery mechanism. Outside them a partial
	// or absent section is merely inert — every repair failure fails open to
	// the historical marker-less behavior, so there is nothing to
	// misconfigure into a broken run.
	if (c.Slim.Enabled || c.Mid) && (c.VerdictRepair.URL == "" || c.VerdictRepair.Model == "") {
		return fmt.Errorf("config %s: verdict_repair: slim and mid modes require url and model — the decision model that classifies marker-less review verdicts", path)
	}
	if !slices.Contains(fallbackTypes, c.Fallback.Type) {
		return fmt.Errorf("config %s: fallback: unknown type %q (available: %s)",
			path, c.Fallback.Type, strings.Join(fallbackTypes, ", "))
	}
	// Path safety matters only when filing is on: an off toggle never
	// renders a dir anywhere.
	if dir := c.BugFilingDir(); dir != "" {
		if err := ValidateBugDir(dir); err != nil {
			return fmt.Errorf("config %s: bug_filing: %w", path, err)
		}
		// With a mirror configured, dir is composed into the jail's
		// --rw-map <mirror>:<dir> mount spec, so a colon in dir would make
		// the SOURCE:DEST split ambiguous — reject it at load alongside
		// the path-safety gate. The mirror side of the spec is capped the
		// same way in ResolveMirror.
		if c.BugFiling.Mirror != "" && strings.ContainsRune(dir, ':') {
			return fmt.Errorf("config %s: bug_filing: dir %q must not contain %q while a mirror is configured — it is composed into the jail's --rw-map <mirror>:<dir> mount spec", path, dir, ":")
		}
	}
	if dir := c.TestOutputDir(); dir != "" {
		if err := ValidateTestOutputDir(dir); err != nil {
			return fmt.Errorf("config %s: test_output: %w", path, err)
		}
	}
	// The mirrors are host paths, validated regardless of their section's
	// toggle so a typo fails load even while mirroring is inert.
	if _, err := ResolveMirror("bug_filing mirror", c.BugFiling.Mirror); err != nil {
		return fmt.Errorf("config %s: bug_filing: %w", path, err)
	}
	if _, err := ResolveMirror("test_output mirror", c.TestOutput.Mirror); err != nil {
		return fmt.Errorf("config %s: test_output: %w", path, err)
	}
	return nil
}

// ValidateBugDir rejects bug_filing.dir values that could not name a
// worktree-relative folder (see validateWorktreeRelDir).
func ValidateBugDir(dir string) error {
	return validateWorktreeRelDir("bug dir", dir)
}

// ValidateTestOutputDir rejects test_output.dir values that could not name
// a worktree-relative folder (see validateWorktreeRelDir).
func ValidateTestOutputDir(dir string) error {
	return validateWorktreeRelDir("test_output dir", dir)
}

// ResolveMirror validates and resolves a bug_filing.mirror /
// test_output.mirror value — the host-path complement of dir, and one
// shared code path for both sections. Empty stays empty (mirroring off,
// today's behavior). ~ and ~/… expand against the user's home directory;
// everything else must already be absolute: a relative mirror would
// resolve against the worktree (the copy would land back inside the tree
// and die with its cleanup) or the daemon's cwd (nondeterministic), so
// the error points at dir for worktree-relative intent. The resolved path
// is capped two ways because the bug mirror is mounted read-write into
// the round's jail (activities' --rw-map): the filesystem root is
// rejected (a root mirror would expose the whole host, HOME=/ included),
// and so is any colon (it would make the --rw-map SOURCE:DEST split
// ambiguous).
func ResolveMirror(what, mirror string) (string, error) {
	if mirror == "" {
		return "", nil
	}
	resolved := mirror
	if mirror == "~" || strings.HasPrefix(mirror, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("%s %q: resolve home directory: %w", what, mirror, err)
		}
		resolved = filepath.Join(home, strings.TrimPrefix(mirror, "~"))
	} else if !filepath.IsAbs(mirror) {
		return "", fmt.Errorf("%s %q must be an absolute host path (or ~/…); mirror is host-side — use dir for a worktree-relative folder", what, mirror)
	}
	clean := filepath.Clean(resolved)
	if clean == "/" {
		return "", fmt.Errorf("%s %q must name a host directory, not the filesystem root — the round would mount a read-write window on the whole host", what, mirror)
	}
	if strings.ContainsRune(clean, ':') {
		return "", fmt.Errorf("%s %q must not contain %q — the mirror is composed into the jail's --rw-map <mirror>:<dir> mount spec", what, mirror, ":")
	}
	return clean, nil
}

// validateWorktreeRelDir rejects dir values that could not name a
// worktree-relative folder: absolute paths, ~-prefixed values, ".."
// escaping the worktree root, and empty or dot components (asserted on
// the cleaned path, so
// "a/./b" and "a//b" normalize before the check rather than being rejected
// outright). The same path-safety class as the task-log/worktree segment
// validation in internal/activities, widened to a multi-segment dir.
func validateWorktreeRelDir(what, dir string) error {
	// A ~-prefixed dir would pass the checks below (filepath.IsAbs is
	// false for it) and silently create a literal "~" directory inside
	// the worktree — the failure mode the section's mirror exists for,
	// so name it in the error.
	if strings.HasPrefix(dir, "~") {
		return fmt.Errorf("%s %q must be worktree-relative; ~ is not expanded — use the section's mirror for host paths", what, dir)
	}
	if filepath.IsAbs(dir) {
		return fmt.Errorf("%s %q must be worktree-relative, not absolute", what, dir)
	}
	clean := filepath.Clean(dir)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%s %q must not escape the worktree root", what, dir)
	}
	for seg := range strings.SplitSeq(clean, string(filepath.Separator)) {
		if seg == "" || seg == "." {
			return fmt.Errorf("%s %q is not a valid worktree-relative folder path", what, dir)
		}
	}
	return nil
}

// reservedBranchPrefixes are daedalus's internal branch namespaces — the
// in-flight feat/ prefix and the aborted/ continue-snapshot prefix in
// internal/activities. A preserved-branch prefix colliding with either
// would make cleanup and abort/continue logic treat approved deliverables
// as their own bookkeeping, so validation rejects them up front.
var reservedBranchPrefixes = []string{"feat", "aborted"}

// ValidateBranchPrefix rejects values that cannot head a git branch name,
// following git check-ref-format's rules for the prospective name
// <prefix>/issue-<id>-<timestamp>: no component may begin with "." or end
// with ".lock"; no control characters, space, or ~ ^ : ? * [ \ anywhere;
// no "..", "//", or "@{"; the name cannot begin with "-". A bad prefix
// must fail here, at the start of a run, rather
// than at the finalize step after the whole pipeline has already run.
func ValidateBranchPrefix(p string) error {
	if p == "" || strings.HasPrefix(p, "-") || strings.HasPrefix(p, "/") || strings.HasSuffix(p, "/") ||
		strings.ContainsAny(p, " ~^:?*[\\") ||
		strings.Contains(p, "..") || strings.Contains(p, "//") || strings.Contains(p, "@{") {
		return fmt.Errorf("branch prefix %q is not a valid git branch name prefix", p)
	}
	for _, r := range p {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("branch prefix %q is not a valid git branch name prefix", p)
		}
	}
	for seg := range strings.SplitSeq(p, "/") {
		if strings.HasPrefix(seg, ".") || strings.HasSuffix(seg, ".lock") {
			return fmt.Errorf("branch prefix %q is not a valid git branch name prefix", p)
		}
	}
	if slices.Contains(reservedBranchPrefixes, strings.Split(p, "/")[0]) {
		return fmt.Errorf("branch prefix %q collides with a reserved daedalus namespace (reserved: %s)",
			p, strings.Join(reservedBranchPrefixes, ", "))
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.Agent == "" {
		c.Agent = DefaultAgent
	}
	if c.BranchPrefix == "" {
		c.BranchPrefix = DefaultBranchPrefix
	}
	if c.Temporal.Host == "" {
		c.Temporal.Host = DefaultTemporalHost
	}
	if c.Temporal.UIPort == 0 {
		c.Temporal.UIPort = DefaultTemporalUIPort
	}
	if c.Temporal.TaskQueue == "" {
		c.Temporal.TaskQueue = DefaultTaskQueue
	}
	if c.Anthropic.TimeoutMS == 0 {
		c.Anthropic.TimeoutMS = DefaultAnthropicTimeoutMS
	}
	if c.TestsTimeout == 0 {
		c.TestsTimeout = DefaultTestsTimeout
	}
	if c.AgentRunTimeout == 0 {
		c.AgentRunTimeout = DefaultAgentRunTimeout
	}
	if c.ReviewTimeout == 0 {
		c.ReviewTimeout = DefaultReviewTimeout
	}
	if c.CleanupTimeout == 0 {
		c.CleanupTimeout = DefaultCleanupTimeout
	}
	if c.MaxConcurrentAgentRuns == 0 {
		c.MaxConcurrentAgentRuns = DefaultMaxConcurrentAgentRuns
	}
	if c.MaxConcurrentTests == 0 {
		c.MaxConcurrentTests = DefaultMaxConcurrentTests
	}
	if c.Metrics.Port == 0 {
		c.Metrics.Port = DefaultMetricsUIPort
	}
	if c.Metrics.RelayPort == 0 {
		c.Metrics.RelayPort = DefaultMetricsRelayPort
	}
	if c.Fallback.Type == "" {
		c.Fallback.Type = DefaultFallbackType
	}
	// The provider section's ceiling follows the legacy sections' rule: an
	// explicit timeout is never required, but a ceiling is always exported,
	// so unset defaults here rather than at the export.
	if c.Provider != nil && c.Provider.TimeoutMS == 0 {
		c.Provider.TimeoutMS = DefaultAnthropicTimeoutMS
	}
	if c.Anthropic.TimeoutMS == 0 {
		c.Anthropic.TimeoutMS = DefaultAnthropicTimeoutMS
	}
	if c.TestsTimeout == 0 {
		c.TestsTimeout = DefaultTestsTimeout
	}
}
