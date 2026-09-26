// Package config loads Daedalus runtime configuration from a YAML file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
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
	// ReservedTestTaskQueue is the shared test worker's queue (activities
	// serves it as TestTaskQueue). temporal.task_queue rejects it: a main
	// worker polling it too would receive suite tasks it cannot run, while
	// the test worker holds no workflows.
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
	// TopPEnv, PresencePenaltyEnv, TopKEnv, MinPEnv, and
	// RepetitionPenaltyEnv carry the openai section's sampler knobs.
	TopPEnv              = "DAEDALUS_TOP_P"
	PresencePenaltyEnv   = "DAEDALUS_PRESENCE_PENALTY"
	TopKEnv              = "DAEDALUS_TOP_K"
	MinPEnv              = "DAEDALUS_MIN_P"
	RepetitionPenaltyEnv = "DAEDALUS_REPETITION_PENALTY"
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
	// unchanged — see MaxOutputTokens for that). opencode, pi, and amp have
	// no wired lever and simply ignore it — opencode's only mechanism is a
	// config file pointer (OPENCODE_CONFIG), whose precedence against a
	// repo-committed opencode.json was never probed, so no staged-config
	// route is wired for it. Zero (unset) leaves every agent on its own
	// default.
	ContextTokens int `yaml:"context_tokens"`
	// MaxOutputTokens, when set, caps the jailed agent's completion size in
	// tokens. It exports as MaxOutputTokensEnv and is translated to each
	// agent's own lever by the jailed-round builder: claude's round env
	// gains CLAUDE_CODE_MAX_OUTPUT_TOKENS (the var claude 2.1.283's own
	// error text names for its request-level max_tokens override) and the
	// aider staging replaces both sides of the 8192 constant (the staged
	// metadata's max_output_tokens and extra_params.max_tokens). opencode,
	// pi, and amp have no wired route. Zero (unset) keeps the aider
	// constant and every other agent's API default.
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
	// provider their selected model uses. Empty loads as the default.
	Type  string `yaml:"type"`
	URL   string `yaml:"url"`
	Key   string `yaml:"key"`
	Model string `yaml:"model"`
	// HeartbeatModel mirrors AnthropicConfig.HeartbeatModel for fallback
	// rounds; empty falls back to Model, then to the agent's default.
	HeartbeatModel string `yaml:"heartbeat_model"`
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
	// today only by the aider staging: top_p, presence_penalty, and
	// repetition_penalty ride the staged settings' extra_params top level
	// (litellm maps them natively), while top_k and min_p ride
	// extra_body — litellm's syntax for params it does not map. pi's
	// passthrough (if any) is unprobed, so pi ignores them until probed;
	// claude and amp are out of scope by decision; opencode's
	// provider.<id>.options may carry them but was never probed. Zero
	// (unset) means the field is omitted from the request entirely —
	// absent, never sent as null/0, because a sent default would override
	// the backend's own sampler defaults (the exact bug class that
	// motivated these knobs). An explicit 0 in the file is therefore
	// indistinguishable from unset.
	TopP            float64 `yaml:"top_p"`
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
}

// Config holds the runtime configuration for a Daedalus process, loaded
// from a YAML file (see config-example.yaml).
type Config struct {
	// Agent selects which jailed CLI runs the implementing and reviewer
	// agents: "claude" (Claude Code), "opencode", "amp", "pi", or "aider"
	// (see agents for the accepted values).
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
	// Slim targets small self-hosted models (small context window, low max
	// output tokens) on the aider and pi agents: when true, the worker runs
	// a single jailed-agent round at a time (MaxConcurrentAgentRuns is
	// clamped to 1, so no two provider requests are ever in flight) and
	// exports DAEDALUS_SLIM=1, under which jailed aider rounds pin
	// AIDER_WEAK_MODEL/AIDER_EDITOR_MODEL to the effective AIDER_MODEL.
	// Applies to the deployment, not to an agent: valid regardless of
	// `agent`, so a `run --cli aider` override needs no config edit. The
	// per-value limits (model ids, context/output budgets) stay operator-
	// side — see config-example.yaml's slim entry for the per-agent
	// prerequisites.
	Slim bool `yaml:"slim"`
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
	MaxConcurrentTests int             `yaml:"max_concurrent_tests"`
	Temporal           TemporalConfig  `yaml:"temporal"`
	Anthropic          AnthropicConfig `yaml:"anthropic"`
	OpenAI             OpenAIConfig    `yaml:"openai"`
	// Fallback is the independent secondary provider failover uses when
	// the primary is API-exhausted. Inactive unless Enabled.
	Fallback FallbackConfig `yaml:"fallback"`
}

// durationValue marshals a time.Duration the way config files express them:
// a canonical duration string ("30m0s"), never the raw nanosecond count a
// plain re-marshal would emit. Zero — the unset marker a pre-defaults view
// renders — prints as plain 0.
type durationValue time.Duration

func (d durationValue) MarshalYAML() (any, error) {
	if time.Duration(d) == 0 {
		return 0, nil
	}
	return time.Duration(d).String(), nil
}

// renderConfig mirrors Config field-for-field so a re-marshal prints every
// field in struct order with the durations carried by durationValue; the
// nested sections marshal as themselves.
type renderConfig struct {
	Agent                  string          `yaml:"agent"`
	BranchPrefix           string          `yaml:"branch_prefix"`
	Authorship             bool            `yaml:"authorship"`
	WorkerID               string          `yaml:"worker_id"`
	TestsTimeout           durationValue   `yaml:"tests_timeout"`
	AgentRunTimeout        durationValue   `yaml:"agent_run_timeout"`
	ReviewTimeout          durationValue   `yaml:"review_timeout"`
	CleanupTimeout         durationValue   `yaml:"cleanup_timeout"`
	MaxConcurrentAgentRuns int             `yaml:"max_concurrent_agent_runs"`
	MaxConcurrentTests     int             `yaml:"max_concurrent_tests"`
	Temporal               TemporalConfig  `yaml:"temporal"`
	Anthropic              AnthropicConfig `yaml:"anthropic"`
	OpenAI                 OpenAIConfig    `yaml:"openai"`
	Fallback               FallbackConfig  `yaml:"fallback"`
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
		MaxConcurrentTests:     c.MaxConcurrentTests,
		Temporal:               c.Temporal,
		Anthropic:              c.Anthropic,
		OpenAI:                 c.OpenAI,
		Fallback:               c.Fallback,
	})
	return string(out), err
}

// agents lists the accepted config Agent values. amp authenticates through
// AMP_API_KEY in the worker's environment (runJailed passes it into the jail
// when set; amp's host login does not reach the jail) — the anthropic/openai
// config sections do not apply to it. pi and aider authenticate through the
// provider env vars the worker already exports (ANTHROPIC_API_KEY,
// OPENAI_API_KEY, ...) with two caveats: pi also reads ~/.pi/agent/auth.json,
// which the jail's pi preset bridges in read-write (probe-verified — see
// jailedAgentCLI) and which takes priority over the env vars for the same
// provider, so a stale host /login wins; and aider's dotenv load uses
// override=True, but the jail masks the repo's .env to empty, so inside
// jailed rounds the config-derived exports stand. Which wire a round dials
// is chosen by the agent's selected model, not by daedalus — see
// FallbackTypeOpenAI.
var agents = []string{"claude", "opencode", "amp", "pi", "aider"}

// ValidateAgent rejects Agent values Load would refuse. The CLI's
// -cli/--cli flag overrides the config's agent and must fail up front,
// with the same error, rather than at worker startup.
func ValidateAgent(a string) error {
	if !slices.Contains(agents, a) {
		return fmt.Errorf("unknown agent %q (available: %s)", a, strings.Join(agents, ", "))
	}
	return nil
}

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
		return fmt.Errorf("worker id %q must be a plain file-name-safe token (letters, digits, dashes, dots inside)", id)
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

// AgentEnv translates provider configuration into environment variables for
// the jailed agent process. Only explicitly-set values are exported — an
// unset field means "inherit whatever the worker's environment already
// provides", and empty URL/model fields are deliberately not defaulted (the
// defaults live in config-example.yaml as documentation, not in code). The
// worker exports these into its own environment so activities can pass them
// through — the values never travel through workflow history or activity
// inputs.
func (c Config) AgentEnv() []string {
	var env []string
	add := func(key, value string) {
		if value != "" {
			env = append(env, key+"="+value)
		}
	}
	add("ANTHROPIC_BASE_URL", c.Anthropic.URL)
	add("ANTHROPIC_API_KEY", c.Anthropic.Key)
	add("ANTHROPIC_MODEL", c.Anthropic.Model)
	add("ANTHROPIC_DEFAULT_HAIKU_MODEL", c.Anthropic.HeartbeatModel)
	// The context window rides the same env-only channel; unset means the
	// agents' own defaults, so nothing is exported.
	if c.Anthropic.ContextTokens > 0 {
		add(ContextTokensEnv, strconv.Itoa(c.Anthropic.ContextTokens))
	}
	// The completion cap rides the same channel; the jailed-round builder
	// re-exports it under claude's own var and the aider staging consumes
	// it (see MaxOutputTokens).
	if c.Anthropic.MaxOutputTokens > 0 {
		add(MaxOutputTokensEnv, strconv.Itoa(c.Anthropic.MaxOutputTokens))
	}
	// Sampler knobs export only when set — absent means the field is
	// omitted from the staged request entirely (see OpenAIConfig).
	addFloat := func(key string, v float64) {
		if v != 0 {
			env = append(env, key+"="+strconv.FormatFloat(v, 'g', -1, 64))
		}
	}
	addFloat(TopPEnv, c.OpenAI.TopP)
	addFloat(PresencePenaltyEnv, c.OpenAI.PresencePenalty)
	addFloat(TopKEnv, c.OpenAI.TopK)
	addFloat(MinPEnv, c.OpenAI.MinP)
	addFloat(RepetitionPenaltyEnv, c.OpenAI.RepetitionPenalty)
	add("OPENAI_BASE_URL", c.OpenAI.URL)
	add("OPENAI_API_KEY", c.OpenAI.Key)
	add("OPENAI_MODEL", c.OpenAI.Model)
	// Both URL vars carry the same value: aider's litellm layer standardizes
	// on OPENAI_API_BASE while other tooling reads OPENAI_BASE_URL, and
	// litellm versions disagree on which they honor — export both so either
	// resolution lands on the configured endpoint.
	add("OPENAI_API_BASE", c.OpenAI.URL)
	// API_TIMEOUT_MS comes from whichever provider section is serving: an
	// openai section replaces the anthropic one as the jailed round's
	// backend, so its timeout applies instead. An openai section set without
	// a timeout falls back to anthropic's value (Load defaults it), keeping
	// the invariant that the jailed agent gets an explicit ceiling either
	// way — exporting "0" would zero the client's ceiling rather than clear
	// it, and exporting nothing would leave a mixed config's claude rounds
	// (anthropic-served, openai configured for tooling) with no ceiling at
	// all.
	timeoutMS := c.Anthropic.TimeoutMS
	if c.OpenAI.URL != "" && c.OpenAI.TimeoutMS != 0 {
		timeoutMS = c.OpenAI.TimeoutMS
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
	return env
}

// ProviderEnvVars lists every environment variable daedalus derives from
// the config's provider sections — the single source of truth for what a
// spawned worker daemon must NOT inherit from the invoking shell. worker
// start scrubs these from the daemon's inherited environment so the
// recorded config file is the only way provider values reach the daemon:
// a stale export in the operator's terminal can never win over a rotated
// config. Foreground runs keep the inherit semantics (see AgentEnv).
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
	}
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
	if c.Slim {
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
	// returned view must stay raw.
	d := c
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
		return fmt.Errorf("config %s: temporal.task_queue: %q is reserved for the shared test queue", path, c.Temporal.TaskQueue)
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
	if c.MaxConcurrentTests < 0 {
		return fmt.Errorf("config %s: max_concurrent_tests: must not be negative", path)
	}
	if c.Anthropic.ContextTokens < 0 {
		return fmt.Errorf("config %s: anthropic.context_tokens: must not be negative", path)
	}
	if c.Anthropic.MaxOutputTokens < 0 {
		return fmt.Errorf("config %s: anthropic.max_output_tokens: must not be negative", path)
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
	if f := c.Fallback; f.Active() {
		if f.URL == "" || f.Key == "" || f.Model == "" {
			return fmt.Errorf("config %s: fallback: enabled fallback needs url, key, and model", path)
		}
	}
	if !slices.Contains(fallbackTypes, c.Fallback.Type) {
		return fmt.Errorf("config %s: fallback: unknown type %q (available: %s)",
			path, c.Fallback.Type, strings.Join(fallbackTypes, ", "))
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
	if c.Fallback.Type == "" {
		c.Fallback.Type = DefaultFallbackType
	}
	if c.Anthropic.TimeoutMS == 0 {
		c.Anthropic.TimeoutMS = DefaultAnthropicTimeoutMS
	}
	if c.TestsTimeout == 0 {
		c.TestsTimeout = DefaultTestsTimeout
	}
}
