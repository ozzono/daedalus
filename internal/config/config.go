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
	// ReviewerURLEnv and ReviewerKeyEnv carry the reviewer section's
	// endpoint (reviewer.url / reviewer.key) into the jailed-round
	// builder, which pins reviewer rounds' provider endpoint to them while
	// implementing and test rounds keep the serving provider's. Exported
	// only when set — absent means reviewers share the implementing
	// agent's endpoint exactly as before.
	ReviewerURLEnv = "DAEDALUS_REVIEWER_BASE_URL"
	ReviewerKeyEnv = "DAEDALUS_REVIEWER_API_KEY"
	// ToolRelayURLEnv carries the staged base URL of the worker's slim
	// tool-call relay (internal/toolrelay) — the relay's loopback address
	// plus the openai upstream's path prefix, so pi's request paths land
	// on the relay exactly as they would land on the upstream. Exported
	// only while the relay is running; absent means pi dials the upstream
	// directly, the pre-relay behavior. Unset symmetrically and scrubbed
	// from the daemon spawn (ProviderEnvVars) so a stale export can never
	// point a new worker's rounds at a dead relay.
	ToolRelayURLEnv = "DAEDALUS_TOOL_RELAY_URL"
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

// ReviewerConfig optionally points the reviewer rounds (code reviewer in
// the dev loop, test reviewer, docs reviewer) at their own provider
// endpoint, independently of the implementing agent's anthropic/openai
// sections. Setting url and/or key overrides just those values for
// reviewer rounds; absent (the default) means reviewers authenticate
// exactly like every other round. Values travel the env-only channel
// (ReviewerURLEnv / ReviewerKeyEnv — never workflow history, activity
// inputs, or argv); see activities.reviewerEnv for the override rules.
type ReviewerConfig struct {
	URL string `yaml:"url"`
	Key string `yaml:"key"`
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
}

// BugFilingConfig toggles where jailed rounds record out-of-scope bugs.
// Off (the section absent, or enabled: false — the default), no bug files
// are written: bugs surface in the round's reply or review comments only,
// alongside the Arete Memory note every round carries. On, the prompts
// additionally instruct the agent to file every out-of-scope bug as a file
// under dir — worktree-relative, resolved against the run's worktree root,
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
		default:
			return fmt.Errorf("line %d: field %s not found in type config.SlimConfig", key.Line, key.Value)
		}
	}
	return nil
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
	// Fallback is the independent secondary provider failover uses when
	// the primary is API-exhausted. Inactive unless Enabled.
	Fallback FallbackConfig `yaml:"fallback"`
	// Reviewer is the reviewer rounds' own provider endpoint; empty (the
	// section absent) means reviewers share the implementing agent's.
	Reviewer ReviewerConfig `yaml:"reviewer"`
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
	Agent                  string           `yaml:"agent"`
	BranchPrefix           string           `yaml:"branch_prefix"`
	Authorship             bool             `yaml:"authorship"`
	WorkerID               string           `yaml:"worker_id"`
	TestsTimeout           durationValue    `yaml:"tests_timeout"`
	AgentRunTimeout        durationValue    `yaml:"agent_run_timeout"`
	ReviewTimeout          durationValue    `yaml:"review_timeout"`
	CleanupTimeout         durationValue    `yaml:"cleanup_timeout"`
	MaxConcurrentAgentRuns int              `yaml:"max_concurrent_agent_runs"`
	MaxConcurrentTests     int              `yaml:"max_concurrent_tests"`
	SharedTestQueue        *bool            `yaml:"shared_test_queue"`
	Slim                   SlimConfig       `yaml:"slim"`
	Temporal               TemporalConfig   `yaml:"temporal"`
	Anthropic              AnthropicConfig  `yaml:"anthropic"`
	OpenAI                 OpenAIConfig     `yaml:"openai"`
	Fallback               FallbackConfig   `yaml:"fallback"`
	Reviewer               ReviewerConfig   `yaml:"reviewer"`
	BugFiling              BugFilingConfig  `yaml:"bug_filing"`
	TestOutput             TestOutputConfig `yaml:"test_output"`
	Dependency             DependencyConfig `yaml:"dependency"`
	Prompt                 string           `yaml:"prompt"`
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
		SharedTestQueue:        c.SharedTestQueue,
		Slim:                   c.Slim,
		Temporal:               c.Temporal,
		Anthropic:              c.Anthropic,
		OpenAI:                 c.OpenAI,
		Fallback:               c.Fallback,
		Reviewer:               c.Reviewer,
		BugFiling:              c.BugFiling,
		TestOutput:             c.TestOutput,
		Dependency:             c.Dependency,
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
	addFloat(TemperatureEnv, c.OpenAI.Temperature)
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
	add(ReviewerURLEnv, c.Reviewer.URL)
	add(ReviewerKeyEnv, c.Reviewer.Key)
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
		ToolRelayURLEnv,
		BugDirEnv,
		BugMirrorEnv,
		TestOutputMirrorEnv,
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
	if f := c.Fallback; f.Active() {
		if f.URL == "" || f.Key == "" || f.Model == "" {
			return fmt.Errorf("config %s: fallback: enabled fallback needs url, key, and model", path)
		}
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
