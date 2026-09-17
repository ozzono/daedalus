// Package config loads Daedalus runtime configuration from a YAML file.
package config

import (
	"fmt"
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
// ANTHROPIC_API_KEY, ANTHROPIC_MODEL, API_TIMEOUT_MS); the key is required
// for the worker.
type AnthropicConfig struct {
	URL   string `yaml:"url"`
	Key   string `yaml:"key"`
	Model string `yaml:"model"`
	// TimeoutMS bounds the agent's API requests, exported as API_TIMEOUT_MS.
	// Zero (unset) defaults to DefaultAnthropicTimeoutMS — unlike the string
	// fields there is no "inherit the environment" escape hatch: the jailed
	// agent gets an explicit ceiling either way.
	TimeoutMS int `yaml:"timeout_ms"`
}

// OpenAIConfig is injected into the jailed agent's environment as
// OPENAI_BASE_URL / OPENAI_API_KEY / OPENAI_MODEL. Entirely optional —
// available to tooling the agent runs, not consumed by daedalus itself.
type OpenAIConfig struct {
	URL   string `yaml:"url"`
	Key   string `yaml:"key"`
	Model string `yaml:"model"`
}

// Config holds the runtime configuration for a Daedalus process, loaded
// from a YAML file (see config-example.yaml).
type Config struct {
	// Agent selects which jailed CLI runs the implementing and reviewer
	// agents: "claude" (Claude Code), "opencode", or "amp" (see agents for
	// the accepted values).
	Agent string `yaml:"agent"`
	// BranchPrefix names the preserved branch carrying a run's approved
	// work: <prefix>/issue-<id>-<unix timestamp>. It is deliberately
	// separate from temporal.task_queue — the queue routes workflows and
	// scopes worktree paths, while this is repo-facing branch naming.
	// `daedalus run --prefix` overrides it per run.
	BranchPrefix string `yaml:"branch_prefix"`
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
	// AgentRunTimeout bounds one jailed-agent round — implementation
	// or review (RunJailedClaudeActivity). A time.ParseDuration string
	// in YAML ("45m"); DefaultAgentRunTimeout when unset.
	AgentRunTimeout time.Duration `yaml:"agent_run_timeout"`
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
	MaxConcurrentAgentRuns int             `yaml:"max_concurrent_agent_runs"`
	Temporal               TemporalConfig  `yaml:"temporal"`
	Anthropic              AnthropicConfig `yaml:"anthropic"`
	OpenAI                 OpenAIConfig    `yaml:"openai"`
}

// agents lists the accepted config Agent values. amp authenticates through
// AMP_API_KEY in the worker's environment (runJailed passes it into the jail
// when set; amp's host login does not reach the jail) — the anthropic/openai
// config sections do not apply to it.
var agents = []string{"claude", "opencode", "amp"}

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
	add("API_TIMEOUT_MS", strconv.Itoa(c.Anthropic.TimeoutMS))
	add("OPENAI_BASE_URL", c.OpenAI.URL)
	add("OPENAI_API_KEY", c.OpenAI.Key)
	add("OPENAI_MODEL", c.OpenAI.Model)
	return env
}

// Load reads the YAML configuration at path and applies defaults for every
// unset field.
func Load(path string) (Config, error) {
	var c Config
	data, err := os.ReadFile(path)
	if err != nil {
		return c, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &c); err != nil {
		return c, fmt.Errorf("parse config %s: %w", path, err)
	}
	c.applyDefaults()
	if err := ValidateAgent(c.Agent); err != nil {
		return c, fmt.Errorf("config %s: agent: %w", path, err)
	}
	if err := ValidateBranchPrefix(c.BranchPrefix); err != nil {
		return c, fmt.Errorf("config %s: %w", path, err)
	}
	if err := ValidateWorkerID(c.WorkerID); err != nil {
		return c, fmt.Errorf("config %s: %w", path, err)
	}
	if c.TestsTimeout < 0 {
		return c, fmt.Errorf("config %s: tests_timeout: must not be negative", path)
	}
	if c.AgentRunTimeout < 0 {
		return c, fmt.Errorf("config %s: agent_run_timeout: must not be negative", path)
	}
	if c.CleanupTimeout < 0 {
		return c, fmt.Errorf("config %s: cleanup_timeout: must not be negative", path)
	}
	if c.MaxConcurrentAgentRuns < 0 {
		return c, fmt.Errorf("config %s: max_concurrent_agent_runs: must not be negative", path)
	}
	return c, nil
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
	if c.CleanupTimeout == 0 {
		c.CleanupTimeout = DefaultCleanupTimeout
	}
	if c.MaxConcurrentAgentRuns == 0 {
		c.MaxConcurrentAgentRuns = DefaultMaxConcurrentAgentRuns
	}
	if c.Anthropic.TimeoutMS == 0 {
		c.Anthropic.TimeoutMS = DefaultAnthropicTimeoutMS
	}
	if c.TestsTimeout == 0 {
		c.TestsTimeout = DefaultTestsTimeout
	}
}
