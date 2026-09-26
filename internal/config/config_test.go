package config

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Temporal.Host != DefaultTemporalHost {
		t.Errorf("Temporal.Host = %q, want %q", cfg.Temporal.Host, DefaultTemporalHost)
	}
	if cfg.Temporal.UIPort != DefaultTemporalUIPort {
		t.Errorf("Temporal.UIPort = %d, want %d", cfg.Temporal.UIPort, DefaultTemporalUIPort)
	}
	if got, want := cfg.UIURL(), "http://127.0.0.1:8233"; got != want {
		t.Errorf("UIURL() = %q, want %q", got, want)
	}
	if cfg.Anthropic.URL != "" {
		t.Errorf("Anthropic.URL = %q, want empty (unset means inherit the environment)", cfg.Anthropic.URL)
	}
	if cfg.OpenAI.URL != "" {
		t.Errorf("OpenAI.URL = %q, want empty (unset means inherit the environment)", cfg.OpenAI.URL)
	}
	if cfg.Temporal.TaskQueue != DefaultTaskQueue {
		t.Errorf("Temporal.TaskQueue = %q, want %q", cfg.Temporal.TaskQueue, DefaultTaskQueue)
	}
	if cfg.Agent != DefaultAgent {
		t.Errorf("Agent = %q, want %q", cfg.Agent, DefaultAgent)
	}
	if cfg.BranchPrefix != DefaultBranchPrefix {
		t.Errorf("BranchPrefix = %q, want %q", cfg.BranchPrefix, DefaultBranchPrefix)
	}
	if cfg.Anthropic.TimeoutMS != DefaultAnthropicTimeoutMS {
		t.Errorf("Anthropic.TimeoutMS = %d, want %d", cfg.Anthropic.TimeoutMS, DefaultAnthropicTimeoutMS)
	}
	if cfg.TestsTimeout != DefaultTestsTimeout {
		t.Errorf("TestsTimeout = %v, want %v", cfg.TestsTimeout, DefaultTestsTimeout)
	}
	if cfg.AgentRunTimeout != DefaultAgentRunTimeout {
		t.Errorf("AgentRunTimeout = %v, want %v", cfg.AgentRunTimeout, DefaultAgentRunTimeout)
	}
	if cfg.MaxConcurrentAgentRuns != DefaultMaxConcurrentAgentRuns {
		t.Errorf("MaxConcurrentAgentRuns = %d, want %d", cfg.MaxConcurrentAgentRuns, DefaultMaxConcurrentAgentRuns)
	}
	if cfg.MaxConcurrentTests != DefaultMaxConcurrentTests {
		t.Errorf("MaxConcurrentTests = %d, want %d", cfg.MaxConcurrentTests, DefaultMaxConcurrentTests)
	}
}

// TestLoadAgent pins the accepted agent values: every shipped agent loads,
// anything else is rejected with the available choices named.
func TestLoadAgent(t *testing.T) {
	for _, agent := range []string{"claude", "opencode", "amp", "pi", "aider"} {
		cfg, err := Load(writeConfig(t, "agent: "+agent+"\n"))
		if err != nil {
			t.Fatalf("Load(agent: %s): %v", agent, err)
		}
		if cfg.Agent != agent {
			t.Errorf("Agent = %q, want %q", cfg.Agent, agent)
		}
	}

	_, err := Load(writeConfig(t, "agent: cursor\n"))
	if err == nil || !strings.Contains(err.Error(), "unknown agent") {
		t.Fatalf("want unknown-agent error, got %v", err)
	}
}

// TestLoadTestsTimeout pins the tests_timeout parsing: duration strings
// load, and a negative value is rejected up front rather than silently
// becoming "no ceiling" downstream.
func TestLoadTestsTimeout(t *testing.T) {
	cfg, err := Load(writeConfig(t, "tests_timeout: 1h30m\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.TestsTimeout != 90*time.Minute {
		t.Errorf("TestsTimeout = %v, want 1h30m", cfg.TestsTimeout)
	}

	_, err = Load(writeConfig(t, "tests_timeout: -5m\n"))
	if err == nil || !strings.Contains(err.Error(), "tests_timeout") {
		t.Fatalf("want tests_timeout error, got %v", err)
	}
}

// TestLoadAgentRunTimeout pins the agent_run_timeout parsing: duration
// strings load, and a negative value is rejected up front rather than
// silently widening the ceiling downstream.
func TestLoadAgentRunTimeout(t *testing.T) {
	cfg, err := Load(writeConfig(t, "agent_run_timeout: 1h30m\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AgentRunTimeout != 90*time.Minute {
		t.Errorf("AgentRunTimeout = %v, want 1h30m", cfg.AgentRunTimeout)
	}
	_, err = Load(writeConfig(t, "agent_run_timeout: -5m\n"))
	if err == nil || !strings.Contains(err.Error(), "agent_run_timeout") {
		t.Fatalf("want agent_run_timeout error, got %v", err)
	}
}

// TestLoadReviewTimeout pins the review_timeout parsing: duration strings
// load, unset falls back to the historical 15-minute reviewer ceiling, and
// a negative value is rejected up front rather than silently widening the
// ceiling downstream.
func TestLoadReviewTimeout(t *testing.T) {
	cfg, err := Load(writeConfig(t, "review_timeout: 25m\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ReviewTimeout != 25*time.Minute {
		t.Errorf("ReviewTimeout = %v, want 25m", cfg.ReviewTimeout)
	}
	if cfg, err := Load(writeConfig(t, "")); err != nil || cfg.ReviewTimeout != DefaultReviewTimeout {
		t.Errorf("unset ReviewTimeout = %v (err %v), want %v", cfg.ReviewTimeout, err, DefaultReviewTimeout)
	}
	_, err = Load(writeConfig(t, "review_timeout: -5m\n"))
	if err == nil || !strings.Contains(err.Error(), "review_timeout") {
		t.Fatalf("want review_timeout error, got %v", err)
	}
}

// TestLoadCleanupTimeout pins the cleanup_timeout parsing: duration strings
// load, unset falls back to the default, and a negative value is rejected
// up front rather than silently widening the ceiling downstream.
func TestLoadCleanupTimeout(t *testing.T) {
	cfg, err := Load(writeConfig(t, "cleanup_timeout: 30m\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.CleanupTimeout != 30*time.Minute {
		t.Errorf("CleanupTimeout = %v, want 30m", cfg.CleanupTimeout)
	}
	if cfg, err := Load(writeConfig(t, "")); err != nil || cfg.CleanupTimeout != DefaultCleanupTimeout {
		t.Errorf("unset CleanupTimeout = %v (err %v), want %v", cfg.CleanupTimeout, err, DefaultCleanupTimeout)
	}
	_, err = Load(writeConfig(t, "cleanup_timeout: -1m\n"))
	if err == nil || !strings.Contains(err.Error(), "cleanup_timeout") {
		t.Fatalf("want cleanup_timeout error, got %v", err)
	}
}

// TestLoadMaxConcurrentAgentRuns pins the max_concurrent_agent_runs parsing:
// an explicit value loads, and a negative value is rejected up front rather
// than becoming a zero-capacity (permanently stuck) semaphore downstream.
func TestLoadMaxConcurrentAgentRuns(t *testing.T) {
	cfg, err := Load(writeConfig(t, "max_concurrent_agent_runs: 4\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaxConcurrentAgentRuns != 4 {
		t.Errorf("MaxConcurrentAgentRuns = %d, want 4", cfg.MaxConcurrentAgentRuns)
	}

	_, err = Load(writeConfig(t, "max_concurrent_agent_runs: -1\n"))
	if err == nil || !strings.Contains(err.Error(), "max_concurrent_agent_runs") {
		t.Fatalf("want max_concurrent_agent_runs error, got %v", err)
	}
}

// TestLoadSlimClampsAgentConcurrency pins slim mode's concurrency contract:
// slim: true forces max_concurrent_agent_runs to 1 — normalization is the
// single source, so the env export, the semaphore, and report's slot
// display all just see 1 — while max_concurrent_tests is untouched (native
// suites dial no LLM). Validation precedes the clamp, so an invalid
// negative max_concurrent_agent_runs still errors.
func TestLoadSlimClampsAgentConcurrency(t *testing.T) {
	cfg, err := Load(writeConfig(t, "slim: true\nmax_concurrent_agent_runs: 4\nmax_concurrent_tests: 3\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaxConcurrentAgentRuns != 1 {
		t.Errorf("slim MaxConcurrentAgentRuns = %d, want 1", cfg.MaxConcurrentAgentRuns)
	}
	if cfg.MaxConcurrentTests != 3 {
		t.Errorf("slim MaxConcurrentTests = %d, want the explicit 3 (suites dial no LLM)", cfg.MaxConcurrentTests)
	}

	cfg, err = Load(writeConfig(t, "slim: false\nmax_concurrent_agent_runs: 4\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaxConcurrentAgentRuns != 4 {
		t.Errorf("non-slim MaxConcurrentAgentRuns = %d, want 4", cfg.MaxConcurrentAgentRuns)
	}

	_, err = Load(writeConfig(t, "slim: true\nmax_concurrent_agent_runs: -1\n"))
	if err == nil || !strings.Contains(err.Error(), "max_concurrent_agent_runs") {
		t.Fatalf("want max_concurrent_agent_runs error under slim, got %v", err)
	}
}

// TestLoadThinkingDisabled pins the thinking toggle's pointer semantics: an
// absent key and an explicit true both mean pi follows its host default;
// only an explicit false disables thinking on jailed pi rounds.
func TestLoadThinkingDisabled(t *testing.T) {
	for _, c := range []struct {
		name string
		yaml string
		want bool
	}{
		{"absent", "", false},
		{"explicit true", "thinking: true\n", false},
		{"explicit false", "thinking: false\n", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, c.yaml))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got := cfg.ThinkingDisabled(); got != c.want {
				t.Errorf("ThinkingDisabled() = %v (Thinking = %v), want %v", got, cfg.Thinking, c.want)
			}
		})
	}
}

// TestLoadMaxConcurrentTests pins the max_concurrent_tests parsing: an
// explicit value loads, and a negative value is rejected up front rather
// than becoming a zero-capacity (permanently stuck) semaphore downstream.
func TestLoadMaxConcurrentTests(t *testing.T) {
	cfg, err := Load(writeConfig(t, "max_concurrent_tests: 3\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaxConcurrentTests != 3 {
		t.Errorf("MaxConcurrentTests = %d, want 3", cfg.MaxConcurrentTests)
	}

	_, err = Load(writeConfig(t, "max_concurrent_tests: -1\n"))
	if err == nil || !strings.Contains(err.Error(), "max_concurrent_tests") {
		t.Fatalf("want max_concurrent_tests error, got %v", err)
	}
}

// TestLoadRejectsReservedTestQueue pins the namespace split: temporal.task_queue
// rejects the shared test queue's name — a main worker polling it would
// receive suite tasks it cannot run, while the test worker holds no
// workflows.
func TestLoadRejectsReservedTestQueue(t *testing.T) {
	_, err := Load(writeConfig(t, "temporal:\n  task_queue: test\n"))
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("want reserved-queue error, got %v", err)
	}
	// Any other queue name is fine, including ones merely containing "test".
	if _, err := Load(writeConfig(t, "temporal:\n  task_queue: testflight\n")); err != nil {
		t.Errorf("Load(task_queue: testflight): %v, want accepted", err)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
temporal:
  host: temporal.example:7234
  ui_port: 9999
  task_queue: myapp
anthropic:
  url: https://proxy.example
  key: sk-ant-test
  model: claude-opus-5
  heartbeat_model: claude-haiku-test
  timeout_ms: 60000
openai:
  url: https://oa.example/v1
  key: sk-oa-test
  model: gpt-test
fallback:
  enabled: true
  url: https://backup.example
  key: sk-backup-test
  model: glm-backup
  heartbeat_model: glm-backup-air
tests_timeout: 45m
agent_run_timeout: 30m
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Temporal.Host != "temporal.example:7234" {
		t.Errorf("Temporal.Host = %q, want temporal.example:7234", cfg.Temporal.Host)
	}
	if cfg.Temporal.UIPort != 9999 {
		t.Errorf("Temporal.UIPort = %d, want 9999", cfg.Temporal.UIPort)
	}
	if cfg.Temporal.TaskQueue != "myapp" {
		t.Errorf("Temporal.TaskQueue = %q, want myapp", cfg.Temporal.TaskQueue)
	}
	if got, want := cfg.UIURL(), "http://127.0.0.1:9999"; got != want {
		t.Errorf("UIURL() = %q, want %q", got, want)
	}
	if cfg.Anthropic.Model != "claude-opus-5" {
		t.Errorf("Anthropic.Model = %q, want claude-opus-5", cfg.Anthropic.Model)
	}
	if cfg.Anthropic.TimeoutMS != 60000 {
		t.Errorf("Anthropic.TimeoutMS = %d, want 60000", cfg.Anthropic.TimeoutMS)
	}
	if cfg.OpenAI.Model != "gpt-test" {
		t.Errorf("OpenAI.Model = %q, want gpt-test", cfg.OpenAI.Model)
	}
	if cfg.TestsTimeout != 45*time.Minute {
		t.Errorf("TestsTimeout = %v, want 45m", cfg.TestsTimeout)
	}
	if cfg.AgentRunTimeout != 30*time.Minute {
		t.Errorf("AgentRunTimeout = %v, want 30m", cfg.AgentRunTimeout)
	}

	env := cfg.AgentEnv()
	// OPENAI_API_BASE carries the same value as OPENAI_BASE_URL (litellm
	// versions disagree on which var they honor), and API_TIMEOUT_MS comes
	// after the openai block: with openai.url set but no openai
	// timeout_ms, the anthropic ceiling still applies.
	want := []string{
		"ANTHROPIC_BASE_URL=https://proxy.example",
		"ANTHROPIC_API_KEY=sk-ant-test",
		"ANTHROPIC_MODEL=claude-opus-5",
		"ANTHROPIC_DEFAULT_HAIKU_MODEL=claude-haiku-test",
		"OPENAI_BASE_URL=https://oa.example/v1",
		"OPENAI_API_KEY=sk-oa-test",
		"OPENAI_MODEL=gpt-test",
		"OPENAI_API_BASE=https://oa.example/v1",
		"API_TIMEOUT_MS=60000",
		"DAEDALUS_FALLBACK_BASE_URL=https://backup.example",
		"DAEDALUS_FALLBACK_API_KEY=sk-backup-test",
		"DAEDALUS_FALLBACK_MODEL=glm-backup",
		"DAEDALUS_FALLBACK_HEARTBEAT_MODEL=glm-backup-air",
	}
	if !slices.Equal(env, want) {
		t.Errorf("AgentEnv() = %v, want %v", env, want)
	}
	if !cfg.Fallback.Active() {
		t.Error("Fallback.Active() = false, want true for enabled fallback")
	}
}

// TestFallbackInactiveOmitted pins that a disabled or absent fallback
// contributes nothing to AgentEnv — the worker only arms failover when the
// config explicitly enables it.
func TestFallbackInactiveOmitted(t *testing.T) {
	for name, doc := range map[string]string{
		"absent":   "",
		"disabled": "fallback:\n  enabled: false\n  url: https://backup.example\n  key: k\n  model: m\n",
	} {
		cfg, err := Load(writeConfig(t, doc))
		if err != nil {
			t.Fatalf("%s: Load: %v", name, err)
		}
		if cfg.Fallback.Active() {
			t.Errorf("%s: Fallback.Active() = true, want false", name)
		}
		for _, kv := range cfg.AgentEnv() {
			if strings.HasPrefix(kv, "DAEDALUS_FALLBACK_") {
				t.Errorf("%s: AgentEnv() leaked %q from inactive fallback", name, kv)
			}
		}
	}
}

// TestLoadFallbackValidation pins that an enabled fallback missing its
// url/key/model fails at config load, before any worker arms failover on
// half-configured values.
func TestLoadFallbackValidation(t *testing.T) {
	for _, doc := range []string{
		"fallback:\n  enabled: true\n",
		"fallback:\n  enabled: true\n  url: https://backup.example\n",
		"fallback:\n  enabled: true\n  url: https://backup.example\n  key: k\n",
	} {
		_, err := Load(writeConfig(t, doc))
		if err == nil {
			t.Fatalf("Load accepted incomplete fallback %q", doc)
		}
		if !strings.Contains(err.Error(), "fallback: enabled fallback needs url, key, and model") {
			t.Errorf("Load(%q) error = %v, want fallback validation message", doc, err)
		}
	}
}

// TestLoadFallbackType pins the fallback.type parsing: absent loads as the
// anthropic default, both accepted styles load through, and an unknown
// style is rejected at load with the accepted choices named — before any
// worker arms failover on a wire style nothing speaks.
func TestLoadFallbackType(t *testing.T) {
	cfg, err := Load(writeConfig(t, ""))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Fallback.Type != DefaultFallbackType {
		t.Errorf("Fallback.Type = %q, want the default %q", cfg.Fallback.Type, DefaultFallbackType)
	}

	for _, typ := range fallbackTypes {
		cfg, err := Load(writeConfig(t, "fallback:\n  type: "+typ+"\n"))
		if err != nil {
			t.Fatalf("Load(fallback.type: %s): %v", typ, err)
		}
		if cfg.Fallback.Type != typ {
			t.Errorf("Fallback.Type = %q, want %q", cfg.Fallback.Type, typ)
		}
	}

	_, err = Load(writeConfig(t, "fallback:\n  type: azure\n"))
	if err == nil || !strings.Contains(err.Error(), `unknown type "azure"`) {
		t.Fatalf("want unknown-fallback-type error, got %v", err)
	}
}

// TestAgentEnvFallbackTypeOverride pins that the fallback's wire style
// travels to the worker only as an override: an openai fallback exports
// DAEDALUS_FALLBACK_TYPE=openai, while the anthropic default — absent or
// explicit — exports nothing, so unset means "use the default" like every
// other field.
func TestAgentEnvFallbackTypeOverride(t *testing.T) {
	const doc = "fallback:\n  enabled: true\n  url: https://backup.example\n  key: sk-backup\n  model: glm-backup\n"

	cfg, err := Load(writeConfig(t, doc+"  type: openai\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !slices.Contains(cfg.AgentEnv(), "DAEDALUS_FALLBACK_TYPE=openai") {
		t.Errorf("AgentEnv() = %v, want DAEDALUS_FALLBACK_TYPE=openai for an openai fallback", cfg.AgentEnv())
	}

	for name, typ := range map[string]string{"absent": "", "explicit anthropic": "  type: anthropic\n"} {
		cfg, err := Load(writeConfig(t, doc+typ))
		if err != nil {
			t.Fatalf("%s: Load: %v", name, err)
		}
		for _, kv := range cfg.AgentEnv() {
			if strings.HasPrefix(kv, "DAEDALUS_FALLBACK_TYPE") {
				t.Errorf("%s: AgentEnv() leaked %q; the default must not be exported", name, kv)
			}
		}
	}
}

func TestAgentEnvOmitsEmpty(t *testing.T) {
	cfg, err := Load(writeConfig(t, "anthropic:\n  key: sk-only\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	env := cfg.AgentEnv()
	want := []string{
		"ANTHROPIC_API_KEY=sk-only",
		"API_TIMEOUT_MS=3000000",
	}
	if !slices.Equal(env, want) {
		t.Errorf("AgentEnv() = %v, want %v", env, want)
	}
}

// agentEnvValue returns the value of name in an AgentEnv slice, "" when
// absent.
func agentEnvValue(t *testing.T, env []string, name string) string {
	t.Helper()
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, name+"="); ok {
			return v
		}
	}
	return ""
}

// TestAgentEnvOpenAITimeout pins the API_TIMEOUT_MS selection rule: an
// openai section with a url serves the jailed round's API, so its
// timeout_ms replaces anthropic's — but only when set (zero falls back to
// anthropic's value, which Load defaults, so a ceiling always applies) and
// only when the section is actually serving (url set): an openai section
// without a url exists purely for openai-dialing tooling, and its timeout
// must not reach the anthropic-served rounds.
func TestAgentEnvOpenAITimeout(t *testing.T) {
	for name, doc := range map[string]string{
		"openai url and timeout": "anthropic:\n  timeout_ms: 60000\nopenai:\n  url: https://oa.example/v1\n  timeout_ms: 900000\n",
		"openai url, no timeout": "anthropic:\n  timeout_ms: 60000\nopenai:\n  url: https://oa.example/v1\n",
		"openai timeout, no url": "anthropic:\n  timeout_ms: 60000\nopenai:\n  timeout_ms: 900000\n",
	} {
		cfg, err := Load(writeConfig(t, doc))
		if err != nil {
			t.Fatalf("%s: Load: %v", name, err)
		}
		want := "60000"
		if name == "openai url and timeout" {
			want = "900000"
		}
		if got := agentEnvValue(t, cfg.AgentEnv(), "API_TIMEOUT_MS"); got != want {
			t.Errorf("%s: API_TIMEOUT_MS = %q, want %q", name, got, want)
		}
	}
}

// TestAgentEnvOpenAIBaseMirrorsURL pins the two-var URL export: when the
// openai section is set, OPENAI_API_BASE carries the same value as
// OPENAI_BASE_URL (litellm versions disagree on which var they honor); it
// is omitted entirely when the url is unset, like every other empty
// export.
func TestAgentEnvOpenAIBaseMirrorsURL(t *testing.T) {
	cfg, err := Load(writeConfig(t, "openai:\n  url: https://oa.example/v1\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	env := cfg.AgentEnv()
	if got, want := agentEnvValue(t, env, "OPENAI_API_BASE"), agentEnvValue(t, env, "OPENAI_BASE_URL"); got != want || got == "" {
		t.Errorf("OPENAI_API_BASE = %q, OPENAI_BASE_URL = %q; want both set to the same value", got, want)
	}

	cfg, err = Load(writeConfig(t, "openai:\n  key: sk-only\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, kv := range cfg.AgentEnv() {
		if strings.HasPrefix(kv, "OPENAI_API_BASE=") {
			t.Errorf("AgentEnv() leaked %q with no openai url configured", kv)
		}
	}
}

// TestProviderEnvVarsIncludesOpenAIAPIBase pins that OPENAI_API_BASE is in
// the scrub/restore list: the worker must treat it as a derived provider
// var (cleared and re-exported from config), or a stale inherited value
// would ride along next to the OPENAI_BASE_URL it is supposed to mirror.
func TestProviderEnvVarsIncludesOpenAIAPIBase(t *testing.T) {
	if !slices.Contains(ProviderEnvVars(), "OPENAI_API_BASE") {
		t.Errorf("ProviderEnvVars() = %v, want OPENAI_API_BASE listed", ProviderEnvVars())
	}
}

// TestLoadContextTokens pins the context_tokens knob end to end: a set
// value loads through and exports as ContextTokensEnv (the var jailed
// claude reads and the aider staging consumes), zero/unset exports
// nothing so every agent keeps its own default, and a negative value is
// rejected at load time.
func TestLoadContextTokens(t *testing.T) {
	cfg, err := Load(writeConfig(t, "anthropic:\n  key: sk-ant\n  context_tokens: 131072\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Anthropic.ContextTokens != 131072 {
		t.Errorf("Anthropic.ContextTokens = %d, want 131072", cfg.Anthropic.ContextTokens)
	}
	if got, want := agentEnvValue(t, cfg.AgentEnv(), ContextTokensEnv), "131072"; got != want {
		t.Errorf("AgentEnv() ContextTokensEnv = %q, want %q", got, want)
	}

	for _, doc := range []string{
		"anthropic:\n  key: sk-ant\n  context_tokens: 0\n",
		"anthropic:\n  key: sk-ant\n",
	} {
		cfg, err := Load(writeConfig(t, doc))
		if err != nil {
			t.Fatalf("Load(%q): %v", doc, err)
		}
		for _, kv := range cfg.AgentEnv() {
			if strings.HasPrefix(kv, ContextTokensEnv+"=") {
				t.Errorf("AgentEnv() leaked %q with context_tokens unset/zero (%q)", kv, doc)
			}
		}
	}

	if _, err := Load(writeConfig(t, "anthropic:\n  context_tokens: -1\n")); err == nil ||
		!strings.Contains(err.Error(), "anthropic.context_tokens") ||
		!strings.Contains(err.Error(), "must not be negative") {
		t.Errorf("Load(context_tokens: -1) err = %v, want an anthropic.context_tokens rejection", err)
	}
}

// TestProviderEnvVarsIncludesContextTokens pins that ContextTokensEnv is
// in the scrub/restore list: a daemon must clear an ambient export (agent
// harnesses set the var), or an unset config's context_tokens would be
// silently overridden by whatever the worker's shell inherited.
func TestProviderEnvVarsIncludesContextTokens(t *testing.T) {
	if !slices.Contains(ProviderEnvVars(), ContextTokensEnv) {
		t.Errorf("ProviderEnvVars() = %v, want %s listed", ProviderEnvVars(), ContextTokensEnv)
	}
}

// TestLoadBranchPrefix pins that a configured prefix loads through and an
// unusable one fails at config load, before any run starts.
func TestWorkerName(t *testing.T) {
	// An explicit worker_id names the worker, independently of the queue.
	cfg, err := Load(writeConfig(t, `
worker_id: arete
temporal:
  task_queue: q7
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.WorkerName(); got != "arete" {
		t.Errorf("WorkerName() = %q, want arete (the worker id, not the queue)", got)
	}

	// Without one, the task queue names the worker — the single-queue
	// default keeps working with no id to set.
	cfg, err = Load(writeConfig(t, "temporal:\n  task_queue: arete\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.WorkerName(); got != "arete" {
		t.Errorf("WorkerName() = %q, want arete (the task queue)", got)
	}
}

func TestLoadWorkerID(t *testing.T) {
	// Load rejects ids that cannot key the daemon-dir file names — the same
	// error shape as the other Load-time validations.
	for _, id := range []string{"../evil", "a/b", "a\\b", "a b", ".\t.", "-", ".", ".."} {
		_, err := Load(writeConfig(t, "worker_id: "+strconv.Quote(id)+"\n"))
		if err == nil || !strings.Contains(err.Error(), "worker id") {
			t.Errorf("Load(worker_id %q) err = %v, want a worker-id rejection", id, err)
		}
	}
	for _, id := range []string{"", "arete", "worker.2", "a-b_c"} {
		if _, err := Load(writeConfig(t, "worker_id: "+strconv.Quote(id)+"\n")); err != nil {
			t.Errorf("Load(worker_id %q): %v, want accepted", id, err)
		}
	}
}

func TestLoadBranchPrefix(t *testing.T) {
	cfg, err := Load(writeConfig(t, "branch_prefix: team/ship\n"))
	if err != nil {
		t.Fatalf("Load(branch_prefix): %v", err)
	}
	if cfg.BranchPrefix != "team/ship" {
		t.Errorf("BranchPrefix = %q, want team/ship", cfg.BranchPrefix)
	}

	_, err = Load(writeConfig(t, "branch_prefix: feat\n"))
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("want reserved-namespace error, got %v", err)
	}
}

// TestLoadAuthorship pins the authorship flag: unset stays false (the
// worker's git config authors daedalus's commits) and `authorship: true`
// opts into the forced "daedalus <daedalus@local>" identity.
func TestLoadAuthorship(t *testing.T) {
	cfg, err := Load(writeConfig(t, ""))
	if err != nil {
		t.Fatalf("Load(defaults): %v", err)
	}
	if cfg.Authorship {
		t.Errorf("Authorship = true, want the false default")
	}

	cfg, err = Load(writeConfig(t, "authorship: true\n"))
	if err != nil {
		t.Fatalf("Load(authorship): %v", err)
	}
	if !cfg.Authorship {
		t.Errorf("Authorship = false, want true")
	}
}

// TestValidateBranchPrefix pins the accepted and rejected prefixes, following
// git check-ref-format for the prospective <prefix>/issue-<id>-<ts> plus the
// reserved feat/aborted namespaces.
func TestValidateBranchPrefix(t *testing.T) {
	for _, p := range []string{"daedalus", "team", "team/ship", "v1.x", "a-b_c"} {
		if err := ValidateBranchPrefix(p); err != nil {
			t.Errorf("ValidateBranchPrefix(%q) = %v, want nil", p, err)
		}
	}
	for _, p := range []string{
		"",                 // unset means default, but a flag value must be real
		"-x",               // would read as an option
		"/x", "x/", "a//b", // empty path components
		"a b", "a~b", "a^b", "a:b", "a?b", "a*b", "a[b", `a\b`,
		"a..b",       // range syntax
		"a@{b",       // reflog syntax
		".x", "x/.y", // components may not begin with "."
		"x.lock", "a/b.lock", // lock suffix is reserved
		"feat", "aborted", // daedalus's own namespaces
		"feat/x", "aborted/y", // ... including as a leading component
		"a\x01b", // control characters
	} {
		if err := ValidateBranchPrefix(p); err == nil {
			t.Errorf("ValidateBranchPrefix(%q) = nil, want rejection", p)
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Fatal("want error for a missing config file")
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	if _, err := Load(writeConfig(t, "temporal: [broken")); err == nil {
		t.Fatal("want error for invalid YAML")
	}
}

// TestLoadRejectsUnknownKeys pins the strict decode: a key the config
// structs do not know — a typo, a plain-scalar document, or a key nested
// one section too deep (thinking: under openai:, which once disabled every
// lever by loading as all-defaults) — fails the load naming the field,
// instead of silently producing default behavior partway into a run. The
// empty document is the sanctioned all-defaults load (TestLoadDefaults).
func TestLoadRejectsUnknownKeys(t *testing.T) {
	cases := []struct{ name, doc, want string }{
		{"unknown top-level key", "tests_timeot: 5m\n", "tests_timeot"},
		{"misplaced section key", "openai:\n  thinking: false\n", "thinking"},
		{"plain scalar document", "just a scalar\n", "cannot unmarshal"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, c.doc)); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Load(%s) error = %v, want one containing %q", c.name, err, c.want)
			}
		})
	}

	// LoadRaw shares parse's single decode point, so it is strict too.
	if _, err := LoadRaw(writeConfig(t, "nosuchkey: 1\n")); err == nil || !strings.Contains(err.Error(), "nosuchkey") {
		t.Fatalf("LoadRaw error = %v, want one naming the unknown key", err)
	}
}

// TestExampleYAML pins that loading the example yields exactly the default
// configuration — every field at its default, nothing more.
func TestExampleYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config-example.yaml")
	if err := os.WriteFile(path, []byte(ExampleYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load(ExampleYAML): %v", err)
	}
	if cfg.Agent != DefaultAgent || cfg.BranchPrefix != DefaultBranchPrefix ||
		cfg.Temporal.Host != DefaultTemporalHost ||
		cfg.Temporal.UIPort != DefaultTemporalUIPort || cfg.Temporal.TaskQueue != DefaultTaskQueue ||
		cfg.WorkerID != "" {
		t.Errorf("ExampleYAML values = %+v, want the documented defaults", cfg)
	}
	if cfg.Anthropic.URL != "" || cfg.Anthropic.Key != "" || cfg.Anthropic.Model != "" ||
		cfg.Anthropic.TimeoutMS != DefaultAnthropicTimeoutMS || cfg.OpenAI != (OpenAIConfig{}) {
		t.Errorf("ExampleYAML provider values = %+v %+v, want empty (inherit the environment) except the timeout default", cfg.Anthropic, cfg.OpenAI)
	}
}

// TestExampleYAMLMatchesRepoFile keeps the shipped constant and the
// repository's config-example.yaml in lockstep.
func TestExampleYAMLMatchesRepoFile(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "config-example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != ExampleYAML {
		t.Error("config-example.yaml and config.ExampleYAML drifted apart — update both (or regenerate the file via `daedalus init`)")
	}
}

// TestLoadRawRawView pins LoadRaw's contract: the same read, parse, and
// validation as Load, but the returned Config holds the file's own values —
// set fields verbatim, absent fields at their zero value where Load would
// have filled in the default.
func TestLoadRawRawView(t *testing.T) {
	raw, err := LoadRaw(writeConfig(t, ""))
	if err != nil {
		t.Fatalf("LoadRaw(empty): %v", err)
	}
	if raw.Agent != "" || raw.BranchPrefix != "" || raw.Temporal.Host != "" ||
		raw.Temporal.TaskQueue != "" || raw.TestsTimeout != 0 || raw.MaxConcurrentAgentRuns != 0 {
		t.Errorf("LoadRaw(empty) = %+v, want the file's own zero values, not the defaults", raw)
	}

	raw, err = LoadRaw(writeConfig(t, "agent: pi\ntests_timeout: 45m\n"))
	if err != nil {
		t.Fatalf("LoadRaw: %v", err)
	}
	if raw.Agent != "pi" {
		t.Errorf("Agent = %q, want %q", raw.Agent, "pi")
	}
	if raw.TestsTimeout != 45*time.Minute {
		t.Errorf("TestsTimeout = %v, want 45m", raw.TestsTimeout)
	}
	if raw.BranchPrefix != "" {
		t.Errorf("BranchPrefix = %q, want empty (absent from the file stays absent)", raw.BranchPrefix)
	}
}

// TestLoadRawValidates pins that LoadRaw refuses what Load refuses, with
// Load's own diagnostics — `daedalus config` surfaces the same answer to
// "why does this config not work" a jailed run would get.
func TestLoadRawValidates(t *testing.T) {
	for _, doc := range []string{
		"agent: cursor\n",
		"branch_prefix: aborted\n",
		"worker_id: bad id\n",
		"tests_timeout: -5m\n",
		"temporal:\n  task_queue: test\n",
		"fallback:\n  enabled: true\n  url: https://backup.example\n  key: k\n",
	} {
		path := writeConfig(t, doc)
		_, err := LoadRaw(path)
		if err == nil {
			t.Errorf("LoadRaw(%q) = nil, want rejection", doc)
			continue
		}
		_, lerr := Load(path)
		if lerr == nil || lerr.Error() != err.Error() {
			t.Errorf("LoadRaw(%q) error = %v, want Load's diagnostic verbatim (Load gave %v)", doc, err, lerr)
		}
	}
	if _, err := LoadRaw(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Error("LoadRaw accepted a missing config file")
	}
	if _, err := LoadRaw(writeConfig(t, "temporal: [broken")); err == nil {
		t.Error("LoadRaw accepted invalid YAML")
	}
}

// TestLoadRawValidatesDefaultsCopy pins the subtlety in LoadRaw's split:
// validation runs on a defaults-applied copy while the returned view stays
// raw. An enabled fallback with no type passes — validating the raw view
// would reject type "" against the fallback-type whitelist — yet the
// returned Fallback.Type stays empty, and an explicitly bogus type is still
// rejected.
func TestLoadRawValidatesDefaultsCopy(t *testing.T) {
	raw, err := LoadRaw(writeConfig(t, "fallback:\n  enabled: true\n  url: https://backup.example\n  key: k\n  model: m\n"))
	if err != nil {
		t.Fatalf("LoadRaw(enabled fallback without type): %v", err)
	}
	if raw.Fallback.Type != "" {
		t.Errorf("Fallback.Type = %q, want the file's own empty value, not the default", raw.Fallback.Type)
	}

	_, err = LoadRaw(writeConfig(t, "fallback:\n  enabled: true\n  type: bogus\n  url: https://backup.example\n  key: k\n  model: m\n"))
	if err == nil || !strings.Contains(err.Error(), "unknown type") {
		t.Errorf("LoadRaw(bogus fallback type) error = %v, want unknown-type rejection", err)
	}
}

// TestRenderYAML pins the `daedalus config` render of a fully-set config:
// every field in struct order, durations as canonical duration strings
// rather than nanosecond counts, keys unredacted, and the output
// round-tripping back to exactly the Config LoadRaw returned.
func TestRenderYAML(t *testing.T) {
	raw, err := LoadRaw(writeConfig(t, `agent: pi
branch_prefix: feat-
authorship: true
worker_id: w1
tests_timeout: 45m
agent_run_timeout: 1h
review_timeout: 15m
cleanup_timeout: 5m
max_concurrent_agent_runs: 3
max_concurrent_tests: 2
temporal:
  host: temporal.example:7233
  ui_port: 8234
  task_queue: myqueue
anthropic:
  url: https://api.example.com
  key: sk-secret
  model: claude-x
  heartbeat_model: claude-haiku
  timeout_ms: 60000
openai:
  url: https://oai.example.com
  key: sk-oai
  model: gpt-x
fallback:
  enabled: true
  type: openai
  url: https://fb.example.com
  key: fb-key
  model: fb-model
  heartbeat_model: fb-haiku
`))
	if err != nil {
		t.Fatalf("LoadRaw: %v", err)
	}

	out, err := raw.RenderYAML()
	if err != nil {
		t.Fatalf("RenderYAML: %v", err)
	}

	// Every field, in struct order — the renderConfig mirror's contract.
	prev := -1
	for _, key := range []string{
		"agent:", "branch_prefix:", "authorship:", "worker_id:",
		"tests_timeout:", "agent_run_timeout:", "review_timeout:", "cleanup_timeout:",
		"max_concurrent_agent_runs:", "max_concurrent_tests:",
		"temporal:", "anthropic:", "openai:", "fallback:",
	} {
		i := strings.Index(out, key)
		if i < 0 {
			t.Errorf("RenderYAML output is missing %q:\n%s", key, out)
			continue
		}
		if i <= prev {
			t.Errorf("RenderYAML output has %q out of struct order:\n%s", key, out)
		}
		prev = i
	}

	// Durations render the way config files express them, never as the raw
	// nanosecond count a plain re-marshal would emit.
	for _, want := range []string{
		"tests_timeout: 45m0s", "agent_run_timeout: 1h0m0s",
		"review_timeout: 15m0s", "cleanup_timeout: 5m0s",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("RenderYAML output is missing %q:\n%s", want, out)
		}
	}
	for _, leaked := range []string{"2700000000000", "3600000000000"} {
		if strings.Contains(out, leaked) {
			t.Errorf("RenderYAML output leaked the nanosecond count %q:\n%s", leaked, out)
		}
	}

	// Keys print unredacted by design: the operator's own file, their own
	// terminal.
	for _, want := range []string{"key: sk-secret", "key: sk-oai", "key: fb-key"} {
		if !strings.Contains(out, want) {
			t.Errorf("RenderYAML output is missing unredacted %q:\n%s", want, out)
		}
	}

	// The render is lossless: parsing it back yields the same config.
	var back Config
	if err := yaml.Unmarshal([]byte(out), &back); err != nil {
		t.Fatalf("parse RenderYAML output: %v\n%s", err, out)
	}
	if back != raw {
		t.Errorf("RenderYAML round-trip = %+v, want %+v", back, raw)
	}
}

// TestRenderYAMLUnsetFields pins the pre-defaults view of an empty config:
// unset durations print plain 0 — the "documented default applies" marker,
// not an actual zero ceiling — unset strings print empty, and the inactive
// fallback prints disabled.
func TestRenderYAMLUnsetFields(t *testing.T) {
	raw, err := LoadRaw(writeConfig(t, ""))
	if err != nil {
		t.Fatalf("LoadRaw(empty): %v", err)
	}
	out, err := raw.RenderYAML()
	if err != nil {
		t.Fatalf("RenderYAML: %v", err)
	}
	for _, want := range []string{"tests_timeout: 0", "agent_run_timeout: 0", `agent: ""`, "authorship: false", "enabled: false"} {
		if !strings.Contains(out, want) {
			t.Errorf("RenderYAML(empty) output is missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "600000000000") {
		t.Errorf("RenderYAML(empty) leaked a nanosecond count for a duration:\n%s", out)
	}
}

// TestLoadMaxOutputTokens pins the max_output_tokens knob end to end: a set
// value loads through and exports as MaxOutputTokensEnv (the var the
// jailed-round builder re-exports for claude and the aider staging
// consumes), zero/unset exports nothing so every agent keeps its own
// default, and a negative value is rejected at load time.
func TestLoadMaxOutputTokens(t *testing.T) {
	cfg, err := Load(writeConfig(t, "anthropic:\n  key: sk-ant\n  max_output_tokens: 16384\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Anthropic.MaxOutputTokens != 16384 {
		t.Errorf("Anthropic.MaxOutputTokens = %d, want 16384", cfg.Anthropic.MaxOutputTokens)
	}
	if got, want := agentEnvValue(t, cfg.AgentEnv(), MaxOutputTokensEnv), "16384"; got != want {
		t.Errorf("AgentEnv() MaxOutputTokensEnv = %q, want %q", got, want)
	}

	for _, doc := range []string{
		"anthropic:\n  key: sk-ant\n  max_output_tokens: 0\n",
		"anthropic:\n  key: sk-ant\n",
	} {
		cfg, err := Load(writeConfig(t, doc))
		if err != nil {
			t.Fatalf("Load(%q): %v", doc, err)
		}
		for _, kv := range cfg.AgentEnv() {
			if strings.HasPrefix(kv, MaxOutputTokensEnv+"=") {
				t.Errorf("AgentEnv() leaked %q with max_output_tokens unset/zero (%q)", kv, doc)
			}
		}
	}

	if _, err := Load(writeConfig(t, "anthropic:\n  max_output_tokens: -1\n")); err == nil ||
		!strings.Contains(err.Error(), "anthropic.max_output_tokens") ||
		!strings.Contains(err.Error(), "must not be negative") {
		t.Errorf("Load(max_output_tokens: -1) err = %v, want an anthropic.max_output_tokens rejection", err)
	}
}

// TestLoadSamplerKnobs pins the openai section's five sampler knobs: a set
// value loads through and exports under its DAEDALUS_* name (the aider
// staging's only channel), zero/unset exports nothing — absent means the
// field is omitted from the staged request, never sent as a default — a
// negative value is rejected for the four whose API range is non-negative,
// while presence_penalty's legal [-2, 2] range makes a negative value a
// valid load, and NaN/Inf are rejected for all five (they would render as
// non-float literals in the staged aider settings).
func TestLoadSamplerKnobs(t *testing.T) {
	knobs := []struct {
		yamlKey string
		envVar  string
		value   string
		negOK   bool
	}{
		{"top_p", TopPEnv, "0.95", false},
		{"presence_penalty", PresencePenaltyEnv, "-0.5", true},
		{"top_k", TopKEnv, "40", false},
		{"min_p", MinPEnv, "0.05", false},
		{"repetition_penalty", RepetitionPenaltyEnv, "1.05", false},
	}
	for _, k := range knobs {
		t.Run(k.yamlKey+" exports when set", func(t *testing.T) {
			cfg, err := Load(writeConfig(t, "openai:\n  "+k.yamlKey+": "+k.value+"\n"))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got, want := agentEnvValue(t, cfg.AgentEnv(), k.envVar), k.value; got != want {
				t.Errorf("AgentEnv() %s = %q, want %q", k.envVar, got, want)
			}
		})
		t.Run(k.yamlKey+" omitted when unset", func(t *testing.T) {
			for _, doc := range []string{
				"openai:\n  " + k.yamlKey + ": 0\n",
				"openai:\n  url: https://selfhost.example/v1\n",
			} {
				cfg, err := Load(writeConfig(t, doc))
				if err != nil {
					t.Fatalf("Load(%q): %v", doc, err)
				}
				for _, kv := range cfg.AgentEnv() {
					if strings.HasPrefix(kv, k.envVar+"=") {
						t.Errorf("AgentEnv() leaked %q with %s unset/zero (%q)", kv, k.yamlKey, doc)
					}
				}
			}
		})
		t.Run(k.yamlKey+" negative", func(t *testing.T) {
			_, err := Load(writeConfig(t, "openai:\n  "+k.yamlKey+": -1\n"))
			if k.negOK {
				if err != nil {
					t.Fatalf("Load(%s: -1): %v, want a legal load (API range is [-2, 2])", k.yamlKey, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "openai."+k.yamlKey) ||
				!strings.Contains(err.Error(), "must not be negative") {
				t.Errorf("Load(%s: -1) err = %v, want an openai.%s rejection", k.yamlKey, err, k.yamlKey)
			}
		})
		t.Run(k.yamlKey+" rejects non-finite", func(t *testing.T) {
			for _, lit := range []string{".nan", ".inf", "-.inf"} {
				if _, err := Load(writeConfig(t, "openai:\n  "+k.yamlKey+": "+lit+"\n")); err == nil ||
					!strings.Contains(err.Error(), "openai."+k.yamlKey) ||
					!strings.Contains(err.Error(), "finite") {
					t.Fatalf("Load(%s: %s) err = %v, want an openai.%s rejection", k.yamlKey, lit, err, k.yamlKey)
				}
			}
		})
	}
}

// TestStreamSetting pins the stream toggle's tri-state semantics, the same
// shape as Thinking: an absent key (and an explicit YAML null) reports
// not-set so every agent keeps its own streaming default; an explicit
// true/false reports "on"/"off".
func TestStreamSetting(t *testing.T) {
	for _, c := range []struct {
		name    string
		yaml    string
		wantVal string
		wantOK  bool
	}{
		{"absent", "", "", false},
		{"explicit null", "stream: null\n", "", false},
		{"explicit true", "stream: true\n", "on", true},
		{"explicit false", "stream: false\n", "off", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, c.yaml))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			got, ok := cfg.StreamSetting()
			if got != c.wantVal || ok != c.wantOK {
				t.Errorf("StreamSetting() = (%q, %v), want (%q, %v)", got, ok, c.wantVal, c.wantOK)
			}
		})
	}
}

// TestProviderEnvVarsIncludesKnobExports pins that the six new knob exports
// are in the scrub/restore list: a daemon must clear ambient values (real
// worker shells carry them once a config sets the knobs), or an unset
// config's knobs would be silently overridden by whatever the invoking
// shell inherited.
func TestProviderEnvVarsIncludesKnobExports(t *testing.T) {
	have := ProviderEnvVars()
	for _, name := range []string{
		MaxOutputTokensEnv,
		TopPEnv,
		PresencePenaltyEnv,
		TopKEnv,
		MinPEnv,
		RepetitionPenaltyEnv,
	} {
		if !slices.Contains(have, name) {
			t.Errorf("ProviderEnvVars() = %v, want %s listed", have, name)
		}
	}
}
