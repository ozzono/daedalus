package config

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
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
	for _, agent := range []string{"claude", "opencode", "amp", "pi", "aider", "codex"} {
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
// slim.enabled: true forces max_concurrent_agent_runs to 1 — normalization
// is the single source, so the env export, the semaphore, and report's slot
// display all just see 1 — while max_concurrent_tests is untouched (native
// suites dial no LLM). Validation precedes the clamp, so an invalid
// negative max_concurrent_agent_runs still errors. The section also loads
// the relay's parser_model, and the historical top-level `slim: true`
// boolean still loads (it decodes into enabled — the README-documented
// migration for the 2026-10-02 reshape), so an upgrading operator's config
// keeps working while the strict decode still rejects a typo'd section key
// and a section-shaped value that is neither.
func TestLoadSlimClampsAgentConcurrency(t *testing.T) {
	cfg, err := Load(writeConfig(t, "slim:\n  enabled: true\n  parser_model: qwen2.5-coder:3b-parser\nmax_concurrent_agent_runs: 4\nmax_concurrent_tests: 3\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaxConcurrentAgentRuns != 1 {
		t.Errorf("slim MaxConcurrentAgentRuns = %d, want 1", cfg.MaxConcurrentAgentRuns)
	}
	if cfg.MaxConcurrentTests != 3 {
		t.Errorf("slim MaxConcurrentTests = %d, want the explicit 3 (suites dial no LLM)", cfg.MaxConcurrentTests)
	}
	if cfg.Slim.ParserModel != "qwen2.5-coder:3b-parser" {
		t.Errorf("slim parser_model = %q, want the configured parser", cfg.Slim.ParserModel)
	}

	cfg, err = Load(writeConfig(t, "slim:\n  enabled: false\nmax_concurrent_agent_runs: 4\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaxConcurrentAgentRuns != 4 {
		t.Errorf("non-slim MaxConcurrentAgentRuns = %d, want 4", cfg.MaxConcurrentAgentRuns)
	}

	_, err = Load(writeConfig(t, "slim:\n  enabled: true\nmax_concurrent_agent_runs: -1\n"))
	if err == nil || !strings.Contains(err.Error(), "max_concurrent_agent_runs") {
		t.Fatalf("want max_concurrent_agent_runs error under slim, got %v", err)
	}

	// The legacy top-level boolean spelling loads — it decodes into
	// enabled, so an un-migrated config clamps exactly like the section
	// shape instead of failing with a struct-name decode error.
	cfg, err = Load(writeConfig(t, "slim: true\nmax_concurrent_agent_runs: 4\n"))
	if err != nil {
		t.Fatalf("Load(legacy slim: true): %v", err)
	}
	if !cfg.Slim.Enabled || cfg.MaxConcurrentAgentRuns != 1 {
		t.Errorf("legacy slim: true = enabled %v, %d runs; want enabled with the clamp to 1", cfg.Slim.Enabled, cfg.MaxConcurrentAgentRuns)
	}
	cfg, err = Load(writeConfig(t, "slim: false\nmax_concurrent_agent_runs: 4\n"))
	if err != nil {
		t.Fatalf("Load(legacy slim: false): %v", err)
	}
	if cfg.Slim.Enabled || cfg.MaxConcurrentAgentRuns != 4 {
		t.Errorf("legacy slim: false = enabled %v, %d runs; want disabled and untouched", cfg.Slim.Enabled, cfg.MaxConcurrentAgentRuns)
	}

	// KnownFields strictness does not reach through a custom unmarshaler,
	// so a typo'd section key must still fail the load — the migration
	// must not reopen the silent-typo hole.
	_, err = Load(writeConfig(t, "slim:\n  enabld: true\n"))
	if err == nil || !strings.Contains(err.Error(), "field enabld not found") {
		t.Errorf("Load(typo'd slim key) error = %v, want the field-not-found rejection", err)
	}

	// A value that is neither the legacy scalar nor a mapping is still a
	// decode error, not a zero-value section.
	_, err = Load(writeConfig(t, "slim:\n  - true\n"))
	if err == nil || !strings.Contains(err.Error(), "cannot unmarshal !!seq into config.SlimConfig") {
		t.Errorf("Load(slim as a sequence) error = %v, want the shape rejection", err)
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
worker_id: alpha
temporal:
  task_queue: q7
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.WorkerName(); got != "alpha" {
		t.Errorf("WorkerName() = %q, want alpha (the worker id, not the queue)", got)
	}

	// Without one, the task queue names the worker — the single-queue
	// default keeps working with no id to set.
	cfg, err = Load(writeConfig(t, "temporal:\n  task_queue: alpha\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.WorkerName(); got != "alpha" {
		t.Errorf("WorkerName() = %q, want alpha (the task queue)", got)
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
	for _, id := range []string{"", "alpha", "worker.2", "a-b_c"} {
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

// loadCapturingAlerts loads content while capturing the stderr the
// duplicate-key alerts print to, returning the loaded config, the config's
// path (the alerts name it), the captured text, and Load's error. The swap is
// safe here: nothing in the package's tests runs in parallel, and the alerts
// are a few short lines — far under the pipe buffer, so the write never
// blocks on this read.
func loadCapturingAlerts(t *testing.T, content string) (Config, string, string, error) {
	t.Helper()
	path := writeConfig(t, content)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stderr
	os.Stderr = w
	cfg, loadErr := Load(path)
	os.Stderr = saved
	w.Close()
	data, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatal(err)
	}
	return cfg, path, string(data), loadErr
}

// TestLoadDuplicateKeys pins the tolerated-duplicate contract: a repeated
// key in any section — the slim section's hand-walked unmarshaler included —
// loads with the last value winning, one stderr alert per dropped occurrence
// naming the file, section, key, and winning line. Nothing else loosens: a
// typo'd key still fails the strict decode through the doc a prune leaves
// behind, and an ordinary config still decodes from the file's own bytes, so
// its decode errors keep the file's own line numbers (a re-encode drops the
// blank lines between top-level sections, renumbering everything after the
// first).
func TestLoadDuplicateKeys(t *testing.T) {
	t.Run("duplicate in the slim section keeps the last value, loudly", func(t *testing.T) {
		cfg, path, alerts, err := loadCapturingAlerts(t, "slim:\n  enabled: true\n  enabled: false\n")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.Slim.Enabled {
			t.Error("Slim.Enabled = true, want false — the last value wins")
		}
		want := fmt.Sprintf("config alert: duplicate key %q in section %q (%s:3) — the last value wins\n", "enabled", "slim", path)
		if alerts != want {
			t.Errorf("alerts = %q, want exactly %q", alerts, want)
		}
	})

	t.Run("duplicate at the top level — a load error before — keeps the last value", func(t *testing.T) {
		cfg, path, alerts, err := loadCapturingAlerts(t, "agent: claude\nagent: pi\n")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.Agent != "pi" {
			t.Errorf("Agent = %q, want pi — the last value wins", cfg.Agent)
		}
		want := fmt.Sprintf("config alert: duplicate key %q in section %q (%s:2) — the last value wins\n", "agent", "top level", path)
		if alerts != want {
			t.Errorf("alerts = %q, want exactly %q", alerts, want)
		}
	})

	t.Run("duplicate in a nested section names the section", func(t *testing.T) {
		cfg, path, alerts, err := loadCapturingAlerts(t, "openai:\n  url: https://a.example/v1\n  url: https://b.example/v1\n")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.OpenAI.URL != "https://b.example/v1" {
			t.Errorf("OpenAI.URL = %q, want the last value", cfg.OpenAI.URL)
		}
		want := fmt.Sprintf("config alert: duplicate key %q in section %q (%s:3) — the last value wins\n", "url", "openai", path)
		if alerts != want {
			t.Errorf("alerts = %q, want exactly %q", alerts, want)
		}
	})

	t.Run("a typo still fails through the pruned doc", func(t *testing.T) {
		_, _, alerts, err := loadCapturingAlerts(t, "agent: claude\nagent: pi\ntests_timeot: 5m\n")
		if err == nil || !strings.Contains(err.Error(), "tests_timeot") {
			t.Fatalf("Load error = %v, want the unknown-key rejection to survive the prune's re-encode", err)
		}
		if !strings.Contains(alerts, `duplicate key "agent"`) {
			t.Errorf("alerts = %q, want the duplicate-key alert alongside the rejection", alerts)
		}
	})

	t.Run("a dup-free config decodes the file's own bytes", func(t *testing.T) {
		// The blank line between the sections is what a re-encode would
		// drop, pulling thinking: up to line 3; the decode error must carry
		// the file's own line 4.
		_, err := Load(writeConfig(t, "agent: claude\n\nopenai:\n  thinking: false\n"))
		if err == nil || !strings.Contains(err.Error(), "line 4") {
			t.Fatalf("Load error = %v, want the misplaced-key rejection at the file's own line 4", err)
		}
	})

	t.Run("no duplicate, no alert", func(t *testing.T) {
		_, _, alerts, err := loadCapturingAlerts(t, "agent: claude\nslim:\n  enabled: true\n")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if alerts != "" {
			t.Errorf("alerts = %q, want stderr untouched for an ordinary config", alerts)
		}
	})
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

// TestExampleYAMLFor pins ExampleYAMLFor's composition: the base
// configuration in every output (so a profiled file stands alone — Load
// accepts each of them), the slim slices and the prompt slice only when
// requested, the slim slices keeping the example's file order, and both
// profiles reproducing ExampleYAML byte for byte.
func TestExampleYAMLFor(t *testing.T) {
	// Key-level markers, each unique to its piece of the example: the base
	// ones ride exampleBase1/3, the slim and prompt ones a profile's slices.
	markers := []struct {
		label string
		mark  string
	}{
		{"agent", "\nagent: claude\n"},
		{"thinking", "\nthinking: true\n"},
		{"slim section", "\nslim:\n"},
		{"openai section", "\nopenai:\n"},
		{"anthropic context_tokens", "\n  context_tokens: 0\n"},
		{"prompt section", "# Project-wise prompt overrides"},
	}
	inBase := map[string]bool{"agent": true, "thinking": true}
	inSlim := map[string]bool{"slim section": true, "openai section": true, "anthropic context_tokens": true}
	inPrompt := map[string]bool{"prompt section": true}

	for _, c := range []struct {
		slim, prompt bool
	}{
		{false, false},
		{true, false},
		{false, true},
		{true, true},
	} {
		out := ExampleYAMLFor(c.slim, c.prompt)
		for _, m := range markers {
			want := inBase[m.label] || (inSlim[m.label] && c.slim) || (inPrompt[m.label] && c.prompt)
			if got := strings.Contains(out, m.mark); got != want {
				t.Errorf("ExampleYAMLFor(slim=%t, prompt=%t) %s present = %v, want %v", c.slim, c.prompt, m.label, got, want)
			}
		}
		// A profiled init's file is a configuration like any other: it must
		// load.
		path := filepath.Join(t.TempDir(), "config-example.yaml")
		if err := os.WriteFile(path, []byte(out), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err != nil {
			t.Errorf("Load(ExampleYAMLFor(slim=%t, prompt=%t)): %v", c.slim, c.prompt, err)
		}
	}

	// Both requested is the whole example, nothing reordered.
	if got := ExampleYAMLFor(true, true); got != ExampleYAML {
		t.Error("ExampleYAMLFor(slim=true, prompt=true) should equal ExampleYAML byte for byte")
	}

	// The slim slices sit where the example puts them: the slim: block
	// before the remaining general fields, the openai: section (and
	// anthropic's sizing) after them and before the base provider plumbing.
	slimOnly := ExampleYAMLFor(true, false)
	first := func(mark string) int { return strings.Index(slimOnly, mark) }
	if a, b, c, d := first("\nslim:\n"), first("max_concurrent_tests: 2"), first("\nopenai:\n"), first("\nfallback:\n"); !(a < b && b < c && c < d) {
		t.Errorf("slim output out of file order: slim@%d max_concurrent_tests@%d openai@%d fallback@%d", a, b, c, d)
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
reviewer:
  url: https://review.example
  key: sk-review
dependency:
  enabled: false
  fallback_branch: dep-fallback
  skip_parked: false
  skip_failed: true
  skip_stuck: true
  skip_canceled: true
prompt: overrides/
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
		"temporal:", "anthropic:", "openai:", "fallback:", "reviewer:", "dependency:", "prompt:",
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
	for _, want := range []string{"key: sk-secret", "key: sk-oai", "key: fb-key", "key: sk-review"} {
		if !strings.Contains(out, want) {
			t.Errorf("RenderYAML output is missing unredacted %q:\n%s", want, out)
		}
	}

	// The render is lossless: parsing it back — strictly, so the render
	// must emit only keys the decoder accepts — yields the same config.
	// Config's tri-state dependency pointers make == compare by address
	// (the known Config-equality wart), so the whole-config pin is the
	// re-render: every field prints, so equal output means equal values.
	back, err := LoadRaw(writeConfig(t, out))
	if err != nil {
		t.Fatalf("parse RenderYAML output: %v\n%s", err, out)
	}
	roundTrip, err := back.RenderYAML()
	if err != nil {
		t.Fatalf("RenderYAML round-trip: %v", err)
	}
	if roundTrip != out {
		t.Errorf("RenderYAML round-trip =\n%s\nwant\n%s", roundTrip, out)
	}
	// The explicit non-defaults survive un-defaulted: the tri-state flags
	// keep their explicit false (nil would default to true), and the plain
	// flags and branch name arrive verbatim.
	if back.Dependency.EnabledOrDefault() || back.Dependency.SkipParkedOrDefault() {
		t.Errorf("RenderYAML round-trip defaulted the explicit tri-state dependency flags: %+v", back.Dependency)
	}
	if back.Dependency.FallbackBranch != "dep-fallback" || !back.Dependency.SkipFailed ||
		!back.Dependency.SkipStuck || !back.Dependency.SkipCanceled {
		t.Errorf("RenderYAML round-trip lost the explicit dependency values: %+v", back.Dependency)
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
	for _, want := range []string{"tests_timeout: 0", "agent_run_timeout: 0", `agent: ""`, "authorship: false", "enabled: false", `prompt: ""`} {
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

// TestLoadSamplerKnobs pins the openai section's six sampler knobs: a set
// value loads through and exports under its DAEDALUS_* name (the aider
// staging's only channel), zero/unset exports nothing — absent means the
// field is omitted from the staged request, never sent as a default — a
// negative value is rejected for the five whose API range is non-negative,
// while presence_penalty's legal [-2, 2] range makes a negative value a
// valid load, and NaN/Inf are rejected for all six (they would render as
// non-float literals in the staged aider settings).
func TestLoadSamplerKnobs(t *testing.T) {
	knobs := []struct {
		yamlKey string
		envVar  string
		value   string
		negOK   bool
	}{
		{"temperature", TemperatureEnv, "0.2", false},
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

// TestProviderEnvVarsIncludesKnobExports pins that the seven new knob exports
// are in the scrub/restore list: a daemon must clear ambient values (real
// worker shells carry them once a config sets the knobs), or an unset
// config's knobs would be silently overridden by whatever the invoking
// shell inherited.
func TestProviderEnvVarsIncludesKnobExports(t *testing.T) {
	have := ProviderEnvVars()
	for _, name := range []string{
		MaxOutputTokensEnv,
		TopPEnv,
		TemperatureEnv,
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

// TestBugFilingDir pins the effective-dir resolution: off (the section
// absent, or enabled: false) returns "" — the off signal the env export
// and the prompt render sites key on; enabled without a dir keeps the
// historical DefaultBugDir path; an explicit dir loads through verbatim.
func TestBugFilingDir(t *testing.T) {
	for _, tc := range []struct {
		name, doc, want string
	}{
		{name: "absent", doc: "", want: ""},
		{name: "disabled", doc: "bug_filing:\n  enabled: false\n  dir: docs/bugs\n", want: ""},
		{name: "enabled default dir", doc: "bug_filing:\n  enabled: true\n", want: DefaultBugDir},
		{name: "enabled explicit dir", doc: "bug_filing:\n  enabled: true\n  dir: docs/known-bugs\n", want: "docs/known-bugs"},
	} {
		cfg, err := Load(writeConfig(t, tc.doc))
		if err != nil {
			t.Fatalf("%s: Load: %v", tc.name, err)
		}
		if got := cfg.BugFilingDir(); got != tc.want {
			t.Errorf("%s: BugFilingDir() = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestValidateBugDir pins the path-safety classes a worktree-relative
// folder must satisfy: absolute paths, ".." escaping the root, and empty
// or dot components are rejected, while equivalent spellings ("a/./b",
// "a//b", trailing slash) normalize to the same clean path and load.
func TestValidateBugDir(t *testing.T) {
	for _, dir := range []string{"backlog/bugs", "docs", "a/./b", "a//b", "a/"} {
		if err := ValidateBugDir(dir); err != nil {
			t.Errorf("ValidateBugDir(%q) = %v, want nil", dir, err)
		}
	}
	for _, dir := range []string{"/abs", "", ".", "..", "../up", "a/../..", "a/..", "~", "~/bugs"} {
		if err := ValidateBugDir(dir); err == nil {
			t.Errorf("ValidateBugDir(%q) = nil, want an error", dir)
		}
	}
}

// TestLoadBugFilingValidation pins that the path-safety gate arms only
// when filing is on: an off toggle never renders a dir anywhere, so a
// garbage dir loads fine disabled but is rejected at load — with the
// section named — once enabled.
func TestLoadBugFilingValidation(t *testing.T) {
	if _, err := Load(writeConfig(t, "bug_filing:\n  enabled: false\n  dir: ../escape\n")); err != nil {
		t.Fatalf("Load (off): %v", err)
	}
	_, err := Load(writeConfig(t, "bug_filing:\n  enabled: true\n  dir: ../escape\n"))
	if err == nil || !strings.Contains(err.Error(), "bug_filing:") || !strings.Contains(err.Error(), "must not escape the worktree root") {
		t.Fatalf("Load (on, escaping dir) error = %v, want a bug_filing path-safety rejection", err)
	}
}

// TestProviderEnvVarsIncludesBugDir pins that DAEDALUS_BUG_DIR is in the
// scrub/restore list: a daemon spawn must clear ambient values, or an
// off-config worker would silently inherit a stale shell export that
// re-enables filing.
func TestProviderEnvVarsIncludesBugDir(t *testing.T) {
	if !slices.Contains(ProviderEnvVars(), BugDirEnv) {
		t.Errorf("ProviderEnvVars() = %v, want %s listed", ProviderEnvVars(), BugDirEnv)
	}
}

// TestLoadSharedTestQueue pins the shared_test_queue toggle's pointer
// semantics: an absent key and an explicit true both report the
// fleet-shared routing — the historical behavior, and what a config from
// before the knob means — and only an explicit false opts this
// deployment's suites onto its own derived queue.
func TestLoadSharedTestQueue(t *testing.T) {
	for _, c := range []struct {
		name string
		yaml string
		want bool
	}{
		{"absent", "", true},
		{"explicit true", "shared_test_queue: true\n", true},
		{"explicit false", "shared_test_queue: false\n", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, c.yaml))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got := cfg.SharesTestQueue(); got != c.want {
				t.Errorf("SharesTestQueue() = %v (SharedTestQueue = %v), want %v", got, cfg.SharedTestQueue, c.want)
			}
		})
	}
}

// TestLoadDependency pins the dependency section's decode and tri-state
// defaults: an absent section is the historical refusal posture (enabled,
// but with no fallback branch there is nowhere to release to), only an
// explicit false disables, and skip_parked is the one skip flag defaulted
// on — the stopped state whose work survives on its aborted branch.
func TestLoadDependency(t *testing.T) {
	t.Run("absent section is enabled with no fallback", func(t *testing.T) {
		cfg, err := Load(writeConfig(t, ""))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !cfg.Dependency.EnabledOrDefault() {
			t.Error("EnabledOrDefault() = false on an absent section, want true")
		}
		if !cfg.Dependency.SkipParkedOrDefault() {
			t.Error("SkipParkedOrDefault() = false on an absent section, want true")
		}
		if cfg.Dependency.ReleasePosture() {
			t.Error("ReleasePosture() = true with no fallback_branch, want false — a release with nowhere to land is the refusal")
		}
	})

	t.Run("explicit flags decode", func(t *testing.T) {
		cfg, err := Load(writeConfig(t, ""+
			"dependency:\n"+
			"  enabled: true\n"+
			"  fallback_branch: release-base\n"+
			"  skip_parked: false\n"+
			"  skip_failed: true\n"+
			"  skip_stuck: true\n"+
			"  skip_canceled: true\n"))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.Dependency.FallbackBranch != "release-base" {
			t.Errorf("FallbackBranch = %q, want release-base", cfg.Dependency.FallbackBranch)
		}
		if cfg.Dependency.Enabled == nil || !*cfg.Dependency.Enabled {
			t.Errorf("Enabled = %v, want the explicit true decoded into the pointer", cfg.Dependency.Enabled)
		}
		if cfg.Dependency.SkipParked == nil || *cfg.Dependency.SkipParked {
			t.Errorf("SkipParked = %v, want the explicit false decoded into the pointer", cfg.Dependency.SkipParked)
		}
		if cfg.Dependency.SkipParkedOrDefault() {
			t.Error("SkipParkedOrDefault() = true, want the explicit false")
		}
		if !cfg.Dependency.SkipFailed || !cfg.Dependency.SkipStuck || !cfg.Dependency.SkipCanceled {
			t.Errorf("skip flags = %v/%v/%v, want all true", cfg.Dependency.SkipFailed, cfg.Dependency.SkipStuck, cfg.Dependency.SkipCanceled)
		}
		if !cfg.Dependency.ReleasePosture() {
			t.Error("ReleasePosture() = false, want true (enabled with a fallback branch)")
		}
	})

	t.Run("explicit enabled false disables the section", func(t *testing.T) {
		cfg, err := Load(writeConfig(t, "dependency:\n  enabled: false\n  fallback_branch: release-base\n"))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.Dependency.EnabledOrDefault() {
			t.Error("EnabledOrDefault() = true on an explicit false, want false")
		}
		if cfg.Dependency.ReleasePosture() {
			t.Error("ReleasePosture() = true while disabled, want false — enabled: false restores the unconditional refusal whatever the other fields say")
		}
	})

	t.Run("unknown key under dependency is rejected", func(t *testing.T) {
		_, err := Load(writeConfig(t, "dependency:\n  fallback_branchn: typo\n"))
		if err == nil || !strings.Contains(err.Error(), "fallback_branchn") {
			t.Errorf("Load(unknown dependency key) err = %v, want the strict-decode rejection naming the key", err)
		}
	})
}

// TestReleasePosture pins the posture gate both the submit preflight and the
// workflow key on: enabled with a resolved fallback branch, and nothing
// else — the zero value (a run whose input predates the field) refuses,
// replay-safe in the loud direction.
func TestReleasePosture(t *testing.T) {
	if (DependencyConfig{}).ReleasePosture() {
		t.Error("zero DependencyConfig is in release posture, want refusal")
	}
	enabledNoBranch := DependencyConfig{SkipParked: new(bool)}
	if enabledNoBranch.ReleasePosture() {
		t.Error("enabled with no fallback branch is in release posture, want refusal")
	}
	disabled := false
	disabledWithBranch := DependencyConfig{FallbackBranch: "b", Enabled: &disabled}
	if disabledWithBranch.ReleasePosture() {
		t.Error("disabled section with a fallback branch is in release posture, want refusal")
	}
	if !(DependencyConfig{FallbackBranch: "b"}).ReleasePosture() {
		t.Error("enabled section with a fallback branch refuses, want release posture")
	}
}

// TestDerivedSuiteQueueName pins TestQueueFor's naming: the deployment's
// main task queue plus the "-test" suffix — the name the "-test"-suffix
// validation (TestLoadRejectsTestSuffixQueue) must keep out of the main
// queue namespace.
func TestDerivedSuiteQueueName(t *testing.T) {
	for queue, want := range map[string]string{
		DefaultTaskQueue: DefaultTaskQueue + "-test",
		"q7":             "q7-test",
	} {
		if got := TestQueueFor(queue); got != want {
			t.Errorf("TestQueueFor(%q) = %q, want %q", queue, got, want)
		}
	}
}

// TestLoadRejectsTestSuffixQueue pins the derived-queue collision gate: a
// main queue carrying the "-test" suffix would derive a suite queue
// colliding with the stem name's deployment, so it is rejected at load
// with the collision named, while names merely containing "test" as a
// substring or prefix stay legal.
func TestLoadRejectsTestSuffixQueue(t *testing.T) {
	_, err := Load(writeConfig(t, "temporal:\n  task_queue: dev-test\n"))
	if err == nil || !strings.Contains(err.Error(), "-test") || !strings.Contains(err.Error(), "dev") {
		t.Fatalf("Load(task_queue: dev-test) err = %v, want a -test-suffix rejection naming the derivation", err)
	}
	for _, q := range []string{"dev-testing", "test-dev", "attest"} {
		if _, err := Load(writeConfig(t, "temporal:\n  task_queue: "+q+"\n")); err != nil {
			t.Errorf("Load(task_queue: %s): %v, want accepted", q, err)
		}
	}
}

// TestRenderYAMLSharedTestQueue pins the toggle's `daedalus config` render:
// an explicit opt-out prints verbatim and parses back to the same
// opt-out. The comparison checks the effective value rather than struct
// equality: two separately parsed Configs carrying an explicit false hold
// distinct *bool pointers, so == would report them unequal (the
// pointer-field wart in Config's comparability), while the round trip's
// contract is about the values.
func TestRenderYAMLSharedTestQueue(t *testing.T) {
	raw, err := LoadRaw(writeConfig(t, "tests_timeout: 45m\nagent_run_timeout: 30m\nreview_timeout: 15m\ncleanup_timeout: 5m\nshared_test_queue: false\n"))
	if err != nil {
		t.Fatalf("LoadRaw: %v", err)
	}
	out, err := raw.RenderYAML()
	if err != nil {
		t.Fatalf("RenderYAML: %v", err)
	}
	if !strings.Contains(out, "shared_test_queue: false") {
		t.Errorf("RenderYAML output is missing the explicit opt-out:\n%s", out)
	}
	// Round-trip through Load, the way `daedalus config` consumers would.
	// The doc pins every duration: the render of an unset duration is a
	// bare 0, which Load's time.Duration decode rejects outright (filed
	// separately), and the round trip here must not stand on that bug.
	back, err := Load(writeConfig(t, out))
	if err != nil {
		t.Fatalf("Load(RenderYAML output): %v\n%s", err, out)
	}
	if back.SharedTestQueue == nil || *back.SharedTestQueue {
		t.Errorf("round-trip SharedTestQueue = %v, want an explicit false", back.SharedTestQueue)
	}
	if back.SharesTestQueue() {
		t.Error("round-trip SharesTestQueue() = true, want false")
	}
}

// TestTestOutputDir pins the effective-dir resolution: off (the section
// absent, or enabled: false) returns "" — the off signal the pipeline
// input carries to the suite activity; enabled without a dir keeps
// DefaultTestOutputDir; an explicit dir loads through verbatim.
func TestTestOutputDir(t *testing.T) {
	for _, tc := range []struct {
		name, doc, want string
	}{
		{name: "absent", doc: "", want: ""},
		{name: "disabled", doc: "test_output:\n  enabled: false\n  dir: dumps\n", want: ""},
		{name: "enabled default dir", doc: "test_output:\n  enabled: true\n", want: DefaultTestOutputDir},
		{name: "enabled explicit dir", doc: "test_output:\n  enabled: true\n  dir: tmp/suite-dumps\n", want: "tmp/suite-dumps"},
	} {
		cfg, err := Load(writeConfig(t, tc.doc))
		if err != nil {
			t.Fatalf("%s: Load: %v", tc.name, err)
		}
		if got := cfg.TestOutputDir(); got != tc.want {
			t.Errorf("%s: TestOutputDir() = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestValidateTestOutputDir pins the path-safety classes a worktree-relative
// folder must satisfy: absolute paths, ".." escaping the root, and empty
// or dot components are rejected, while equivalent spellings ("a/./b",
// "a//b", trailing slash) normalize to the same clean path and load.
func TestValidateTestOutputDir(t *testing.T) {
	for _, dir := range []string{".daedalus/test-output", "tmp", "a/./b", "a//b", "a/"} {
		if err := ValidateTestOutputDir(dir); err != nil {
			t.Errorf("ValidateTestOutputDir(%q) = %v, want nil", dir, err)
		}
	}
	for _, dir := range []string{"/abs", "", ".", "..", "../up", "a/../..", "a/..", "~", "~/dumps"} {
		if err := ValidateTestOutputDir(dir); err == nil {
			t.Errorf("ValidateTestOutputDir(%q) = nil, want an error", dir)
		}
	}
}

// TestLoadTestOutputValidation pins that the path-safety gate arms only
// when dumping is on: an off toggle never writes anything, so a garbage
// dir loads fine disabled but is rejected at load — with the section
// named — once enabled.
func TestLoadTestOutputValidation(t *testing.T) {
	if _, err := Load(writeConfig(t, "test_output:\n  enabled: false\n  dir: ../escape\n")); err != nil {
		t.Fatalf("Load (off): %v", err)
	}
	_, err := Load(writeConfig(t, "test_output:\n  enabled: true\n  dir: ../escape\n"))
	if err == nil || !strings.Contains(err.Error(), "test_output:") || !strings.Contains(err.Error(), "must not escape the worktree root") {
		t.Fatalf("Load (on, escaping dir) error = %v, want a test_output path-safety rejection", err)
	}
}

// TestRenderYAMLTestOutput pins that the test_output section renders and
// round-trips: an enabled run with an explicit dir renders the section and
// parses back to the same Config.
func TestRenderYAMLTestOutput(t *testing.T) {
	// The durations are pinned because the render of an unset duration is
	// a bare 0, which Load's time.Duration decode rejects outright (filed
	// separately) — the round trip here must not stand on that bug.
	raw, err := LoadRaw(writeConfig(t, "tests_timeout: 45m\nagent_run_timeout: 30m\nreview_timeout: 15m\ncleanup_timeout: 5m\ntest_output:\n  enabled: true\n  dir: tmp/suite-dumps\n  mirror: /host/dumps\n"))
	if err != nil {
		t.Fatalf("LoadRaw: %v", err)
	}
	out, err := raw.RenderYAML()
	if err != nil {
		t.Fatalf("RenderYAML: %v", err)
	}
	for _, want := range []string{"test_output:", "enabled: true", "dir: tmp/suite-dumps", "mirror: /host/dumps"} {
		if !strings.Contains(out, want) {
			t.Errorf("RenderYAML output is missing %q:\n%s", want, out)
		}
	}
	back, err := Load(writeConfig(t, out))
	if err != nil {
		t.Fatalf("Load(RenderYAML output): %v\n%s", err, out)
	}
	if back.TestOutput != raw.TestOutput {
		t.Errorf("round-trip TestOutput = %+v, want %+v", back.TestOutput, raw.TestOutput)
	}
	if got := back.TestOutputDir(); got != "tmp/suite-dumps" {
		t.Errorf("round-trip TestOutputDir() = %q, want tmp/suite-dumps", got)
	}
}

// TestResolveMirror pins the host-path resolution shared by
// bug_filing.mirror and test_output.mirror: empty stays empty (mirroring
// off), ~/… expands against the user's home, an already-absolute path is
// cleaned verbatim, and a relative path is rejected — the error pointing
// at dir for worktree-relative intent.
func TestResolveMirror(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, tc := range []struct{ in, want string }{
		{"", ""},
		{"~/bugs", filepath.Join(home, "bugs")},
		{"/host/bugs", "/host/bugs"},
		// Cleaning normalizes redundant spellings rather than rejecting.
		{"/host//bugs/./x", "/host/bugs/x"},
	} {
		got, err := ResolveMirror("bug_filing mirror", tc.in)
		if err != nil {
			t.Errorf("ResolveMirror(%q) error = %v, want nil", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ResolveMirror(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	_, err := ResolveMirror("bug_filing mirror", "rel/bugs")
	if err == nil || !strings.Contains(err.Error(), "absolute") || !strings.Contains(err.Error(), "dir") {
		t.Errorf("ResolveMirror(%q) error = %v, want a rejection naming the host-path rule and dir", "rel/bugs", err)
	}
}

// TestLoadMirrorValidation pins the load-time mirror gate and its
// asymmetry with dir: a dir's path-safety gate arms only when its section
// is enabled, but a mirror is a host path validated regardless of the
// toggle — a typo fails load even while mirroring is inert, with the
// section named. Accepted shapes (absolute, ~/…) load with the raw value
// intact — resolution is the worker's job, not the load's.
func TestLoadMirrorValidation(t *testing.T) {
	for _, section := range []string{"bug_filing", "test_output"} {
		_, err := Load(writeConfig(t, section+":\n  mirror: rel/bugs\n"))
		if err == nil || !strings.Contains(err.Error(), section) || !strings.Contains(err.Error(), "absolute") {
			t.Errorf("Load(%s mirror: rel) error = %v, want a %s mirror rejection even with the section disabled", section, err, section)
		}
	}
	cfg, err := Load(writeConfig(t, "bug_filing:\n  enabled: true\n  dir: docs/bugs\n  mirror: /host/bugs\ntest_output:\n  enabled: true\n  mirror: ~/dumps\n"))
	if err != nil {
		t.Fatalf("Load (absolute and ~/… mirrors): %v", err)
	}
	if cfg.BugFiling.Mirror != "/host/bugs" {
		t.Errorf("BugFiling.Mirror = %q, want /host/bugs", cfg.BugFiling.Mirror)
	}
	if cfg.TestOutput.Mirror != "~/dumps" {
		t.Errorf("TestOutput.Mirror = %q, want the raw ~/dumps (resolution is the worker's job)", cfg.TestOutput.Mirror)
	}
}

// TestResolveMirrorMountSpecCaps pins the two caps the jail mount puts on
// the resolved host path: the filesystem root is rejected (a root mirror
// would mount a read-write window on the whole host) — including a
// redundant spelling that only cleans down to the root — and so is any
// colon (the mirror is composed into ai-jail's --rw-map <mirror>:<dir>
// mount spec, where a colon would make the SOURCE:DEST split ambiguous),
// including one introduced by tilde expansion.
func TestResolveMirrorMountSpecCaps(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	for _, tc := range []struct{ name, in, wantErr string }{
		{"filesystem root", "/", "filesystem root"},
		{"redundant spelling cleaning to the root", "/host/..", "filesystem root"},
		{"colon in an absolute path", "/host/bugs:1", "rw-map"},
		{"colon introduced by tilde expansion", "~/bugs:1", "rw-map"},
	} {
		_, err := ResolveMirror("bug_filing mirror", tc.in)
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: ResolveMirror(%q) error = %v, want a rejection naming %q", tc.name, tc.in, err, tc.wantErr)
		}
	}
}

// TestLoadBugDirColonGate pins the dir half of the --rw-map SOURCE:DEST
// split: with a mirror configured, a colon in bug_filing.dir is rejected
// at load (dir is composed into the mount spec's DEST half), while the
// same dir without a mirror still loads — unmounted, its bytes render
// verbatim into the round prompts and no mount spec exists to be
// ambiguous.
func TestLoadBugDirColonGate(t *testing.T) {
	_, err := Load(writeConfig(t, "bug_filing:\n  enabled: true\n  dir: back:log\n  mirror: /host/bugs\n"))
	if err == nil || !strings.Contains(err.Error(), "bug_filing") || !strings.Contains(err.Error(), "back:log") {
		t.Errorf("Load(colon dir with mirror) error = %v, want a bug_filing rejection naming the dir", err)
	}
	cfg, err := Load(writeConfig(t, "bug_filing:\n  enabled: true\n  dir: back:log\n"))
	if err != nil {
		t.Fatalf("Load(colon dir without mirror): %v", err)
	}
	if cfg.BugFiling.Dir != "back:log" {
		t.Errorf("BugFiling.Dir = %q, want back:log kept verbatim without a mirror", cfg.BugFiling.Dir)
	}
}

// TestProviderEnvVarsIncludesMirrorEnvs pins that both mirror env vars
// are in the scrub/restore list: a daemon spawn must clear ambient
// values, or an off-config worker would silently inherit a stale shell
// export pointing its rounds at the wrong host directory.
func TestProviderEnvVarsIncludesMirrorEnvs(t *testing.T) {
	for _, env := range []string{BugMirrorEnv, TestOutputMirrorEnv} {
		if !slices.Contains(ProviderEnvVars(), env) {
			t.Errorf("ProviderEnvVars() = %v, want %s listed", ProviderEnvVars(), env)
		}
	}
}

// TestResolveMirrorBareTilde pins the bare "~" spelling: ResolveMirror
// expands it to the home directory, like the shell (a past implementation
// sliced mirror[2:] on the one-byte string and panicked here, which is
// why this lives apart from TestResolveMirror, last in the file — the
// placement keeps any future panic from masking the table's results).
func TestResolveMirrorBareTilde(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	got, err := ResolveMirror("bug_filing mirror", "~")
	if err != nil {
		t.Fatalf("ResolveMirror(\"~\") error = %v, want nil", err)
	}
	if got != home {
		t.Errorf("ResolveMirror(\"~\") = %q, want %q", got, home)
	}
}

// TestAgentEnvReviewerEndpoint pins the reviewer section's env-only
// channel: a set url/key load through and export as the reviewer env vars
// runJailedRound's override reads, while an absent section exports nothing
// — reviewers then share the implementing agent's endpoint exactly as
// before.
func TestAgentEnvReviewerEndpoint(t *testing.T) {
	cfg, err := Load(writeConfig(t, "reviewer:\n  url: https://review.example\n  key: sk-review\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	env := cfg.AgentEnv()
	if got := agentEnvValue(t, env, ReviewerURLEnv); got != "https://review.example" {
		t.Errorf("AgentEnv() %s = %q, want https://review.example", ReviewerURLEnv, got)
	}
	if got := agentEnvValue(t, env, ReviewerKeyEnv); got != "sk-review" {
		t.Errorf("AgentEnv() %s = %q, want sk-review", ReviewerKeyEnv, got)
	}

	cfg, err = Load(writeConfig(t, "anthropic:\n  key: sk-only\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, kv := range cfg.AgentEnv() {
		if strings.HasPrefix(kv, "DAEDALUS_REVIEWER_") {
			t.Errorf("AgentEnv() leaked %q with no reviewer section configured", kv)
		}
	}
}

// TestProviderEnvVarsIncludesReviewerEnvs pins that the reviewer vars are
// in the scrub/restore list: the worker must treat them as config-derived
// exports, or a stale inherited value would arm (or mis-arm) the reviewer
// override next to the config it is supposed to mirror.
func TestProviderEnvVarsIncludesReviewerEnvs(t *testing.T) {
	for _, name := range []string{ReviewerURLEnv, ReviewerKeyEnv} {
		if !slices.Contains(ProviderEnvVars(), name) {
			t.Errorf("ProviderEnvVars() = %v, want %s listed", ProviderEnvVars(), name)
		}
	}
}
