package config

import (
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
}

// TestLoadAgent pins the accepted agent values: every shipped agent loads,
// anything else is rejected with the available choices named.
func TestLoadAgent(t *testing.T) {
	for _, agent := range []string{"claude", "opencode", "amp"} {
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
  timeout_ms: 60000
openai:
  url: https://oa.example/v1
  key: sk-oa-test
  model: gpt-test
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
	want := []string{
		"ANTHROPIC_BASE_URL=https://proxy.example",
		"ANTHROPIC_API_KEY=sk-ant-test",
		"ANTHROPIC_MODEL=claude-opus-5",
		"API_TIMEOUT_MS=60000",
		"OPENAI_BASE_URL=https://oa.example/v1",
		"OPENAI_API_KEY=sk-oa-test",
		"OPENAI_MODEL=gpt-test",
	}
	if !slices.Equal(env, want) {
		t.Errorf("AgentEnv() = %v, want %v", env, want)
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
