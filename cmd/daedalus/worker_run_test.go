package main

import (
	"embed"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/version"

	"github.com/ozzono/daedalus/internal/config"
)

// pkgSources is this package's own production source, embedded at
// build: the reconciliation scan anchors on it, immune to both the
// process cwd and -trimpath (a runtime.Caller path is module-relative
// under trimpath; a cwd-relative read breaks outside the package dir).
// The embed pattern cannot express the test-file negation — the scan's
// walk skips _test.go names itself.
//
//go:embed all:*.go
var pkgSources embed.FS

// TestWorkerTypePollerSelection pins the run type → poller mapping: the
// empty type starts both pollers (there is deliberately no "all" spelling),
// dev drops the test-queue poller, and test drops the main-queue poller —
// each type always keeps exactly one poller.
func TestWorkerTypePollerSelection(t *testing.T) {
	for _, c := range []struct {
		workerType   string
		wantPipeline bool
		wantTest     bool
	}{
		{"", true, true},
		{workerTypeDev, true, false},
		{workerTypeTest, false, true},
	} {
		if got := wantsPipelineWorker(c.workerType); got != c.wantPipeline {
			t.Errorf("wantsPipelineWorker(%q) = %v, want %v", c.workerType, got, c.wantPipeline)
		}
		if got := wantsTestWorker(c.workerType); got != c.wantTest {
			t.Errorf("wantsTestWorker(%q) = %v, want %v", c.workerType, got, c.wantTest)
		}
	}
}

// TestPublishWorkerStatusRecordsVersion pins the publish side of the
// VERSION column: the status file a worker writes names the version of the
// binary doing the writing (with its pid), so `worker status` can show a
// daemon still running a pre-upgrade build. A closed stop channel makes
// the publisher do its one up-front write and return synchronously.
func TestPublishWorkerStatusRecordsVersion(t *testing.T) {
	useDaemonDir(t)
	stop := make(chan struct{})
	close(stop)
	publishWorkerStatus("pub", "q9", stop)

	data, err := os.ReadFile(filepath.Join(daemonDir, "worker-pub.status"))
	if err != nil {
		t.Fatalf("read published status: %v", err)
	}
	var st workerStatus
	if err := json.Unmarshal(data, &st); err != nil {
		t.Fatalf("parse published status %s: %v", data, err)
	}
	if st.Version != version.String() {
		t.Errorf("published version = %q, want the running build %q", st.Version, version.String())
	}
	if st.PID != os.Getpid() {
		t.Errorf("published pid = %d, want this process's %d", st.PID, os.Getpid())
	}
}

// restoreProcessEnv snapshots the named variables and restores them at
// cleanup, so runWorker's exports cannot leak into sibling tests.
func restoreProcessEnv(t *testing.T, names ...string) {
	t.Helper()
	old := make(map[string]string, len(names))
	for _, n := range names {
		old[n] = os.Getenv(n)
	}
	t.Cleanup(func() {
		for _, n := range names {
			if v := old[n]; v != "" {
				os.Setenv(n, v)
			} else {
				os.Unsetenv(n)
			}
		}
	})
}

// TestRunWorkerSlimEnvExports pins the tri-state env channels at worker
// startup: a slim config exports DAEDALUS_SLIM=1, an explicit thinking:
// false exports DAEDALUS_THINKING=off (the sole thinking value daedalus
// ever sends), an explicit stream: true/false exports DAEDALUS_STREAM=
// on/off, and a config that says off/absent clears each. The daemon spawn
// scrub (config.AllWorkerEnvVars) covers all of them, and these set/unset
// branches are the second path guarding the same hazard — neither
// redundant: the scrub clears the daemon's inherited env at spawn, the
// branches clear what a process that kept the inherit semantics (worker
// foreground) carried in. The slim tool relay's
// DAEDALUS_TOOL_RELAY_URL obeys the same tri-state rule and additionally
// needs slim.parser_model plus the openai section. runWorker is driven to
// its first failure (an empty task queue fails worktree preflight before
// any Temporal dial) purely to observe the exports.
func TestRunWorkerSlimEnvExports(t *testing.T) {
	// API_TIMEOUT_MS is in the list because a zero config still exports it
	// (AgentEnv's strconv.Itoa(0) passes the non-empty gate) — a value
	// production configs can never produce, since Load always defaults
	// timeout_ms. The OPENAI vars are in it because the relay segment
	// below sets openai.url, which AgentEnv exports.
	restoreProcessEnv(t, "DAEDALUS_SLIM", "DAEDALUS_THINKING", "DAEDALUS_STREAM", "DAEDALUS_AGENT",
		"DAEDALUS_MAX_CONCURRENT_AGENT_RUNS", "DAEDALUS_MAX_CONCURRENT_TESTS", "DAEDALUS_WORKER_NAME",
		"API_TIMEOUT_MS", config.ToolRelayURLEnv, "OPENAI_BASE_URL", "OPENAI_API_BASE")

	// Stale exports must not outlive a config that says off.
	t.Setenv("DAEDALUS_SLIM", "1")
	t.Setenv("DAEDALUS_THINKING", "off")
	t.Setenv("DAEDALUS_STREAM", "on")
	t.Setenv(config.ToolRelayURLEnv, "http://127.0.0.1:1/stale-relay")
	if err := runWorker(config.Config{Metrics: config.MetricsConfig{Enabled: boolPtr(false)}}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	if got := os.Getenv("DAEDALUS_SLIM"); got != "" {
		t.Errorf("DAEDALUS_SLIM = %q after a slim-off config, want the stale export cleared", got)
	}
	if got := os.Getenv("DAEDALUS_THINKING"); got != "" {
		t.Errorf("DAEDALUS_THINKING = %q after a thinking-on config, want the stale export cleared", got)
	}
	if got := os.Getenv("DAEDALUS_STREAM"); got != "" {
		t.Errorf("DAEDALUS_STREAM = %q after a stream-absent config, want the stale export cleared", got)
	}
	if got := os.Getenv(config.ToolRelayURLEnv); got != "" {
		t.Errorf("%s = %q after a relay-off config, want the stale export cleared", config.ToolRelayURLEnv, got)
	}

	// The on-config exports all three — but slim alone never starts the
	// relay: without a parser model (and an openai upstream) the staged
	// URL stays unset, the pre-relay behavior.
	off := false
	if err := runWorker(config.Config{Slim: config.SlimConfig{Enabled: true}, Thinking: &off, Metrics: config.MetricsConfig{Enabled: boolPtr(false)}}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	if got := os.Getenv("DAEDALUS_SLIM"); got != "1" {
		t.Errorf("DAEDALUS_SLIM = %q after a slim config, want 1", got)
	}
	if got := os.Getenv("DAEDALUS_THINKING"); got != "off" {
		t.Errorf("DAEDALUS_THINKING = %q after a thinking-false config, want off", got)
	}
	if got := os.Getenv(config.ToolRelayURLEnv); got != "" {
		t.Errorf("%s = %q after a slim config without parser_model, want it unset (relay off)", config.ToolRelayURLEnv, got)
	}

	// The full relay arming — slim enabled, a parser model, and an openai
	// upstream — starts the loopback relay and stages its URL: the relay's
	// own 127.0.0.1 port plus the upstream's path prefix, so pi's request
	// paths land on the relay exactly as they would land on the upstream.
	if err := runWorker(config.Config{
		Slim:     config.SlimConfig{Enabled: true, ParserModel: "qwen2.5-coder:3b-parser"},
		OpenAI:   config.OpenAIConfig{URL: "http://127.0.0.1:11434/v1"},
		Thinking: &off,
		Metrics:  config.MetricsConfig{Enabled: boolPtr(false)},
	}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	staged, err := url.Parse(os.Getenv(config.ToolRelayURLEnv))
	if err != nil {
		t.Fatalf("parse staged %s: %v", config.ToolRelayURLEnv, err)
	}
	if staged.Scheme != "http" || staged.Hostname() != "127.0.0.1" {
		t.Errorf("staged relay URL %q, want a loopback http address", staged)
	}
	if staged.Path != "/v1" {
		t.Errorf("staged relay URL %q, want the upstream's /v1 path prefix preserved", staged)
	}

	// The stream toggle exports both spellings, and an explicit false is
	// an export, not a clear.
	for _, c := range []struct {
		stream bool
		want   string
	}{
		{true, "on"},
		{false, "off"},
	} {
		if err := runWorker(config.Config{Stream: &c.stream, Metrics: config.MetricsConfig{Enabled: boolPtr(false)}}, ""); err == nil {
			t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
		}
		if got := os.Getenv("DAEDALUS_STREAM"); got != c.want {
			t.Errorf("DAEDALUS_STREAM = %q after stream: %v, want %q", got, c.stream, c.want)
		}
	}
}

// TestRunWorkerReviewerSplitEnvExports pins the reviewer-split env channel
// at worker startup: a reviewer.model export travels as
// DAEDALUS_REVIEWER_MODEL (exported only when set — scrubbed from the
// daemon spawn via config.ProviderEnvVars, and unset symmetrically on the
// foreground path, where no spawn scrub runs, so a stale shell export
// cannot retarget a config that says nothing), and the two
// wire-level thinking knobs export as DAEDALUS_OPENAI_THINKING=off /
// DAEDALUS_REVIEWER_THINKING=off — only an explicit disable_thinking: true
// sends a signal (never-send-a-default), and like the toggle family they
// mirror (DAEDALUS_SLIM/THINKING/STREAM) they unset symmetrically. The
// openai-side value reads the projected openai channel (ProviderChannels),
// so a provider-typed section's knob governs exactly like the legacy
// openai section's. runWorker is driven to its first failure (an empty
// task queue fails worktree preflight before any Temporal dial) purely to
// observe the exports.
func TestRunWorkerReviewerSplitEnvExports(t *testing.T) {
	restoreProcessEnv(t, config.ReviewerModelEnv, config.OpenAIThinkingEnv, config.ReviewerThinkingEnv,
		"DAEDALUS_AGENT", "DAEDALUS_MAX_CONCURRENT_AGENT_RUNS", "DAEDALUS_MAX_CONCURRENT_TESTS", "DAEDALUS_WORKER_NAME")

	// Stale exports must not outlive a config that says nothing — the
	// set-or-unset symmetry, the reviewer model included.
	t.Setenv(config.OpenAIThinkingEnv, "off")
	t.Setenv(config.ReviewerThinkingEnv, "off")
	t.Setenv(config.ReviewerModelEnv, "stale-review-model")
	if err := runWorker(config.Config{Metrics: config.MetricsConfig{Enabled: boolPtr(false)}}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	if got := os.Getenv(config.OpenAIThinkingEnv); got != "" {
		t.Errorf("%s = %q after an unconfigured config, want the stale export cleared", config.OpenAIThinkingEnv, got)
	}
	if got := os.Getenv(config.ReviewerThinkingEnv); got != "" {
		t.Errorf("%s = %q after an unconfigured config, want the stale export cleared", config.ReviewerThinkingEnv, got)
	}
	if got := os.Getenv(config.ReviewerModelEnv); got != "" {
		t.Errorf("%s = %q after an unconfigured config, want the stale export cleared", config.ReviewerModelEnv, got)
	}

	// The set branches: a reviewer model rides its own var, each knob arms
	// only its own role's signal, and the two never merge.
	if err := runWorker(config.Config{
		Reviewer: config.RoleOverrideConfig{Model: "qwen3-review", DisableThinking: true},
		OpenAI:   config.OpenAIConfig{DisableThinking: true},
		Metrics:  config.MetricsConfig{Enabled: boolPtr(false)},
	}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	if got := os.Getenv(config.ReviewerModelEnv); got != "qwen3-review" {
		t.Errorf("%s = %q after a reviewer.model config, want qwen3-review", config.ReviewerModelEnv, got)
	}
	if got := os.Getenv(config.OpenAIThinkingEnv); got != "off" {
		t.Errorf("%s = %q after an openai disable_thinking config, want off", config.OpenAIThinkingEnv, got)
	}
	if got := os.Getenv(config.ReviewerThinkingEnv); got != "off" {
		t.Errorf("%s = %q after a reviewer disable_thinking config, want off", config.ReviewerThinkingEnv, got)
	}

	// A provider-typed openai section governs the openai-side knob through
	// the projected channel; the reviewer side stays silent on its own.
	if err := runWorker(config.Config{
		Provider: &config.ProviderConfig{Type: config.FallbackTypeOpenAI, URL: "http://127.0.0.1:11434/v1", DisableThinking: true},
		Metrics:  config.MetricsConfig{Enabled: boolPtr(false)},
	}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	if got := os.Getenv(config.OpenAIThinkingEnv); got != "off" {
		t.Errorf("%s = %q after a provider-typed disable_thinking config, want off (the projected channel governs)", config.OpenAIThinkingEnv, got)
	}
	if got := os.Getenv(config.ReviewerThinkingEnv); got != "" {
		t.Errorf("%s = %q with no reviewer disable_thinking, want it unset", config.ReviewerThinkingEnv, got)
	}

	// The reviewer knob alone arms only the reviewer signal.
	if err := runWorker(config.Config{Reviewer: config.RoleOverrideConfig{DisableThinking: true}, Metrics: config.MetricsConfig{Enabled: boolPtr(false)}}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	if got := os.Getenv(config.ReviewerThinkingEnv); got != "off" {
		t.Errorf("%s = %q after a reviewer-only disable_thinking config, want off", config.ReviewerThinkingEnv, got)
	}
	if got := os.Getenv(config.OpenAIThinkingEnv); got != "" {
		t.Errorf("%s = %q with no openai disable_thinking, want it unset", config.OpenAIThinkingEnv, got)
	}
}

// TestRunWorkerRoleOverrideEnvExports pins the per-role provider override
// channel at worker startup (runWorker's dynamic os.Setenv loop over
// config.RoleOverrideEnv, the site TestRunWorkerExportsAreScrubbed
// re-anchored to cover beside the AgentEnv loop): every field set travels
// as its own var (dev and final_reviewer url/key/model/context_tokens and
// the off-only thinking knobs, the reviewer section's context window), a
// subset config exports only its set fields, and the whole family unsets
// symmetrically — a stale shell export of any var must not outlive a
// config that says nothing, or it would retarget a role's rounds behind
// the config's back. runWorker is driven to its first failure (an empty
// task queue fails worktree preflight before any Temporal dial) purely to
// observe the exports.
func TestRunWorkerRoleOverrideEnvExports(t *testing.T) {
	restoreProcessEnv(t, config.RoleOverrideEnvVars()...)

	// Stale exports of the whole family must not outlive a config that
	// says nothing — the set-or-unset symmetry the fallback chain rides on.
	for _, name := range config.RoleOverrideEnvVars() {
		t.Setenv(name, "stale")
	}
	if err := runWorker(config.Config{Metrics: config.MetricsConfig{Enabled: boolPtr(false)}}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	for _, name := range config.RoleOverrideEnvVars() {
		if got := os.Getenv(name); got != "" {
			t.Errorf("%s = %q after an unconfigured config, want the stale export cleared", name, got)
		}
	}

	// The set branches: every configured field rides its own var, and the
	// thinking knobs export only the off value.
	if err := runWorker(config.Config{
		Dev:           config.RoleOverrideConfig{URL: "https://dev.example", Key: "sk-dev", Model: "qwen3-dev", ContextTokens: 131072, DisableThinking: true},
		Reviewer:      config.RoleOverrideConfig{ContextTokens: 65536},
		FinalReviewer: config.RoleOverrideConfig{URL: "https://final.example", Model: "qwen3-final"},
		Metrics:       config.MetricsConfig{Enabled: boolPtr(false)},
	}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	for name, wantVal := range map[string]string{
		config.DevURLEnv:                "https://dev.example",
		config.DevKeyEnv:                "sk-dev",
		config.DevModelEnv:              "qwen3-dev",
		config.DevContextTokensEnv:      "131072",
		config.DevThinkingEnv:           "off",
		config.ReviewerContextTokensEnv: "65536",
		config.FinalReviewerURLEnv:      "https://final.example",
		config.FinalReviewerModelEnv:    "qwen3-final",
	} {
		if got := os.Getenv(name); got != wantVal {
			t.Errorf("%s = %q, want %q", name, got, wantVal)
		}
	}
	// Unset final_reviewer fields export nothing — the fallback chain's
	// absent-means-fall-back contract, not an empty-string signal.
	for _, name := range []string{config.FinalReviewerKeyEnv, config.FinalReviewerContextTokensEnv, config.FinalReviewerThinkingEnv} {
		if got := os.Getenv(name); got != "" {
			t.Errorf("%s = %q with the field unset, want it unset (absent means fall back)", name, got)
		}
	}
}

// TestRunWorkerRelayProviderUpstream pins the relay's upstream resolution
// through the provider channels: an openai-typed provider section serves as
// the relay's upstream exactly as the legacy openai section does (the
// staged URL keeps the upstream's path prefix), an anthropic-typed provider
// leaves the relay off (its wire names no openai channel), and the
// cross-wire mix arms the relay from the legacy openai section.
func TestRunWorkerRelayProviderUpstream(t *testing.T) {
	restoreProcessEnv(t, "DAEDALUS_SLIM", config.ToolRelayURLEnv,
		"OPENAI_BASE_URL", "OPENAI_API_BASE", "OPENAI_API_KEY", "OPENAI_MODEL", "API_TIMEOUT_MS")

	// An openai-typed provider: the relay arms on the section's url.
	// Metrics is opted out explicitly: the default is on, and these pins
	// are about the relay staging, not the metrics boot gate.
	if err := runWorker(config.Config{
		Slim:     config.SlimConfig{Enabled: true, ParserModel: "qwen2.5-coder:3b-parser"},
		Provider: &config.ProviderConfig{Type: config.FallbackTypeOpenAI, URL: "http://127.0.0.1:11434/v1", Key: "k"},
		Metrics:  config.MetricsConfig{Enabled: boolPtr(false)},
	}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	staged, err := url.Parse(os.Getenv(config.ToolRelayURLEnv))
	if err != nil {
		t.Fatalf("parse staged %s: %v", config.ToolRelayURLEnv, err)
	}
	if staged.Scheme != "http" || staged.Hostname() != "127.0.0.1" || staged.Path != "/v1" {
		t.Errorf("staged relay URL %q, want the loopback relay keeping the provider url's /v1 prefix", staged)
	}

	// An anthropic-typed provider: no openai-channel upstream, the relay
	// stays off and the staged URL unset — the pre-relay behavior.
	if err := runWorker(config.Config{
		Slim:     config.SlimConfig{Enabled: true, ParserModel: "qwen2.5-coder:3b-parser"},
		Provider: &config.ProviderConfig{Type: config.FallbackTypeAnthropic, URL: "http://127.0.0.1:8082", Key: "k"},
		Metrics:  config.MetricsConfig{Enabled: boolPtr(false)},
	}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	if got := os.Getenv(config.ToolRelayURLEnv); got != "" {
		t.Errorf("%s = %q after an anthropic-typed provider, want it unset (relay off)", config.ToolRelayURLEnv, got)
	}

	// The cross-wire mix: the legacy openai section still arms the relay.
	if err := runWorker(config.Config{
		Slim:     config.SlimConfig{Enabled: true, ParserModel: "qwen2.5-coder:3b-parser"},
		Provider: &config.ProviderConfig{Type: config.FallbackTypeAnthropic, URL: "http://127.0.0.1:8082", Key: "k"},
		OpenAI:   config.OpenAIConfig{URL: "http://127.0.0.1:11434/v1"},
		Metrics:  config.MetricsConfig{Enabled: boolPtr(false)},
	}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	staged, err = url.Parse(os.Getenv(config.ToolRelayURLEnv))
	if err != nil {
		t.Fatalf("parse staged %s: %v", config.ToolRelayURLEnv, err)
	}
	if staged.Path != "/v1" {
		t.Errorf("staged relay URL %q, want the legacy openai upstream's /v1 prefix", staged)
	}
}

// TestRunWorkerBugDirEnvExports pins the bug-filing toggle's env channel:
// DAEDALUS_BUG_DIR (the effective bug_filing.dir, default applied) is
// exported only when filing is enabled — absent means off — and a stale
// export in the invoking shell is cleared by a config that says off, so
// it can never re-enable filing.
func TestRunWorkerBugDirEnvExports(t *testing.T) {
	restoreProcessEnv(t, "DAEDALUS_SLIM", "DAEDALUS_THINKING", "DAEDALUS_STREAM", "DAEDALUS_BUG_DIR")

	// A stale export must not outlive a config that says off.
	t.Setenv("DAEDALUS_BUG_DIR", "stale/bugs")
	if err := runWorker(config.Config{Metrics: config.MetricsConfig{Enabled: boolPtr(false)}}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	if got := os.Getenv("DAEDALUS_BUG_DIR"); got != "" {
		t.Errorf("DAEDALUS_BUG_DIR = %q after an off config, want the stale export cleared", got)
	}

	// Enabled without an explicit dir exports the historical default;
	// an explicit dir exports verbatim.
	for _, c := range []struct {
		cfg  config.Config
		want string
	}{
		{config.Config{BugFiling: config.BugFilingConfig{Enabled: true}, Metrics: config.MetricsConfig{Enabled: boolPtr(false)}}, config.DefaultBugDir},
		{config.Config{BugFiling: config.BugFilingConfig{Enabled: true, Dir: "docs/known-bugs"}, Metrics: config.MetricsConfig{Enabled: boolPtr(false)}}, "docs/known-bugs"},
	} {
		if err := runWorker(c.cfg, ""); err == nil {
			t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
		}
		if got := os.Getenv("DAEDALUS_BUG_DIR"); got != c.want {
			t.Errorf("DAEDALUS_BUG_DIR = %q after an enabled config, want %q", got, c.want)
		}
	}
}

// TestRunWorkerMirrorEnvExports pins the host-mirror env channel:
// DAEDALUS_BUG_MIRROR and DAEDALUS_TEST_OUTPUT_MIRROR carry the resolved
// bug_filing.mirror / test_output.mirror only when their section is
// enabled AND a mirror is configured — absent means mirroring off, and a
// stale shell export is cleared by a config that says off. The ~/… shape
// is resolved by the worker (against its home), and a relative mirror
// fails the worker startup outright with the resolver's rejection.
func TestRunWorkerMirrorEnvExports(t *testing.T) {
	restoreProcessEnv(t, "DAEDALUS_BUG_MIRROR", "DAEDALUS_TEST_OUTPUT_MIRROR")

	// A stale export must not outlive a config that says off.
	t.Setenv("DAEDALUS_BUG_MIRROR", "stale/host")
	t.Setenv("DAEDALUS_TEST_OUTPUT_MIRROR", "stale/host")
	if err := runWorker(config.Config{Metrics: config.MetricsConfig{Enabled: boolPtr(false)}}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	for _, env := range []string{"DAEDALUS_BUG_MIRROR", "DAEDALUS_TEST_OUTPUT_MIRROR"} {
		if got := os.Getenv(env); got != "" {
			t.Errorf("%s = %q after an off config, want the stale export cleared", env, got)
		}
	}

	home := t.TempDir()
	t.Setenv("HOME", home)
	// The on-config exports both mirrors — the absolute one verbatim, the
	// ~/… one resolved against the worker's home. A section enabled
	// without a mirror, and a mirror configured while its section is
	// disabled, both keep the env absent.
	cfg := config.Config{
		BugFiling:  config.BugFilingConfig{Enabled: true, Dir: "docs/bugs", Mirror: "/host/bugs"},
		TestOutput: config.TestOutputConfig{Enabled: true, Dir: "dumps", Mirror: "~/mirror"},
		Metrics:    config.MetricsConfig{Enabled: boolPtr(false)},
	}
	if err := runWorker(cfg, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	if got := os.Getenv("DAEDALUS_BUG_MIRROR"); got != "/host/bugs" {
		t.Errorf("DAEDALUS_BUG_MIRROR = %q, want /host/bugs", got)
	}
	if got := os.Getenv("DAEDALUS_TEST_OUTPUT_MIRROR"); got != filepath.Join(home, "mirror") {
		t.Errorf("DAEDALUS_TEST_OUTPUT_MIRROR = %q, want %q", got, filepath.Join(home, "mirror"))
	}

	// Enabled without a mirror: the env stays absent.
	if err := runWorker(config.Config{
		BugFiling:  config.BugFilingConfig{Enabled: true, Dir: "docs/bugs"},
		TestOutput: config.TestOutputConfig{Enabled: true, Dir: "dumps"},
		Metrics:    config.MetricsConfig{Enabled: boolPtr(false)},
	}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	for _, env := range []string{"DAEDALUS_BUG_MIRROR", "DAEDALUS_TEST_OUTPUT_MIRROR"} {
		if got := os.Getenv(env); got != "" {
			t.Errorf("%s = %q with no mirror configured, want it absent", env, got)
		}
	}

	// A mirror configured while its section is disabled: absent, whatever
	// the mirror's shape.
	if err := runWorker(config.Config{
		BugFiling:  config.BugFilingConfig{Mirror: "/host/bugs"},
		TestOutput: config.TestOutputConfig{Mirror: "~/mirror"},
		Metrics:    config.MetricsConfig{Enabled: boolPtr(false)},
	}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	for _, env := range []string{"DAEDALUS_BUG_MIRROR", "DAEDALUS_TEST_OUTPUT_MIRROR"} {
		if got := os.Getenv(env); got != "" {
			t.Errorf("%s = %q with the section disabled, want it absent", env, got)
		}
	}

	// A relative mirror fails startup at resolve, before preflight —
	// the error names the resolver, distinguishing it from the empty-queue
	// preflight failure every case above rides on.
	_, relErr := config.ResolveMirror("bug_filing mirror", "rel/bugs")
	if err := runWorker(config.Config{
		BugFiling: config.BugFilingConfig{Enabled: true, Mirror: "rel/bugs"},
		Metrics:   config.MetricsConfig{Enabled: boolPtr(false)},
	}, ""); err == nil || !strings.Contains(err.Error(), "resolve bug_filing.mirror") || err.Error() != "resolve bug_filing.mirror: "+relErr.Error() {
		t.Errorf("runWorker (relative bug mirror) error = %v, want the resolver's rejection wrapped under resolve bug_filing.mirror", err)
	}
}

// TestRunWorkerExportsAreScrubbed pins the add-direction reconciliation
// between the worker daemon's env exports and the test-suite scrub
// lists: every env name the non-test sources of this package set must
// be in config.AllWorkerEnvVars, else a new export rides the jailed
// os.Environ() passthrough into every jailed `go test` — green on dev
// hosts and CI, failing only on a serving worker. This scans the
// sources' os.Setenv call sites instead of executing runWorker against
// a representative config: most export sites sit behind metrics/daemon
// liveness gates a sandboxed subprocess cannot reach, so a behavioral
// probe would silently under-observe while this static walk sees every
// site. The walk is a go/ast parse of every non-test file in the
// package, embedded at build (pkgSources) so the scan is immune to both
// the process cwd and -trimpath — a text scan would misread a call
// wrapped across lines as absent, a comment mentioning os.Setenv as a
// site, and a cwd/trimpath-relative read as an empty package. Receiver
// idents resolve through the file's import table (any import whose path
// is os, named or aliased); a .Setenv on an unknown receiver fails
// loudly. Coverage by first-argument shape: a string literal is checked
// directly; a config-package env constant is resolved through envConsts;
// any other expression fails unless it is one of the two pinned dynamic
// sites — runWorker's AgentEnv application loop and its RoleOverrideEnv
// application loop (both pairs of ident arguments key, value), anchored
// by enclosing function, so a rename, a move to a helper, or a third
// dynamic site fails loudly and forces a re-anchor (and a scrub-list
// decision) instead of passing silently. The AgentEnv site is safe by
// construction because TestAgentEnvKeysAreProviderEnvVars
// (internal/config) pins AgentEnv's whole key family into
// ProviderEnvVars; the RoleOverrideEnv site's twin is
// TestRoleOverrideEnvExports (internal/config), which pins
// RoleOverrideEnvVars into ProviderEnvVars and a fully-set config's
// emitted keys onto that family exactly. Package-level func-literal
// bodies — direct or nested
// in composite literals, the outermost walked per literal — run at init
// and are real export sites, under the same rules; a dynamic one there
// can never be the pinned loop. A name assembled at runtime beyond that
// loop would evade this scan. This scan and the AgentEnv key scan in
// internal/config/config_test.go are deliberate twins — the same
// go/ast machinery twice rather than a shared internal package at this
// size; change both or fold them later.
func TestRunWorkerExportsAreScrubbed(t *testing.T) {
	// envConsts resolves the os.Setenv call sites whose first argument
	// is a config-package env constant.
	envConsts := map[string]string{
		"MetricsRelayAnthropicEnv": config.MetricsRelayAnthropicEnv,
		"MetricsRelayOpenAIEnv":    config.MetricsRelayOpenAIEnv,
		"MetricsRelayTokenEnv":     config.MetricsRelayTokenEnv,
		"ToolRelayURLEnv":          config.ToolRelayURLEnv,
		"BugDirEnv":                config.BugDirEnv,
		"BugMirrorEnv":             config.BugMirrorEnv,
		"TestOutputMirrorEnv":      config.TestOutputMirrorEnv,
	}
	const dynamicFunc = "runWorker"
	scrubbed := config.AllWorkerEnvVars()
	dynamics := 0
	// checkSetenv inspects one os.Setenv call found in fname's scope:
	// the receiver must be the file's os import, string-literal and
	// config-constant first arguments are resolved against the scrub
	// set, and any other expression is the single pinned dynamic site.
	checkSetenv := func(fname string, fset *token.FileSet, srcName string, imports map[string]string, call *ast.CallExpr) {
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return
		}
		if sel.Sel.Name != "Setenv" {
			return
		}
		recv, ok := sel.X.(*ast.Ident)
		if !ok {
			return
		}
		ipath, isImport := imports[recv.Name]
		if !isImport {
			t.Fatalf("%s: .Setenv on unknown receiver %s — resolve the receiver or extend the scan", srcName, recv.Name)
		}
		if ipath != "os" {
			return // another package's Setenv
		}
		if len(call.Args) == 0 {
			t.Fatalf("%s: os.Setenv without arguments", srcName)
		}
		requireScrubbed := func(envName string) {
			if !slices.Contains(scrubbed, envName) {
				t.Errorf("%s sets %s, want it listed in config.AllWorkerEnvVars() so the test scrubs clear it", srcName, envName)
			}
		}
		switch arg := call.Args[0].(type) {
		case *ast.BasicLit:
			if arg.Kind != token.STRING {
				t.Fatalf("%s: os.Setenv first argument is a %s literal, not a string — resolve by hand", srcName, arg.Kind)
			}
			lit, err := strconv.Unquote(arg.Value)
			if err != nil {
				t.Fatalf("%s: os.Setenv string literal %s does not unquote: %v", srcName, arg.Value, err)
			}
			requireScrubbed(lit)
		case *ast.SelectorExpr:
			pkg, ok := arg.X.(*ast.Ident)
			if !ok || pkg.Name != "config" {
				t.Fatalf("%s: os.Setenv first argument at %s is a selector the scan does not resolve — extend envConsts or the scrub lists", srcName, fset.Position(arg.Pos()))
			}
			resolved, ok := envConsts[arg.Sel.Name]
			if !ok {
				t.Fatalf("%s sets config.%s — an env constant the scan does not resolve; extend envConsts (and the scrub lists, if new)", srcName, arg.Sel.Name)
			}
			requireScrubbed(resolved)
		default:
			dynamics++
			first, ok := arg.(*ast.Ident)
			// a 1-argument os.Setenv lookalike (a build-tag-excluded file
			// is parsed even though it never compiles) must hit this
			// re-anchor failure, not an index panic
			second, ok2 := (*ast.Ident)(nil), false
			if len(call.Args) >= 2 {
				second, ok2 = call.Args[1].(*ast.Ident)
			}
			if fname != dynamicFunc || !ok || !ok2 || first.Name != "key" || second.Name != "value" {
				t.Fatalf("%s: dynamic os.Setenv in %s — outside the two pinned export loops (AgentEnv, RoleOverrideEnv) in %s; extend the scrub lists and re-anchor the pin", srcName, fname, dynamicFunc)
			}
		}
	}
	// walk the embedded package sources; the embed pattern cannot
	// express the test-file negation, so the walk skips _test.go itself
	entries, err := fs.ReadDir(pkgSources, ".")
	if err != nil {
		t.Fatalf("list embedded package sources: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		srcBytes, err := fs.ReadFile(pkgSources, name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, name, srcBytes, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		// the file's import table: local name → import path, so an
		// aliased os import is recognized and a foreign .Setenv is loud
		imports := map[string]string{}
		for _, imp := range file.Imports {
			ipath := strings.Trim(imp.Path.Value, `"`)
			local := ipath[strings.LastIndexByte(ipath, '/')+1:]
			if imp.Name != nil {
				local = imp.Name.Name
			}
			imports[local] = ipath
		}
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Body == nil {
					continue
				}
				ast.Inspect(d.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					checkSetenv(d.Name.Name, fset, name, imports, call)
					return true
				})
			case *ast.GenDecl:
				// package-level func literals run at init: an os.Setenv
				// inside one is a real export site, walked under the same
				// rules (a dynamic one can never be the pinned loop).
				// FuncLits nested in composite literals are collected by
				// subtree walk; only outermost literals are walked so a
				// nested one is not counted twice.
				for _, spec := range d.Specs {
					vs, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for _, value := range vs.Values {
						var lits []*ast.FuncLit
						ast.Inspect(value, func(n ast.Node) bool {
							if lit, ok := n.(*ast.FuncLit); ok {
								lits = append(lits, lit)
							}
							return true
						})
						for _, lit := range lits {
							contained := false
							for _, outer := range lits {
								if outer != lit && outer.Pos() < lit.Pos() && lit.End() < outer.End() {
									contained = true
									break
								}
							}
							if contained {
								continue
							}
							owner := vs.Names[0].Name + " (package var)"
							ast.Inspect(lit.Body, func(n ast.Node) bool {
								call, ok := n.(*ast.CallExpr)
								if !ok {
									return true
								}
								checkSetenv(owner, fset, name, imports, call)
								return true
							})
						}
					}
				}
			}
		}
	}
	if dynamics != 2 {
		t.Errorf("scanned package sources carry %d dynamic os.Setenv sites, want exactly the two pinned export loops (AgentEnv, RoleOverrideEnv) in %s", dynamics, dynamicFunc)
	}
}
