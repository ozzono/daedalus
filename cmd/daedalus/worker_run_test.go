package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/version"

	"github.com/ozzono/daedalus/internal/config"
)

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
// on/off, and a config that says off/absent clears each — none of the vars
// is in the daemon spawn scrub, so a stale export in the invoking shell
// must not survive into the daemon and silently beat the config. runWorker
// is driven to its first failure (an empty task queue fails worktree
// preflight before any Temporal dial) purely to observe the exports.
func TestRunWorkerSlimEnvExports(t *testing.T) {
	// API_TIMEOUT_MS is in the list because a zero config still exports it
	// (AgentEnv's strconv.Itoa(0) passes the non-empty gate) — a value
	// production configs can never produce, since Load always defaults
	// timeout_ms.
	restoreProcessEnv(t, "DAEDALUS_SLIM", "DAEDALUS_THINKING", "DAEDALUS_STREAM", "DAEDALUS_AGENT",
		"DAEDALUS_MAX_CONCURRENT_AGENT_RUNS", "DAEDALUS_MAX_CONCURRENT_TESTS", "DAEDALUS_WORKER_NAME",
		"API_TIMEOUT_MS")

	// Stale exports must not outlive a config that says off.
	t.Setenv("DAEDALUS_SLIM", "1")
	t.Setenv("DAEDALUS_THINKING", "off")
	t.Setenv("DAEDALUS_STREAM", "on")
	if err := runWorker(config.Config{}, ""); err == nil {
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

	// The on-config exports all three.
	off := false
	if err := runWorker(config.Config{Slim: true, Thinking: &off}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	if got := os.Getenv("DAEDALUS_SLIM"); got != "1" {
		t.Errorf("DAEDALUS_SLIM = %q after a slim config, want 1", got)
	}
	if got := os.Getenv("DAEDALUS_THINKING"); got != "off" {
		t.Errorf("DAEDALUS_THINKING = %q after a thinking-false config, want off", got)
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
		if err := runWorker(config.Config{Stream: &c.stream}, ""); err == nil {
			t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
		}
		if got := os.Getenv("DAEDALUS_STREAM"); got != c.want {
			t.Errorf("DAEDALUS_STREAM = %q after stream: %v, want %q", got, c.stream, c.want)
		}
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
	if err := runWorker(config.Config{}, ""); err == nil {
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
		{config.Config{BugFiling: config.BugFilingConfig{Enabled: true}}, config.DefaultBugDir},
		{config.Config{BugFiling: config.BugFilingConfig{Enabled: true, Dir: "docs/known-bugs"}}, "docs/known-bugs"},
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
	if err := runWorker(config.Config{}, ""); err == nil {
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
	}, ""); err == nil || !strings.Contains(err.Error(), "resolve bug_filing.mirror") || err.Error() != "resolve bug_filing.mirror: "+relErr.Error() {
		t.Errorf("runWorker (relative bug mirror) error = %v, want the resolver's rejection wrapped under resolve bug_filing.mirror", err)
	}
}
