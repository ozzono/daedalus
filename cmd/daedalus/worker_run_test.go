package main

import (
	"encoding/json"
	"os"
	"path/filepath"
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
