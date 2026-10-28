package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ozzono/daedalus/internal/config"
)

// TestIsDashForegroundArgv pins the argv shape the dash orphan sweep
// identifies: the consecutive dash/foreground tokens, wherever they sit in
// the daemon's command line (the re-exec carries flags before and after
// the pair) — and never a flag-pried or merely coincidental shape, and
// never the worker daemon's own foreground line, the sweep's named
// exclusion: the two daemons share the binary basename, so the argv pair
// is what keeps the dash sweep off the worker.
func TestIsDashForegroundArgv(t *testing.T) {
	for _, c := range []struct {
		args []string
		want bool
	}{
		{[]string{"dash", "foreground"}, true},
		// The daemon's own re-exec shape: metricsStart's spawnDaemon args.
		{[]string{"/usr/local/bin/daedalus", "dash", "foreground", "-c", "/tmp/daedalus/config.yaml"}, true},
		{[]string{"daedalus", "-c", "cfg.yaml", "dash", "foreground"}, true},
		// A flag between the tokens breaks the pair.
		{[]string{"dash", "-c", "cfg.yaml", "foreground"}, false},
		{[]string{"dash"}, false},
		{[]string{"dash", "restart"}, false},
		{[]string{"foreground", "dash"}, false},
		{[]string{"dashx", "foreground"}, false},
		{[]string{"dash", "foregroundx"}, false},
		// The worker daemon's shape — never the dash sweep's target.
		{[]string{"worker", "foreground"}, false},
		{[]string{"daedalus", "worker", "foreground"}, false},
		{nil, false},
	} {
		if got := isDashForegroundArgv(c.args); got != c.want {
			t.Errorf("isDashForegroundArgv(%q) = %v, want %v", c.args, got, c.want)
		}
	}
}

// TestAbsorbOrphanDash pins the sweep's identification and action end to
// end, against real processes — sweepStaleWorkers' shape retargeted at the
// dash. The orphan's binary copy stays on disk on purpose: the
// identification carries no exe-freshness clause (basename and argv alone,
// behind the caller's live-pid-file miss), so the replaced-binary incident
// — the orphan class that started this — and a daemon whose binary still
// stands are one target shape. The exclusion is the pin's core: a
// `worker foreground` sibling is never touched, though it shares the
// binary basename the sweep keys on.
func TestAbsorbOrphanDash(t *testing.T) {
	dir := useDaemonDir(t)

	orphan, _ := spawnSweepChild(t, dir, "orphan", "dash", "foreground")
	worker, _ := spawnSweepChild(t, dir, "worker-lookalike", "worker", "foreground")

	out := captureStdout(t, func() {
		absorbed, err := absorbOrphanDash()
		if err != nil {
			t.Errorf("absorbOrphanDash: %v", err)
		}
		if absorbed != 1 {
			t.Errorf("absorbOrphanDash absorbed %d, want 1", absorbed)
		}
	})

	// The orphan is gone — dead and reaped, pidAlive no longer answering.
	deadline := time.Now().Add(workerStopGrace)
	for pidAlive(orphan.Process.Pid) {
		if time.Now().After(deadline) {
			t.Fatalf("orphan pid %d survived the sweep's SIGTERM", orphan.Process.Pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := orphan.Wait(); err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != -1 {
			t.Errorf("orphan child Wait = %v, want a signal death", err)
		}
	}
	if !strings.Contains(out, fmt.Sprintf("metrics daemon orphan absorbed (pid %d)", orphan.Process.Pid)) {
		t.Errorf("sweep output %q should announce the absorbed pid", out)
	}

	// The worker-foreground lookalike survives untouched and unnamed —
	// the sweep's identification must never reach the worker daemon's
	// shape.
	if !pidAlive(worker.Process.Pid) {
		t.Error("the worker-foreground lookalike was killed — the dash sweep's identification is too broad")
	}
	if strings.Contains(out, strconv.Itoa(worker.Process.Pid)) {
		t.Errorf("sweep output %q should not name untouched pid %d", out, worker.Process.Pid)
	}
}

// TestMetricsStopAbsorbsOrphan pins the stop path's two faces: behind a
// stale record a pid-file-less `dash foreground` daemon is absorbed with
// the managed stop's own grace and stop succeeds — the property `dash
// restart` leans on to reach a free port instead of dying on the orphan's
// held listeners — while behind a live record nothing is swept: a daemon
// the pid file names is the managed path, and the pid-file-less orphan
// beside it must outlive that stop.
func TestMetricsStopAbsorbsOrphan(t *testing.T) {
	t.Run("a pid-file-less dash is absorbed behind a stale record", func(t *testing.T) {
		dir := useDaemonDir(t)
		orphan, _ := spawnSweepChild(t, dir, "orphan", "dash", "foreground")
		pidFile, _, _ := metricsPaths()
		// A stale record: the live-pid miss the absorb runs behind.
		if err := os.WriteFile(pidFile, []byte(strconv.Itoa(reapedChild(t))), 0o644); err != nil {
			t.Fatal(err)
		}

		out := captureStdout(t, func() {
			if err := metricsStop(); err != nil {
				t.Errorf("metricsStop: %v", err)
			}
		})
		deadline := time.Now().Add(workerStopGrace)
		for pidAlive(orphan.Process.Pid) {
			if time.Now().After(deadline) {
				t.Fatalf("orphan pid %d survived the stop", orphan.Process.Pid)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !strings.Contains(out, fmt.Sprintf("orphan absorbed (pid %d)", orphan.Process.Pid)) {
			t.Errorf("stop output %q should announce the absorbed orphan", out)
		}
		if strings.Contains(out, "metrics daemon not running") {
			t.Errorf("stop output %q must not claim nothing ran — an orphan was just absorbed", out)
		}
		if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
			t.Errorf("stale pid file survived the stop: %v", err)
		}
	})

	t.Run("a live record takes the managed path and never sweeps", func(t *testing.T) {
		dir := useDaemonDir(t)
		managed, _ := spawnSweepChild(t, dir, "managed", "dash", "foreground")
		orphan, _ := spawnSweepChild(t, dir, "orphan", "dash", "foreground")
		pidFile, _, _ := metricsPaths()
		if err := os.WriteFile(pidFile, []byte(strconv.Itoa(managed.Process.Pid)), 0o644); err != nil {
			t.Fatal(err)
		}

		out := captureStdout(t, func() {
			if err := metricsStop(); err != nil {
				t.Errorf("metricsStop: %v", err)
			}
		})
		if !strings.Contains(out, "metrics daemon stopped (pid "+strconv.Itoa(managed.Process.Pid)+")") {
			t.Errorf("stop output %q, want the managed daemon's stopped line", out)
		}
		if strings.Contains(out, "orphan absorbed") {
			t.Errorf("stop output %q announced an absorb behind a live record — the sweep escaped its guard", out)
		}
		if pidAlive(managed.Process.Pid) {
			t.Error("the managed daemon survived its own stop")
		}
		if !pidAlive(orphan.Process.Pid) {
			t.Error("the pid-file-less orphan was killed by a managed stop — the sweep ran behind a live record")
		}
	})
}

// reapSpawnedDaemon reaps the daemon a start path left behind through
// spawnDaemon: the spawn releases the child, so nothing waits it, and a
// still-exiting (or zombie) child carries the daemon's own exe and argv
// past this test — indistinguishable, to a later stop's orphan sweep, from
// a real pid-file-less daemon, which would then be absorbed mid-suite and
// flake whichever test ran next. The defusal env makes the child exit on
// its own immediately; this only makes sure the exit is gathered before
// the test ends. A child still alive at the deadline means the defusal
// broke — fail loudly rather than leak a phantom daemon into the suite.
func reapSpawnedDaemon(t *testing.T, pidFile string) {
	t.Helper()
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("read pid file: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		t.Fatalf("pid file = %q, want the spawned daemon's pid", data)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var ws syscall.WaitStatus
		rpid, err := syscall.Wait4(pid, &ws, syscall.WNOHANG, nil)
		if rpid == pid || err == syscall.ECHILD {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("defused daemon pid %d never exited — the spawn defusal is broken and its argv would leak into the next stop's orphan sweep", pid)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestMetricsRestartAbsorbsOrphan pins the round's user-facing shape end
// to end: `dash restart` in an orphan's presence absorbs it and still
// starts. The start's child is defused like every spawned-daemon test, so
// the pin is the lifecycle — stop's absorb feeding start — not the
// serving; the child is reaped before the test ends, since a released
// child carrying the daemon's argv would be orphan-sweep fodder for the
// next stop the suite runs.
func TestMetricsRestartAbsorbsOrphan(t *testing.T) {
	dir := useDaemonDir(t)
	noDaemonSpawn(t)
	orphan, _ := spawnSweepChild(t, dir, "orphan", "dash", "foreground")
	pidFile, _, _ := metricsPaths()
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(reapedChild(t))), 0o644); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() {
		if err := metricsRestart(config.Config{}, ""); err != nil {
			t.Errorf("metricsRestart: %v", err)
		}
	})
	deadline := time.Now().Add(workerStopGrace)
	for pidAlive(orphan.Process.Pid) {
		if time.Now().After(deadline) {
			t.Fatalf("orphan pid %d survived the restart", orphan.Process.Pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, want := range []string{
		fmt.Sprintf("orphan absorbed (pid %d)", orphan.Process.Pid),
		"metrics daemon started",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("restart output %q should contain %q", out, want)
		}
	}
	reapSpawnedDaemon(t, pidFile)
}
