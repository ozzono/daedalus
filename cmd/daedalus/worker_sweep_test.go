package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ozzono/daedalus/internal/config"
)

// spawnSweepChild execs a byte copy of this test binary — in its own
// subdirectory of dir, under the binary's own basename — with the
// sweep-sleep env set and the given argv, waits for the child to report
// ready (proof the exec landed and the child is in its sleep mode, not
// running the test suite), and returns the command and the copy's path.
// Cleanup kills and reaps the child whatever the assertions did. The copy
// is a real byte copy, not a link: the sweep keys on the kernel's
// "(deleted)" exe marker, which the unlink must leave on an inode with no
// other name.
func spawnSweepChild(t *testing.T, dir, markerName string, args ...string) (cmd *exec.Cmd, copyPath string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test binary: %v", err)
	}
	// One subdirectory per child: a second copy over the first's path is
	// text-file-busy while the first child executes it.
	own := filepath.Join(dir, markerName)
	if err := os.MkdirAll(own, 0o755); err != nil {
		t.Fatal(err)
	}
	copyPath = filepath.Join(own, filepath.Base(exe))
	data, err := os.ReadFile(exe)
	if err != nil {
		t.Fatalf("read test binary: %v", err)
	}
	if err := os.WriteFile(copyPath, data, 0o755); err != nil {
		t.Fatalf("copy test binary: %v", err)
	}
	marker := filepath.Join(own, "ready")
	cmd = exec.Command(copyPath, args...)
	cmd.Env = append(os.Environ(), reexecSweepSleepEnv+"="+marker)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sweep child: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			return cmd, copyPath
		}
		if time.Now().After(deadline) {
			t.Fatal("sweep child never reported ready — it is running the test suite instead of its sleep mode")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestIsWorkerForegroundArgv pins the argv shape the sweep identifies: the
// consecutive worker/foreground tokens, wherever they sit in the daemon's
// command line (the re-exec carries flags before and after the pair) — and
// never a flag-pried or merely coincidental shape.
func TestIsWorkerForegroundArgv(t *testing.T) {
	for _, c := range []struct {
		args []string
		want bool
	}{
		{[]string{"worker", "foreground"}, true},
		// The daemon's own re-exec shape: workerStart's childArgs.
		{[]string{"/usr/local/bin/daedalus", "worker", "foreground", "-c", "/tmp/daedalus/config.yaml"}, true},
		{[]string{"daedalus", "-c", "cfg.yaml", "worker", "foreground"}, true},
		// A flag between the tokens breaks the pair.
		{[]string{"worker", "-c", "cfg.yaml", "foreground"}, false},
		{[]string{"worker"}, false},
		{[]string{"worker", "restart"}, false},
		{[]string{"foreground", "worker"}, false},
		{[]string{"workerx", "foreground"}, false},
		{[]string{"worker", "foregroundx"}, false},
		{nil, false},
	} {
		if got := isWorkerForegroundArgv(c.args); got != c.want {
			t.Errorf("isWorkerForegroundArgv(%q) = %v, want %v", c.args, got, c.want)
		}
	}
}

// TestRemovePidFilesFor pins the pid-file cleanup: every worker-*.pid file
// naming the pid goes, whatever worker it belongs to (the sweep is
// name-agnostic by design — the stale worker may belong to any of them),
// while pid files naming another pid, unparsable ones, non-daemon files,
// and the other daemon-file kinds stay.
func TestRemovePidFilesFor(t *testing.T) {
	dir := useDaemonDir(t)
	const pid = 4242

	files := map[string]string{
		"worker-alpha.pid":  strconv.Itoa(pid),         // names the pid: goes
		"worker-beta.pid":   " 4242\n",                 // trimmed before the compare: goes
		"worker-gamma.pid":  strconv.Itoa(os.Getpid()), // names another pid: stays
		"worker-delta.pid":  "not a pid",               // unparsable: stays
		"unrelated.pid":     strconv.Itoa(pid),         // not a daemon pid file: stays
		"worker-alpha.log":  "log text",                // another daemon-file kind: stays
		"worker-alpha.conf": "/somewhere/config.yaml",  // and its record: stays
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	removePidFilesFor(pid)

	for _, gone := range []string{"worker-alpha.pid", "worker-beta.pid"} {
		if _, err := os.Stat(filepath.Join(dir, gone)); !os.IsNotExist(err) {
			t.Errorf("%s should be removed (stat err = %v)", gone, err)
		}
	}
	for name := range files {
		switch name {
		case "worker-alpha.pid", "worker-beta.pid":
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s should survive the cleanup: %v", name, err)
		}
	}
}

// TestSweepStaleWorkers pins the sweep's identification and action end to
// end, against real processes. The sweep rides only the restart-all force
// scope, so its reach is the pin's core: a deleted-binary worker-foreground
// ORPHAN — no pid file naming it, so no stop or restart can address it — is
// reaped, while a deleted-binary lookalike with a different argv, a
// same-argv sibling with an intact binary, and a managed worker whose live
// pid file names it are all left alone. Those three exclusions are the
// sweep's safety property: it signals processes it found by scanning /proc,
// so a too-broad match would kill whatever it mistook for a stale worker —
// exactly the mass-reap incident the pid-file guard closes (a rebuilt
// binary turns every running daemon's exe "(deleted)", and the pre-guard
// sweep reaped all of a host's healthy siblings).
func TestSweepStaleWorkers(t *testing.T) {
	dir := useDaemonDir(t)

	orphan, orphanCopy := spawnSweepChild(t, dir, "orphan", "worker", "foreground")
	managed, managedCopy := spawnSweepChild(t, dir, "managed", "worker", "foreground")
	wrongArgv, wrongArgvCopy := spawnSweepChild(t, dir, "wrongargv", "daemon", "helper")
	healthy, _ := spawnSweepChild(t, dir, "healthy", "worker", "foreground")

	// Three of the copies die on disk; the running children keep their
	// inodes with the kernel's "(deleted)" marker — the exact condition
	// the sweep keys on. Verified before sweeping: if the marker never
	// appears, the sweep below is pinning nothing. The healthy lookalike
	// keeps its binary — the intact-exe exclusion needs it.
	for _, c := range []struct {
		name     string
		copyPath string
		pid      int
	}{{"orphan", orphanCopy, orphan.Process.Pid}, {"managed", managedCopy, managed.Process.Pid}, {"wrongargv", wrongArgvCopy, wrongArgv.Process.Pid}} {
		if err := os.Remove(c.copyPath); err != nil {
			t.Fatalf("unlink %s copy: %v", c.name, err)
		}
		link, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", c.pid))
		if err != nil {
			t.Fatalf("readlink %s child exe: %v", c.name, err)
		}
		if !strings.HasSuffix(link, deletedExeSuffix) {
			t.Fatalf("/proc/%d/exe = %q after the unlink, want the %q marker", c.pid, link, deletedExeSuffix)
		}
	}

	// The managed worker's live pid file names it — stop and restart reach
	// it, so the sweep must never touch it even under a deleted binary.
	managedPidFile := filepath.Join(dir, "worker-managed.pid")
	if err := os.WriteFile(managedPidFile, []byte(strconv.Itoa(managed.Process.Pid)), 0o644); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() { sweepStaleWorkers() })

	// The orphan is gone — dead and reaped, pidAlive no longer answering.
	deadline := time.Now().Add(workerStopGrace)
	for pidAlive(orphan.Process.Pid) {
		if time.Now().After(deadline) {
			t.Fatalf("stale orphan pid %d survived the sweep's SIGTERM", orphan.Process.Pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := orphan.Wait(); err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != -1 {
			t.Errorf("orphan child Wait = %v, want a signal death", err)
		}
	}
	if !strings.Contains(out, fmt.Sprintf("reaped stale worker pid %d", orphan.Process.Pid)) {
		t.Errorf("sweep output %q should announce the reaped pid", out)
	}

	// The exclusions are untouched: the managed worker (a live pid file
	// names it), the deleted-binary non-worker (a different argv), and the
	// healthy worker lookalike (an intact binary).
	for _, c := range []struct {
		name string
		pid  int
	}{{"the managed worker", managed.Process.Pid}, {"the deleted-binary non-worker", wrongArgv.Process.Pid}, {"the healthy worker lookalike", healthy.Process.Pid}} {
		if !pidAlive(c.pid) {
			t.Errorf("%s (pid %d) was killed — the sweep's identification is too broad", c.name, c.pid)
		}
		if strings.Contains(out, strconv.Itoa(c.pid)) {
			t.Errorf("sweep output %q should not name untouched pid %d", out, c.pid)
		}
	}
	if _, err := os.Stat(managedPidFile); err != nil {
		t.Errorf("the managed worker's pid file should survive the sweep: %v", err)
	}
}

// TestRunWorkerSparesStaleWorkers pins the blast-radius ruling's plain-start
// half: a worker start signals exactly the worker it addresses — never a
// sibling. The startup sweep the old runWorker ran reaped every
// deleted-binary worker-foreground process on the host after any rebuild
// (the mass-reap incident); it is gone from the startup path, so even the
// maximally sweepable shape — a deleted-binary orphan running the daemon's
// own argv with no pid file naming it — survives an empty-config start that
// dies at worktree preflight.
func TestRunWorkerSparesStaleWorkers(t *testing.T) {
	dir := useDaemonDir(t)

	orphan, orphanCopy := spawnSweepChild(t, dir, "spare", "worker", "foreground")
	if err := os.Remove(orphanCopy); err != nil {
		t.Fatalf("unlink the orphan copy: %v", err)
	}
	if link, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", orphan.Process.Pid)); err != nil || !strings.HasSuffix(link, deletedExeSuffix) {
		t.Fatalf("orphan exe link = %q (%v), want the %q marker — the pin below would hold vacuously", link, err, deletedExeSuffix)
	}

	out := captureStdout(t, func() {
		if err := runWorker(config.Config{}, ""); err == nil {
			t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
		}
	})

	if !pidAlive(orphan.Process.Pid) {
		t.Fatal("the orphan was killed by a plain worker start — the startup sweep is back")
	}
	if strings.Contains(out, "reaped stale worker") {
		t.Errorf("a plain start's output %q must never announce a sweep reap", out)
	}
}

// TestRestartAllForceSweeps pins the ruling's restart-all half: the stale
// sweep rides only the force scope of `worker restart all` — the one form
// whose reach is every worker on the host — and the plain restart-all (the
// drain default) never signals a worker it was not addressing. The orphan
// here is the only shape the force sweep may reap: deleted binary, the
// daemon's own argv, no pid file.
func TestRestartAllForceSweeps(t *testing.T) {
	spawnOrphan := func(t *testing.T, dir, markerName string) (pid int, gone func() error) {
		t.Helper()
		child, copyPath := spawnSweepChild(t, dir, markerName, "worker", "foreground")
		if err := os.Remove(copyPath); err != nil {
			t.Fatalf("unlink the orphan copy: %v", err)
		}
		if link, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", child.Process.Pid)); err != nil || !strings.HasSuffix(link, deletedExeSuffix) {
			t.Fatalf("orphan exe link = %q (%v), want the %q marker — the pin below would hold vacuously", link, err, deletedExeSuffix)
		}
		return child.Process.Pid, func() error { return child.Wait() }
	}

	t.Run("the force scope reaps the pid-file-less orphan", func(t *testing.T) {
		dir := useDaemonDir(t)
		noDaemonSpawn(t)
		cfgPath := writeQueueConfig(t, "daedalus")
		writeRecord(t, "daedalus", cfgPath)
		orphan, wait := spawnOrphan(t, dir, "force-sweep")

		out := captureStdout(t, func() {
			if err := workerRestartAll(true); err != nil {
				t.Errorf("workerRestartAll(force): %v", err)
			}
		})
		deadline := time.Now().Add(workerStopGrace)
		for pidAlive(orphan) {
			if time.Now().After(deadline) {
				t.Fatalf("orphan pid %d survived the force restart-all's sweep", orphan)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err := wait(); err != nil {
			var ee *exec.ExitError
			if !errors.As(err, &ee) || ee.ExitCode() != -1 {
				t.Errorf("orphan child Wait = %v, want a signal death", err)
			}
		}
		if !strings.Contains(out, fmt.Sprintf("reaped stale worker pid %d", orphan)) {
			t.Errorf("force restart-all output %q should announce the reap", out)
		}
		if !strings.Contains(out, "== worker daedalus (") {
			t.Errorf("force restart-all output %q should still cycle the recorded worker", out)
		}
	})

	t.Run("the plain restart-all never sweeps", func(t *testing.T) {
		dir := useDaemonDir(t)
		noDaemonSpawn(t)
		cfgPath := writeQueueConfig(t, "daedalus")
		writeRecord(t, "daedalus", cfgPath)
		orphan, _ := spawnOrphan(t, dir, "plain-sweep")

		out := captureStdout(t, func() {
			if err := workerRestartAll(); err != nil {
				t.Errorf("workerRestartAll: %v", err)
			}
		})
		if !pidAlive(orphan) {
			t.Fatal("the orphan was killed by a plain restart-all — the sweep escaped the force scope")
		}
		if strings.Contains(out, "reaped stale worker") {
			t.Errorf("plain restart-all output %q must never announce a sweep reap", out)
		}
	})
}
