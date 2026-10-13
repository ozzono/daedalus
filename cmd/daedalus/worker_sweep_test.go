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
// end, against real processes: a worker-foreground argv under a deleted
// copy of this binary is reaped — SIGTERM, the daemon pid file naming it
// removed, the reap announced by pid — while a deleted-binary lookalike
// with a different argv and a same-argv sibling with an intact binary are
// both left alone. Those two exclusions are the sweep's safety property:
// it signals processes it found by scanning /proc, so a too-broad match
// would kill whatever it mistook for a stale worker.
func TestSweepStaleWorkers(t *testing.T) {
	dir := useDaemonDir(t)

	stale, staleCopy := spawnSweepChild(t, dir, "stale", "worker", "foreground")
	wrongArgv, wrongArgvCopy := spawnSweepChild(t, dir, "wrongargv", "daemon", "helper")
	healthy, _ := spawnSweepChild(t, dir, "healthy", "worker", "foreground")

	// Both copies die on disk; the running children keep their inodes with
	// the kernel's "(deleted)" marker — the exact condition the sweep keys
	// on. Verified before sweeping: if the marker never appears, the sweep
	// below is pinning nothing.
	for _, c := range []struct {
		name     string
		copyPath string
		pid      int
	}{{"stale", staleCopy, stale.Process.Pid}, {"wrongargv", wrongArgvCopy, wrongArgv.Process.Pid}} {
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

	// The daemon-dir pid file names the stale worker; a second names this
	// test process and must survive.
	pidFile := filepath.Join(dir, "worker-daedalus.pid")
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(stale.Process.Pid)), 0o644); err != nil {
		t.Fatal(err)
	}
	ownPidFile := filepath.Join(dir, "worker-other.pid")
	if err := os.WriteFile(ownPidFile, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() { sweepStaleWorkers() })

	// The stale worker is gone — dead and reaped, pidAlive no longer
	// answering.
	deadline := time.Now().Add(workerStopGrace)
	for pidAlive(stale.Process.Pid) {
		if time.Now().After(deadline) {
			t.Fatalf("stale worker pid %d survived the sweep's SIGTERM", stale.Process.Pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := stale.Wait(); err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != -1 {
			t.Errorf("stale child Wait = %v, want a signal death", err)
		}
	}
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Errorf("the stale worker's pid file should be removed (stat err = %v)", err)
	}
	if _, err := os.Stat(ownPidFile); err != nil {
		t.Errorf("the pid file naming this test process should survive: %v", err)
	}
	if !strings.Contains(out, fmt.Sprintf("reaped stale worker pid %d", stale.Process.Pid)) {
		t.Errorf("sweep output %q should announce the reaped pid", out)
	}

	// The lookalikes are untouched: same deleted binary but a different
	// argv, and same argv but an intact binary.
	for _, c := range []struct {
		name string
		pid  int
	}{{"the deleted-binary non-worker", wrongArgv.Process.Pid}, {"the healthy worker lookalike", healthy.Process.Pid}} {
		if !pidAlive(c.pid) {
			t.Errorf("%s (pid %d) was killed — the sweep's identification is too broad", c.name, c.pid)
		}
		if strings.Contains(out, strconv.Itoa(c.pid)) {
			t.Errorf("sweep output %q should not name untouched pid %d", out, c.pid)
		}
	}
}

// TestRunWorkerSweepsStaleWorkers pins the sweep's production wiring:
// runWorker sweeps first, before anything else it does — an empty-config
// start that dies at worktree preflight still reaps a stale deleted-binary
// worker on the way in, pruning the daemon pid file naming it. The direct
// call TestSweepStaleWorkers pins cannot catch the call site dying: deleting
// it from runWorker (or moving it behind the first error return) strands
// the stale poller again, and this fails.
func TestRunWorkerSweepsStaleWorkers(t *testing.T) {
	dir := useDaemonDir(t)

	stale, staleCopy := spawnSweepChild(t, dir, "wiring", "worker", "foreground")
	if err := os.Remove(staleCopy); err != nil {
		t.Fatalf("unlink the stale copy: %v", err)
	}
	pidFile := filepath.Join(dir, "worker-daedalus.pid")
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(stale.Process.Pid)), 0o644); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() {
		if err := runWorker(config.Config{}, ""); err == nil {
			t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
		}
	})

	// The stale worker did not survive the startup it now competes with.
	deadline := time.Now().Add(workerStopGrace)
	for pidAlive(stale.Process.Pid) {
		if time.Now().After(deadline) {
			t.Fatalf("stale worker pid %d survived runWorker's sweep", stale.Process.Pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := stale.Wait(); err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != -1 {
			t.Errorf("stale child Wait = %v, want a signal death", err)
		}
	}
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Errorf("the stale worker's pid file should be removed (stat err = %v)", err)
	}
	if !strings.Contains(out, fmt.Sprintf("reaped stale worker pid %d", stale.Process.Pid)) {
		t.Errorf("runWorker's sweep output %q should announce the reaped pid", out)
	}
}
