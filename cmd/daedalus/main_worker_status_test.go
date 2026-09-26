package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestWorkerStatusAll pins the record-driven roster listing: every recorded
// queue — plus any live stray running without a record, marked "(no config
// record)" — appears sorted, with its running state, its config record, and
// its log path; an empty or missing roster prints the start-one-first hint
// instead of failing.
func TestWorkerStatusAll(t *testing.T) {
	t.Run("empty roster prints the hint, not an error", func(t *testing.T) {
		dir := useDaemonDir(t)
		out := captureStdout(t, func() {
			if err := workerStatusAll(); err != nil {
				t.Errorf("workerStatusAll(empty): %v", err)
			}
		})
		if want := "no workers on record in " + dir; !strings.Contains(out, want) {
			t.Errorf("status output %q should carry %q", out, want)
		}
	})

	t.Run("missing daemon dir reads as an empty roster", func(t *testing.T) {
		// A fresh machine: the daemon dir has never been created, and the
		// hint must still say "start one first", not report a read failure.
		gone := filepath.Join(t.TempDir(), "gone")
		reset := t.TempDir()
		t.Cleanup(func() { daemonDir = reset })
		daemonDir = gone

		out := captureStdout(t, func() {
			if err := workerStatusAll(); err != nil {
				t.Errorf("workerStatusAll(missing dir): %v", err)
			}
		})
		if want := "no workers on record in " + gone; !strings.Contains(out, want) {
			t.Errorf("status output %q should carry %q", out, want)
		}
	})

	t.Run("lists records and live strays with state, config, and log", func(t *testing.T) {
		dir := useDaemonDir(t)
		cfgB := writeQueueConfig(t, "b")
		writeLivePidFile(t, "a")  // live stray, no record
		writeRecord(t, "b", cfgB) // recorded, running
		writeLivePidFile(t, "b")
		writeRecord(t, "z", writeQueueConfig(t, "z")) // recorded, not running

		out := captureStdout(t, func() {
			if err := workerStatusAll(); err != nil {
				t.Errorf("workerStatusAll: %v", err)
			}
		})

		for _, want := range []string{"WORKER", "STATE", "VERSION", "API", "CONFIG", "CODE PATH", "LOG"} {
			if !strings.Contains(out, want) {
				t.Errorf("status output %q should carry the %s column header", out, want)
			}
		}
		// Neither live worker has published a status file here, so the
		// VERSION column cannot confirm a build and says "(unknown)"; the
		// stopped worker has nothing to report at all.
		if !strings.Contains(out, "(unknown)") {
			t.Errorf("status output %q should mark the live workers' unknown version", out)
		}
		if !strings.Contains(out, "n/a (not running)") {
			t.Errorf("status output %q should leave the stopped worker's version n/a", out)
		}
		// Row b: running under this process's pid, with its recorded config
		// and log path.
		for _, want := range []string{
			"running (pid " + strconv.Itoa(os.Getpid()) + ")",
			cfgB,
			filepath.Join(dir, "worker-b.log"),
		} {
			if !strings.Contains(out, want) {
				t.Errorf("status output %q should carry %q for the running worker", out, want)
			}
		}
		if !strings.Contains(out, "not running") {
			t.Errorf("status output %q should mark the stopped worker", out)
		}
		if !strings.Contains(out, "(no config record)") {
			t.Errorf("status output %q should mark the live stray's missing record", out)
		}
		// The stray's API column must read n/a, not "config error": the
		// placeholder is display text for the CONFIG column only, never a
		// config path for providerStatus to load.
		if strings.Contains(out, "config error") {
			t.Errorf("status output %q should report n/a for the missing record, not \"config error\"", out)
		}
		// The merged roster runs sorted. The stray deliberately sorts before
		// both records: recordedWorkers yields [b, z] and the stray appends
		// as [a], so without workerStatusAll's own merge sort the
		// concatenation would print b, z, a and this ordering assertion
		// fails.
		ia, ib, iz := strings.Index(out, "\na"), strings.Index(out, "\nb"), strings.Index(out, "\nz")
		if ia < 0 || ib < 0 || iz < 0 || !(ia < ib && ib < iz) {
			t.Errorf("status output %q should list workers a, b, z sorted (indexes %d, %d, %d)", out, ia, ib, iz)
		}
	})

	t.Run("shows the published version of a running worker, unknown when the file disagrees", func(t *testing.T) {
		useDaemonDir(t)
		// pub is live and its status file names this pid and a build;
		// mis is live but its file was left by a different pid, so the
		// version in it must not be shown.
		writeLivePidFile(t, "mis")
		writeStatusFile(t, "mis", workerStatus{PID: os.Getpid() + 1, Updated: time.Now().UTC(), Version: "v0.0-wrong"})
		writeLivePidFile(t, "pub")
		writeStatusFile(t, "pub", workerStatus{PID: os.Getpid(), Updated: time.Now().UTC(), Version: "v0.0-test"})

		out := captureStdout(t, func() {
			if err := workerStatusAll(); err != nil {
				t.Errorf("workerStatusAll: %v", err)
			}
		})

		row := func(name string) string {
			for _, line := range strings.Split(out, "\n") {
				if strings.HasPrefix(line, name) {
					return line
				}
			}
			t.Fatalf("status output %q has no row for %s", out, name)
			return ""
		}
		if r := row("pub"); !strings.Contains(r, "v0.0-test") {
			t.Errorf("row %q should carry the published version v0.0-test", r)
		}
		if r := row("mis"); !strings.Contains(r, "(unknown)") || strings.Contains(r, "v0.0-wrong") {
			t.Errorf("row %q should distrust the mismatched file's version", r)
		}
	})
}

// TestWorkerCodePathLadder pins workerCodePath's display-text ladder for
// the cases decidable without a Temporal round trip: no config record, and
// an unloadable config. Everything past config.Load (dial, query, history)
// degrades to "temporal unreachable"/"idle" in the row and is exercised
// only live.
func TestWorkerCodePathLadder(t *testing.T) {
	if got := workerCodePath(""); got != "n/a (no config record)" {
		t.Errorf("workerCodePath(no record) = %q, want the n/a placeholder", got)
	}
	if got := workerCodePath(filepath.Join(t.TempDir(), "gone.yaml")); got != "config error" {
		t.Errorf("workerCodePath(missing config) = %q, want \"config error\"", got)
	}
}

// TestWorkerVersionLadder pins workerVersion's display-text ladder for the
// cases decidable without a live daemon: a worker that is not running has
// nothing to report, and a running worker's version is shown only when a
// readable status file confirms it with a matching pid and a non-empty
// version — no file, a pre-version file from an old build, a pid
// mismatch, or corrupt JSON all degrade to "(unknown)".
func TestWorkerVersionLadder(t *testing.T) {
	useDaemonDir(t)
	writeStatusFile(t, "ok", workerStatus{PID: os.Getpid(), Updated: time.Now().UTC(), Version: "v1.2.3"})
	// A file from a worker old enough to predate the version field.
	writeStatusFile(t, "old", workerStatus{PID: os.Getpid(), Updated: time.Now().UTC()})
	writeStatusFile(t, "pid", workerStatus{PID: os.Getpid() + 1, Updated: time.Now().UTC(), Version: "v1.2.3"})
	if err := os.WriteFile(filepath.Join(daemonDir, "worker-bad.status"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name string // worker name; also keys its worker-<name>.status file
		live bool
		want string
	}{
		{"stopped", false, "n/a (not running)"},
		{"nofile", true, "(unknown)"},
		{"ok", true, "v1.2.3"},
		{"old", true, "(unknown)"},
		{"pid", true, "(unknown)"},
		{"bad", true, "(unknown)"},
	} {
		if got := workerVersion(c.name, os.Getpid(), c.live); got != c.want {
			t.Errorf("workerVersion(%q, live=%v) = %q, want %q", c.name, c.live, got, c.want)
		}
	}
}

// TestMainWorkerStatusNeedsNoConfig pins the dispatch wiring in-process:
// `worker status` runs with no config resolvable anywhere (bare cwd, empty
// HOME) and still lists the records — without the isWorkerStatus bypass in
// main, resolveConfigPath would exit(1) with a "load config" diagnostic
// before any record is read. Status spawns nothing, so the real daemon dir
// is only replaced to keep the listing deterministic.
func TestMainWorkerStatusNeedsNoConfig(t *testing.T) {
	useDaemonDir(t)
	cfg := writeQueueConfig(t, "daedalus")
	writeRecord(t, "daedalus", cfg)

	t.Setenv("HOME", t.TempDir()) // no ~/.config/daedalus/config.yaml fallback
	t.Chdir(t.TempDir())          // no ./config.yaml

	args := os.Args
	os.Args = []string{"daedalus", "worker", "status"}
	defer func() { os.Args = args }()
	out := captureStdout(t, main)

	if !strings.Contains(out, "daedalus") || !strings.Contains(out, cfg) {
		t.Errorf("worker status output %q should list the recorded worker with its config", out)
	}
}
