package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
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

		for _, want := range []string{"WORKER", "STATE", "API", "CONFIG", "CODE PATH", "LOG"} {
			if !strings.Contains(out, want) {
				t.Errorf("status output %q should carry the %s column header", out, want)
			}
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
