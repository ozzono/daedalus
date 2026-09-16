package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
)

// useDaemonDir points the package's daemonDir at a fresh temp dir for this
// test. As in TestPruneOldLogs, cleanup restores a throwaway dir rather than
// the real /tmp/daedalus, so no later test can touch a live deployment's
// pid, log, or config-record files.
func useDaemonDir(t *testing.T) string {
	t.Helper()
	reset := t.TempDir()
	t.Cleanup(func() { daemonDir = reset })
	daemonDir = t.TempDir()
	return daemonDir
}

// writeQueueConfig writes a loadable config naming queue in its own temp dir
// and returns its absolute path — distinct calls yield distinct paths even
// for identical contents, which is what the record tests discriminate on.
func writeQueueConfig(t *testing.T, queue string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("agent: claude\ntemporal:\n  task_queue: "+queue+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// writeRecord seeds queue's config record with path.
func writeRecord(t *testing.T, queue, path string) {
	t.Helper()
	_, _, confFile := daemonPaths(queue)
	if err := os.WriteFile(confFile, []byte(path), 0o644); err != nil {
		t.Fatal(err)
	}
}

// writeLivePidFile seeds queue's pid file with this process's pid — the
// cheapest way to fake a running worker for the roster logic.
func writeLivePidFile(t *testing.T, queue string) {
	t.Helper()
	pidFile, _, _ := daemonPaths(queue)
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		t.Fatal(err)
	}
}

// noDaemonSpawn defuses the child workerStart re-executes: it inherits this
// process's environment, so planting reexecEnv="-v" makes the child print
// the version and exit instead of running the whole test suite as a
// "daemon". The spawn is still observable through the files it leaves
// behind (pid file, log, rewritten config record).
func noDaemonSpawn(t *testing.T) {
	t.Helper()
	t.Setenv(reexecEnv, "-v")
}

// TestRecordedConfigPath pins the record's read-side contract: a written
// path comes back trimmed, and a missing, empty, or whitespace-only record
// counts as no record at all.
func TestRecordedConfigPath(t *testing.T) {
	useDaemonDir(t)

	if got := recordedConfigPath("daedalus"); got != "" {
		t.Errorf("recordedConfigPath(no record) = %q, want %q", got, "")
	}

	writeRecord(t, "daedalus", "/somewhere/config.yaml\n")
	if got, want := recordedConfigPath("daedalus"), "/somewhere/config.yaml"; got != want {
		t.Errorf("recordedConfigPath = %q, want %q (trimmed)", got, want)
	}

	writeRecord(t, "daedalus", "  \n")
	if got := recordedConfigPath("daedalus"); got != "" {
		t.Errorf("recordedConfigPath(blank record) = %q, want %q", got, "")
	}
}

// TestRecordedQueues pins the roster: every worker-<queue>.conf counts,
// sorted, while bare, unprefixed, and non-record files do not — and an
// unreadable daemon dir is an error, not an empty roster.
func TestRecordedQueues(t *testing.T) {
	dir := useDaemonDir(t)
	for _, name := range []string{
		"worker-b.conf", "worker-a.conf", "worker-.conf", "unrelated.conf",
		"worker-c.pid", "worker-d.log",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := recordedQueues()
	if err != nil {
		t.Fatalf("recordedQueues: %v", err)
	}
	if want := "a,b"; strings.Join(got, ",") != want {
		t.Errorf("recordedQueues = %v, want [%s]", got, want)
	}

	// A daemon dir that cannot be read must surface as an error — an empty
	// roster would read as "nothing to restart" instead.
	notDir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notDir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	daemonDir = notDir
	if _, err := recordedQueues(); err == nil || !strings.Contains(err.Error(), "read") {
		t.Errorf("recordedQueues(unreadable dir) err = %v, want a read failure", err)
	}
}

// TestParseFlagsRestartAllConfig pins the -c rejection: an explicit
// -c/--config alongside `worker restart all` (either spelling, either side
// of the subcommand) is refused, while the same flag on the neighboring
// worker actions still parses — the rejection is scoped to the one
// subcommand that never uses the config.
func TestParseFlagsRestartAllConfig(t *testing.T) {
	const rejection = "-c/--config does not apply to worker restart all"
	for _, c := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "-c before the subcommand",
			args:    []string{"-c", "cfg.yaml", "worker", "restart", "all"},
			wantErr: rejection,
		},
		{
			name:    "--config= with the --all spelling",
			args:    []string{"--config=cfg.yaml", "worker", "restart", "--all"},
			wantErr: rejection,
		},
		{
			name:    "flag after the subcommand",
			args:    []string{"worker", "restart", "all", "-c", "cfg.yaml"},
			wantErr: rejection,
		},
		{
			name: "plain worker restart keeps -c",
			args: []string{"-c", "cfg.yaml", "worker", "restart"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, _, err := parseFlags(c.args)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Errorf("parseFlags(%v) err = %v, want it to contain %q", c.args, err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseFlags(%v): %v", c.args, err)
			}
			if !f.configSet {
				t.Errorf("parseFlags(%v) configSet = false, want true", c.args)
			}
		})
	}

	// Without -c the subcommand parses untouched — the rejection must not
	// reach the default config path.
	f, rest, err := parseFlags([]string{"worker", "restart", "all"})
	if err != nil {
		t.Fatalf("parseFlags(worker restart all): %v", err)
	}
	if f.configSet {
		t.Error("configSet should be false without an explicit -c")
	}
	if want := []string{"worker", "restart", "all"}; strings.Join(rest, ",") != strings.Join(want, ",") {
		t.Errorf("parseFlags rest = %v, want %v", rest, want)
	}
}

// TestWorkerRestartUsesRecordedConfig pins the restart's config choice: a
// loadable record naming the same queue wins over the invoking command's
// own config, and a record that no longer loads or has been edited to
// another queue falls back to the CLI's — the observable being which path
// the restart re-records (workerStart rewrites the record with the config
// it actually started the worker with).
func TestWorkerRestartUsesRecordedConfig(t *testing.T) {
	const queue = "daedalus"
	// restart runs the stop/start cycle for real; the spawned child is
	// defused per-subtest by noDaemonSpawn.
	restart := func(t *testing.T, cliCfg string) string {
		cfg, err := config.Load(cliCfg)
		if err != nil {
			t.Fatal(err)
		}
		return captureStdout(t, func() {
			if err := workerRestart(cfg, cliCfg, ""); err != nil {
				t.Errorf("workerRestart: %v", err)
			}
		})
	}
	// recordedAfter reports which config the restart ended up using —
	// workerStart rewrites the record with the config it started with.
	recordedAfter := func() string { return recordedConfigPath(queue) }

	t.Run("recorded config wins over the CLI's", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		cliCfg := writeQueueConfig(t, queue)
		recordedCfg := writeQueueConfig(t, queue) // same queue, different file
		writeRecord(t, queue, recordedCfg)

		out := restart(t, cliCfg)
		if !strings.Contains(out, "worker started") {
			t.Errorf("restart output %q should report a started worker", out)
		}
		if got := recordedAfter(); got != recordedCfg {
			t.Errorf("restart re-recorded %q, want the recorded config %q (not the CLI's %q)", got, recordedCfg, cliCfg)
		}
	})

	t.Run("unreadable record falls back to the CLI's", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		cliCfg := writeQueueConfig(t, queue)
		writeRecord(t, queue, filepath.Join(t.TempDir(), "gone.yaml"))

		out := restart(t, cliCfg)
		if !strings.Contains(out, "unreadable") {
			t.Errorf("restart output %q should carry the unreadable-record diagnostic", out)
		}
		if got := recordedAfter(); got != cliCfg {
			t.Errorf("restart re-recorded %q, want the CLI's config %q", got, cliCfg)
		}
	})

	t.Run("record naming another queue falls back to the CLI's", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		cliCfg := writeQueueConfig(t, queue)
		writeRecord(t, queue, writeQueueConfig(t, "elsewhere"))

		out := restart(t, cliCfg)
		if !strings.Contains(out, "now names queue") {
			t.Errorf("restart output %q should carry the queue-mismatch diagnostic", out)
		}
		if got := recordedAfter(); got != cliCfg {
			t.Errorf("restart re-recorded %q, want the CLI's config %q", got, cliCfg)
		}
	})
}

// TestWorkerRestartAll pins the record-driven roster logic: every recorded
// queue is restarted with its own config in roster order, broken records
// are skipped and reported while the rest still restart, an empty roster is
// an error (naming any live unrecorded workers rather than implying none
// exist), and live workers without a record are reported, not restarted.
func TestWorkerRestartAll(t *testing.T) {
	t.Run("empty roster is an error", func(t *testing.T) {
		dir := useDaemonDir(t)
		err := workerRestartAll()
		if err == nil || !strings.Contains(err.Error(), "no workers on record in "+dir) {
			t.Errorf("workerRestartAll(empty) err = %v, want the no-workers diagnostic", err)
		}
	})

	t.Run("empty roster names live unrecorded workers", func(t *testing.T) {
		useDaemonDir(t)
		writeLivePidFile(t, "old")
		err := workerRestartAll()
		if err == nil || !strings.Contains(err.Error(), "running without a record") ||
			!strings.Contains(err.Error(), "old") {
			t.Errorf("workerRestartAll(empty, live stray) err = %v, want it to name queue old as left alone", err)
		}
	})

	t.Run("restarts every recorded queue with its own config", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		// Recorded out of order: the roster runs sorted, one restart per
		// queue, each under its own recorded config.
		cfgB, cfgA := writeQueueConfig(t, "b"), writeQueueConfig(t, "a")
		writeRecord(t, "b", cfgB)
		writeRecord(t, "a", cfgA)

		out := captureStdout(t, func() {
			if err := workerRestartAll(); err != nil {
				t.Errorf("workerRestartAll: %v", err)
			}
		})
		ia, ib := strings.Index(out, "== worker a ("), strings.Index(out, "== worker b (")
		if ia < 0 || ib < 0 {
			t.Fatalf("restart-all output %q should restart both recorded workers", out)
		}
		if ia > ib {
			t.Errorf("restart-all output %q should run the roster sorted (a before b)", out)
		}
		for queue, want := range map[string]string{"a": cfgA, "b": cfgB} {
			if got := recordedConfigPath(queue); got != want {
				t.Errorf("queue %s re-recorded %q, want its own config %q", queue, got, want)
			}
		}
	})

	t.Run("broken records are skipped and reported, the rest restarted", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		goodCfg := writeQueueConfig(t, "good")
		writeRecord(t, "good", goodCfg)
		writeRecord(t, "missing", filepath.Join(t.TempDir(), "gone.yaml"))
		writeRecord(t, "blank", "  \n")
		writeRecord(t, "moved", writeQueueConfig(t, "elsewhere"))

		out := captureStdout(t, func() {
			err := workerRestartAll()
			for _, queue := range []string{"missing", "blank", "moved"} {
				if err == nil || !strings.Contains(err.Error(), "queue "+queue) {
					t.Errorf("workerRestartAll err = %v, want it to report skipped queue %s", err, queue)
				}
			}
			if err == nil {
				t.Error("workerRestartAll should fail when a recorded queue is skipped")
			}
		})
		if !strings.Contains(out, "== worker good (") {
			t.Errorf("restart-all output %q should still restart the healthy worker", out)
		}
		for _, queue := range []string{"missing", "blank", "moved"} {
			if strings.Contains(out, "== worker "+queue+" (") {
				t.Errorf("restart-all output %q should not restart broken queue %s", out, queue)
			}
		}
	})

	t.Run("live workers without a record are reported, not restarted", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		cfg := writeQueueConfig(t, "good")
		writeRecord(t, "good", cfg)
		writeLivePidFile(t, "zombie")

		out := captureStdout(t, func() {
			if err := workerRestartAll(); err != nil {
				t.Errorf("workerRestartAll: %v", err)
			}
		})
		if want := "skipped 1 running worker(s) without a config record: zombie"; !strings.Contains(out, want) {
			t.Errorf("restart-all output %q should carry %q", out, want)
		}
	})
}

// TestMainRestartAllNeedsNoConfig pins the dispatch wiring in-process:
// `worker restart all` runs with no config resolvable anywhere (bare cwd,
// empty HOME) and still completes against the records — without the
// isRestartAll bypass in main, resolveConfigPath would exit(1) with a
// "load config" diagnostic before any record is read. The one recorded
// worker is restarted against a throwaway daemon dir, with the re-exec'd
// child defused by noDaemonSpawn, so main() returns instead of exiting.
func TestMainRestartAllNeedsNoConfig(t *testing.T) {
	useDaemonDir(t)
	noDaemonSpawn(t)
	cfg := writeQueueConfig(t, "daedalus")
	writeRecord(t, "daedalus", cfg)

	t.Setenv("HOME", t.TempDir()) // no ~/.config/daedalus/config.yaml fallback
	t.Chdir(t.TempDir())          // no ./config.yaml

	args := os.Args
	os.Args = []string{"daedalus", "worker", "restart", "all"}
	defer func() { os.Args = args }()
	out := captureStdout(t, main)

	if !strings.Contains(out, "== worker daedalus (") {
		t.Errorf("worker restart all output %q should restart the recorded worker", out)
	}
	if got := recordedConfigPath("daedalus"); got != cfg {
		t.Errorf("restart-all re-recorded %q, want %q", got, cfg)
	}
}

// TestMainRestartAllRejectsConfig pins the -c rejection's exit contract in
// a subprocess: usageFail's shape (diagnostic, blank line, usage text) and
// exit 1 — asserted only on paths that fail before the real daemon dir is
// ever read, so the live workers in /tmp/daedalus are never touched.
func TestMainRestartAllRejectsConfig(t *testing.T) {
	cfg := validConfig(t)

	stdout, stderr, code := runMainIn(t, "", "-c", cfg, "worker", "restart", "all")
	if code != 1 {
		t.Errorf("daedalus -c <config> worker restart all exit code = %d, want 1", code)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
	if !strings.HasPrefix(stderr, "-c/--config does not apply to worker restart all\n\n") {
		t.Errorf("stderr should start with the rejection and a blank line, got %q", stderr)
	}
	if !strings.HasSuffix(stderr, usage) {
		t.Errorf("stderr should end with the usage text, got %q", stderr)
	}

	// Anything beyond `restart all` is a plain usage error, not restart-all.
	_, stderr, code = runMainIn(t, "", "-c", cfg, "worker", "restart", "all", "extra")
	if code != 1 {
		t.Errorf("daedalus worker restart all extra exit code = %d, want 1", code)
	}
	if !strings.HasPrefix(stderr, "worker takes at most one action\n\n") {
		t.Errorf("stderr should start with the too-many-actions rejection, got %q", stderr)
	}
}
