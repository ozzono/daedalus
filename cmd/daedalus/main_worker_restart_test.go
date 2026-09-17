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

// writeWorkerIDConfig writes a loadable config whose worker is named by an
// explicit worker_id (queue q7 — deliberately different from the id, so the
// test proves the id, not the queue, keys the daemon files).
func writeWorkerIDConfig(t *testing.T, id string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("agent: claude\nworker_id: "+id+"\ntemporal:\n  task_queue: q7\n"), 0o644); err != nil {
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

// TestRecordedWorkers pins the roster: every worker-<name>.conf counts,
// sorted, while bare, unprefixed, and non-record files do not — and an
// unreadable daemon dir is an error, not an empty roster.
func TestRecordedWorkers(t *testing.T) {
	dir := useDaemonDir(t)
	for _, name := range []string{
		"worker-b.conf", "worker-a.conf", "worker-.conf", "unrelated.conf",
		"worker-c.pid", "worker-d.log",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := recordedWorkers()
	if err != nil {
		t.Fatalf("recordedWorkers: %v", err)
	}
	if want := "a,b"; strings.Join(got, ",") != want {
		t.Errorf("recordedWorkers = %v, want [%s]", got, want)
	}

	// A daemon dir that cannot be read must surface as an error — an empty
	// roster would read as "nothing to restart" instead.
	notDir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notDir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	daemonDir = notDir
	if _, err := recordedWorkers(); err == nil || !strings.Contains(err.Error(), "read") {
		t.Errorf("recordedWorkers(unreadable dir) err = %v, want a read failure", err)
	}
}

// TestParseFlagsRecordDrivenRestartConfig pins the -c/-cli rejections: an
// explicit -c/--config alongside the record-driven restarts (`worker
// restart all`, either spelling and either side of the subcommand, and
// `worker restart <name>`) is refused, while the same flag on a plain
// restart still parses — the rejection is scoped to the invocations that
// never use the config.
func TestParseFlagsRecordDrivenRestartConfig(t *testing.T) {
	const rejection = "-c/--config does not apply to worker restart all or restart <worker>"
	for _, c := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "-c before restart all",
			args:    []string{"-c", "cfg.yaml", "worker", "restart", "all"},
			wantErr: rejection,
		},
		{
			name:    "--config= with the --all spelling",
			args:    []string{"--config=cfg.yaml", "worker", "restart", "--all"},
			wantErr: rejection,
		},
		{
			name:    "flag after restart all",
			args:    []string{"worker", "restart", "all", "-c", "cfg.yaml"},
			wantErr: rejection,
		},
		{
			name:    "-c before restart <worker>",
			args:    []string{"-c", "cfg.yaml", "worker", "restart", "arete"},
			wantErr: rejection,
		},
		{
			name:    "flag after restart <worker>",
			args:    []string{"worker", "restart", "arete", "--config=cfg.yaml"},
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

	// Without -c the record-driven restarts parse untouched — the rejection
	// must not reach the default config path.
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
	f, rest, err = parseFlags([]string{"worker", "restart", "arete"})
	if err != nil {
		t.Fatalf("parseFlags(worker restart arete): %v", err)
	}
	if f.configSet {
		t.Error("configSet should be false without an explicit -c")
	}
	if want := []string{"worker", "restart", "arete"}; strings.Join(rest, ",") != strings.Join(want, ",") {
		t.Errorf("parseFlags rest = %v, want %v", rest, want)
	}
}

// TestWorkerRestartUsesCLIConfig pins the restart's config choice: the
// invoking command's config always wins — the record is not consulted — so
// values changed since the worker was started take effect on every restart.
// The observable is which path the restart re-records (workerStart rewrites
// the record with the config it actually started the worker with).
func TestWorkerRestartUsesCLIConfig(t *testing.T) {
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

	t.Run("CLI config wins over a differing record", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		cliCfg := writeQueueConfig(t, queue)
		recordedCfg := writeQueueConfig(t, queue) // same queue, different file
		writeRecord(t, queue, recordedCfg)

		out := restart(t, cliCfg)
		if !strings.Contains(out, "worker started") {
			t.Errorf("restart output %q should report a started worker", out)
		}
		if got := recordedAfter(); got != cliCfg {
			t.Errorf("restart re-recorded %q, want the CLI's config %q (not the stale record %q)", got, cliCfg, recordedCfg)
		}
	})

	t.Run("no record at all still restarts with the CLI's", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		cliCfg := writeQueueConfig(t, queue)

		out := restart(t, cliCfg)
		if !strings.Contains(out, "worker started") {
			t.Errorf("restart output %q should report a started worker", out)
		}
		if got := recordedAfter(); got != cliCfg {
			t.Errorf("restart re-recorded %q, want the CLI's config %q", got, cliCfg)
		}
	})
}

// TestWorkerRestartNamed pins the record-driven single-worker restart: the
// named worker is restarted from its recorded config (re-read from disk, so
// edits apply), an explicit worker_id — not the task queue — keys the daemon
// files, and a missing record, an unloadable one, or one whose config now
// names a different worker is an error rather than a best-effort fallback.
func TestWorkerRestartNamed(t *testing.T) {
	t.Run("restarts by worker_id, re-reading the recorded config", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		cfgPath := writeWorkerIDConfig(t, "arete")
		writeRecord(t, "arete", cfgPath)

		out := captureStdout(t, func() {
			if err := workerRestartNamed("arete"); err != nil {
				t.Errorf("workerRestartNamed: %v", err)
			}
		})
		if !strings.Contains(out, "worker started") {
			t.Errorf("restart output %q should report a started worker", out)
		}
		if got := recordedConfigPath("arete"); got != cfgPath {
			t.Errorf("restart re-recorded %q, want the recorded config %q", got, cfgPath)
		}
		// The id, not the queue (q7), keyed the daemon files.
		if _, err := os.Stat(filepath.Join(daemonDir, "worker-arete.pid")); err != nil {
			t.Errorf("restart should key its files by the worker id: %v", err)
		}
		if _, err := os.Stat(filepath.Join(daemonDir, "worker-q7.pid")); err == nil {
			t.Error("restart should not key its files by the task queue when a worker id is set")
		}
	})

	t.Run("edits to the recorded config are picked up", func(t *testing.T) {
		useDaemonDir(t)
		cfgPath := writeQueueConfig(t, "daedalus")
		writeRecord(t, "daedalus", cfgPath)
		// Edit the config in place — the restart must re-read the file from
		// disk, not reuse what an earlier start loaded. The observable edit:
		// the config now names a different worker, which the named restart
		// must refuse.
		if err := os.WriteFile(cfgPath, []byte("agent: claude\nworker_id: moved\ntemporal:\n  task_queue: daedalus\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		err := workerRestartNamed("daedalus")
		if err == nil || !strings.Contains(err.Error(), `now names worker "moved"`) {
			t.Errorf("workerRestartNamed(edited config) err = %v, want the renamed-worker diagnostic proving a fresh read", err)
		}
	})

	t.Run("missing record is an error", func(t *testing.T) {
		useDaemonDir(t)
		err := workerRestartNamed("ghost")
		if err == nil || !strings.Contains(err.Error(), `no worker "ghost" on record`) {
			t.Errorf("workerRestartNamed(missing) err = %v, want the no-record diagnostic", err)
		}
	})

	t.Run("unloadable record is an error", func(t *testing.T) {
		useDaemonDir(t)
		writeRecord(t, "daedalus", filepath.Join(t.TempDir(), "gone.yaml"))
		err := workerRestartNamed("daedalus")
		if err == nil || !strings.Contains(err.Error(), "load") {
			t.Errorf("workerRestartNamed(unloadable) err = %v, want a load failure", err)
		}
	})

	t.Run("record whose config now names another worker is an error", func(t *testing.T) {
		useDaemonDir(t)
		writeRecord(t, "arete", writeWorkerIDConfig(t, "elsewhere"))
		err := workerRestartNamed("arete")
		if err == nil || !strings.Contains(err.Error(), `now names worker "elsewhere"`) {
			t.Errorf("workerRestartNamed(renamed) err = %v, want the renamed-worker diagnostic", err)
		}
	})

	t.Run("invalid name is an error", func(t *testing.T) {
		useDaemonDir(t)
		if err := workerRestartNamed("../evil"); err == nil {
			t.Error("workerRestartNamed(path smuggle) should be rejected")
		}
	})
}

// TestWorkerRestartAll pins the record-driven roster logic: every recorded
// worker is restarted with its own config in roster order, broken records
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
			t.Errorf("workerRestartAll(empty, live stray) err = %v, want it to name worker old as left alone", err)
		}
	})

	t.Run("restarts every recorded worker with its own config", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		// Recorded out of order: the roster runs sorted, one restart per
		// worker, each under its own recorded config.
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
		for name, want := range map[string]string{"a": cfgA, "b": cfgB} {
			if got := recordedConfigPath(name); got != want {
				t.Errorf("worker %s re-recorded %q, want its own config %q", name, got, want)
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
			for _, name := range []string{"missing", "blank", "moved"} {
				if err == nil || !strings.Contains(err.Error(), "worker "+name) {
					t.Errorf("workerRestartAll err = %v, want it to report skipped worker %s", err, name)
				}
			}
			if err == nil {
				t.Error("workerRestartAll should fail when a recorded worker is skipped")
			}
		})
		if !strings.Contains(out, "== worker good (") {
			t.Errorf("restart-all output %q should still restart the healthy worker", out)
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
// ever read, so the live workers in /tmp/daedalus are never touched. The
// same rejection covers the record-driven `restart <worker>`.
func TestMainRestartAllRejectsConfig(t *testing.T) {
	cfg := validConfig(t)
	const rejection = "-c/--config does not apply to worker restart all or restart <worker>"

	stdout, stderr, code := runMainIn(t, "", "-c", cfg, "worker", "restart", "all")
	if code != 1 {
		t.Errorf("daedalus -c <config> worker restart all exit code = %d, want 1", code)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
	if !strings.HasPrefix(stderr, rejection+"\n\n") {
		t.Errorf("stderr should start with the rejection and a blank line, got %q", stderr)
	}
	if !strings.HasSuffix(stderr, usage) {
		t.Errorf("stderr should end with the usage text, got %q", stderr)
	}

	_, stderr, code = runMainIn(t, "", "-c", cfg, "worker", "restart", "arete")
	if code != 1 {
		t.Errorf("daedalus -c <config> worker restart arete exit code = %d, want 1", code)
	}
	if !strings.HasPrefix(stderr, rejection+"\n\n") {
		t.Errorf("stderr should start with the rejection and a blank line, got %q", stderr)
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

// TestMainRestartNamedNeedsNoConfig pins the by-name dispatch wiring
// in-process, mirroring TestMainRestartAllNeedsNoConfig: `worker restart
// <name>` runs with no config resolvable anywhere (bare cwd, empty HOME)
// and still completes against the record — without the isRestartNamed
// bypass in main, resolveConfigPath would exit(1) with a "load config"
// diagnostic before any record is read. The one recorded worker is
// restarted against a throwaway daemon dir, with the re-exec'd child
// defused by noDaemonSpawn, so main() returns instead of exiting.
func TestMainRestartNamedNeedsNoConfig(t *testing.T) {
	useDaemonDir(t)
	noDaemonSpawn(t)
	cfg := writeWorkerIDConfig(t, "arete")
	writeRecord(t, "arete", cfg)

	t.Setenv("HOME", t.TempDir()) // no ~/.config/daedalus/config.yaml fallback
	t.Chdir(t.TempDir())          // no ./config.yaml

	args := os.Args
	os.Args = []string{"daedalus", "worker", "restart", "arete"}
	defer func() { os.Args = args }()
	out := captureStdout(t, main)

	if !strings.Contains(out, "worker started") {
		t.Errorf("worker restart arete output %q should restart the recorded worker", out)
	}
	if got := recordedConfigPath("arete"); got != cfg {
		t.Errorf("restart re-recorded %q, want %q", got, cfg)
	}
}
