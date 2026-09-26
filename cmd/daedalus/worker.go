package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
)

// daemonDir hosts the worker daemon's runtime files: its pid file and
// appended logs, plus the per-task task logs the activities write next to
// them (activities.TaskLogDir — one source of truth, so the prune below
// covers both). Overridable in tests.
var daemonDir = activities.TaskLogDir

// daemonEnv marks the re-exec'd background worker process so the child
// knows to clear the pid file on exit.
const daemonEnv = "DAEDALUS_WORKER_DAEMON"

// logRetention bounds how long daemon logs are kept.
const logRetention = 7 * 24 * time.Hour

// daemonPaths returns the per-worker pid, log, and config-record file
// paths, keyed by the worker's name (config worker_id, else the task
// queue). One daemon per name: a second `worker start` under a live name
// refuses rather than doubles up. The config record holds the absolute path
// of the config the worker was started with — the worker's own copy, so
// `restart <name>` and `restart all` bring it back from any directory.
func daemonPaths(name string) (pidFile, logFile, confFile string) {
	return filepath.Join(daemonDir, "worker-"+name+".pid"),
		filepath.Join(daemonDir, "worker-"+name+".log"),
		filepath.Join(daemonDir, "worker-"+name+".conf")
}

// recordedConfigPath returns the config file the worker named name was
// started with, or "" when no record exists (the worker predates records,
// or the record was wiped). An empty or unreadable record counts as no
// record.
func recordedConfigPath(name string) string {
	_, _, confFile := daemonPaths(name)
	data, err := os.ReadFile(confFile)
	if err != nil {
		return ""
	}
	path := strings.TrimSpace(string(data))
	if path == "" {
		return ""
	}
	return path
}

// isRestartAll reports whether args spell out `worker restart all` (or its
// --all spelling; -a is taken by run's --append) — one of the invocations
// whose behavior is record-driven end to end, needing no config of its own.
func isRestartAll(args []string) bool {
	return len(args) == 3 && args[0] == "worker" && args[1] == "restart" &&
		(args[2] == "all" || args[2] == "--all")
}

// isRestartNamed reports the worker name in `worker restart <name>` — the
// single-worker counterpart of `restart all`: record-driven, so it needs no
// config of its own and works from any directory.
func isRestartNamed(args []string) (string, bool) {
	if len(args) == 3 && args[0] == "worker" && args[1] == "restart" && !isRestartAll(args) {
		return args[2], true
	}
	return "", false
}

// isWorkerStatus reports whether args spell out `worker status` — the other
// record-driven invocation: it lists workers from the per-worker records,
// needing no config of its own.
func isWorkerStatus(args []string) bool {
	return len(args) == 2 && args[0] == "worker" && args[1] == "status"
}

// recordedWorkers lists the worker names with a config record on file,
// sorted — the roster of workers this deployment has started, running or
// not. A worker's name is its config worker_id, else its task queue. A
// missing daemon directory is simply nothing on record, not an error: on a
// fresh machine the caller's "start one first" diagnostic says more than a
// ReadDir failure would.
func recordedWorkers() ([]string, error) {
	entries, err := os.ReadDir(daemonDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", daemonDir, err)
	}
	var names []string
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".conf")
		if !ok || !strings.HasPrefix(name, "worker-") || name == "worker-" {
			continue
		}
		names = append(names, strings.TrimPrefix(name, "worker-"))
	}
	sort.Strings(names)
	return names, nil
}

// envWithoutProviderVars drops every config-derived provider variable (see
// config.ProviderEnvVars) from an inherited environment. The spawned daemon
// gets provider values only from the config file it loads — runWorker
// re-exports them — so a stale export in the invoking shell (e.g. an old
// ANTHROPIC_API_KEY) can never win over a rotated config. Manual
// `worker foreground` runs keep the inherit semantics: this scrub applies
// only to the detached daemon.
func envWithoutProviderVars(environ []string) []string {
	names := config.ProviderEnvVars()
	scrub := make(map[string]bool, len(names))
	for _, name := range names {
		scrub[name] = true
	}
	var out []string
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if scrub[name] {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// workerStart launches the worker as a detached daemon: it re-executes
// itself with `worker foreground`, redirected into the per-queue log, in
// its own session so the terminal is released immediately. workerType is
// the invoking command's run config (-t/--type): when set it travels with
// the re-exec so the daemon starts with the same poller selection.
func workerStart(cfg config.Config, configPath, workerType string) error {
	pidFile, logFile, confFile := daemonPaths(cfg.WorkerName())
	if pid, ok := readLivePid(pidFile); ok {
		return fmt.Errorf("already running (pid %d) — use 'daedalus worker restart' or 'stop'", pid)
	}
	if err := os.MkdirAll(daemonDir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", daemonDir, err)
	}
	pruneOldLogs()

	log, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open log %s: %w", logFile, err)
	}
	defer log.Close()

	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}
	// The resolved config path is absolute (resolveConfigPath), so the
	// daemon never re-resolves against the working directory it was
	// started from — it keeps serving if that directory goes away.
	childArgs := []string{"worker", "foreground", "-c", configPath}
	// The run type must survive the re-exec or the daemon silently comes
	// up untyped; record-driven restarters pass "" and the child stays
	// untyped too (a run config is never persisted).
	if workerType != "" {
		childArgs = append(childArgs, "-t", workerType)
	}
	// Record the config this worker will be started with so restarts —
	// here or via `restart --all` — reuse it rather than whatever the
	// invoking directory resolves to. Written before the spawn: a failed
	// write leaves only a harmlessly-updated record, instead of a running
	// daemon the record no longer describes.
	if err := os.WriteFile(confFile, []byte(configPath), 0o644); err != nil {
		return fmt.Errorf("write config record %s: %w", confFile, err)
	}
	cmd := exec.Command(self, childArgs...)
	cmd.Env = append(envWithoutProviderVars(os.Environ()), daemonEnv+"=1")
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // detach from the terminal
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start daemon: %w", err)
	}
	pid := cmd.Process.Pid
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(pid)), 0o644); err != nil {
		return fmt.Errorf("write pid file %s: %w", pidFile, err)
	}
	cmd.Process.Release() // the daemon outlives this process

	fmt.Printf("worker started (pid %d)\n", pid)
	fmt.Printf("  log:     %s\n", logFile)
	fmt.Printf("  stop:    daedalus worker stop\n")
	fmt.Printf("  restart: daedalus worker restart %s\n", cfg.WorkerName())
	return nil
}

// workerStop gracefully terminates the daemon: SIGTERM lets Temporal's
// worker drain, then the pid file is cleared once the process is gone.
func workerStop(cfg config.Config) error {
	pidFile, logFile, _ := daemonPaths(cfg.WorkerName())
	pid, ok := readLivePid(pidFile)
	if !ok {
		os.Remove(pidFile)
		fmt.Println("worker not running")
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("signal pid %d: %w", pid, err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := readLivePid(pidFile); !ok {
			break
		}
		if !pidAlive(pid) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if pidAlive(pid) {
		os.Remove(pidFile)
		return fmt.Errorf("pid %d still alive after 30s — it may need SIGKILL; log: %s", pid, logFile)
	}
	os.Remove(pidFile)
	fmt.Printf("worker stopped (pid %d)\n", pid)
	return nil
}

// workerRestart stops and restarts the worker cfg names with the invoking
// command's config, whatever it now contains. The config is re-read from
// disk at every invocation, so values changed since the worker was started
// — a rotated API key, a new model — take effect on every restart, and the
// start that follows rewrites the record to this config. The record is not
// consulted: the config the restart is invoked with is the worker's new
// settings, and its worker name (worker_id, else task queue) picks the
// worker. To restart a worker with its recorded config instead, from any
// directory, address it by name: `worker restart <id>`. workerType is the
// invoking command's run config (-t/--type); the record-driven paths
// (workerRestartNamed, workerRestartAll) pass "" — they bring the daemon
// back untyped, since a run config is not on record.
func workerRestart(cfg config.Config, cliConfigPath, workerType string) error {
	if err := workerStop(cfg); err != nil {
		return err
	}
	return workerStart(cfg, cliConfigPath, workerType)
}

// workerRestartNamed restarts the worker on record under name — the
// single-worker counterpart of `restart all`, addressable from any
// directory. The recorded config is re-read from disk, so edits since the
// last start (a rotated API key, a new model) apply. A missing record, a
// record that no longer loads, or one whose config now names a different
// worker (its worker_id or task_queue was edited) is an error, not a
// best-effort fallback: the named restart must target the named worker.
func workerRestartNamed(name string) error {
	if err := config.ValidateWorkerID(name); err != nil {
		return err
	}
	recorded := recordedConfigPath(name)
	if recorded == "" {
		return fmt.Errorf("no worker %q on record in %s — start one first", name, daemonDir)
	}
	cfg, err := config.Load(recorded)
	if err != nil {
		return fmt.Errorf("load %s: %w", recorded, err)
	}
	if got := cfg.WorkerName(); got != name {
		return fmt.Errorf("recorded config %s now names worker %q — restart it as %q instead", recorded, got, got)
	}
	// Named restarts always come back untyped: a run config is not on
	// record, and parseFlags rejects -t on this invocation outright.
	return workerRestart(cfg, recorded, "")
}

// workerRestartAll restarts every worker with a config record on file — the
// single call that cycles a multi-worker deployment, each worker with its
// own recorded config. A worker whose config no longer loads is reported and
// skipped; the rest still get restarted.
func workerRestartAll() error {
	names, err := recordedWorkers()
	if err != nil {
		return err
	}
	onRecord := make(map[string]bool, len(names))
	for _, name := range names {
		onRecord[name] = true
	}
	if len(names) == 0 {
		// Even with nothing on record there may be live workers this
		// command cannot restart; name them rather than imply none exist.
		if live := runningUnrecordedWorkers(onRecord); len(live) > 0 {
			return fmt.Errorf("no workers on record in %s — start one first (these are running without a record, left alone: %s)",
				daemonDir, strings.Join(live, ", "))
		}
		return fmt.Errorf("no workers on record in %s — start one first", daemonDir)
	}
	var skipped []error
	for _, name := range names {
		if recorded := recordedConfigPath(name); recorded != "" {
			fmt.Printf("== worker %s (%s)\n", name, recorded)
		}
		if err := workerRestartNamed(name); err != nil {
			skipped = append(skipped, fmt.Errorf("worker %s: %w", name, err))
		}
	}
	// A worker with a live pid but no record (started before records
	// existed, or with the record wiped) is not this command's to restart;
	// report it instead of silently leaving it out.
	if live := runningUnrecordedWorkers(onRecord); len(live) > 0 {
		fmt.Printf("skipped %d running worker(s) without a config record: %s\n",
			len(live), strings.Join(live, ", "))
	}
	return errors.Join(skipped...)
}

// runningUnrecordedWorkers lists the worker names with a live pid file but
// no config record — workers running from before records existed.
func runningUnrecordedWorkers(onRecord map[string]bool) []string {
	entries, err := os.ReadDir(daemonDir)
	if err != nil {
		return nil
	}
	var live []string
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".pid")
		if !ok || !strings.HasPrefix(name, "worker-") || name == "worker-" {
			continue
		}
		worker := strings.TrimPrefix(name, "worker-")
		if onRecord[worker] {
			continue
		}
		if _, ok := readLivePid(filepath.Join(daemonDir, e.Name())); ok {
			live = append(live, worker)
		}
	}
	sort.Strings(live)
	return live
}

// readLivePid returns the pid recorded in the pid file when the file exists
// and that process is still alive; stale files are ignored.
func readLivePid(pidFile string) (int, bool) {
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 || !pidAlive(pid) {
		return 0, false
	}
	return pid, true
}

// pidAlive reports whether the process exists (signal 0 probes without
// delivering anything).
func pidAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

// pruneOldLogs removes daemon logs untouched for longer than the retention
// window. Best-effort: a failed prune never blocks the daemon.
func pruneOldLogs() {
	entries, err := os.ReadDir(daemonDir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-logRetention)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		if info, err := e.Info(); err == nil && info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(daemonDir, e.Name()))
		}
	}
}
