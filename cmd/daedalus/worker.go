package main

import (
	"bytes"
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

// daemonEnv marks a background-managed worker process: the re-exec'd
// daemon clears its pid file on exit and arms the SIGHUP drain restart,
// and a supervisor-run worker (the systemd unit sets the marker) also
// writes its own pid file at boot, so the CLI's pid-file gates treat both
// exactly alike.
const daemonEnv = "DAEDALUS_WORKER_DAEMON"

// logRetention bounds how long daemon logs are kept.
const logRetention = 7 * 24 * time.Hour

// daemonPaths returns the per-worker pid, log, and config-record file
// paths, keyed by the worker's name (config worker_id, else the task
// queue). One daemon per name: a second `worker start` under a live name
// refuses rather than doubles up. The config record holds the absolute path
// of the config the worker was started with — the worker's own copy, so
// `restart <name>` and `restart all` bring it back from any directory —
// plus, on an optional second line, the run type the daemon was started
// with (writeConfigRecord).
func daemonPaths(name string) (pidFile, logFile, confFile string) {
	return filepath.Join(daemonDir, "worker-"+name+".pid"),
		filepath.Join(daemonDir, "worker-"+name+".log"),
		filepath.Join(daemonDir, "worker-"+name+".conf")
}

// readConfigRecord returns the config path and run type recorded for the
// worker named name: line 1 is the config path, the optional line 2 the
// run type (writeConfigRecord appends it only for a typed start). Both are
// "" when no record exists (the worker predates records, or the record was
// wiped); an empty or unreadable record counts as no record.
func readConfigRecord(name string) (path, workerType string) {
	_, _, confFile := daemonPaths(name)
	data, err := os.ReadFile(confFile)
	if err != nil {
		return "", ""
	}
	path, workerType, _ = strings.Cut(strings.TrimSpace(string(data)), "\n")
	return path, workerType
}

// recordedConfigPath returns the config file the worker named name was
// started with — readConfigRecord's first line — or "" when no record
// exists.
func recordedConfigPath(name string) string {
	path, _ := readConfigRecord(name)
	return path
}

// writeConfigRecord records the config a worker is started (or restarted)
// with: line 1 the absolute config path, line 2 the run type when one is
// given. The drain-restart re-exec and the record-driven restarters both
// read the record back, so a run type survives it only when written here —
// which is why the record-driven paths pass "": a run config is never
// persisted, and their revival is untyped, exactly as their spawn always
// was.
func writeConfigRecord(confFile, configPath, workerType string) error {
	// The daemon dir may not exist yet (first-ever start on a fresh host):
	// this write is the dir's first touch on the workerStart path, which
	// reaches it before spawnDaemon's own MkdirAll.
	if err := os.MkdirAll(filepath.Dir(confFile), 0o755); err != nil {
		return fmt.Errorf("write config record %s: %w", confFile, err)
	}
	record := configPath
	if workerType != "" {
		record += "\n" + workerType
	}
	if err := os.WriteFile(confFile, []byte(record), 0o644); err != nil {
		return fmt.Errorf("write config record %s: %w", confFile, err)
	}
	return nil
}

// workerActions lists the `worker` subcommands, in the order the unknown-
// action diagnostic names them. It is the shared source of that diagnostic
// (main.go's worker dispatch) and the shell completion's candidates, so a
// new action is dispatched, diagnosed, and completed from one list.
var workerActions = []string{"start", "stop", "status", "restart", "wakeup", "report", "foreground"}

// isRestartAll reports whether args spell out `worker restart all` (or its
// --all spelling; -a is taken by run's --append) — one of the invocations
// whose behavior is record-driven end to end, needing no config of its own.
// The restart scope's force flag (-f/--force after the `worker restart`
// prefix) is ignored wherever it sits.
func isRestartAll(args []string) bool {
	args = stripRestartForce(args)
	return len(args) == 3 && args[0] == "worker" && args[1] == "restart" &&
		(args[2] == "all" || args[2] == "--all")
}

// isRestartNamed reports the worker name in `worker restart <name>` — the
// single-worker counterpart of `restart all`: record-driven, so it needs no
// config of its own and works from any directory. Force flags are ignored
// like isRestartAll's.
func isRestartNamed(args []string) (string, bool) {
	args = stripRestartForce(args)
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

// envWithoutWorkerVars drops every environment variable the worker
// ecosystem derives from config or exports itself (config.AllWorkerEnvVars:
// the provider sections and the fallback/reviewer/verdict-repair/tool-relay/
// metrics channels, plus the worker's own exports — DAEDALUS_AGENT, the
// concurrency clamps, the mode switches, the worker name, OPENCODE_CONFIG).
// The spawned daemon gets provider values only from the config file it
// loads — runWorker re-exports every member of that set EXCEPT
// OPENCODE_CONFIG, which must never return ambient (its only legitimate
// channel is the opencode bridge's round-scoped staging) — so a stale export
// in the invoking shell (e.g. an old ANTHROPIC_API_KEY, or an
// OPENCODE_CONFIG left over from an unrelated project) can never win over
// the config. Manual `worker foreground` runs keep the inherit semantics:
// this scrub applies only to the detached daemon.
func envWithoutWorkerVars(environ []string) []string {
	names := config.AllWorkerEnvVars()
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

// spawnDaemon is the one detached-daemon spawn shape (workerStart,
// metricsStart): it re-executes this binary with args in its own session,
// stdout and stderr appended into logFile, on an environment scrubbed of
// every worker-exported variable (envWithoutWorkerVars) with the daemon
// marker re-appended, and records the pid in pidFile. The scrub lives
// here so a new spawn site inherits it instead of copying the block and
// dropping the line. Returns the daemon's pid.
func spawnDaemon(pidFile, logFile string, args ...string) (int, error) {
	if err := os.MkdirAll(daemonDir, 0o755); err != nil {
		return 0, fmt.Errorf("create %s: %w", daemonDir, err)
	}
	pruneOldLogs()
	log, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, fmt.Errorf("open log %s: %w", logFile, err)
	}
	defer log.Close()
	self, err := os.Executable()
	if err != nil {
		return 0, fmt.Errorf("resolve executable: %w", err)
	}
	cmd := exec.Command(self, args...)
	cmd.Env = append(envWithoutWorkerVars(os.Environ()), daemonEnv+"=1")
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // detach from the terminal
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start daemon: %w", err)
	}
	pid := cmd.Process.Pid
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(pid)), 0o644); err != nil {
		return 0, fmt.Errorf("write pid file %s: %w", pidFile, err)
	}
	cmd.Process.Release() // the daemon outlives this process
	return pid, nil
}

// workerStart launches the worker as a detached daemon: it re-executes
// itself with `worker foreground`, redirected into the per-queue log, in
// its own session so the terminal is released immediately. workerType is
// the invoking command's run config (-t/--type): when set it travels with
// the re-exec — and onto the config record's second line, so the daemon's
// drain-restart re-exec comes back typed. The optional trailing force
// skips only the live-pid refusal — the --force restart's immediate swap
// onto a live name; every other check (the metrics gate included) stays.
func workerStart(cfg config.Config, configPath, workerType string, force ...bool) error {
	pidFile, logFile, confFile := daemonPaths(cfg.WorkerName())
	if !forceSet(force) {
		if pid, ok := readLivePid(pidFile); ok {
			return fmt.Errorf("already running (pid %d) — use 'daedalus worker restart' or 'stop'", pid)
		}
	}
	// The metrics gate (Q12): with metrics enabled, this worker's rounds
	// stage their provider calls onto the host metrics relay. A relay that
	// is not up would fail every round at dial time instead, so fail the
	// start with the remedy instead.
	if cfg.MetricsEnabled() {
		if err := metricsGateCheck(cfg); err != nil {
			return err
		}
	}
	childArgs := []string{"worker", "foreground", "-c", configPath}
	// The resolved config path is absolute (resolveConfigPath), so the
	// daemon never re-resolves against the working directory it was
	// started from — it keeps serving if that directory goes away. The run
	// type must survive the re-exec or the daemon silently comes up
	// untyped; record-driven restarters pass "" and the child stays
	// untyped too (a run config is never persisted).
	if workerType != "" {
		childArgs = append(childArgs, "-t", workerType)
	}
	// Record the config this worker will be started with — path, and the
	// run type when given — so restarts, here or via `restart --all` or
	// the daemon's own drain-restart re-exec, reuse it rather than
	// whatever the invoking directory resolves to. Written before the
	// spawn: a failed write leaves only a harmlessly-updated record,
	// instead of a running daemon the record no longer describes.
	if err := writeConfigRecord(confFile, configPath, workerType); err != nil {
		return err
	}
	pid, err := spawnDaemon(pidFile, logFile, childArgs...)
	if err != nil {
		return err
	}

	fmt.Printf("worker started (pid %d)\n", pid)
	fmt.Printf("  log:     %s\n", logFile)
	fmt.Printf("  stop:    daedalus worker stop\n")
	fmt.Printf("  restart: daedalus worker restart %s\n", cfg.WorkerName())
	return nil
}

// workerStop gracefully terminates the daemon: SIGTERM lets Temporal's
// worker drain, then the pid file is cleared once the process is gone —
// and only while it still names the pid that was signaled, so a
// replacement started concurrently (a force swap) keeps its record; a
// daemon still alive after the grace keeps the file it owns.
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
		// Still alive after the grace: the daemon owns the pid file — leave it.
		return fmt.Errorf("pid %d still alive after 30s — it may need SIGKILL; log: %s", pid, logFile)
	}
	removePidFileFor(pidFile, pid)
	fmt.Printf("worker stopped (pid %d)\n", pid)
	return nil
}

// workerRestart restarts the worker cfg names with the invoking command's
// config, whatever it now contains. The config is re-read from disk at
// every invocation, so values changed since the worker was started
// — a rotated API key, a new model — take effect on every restart, and the
// restart rewrites the record to this config. The record is not consulted:
// the config the restart is invoked with is the worker's new settings, and
// its worker name (worker_id, else task queue) picks the worker. To
// restart a worker with its recorded config instead, from any directory,
// address it by name: `worker restart <id>`. workerType is the invoking
// command's run config (-t/--type); the record-driven paths
// (workerRestartNamed, workerRestartAll) pass "" — they bring the daemon
// back untyped, since a run config is not on record.
//
// On a live daemon the restart is non-blocking: the config is validated
// (the metrics gate included) and re-recorded, then a SIGHUP schedules the
// daemon to drain every active task and re-exec itself on the fresh record
// — the command returns as soon as the signal is delivered, with no
// 30s wait and no pid-file change. The optional trailing force swaps
// immediately instead: SIGTERM to the old daemon and a replacement started
// on the spot (workerStart's live-pid refusal skipped), never blocking on
// the old process's drain. A worker that is not running starts exactly as
// `worker start` would.
func workerRestart(cfg config.Config, cliConfigPath, workerType string, force ...bool) error {
	pidFile, _, confFile := daemonPaths(cfg.WorkerName())
	if forceSet(force) {
		// Force keeps the two checks that would otherwise leave nothing
		// running. The config load happened in the caller; the metrics gate
		// fires here, before anything is signaled — a forced restart onto a
		// config that cannot boot must not trade a working worker for a
		// dead one. (workerStart probes the gate again: a second
		// worst-case two-second localhost dial, never a correctness
		// problem.)
		if cfg.MetricsEnabled() {
			if err := metricsGateCheck(cfg); err != nil {
				return err
			}
		}
		if old, ok := readLivePid(pidFile); ok {
			if err := syscall.Kill(old, syscall.SIGTERM); err != nil {
				return fmt.Errorf("signal pid %d: %w", old, err)
			}
			fmt.Printf("force restart: signaled the old daemon (pid %d) — its drain is its own; starting the replacement now\n", old)
		}
		return workerStart(cfg, cliConfigPath, workerType, true)
	}
	if pid, ok := readLivePid(pidFile); ok {
		if cfg.MetricsEnabled() {
			if err := metricsGateCheck(cfg); err != nil {
				return err
			}
		}
		if err := writeConfigRecord(confFile, cliConfigPath, workerType); err != nil {
			return err
		}
		if err := syscall.Kill(pid, syscall.SIGHUP); err != nil {
			return fmt.Errorf("signal pid %d: %w", pid, err)
		}
		fmt.Printf("restart scheduled (drains current tasks) for worker %s (pid %d)\n", cfg.WorkerName(), pid)
		return nil
	}
	return workerStart(cfg, cliConfigPath, workerType)
}

// forceSet reports the optional trailing force flag's value — the
// start/restart family takes force as a variadic so existing callers keep
// their call shape; the first value wins.
func forceSet(force []bool) bool {
	return len(force) > 0 && force[0]
}

// workerRestartNamed restarts the worker on record under name — the
// single-worker counterpart of `restart all`, addressable from any
// directory. The recorded config is re-read from disk, so edits since the
// last start (a rotated API key, a new model) apply. A missing record, a
// record that no longer loads, or one whose config now names a different
// worker (its worker_id or task_queue was edited) is an error, not a
// best-effort fallback: the named restart must target the named worker.
// The optional trailing force passes through to workerRestart.
func workerRestartNamed(name string, force ...bool) error {
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
	return workerRestart(cfg, recorded, "", force...)
}

// workerRestartAll restarts every worker with a config record on file — the
// single call that cycles a multi-worker deployment, each worker with its
// own recorded config. A worker whose config no longer loads is reported and
// skipped; the rest still get restarted. The optional trailing force passes
// through to every named restart.
func workerRestartAll(force ...bool) error {
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
	// The stale-worker sweep is host-wide by nature and rides only this
	// command's force scope — the one form whose reach is every worker (a
	// single-worker restart or a plain start never signals a worker it was
	// not addressed to). Its pid-file guard leaves managed workers to the
	// cycle below; only pid-file-less orphans running a deleted binary are
	// reaped here. Best-effort — it never fails the restart.
	if forceSet(force) {
		sweepStaleWorkers()
	}
	var skipped []error
	for _, name := range names {
		if recorded := recordedConfigPath(name); recorded != "" {
			fmt.Printf("== worker %s (%s)\n", name, recorded)
		}
		if err := workerRestartNamed(name, force...); err != nil {
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

// removeOwnPidFile removes pidFile when its contents name this process —
// the daemon's exit cleanup, guarded so a --force swap cannot have the
// exiting daemon delete the file that already names its replacement: the
// unlink happens only while the record is still ours.
func removeOwnPidFile(pidFile string) {
	removePidFileFor(pidFile, os.Getpid())
}

// removePidFileFor removes pidFile only while its contents still name pid —
// the stop path's clear, the same guard the daemon exit runs with its own
// pid: a concurrent force swap rewrites the file to name its replacement,
// and the waiter must not delete the new daemon's record.
func removePidFileFor(pidFile string, pid int) {
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return
	}
	if onFile, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && onFile == pid {
		os.Remove(pidFile)
	}
}

// pidAlive reports whether the process exists and is live. Signal 0
// probes without delivering anything — but it is also answered by a
// zombie: a daemon is spawned released and can stay its spawner's
// unreaped child for as long as that spawner lives (a `run` that started
// a worker waits up to runWaitTimeout), and a zombie is gone for every
// daedalus purpose. Its /proc state says so; on anything without /proc
// the read fails and existence alone decides, as before.
func pidAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	if err != nil {
		return err == syscall.EPERM
	}
	if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
		// stat's second field (the comm, wrapped in parentheses) may itself
		// contain parentheses and spaces; the state letter sits after the
		// LAST closing paren, and Z is the zombie state.
		if i := bytes.LastIndexByte(data, ')'); i >= 0 && i+2 < len(data) {
			return data[i+2] != 'Z'
		}
	}
	return true
}

// deletedExeSuffix is what /proc/<pid>/exe readlink appends when the
// binary a running process was exec'd from no longer exists on disk.
const deletedExeSuffix = " (deleted)"

// sweepStaleWorkers reaps leftover worker processes a stop/restart cannot
// take down: a worker whose binary has since been deleted or replaced on
// disk — /proc/<pid>/exe ends " (deleted)" — can never be the current code,
// yet it keeps polling its task queue as a live, invisible poller. The pid
// file no longer names it (a stop removes the file, a restart's start
// rewrites it), so workerStop addresses nothing and the orphan survives
// silently. The sweep rides only "worker restart all --force" — the one
// command whose scope is every worker on the host; a single-worker
// restart or a plain start signals exactly the worker it addresses, never
// its siblings. Even there, a candidate a live worker pid file still
// names is managed — stop and restart reach it — and is skipped, so the
// sweep's targets are the pid-file-less orphans only. Identification is
// deliberately precise — deleted exe, the
// daedalus basename, AND a consecutive "worker foreground" argv (the
// daemon's re-exec shape) — so a healthy worker of another deployment
// (binary intact) or any other deleted-binary process of ours is never
// touched, and the sweep skips its own pid: it runs from the restart-all
// process itself. Matched processes get workerStop's own semantics — SIGTERM,
// then a workerStopGrace drain watched through pidAlive — and the daemon-dir
// pid file naming a reaped pid is removed. Survivors are named loudly and
// left alone; nothing here signals a process group (ai-jail/bwrap children
// live under the worker's pid), only the one stale pid. Best-effort by
// contract: a candidate whose /proc entries cannot be read is skipped, and
// a failed sweep never fails the restart.
func sweepStaleWorkers() {
	self := os.Getpid()
	ownExe, err := os.Executable()
	if err != nil {
		return
	}
	ownName := filepath.Base(strings.TrimSuffix(ownExe, deletedExeSuffix))
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return
	}
	var stale []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
		if err != nil || !strings.HasSuffix(exe, deletedExeSuffix) {
			continue
		}
		if filepath.Base(strings.TrimSuffix(exe, deletedExeSuffix)) != ownName {
			continue
		}
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			continue
		}
		args := strings.Split(string(bytes.TrimRight(data, "\x00")), "\x00")
		if !isWorkerForegroundArgv(args) {
			continue
		}
		if len(workerPidFilesHolding(pid)) > 0 {
			// A live pid file still names it: a managed worker — stop
			// and restart reach it. Never sweep fodder.
			continue
		}
		stale = append(stale, pid)
	}
	for _, pid := range stale {
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
			continue
		}
		deadline := time.Now().Add(workerStopGrace)
		for time.Now().Before(deadline) && pidAlive(pid) {
			time.Sleep(200 * time.Millisecond)
		}
		if pidAlive(pid) {
			fmt.Printf("worker sweep: stale worker pid %d is running a deleted binary and ignored SIGTERM — still alive; kill it by hand\n", pid)
			continue
		}
		fmt.Printf("worker sweep: reaped stale worker pid %d (was running a deleted binary)\n", pid)
		removePidFilesFor(pid)
	}
}

// isWorkerForegroundArgv reports whether args is a `worker foreground`
// command line — the daemon's re-exec shape (workerStart's childArgs).
// Consecutive tokens, so flag position does not matter.
func isWorkerForegroundArgv(args []string) bool {
	for i, a := range args {
		if a == "worker" && i+1 < len(args) && args[i+1] == "foreground" {
			return true
		}
	}
	return false
}

// workerPidFilesHolding lists the worker-*.pid files in the daemon dir
// whose content names pid — daemon files are keyed by worker name
// (worker_id, else task queue) and the sweep is name-agnostic by design:
// the stale worker may belong to any of them. Best-effort: unreadable
// entries are skipped.
func workerPidFilesHolding(pid int) []string {
	entries, err := os.ReadDir(daemonDir)
	if err != nil {
		return nil
	}
	var files []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "worker-") || !strings.HasSuffix(name, ".pid") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(daemonDir, name))
		if err != nil {
			continue
		}
		if content, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && content == pid {
			files = append(files, filepath.Join(daemonDir, name))
		}
	}
	return files
}

// removePidFilesFor removes every pid file workerPidFilesHolding names —
// the sweep's cleanup for a pid it just reaped. Best-effort.
func removePidFilesFor(pid int) {
	for _, f := range workerPidFilesHolding(pid) {
		os.Remove(f)
	}
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
