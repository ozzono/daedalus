// Command daedalus is the entrypoint for the Daedalus control plane: it runs
// Temporal workers and starts feature-development pipelines.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/version"
	"github.com/ozzono/daedalus/internal/workflows"
)

// runWaitTimeout bounds how long `daedalus run` waits for a pipeline to
// finish. It is a client-side patience budget, not a coverage guarantee:
// both review loops are unbounded, so a many-round run can exceed it with
// no quota trouble at all, and a quota-exhaustion streak (five hourly
// sleeps, with the budget reset by any completed round) starting late in a
// long run can push the park itself past the deadline — the operator then
// sees the deadline error below instead of the park's resume hint. The
// wait giving out does not stop the workflow; `daedalus attach` reconnects
// to it and reports the eventual outcome, parked or otherwise.
const runWaitTimeout = 12 * time.Hour

// defaultConfigPath is used when -c/--config is not given: looked up in the
// working directory, then in the user-level home (see resolveConfigPath).
const defaultConfigPath = "config.yaml"

// homeConfigDir is the config's user-level home, ~/.config/daedalus. A
// config there is found from any directory, so worker and client commands
// work wherever they are invoked.
const homeConfigDir = ".config/daedalus"

// defaultWorkflowName is used when -w/--workflow is not given.
const defaultWorkflowName = "feature-dev"

// workflowRegistry maps the names accepted by -w/--workflow to workflow
// functions. Adding a flow later means adding an entry here (plus any needed
// input shaping in startPipeline) — no CLI changes.
var workflowRegistry = map[string]any{
	"feature-dev": workflows.FeatureDevWorkflow,
}

var usage = `Daedalus — a local, sandboxed AI developer agent control plane.

Usage:
  daedalus -v, --version        Print the version and exit.
  daedalus init
      Write config-example.yaml in the current directory: every field with
      its default value, fully commented. Copy it to config.yaml and edit.
  daedalus [-c config.yaml] worker [start|stop|status|restart|foreground]
      Run the Temporal worker hosting the pipelines. The default action,
      start, runs it as a detached daemon: logs append to
      /tmp/daedalus/worker-<queue>.log (pruned to the past week), the pid
      lives in /tmp/daedalus/worker-<queue>.pid, one daemon per task queue.
      stop drains it gracefully (SIGTERM); restart is stop + start; status
      reports pid and log path. foreground runs attached to this terminal.
      anthropic.key is optional — if unset, the jailed agent authenticates
      through the worker's inherited environment or its own login.
  daedalus [-c config.yaml] list [max]
      List past and current sessions (pipeline runs) on this task queue,
      newest first: session id, status, last interaction datetime (close
      time once closed, start time while running). Defaults to the 10 most
      recent; pass a larger max to list more.
  daedalus [-c config.yaml] [-w workflow] run [-d] [-p prefix] <repo-path> <issue-id> "<prompt>"
      Start a pipeline for an issue. -w selects the workflow
      (default: feature-dev; available: ` + workflowNames() + `).
      The "<prompt>" argument is the full task description; pass
      -f/--file <path> to read it from a file instead — either the
      argument or the file, never both.
      -p/--prefix names the preserved branch <prefix>/issue-<id>-<timestamp>
      for this run, overriding the config's branch_prefix (the issue part of
      the name stays as given).
      -d/--detach starts the pipeline and returns immediately instead of
      blocking until it finishes; "daedalus attach" reconnects later.
  daedalus [-c config.yaml] run -a <workflow-id> "<prompt>"
      Append instructions to a pipeline that is already running instead of
      starting a new run: the prompt is folded into the agent's next fix
      round (same as "daedalus guide"); -f/--file works here too.
      -d has no effect in append mode, and -p is rejected there: the run
      keeps the branch prefix it started with.
  daedalus [-c config.yaml] guide <workflow-id> "<message>"
      Send operator guidance to a running pipeline: the message is folded
      into the agent's next fix prompt, steering a stuck review loop
      without restarting the run.
  daedalus [-c config.yaml] continue <workflow-id> "<prompt>"
      Resume a closed session (canceled, failed, or parked — an
      API-exhaustion halt past its hourly heartbeats, or a reviewer
      NEEDS_MAINTAINER halt on an impossible task) under a new prompt: the
      aborted attempt's work, preserved on its aborted/ branch, becomes
      the new run's starting point, and that attempt's last review
      feedback is folded into the opening prompt. -d works here too; -p
      does not — the continued run keeps the original run's branch prefix.
  daedalus [-c config.yaml] attach <workflow-id>
      Reattach to a running (or already finished) pipeline, block until it
      finishes, and report the outcome — the other half of "run -d".
      <workflow-id> is the identifier printed by "run" (also visible in
      "temporal workflow list").

Configuration is read from the first of ./config.yaml and
~/.config/daedalus/config.yaml (-c/--config to override the path); the
latter makes the CLI work from any directory. See config-example.yaml for
all fields and their defaults:
  agent                Jailed agent CLI: claude or opencode (default claude)
  branch_prefix        Prefix for preserved branches, <prefix>/issue-<id>-<ts>
                       (default daedalus; run -p/--prefix overrides per run)
  temporal.host        Temporal frontend address   (default 127.0.0.1:7233)
  temporal.ui_port     Temporal UI port, shown at worker startup (default 8233)
  temporal.task_queue  routing key; distinct projects or flows sharing one
                       Temporal server use distinct queues (default daedalus)
  anthropic.url        Anthropic API base URL       (default https://api.anthropic.com)
  anthropic.key        Anthropic API key            (optional — skipped if unset)
  anthropic.model      Model for the jailed agent   (agent default if unset)
  anthropic.timeout_ms Agent API timeout in ms, exported as API_TIMEOUT_MS
                       (default 3000000 = 50 minutes)
  openai.url/key/model Optional OpenAI settings injected into the agent environment

For a local Temporal dev server matching the defaults:
  temporal server start-dev
`

func main() {
	configPath, args, err := parseFlags(os.Args[1:])
	if err != nil {
		usageFail("%v", err)
	}
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	// Every subcommand except the no-config ones (help, version, init)
	// loads a configuration; resolve its location once, up front, so the
	// subcommand — and the daemon `worker start` re-executes — agree on
	// it wherever the CLI is invoked from.
	switch args[0] {
	case "-h", "--help", "help", "-v", "--version", "init":
	default:
		path, err := resolveConfigPath(configPath.configPath)
		if err != nil {
			exitf("load config: %v", err)
		}
		configPath.configPath = path
	}

	switch args[0] {
	case "-h", "--help", "help":
		fmt.Print(usage)
	case "-v", "--version":
		fmt.Println(version.String())
	case "init":
		if err := writeExampleConfig("."); err != nil {
			fail("init", err)
		}
	case "list":
		// daedalus list [max] — the most recent sessions, newest first.
		max := 10
		if len(args) == 2 {
			n, err := strconv.Atoi(args[1])
			if err != nil || n <= 0 {
				usageFail("list takes an optional maximum number of sessions (got %q)", args[1])
			}
			max = n
		} else if len(args) > 2 {
			usageFail("list takes at most one argument")
		}
		cfg := loadConfig(configPath.configPath)
		if err := listPipelines(cfg, max); err != nil {
			fail("list", err)
		}
	case "worker":
		// daedalus worker [start|stop|status|restart|foreground] — default
		// start runs the worker as a detached daemon.
		action := "start"
		if len(args) == 2 {
			action = args[1]
		} else if len(args) > 2 {
			usageFail("worker takes at most one action")
		}
		cfg := loadConfig(configPath.configPath)
		switch action {
		case "start":
			if err := workerStart(cfg, configPath.configPath); err != nil {
				fail("worker start", err)
			}
		case "stop":
			if err := workerStop(cfg); err != nil {
				fail("worker stop", err)
			}
		case "status":
			workerStatus(cfg)
		case "restart":
			if err := workerStop(cfg); err != nil {
				fail("worker stop", err)
			}
			if err := workerStart(cfg, configPath.configPath); err != nil {
				fail("worker start", err)
			}
		case "foreground":
			// Run attached to this terminal — the daemon child's mode, and
			// the way to debug a worker that will not start.
			if err := runWorker(cfg); err != nil {
				fail("worker", err)
			}
			if os.Getenv(daemonEnv) == "1" {
				pidFile, _ := daemonPaths(cfg.Temporal.TaskQueue)
				os.Remove(pidFile)
			}
		default:
			usageFail("unknown worker action %q (start, stop, status, restart, foreground)", action)
		}
	case "run":
		cfg := loadConfig(configPath.configPath)
		if configPath.appendID != "" {
			// Append mode: no repo path or issue id — just a prompt (or
			// -f file) for the pipeline that is already running.
			var prompt string
			switch {
			case configPath.taskFile != "" && len(args) == 1:
				p, err := readTaskFile(configPath.taskFile)
				if err != nil {
					exitf("%v", err)
				}
				prompt = p
			case configPath.taskFile == "" && len(args) == 2:
				prompt = args[1]
			default:
				usageFail(`run -a/--append takes <workflow-id> "<prompt>" (or -f <file>)`)
			}
			if err := guidePipeline(cfg, configPath.appendID, prompt); err != nil {
				fail("run", err)
			}
		} else {
			prompt, err := runPrompt(configPath.taskFile, args[1:])
			if err != nil {
				usageFail("%v", err)
			}
			err = startPipeline(cfg, configPath.workflow, args[1], args[2], prompt, configPath.detach,
				resolveBranchPrefix(configPath.branchPrefix, cfg.BranchPrefix))
			if err != nil {
				fail("run", err)
			}
		}
	case "guide":
		if len(args) != 3 {
			usageFail(`guide takes <workflow-id> "<message>"`)
		}
		cfg := loadConfig(configPath.configPath)
		if err := guidePipeline(cfg, args[1], args[2]); err != nil {
			fail("guide", err)
		}
	case "continue":
		if len(args) != 3 {
			usageFail(`continue takes <workflow-id> "<prompt>"`)
		}
		cfg := loadConfig(configPath.configPath)
		if err := continuePipeline(cfg, args[1], args[2], configPath.detach); err != nil {
			fail("continue", err)
		}
	case "attach":
		if len(args) != 2 {
			usageFail("attach takes <workflow-id>")
		}
		cfg := loadConfig(configPath.configPath)
		if err := attachPipeline(cfg, args[1]); err != nil {
			fail("attach", err)
		}
	default:
		usageFail("unknown subcommand %q", args[0])
	}
}

// exitf prints the formatted diagnostic to stderr and exits nonzero — the
// CLI's uniform failure exit.
func exitf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}

// fail reports a failing subcommand ("<cmd> failed: <err>") and exits.
func fail(cmd string, err error) {
	exitf("%s failed: %v", cmd, err)
}

// usageFail reports an argument error — the message followed by the usage
// text — and exits nonzero.
func usageFail(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n\n", a...)
	fmt.Fprint(os.Stderr, usage)
	os.Exit(1)
}

// loadConfig loads a subcommand's configuration, exiting with the shared
// diagnostic when it cannot be read.
func loadConfig(path string) config.Config {
	cfg, err := config.Load(path)
	if err != nil {
		exitf("load config: %v", err)
	}
	return cfg
}

// flags holds the global options extractable from anywhere in the argument
// list.
type flags struct {
	configPath string
	workflow   string
	taskFile   string
	detach     bool
	// branchPrefix, set via -p/--prefix on `run`, overrides the config's
	// branch_prefix for that run's preserved branch.
	branchPrefix string
	// appendID, set via -a/--append on `run`, targets an already-running
	// pipeline instead of starting a new one.
	appendID string
}

// parseFlags extracts -c/--config and -w/--workflow (which may appear
// anywhere) from args and returns them plus the remaining subcommand
// arguments.
func parseFlags(args []string) (f flags, rest []string, err error) {
	f = flags{configPath: defaultConfigPath, workflow: defaultWorkflowName}
	set := func(name, value string) error {
		switch name {
		case "config":
			f.configPath = value
		case "file":
			f.taskFile = value
		case "append":
			f.appendID = value
		case "workflow":
			if _, ok := workflowRegistry[value]; !ok {
				return fmt.Errorf("unknown workflow %q (available: %s)", value, workflowNames())
			}
			f.workflow = value
		case "prefix":
			if err := config.ValidateBranchPrefix(value); err != nil {
				return err
			}
			f.branchPrefix = value
		}
		return nil
	}
parse:
	for i := 0; i < len(args); i++ {
		a := args[i]
		// "--" ends flag parsing: everything after it is positional, so a
		// prompt that happens to start with -c or --workflow= survives.
		if a == "--" {
			rest = append(rest, args[i+1:]...)
			break parse
		}
		var name string
		var value string
		switch {
		case a == "-c" || a == "--config" || a == "-w" || a == "--workflow" || a == "-f" || a == "--file" || a == "-a" || a == "--append" || a == "-p" || a == "--prefix":
			if i+1 >= len(args) {
				return f, nil, fmt.Errorf("%s requires a value", a)
			}
			i++
			switch a {
			case "-c", "--config":
				name = "config"
			case "-f", "--file":
				name = "file"
			case "-a", "--append":
				name = "append"
			case "-p", "--prefix":
				name = "prefix"
			default:
				name = "workflow"
			}
			value = args[i]
		case strings.HasPrefix(a, "--config="):
			name, value = "config", strings.TrimPrefix(a, "--config=")
		case strings.HasPrefix(a, "--file="):
			name, value = "file", strings.TrimPrefix(a, "--file=")
		case strings.HasPrefix(a, "--append="):
			name, value = "append", strings.TrimPrefix(a, "--append=")
		case strings.HasPrefix(a, "--prefix="):
			name, value = "prefix", strings.TrimPrefix(a, "--prefix=")
		case a == "-d" || a == "--detach":
			f.detach = true
			continue
		case strings.HasPrefix(a, "--workflow="):
			name, value = "workflow", strings.TrimPrefix(a, "--workflow=")
		default:
			rest = append(rest, a)
			continue
		}
		if err := set(name, value); err != nil {
			return f, nil, err
		}
	}
	// -p/--prefix only means anything on a fresh `run`; reject it elsewhere —
	// append mode included — instead of accepting (and validating) an option
	// the subcommand ignores.
	if f.branchPrefix != "" && len(rest) > 0 && (rest[0] != "run" || f.appendID != "") {
		return f, nil, errors.New("-p/--prefix only applies to run")
	}
	return f, rest, nil
}

// resolveConfigPath locates the configuration when -c/--config is absent:
// ./config.yaml first (a deployment kept in the working directory), then
// ~/.config/daedalus/config.yaml (the user-level home), so worker and
// client commands work from any directory. An explicit -c is honored
// verbatim. The path is returned absolute so the re-exec'd daemon child
// does not depend on the working directory it inherited at start time.
func resolveConfigPath(given string) (string, error) {
	if given != defaultConfigPath {
		return filepath.Abs(given)
	}
	if _, err := os.Stat(defaultConfigPath); err == nil {
		return filepath.Abs(defaultConfigPath)
	}
	fallback, err := homeConfigPath()
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(fallback); err == nil {
		return fallback, nil
	}
	return "", fmt.Errorf("no config found — looked for ./%s and %s (pass -c, or `daedalus init` into one of them)",
		defaultConfigPath, fallback)
}

// homeConfigPath is the config's fallback location under the user-level
// config home, ~/.config/daedalus.
func homeConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, homeConfigDir, defaultConfigPath), nil
}

// resolveBranchPrefix picks a new run's branch prefix: -p/--prefix wins for
// this run; otherwise the config's (already validated and defaulted)
// branch_prefix applies.
func resolveBranchPrefix(flagPrefix, configPrefix string) string {
	if flagPrefix != "" {
		return flagPrefix
	}
	return configPrefix
}

// readTaskFile reads the task description given via -f/--file.
func readTaskFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read task file: %w", err)
	}
	return string(data), nil
}

// runPrompt resolves the task description for `daedalus run`: the content of
// the file given via -f/--file, or the trailing positional argument — exactly
// one of the two. args are the positional arguments after the subcommand
// (<repo-path> <issue-id> ["<prompt>"]).
func runPrompt(taskFile string, args []string) (string, error) {
	switch {
	case taskFile != "" && len(args) == 2:
		return readTaskFile(taskFile)
	case taskFile == "" && len(args) == 3:
		return args[2], nil
	case taskFile != "":
		return "", errors.New(`-f/--file replaces the "<prompt>" argument — pass either, not both`)
	default:
		return "", errors.New(`run takes <repo-path> <issue-id> "<prompt>"`)
	}
}

// workflowNames lists the registered workflow names, comma-separated.
func workflowNames() string {
	names := make([]string, 0, len(workflowRegistry))
	for name := range workflowRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// newClient connects to the Temporal frontend described by cfg.
func newClient(cfg config.Config) (client.Client, error) {
	c, err := client.Dial(client.Options{
		HostPort: cfg.Temporal.Host,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to temporal at %s: %w", cfg.Temporal.Host, err)
	}
	return c, nil
}

// daemonDir hosts the worker daemon's runtime files: its pid file and
// appended logs. Overridable in tests.
var daemonDir = "/tmp/daedalus"

// daemonEnv marks the re-exec'd background worker process so the child
// knows to clear the pid file on exit.
const daemonEnv = "DAEDALUS_WORKER_DAEMON"

// logRetention bounds how long daemon logs are kept.
const logRetention = 7 * 24 * time.Hour

// daemonPaths returns the per-task-queue pid and log file paths. One daemon
// per queue: a second `worker start` on a live queue refuses rather than
// doubles up.
func daemonPaths(queue string) (pidFile, logFile string) {
	return filepath.Join(daemonDir, "worker-"+queue+".pid"),
		filepath.Join(daemonDir, "worker-"+queue+".log")
}

// workerStart launches the worker as a detached daemon: it re-executes
// itself with `worker foreground`, redirected into the per-queue log, in
// its own session so the terminal is released immediately.
func workerStart(cfg config.Config, configPath string) error {
	pidFile, logFile := daemonPaths(cfg.Temporal.TaskQueue)
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
	cmd := exec.Command(self, childArgs...)
	cmd.Env = append(os.Environ(), daemonEnv+"=1")
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
	fmt.Printf("  log:    %s\n", logFile)
	fmt.Printf("  stop:   daedalus worker stop\n")
	return nil
}

// workerStop gracefully terminates the daemon: SIGTERM lets Temporal's
// worker drain, then the pid file is cleared once the process is gone.
func workerStop(cfg config.Config) error {
	pidFile, logFile := daemonPaths(cfg.Temporal.TaskQueue)
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

// workerStatus reports whether the daemon is running and where it logs.
func workerStatus(cfg config.Config) {
	pidFile, logFile := daemonPaths(cfg.Temporal.TaskQueue)
	if pid, ok := readLivePid(pidFile); ok {
		fmt.Printf("worker running (pid %d)\n  log: %s\n", pid, logFile)
		return
	}
	fmt.Println("worker not running")
	fmt.Printf("  log: %s\n", logFile)
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

// runWorker registers the workflows and all activities and blocks running the worker.
func runWorker(cfg config.Config) error {
	// Export provider settings into the worker's environment — but only the
	// ones actually set in the config. A missing key is not an error: the
	// jailed agent may authenticate through the worker's inherited
	// environment or its own login instead. Values never travel through
	// workflow history, activity inputs, or argv.
	for _, kv := range cfg.AgentEnv() {
		if key, value, ok := strings.Cut(kv, "="); ok {
			if err := os.Setenv(key, value); err != nil {
				return fmt.Errorf("set %s: %w", key, err)
			}
		}
	}
	// The jailed agent selection travels the same channel: activities read
	// DAEDALUS_AGENT when building the ai-jail command line.
	if err := os.Setenv("DAEDALUS_AGENT", cfg.Agent); err != nil {
		return fmt.Errorf("set DAEDALUS_AGENT: %w", err)
	}

	if err := activities.PreflightWorktreeRoot(cfg.Temporal.TaskQueue); err != nil {
		return fmt.Errorf("worktree preflight: %w", err)
	}

	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	w := worker.New(c, cfg.Temporal.TaskQueue, worker.Options{})
	for _, fn := range workflowRegistry {
		w.RegisterWorkflow(fn)
	}
	w.RegisterActivity(activities.CreateWorktreeActivity)
	w.RegisterActivity(activities.RunJailedClaudeActivity)
	w.RegisterActivity(activities.RunJailedReviewerActivity)
	w.RegisterActivity(activities.RunNativeTestsActivity)
	w.RegisterActivity(activities.FinalizeWorktreeActivity)
	w.RegisterActivity(activities.CleanupWorktreeActivity)

	fmt.Printf("daedalus worker listening on task queue %q (temporal %s, UI %s)\n",
		cfg.Temporal.TaskQueue, cfg.Temporal.Host, cfg.UIURL())
	return w.Run(worker.InterruptCh())
}

// startPipeline triggers the named workflow for the given issue on the
// configured task queue. branchPrefix names the run's preserved branch.
// Unless detach is set, it then blocks until the pipeline finishes.
func startPipeline(cfg config.Config, workflowName, repoPath, issueID, prompt string, detach bool, branchPrefix string) error {
	workflowFn, ok := workflowRegistry[workflowName]
	if !ok {
		return fmt.Errorf("unknown workflow %q (available: %s)", workflowName, workflowNames())
	}
	if info, err := os.Stat(repoPath); err != nil || !info.IsDir() {
		return fmt.Errorf("repo path %s is not a directory", repoPath)
	}

	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	// The workflow ID is scoped by task queue so different projects or flows
	// on one Temporal server never collide on issue IDs.
	workflowID := fmt.Sprintf("%s-issue-%s", cfg.Temporal.TaskQueue, issueID)
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: cfg.Temporal.TaskQueue,
		// Allow re-running an issue whose previous pipeline succeeded
		// instead of failing with an already-exists error.
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}, workflowFn, workflows.PipelineInput{
		RepoPath:        repoPath,
		TaskQueue:       cfg.Temporal.TaskQueue,
		IssueID:         issueID,
		Prompt:          prompt,
		BranchPrefix:    branchPrefix,
		TestTimeout:     cfg.TestsTimeout,
		AgentRunTimeout: cfg.AgentRunTimeout,
	})
	if err != nil {
		return fmt.Errorf("start workflow: %w", err)
	}

	fmt.Printf("Started workflow:\n")
	fmt.Printf("  Workflow ID: %s\n", run.GetID())
	fmt.Printf("  Run ID:      %s\n", run.GetRunID())

	if detach {
		fmt.Printf("Detached — reattach with: daedalus attach %s\n", run.GetID())
		return nil
	}
	return awaitPipeline(run)
}

// writeExampleConfig writes the fully-commented example configuration —
// every field at its default — as config-example.yaml in dir, refusing to
// overwrite an existing file.
func writeExampleConfig(dir string) error {
	path := filepath.Join(dir, "config-example.yaml")
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%s already exists — remove it first to regenerate", path)
	}
	if err := os.WriteFile(path, []byte(config.ExampleYAML), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	fmt.Printf("wrote %s — copy to config.yaml and edit\n", path)
	return nil
}

// listPipelines prints the most recent sessions on this task queue, newest
// first: workflow ID (= daedalus session id), status, and the last
// interaction datetime (close time when the session has ended, start time
// while it is running).
func listPipelines(cfg config.Config, max int) error {
	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	resp, err := c.ListWorkflow(context.Background(), &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: "default",
		PageSize:  int32(max),
		Query:     fmt.Sprintf("TaskQueue = '%s'", cfg.Temporal.TaskQueue),
	})
	if err != nil {
		return fmt.Errorf("list workflows: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "SESSION ID\tSTATUS\tLAST INTERACTION")
	for _, info := range resp.GetExecutions() {
		when := info.GetCloseTime()
		note := ""
		if !when.IsValid() {
			when = info.GetStartTime()
			note = " (started)"
		}
		fmt.Fprintf(w, "%s\t%s\t%s%s\n",
			info.GetExecution().GetWorkflowId(),
			info.GetStatus(),
			when.AsTime().Local().Format("2006-01-02 15:04:05"),
			note)
	}
	return w.Flush()
}

// continuePipeline resumes a closed pipeline (canceled, failed — including
// an API-exhaustion halt) under a new prompt: the aborted attempt's
// preserved aborted/<issue> branch becomes the new run's starting point,
// and the previous run's last review feedback is folded into the opening
// prompt.
func continuePipeline(cfg config.Config, workflowID, prompt string, detach bool) error {
	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	prev, lastReview, err := readPriorRun(c, workflowID)
	if err != nil {
		return err
	}
	if prev.IssueID == "" {
		return fmt.Errorf("workflow %s history carries no daedalus pipeline input", workflowID)
	}
	base, err := activities.AbortedBranchName(prev.IssueID)
	if err != nil {
		return err
	}
	if err := verifyBranch(prev.RepoPath, base); err != nil {
		return fmt.Errorf("nothing to continue for issue %s (%v) — start a fresh run instead", prev.IssueID, err)
	}

	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             cfg.Temporal.TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}, workflows.FeatureDevWorkflow, workflows.PipelineInput{
		RepoPath:  prev.RepoPath,
		TaskQueue: cfg.Temporal.TaskQueue,
		IssueID:   prev.IssueID,
		Prompt:    prompt,
		// A pre-branch-prefix run (empty in its recorded history) resolves
		// like a fresh run: through the operator's current config.
		BranchPrefix:    resolveBranchPrefix(prev.BranchPrefix, cfg.BranchPrefix),
		TestTimeout:     cfg.TestsTimeout,
		AgentRunTimeout: cfg.AgentRunTimeout,
		BaseBranch:      base,
		PriorFeedback:   tail(lastReview.Comments, maxPriorFeedback),
	})
	if err != nil {
		return fmt.Errorf("start workflow: %w", err)
	}
	fmt.Printf("Continued workflow:\n")
	fmt.Printf("  Workflow ID: %s\n", run.GetID())
	fmt.Printf("  Run ID:      %s\n", run.GetRunID())
	fmt.Printf("  Base branch: %s\n", base)

	if detach {
		fmt.Printf("Detached — reattach with: daedalus attach %s\n", run.GetID())
		return nil
	}
	return awaitPipeline(run)
}

// maxPriorFeedback bounds the previous run's last review feedback carried
// into the continued run's opening prompt.
const maxPriorFeedback = 16 * 1024

// readPriorRun walks the workflow's history for the original pipeline input
// and the last reviewer verdict.
func readPriorRun(c client.Client, workflowID string) (prev workflows.PipelineInput, lastReview activities.ReviewResult, err error) {
	dc := converter.GetDefaultDataConverter()
	scheduled := map[int64]string{}
	iter := c.GetWorkflowHistory(context.Background(), workflowID, "",
		false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			return prev, lastReview, fmt.Errorf("read history of %s: %w", workflowID, err)
		}
		switch {
		case ev.GetWorkflowExecutionStartedEventAttributes() != nil:
			ps := ev.GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()
			if len(ps) > 0 {
				if err := dc.FromPayload(ps[0], &prev); err != nil {
					return prev, lastReview, fmt.Errorf("decode pipeline input: %w", err)
				}
			}
		case ev.GetActivityTaskScheduledEventAttributes() != nil:
			a := ev.GetActivityTaskScheduledEventAttributes()
			scheduled[ev.GetEventId()] = a.GetActivityType().GetName()
		case ev.GetActivityTaskCompletedEventAttributes() != nil:
			a := ev.GetActivityTaskCompletedEventAttributes()
			if scheduled[a.GetScheduledEventId()] != "RunJailedReviewerActivity" {
				continue
			}
			ps := a.GetResult().GetPayloads()
			if len(ps) == 0 {
				continue
			}
			var r activities.ReviewResult
			if err := dc.FromPayload(ps[0], &r); err == nil {
				lastReview = r
			}
		}
	}
	return prev, lastReview, nil
}

// verifyBranch fails unless ref resolves in the repository.
func verifyBranch(repoPath, ref string) error {
	if out, err := exec.Command("git", "-C", repoPath, "rev-parse", "--verify", "--quiet", ref).CombinedOutput(); err != nil {
		return fmt.Errorf("branch %s not found in %s: %s", ref, repoPath, strings.TrimSpace(string(out)))
	}
	return nil
}

// tail keeps the last max bytes of s with a truncation marker.
func tail(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return "[... earlier feedback truncated ...]" + s[len(s)-max:]
}

// guidePipeline sends operator guidance to a running pipeline: the message
// travels as a "guide" signal and is folded into the pipeline's next agent
// fix prompt. The empty run ID addresses the latest execution.
func guidePipeline(cfg config.Config, workflowID, message string) error {
	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	if err := c.SignalWorkflow(context.Background(), workflowID, "", "guide", message); err != nil {
		return fmt.Errorf("signal workflow %s: %w", workflowID, err)
	}
	fmt.Printf("Guidance sent to %s — it lands in the next agent round.\n", workflowID)
	return nil
}

// attachPipeline reconnects to an already-started pipeline and blocks until
// it finishes — the other half of `run -d`. The empty run ID makes Temporal
// resolve the latest execution of the workflow ID, so reattaching after a
// finished or failed run reports that run's outcome.
func attachPipeline(cfg config.Config, workflowID string) error {
	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	resp, err := c.DescribeWorkflowExecution(context.Background(), workflowID, "")
	if err != nil {
		return fmt.Errorf("workflow %s: %w", workflowID, err)
	}
	exec := resp.GetWorkflowExecutionInfo().GetExecution()
	fmt.Printf("Attaching to workflow %s (run %s, %s)\n",
		workflowID, exec.GetRunId(), resp.GetWorkflowExecutionInfo().GetStatus())

	return awaitPipeline(c.GetWorkflow(context.Background(), workflowID, exec.GetRunId()))
}

// errRunParked distinguishes a parked outcome from success for callers
// gating on the exit code of `daedalus run`/`attach`/`continue`: the run
// produced no deliverable and is FAILED in Temporal. awaitPipeline has
// already printed the resume hint by the time it returns this; the error
// only labels the outcome.
var errRunParked = errors.New("run parked awaiting maintainer input")

// awaitPipeline blocks until the run finishes (bounded by runWaitTimeout) and
// reports the outcome: the preserved branch on success, a parked run's
// resume hint (as errRunParked, so callers gating on the exit code see a
// non-zero one), or the execution error.
func awaitPipeline(run client.WorkflowRun) error {
	waitCtx, cancel := context.WithTimeout(context.Background(), runWaitTimeout)
	defer cancel()
	var preservedBranch string
	if err := run.Get(waitCtx, &preservedBranch); err != nil {
		// A parked run arrives here as a workflow failure carrying
		// ErrAwaitingMaintainer — API exhausted past every heartbeat, or a
		// reviewer halt on an impossible task. The failure already put the
		// reason in the history and FAILED in `daedalus list`; here it just
		// needs the resume hint. Crossing the workflow→client boundary the
		// typed error survives only as its message text, so match it the
		// same way isAPIExhaustion does on the workflow side.
		if strings.Contains(err.Error(), workflows.ErrAwaitingMaintainer.Error()) {
			fmt.Printf("Workflow parked: %v\n", err)
			fmt.Printf("The attempt's work is preserved on its aborted/ branch — resume with: daedalus continue %s \"<prompt>\"\n", run.GetID())
			return errRunParked
		}
		return fmt.Errorf("workflow execution: %w", err)
	}
	fmt.Printf("Workflow completed successfully — approved work committed to branch %s\n", preservedBranch)
	return nil
}
