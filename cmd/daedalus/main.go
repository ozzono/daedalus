// Command daedalus is the entrypoint for the Daedalus control plane: it runs
// Temporal workers and starts feature-development pipelines. main is a thin
// dispatch router: argument parsing lives in flags.go, the help screens in
// help.go, and each command's implementation in its own file (pipeline.go,
// worker.go, worker_run.go, worker_status.go, report.go, recapture.go,
// tasklog.go).
package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/version"
)

func main() {
	configPath, args, err := parseFlags(os.Args[1:])
	if err != nil {
		usageFail("%v", err)
	}
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	// "daedalus <command> --help" (and -h/help): the command's detailed
	// help screen. Checked before config resolution so help works even
	// where no config can be found.
	if len(args) == 2 && (args[1] == "-h" || args[1] == "--help" || args[1] == "help") {
		if h, ok := commandHelp[args[0]]; ok {
			fmt.Print(h)
			return
		}
	}

	// Every subcommand except the no-config ones (help, version, init,
	// report, the record-driven worker commands — `worker restart all`,
	// `worker restart <name>`, and `worker status`, which read only the
	// recorded per-worker configs — and `log`, which reads a file by name
	// alone) loads a configuration; resolve its location once, up front, so
	// the subcommand — and the daemon `worker start` re-executes — agree on
	// it wherever the CLI is invoked from.
	_, restartNamed := isRestartNamed(args)
	switch {
	case args[0] == "-h", args[0] == "--help", args[0] == "help",
		args[0] == "-v", args[0] == "--version", args[0] == "version", args[0] == "init",
		args[0] == "report", args[0] == "log",
		isRestartAll(args), restartNamed, isWorkerStatus(args):
	default:
		// A fresh `run <repo-path>` prefers the repo's own project-local
		// config over the default resolution (see runLocalConfigPath);
		// everything else resolves the usual way.
		if local, ok := runLocalConfigPath(args, configPath); ok {
			configPath.configPath = local
		} else {
			path, err := resolveConfigPath(configPath.configPath)
			if err != nil {
				exitf("load config: %v", err)
			}
			configPath.configPath = path
		}
	}

	switch args[0] {
	case "-h", "--help", "help":
		fmt.Print(usage)
	case "-v", "--version", "version":
		fmt.Println(version.String())
	case "init":
		if err := writeExampleConfig("."); err != nil {
			fail("init", err)
		}
	case "config":
		// daedalus config — print the resolved config path plus every
		// config field, the file's pre-defaults view.
		if len(args) != 1 {
			usageFail("config takes no arguments")
		}
		raw, err := config.LoadRaw(configPath.configPath)
		if err != nil {
			exitf("load config: %v", err)
		}
		fmt.Printf("# config: %s\n", configPath.configPath)
		out, err := raw.RenderYAML()
		if err != nil {
			fail("config", err)
		}
		fmt.Print(out)
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
		// daedalus worker [start|stop|status|restart|foreground|wakeup] —
		// default start runs the worker as a detached daemon. `restart`
		// additionally takes `all` (or --all) to restart every worker on
		// record in one call, or a worker name (`restart <id>`) to restart
		// that one worker from its recorded config, from any directory.
		// `wakeup <workflow-id>` interrupts a running session's quota
		// heartbeat so the round resumes past a recovered provider.
		action := "start"
		all := false
		var restartName string
		if len(args) >= 2 {
			action = args[1]
		}
		if isRestartAll(args) {
			all = true
		} else if name, ok := isRestartNamed(args); ok {
			restartName = name
		} else if action == "wakeup" {
			if len(args) != 3 {
				usageFail("wakeup takes <workflow-id>")
			}
		} else if len(args) > 2 {
			usageFail("worker takes at most one action")
		}
		// The record-driven worker commands (`restart all`, `restart
		// <name>`, `status`) work purely from the recorded per-worker
		// configs and must not depend on whatever the invoking directory
		// resolves to; every other action needs the CLI's config.
		var cfg config.Config
		if !(action == "restart" && (all || restartName != "")) && action != "status" {
			cfg = loadConfig(configPath.configPath)
		}
		switch action {
		case "start":
			if err := workerStart(cfg, configPath.configPath, configPath.workerType); err != nil {
				fail("worker start", err)
			}
		case "stop":
			if err := workerStop(cfg); err != nil {
				fail("worker stop", err)
			}
		case "status":
			if err := workerStatusAll(); err != nil {
				fail("worker status", err)
			}
		case "restart":
			switch {
			case all:
				if err := workerRestartAll(); err != nil {
					fail("worker restart", err)
				}
			case restartName != "":
				if err := workerRestartNamed(restartName); err != nil {
					fail("worker restart", err)
				}
			default:
				// The bare restart forwards the invoking command's run type;
				// the record-driven paths above pass none (untyped revival).
				if err := workerRestart(cfg, configPath.configPath, configPath.workerType); err != nil {
					fail("worker restart", err)
				}
			}
		case "wakeup":
			// Interrupt a running session's quota heartbeat — the automated
			// "the provider recovered, resume now" lever. Needs the config
			// for the Temporal connection and the owning task queue.
			if err := wakeupPipeline(cfg, args[2]); err != nil {
				fail("worker wakeup", err)
			}
		case "foreground":
			// Run attached to this terminal — the daemon child's mode, and
			// the way to debug a worker that will not start.
			if err := runWorker(cfg, configPath.workerType); err != nil {
				fail("worker", err)
			}
			if os.Getenv(daemonEnv) == "1" {
				pidFile, _, _ := daemonPaths(cfg.WorkerName())
				os.Remove(pidFile)
			}
		default:
			usageFail("unknown worker action %q (start, stop, status, restart, wakeup, foreground)", action)
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
				resolveBranchPrefix(configPath.branchPrefix, cfg.BranchPrefix), configPath.agentCLI)
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
	case "wipe":
		// daedalus wipe <workflow-id> — stop the run if it is still
		// executing, then erase everything daedalus put on disk for it
		// (worktree, branches, session record, task log). Needs the config
		// for the Temporal connection and the owning task queue; --yes
		// skips the interactive confirmation.
		if len(args) != 2 {
			usageFail("wipe takes <workflow-id>")
		}
		cfg := loadConfig(configPath.configPath)
		if err := wipePipeline(cfg, args[1], configPath.yes); err != nil {
			fail("wipe", err)
		}
	case "log":
		// daedalus log <workflow-id> — print the task's captured log;
		// --status prints a status brief instead; -cot prints the run's
		// chain-of-thought logs from Temporal history instead. Raw and
		// --status read the file by name alone: no config, no temporal.
		// -cot dials Temporal at the resolved config's host, else the
		// default (the config is looked up inside runTaskLogCot, so log
		// stays config-exempt at dispatch).
		if len(args) != 2 {
			usageFail("log takes <workflow-id>")
		}
		if configPath.cot {
			runTaskLogCot(args[1])
			return
		}
		runTaskLog(args[1], configPath.status)
	case "report":
		if err := runReport(configPath.configPath, args[1:]); err != nil {
			fail("report", err)
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
