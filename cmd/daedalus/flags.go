package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go.temporal.io/sdk/client"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/workflows"
)

// defaultConfigPath is used when -c/--config is not given: looked up in the
// working directory, then in the working directory's .daedalus/, then in the
// user-level home (see resolveConfigPath).
const defaultConfigPath = "config.yaml"

// homeConfigDir is the config's user-level home, ~/.config/daedalus. A
// config there is found from any directory, so worker and client commands
// work wherever they are invoked.
const homeConfigDir = ".config/daedalus"

// defaultWorkflowName is used when -w/--workflow is not given.
const defaultWorkflowName = "feature-dev"

// workflowRegistry maps the names accepted by -w/--workflow to their
// flows: the workflow function plus the per-flow write-scope policy
// startPipeline carries in the run's input (verified structurally before
// finalize — see workflows.Flow). Adding a flow later means adding an
// entry here (plus its workflow function and files) — no CLI changes.
var workflowRegistry = map[string]workflows.Flow{
	"feature-dev": {Fn: workflows.FeatureDevWorkflow},
	// dev-only is feature-dev's phase 1 alone: implementation ↔ code
	// review, landing without any test-phase execution (and without the
	// preflight suite gate — see DevOnlyWorkflow). Selection is CLI-only;
	// no config key picks a flow.
	"dev-only": {Fn: workflows.DevOnlyWorkflow},
	// slim is the micro-stepped atomic loop for context-limited
	// self-hosted models (target agent: pi): a planner round atomizes the
	// task into an ordered queue of 1–2-file sub-tasks, then each
	// sub-task runs its own implement ↔ review loop — fresh reviewer
	// session per round, native suite as terminal ground truth. Also
	// config-gated: with slim: true, a run without an explicit -w starts
	// this flow (see the run path in main.go).
	"slim": {Fn: workflows.SlimWorkflow},
	"investigate": {Fn: workflows.InvestigateWorkflow,
		AllowedPaths: []string{"*.md", "docs/"}},
	"test-only": {Fn: workflows.TestOnlyWorkflow,
		AllowedPaths: activities.TestPathPatterns},
	"refactor": {Fn: workflows.RefactorWorkflow,
		FrozenPaths: activities.TestPathPatterns},
	"bug-fix": {Fn: workflows.BugFixWorkflow},
}

// Worker-type values for -t/--type on the worker command: a run config
// choosing which pollers one daemon process starts. Absence (the empty
// string) means both pollers — there is deliberately no "all" spelling.
const (
	workerTypeDev  = "dev"
	workerTypeTest = "test"
)

// flags holds the global options extractable from anywhere in the argument
// list.
type flags struct {
	configPath string
	// configSet reports that -c/--config appeared on the command line, so
	// an explicit (but ignored) -c can be told apart from the default.
	configSet bool
	workflow  string
	// workflowSet reports that -w/--workflow appeared on the command line:
	// the config gate on the slim flow applies only to a defaulted -w, an
	// explicit one always wins (see the run path in main.go).
	workflowSet bool
	taskFile    string
	detach      bool
	// branchPrefix, set via -p/--prefix on `run`, overrides the config's
	// branch_prefix for that run's preserved branch.
	branchPrefix string
	// appendID, set via -a/--append on `run`, targets an already-running
	// pipeline instead of starting a new one.
	appendID string
	// agentCLI, set via -cli/--cli on `run`, overrides the config's agent
	// for that run: the selection travels with the run's workflow input, so
	// it applies on whichever worker serves the queue.
	agentCLI string
	// folders, set via -folder/--folder on `run` (repeatable), grants the
	// run read-write access to host folders: each path travels with the
	// run's workflow input and is mounted into the jailed rounds' sandbox.
	folders []string
	// depends, set via -dep/--depends on `run`, chains the fresh run
	// behind another workflow: the id travels with the run's workflow
	// input and the gate inside it blocks until the dependency resolves.
	depends string
	// status, set via --status on `log`, prints the task's status brief
	// instead of the raw log.
	status bool
	// cot, set via -cot on `log`, prints the run's chain-of-thought logs
	// from Temporal history instead of the raw log.
	cot bool
	// workerType, set via -t/--type on the worker command, shapes that
	// daemon process's pollers: a run config, not recorded anywhere —
	// restarts of the same worker come back untyped (both pollers).
	workerType string
	// yes, set via --yes on `wipe`, skips the interactive confirmation for
	// the destructive erase (scripted use).
	yes bool
}

// flagSpec is one command-line flag: its semantic name (value-taking flags
// switch on it in set below, booleans map to their field directly) and
// whether it takes a value.
type flagSpec struct {
	name  string
	value bool
}

// flagTable maps every accepted flag spelling to its spec — the single
// source read by both parseFlags and the shell completion's candidates
// (completion.go), so a flag added once is parsed and completed. Long
// spellings also accept the "--name=" value form.
var flagTable = map[string]flagSpec{
	"-c":         {name: "config", value: true},
	"--config":   {name: "config", value: true},
	"-w":         {name: "workflow", value: true},
	"--workflow": {name: "workflow", value: true},
	"-f":         {name: "file", value: true},
	"--file":     {name: "file", value: true},
	"-a":         {name: "append", value: true},
	"--append":   {name: "append", value: true},
	"-p":         {name: "prefix", value: true},
	"--prefix":   {name: "prefix", value: true},
	"-cli":       {name: "cli", value: true},
	"--cli":      {name: "cli", value: true},
	"-folder":    {name: "folder", value: true},
	"--folder":   {name: "folder", value: true},
	"-dep":       {name: "depends", value: true},
	"--depends":  {name: "depends", value: true},
	"-t":         {name: "type", value: true},
	"--type":     {name: "type", value: true},
	"-d":         {name: "detach"},
	"--detach":   {name: "detach"},
	"--yes":      {name: "yes"},
	"--status":   {name: "status"},
	"-cot":       {name: "cot"},
	"--cot":      {name: "cot"},
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
			f.configSet = true
		case "file":
			f.taskFile = value
		case "append":
			f.appendID = value
		case "workflow":
			if _, ok := workflowRegistry[value]; !ok {
				return fmt.Errorf("unknown workflow %q (available: %s)", value, workflowNames())
			}
			f.workflow = value
			f.workflowSet = true
		case "prefix":
			if err := config.ValidateBranchPrefix(value); err != nil {
				return err
			}
			f.branchPrefix = value
		case "cli":
			if err := config.ValidateAgent(value); err != nil {
				return err
			}
			f.agentCLI = value
		case "folder":
			f.folders = append(f.folders, value)
		case "depends":
			f.depends = value
		case "type":
			if value != workerTypeDev && value != workerTypeTest {
				return fmt.Errorf("unknown worker type %q (valid: %s, %s)", value, workerTypeDev, workerTypeTest)
			}
			f.workerType = value
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
		// Every accepted spelling is in flagTable: an exact match first,
		// then the "--name=" value form of a long value flag. Anything
		// else is positional.
		spec, known := flagTable[a]
		hasEq, eqValue := false, ""
		if !known && strings.HasPrefix(a, "--") {
			if eq := strings.IndexByte(a, '='); eq > 0 {
				if s, ok := flagTable[a[:eq]]; ok && s.value {
					spec, known = s, true
					hasEq, eqValue = true, a[eq+1:]
				}
			}
		}
		if !known {
			rest = append(rest, a)
			continue
		}
		if spec.value {
			value := eqValue
			if !hasEq {
				if i+1 >= len(args) {
					return f, nil, fmt.Errorf("%s requires a value", a)
				}
				i++
				value = args[i]
			}
			if err := set(spec.name, value); err != nil {
				return f, nil, err
			}
			continue
		}
		switch spec.name {
		case "detach":
			f.detach = true
		case "yes":
			f.yes = true
		case "status":
			f.status = true
		case "cot":
			f.cot = true
		}
	}
	// -p/--prefix only means anything on a fresh `run`; reject it elsewhere —
	// append mode included — instead of accepting (and validating) an option
	// the subcommand ignores.
	if f.branchPrefix != "" && len(rest) > 0 && (rest[0] != "run" || f.appendID != "") {
		return f, nil, errors.New("-p/--prefix only applies to run")
	}
	// Likewise -cli/--cli: the agent selection travels with a run's
	// workflow input, so only a fresh `run` can honor it — append mode
	// targets a pipeline whose agent is already fixed.
	if f.agentCLI != "" && len(rest) > 0 && (rest[0] != "run" || f.appendID != "") {
		return f, nil, errors.New("-cli/--cli only applies to run")
	}
	// Likewise -folder/--folder: the grants travel with a run's workflow
	// input, so only a fresh `run` can honor them — append mode steers a
	// pipeline whose trust surface (grants included) is already fixed.
	if len(f.folders) > 0 && len(rest) > 0 && (rest[0] != "run" || f.appendID != "") {
		return f, nil, errors.New("-folder/--folder only applies to a fresh run")
	}
	// Likewise -dep/--depends: the chain is decided at submit (the
	// dependency preflight runs once, at start), so only a fresh `run`
	// can carry one — append mode steers a pipeline whose dependency was
	// already resolved or never existed.
	if f.depends != "" && len(rest) > 0 && (rest[0] != "run" || f.appendID != "") {
		return f, nil, errors.New("-dep/--depends only applies to a fresh run")
	}
	// The record-driven worker commands (`worker restart all`, `worker
	// restart <name>`, `worker status`) work from the recorded configs
	// alone; an explicit -c there is silently ignored by every step —
	// reject it instead of accepting an option the subcommand never uses.
	// The task log has its own wording: `daedalus log` likewise reads a
	// file by name alone and never touches a config. (The shared message
	// below is pinned verbatim by the worker-status tests, so `log`'s
	// rejection cannot just be appended to it.)
	_, restartNamed := isRestartNamed(rest)
	if f.configSet {
		if isTaskLog(rest) {
			return f, nil, errors.New("-c/--config does not apply to daedalus log")
		}
		if isRestartAll(rest) || restartNamed || isWorkerStatus(rest) {
			return f, nil, errors.New("-c/--config does not apply to worker restart all, restart <worker>, or worker status")
		}
	}
	// -t/--type is the run-config counterpart: the record-driven worker
	// commands act from the records alone and revive the daemon untyped, so
	// a -t there could only invert the invoked intent silently — reject it.
	if f.workerType != "" && (isRestartAll(rest) || restartNamed || isWorkerStatus(rest)) {
		return f, nil, errors.New("-t/--type does not apply to worker restart all, restart <worker>, or worker status")
	}
	// -t/--type on `run` would shape the daemon the worker preflight starts
	// — but a run always needs the main-queue poller, and a test-only
	// daemon serves none: it would pass the preflight's status-file
	// handshake while the workflow waits for a poller that never comes. A
	// run-started daemon therefore always comes up untyped (both pollers),
	// and the flag is rejected here where it cannot be honored — typing a
	// daemon stays an explicit `worker start`/`worker restart` concern.
	if f.workerType != "" && len(rest) > 0 && rest[0] == "run" {
		return f, nil, errors.New("-t/--type does not apply to daedalus run — the worker it starts comes untyped (both pollers); type one with `daedalus worker start -t <type>`")
	}
	// --status switches `log` from the raw file to the status brief;
	// anywhere else it would be an option the subcommand ignores — reject
	// it.
	if f.status && !isTaskLog(rest) {
		return f, nil, errors.New("--status only applies to daedalus log <workflow-id>")
	}
	// -cot is `log`'s other mode switch — the chain-of-thought view, read
	// from Temporal history; anywhere else it would be an option the
	// subcommand ignores, exactly like --status above.
	if f.cot && !isTaskLog(rest) {
		return f, nil, errors.New("-cot only applies to daedalus log <workflow-id>")
	}
	// One log invocation prints one view: the raw file, the brief, or the
	// CoT — never a mixture.
	if f.status && f.cot {
		return f, nil, errors.New("--status and -cot are mutually exclusive")
	}
	// --yes is likewise `wipe`'s only switch — it skips that command's
	// destructive-erase confirmation and has no meaning anywhere else.
	if f.yes && len(rest) > 0 && rest[0] != "wipe" {
		return f, nil, errors.New("--yes only applies to daedalus wipe <workflow-id>")
	}
	return f, rest, nil
}

// isTaskLog reports whether rest is `daedalus log <workflow-id>` — the
// record-free task-log read.
func isTaskLog(rest []string) bool {
	return len(rest) == 2 && rest[0] == "log"
}

// resolveConfigPath locates the configuration when -c/--config is absent:
// ./config.yaml first (a deployment kept in the working directory), then
// ./.daedalus/config.yaml (the repo-local config of the repo the invoking
// shell sits in), then ~/.config/daedalus/config.yaml (the user-level home),
// so worker and client commands work from any directory. An explicit -c is
// honored verbatim. The path is returned absolute so the re-exec'd daemon
// child does not depend on the working directory it inherited at start time.
func resolveConfigPath(given string) (string, error) {
	if given != defaultConfigPath {
		return filepath.Abs(given)
	}
	if _, err := os.Stat(defaultConfigPath); err == nil {
		return filepath.Abs(defaultConfigPath)
	}
	repoLocal := filepath.Join(".daedalus", defaultConfigPath)
	if _, err := os.Stat(repoLocal); err == nil {
		return filepath.Abs(repoLocal)
	}
	fallback, err := homeConfigPath()
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(fallback); err == nil {
		return fallback, nil
	}
	return "", fmt.Errorf("no config found — looked for ./%s, ./%s, and %s (pass -c, or `daedalus init` into one of them)",
		defaultConfigPath, repoLocal, fallback)
}

// runLocalConfigPath reports the project-local config of a fresh
// `daedalus run <repo-path>`: <repo-path>/.daedalus/config.yaml, when the
// target repo carries one. It replaces the default config outright —
// config.Load reads this file alone, no fields merge from the default —
// while an explicit -c/--config still wins over it. It also wins over the
// cwd source in resolveConfigPath: `run <other-repo>` invoked from inside a
// repo carrying its own .daedalus/config.yaml must configure the run for
// the target repo, not the shell's cwd. Append mode has no
// repo path, so there is nothing to look up. The path is returned
// absolute, matching resolveConfigPath.
func runLocalConfigPath(args []string, f flags) (string, bool) {
	if args[0] != "run" || f.configSet || f.appendID != "" || len(args) < 2 {
		return "", false
	}
	local := filepath.Join(args[1], ".daedalus", "config.yaml")
	if _, err := os.Stat(local); err != nil {
		return "", false
	}
	abs, err := filepath.Abs(local)
	if err != nil {
		return "", false
	}
	return abs, true
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

// readTaskFile reads the task description given via -f/--file. A glob
// pattern (e.g. notes/*.md) expands to every matching file, concatenated
// in lexical order; a plain path is read as-is. Directories matching a
// glob are skipped rather than failing the read.
func readTaskFile(pattern string) (string, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", fmt.Errorf("read task file: %w", err)
	}
	var parts []string
	for _, match := range matches {
		info, err := os.Stat(match)
		if err != nil {
			return "", fmt.Errorf("read task file: %w", err)
		}
		if info.IsDir() {
			continue
		}
		data, err := os.ReadFile(match)
		if err != nil {
			return "", fmt.Errorf("read task file: %w", err)
		}
		parts = append(parts, string(data))
	}
	if len(parts) == 0 {
		if !strings.ContainsAny(pattern, `*?[\`) {
			// A plain path Glob found nothing for doesn't exist (Glob
			// stats it and returns no matches): surface the real error
			// rather than a generic "no matching files".
			if _, err := os.Stat(pattern); err != nil {
				return "", fmt.Errorf("read task file: %w", err)
			}
		}
		return "", fmt.Errorf("read task file: %s: no matching files", pattern)
	}
	return strings.Join(parts, "\n\n"), nil
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
