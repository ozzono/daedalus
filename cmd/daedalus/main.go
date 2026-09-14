// Command daedalus is the entrypoint for the Daedalus control plane: it runs
// Temporal workers and starts feature-development pipelines.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"daedalus/internal/activities"
	"daedalus/internal/config"
	"daedalus/internal/workflows"
)

// runWaitTimeout bounds how long `daedalus run` waits for a pipeline to
// finish. Multiple agent and review rounds at a 15-minute ceiling each fit
// comfortably.
const runWaitTimeout = 4 * time.Hour

// defaultConfigPath is used when -c/--config is not given.
const defaultConfigPath = "config.yaml"

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
  daedalus [-c config.yaml] worker
      Start a Temporal worker hosting the pipelines.
      anthropic.key is optional — if unset, the jailed agent authenticates
      through the worker's inherited environment or its own login.
  daedalus [-c config.yaml] [-w workflow] run [-d] <repo-path> <issue-id> "<prompt>"
      Start a pipeline for an issue. -w selects the workflow
      (default: feature-dev; available: ` + workflowNames() + `).
      The "<prompt>" argument is the full task description; pass
      -f/--file <path> to read it from a file instead — either the
      argument or the file, never both.
      -d/--detach starts the pipeline and returns immediately instead of
      blocking until it finishes; "daedalus attach" reconnects later.
  daedalus [-c config.yaml] run -a <workflow-id> "<prompt>"
      Append instructions to a pipeline that is already running instead of
      starting a new run: the prompt is folded into the agent's next fix
      round (same as "daedalus guide"); -f/--file works here too.
      -d has no effect in append mode.
  daedalus [-c config.yaml] guide <workflow-id> "<message>"
      Send operator guidance to a running pipeline: the message is folded
      into the agent's next fix prompt, steering a stuck review loop
      without restarting the run.
  daedalus [-c config.yaml] attach <workflow-id>
      Reattach to a running (or already finished) pipeline, block until it
      finishes, and report the outcome — the other half of "run -d".
      <workflow-id> is the identifier printed by "run" (also visible in
      "temporal workflow list").

Configuration is read from config.yaml (-c/--config to override the path);
see config-example.yaml for all fields and their defaults:
  temporal.host        Temporal frontend address   (default 127.0.0.1:7233)
  temporal.ui_port     Temporal UI port, shown at worker startup (default 8233)
  temporal.task_queue  routing key; distinct projects or flows sharing one
                       Temporal server use distinct queues (default daedalus)
  anthropic.url        Anthropic API base URL       (default https://api.anthropic.com)
  anthropic.key        Anthropic API key            (optional — skipped if unset)
  anthropic.model      Model for the jailed agent   (agent default if unset)
  openai.url/key/model Optional OpenAI settings injected into the agent environment

For a local Temporal dev server matching the defaults:
  temporal server start-dev
`

func main() {
	configPath, args, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n\n%s", err, usage)
		os.Exit(1)
	}
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	switch args[0] {
	case "-h", "--help", "help":
		fmt.Print(usage)
	case "worker", "run":
		cfg, err := config.Load(configPath.configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load config: %v\n", err)
			os.Exit(1)
		}
		if args[0] == "worker" {
			if err := runWorker(cfg); err != nil {
				fmt.Fprintf(os.Stderr, "worker failed: %v\n", err)
				os.Exit(1)
			}
		} else if configPath.appendID != "" {
			// Append mode: no repo path or issue id — just a prompt (or
			// -f file) for the pipeline that is already running.
			var prompt string
			switch {
			case configPath.taskFile != "" && len(args) == 1:
				data, err := os.ReadFile(configPath.taskFile)
				if err != nil {
					fmt.Fprintf(os.Stderr, "read task file: %v\n", err)
					os.Exit(1)
				}
				prompt = string(data)
			case configPath.taskFile == "" && len(args) == 2:
				prompt = args[1]
			default:
				fmt.Fprintf(os.Stderr, "run -a/--append takes <workflow-id> \"<prompt>\" (or -f <file>)\n\n%s", usage)
				os.Exit(1)
			}
			if err := guidePipeline(cfg, configPath.appendID, prompt); err != nil {
				fmt.Fprintf(os.Stderr, "run failed: %v\n", err)
				os.Exit(1)
			}
		} else {
			prompt, err := runPrompt(configPath.taskFile, args[1:])
			if err != nil {
				fmt.Fprintf(os.Stderr, "%v\n\n%s", err, usage)
				os.Exit(1)
			}
			err = startPipeline(cfg, configPath.workflow, args[1], args[2], prompt, configPath.detach)
			if err != nil {
				fmt.Fprintf(os.Stderr, "run failed: %v\n", err)
				os.Exit(1)
			}
		}
	case "guide":
		if len(args) != 3 {
			fmt.Fprintf(os.Stderr, "guide takes <workflow-id> \"<message>\"\n\n%s", usage)
			os.Exit(1)
		}
		cfg, err := config.Load(configPath.configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load config: %v\n", err)
			os.Exit(1)
		}
		if err := guidePipeline(cfg, args[1], args[2]); err != nil {
			fmt.Fprintf(os.Stderr, "guide failed: %v\n", err)
			os.Exit(1)
		}
	case "attach":
		if len(args) != 2 {
			fmt.Fprintf(os.Stderr, "attach takes <workflow-id>\n\n%s", usage)
			os.Exit(1)
		}
		cfg, err := config.Load(configPath.configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load config: %v\n", err)
			os.Exit(1)
		}
		if err := attachPipeline(cfg, args[1]); err != nil {
			fmt.Fprintf(os.Stderr, "attach failed: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n%s", args[0], usage)
		os.Exit(1)
	}
}

// flags holds the global options extractable from anywhere in the argument
// list.
type flags struct {
	configPath string
	workflow   string
	taskFile   string
	detach     bool
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
		}
		return nil
	}
	for i := 0; i < len(args); i++ {
		a := args[i]
		// "--" ends flag parsing: everything after it is positional, so a
		// prompt that happens to start with -c or --workflow= survives.
		if a == "--" {
			rest = append(rest, args[i+1:]...)
			return f, rest, nil
		}
		var name string
		var value string
		switch {
		case a == "-c" || a == "--config" || a == "-w" || a == "--workflow" || a == "-f" || a == "--file" || a == "-a" || a == "--append":
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
	return f, rest, nil
}

// runPrompt resolves the task description for `daedalus run`: the content of
// the file given via -f/--file, or the trailing positional argument — exactly
// one of the two. args are the positional arguments after the subcommand
// (<repo-path> <issue-id> ["<prompt>"]).
func runPrompt(taskFile string, args []string) (string, error) {
	switch {
	case taskFile != "" && len(args) == 2:
		data, err := os.ReadFile(taskFile)
		if err != nil {
			return "", fmt.Errorf("read task file: %w", err)
		}
		return string(data), nil
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
// configured task queue. Unless detach is set, it then blocks until the
// pipeline finishes.
func startPipeline(cfg config.Config, workflowName, repoPath, issueID, prompt string, detach bool) error {
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
		RepoPath:  repoPath,
		TaskQueue: cfg.Temporal.TaskQueue,
		IssueID:   issueID,
		Prompt:    prompt,
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

// awaitPipeline blocks until the run finishes (bounded by runWaitTimeout) and
// reports the outcome: the preserved branch on success, the execution error
// otherwise.
func awaitPipeline(run client.WorkflowRun) error {
	waitCtx, cancel := context.WithTimeout(context.Background(), runWaitTimeout)
	defer cancel()
	var preservedBranch string
	if err := run.Get(waitCtx, &preservedBranch); err != nil {
		return fmt.Errorf("workflow execution: %w", err)
	}
	fmt.Printf("Workflow completed successfully — approved work committed to branch %s\n", preservedBranch)
	return nil
}
