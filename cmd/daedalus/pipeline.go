package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
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

// resolveRepoPath turns the operator's repo path into an absolute path and
// verifies it is a git repository before the workflow starts. The path
// crosses a process boundary: activities execute on the worker daemon,
// whose working directory is wherever it was started from, so a relative
// path arriving verbatim resolved there — against a directory that may not
// be a repository — and failed every pipeline inside
// CreateWorktreeActivity with an opaque git error. That is the same
// boundary the daemon's config path was made absolute for (workerStart).
func resolveRepoPath(repoPath string) (string, error) {
	abs, err := filepath.Abs(repoPath)
	if err != nil {
		return "", fmt.Errorf("resolve repo path: %w", err)
	}
	if info, err := os.Stat(abs); err != nil || !info.IsDir() {
		return "", fmt.Errorf("repo path %s is not a directory", repoPath)
	}
	cmd := exec.Command("git", "-C", abs, "rev-parse", "--git-dir")
	cmd.Env = envWithoutGitRepoOverrides(os.Environ())
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("repo path %s is not a git repository: %s", abs, strings.TrimSpace(string(out)))
	}
	return abs, nil
}

// envWithoutGitRepoOverrides strips the environment variables that override
// git's own repository discovery. An exported GIT_DIR would have rev-parse
// validate an unrelated repository and ship a non-repo path to the worker,
// and GIT_CEILING_DIRECTORIES could reject a valid one; the check must
// judge the directory on disk, not the invoking shell's git state.
func envWithoutGitRepoOverrides(environ []string) []string {
	scrub := map[string]bool{
		"GIT_DIR":                 true,
		"GIT_WORK_TREE":           true,
		"GIT_INDEX_FILE":          true,
		"GIT_CEILING_DIRECTORIES": true,
	}
	var out []string
	for _, kv := range environ {
		if name, _, _ := strings.Cut(kv, "="); scrub[name] {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// startPipeline triggers the named workflow for the given issue on the
// configured task queue. branchPrefix names the run's preserved branch;
// agent, when set by -cli/--cli, overrides the config's jailed agent for
// this run. Unless detach is set, it then blocks until the pipeline
// finishes.
func startPipeline(cfg config.Config, workflowName, repoPath, issueID, prompt string, detach bool, branchPrefix, agent string) error {
	spec, ok := workflowRegistry[workflowName]
	if !ok {
		return fmt.Errorf("unknown workflow %q (available: %s)", workflowName, workflowNames())
	}
	repoPath, err := resolveRepoPath(repoPath)
	if err != nil {
		return err
	}

	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	// The workflow ID is scoped by task queue so different projects or flows
	// on one Temporal server never collide on issue IDs. Flow-scoped runs
	// carry the flow name too, and everything the run derives — worktree
	// path, in-flight and aborted branches (workflows.FlowScope) — is
	// scoped with it: two concurrent flows on one issue must not share a
	// worktree, since the second create's cleanup would preserve-and-delete
	// the first run's in-flight state. feature-dev keeps its legacy
	// unscoped ID and names so existing history, `daedalus continue`, and
	// recapture stay untouched. The id is opaque to every consumer (list,
	// log, wipe, attach, continue, and the task-log filename all take it
	// verbatim), and branch naming keys off input.IssueID directly — so
	// runs started before the "-issue-" infix was dropped keep working
	// alongside new ids, with no history migration.
	workflowID := fmt.Sprintf("%s-%s", cfg.Temporal.TaskQueue, issueID)
	if workflowName != defaultWorkflowName {
		workflowID = fmt.Sprintf("%s-%s-%s", cfg.Temporal.TaskQueue, workflowName, issueID)
	}
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: cfg.Temporal.TaskQueue,
		// Allow re-running an issue whose previous pipeline succeeded
		// instead of failing with an already-exists error.
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}, spec.Fn, workflows.PipelineInput{
		RepoPath:        repoPath,
		TaskQueue:       cfg.Temporal.TaskQueue,
		IssueID:         issueID,
		Prompt:          prompt,
		Flow:            workflowName,
		AllowedPaths:    spec.AllowedPaths,
		FrozenPaths:     spec.FrozenPaths,
		BranchPrefix:    branchPrefix,
		Agent:           agent,
		Authorship:      cfg.Authorship,
		TestTimeout:     cfg.TestsTimeout,
		AgentRunTimeout: cfg.AgentRunTimeout,
		ReviewTimeout:   cfg.ReviewTimeout,
		CleanupTimeout:  cfg.CleanupTimeout,
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

	// A run's flow type is fixed at start — continuing is resuming, not
	// re-scoping — so the new run resolves the original flow's function
	// through the registry and keeps its write-scope policy. A pre-field
	// attempt's empty Flow replays as feature-dev, the same zero-value
	// fallback every other field uses; a flow no longer registered is a
	// hard stop — silently re-scoping the run would be worse.
	flow := prev.Flow
	if flow == "" {
		flow = defaultWorkflowName
	}
	spec, ok := workflowRegistry[flow]
	if !ok {
		return fmt.Errorf("workflow %s ran flow %q, which is no longer registered", workflowID, flow)
	}

	base, err := activities.AbortedBranchNameFor(prev.IssueID, workflows.FlowScope(flow))
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
	}, spec.Fn, workflows.PipelineInput{
		RepoPath:  prev.RepoPath,
		TaskQueue: cfg.Temporal.TaskQueue,
		IssueID:   prev.IssueID,
		Prompt:    prompt,
		Flow:      flow,
		// The continued run keeps the policy the aborted attempt started
		// with — its contract, frozen at start.
		AllowedPaths: prev.AllowedPaths,
		FrozenPaths:  prev.FrozenPaths,
		// A pre-branch-prefix run (empty in its recorded history) resolves
		// like a fresh run: through the operator's current config.
		BranchPrefix: resolveBranchPrefix(prev.BranchPrefix, cfg.BranchPrefix),
		// The continued run keeps the agent the aborted attempt ran with —
		// a pre-field attempt's empty value falls back to the worker's.
		Agent:           prev.Agent,
		Authorship:      cfg.Authorship,
		TestTimeout:     cfg.TestsTimeout,
		AgentRunTimeout: cfg.AgentRunTimeout,
		ReviewTimeout:   cfg.ReviewTimeout,
		CleanupTimeout:  cfg.CleanupTimeout,
		BaseBranch:      base,
		// No truncation anywhere in the app: the complete last review rides
		// into the continued run's opening prompt.
		PriorFeedback: lastReview.Comments,
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

// wakeupPipeline interrupts a running pipeline's quota heartbeat so the
// sleeping round resumes immediately — for when the operator has verified
// the provider recovered and will not wait out the hourly sleep. The
// "wakeup" signal lands in the workflow's heartbeat select; a running-but-
// not-sleeping workflow buffers it and merely skips its next heartbeat.
// Closed sessions (canceled, failed, parked) have their own resume path —
// `continue`, which takes a prompt — so they are a usage error here.
func wakeupPipeline(cfg config.Config, workflowID string) error {
	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	resp, err := c.DescribeWorkflowExecution(context.Background(), workflowID, "")
	if err != nil {
		// Only a genuinely absent id is a usage error; an unreachable
		// server or any other failure returns plainly — the "check the id
		// against daedalus list" advice needs the server that just failed.
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			usageFail("workflow %s: not found — check the id against \"daedalus list\"; closed sessions resume with: daedalus continue %s \"<prompt>\"",
				workflowID, workflowID)
		}
		return fmt.Errorf("workflow %s: %w", workflowID, err)
	}
	info := resp.GetWorkflowExecutionInfo()
	// The workflow id alone does not say which queue owns it: canceling (or
	// here, signaling) across queues would act on someone else's execution.
	// The task queue is resolved from the active config — the -c chain says
	// which worker's session the operator means.
	if q := info.GetTaskQueue(); q != cfg.Temporal.TaskQueue {
		return fmt.Errorf("workflow %s runs on task queue %q, not this config's %q — point -c at the config of the queue that owns it",
			workflowID, q, cfg.Temporal.TaskQueue)
	}
	if status := info.GetStatus(); status != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		usageFail("workflow %s is %s, not running — closed sessions resume with: daedalus continue %s \"<prompt>\" (check the id against \"daedalus list\")",
			workflowID, status, workflowID)
	}
	if err := c.SignalWorkflow(context.Background(), workflowID, "", "wakeup", ""); err != nil {
		return fmt.Errorf("signal workflow %s: %w", workflowID, err)
	}
	fmt.Printf("Wakeup sent to %s — a quota-heartbeat sleep it is in (or reaches next) ends immediately; mid-round it only shortens the next one.\n", workflowID)
	fmt.Printf("Reattach with: daedalus attach %s\n", workflowID)
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
