package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"strings"
	"time"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/workflows"
)

// wipeCloseWait bounds how long wipe waits for a canceled run to reach a
// closed state before touching disk. Erasing under a live worker races the
// workflow's cleanup activities, which would recreate the worktree and
// branches after deletion; the workflow's deferred cleanup runs on a
// disconnected context and must be allowed to finish (or fail for good)
// first. A large worktree removal is filesystem-bound and can take minutes,
// hence a bound wider than most, but finite — the operator retries.
var wipeCloseWait = 10 * time.Minute

// wipePollInterval is how often wipe re-describes the workflow while
// waiting for its cancellation to close it.
const wipePollInterval = 500 * time.Millisecond

// wipePipeline implements `daedalus wipe <workflow-id>`: stop the run if it
// is still executing, then permanently erase everything daedalus put on
// disk for it — the worktree, its branches in the target repo (in-flight,
// preserved, and the aborted/ snapshot), the session-state record, and the
// task log — leaving Temporal's history as the only record of the run.
// Best-effort per artifact: failures are reported and the rest still
// erased; the command exits non-zero when anything failed. Unless yes is
// set, an interactive confirmation gates everything — a bare invocation
// never wipes.
func wipePipeline(cfg config.Config, workflowID string, yes bool) error {
	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx := context.Background()
	resp, err := c.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		// Only a genuinely absent id is a usage error; anything else returns
		// plainly (the "check the id" advice needs the server that failed).
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			usageFail("workflow %s: not found — check the id against \"daedalus list\"", workflowID)
		}
		return fmt.Errorf("workflow %s: %w", workflowID, err)
	}
	info := resp.GetWorkflowExecutionInfo()
	// The workflow id alone does not say which queue owns it: erasing across
	// queues would destroy someone else's run (same mismatch rule as
	// wakeup). The queue is resolved from the active config — the -c chain
	// says which worker's session the operator means.
	if q := info.GetTaskQueue(); q != cfg.Temporal.TaskQueue {
		return fmt.Errorf("workflow %s runs on task queue %q, not this config's %q — point -c at the config of the queue that owns it",
			workflowID, q, cfg.Temporal.TaskQueue)
	}
	status := info.GetStatus()

	// The run's own recorded input names everything on disk: the target
	// repo, the issue id, the flow (scoping the worktree path and branch
	// names), and the branch prefix the preserved branches carry.
	prev, _, err := readPriorRun(c, workflowID)
	if err != nil {
		return err
	}
	if prev.IssueID == "" {
		return fmt.Errorf("workflow %s history carries no daedalus pipeline input", workflowID)
	}
	scope := workflows.FlowScope(prev.Flow)
	segment := activities.IssuePathSegment(scope, prev.IssueID)
	// A pre-branch-prefix run (empty in its recorded history) resolves like
	// continue does: through the operator's current config.
	prefix := resolveBranchPrefix(prev.BranchPrefix, cfg.BranchPrefix)
	worktree, err := activities.WorktreePathForFlow(cfg.Temporal.TaskQueue, prev.IssueID, scope)
	if err != nil {
		return err
	}
	aborted, err := activities.AbortedBranchNameFor(prev.IssueID, scope)
	if err != nil {
		return err
	}
	sessionFile, err := activities.SessionStatePath(worktree)
	if err != nil {
		return err
	}
	logFile, err := activities.TaskLogPath(workflowID)
	if err != nil {
		return err
	}
	// The branch globs naming everything the run keeps in the target repo.
	// Both the per-issue segment and the in-flight prefix come from
	// activities, so the naming rule stays single-sourced.
	inFlightGlob := activities.InFlightBranchPrefix + segment + "-*"
	preservedGlob := prefix + "/" + segment + "-*"

	// A parked run's preserved work is resumable via `daedalus continue`;
	// wipe destroys that path permanently. A park shows only as the failed
	// execution's message (the same text awaitPipeline matches), so probe
	// the outcome of an already-closed run for it — a running run cannot
	// be parked yet.
	parked := false
	if status != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		pctx, cancel := context.WithTimeout(ctx, time.Minute)
		var out string
		if err := c.GetWorkflow(pctx, workflowID, "").Get(pctx, &out); err != nil &&
			strings.Contains(err.Error(), workflows.ErrAwaitingMaintainer.Error()) {
			parked = true
		}
		cancel()
	}

	if !yes {
		fmt.Printf("daedalus wipe %s (status: %v)\n", workflowID, status)
		fmt.Printf("  repo:          %s\n", prev.RepoPath)
		fmt.Printf("  worktree:      %s\n", worktree)
		fmt.Printf("  branches:      %s, %s, %s (deleted from the repo)\n",
			inFlightGlob, preservedGlob, aborted)
		fmt.Printf("  session file:  %s\n", sessionFile)
		fmt.Printf("  task log:      %s\n", logFile)
		fmt.Println("Every artifact above is erased permanently; Temporal's history is the only record left.")
		if parked {
			fmt.Println("WARNING: this run is parked — its preserved work is what `daedalus continue` would resume; wipe destroys it permanently.")
		}
		fmt.Print(`Type "yes" to wipe: `)
		answer, rerr := readConfirmLine()
		if rerr != nil {
			return fmt.Errorf("wipe aborted — no confirmation given (pass --yes for scripted use)")
		}
		switch strings.ToLower(strings.TrimSpace(answer)) {
		case "y", "yes":
		default:
			return fmt.Errorf("wipe aborted — nothing was erased")
		}
	}

	// Stop the run first: canceling closes the workflow (its deferred
	// cleanup preserves-or-removes the worktree on a disconnected context),
	// and erasing before that closes would race the cleanup activities.
	if status == enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		fmt.Printf("Stopping %s (cancel + wait for close)...\n", workflowID)
		if err := c.CancelWorkflow(ctx, workflowID, ""); err != nil {
			return fmt.Errorf("cancel workflow %s: %w", workflowID, err)
		}
		if err := awaitWipeClose(c, workflowID); err != nil {
			return err
		}
	}

	// Erase, best-effort with per-artifact reporting: a failure is recorded
	// and the remaining artifacts still erased.
	var failed []string
	erase := func(what string, existed bool, err error) {
		switch {
		case err != nil:
			fmt.Printf("  %-16s FAILED: %v\n", what, err)
			failed = append(failed, what)
		case existed:
			fmt.Printf("  %-16s removed\n", what)
		default:
			fmt.Printf("  %-16s already absent\n", what)
		}
	}

	fmt.Printf("Erasing:\n")
	// The worktree, plus the target repo's .git/worktrees/ admin metadata —
	// `worktree remove --force` for a registered tree, `worktree prune` so
	// a stale registration (a crashed run's) is cleaned too, and a plain
	// RemoveAll for a leftover directory git no longer knows about. The
	// remove failure is tolerated: an unregistered path fails it, and prune
	// below still cleans whatever metadata git did hold.
	_, statErr := os.Stat(worktree)
	existed := statErr == nil
	var wtErr error
	_, _ = wipeGit(prev.RepoPath, "worktree", "remove", "--force", worktree)
	if _, err := wipeGit(prev.RepoPath, "worktree", "prune"); err != nil {
		wtErr = err
	}
	if wtErr == nil {
		if _, err := os.Stat(worktree); err == nil {
			wtErr = os.RemoveAll(worktree)
		}
	}
	erase("worktree", existed, wtErr)

	// Branches: in-flight (crashed runs' stale ones included — the normal
	// path only sweeps them on the next run for the same issue), every
	// preserved deliverable, and the aborted/ continue snapshot (deleting
	// it is what makes post-wipe `continue` fail cleanly). Each category
	// reports its own line.
	for _, cat := range []struct{ label, pattern string }{
		{"in-flight branches", inFlightGlob},
		{"preserved branches", preservedGlob},
		{"aborted branch", aborted},
	} {
		n, err := wipeBranches(prev.RepoPath, cat.pattern)
		erase(cat.label, n > 0, err)
	}

	existed, err = removeFile(sessionFile)
	erase("session file", existed, err)
	existed, err = removeFile(logFile)
	erase("task log", existed, err)

	if len(failed) > 0 {
		return fmt.Errorf("wipe of %s left %d artifact(s) failed: %s", workflowID, len(failed), strings.Join(failed, ", "))
	}
	fmt.Printf("Wiped %s — Temporal's history is now the only record of it.\n", workflowID)
	return nil
}

// awaitWipeClose polls the workflow until it is no longer running — the
// cancellation wipe requested has closed it — or wipeCloseWait runs out.
func awaitWipeClose(c client.Client, workflowID string) error {
	deadline := time.Now().Add(wipeCloseWait)
	for {
		resp, err := c.DescribeWorkflowExecution(context.Background(), workflowID, "")
		if err != nil {
			return fmt.Errorf("describe workflow %s: %w", workflowID, err)
		}
		if status := resp.GetWorkflowExecutionInfo().GetStatus(); status != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("workflow %s did not close within %s of cancellation — its cleanup is still running, and erasing now would race it; retry once it has closed",
				workflowID, wipeCloseWait)
		}
		time.Sleep(wipePollInterval)
	}
}

// wipeBranches deletes every local branch in repoPath matching the glob
// pattern, returning how many were deleted; listing failures and failed
// deletions are returned (joined) — unlike the activities' best-effort
// sweeps, a failed deliberate destruction must be reported, not skipped.
func wipeBranches(repoPath, pattern string) (int, error) {
	out, err := wipeGit(repoPath, "branch", "--list", pattern, "--format=%(refname:short)")
	if err != nil {
		return 0, err
	}
	deleted := 0
	var errs []error
	for name := range strings.FieldsSeq(out) {
		if _, err := wipeGit(repoPath, "branch", "-D", name); err != nil {
			errs = append(errs, fmt.Errorf("delete %s: %w", name, err))
			continue
		}
		deleted++
	}
	return deleted, errors.Join(errs...)
}

// removeFile deletes path, reporting whether the file existed; an
// already-absent file is not an error (wipe of a run that never started
// rounds reports "already absent" across the board).
func removeFile(path string) (bool, error) {
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return true, err
	}
	return true, nil
}

// wipeGit runs a git command against repoPath on the operator's host and
// wraps any failure with the command line and output for the report.
func wipeGit(repoPath string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", repoPath}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("git %s: %s: %s",
			strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// readConfirmLine reads one confirmation line from stdin; an empty read
// (EOF with nothing typed) is an error, so a closed stdin can never confirm.
func readConfirmLine() (string, error) {
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && line == "" {
		return "", err
	}
	return line, nil
}
