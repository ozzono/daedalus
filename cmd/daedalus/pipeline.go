package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflow "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"golang.org/x/sys/unix"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/runstart"
	"github.com/ozzono/daedalus/internal/shrink"
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

// sharedTestQueueInput resolves config shared_test_queue into the
// PipelineInput field the workflow routes suite executions by (the gate
// itself lives in internal/runstart, next to the input build it feeds).
func sharedTestQueueInput(cfg config.Config) *bool {
	return runstart.SharedTestQueue(cfg)
}

// resolveRepoPath turns the operator's repo path into an absolute path and
// verifies it is a git repository before the workflow starts (the check
// lives in internal/runstart, shared with the dashboard's form submit).
func resolveRepoPath(repoPath string) (string, error) {
	return runstart.ResolveRepoPath(repoPath)
}

// confirmCleanRepo is run's repo preflight: a dirty working tree in the
// repo sessions branch from is surfaced to the operator before the
// pipeline starts. Dirty is what `git status --porcelain` reports —
// staged, unstaged, and untracked files alike (the repo state the run's
// worktree and branches derive from). Declining — or a closed stdin, so a
// scripted run can never confirm by accident — returns before any
// workflow is dispatched: no session, branch, or worktree exists yet, so
// the cancellation leaves nothing behind by construction.
func confirmCleanRepo(repoPath string) error {
	dirty, status, err := runstart.RepoStatus(repoPath)
	if err != nil {
		return err
	}
	if !dirty {
		return nil
	}
	fmt.Printf("repo %s has uncommitted changes:\n%s\n", repoPath, status)
	fmt.Print(`Start the pipeline anyway? Type "y" to proceed: `)
	answer, rerr := readConfirmLine()
	if rerr != nil {
		return fmt.Errorf("run aborted — no confirmation given (repo %s is dirty; nothing was started)", repoPath)
	}
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return nil
	default:
		return fmt.Errorf("run aborted — commit or stash the changes in %s first (nothing was started)", repoPath)
	}
}

// workerBootWait bounds how long run's worker preflight waits for a
// freshly started daemon to publish its first status file.
const workerBootWait = 30 * time.Second

// ensureWorkerActive is run's worker preflight: the worker serving cfg
// (named worker_id, else the task queue — the same worker `worker status`
// reports on) must be running before a workflow is dispatched, and is
// started here when it is not. workerStart returns once the daemon is
// spawned; "up" is its first published status file naming the spawned pid
// — the daemon got past config load and the worktree-root preflight and
// is about to poll. A boot that never publishes (a bad config, a failed
// preflight) is a clean error here, not a workflow silently queued for a
// poller that will never come. The daemon is started untyped (both
// pollers), like bare `worker start`: a run always needs the main-queue
// poller, and -t/--type is rejected on run for exactly that reason.
func ensureWorkerActive(cfg config.Config, configPath string) error {
	name := cfg.WorkerName()
	pidFile, logFile, _ := daemonPaths(name)
	if _, ok := readLivePid(pidFile); ok {
		return nil
	}
	fmt.Printf("worker %s is not running — starting it\n", name)
	if err := workerStart(cfg, configPath, ""); err != nil {
		return fmt.Errorf("worker preflight: %w", err)
	}
	statusFile := filepath.Join(daemonDir, "worker-"+name+".status")
	for start := time.Now(); time.Since(start) < workerBootWait; time.Sleep(200 * time.Millisecond) {
		// readLivePid counts a zombie as not alive (see pidAlive): the
		// daemon spawned below is this process's released child, so one
		// that died at boot lingers as a zombie for as long as run waits —
		// and a zombie still answers kill(0). Either way there is nothing
		// to wait out, and the reason is in its log.
		if pid, ok := readLivePid(pidFile); ok {
			data, err := os.ReadFile(statusFile)
			if err == nil {
				var st workerStatus
				if json.Unmarshal(data, &st) == nil && st.PID == pid {
					fmt.Printf("worker %s is up (pid %d)\n", name, pid)
					return nil
				}
			}
		} else {
			return fmt.Errorf("worker preflight: worker %s exited during startup — check its log: %s", name, logFile)
		}
	}
	return fmt.Errorf("worker preflight: worker %s stayed up but never became ready within %s — check its log: %s", name, workerBootWait, logFile)
}

// resolveFolderGrants validates a fresh run's folder grants: each
// -folder/--folder occurrence plus the containing folder of the -f/--file
// task file (the file itself is the prompt; the grant is its folder, so the
// run can retire the brief and update shared indexes through the mount).
// Each grant resolves to a cleaned absolute path that must exist, be a
// directory, not be the filesystem root, and carry no colon — it composes
// into ai-jail's --rw-map <source>:<dest> mount spec — and two grants
// sharing a basename are rejected, the second shadowing the first inside
// .daedalus-folders. A bad grant should fail here at submit, not at the
// first round. Duplicates — the same folder granted twice, or via both -f
// and --folder — collapse to one grant. A glob
// task file's directory part is taken literally, never globbed, so one
// carrying metachars is rejected instead of granting a path that does not
// exist. The grants returned here are not yet split by location —
// runstart.Start resolves each against the repo root, turning in-repo
// grants into write-scope extensions (never mounts) and leaving out-of-repo
// grants to mount. (The worker re-validates all of this through
// activities.FolderMounts; this is the submit-time mirror.)
func resolveFolderGrants(flagFolders []string, taskFile string) ([]string, error) {
	grants := flagFolders
	if taskFile != "" {
		dir := filepath.Dir(taskFile)
		if strings.ContainsAny(dir, `*?[\`) {
			return nil, fmt.Errorf("-f %s: its folder %q carries glob metachars and is taken literally, so it cannot be granted — pass --folder with a plain path instead", taskFile, dir)
		}
		grants = append(grants, dir)
	}
	var out []string
	seen := map[string]bool{}
	bases := map[string]bool{}
	for _, grant := range grants {
		abs, err := filepath.Abs(grant)
		if err != nil {
			return nil, fmt.Errorf("folder grant %s: %w", grant, err)
		}
		if abs == string(filepath.Separator) {
			return nil, errors.New("folder grant must not be the filesystem root")
		}
		if strings.ContainsRune(abs, ':') {
			return nil, fmt.Errorf("folder grant %s must not contain %q — it composes into the jail's --rw-map <source>:<dest> mount spec", abs, ":")
		}
		info, err := os.Stat(abs)
		if err != nil {
			return nil, fmt.Errorf("folder grant %s: %w", abs, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("folder grant %s is not a directory", abs)
		}
		if seen[abs] {
			continue
		}
		seen[abs] = true
		// Same shadow check the worker's FolderMounts applies: two distinct
		// folders mounting under one basename would silently hide the
		// first from the agent.
		base := filepath.Base(abs)
		if bases[base] {
			return nil, fmt.Errorf("two folder grants share the basename %q — the second would shadow the first inside .daedalus-folders", base)
		}
		bases[base] = true
		out = append(out, abs)
	}
	return out, nil
}

// slimInputGate applies the slim flow's input diet to a run's task input
// before anything is submitted: the shrink pass first (internal/shrink —
// structural collapse, prose minification, extractive trimming, gated by
// the caveman invariants), then the token ceiling measured on the shrunk
// text that would actually ship in the prompt. Both the run and continue
// paths route through it — on a continue the prompt is the task input the
// workflow re-plans from. The count is the max of the strict o200k_base
// count and the biased-high chars/4 estimate — the estimator class pi's
// own prompt sizing uses — so the gate cannot under-count against the
// arithmetic that governs the wire budget. Over the cap the run never
// starts — no Temporal workflow, no worktree, no worker round — and the
// error asks the operator to split the task.
//
// The biased-high estimate also runs first, on the raw input, as an
// instant refusal: the strict counter's BPE merge is quadratic in a
// single unbroken word, so letting it near an oversized blob (a base64
// wall, a minified one-liner) would hang the submit for minutes on the
// exact input class the gate exists to reject. The trade: an input whose
// raw estimate is already over the cap is refused before the shrink can
// rescue it — the shrink's headroom applies within the estimate cap — and
// an estimate at/below the cap bounds the strict counter's input to
// ~4×cap characters, sub-second worst case.
func slimInputGate(prompt string, cfg config.Config) (string, error) {
	limit := cfg.Slim.MaxInputTokensOrDefault()
	if est := shrink.Estimate(prompt); est > limit {
		return "", slimOverCapErr(est, limit)
	}
	shrunk := shrink.Task(prompt)
	n, err := shrink.Count(shrunk)
	if err != nil {
		return "", fmt.Errorf("count slim input: %w", err)
	}
	if est := shrink.Estimate(shrunk); est > n {
		n = est
	}
	if n <= limit {
		return shrunk, nil
	}
	return "", slimOverCapErr(n, limit)
}

// slimOverCapErr is the gate's one refusal diagnostic. n is the gate count
// that lost to limit — the raw input's biased-high estimate when the early
// arm refused, max(strict o200k count, estimate) of the shrunk text when
// the post-shrink arm did.
func slimOverCapErr(n, limit int) error {
	return fmt.Errorf(
		"slim input too large: %d tokens against the %d-token slim cap — split the work into a smaller task or pass smaller context (-f a smaller file), or raise slim.max_input_tokens in the config",
		n, limit)
}

// issueIDGate rejects ids a workflow id cannot safely carry (the gate
// lives in internal/runstart, fired at the top of both submit paths so bad
// input fails before any side effect — worker preflight, daemon dial).
func issueIDGate(issueID string) error {
	return runstart.IssueIDGate(issueID)
}

// startPipeline triggers the named workflow for the given issue on the
// configured task queue. configPath is the resolved config file the run
// loaded (the preflight-started daemon is re-executed with it).
// branchPrefix names the run's preserved branch; agent, when set by
// -cli/--cli, overrides the config's jailed agent for this run. Unless
// detach is set, it then blocks until the pipeline finishes.
func startPipeline(cfg config.Config, configPath, workflowName, repoPath, issueID, prompt string, detach bool, branchPrefix, agent string) error {
	return startPipelineFolders(cfg, configPath, workflowName, repoPath, issueID, prompt, detach, false, branchPrefix, agent, nil, "")
}

// startPipelineFolders is startPipeline with the run's validated folder
// grants (resolveFolderGrants) and, when set, the workflow id the run is
// chained behind (-dep/--depends). (Split so startPipeline's existing
// signature — and the tests pinning it — stays untouched.) The dispatch
// itself is internal/runstart.Start (htmx-ui pt2's Q4=C seam, shared with
// the dashboard's New Task form); this wrapper carries the parts only a
// terminal can do: the interactive dirty-repo confirm — whose affirmation
// rides to Start in Request.DirtyOK, so a confirmed dirty tree starts the
// run instead of dead-ending on the seam's refusal — and the worker
// daemon auto-start, in the same order the preflights always ran — repo
// confirm before anything touches a daemon file.
func startPipelineFolders(cfg config.Config, configPath, workflowName, repoPath, issueID, prompt string, detach, skipPreflight bool, branchPrefix, agent string, folders []string, depends string) error {
	// The cheap submit gates first, exactly where they always ran: a bad
	// issue id or wire conflict must not cost a repo prompt or spawn a
	// worker daemon.
	req := runstart.Request{
		WorkflowName:  workflowName,
		RepoPath:      repoPath,
		IssueID:       issueID,
		Prompt:        prompt,
		BranchPrefix:  branchPrefix,
		Agent:         agent,
		Folders:       folders,
		Depends:       depends,
		Warn:          os.Stdout,
		SkipPreflight: skipPreflight,
	}
	if err := runstart.Validate(cfg, req); err != nil {
		return err
	}
	abs, err := resolveRepoPath(repoPath)
	if err != nil {
		return err
	}
	if err := confirmCleanRepo(abs); err != nil {
		return err
	}
	// The confirm was affirmed (or the tree was clean): carry that across
	// the seam, so Start's clean-repo gate — the dashboard's refusal — does
	// not fire after the operator already answered it.
	req.DirtyOK = true
	if err := ensureWorkerActive(cfg, configPath); err != nil {
		return err
	}
	run, err := runstart.Start(cfg, req)
	if err != nil {
		// A dependency id naming no execution is a usage error — the same
		// screen the pre-refactor submit answered with.
		if errors.Is(err, runstart.ErrDependencyNotFound) {
			usageFail("%s", err)
		}
		return err
	}
	if depends != "" {
		// The pre-round note: while the gate holds the run in pending, no
		// round has ever written to the task log — this line is what
		// `daedalus log` (and the idle --status brief) shows meanwhile.
		// Best-effort: a failed write never fails the run.
		if err := activities.AppendSubmitNote(run.WorkflowID, "waiting on dependency: "+depends); err != nil {
			fmt.Printf("warning: could not write the task-log dependency note: %v\n", err)
		}
	}

	fmt.Printf("Started workflow:\n")
	fmt.Printf("  Workflow ID: %s\n", run.WorkflowID)
	fmt.Printf("  Run ID:      %s\n", run.RunID)

	if detach {
		fmt.Printf("Detached — reattach with: daedalus attach %s\n", run.WorkflowID)
		return nil
	}
	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	fmt.Printf("Press d to detach (the run keeps executing); reattach later with: daedalus attach %s\n", run.WorkflowID)
	return awaitPipeline(c.GetWorkflow(context.Background(), run.WorkflowID, run.RunID))
}

// invocationBranch and verifyBranchExists moved with the rest of the
// dependency chain's submit half into internal/runstart (Start calls them;
// continue has no dependency gate and never needed them here).

func invocationBranch(repoPath string) (string, error) {
	return runstart.InvocationBranch(repoPath)
}

func verifyBranchExists(repoPath, branch string) error {
	return runstart.VerifyBranchExists(repoPath, branch)
}

// preflightDependency is the submit-time half of the dependency gate (the
// gate itself lives in internal/runstart, shared with the dashboard's form
// submit). The CLI-side difference is the genuinely-absent dependency: a
// usage error answered with the usage screen, exactly as the pre-extraction
// gate answered it.
func preflightDependency(c client.Client, cfg config.Config, workflowID, depID string) error {
	err := runstart.PreflightDependency(c, cfg, workflowID, depID)
	if errors.Is(err, runstart.ErrDependencyNotFound) {
		usageFail("%s", err)
	}
	return err
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

// runVisibility is the decoded run-visibility search attributes the
// workflow upserts (workflows.DaedalusStatusAttr, LastActivityAttr,
// DaedalusDependsOnAttr, DaedalusStartedAtAttr). Named fields, so no render
// site indexes a payload map.
type runVisibility struct {
	Status    string
	LastAt    time.Time
	StartedAt time.Time
	DependsOn string
}

// decodeRunVisibility reads the attributes from a listed execution's search
// attributes with the default data converter. Undecodable or absent values —
// old runs never backfilled, history is immutable — degrade to zero values,
// never fail the table.
func decodeRunVisibility(sa *commonpb.SearchAttributes, dc converter.DataConverter) runVisibility {
	var vis runVisibility
	if sa == nil {
		return vis
	}
	if p, ok := sa.GetIndexedFields()[workflows.DaedalusStatusAttr]; ok {
		_ = dc.FromPayload(p, &vis.Status)
	}
	if p, ok := sa.GetIndexedFields()[workflows.LastActivityAttr]; ok {
		_ = dc.FromPayload(p, &vis.LastAt)
	}
	if p, ok := sa.GetIndexedFields()[workflows.DaedalusStartedAtAttr]; ok {
		_ = dc.FromPayload(p, &vis.StartedAt)
	}
	if p, ok := sa.GetIndexedFields()[workflows.DaedalusDependsOnAttr]; ok {
		_ = dc.FromPayload(p, &vis.DependsOn)
	}
	return vis
}

// dur renders a human duration for the runs table: whole seconds under an
// hour, one decimal in hours under a day, one decimal in days beyond.
// Negative elapsed (clock skew between server and client) clamps to zero.
func dur(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Hour {
		return d.Round(time.Second).String()
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1fh", d.Hours())
	}
	return fmt.Sprintf("%.1fd", d.Hours()/24)
}

// runDisplayStatus renders one listed execution's status string — the one
// derivation behind `daedalus list`'s STATUS column and the dashboard's
// Runs view, so the two never drift. Live runs prefer the workflow's
// upserted DaedalusStatus over the raw Temporal enum — parks and approvals
// are both Completed there — falling back to the enum for old runs without
// the attribute. Closed runs render by the display taxonomy: approved →
// completed, parked and aborted (Canceled) → paused, everything else
// unchanged; a park not carrying the stamp — pre-marker history, or a
// live-state attribute a close the stamps don't cover left behind — hides
// in a green (or, on the oldest workers, failed) close and is recognized
// by probing that row's own outcome.
func runDisplayStatus(c client.Client, info *workflow.WorkflowExecutionInfo, vis runVisibility) string {
	status := info.GetStatus().String()
	if closed := info.GetCloseTime(); closed.IsValid() {
		// Closed: a terminal DaedalusStatus — CompleteGreen stamps
		// those alone — classifies the run for display. A run closed
		// by other means keeps a live-state attribute (a wipe cancel
		// mid-heartbeat carries waiting, a killed worker leaves
		// running on a TimedOut run), which must not shadow what
		// Temporal recorded. Display taxonomy: only an approved run
		// earns completed; parked and aborted render as paused — a
		// park is a wait on the maintainer and an abort (the wipe
		// cancel, Canceled here) preserved its work for `daedalus
		// continue`, so neither is an end. A run without a terminal
		// attribute — pre-marker history, or a live-state one a close
		// the stamps don't cover left behind — classifies by close
		// enum: Canceled is an abort; a park without the parked stamp
		// hides in the outcome probe below; everything else keeps the
		// raw enum.
		switch workflows.RunStatus(vis.Status) {
		case workflows.StatusApproved:
			return "completed"
		case workflows.StatusParked:
			return "paused"
		case workflows.StatusFailed:
			return vis.Status
		default:
			switch info.GetStatus() {
			case enums.WORKFLOW_EXECUTION_STATUS_CANCELED:
				return "paused"
			case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, enums.WORKFLOW_EXECUTION_STATUS_FAILED:
				if closedRunParked(c, info.GetExecution().GetWorkflowId(), info.GetExecution().GetRunId()) {
					return "paused"
				}
			}
		}
		return status
	}
	if vis.Status != "" {
		return vis.Status
	}
	return status
}

// listPipelines prints the most recent sessions on this task queue, newest
// first. STATUS is runDisplayStatus's derivation — the same one the
// dashboard's Runs view renders. TIME is one merged
// cell, and both of its halves count runtime from DaedalusStartedAt — the
// dependency gate's release, the run's first moment of real work — falling
// back to the dispatch time a run without the attribute worked from (a
// wait is not runtime): while running, elapsed since the last completed
// round next to the work's age (staleness while sleeping is the liveness
// signal); once closed, total runtime next to how long ago it ended. A run
// still pending in its dependency wait owns no work at all, so its cell
// says so instead of counting either half. A run predating the attributes
// shows `-` for the elapsed half.
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
	fmt.Fprintln(w, "SESSION ID\tSTATUS\tDEPENDS_ON\tTIME")
	dc := converter.GetDefaultDataConverter()
	now := time.Now()
	for _, info := range resp.GetExecutions() {
		id := info.GetExecution().GetWorkflowId()
		vis := decodeRunVisibility(info.GetSearchAttributes(), dc)
		status := runDisplayStatus(c, info, vis)
		dependsOn := vis.DependsOn
		if dependsOn == "" {
			dependsOn = "-"
		}
		submitted := info.GetStartTime().AsTime()
		workedFrom := submitted
		if !vis.StartedAt.IsZero() {
			workedFrom = vis.StartedAt
		}
		if closed := info.GetCloseTime(); closed.IsValid() {
			fmt.Fprintf(w, "%s\t%s\t%s\t[%s | ended %s ago]\n",
				id, status, dependsOn, dur(closed.AsTime().Sub(workedFrom)), dur(now.Sub(closed.AsTime())))
			continue
		}
		if workflows.RunStatus(vis.Status) == workflows.StatusPending {
			fmt.Fprintf(w, "%s\t%s\t%s\t[queued | submitted %s ago]\n",
				id, status, dependsOn, dur(now.Sub(submitted)))
			continue
		}
		elapsed := "-"
		if !vis.LastAt.IsZero() {
			elapsed = dur(now.Sub(vis.LastAt))
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t[%s | started %s ago]\n",
			id, status, dependsOn, elapsed, dur(now.Sub(workedFrom)))
	}
	return w.Flush()
}

// closedPark is closedRunParkReason's memoized answer: whether the run
// parked and, when it did, the reason the park carries.
type closedPark struct {
	parked bool
	reason string
}

// closedParkMemo remembers closedRunParkReason's answer per execution. A
// closed run's outcome is immutable, and `daedalus list` derives it per
// ambiguous row per invocation while the dashboard re-derives every row
// every 5s poll — without the memo the probe (a result fetch per
// ambiguous row) would repeat forever, per viewer.
var closedParkMemo = struct {
	sync.Mutex
	m map[string]closedPark
}{m: map[string]closedPark{}}

// closedRunParked reports whether one closed run parked — the bool face
// of closedRunParkReason.
func closedRunParked(c client.Client, workflowID, runID string) bool {
	parked, _ := closedRunParkReason(c, workflowID, runID)
	return parked
}

// closedRunParkReason probes one closed run's outcome for a park and,
// when it finds one, the reason the park carries — the green completion
// CompleteGreen gives parks on current workers ("PARKED: <reason>"), or
// the awaiting-maintainer failure message the pre-CompleteGreen workers
// parked with (the same two shapes wipe recognizes). The run ID is the
// row's own execution, never empty: `continue` restarts under the same
// workflow ID, and an empty run ID would resolve the latest execution —
// classifying an old row from its continuation's outcome, and blocking
// on a live one. Only runs without a terminal DaedalusStatus are probed:
// a stamped run already says parked. The probe runs once per execution
// per process (closedParkMemo); a transient read failure is not memoized
// — the answer "unknown" is not immutable, so the next render retries —
// while the awaiting-maintainer error is the park's own hold and is as
// definitive as a read result. Any read failure degrades to false: the
// row keeps the enum's rendering, the table never fails over a probe.
func closedRunParkReason(c client.Client, workflowID, runID string) (bool, string) {
	key := workflowID + "\x00" + runID
	closedParkMemo.Lock()
	p, ok := closedParkMemo.m[key]
	closedParkMemo.Unlock()
	if ok {
		return p.parked, p.reason
	}
	pctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out string
	err := c.GetWorkflow(pctx, workflowID, runID).Get(pctx, &out)
	switch {
	case err == nil:
		p.parked = workflows.IsParkedResult(out)
		if p.parked {
			p.reason = strings.TrimPrefix(out, workflows.ParkedResultPrefix)
		}
	case strings.Contains(err.Error(), workflows.ErrAwaitingMaintainer.Error()):
		p.parked = true
		p.reason = workflows.ErrAwaitingMaintainer.Error()
	default:
		return false, ""
	}
	closedParkMemo.Lock()
	if len(closedParkMemo.m) >= uiCacheCap {
		closedParkMemo.m = map[string]closedPark{}
	}
	closedParkMemo.m[key] = p
	closedParkMemo.Unlock()
	return p.parked, p.reason
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

	prev, lastReview, gateGreen, err := readPriorRun(c, workflowID)
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

	// A continued slim run re-plans from the continue prompt — it IS the
	// task input the workflow embeds (the original task is not re-sent) —
	// so the slim input gate applies here exactly as at start: shrink,
	// cap, and fail before any submission side effect. The gate is
	// deterministic and idempotent, so re-gating an already-shrunken
	// prompt cannot double-shrink it.
	if flow == "slim" {
		if prompt, err = slimInputGate(prompt, cfg); err != nil {
			return err
		}
	}

	// The submit-time window gate applies here exactly as at start: the
	// continue prompt IS the new run's initial task input, and the
	// continued run dials the CURRENT config's provider channels like the
	// wire gate above — so a continue onto a config whose staged window
	// cannot back this prompt is refused here, before the workflow is
	// created and the aborted branch is staged, not at the first round's
	// window preflight. Placed after the slim gate so a shrunken prompt is
	// what gets measured.
	if err := runstart.WindowGate(cfg, prompt); err != nil {
		return err
	}

	// The wire-vs-agent gate applies here exactly as at start: the continued
	// run carries prev.Agent against the CURRENT config's channels, and a
	// deployment that flipped its served wire between the abort and this
	// continue would otherwise launch rounds with no channel to their agent.
	// A pre-field attempt's empty agent ran as the worker config's agent, so
	// the effective-agent resolution mirrors the start path's.
	effectiveAgent := prev.Agent
	if effectiveAgent == "" {
		effectiveAgent = cfg.Agent
	}
	if err := cfg.AgentWireConflict(effectiveAgent); err != nil {
		return err
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
		// By registered type name, like startPipeline — see its note.
	}, workflows.WorkflowTypeName(spec.Fn), workflows.PipelineInput{
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
		Agent: prev.Agent,
		// The continued run keeps the aborted attempt's folder grants,
		// frozen at start like its write-scope policy.
		Folders: prev.Folders,
		// The continued run keeps the aborted attempt's skill list, frozen
		// at start like its policy fields; a pre-field attempt's empty list
		// continues with none.
		ReviewSkillList: prev.ReviewSkillList,
		Authorship:      cfg.Authorship,
		TestTimeout:     cfg.TestsTimeout,
		AgentRunTimeout: cfg.AgentRunTimeout,
		ReviewTimeout:   cfg.ReviewTimeout,
		CleanupTimeout:  cfg.CleanupTimeout,
		SharedTestQueue: sharedTestQueueInput(cfg),
		TestOutputDir:   cfg.TestOutputDir(),
		BaseBranch:      base,
		// The skip-key, threaded down the chain: this attempt's own green
		// native-suite evidence (the preflight pass, or a green round
		// after it), or the recorded flag of an earlier link — the skip
		// records no suite of its own, so without the threading a second
		// continue of a validated chain would re-gate and re-park on the
		// preserved mid-flight worktree. A never-validated chain carries
		// neither and re-runs the gate on every continue.
		BaselineValidated: gateGreen || prev.BaselineValidated,
		// The skipped run's twin key: a chain opened on a red baseline
		// under --skip-preflight has no green round to derive
		// BaselineValidated from, so its continue would re-run the gate
		// and re-park on the very baseline the fresh run declared
		// acceptable — the recorded skip decision travels instead, frozen
		// at start like the policy fields above. Continue's CLI rejects
		// the flag; it rides the recorded input.
		SkipPreflight: prev.SkipPreflight,
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
	fmt.Printf("Press d to detach (the run keeps executing); reattach later with: daedalus attach %s\n", run.GetID())
	return awaitPipeline(run)
}

// readPriorRun walks the workflow's history for the original pipeline
// input, the last reviewer verdict, and gateGreen — whether any
// native-suite execution completed green (a failed or red suite
// completes with Passed false, a crashed one never completes), the
// recorded preflight pass `continue` threads into the new run's
// BaselineValidated.
func readPriorRun(c client.Client, workflowID string) (prev workflows.PipelineInput, lastReview activities.ReviewResult, gateGreen bool, err error) {
	dc := converter.GetDefaultDataConverter()
	scheduled := map[int64]string{}
	iter := c.GetWorkflowHistory(context.Background(), workflowID, "",
		false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			return prev, lastReview, gateGreen, fmt.Errorf("read history of %s: %w", workflowID, err)
		}
		switch {
		case ev.GetWorkflowExecutionStartedEventAttributes() != nil:
			ps := ev.GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()
			if len(ps) > 0 {
				if err := dc.FromPayload(ps[0], &prev); err != nil {
					return prev, lastReview, gateGreen, fmt.Errorf("decode pipeline input: %w", err)
				}
			}
		case ev.GetActivityTaskScheduledEventAttributes() != nil:
			a := ev.GetActivityTaskScheduledEventAttributes()
			scheduled[ev.GetEventId()] = a.GetActivityType().GetName()
		case ev.GetActivityTaskCompletedEventAttributes() != nil:
			a := ev.GetActivityTaskCompletedEventAttributes()
			switch scheduled[a.GetScheduledEventId()] {
			case "RunJailedReviewerActivity":
				ps := a.GetResult().GetPayloads()
				if len(ps) == 0 {
					continue
				}
				var r activities.ReviewResult
				if err := dc.FromPayload(ps[0], &r); err == nil {
					lastReview = r
				}
			case "RunTestSuiteActivity":
				ps := a.GetResult().GetPayloads()
				if len(ps) == 0 {
					continue
				}
				var r activities.TestResult
				if err := dc.FromPayload(ps[0], &r); err == nil && r.Passed {
					gateGreen = true
				}
			}
		}
	}
	return prev, lastReview, gateGreen, nil
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
// "wakeup" signal has two consumers — the heartbeat select and the
// dependency gate's poll wait (a run pending on a dependency ends that
// wait immediately, granting its grace cadence); a workflow sleeping in
// neither buffers it and merely shortens its next wait.
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
	fmt.Printf("Wakeup sent to %s — a quota-heartbeat or dependency-gate wait it is in (or reaches next) ends immediately; mid-round it only shortens the next one.\n", workflowID)
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
// produced no deliverable. awaitPipeline has already printed the resume
// hint by the time it returns this; the error only labels the outcome.
var errRunParked = errors.New("run parked awaiting maintainer input")

// detachKey is the keystroke that ends an attached wait without touching
// the run: the server-side workflow keeps executing and `daedalus attach`
// reconnects to it.
const detachKey = 'd'

// makeRawStdin switches stdin to a minimal raw mode so a single detachKey
// press is readable the instant it lands: canonical mode would hold it
// until Enter, and echo would print it back. Only ICANON and ECHO are
// cleared — ISIG stays on, so Ctrl-C keeps killing the process, and the
// output flags stay untouched, so the run's own output renders normally.
// The returned closure restores the saved termios; ok is false when the
// switch was impossible (no terminal, no ioctl) and the wait simply runs
// without a detach key. Package var so the suite can stub the tty seam.
var makeRawStdin = func() (restore func(), ok bool) {
	fd := int(os.Stdin.Fd())
	termios, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return nil, false
	}
	saved := *termios
	termios.Lflag &^= unix.ICANON | unix.ECHO
	termios.Cc[unix.VMIN] = 1
	termios.Cc[unix.VTIME] = 0
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, termios); err != nil {
		return nil, false
	}
	return func() { _ = unix.IoctlSetTermios(fd, unix.TCSETS, &saved) }, true
}

// watchDetachKeys reads r byte-wise until a detachKey lands, then signals
// the detach and cancels the wait context. Read errors (a closed stdin,
// a vanished tty) end the watch: losing the detach key must never lose
// the wait.
func watchDetachKeys(r io.Reader, cancel context.CancelFunc, detached chan struct{}) {
	buf := make([]byte, 1)
	for {
		n, err := r.Read(buf)
		if n > 0 && buf[0] == detachKey {
			select {
			case <-detached:
			default:
				close(detached)
			}
			cancel()
			return
		}
		if err != nil {
			return
		}
	}
}

// awaitPipeline blocks until the run finishes (bounded by runWaitTimeout)
// and reports the outcome: the preserved branch on success, a parked run's
// resume hint (as errRunParked, so callers gating on the exit code see a
// non-zero one), or the execution error. While it waits, a detachKey press
// ends the wait cleanly — exit 0, the run untouched server-side — unless
// the run finished first, which wins.
func awaitPipeline(run client.WorkflowRun) error {
	waitCtx, cancel := context.WithTimeout(context.Background(), runWaitTimeout)
	defer cancel()
	ctx, detachCancel := context.WithCancel(waitCtx)
	defer detachCancel()
	detached := make(chan struct{})
	restore, isTerm := makeRawStdin()
	if isTerm {
		defer restore()
		// Ctrl-C's default disposition kills without running defers — the
		// terminal would keep the raw mode this wait armed (echo and line
		// editing dead until reset). Catch the death signals, put the
		// termios back, reset the dispositions, and re-raise, so the
		// process dies exactly as it would have — exit status 130 under a
		// shell's Ctrl-C — with a usable terminal left behind.
		death := make(chan os.Signal, 1)
		signal.Notify(death, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Reset(syscall.SIGINT, syscall.SIGTERM)
		go func() {
			s, ok := <-death
			if !ok {
				return
			}
			restore()
			signal.Reset(syscall.SIGINT, syscall.SIGTERM)
			_ = syscall.Kill(os.Getpid(), s.(syscall.Signal))
		}()
		go watchDetachKeys(os.Stdin, detachCancel, detached)
	}
	var preservedBranch string
	if err := run.Get(ctx, &preservedBranch); err != nil {
		// The detach outcome: the wait was cancelled by the keypress, not
		// by the run failing — report the way back and leave with the run
		// still executing. A run that finished before the press arrives
		// through the success path below instead.
		select {
		case <-detached:
			fmt.Printf("Detached — the run keeps executing; reattach with: daedalus attach %s\n", run.GetID())
			return nil
		default:
		}
		// A park of an older worker (before parked runs completed green)
		// arrives here as a workflow failure carrying
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
	// The current registration completes a parked run green, with the
	// reason as the result (workflows.CompleteGreen). Same resume hint as
	// the failure-shaped parks above; the orchestrator's history already
	// shows the run green with the reason in the completion payload.
	if workflows.IsParkedResult(preservedBranch) {
		fmt.Printf("Workflow parked: %s\n",
			strings.TrimPrefix(preservedBranch, workflows.ParkedResultPrefix))
		fmt.Printf("The attempt's work is preserved on its aborted/ branch — resume with: daedalus continue %s \"<prompt>\"\n", run.GetID())
		return errRunParked
	}
	fmt.Printf("Workflow completed successfully — approved work committed to branch %s\n", preservedBranch)
	return nil
}
