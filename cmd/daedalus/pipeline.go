package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	commonpb "go.temporal.io/api/common/v1"
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

// sharedTestQueueInput resolves config shared_test_queue into the
// PipelineInput field the workflow routes suite executions by: always a
// concrete value (never nil from here), so the run records the routing its
// starting config chose — shared or per-deployment — instead of falling
// back at schedule time.
func sharedTestQueueInput(cfg config.Config) *bool {
	shared := cfg.SharesTestQueue()
	return &shared
}

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

// confirmCleanRepo is run's repo preflight: a dirty working tree in the
// repo sessions branch from is surfaced to the operator before the
// pipeline starts. Dirty is what `git status --porcelain` reports —
// staged, unstaged, and untracked files alike (the repo state the run's
// worktree and branches derive from). Declining — or a closed stdin, so a
// scripted run can never confirm by accident — returns before any
// workflow is dispatched: no session, branch, or worktree exists yet, so
// the cancellation leaves nothing behind by construction.
func confirmCleanRepo(repoPath string) error {
	cmd := exec.Command("git", "-C", repoPath, "status", "--porcelain")
	cmd.Env = envWithoutGitRepoOverrides(os.Environ())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("check %s for uncommitted changes: %v: %s", repoPath, err, strings.TrimSpace(string(out)))
	}
	if strings.TrimSpace(string(out)) == "" {
		return nil
	}
	fmt.Printf("repo %s has uncommitted changes:\n%s\n", repoPath, strings.TrimRight(string(out), "\n"))
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
// exist. (The worker re-validates all of this through
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

// startPipeline triggers the named workflow for the given issue on the
// configured task queue. configPath is the resolved config file the run
// loaded (the preflight-started daemon is re-executed with it).
// branchPrefix names the run's preserved branch; agent, when set by
// -cli/--cli, overrides the config's jailed agent for this run. Unless
// detach is set, it then blocks until the pipeline finishes.
func startPipeline(cfg config.Config, configPath, workflowName, repoPath, issueID, prompt string, detach bool, branchPrefix, agent string) error {
	return startPipelineFolders(cfg, configPath, workflowName, repoPath, issueID, prompt, detach, branchPrefix, agent, nil, "")
}

// startPipelineFolders is startPipeline with the run's validated folder
// grants (resolveFolderGrants) and, when set, the workflow id the run is
// chained behind (-dep/--depends). (Split so startPipeline's existing
// signature — and the tests pinning it — stays untouched.)
func startPipelineFolders(cfg config.Config, configPath, workflowName, repoPath, issueID, prompt string, detach bool, branchPrefix, agent string, folders []string, depends string) error {
	spec, ok := workflowRegistry[workflowName]
	if !ok {
		return fmt.Errorf("unknown workflow %q (available: %s)", workflowName, workflowNames())
	}
	repoPath, err := resolveRepoPath(repoPath)
	if err != nil {
		return err
	}
	if err := confirmCleanRepo(repoPath); err != nil {
		return err
	}
	if err := ensureWorkerActive(cfg, configPath); err != nil {
		return err
	}

	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	// Best-effort alert, never an error: a shared grant with another
	// running pipeline means concurrent, uncoordinated writes to that
	// folder.
	warnFolderOverlap(c, cfg, folders)

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
	if depends != "" {
		// Resolve the release-with-fallback default once, at submit: an
		// enabled section without an explicit fallback_branch releases onto
		// the invocation branch, and the resolved name travels in the
		// workflow input — a config edit mid-run cannot retarget an
		// in-flight chain, and replay re-executes the gate against the
		// exact branch the live run saw. cfg is a value copy, so the
		// mutation here never writes back to the caller's config. An
		// explicitly set branch is verified to exist here — bad input
		// failing at submit, not after the dependency has been waited out.
		if cfg.Dependency.EnabledOrDefault() {
			if cfg.Dependency.FallbackBranch == "" {
				b, err := invocationBranch(repoPath)
				if err != nil {
					return err
				}
				cfg.Dependency.FallbackBranch = b
			} else if err := verifyBranchExists(repoPath, cfg.Dependency.FallbackBranch); err != nil {
				return err
			}
		}
		if err := preflightDependency(c, cfg, workflowID, depends); err != nil {
			return err
		}
	}
	// The workflow starts by its registered type name (WorkflowTypeName) —
	// the same name the worker registers the CompleteGreen-wrapped flow
	// under. The registry's Fn is unwrapped and would reflect to that name
	// by itself, but the string pins both ends to WorkflowTypeName as the
	// single derivation, so a future wrapping of the registry's Fn cannot
	// desynchronize start from registration.
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: cfg.Temporal.TaskQueue,
		// Allow re-running an issue whose previous pipeline succeeded
		// instead of failing with an already-exists error.
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}, workflows.WorkflowTypeName(spec.Fn), workflows.PipelineInput{
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
		SharedTestQueue: sharedTestQueueInput(cfg),
		TestOutputDir:   cfg.TestOutputDir(),
		Folders:         folders,
		DependsOn:       depends,
		Dependency:      cfg.Dependency,
	})
	if err != nil {
		return fmt.Errorf("start workflow: %w", err)
	}
	if depends != "" {
		// The pre-round note: while the gate holds the run in pending, no
		// round has ever written to the task log — this line is what
		// `daedalus log` (and the idle --status brief) shows meanwhile.
		// Best-effort: a failed write never fails the run.
		if err := activities.AppendSubmitNote(workflowID, "waiting on dependency: "+depends); err != nil {
			fmt.Printf("warning: could not write the task-log dependency note: %v\n", err)
		}
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

// invocationBranch names the branch the run is submitted from — the
// default dependency.fallback_branch: a dependent run released past a dead
// dependency continues from where the operator stood when submitting the
// chain. Same env hygiene as resolveRepoPath. A detached HEAD names no
// branch and is refused: "--abbrev-ref" prints the literal "HEAD" there,
// and falling back to a raw commit reference the operator did not name
// would start work from a moving target's idea of a branch — setting
// fallback_branch explicitly is the fix the error names.
func invocationBranch(repoPath string) (string, error) {
	cmd := exec.Command("git", "-C", repoPath, "rev-parse", "--abbrev-ref", "HEAD")
	cmd.Env = envWithoutGitRepoOverrides(os.Environ())
	out, err := cmd.CombinedOutput()
	branch := strings.TrimSpace(string(out))
	if err != nil {
		return "", fmt.Errorf("resolve the invocation branch of %s: %v: %s", repoPath, err, branch)
	}
	if branch == "HEAD" || branch == "" {
		return "", fmt.Errorf("resolve the invocation branch of %s: detached HEAD names no branch — set dependency.fallback_branch in the config to release past a stopped dependency", repoPath)
	}
	return branch, nil
}

// verifyBranchExists refuses a named branch that does not exist as a
// branch in the run's repo — dependency.fallback_branch is verified at
// submit so a typo fails the run before the dependency gate instead of
// after it, with git's raw "invalid reference" at release time. The
// full refname (refs/heads/<branch>) pins the check to branches: a tag or
// a SHA would resolve --verify alone and is not a worktree base this
// feature promises. Same env hygiene as resolveRepoPath.
func verifyBranchExists(repoPath, branch string) error {
	cmd := exec.Command("git", "-C", repoPath, "rev-parse", "--verify", "--quiet", "refs/heads/"+branch)
	cmd.Env = envWithoutGitRepoOverrides(os.Environ())
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("dependency.fallback_branch %q: no such branch in %s (git: %s) — create it or fix the config before chaining behind a dependency",
			branch, repoPath, strings.TrimSpace(string(out)))
	}
	return nil
}

// preflightDependency is the submit-time half of the dependency gate: the
// dependency is named by workflow id verbatim (the same convention as
// continue/attach/wipe) and must exist on this config's task queue, so a
// typo or a foreign-queue id fails before dispatch instead of pending
// forever. A dependency already terminal without approval (failed,
// canceled, wiped — or completed with a park marker) means the chain is
// already broken: the run would fail at its first probe anyway, so the
// refusal happens here — unless the config's dependency section is in
// release posture (cfg.Dependency, with the fallback branch already
// resolved by startPipelineFolders), in which case the dispatch goes ahead
// and the workflow gate applies the skip flags with the same resolved
// section. A paused dependency passes through unconditionally: it is
// resumable (`temporal workflow unpause`), not broken, and the gate waits
// for it. Terminal-approved passes straight through — the workflow's first
// probe sees the approved completion and skips the wait (idempotent
// re-submits). Anything still in flight dispatches and lets the workflow
// gate block.
func preflightDependency(c client.Client, cfg config.Config, workflowID, depID string) error {
	if depID == workflowID {
		return fmt.Errorf("dependency %s is this run's own id — a run cannot depend on itself", depID)
	}
	resp, err := c.DescribeWorkflowExecution(context.Background(), depID, "")
	if err != nil {
		// Only a genuinely absent id is a usage error; an unreachable
		// server or any other failure returns plainly — the "check the id
		// against daedalus list" advice needs the server that just failed.
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			usageFail("dependency %s: not found — check the id against \"daedalus list\"", depID)
		}
		return fmt.Errorf("describe dependency %s: %w", depID, err)
	}
	info := resp.GetWorkflowExecutionInfo()
	// Same queue-ownership check as wakeup: chaining across queues would
	// couple deployments whose workers cannot even see each other's
	// activities coherently — the id says which queue owns the dependency.
	if q := info.GetTaskQueue(); q != cfg.Temporal.TaskQueue {
		return fmt.Errorf("dependency %s runs on task queue %q, not this config's %q — point -c at the config of the queue that owns it",
			depID, q, cfg.Temporal.TaskQueue)
	}
	switch s := info.GetStatus(); s {
	case enums.WORKFLOW_EXECUTION_STATUS_RUNNING, enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		enums.WORKFLOW_EXECUTION_STATUS_PAUSED:
		return nil
	case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		var result string
		if err := c.GetWorkflow(context.Background(), depID, "").Get(context.Background(), &result); err != nil {
			return fmt.Errorf("read result of dependency %s: %w", depID, err)
		}
		if workflows.IsParkedResult(result) && !cfg.Dependency.ReleasePosture() {
			return fmt.Errorf("dependency %s completed parked — the chain is already broken; resume it with `daedalus continue %s \"<prompt>\"` first",
				depID, depID)
		}
		return nil
	default:
		if cfg.Dependency.ReleasePosture() {
			return nil
		}
		return fmt.Errorf("dependency %s is %s — the chain is already broken; resolve it first (closed sessions resume with: daedalus continue %s \"<prompt>\")",
			depID, s, depID)
	}
}

// warnFolderOverlap prints a best-effort warning when this run's folder
// grants overlap a currently running pipeline's on the same task queue:
// concurrent append-one-row edits to a shared folder (a done-index) are
// hand-fixable, so this is an alert naming the shared folder and the other
// run — not an error, since refusing the run would turn a bookkeeping
// convenience into a scheduling constraint. Any failure enumerating (an
// unreachable visibility store, an unreadable history) degrades to silence,
// never a failure. The workflow has not started yet, so the run's own grants
// cannot match themselves.
func warnFolderOverlap(c client.Client, cfg config.Config, folders []string) {
	if len(folders) == 0 {
		return
	}
	granted := make(map[string]bool, len(folders))
	for _, f := range folders {
		granted[f] = true
	}
	resp, err := c.ListWorkflow(context.Background(), &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: "default",
		Query:     fmt.Sprintf("TaskQueue = '%s' AND ExecutionStatus = 'Running'", cfg.Temporal.TaskQueue),
	})
	if err != nil {
		return
	}
	dc := converter.GetDefaultDataConverter()
	for _, ex := range resp.GetExecutions() {
		var prev workflows.PipelineInput
		iter := c.GetWorkflowHistory(context.Background(),
			ex.GetExecution().GetWorkflowId(), ex.GetExecution().GetRunId(),
			false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			ev, err := iter.Next()
			if err != nil {
				break
			}
			if att := ev.GetWorkflowExecutionStartedEventAttributes(); att != nil {
				// Only the started event carries the input; stop walking as
				// soon as it is seen (or undecodable).
				if ps := att.GetInput().GetPayloads(); len(ps) > 0 {
					if dc.FromPayload(ps[0], &prev) == nil {
						for _, f := range prev.Folders {
							if granted[f] {
								fmt.Printf("warning: folder %s is also granted to running pipeline %s — writes to it are not coordinated across runs\n",
									f, ex.GetExecution().GetWorkflowId())
							}
						}
					}
				}
				break
			}
		}
	}
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

// runVisibility is the decoded trio of run-visibility search attributes the
// workflow upserts (workflows.DaedalusStatusAttr, LastActivityAttr,
// DaedalusDependsOnAttr). Named fields, so no render site indexes a payload
// map.
type runVisibility struct {
	Status    string
	LastAt    time.Time
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

// listPipelines prints the most recent sessions on this task queue, newest
// first. STATUS prefers the workflow's upserted DaedalusStatus over the raw
// Temporal enum — parks and approvals are both Completed there — falling
// back to the enum for old runs without the attribute. TIME is one merged
// cell: while running, elapsed since the last completed round next to the
// run's age (staleness while sleeping is the liveness signal); once closed,
// total runtime next to how long ago it ended. A run predating the
// attributes shows `-` for the elapsed half.
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
		status := info.GetStatus().String()
		dependsOn := vis.DependsOn
		if dependsOn == "" {
			dependsOn = "-"
		}
		started := info.GetStartTime().AsTime()
		if closed := info.GetCloseTime(); closed.IsValid() {
			// Closed: the enum is overridden only by a terminal
			// DaedalusStatus — CompleteGreen stamps those alone. A run
			// closed by other means keeps a live-state attribute (a wipe
			// cancel mid-heartbeat carries waiting, a killed worker leaves
			// running on a TimedOut run), which must not shadow what
			// Temporal recorded.
			switch workflows.RunStatus(vis.Status) {
			case workflows.StatusApproved, workflows.StatusParked, workflows.StatusFailed:
				status = vis.Status
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t[%s | ended %s ago]\n",
				id, status, dependsOn, dur(closed.AsTime().Sub(started)), dur(now.Sub(closed.AsTime())))
			continue
		}
		if vis.Status != "" {
			status = vis.Status
		}
		elapsed := "-"
		if !vis.LastAt.IsZero() {
			elapsed = dur(now.Sub(vis.LastAt))
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t[%s | started %s ago]\n",
			id, status, dependsOn, elapsed, dur(now.Sub(started)))
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
		Folders:         prev.Folders,
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
// produced no deliverable. awaitPipeline has already printed the resume
// hint by the time it returns this; the error only labels the outcome.
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
