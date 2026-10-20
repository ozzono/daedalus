// Package runstart is the run-submission seam shared by the CLI's `run`
// command and the htmx dashboard's New Task form (htmx-ui pt2, Q4=C):
// config in, validated input build, one ExecuteWorkflow — (Run, error)
// out. The interactive preflights (the dirty-repo confirm, the worker
// daemon auto-start) print and read the terminal, so they stay with the
// CLI's run path; this package carries everything both callers share —
// the submit gates, the repo and dependency validation, the workflow-id
// derivation, and the dispatch — and refuses (typed errors, nothing
// started) where the CLI would have asked.
package runstart

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/workflows"
)

// Registry maps the workflow names a submission may name to their flows:
// the workflow function plus the per-flow write-scope policy Start carries
// in the run's input (verified structurally before finalize — see
// workflows.Flow). Adding a flow later means adding an entry here (plus
// its workflow function and files) — no CLI or form changes.
var Registry = map[string]workflows.Flow{
	"feature-dev": {Fn: workflows.FeatureDevWorkflow},
	// dev-only is feature-dev's phase 1 alone: implementation ↔ code
	// review, landing without any test-phase execution (and without the
	// preflight suite gate — see DevOnlyWorkflow). Selection is explicit —
	// the CLI's -w or the dashboard's New-Task dropdown; no config key
	// picks a flow.
	"dev-only": {Fn: workflows.DevOnlyWorkflow},
	// slim is the micro-stepped atomic loop for context-limited
	// self-hosted models (target agent: pi): a planner round writes the
	// plan in prose and a parse round transcribes it into an ordered
	// queue of 1–2-file sub-tasks, then each
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

// DefaultWorkflow is the flow a submission without an explicit choice
// runs: the CLI's unset -w and the form's "default" dropdown option both
// resolve here (the config's slim reroute keys off this name — see the
// run path in main.go). The workflow-ID scoping in Start branches on this
// exact name too, so every "is this the legacy unscoped flow" check reads
// the constant, never a second literal.
const DefaultWorkflow = "feature-dev"

// Request is one run submission: the inputs `daedalus run` takes, as the
// form mirrors them (htmx-ui pt2). WorkflowName is the resolved flow name
// (the caller applies the config's slim default and the slim input gate —
// both are prompt-shaped, CLI/form-side steps). BranchPrefix and Agent
// arrive already resolved/overridden by the caller; empty means the
// config's values. Warn receives best-effort warnings (the shared-folder
// overlap alert); nil discards them.
type Request struct {
	WorkflowName string
	RepoPath     string
	IssueID      string
	Prompt       string
	BranchPrefix string
	Agent        string
	Folders      []string
	Depends      string
	Warn         io.Writer
	// DirtyOK carries the CLI's affirmed dirty-repo confirm across the
	// seam: Start skips checkCleanRepo when it is set, so an operator who
	// answered "y" is not refused by the gate they just passed. The
	// dashboard leaves it unset — a form submit is refused, not asked.
	DirtyOK bool
	// SkipPreflight carries `run --skip-preflight`: the run's preflight
	// gate opens on a red baseline instead of parking (the suite still
	// runs and the red result stays on record). The dashboard leaves it
	// unset — opting a form submit into an unvalidated baseline is a
	// deliberate act that belongs on the command line.
	SkipPreflight bool
}

// Run names the execution a Start started.
type Run struct {
	WorkflowID string
	RunID      string
}

// NewClient connects to the Temporal frontend described by cfg.
func NewClient(cfg config.Config) (client.Client, error) {
	c, err := client.Dial(client.Options{
		HostPort: cfg.Temporal.Host,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to temporal at %s: %w", cfg.Temporal.Host, err)
	}
	return c, nil
}

// IssueIDGate rejects ids a workflow id cannot safely carry: the id is
// interpolated verbatim into NotifyDependents' single-quoted Temporal
// visibility filter (internal/activities/depends.go), whose string
// literals have no escape — a character outside the token set (the spirit
// of config.ValidateWorkerID) breaks that filter, so the run's completion
// poke would fail every time and its dependents release only on the
// fallback poll. Fired at the top of Start so bad input fails before any
// submit-path side effect (repo check, daemon dial).
func IssueIDGate(issueID string) error {
	for _, r := range issueID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
		default:
			return fmt.Errorf("issue id %q: character %q is not allowed — use only letters, digits, '.', '_' and '-' (rename the issue and rerun); the id is interpolated into the dependent-wakeup search filter, where other characters break it", issueID, string(r))
		}
	}
	return nil
}

// SharedTestQueue resolves config shared_test_queue into the PipelineInput
// field the workflow routes suite executions by: always a concrete value
// (never nil), so the run records the routing its starting config chose —
// shared or per-deployment — instead of falling back at schedule time.
func SharedTestQueue(cfg config.Config) *bool {
	shared := cfg.SharesTestQueue()
	return &shared
}

// ResolveRepoPath turns the operator's repo path into an absolute path and
// verifies it is a git repository before the workflow starts. The path
// crosses a process boundary: activities execute on the worker daemon,
// whose working directory is wherever it was started from, so a relative
// path arriving verbatim resolved there — against a directory that may not
// be a repository — and failed every pipeline inside
// CreateWorktreeActivity with an opaque git error. That is the same
// boundary the daemon's config path was made absolute for (workerStart).
func ResolveRepoPath(repoPath string) (string, error) {
	abs, err := filepath.Abs(repoPath)
	if err != nil {
		return "", fmt.Errorf("resolve repo path: %w", err)
	}
	if info, err := os.Stat(abs); err != nil || !info.IsDir() {
		return "", fmt.Errorf("repo path %s is not a directory", repoPath)
	}
	cmd := exec.Command("git", "-C", abs, "rev-parse", "--git-dir")
	cmd.Env = EnvWithoutGitRepoOverrides(os.Environ())
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("repo path %s is not a git repository: %s", abs, strings.TrimSpace(string(out)))
	}
	return abs, nil
}

// RepoStatus is the dirty-tree probe behind both callers' clean-repo gate:
// dirty is what `git status --porcelain` reports — staged, unstaged, and
// untracked files alike (the repo state the run's worktree and branches
// derive from). err is the git failure (unreadable repo), dirty reports
// whether the porcelain output names anything.
func RepoStatus(repoPath string) (dirty bool, status string, err error) {
	cmd := exec.Command("git", "-C", repoPath, "status", "--porcelain")
	cmd.Env = EnvWithoutGitRepoOverrides(os.Environ())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return false, "", fmt.Errorf("check %s for uncommitted changes: %v: %s", repoPath, err, strings.TrimSpace(string(out)))
	}
	status = strings.TrimRight(string(out), "\n")
	return strings.TrimSpace(status) != "", status, nil
}

// checkCleanRepo is Start's clean-repo gate — the non-interactive face of
// the CLI's confirm: a dirty working tree in the repo sessions branch from
// is refused, not asked about (a form submit has no terminal to confirm
// on). The CLI's own confirm runs before Start and its affirmation rides
// in Request.DirtyOK, so this gate speaks for the dashboard alone. The
// refusal happens before any side effect — no session, branch, or
// worktree exists yet, so nothing is left behind by construction.
func checkCleanRepo(repoPath string) error {
	dirty, status, err := RepoStatus(repoPath)
	if err != nil {
		return err
	}
	if !dirty {
		return nil
	}
	return fmt.Errorf("repo %s has uncommitted changes — commit or stash them first (a dashboard submit is refused; the CLI's run asks):\n%s", repoPath, status)
}

// Validate fires Start's side-effect-free gates: the issue-id charset, the
// agent-vs-wire conflict, and the workflow registry lookup. The CLI's run
// path calls it before its interactive preflights — a bad id must never
// cost a repo prompt or a daemon auto-start — and Start fires it again on
// its own path (the dashboard's), where no preflight precedes it. The
// wire-vs-agent gate: a launch whose effective agent has no channel on
// this config's served wire would dial whatever the agent's own
// ambient/default endpoint is — failed here, before any worktree, worker
// ping, or round exists. Load-time validation deliberately omits this (an
// openai-url-only section under the default agent is a legal load: the
// section's env channel serves tooling and other agents), so the launch is
// where the conflict can be named with both the agent and the wire. The
// caller's override wins when set, mirroring the effective agent
// everywhere else.
func Validate(cfg config.Config, req Request) error {
	if err := IssueIDGate(req.IssueID); err != nil {
		return err
	}
	effectiveAgent := req.Agent
	if effectiveAgent == "" {
		effectiveAgent = cfg.Agent
	}
	if err := cfg.AgentWireConflict(effectiveAgent); err != nil {
		return err
	}
	if _, ok := Registry[req.WorkflowName]; !ok {
		return fmt.Errorf("unknown workflow %q (available: %s)", req.WorkflowName, workflowNames())
	}
	return nil
}

// Start validates and dispatches one run: the submit gates (Validate), the
// repo checks, the workflow-id derivation, the dependency chain's
// submit-time half, and the ExecuteWorkflow. It touches no terminal and
// starts no daemon — the CLI wraps it with the interactive preflights; the
// dashboard submits it bare.
func Start(cfg config.Config, req Request) (Run, error) {
	if err := Validate(cfg, req); err != nil {
		return Run{}, err
	}
	spec := Registry[req.WorkflowName]
	repoPath, err := ResolveRepoPath(req.RepoPath)
	if err != nil {
		return Run{}, err
	}
	if !req.DirtyOK {
		if err := checkCleanRepo(repoPath); err != nil {
			return Run{}, err
		}
	}

	c, err := NewClient(cfg)
	if err != nil {
		return Run{}, err
	}
	defer c.Close()

	// Best-effort alert, never an error: a shared grant with another
	// running pipeline means concurrent, uncoordinated writes to that
	// folder.
	warnFolderOverlap(c, cfg, req.Folders, req.Warn)

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
	workflowID := fmt.Sprintf("%s-%s", cfg.Temporal.TaskQueue, req.IssueID)
	if req.WorkflowName != DefaultWorkflow {
		workflowID = fmt.Sprintf("%s-%s-%s", cfg.Temporal.TaskQueue, req.WorkflowName, req.IssueID)
	}
	if req.Depends != "" {
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
				b, err := InvocationBranch(repoPath)
				if err != nil {
					return Run{}, err
				}
				cfg.Dependency.FallbackBranch = b
			} else if err := VerifyBranchExists(repoPath, cfg.Dependency.FallbackBranch); err != nil {
				return Run{}, err
			}
		}
		if err := PreflightDependency(c, cfg, workflowID, req.Depends); err != nil {
			return Run{}, err
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
		IssueID:         req.IssueID,
		Prompt:          req.Prompt,
		Flow:            req.WorkflowName,
		AllowedPaths:    spec.AllowedPaths,
		FrozenPaths:     spec.FrozenPaths,
		BranchPrefix:    req.BranchPrefix,
		Agent:           req.Agent,
		Authorship:      cfg.Authorship,
		TestTimeout:     cfg.TestsTimeout,
		AgentRunTimeout: cfg.AgentRunTimeout,
		ReviewTimeout:   cfg.ReviewTimeout,
		CleanupTimeout:  cfg.CleanupTimeout,
		SharedTestQueue: SharedTestQueue(cfg),
		TestOutputDir:   cfg.TestOutputDir(),
		Folders:         req.Folders,
		DependsOn:       req.Depends,
		Dependency:      cfg.Dependency,
		ReviewSkillList: cfg.ReviewSkillList,
		SkipPreflight:   req.SkipPreflight,
	})
	if err != nil {
		return Run{}, fmt.Errorf("start workflow: %w", err)
	}
	return Run{WorkflowID: run.GetID(), RunID: run.GetRunID()}, nil
}

// workflowNames lists the registered workflow names, comma-separated.
func workflowNames() string {
	names := make([]string, 0, len(Registry))
	for name := range Registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// ErrDependencyNotFound marks a dependency id that names no execution on
// the Temporal server — a usage error, not a submit failure: the CLI
// answers it with its usage screen, the dashboard renders it verbatim.
var ErrDependencyNotFound = errors.New("not found")

// PreflightDependency is the submit-time half of the dependency gate: the
// dependency is named by workflow id verbatim (the same convention as
// continue/attach/wipe) and must exist on this config's task queue, so a
// typo or a foreign-queue id fails before dispatch instead of pending
// forever. A dependency already terminal without approval (failed,
// canceled, wiped — or completed with a park marker) means the chain is
// already broken: the run would fail at its first probe anyway, so the
// refusal happens here — unless the config's dependency section is in
// release posture (cfg.Dependency, with the fallback branch already
// resolved by Start), in which case the dispatch goes ahead
// and the workflow gate applies the skip flags with the same resolved
// section. A paused dependency passes through unconditionally: it is
// resumable (`temporal workflow unpause`), not broken, and the gate waits
// for it. Terminal-approved passes straight through — the workflow's first
// probe sees the approved completion and skips the wait (idempotent
// re-submits). Anything still in flight dispatches and lets the workflow
// gate block.
func PreflightDependency(c client.Client, cfg config.Config, workflowID, depID string) error {
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
			return fmt.Errorf("dependency %s: %w — check the id against \"daedalus list\"", depID, ErrDependencyNotFound)
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

// InvocationBranch names the branch the run is submitted from — the
// default dependency.fallback_branch: a dependent run released past a dead
// dependency continues from where the operator stood when submitting the
// chain. Same env hygiene as ResolveRepoPath. A detached HEAD names no
// branch and is refused: "--abbrev-ref" prints the literal "HEAD" there,
// and falling back to a raw commit reference the operator did not name
// would start work from a moving target's idea of a branch — setting
// fallback_branch explicitly is the fix the error names.
func InvocationBranch(repoPath string) (string, error) {
	cmd := exec.Command("git", "-C", repoPath, "rev-parse", "--abbrev-ref", "HEAD")
	cmd.Env = EnvWithoutGitRepoOverrides(os.Environ())
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

// VerifyBranchExists refuses a named branch that does not exist as a
// branch in the run's repo — dependency.fallback_branch is verified at
// submit so a typo fails the run before the dependency gate instead of
// after it, with git's raw "invalid reference" at release time. The
// full refname (refs/heads/<branch>) pins the check to branches: a tag or
// a SHA would resolve --verify alone and is not a worktree base this
// feature promises. Same env hygiene as ResolveRepoPath.
func VerifyBranchExists(repoPath, branch string) error {
	cmd := exec.Command("git", "-C", repoPath, "rev-parse", "--verify", "--quiet", "refs/heads/"+branch)
	cmd.Env = EnvWithoutGitRepoOverrides(os.Environ())
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("dependency.fallback_branch %q: no such branch in %s (git: %s) — create it or fix the config before chaining behind a dependency",
			branch, repoPath, strings.TrimSpace(string(out)))
	}
	return nil
}

// warnFolderOverlap writes a best-effort warning when this run's folder
// grants overlap a currently running pipeline's on the same task queue:
// concurrent append-one-row edits to a shared folder (a done-index) are
// hand-fixable, so this is an alert naming the shared folder and the other
// run — not an error, since refusing the run would turn a bookkeeping
// convenience into a scheduling constraint. Any failure enumerating (an
// unreachable visibility store, an unreadable history) degrades to silence,
// never a failure. The workflow has not started yet, so the run's own grants
// cannot match themselves. warn == nil discards.
func warnFolderOverlap(c client.Client, cfg config.Config, folders []string, warn io.Writer) {
	if len(folders) == 0 || warn == nil {
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
								fmt.Fprintf(warn, "warning: folder %s is also granted to running pipeline %s — writes to it are not coordinated across runs\n",
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

// EnvWithoutGitRepoOverrides strips the environment variables that override
// git's own repository discovery. An exported GIT_DIR would have rev-parse
// validate an unrelated repository and ship a non-repo path to the worker,
// and GIT_CEILING_DIRECTORIES could reject a valid one; the check must
// judge the directory on disk, not the invoking shell's git state.
func EnvWithoutGitRepoOverrides(environ []string) []string {
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
