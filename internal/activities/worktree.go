package activities

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/log"

	"github.com/ozzono/daedalus/internal/config"
)

// WorktreeInput identifies the repository and the per-issue worktree to operate on.
type WorktreeInput struct {
	RepoPath   string
	TaskQueue  string
	IssueID    string
	BranchName string
	// BranchPrefix names the preserved branch for this run (config
	// branch_prefix, overridable per run via `daedalus run --prefix`).
	// Empty means the default — workflow inputs recorded before the
	// setting existed replay with an empty value.
	BranchPrefix string
	// BaseBranch, when set, is the ref the new worktree's branch starts
	// from instead of HEAD — the aborted/<issue> branch a continued run
	// resumes (see ContinueWorktreeFrom / `daedalus continue`).
	BaseBranch string
	// Flow, when set, scopes every per-issue name this input derives —
	// the worktree path, the aborted/ branch, the stale-branch sweep —
	// to the flow, so a concurrent run of another flow on the same issue
	// can neither collide with nor clean up this run's live state. Empty
	// (feature-dev and pre-field runs) keeps the legacy unscoped names.
	Flow string
	// Authorship, when true, commits daedalus's own work (the approved
	// deliverable and the aborted-work snapshot) as author/committer
	// "daedalus <daedalus@local>"; false leaves the commits to the
	// worker's git config.
	Authorship bool
}

// WorktreeOutput reports the location of a created worktree.
type WorktreeOutput struct {
	WorktreePath string
}

// commitAuthorArgs prefixes a git invocation with the forced "daedalus
// <daedalus@local>" identity when authorship is enabled; empty (the
// default) lets the commit use the worker's git config as-is. The args
// must precede the subcommand, so they come first.
func commitAuthorArgs(authorship bool) []string {
	if !authorship {
		return nil
	}
	return []string{"-c", "user.name=daedalus", "-c", "user.email=daedalus@local"}
}

// Branch name prefixes: InFlightBranchPrefix marks in-flight runs (always
// cleaned up, along with any stale in-flight branches from crashed runs);
// the preserved prefix (default daedalus/, configurable via branch_prefix /
// --prefix) marks approved work committed by FinalizeWorktreeActivity,
// which is kept. aborted/ carries a run that closed without approval —
// committed and kept so `daedalus continue` can resume it; each abort
// replaces the previous snapshot.
const (
	// InFlightBranchPrefix is the in-flight branch namespace. Exported so
	// the CLI's wipe command globs the run's in-flight branches by the same
	// rule the activities create and sweep them by.
	InFlightBranchPrefix   = "feat/"
	defaultPreservedPrefix = "daedalus"
	abortedPrefix          = "aborted/"
)

// preservedPrefix returns the validated prefix naming this run's preserved
// branch: the run's BranchPrefix when set, the historical default otherwise.
// Workflow inputs are a trust boundary (see WorktreePathFor), so the prefix is
// validated here too: an unvalidated one could aim the finalized-check glob at
// a reserved namespace and silently suppress the aborted-work snapshot.
func (in WorktreeInput) preservedPrefix() (string, error) {
	if in.BranchPrefix == "" {
		return defaultPreservedPrefix, nil
	}
	if err := config.ValidateBranchPrefix(in.BranchPrefix); err != nil {
		return "", err
	}
	return in.BranchPrefix, nil
}

// AbortedBranchName returns the per-issue branch carrying an aborted run's
// preserved work — the base ref a continued run starts from. The legacy
// unscoped name; see AbortedBranchNameFor for the flow-scoped form.
func AbortedBranchName(issueID string) (string, error) {
	return AbortedBranchNameFor(issueID, "")
}

// AbortedBranchNameFor returns the aborted-run branch for an issue, scoped
// to the flow when one is given (see WorktreeInput.Flow).
func AbortedBranchNameFor(issueID, flow string) (string, error) {
	if err := validatePathSegment(issueID); err != nil {
		return "", fmt.Errorf("issue id: %w", err)
	}
	return abortedPrefix + pathSegment(flow, issueID), nil
}

// pathSegment is the per-issue name component: "issue-<id>", prefixed with
// "<flow>-" when a flow scope is given. flow must already be a valid path
// segment or empty.
func pathSegment(flow, issueID string) string {
	if flow == "" {
		return "issue-" + issueID
	}
	return flow + "-issue-" + issueID
}

// WorktreePathFor returns the path for a given issue's worktree, scoped by
// task queue so pipelines from different projects or flows sharing one
// worker never collide on issue IDs: ~/.daedalus/worktrees/<taskQueue>/issue-<IssueID>.
// The legacy unscoped form; WorktreeInput.worktreePath is the flow-aware one.
// Both segments are validated — they become filesystem path components.
func WorktreePathFor(taskQueue, issueID string) (string, error) {
	return worktreePathFor(taskQueue, issueID, "")
}

// WorktreePathForFlow is WorktreePathFor with a flow scope — the form every
// flow-scoped run actually uses. Exported for the CLI's wipe command, which
// must address the same derived path the activities created.
func WorktreePathForFlow(taskQueue, issueID, flow string) (string, error) {
	return worktreePathFor(taskQueue, issueID, flow)
}

// IssuePathSegment is pathSegment's exported form: the per-issue name
// component ("issue-<id>", flow-scoped) that keys the worktree directory
// and every per-issue branch name — in-flight, preserved, and aborted
// alike. Exported for the CLI's wipe command, which globs the run's
// branches in the target repo by the same naming rule instead of
// duplicating it.
func IssuePathSegment(flow, issueID string) string {
	return pathSegment(flow, issueID)
}

// worktreePathFor is WorktreePathFor with an optional flow scope segment.
func worktreePathFor(taskQueue, issueID, flow string) (string, error) {
	root, err := worktreeRootFor(taskQueue)
	if err != nil {
		return "", err
	}
	if err := validatePathSegment(issueID); err != nil {
		return "", fmt.Errorf("issue id: %w", err)
	}
	if flow != "" {
		if err := validatePathSegment(flow); err != nil {
			return "", fmt.Errorf("flow: %w", err)
		}
	}
	return filepath.Join(root, pathSegment(flow, issueID)), nil
}

// worktreePath resolves this input's worktree path, flow-scoped when the
// run carries a flow.
func (in WorktreeInput) worktreePath() (string, error) {
	return worktreePathFor(in.TaskQueue, in.IssueID, in.Flow)
}

// worktreeRootFor returns this task queue's worktree root,
// ~/.daedalus/worktrees/<taskQueue>.
func worktreeRootFor(taskQueue string) (string, error) {
	if err := validatePathSegment(taskQueue); err != nil {
		return "", fmt.Errorf("task queue: %w", err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".daedalus", "worktrees", taskQueue), nil
}

// PreflightWorktreeRoot verifies up front that this process can create and
// write worktrees under the task queue's root. A worker without that access
// still starts and polls — then fails every pipeline it picks up inside
// CreateWorktreeActivity with an opaque git error, so the check belongs at
// worker startup, not per activity.
func PreflightWorktreeRoot(taskQueue string) error {
	root, err := worktreeRootFor(taskQueue)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return fmt.Errorf("create worktree root %s: %w", root, err)
	}
	probe := filepath.Join(root, ".write-probe")
	if err := os.WriteFile(probe, nil, 0o644); err != nil {
		return fmt.Errorf("worktree root %s is not writable: %w", root, err)
	}
	return os.Remove(probe)
}

// validatePathSegment rejects values that would escape the worktree root
// when joined into a path.
func validatePathSegment(s string) error {
	if s == "" || s == "." || s == ".." || strings.ContainsAny(s, `/\`) {
		return fmt.Errorf("%q is not a valid path segment", s)
	}
	return nil
}

// runGit runs a git command and returns its combined output, wrapping any
// failure with the command line and output for diagnostics.
func runGit(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	setProcessGroup(cmd)
	defer killGroup(cmd)
	out, err := cmd.CombinedOutput()
	if err != nil && !isWaitDelay(err) {
		return string(out), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, out)
	}
	return string(out), nil
}

// cleanWorktree removes all traces of a worktree from the repository: the
// worktree itself (registered or not), its git admin metadata, the run's
// in-flight branch, stale in-flight branches from crashed runs of the same
// issue, and any leftover directory. It is tolerant by design — remove and
// branch failures such as "not a working tree" or "branch not found" are
// ignored, because a partially-failed previous run must not poison the next
// one — while prune failures and an undeletable directory are surfaced as
// errors. Preserved branches are never touched. Each step is logged with
// its duration (with a logger the caller supplies), so a cleanup that
// burns its whole StartToClose budget leaves a record of where the time
// went; before anything else, stragglers left in the worktree by a killed
// agent round are swept, so they cannot churn the tree the steps below
// snapshot and delete.
func cleanWorktree(ctx context.Context, input WorktreeInput, worktreePath string, logger log.Logger) error {
	killWorktreeStragglers(logger, worktreePath)
	step := func(name string, fn func() error) error {
		start := time.Now()
		err := fn()
		logger.Info("Worktree cleanup step", "Step", name, "Duration", time.Since(start).Round(time.Millisecond), "Error", err)
		return err
	}
	if err := step("preserve aborted work", func() error {
		return preserveAbortedWork(ctx, input, worktreePath)
	}); err != nil {
		return err
	}

	// Best-effort: fall through to prune and the RemoveAll fallback below.
	step("git worktree remove", func() error {
		_, err := runGit(ctx, "-C", input.RepoPath, "worktree", "remove", worktreePath, "--force")
		return err
	})

	if out, err := runGit(ctx, "-C", input.RepoPath, "worktree", "prune"); err != nil {
		return fmt.Errorf("git worktree prune: %w: %s", err, out)
	}

	deleteStaleBranches(ctx, input.RepoPath, input.IssueID, input.Flow)

	if _, err := os.Stat(worktreePath); err == nil {
		if err := step("remove leftover directory", func() error {
			return os.RemoveAll(worktreePath)
		}); err != nil {
			return fmt.Errorf("remove stale worktree %s: %w", worktreePath, err)
		}
	}

	// Session tracking leaves one record file per worktree under
	// ~/.daedalus/sessions; the worktree is going away, so its record goes
	// too. Best-effort litter control — a missed file only holds a record
	// whose run id no future run can match.
	if path, err := sessionStatePath(worktreePath); err == nil {
		step("remove session record", func() error {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			return nil
		})
	}
	return nil
}

// stragglerTimeout bounds each command of a straggler sweep.
const stragglerTimeout = 10 * time.Second

// killWorktreeStragglers SIGKILLs processes still referencing a worktree
// path, best-effort and logged. A killed agent round leaves descendants
// behind — test runners that escaped the jail's process group can keep
// respawning, racing the cleanup's git snapshot and directory removal
// (observed as a cleanup burning its whole timeout without finishing).
// The match is an exact string compare against ps's argument text — not a
// pkill pattern, whose regex wildcards and lack of anchoring made one
// issue's path match sibling issues' longer paths (issue-4 vs issue-42)
// and any operator process carrying the path — gated by a boundary check
// (referencesPath) so only the worktree itself and paths inside it match.
// It runs before any of cleanup's own git commands, so those cannot be
// caught either.
func killWorktreeStragglers(logger log.Logger, worktreePath string) {
	lines, pids := stragglersIn(logger, worktreePath)
	if len(pids) == 0 {
		return
	}
	logger.Warn("Killing worktree stragglers", "Worktree", worktreePath, "Processes", strings.Join(lines, "\n"))
	for _, pid := range pids {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

// stragglersIn lists the processes whose command line references path,
// best-effort: their ps lines and pids. An enumeration failure is logged
// rather than swallowed — "could not look" must not read as "none left".
func stragglersIn(logger log.Logger, path string) (lines []string, pids []int) {
	ctx, cancel := context.WithTimeout(context.Background(), stragglerTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ps", "-axo", "pid=,command=").Output()
	if err != nil {
		logger.Warn("Worktree straggler sweep could not enumerate processes", "Error", err)
		return nil, nil
	}
	for line := range strings.SplitSeq(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil || pid <= 0 || pid == os.Getpid() {
			continue
		}
		// ps printed "pid command…"; recover the command text verbatim.
		argv := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), fields[0]))
		if referencesPath(argv, path) {
			lines = append(lines, strings.TrimSpace(line))
			pids = append(pids, pid)
		}
	}
	return lines, pids
}

// referencesPath reports whether argv mentions path at a path boundary:
// the match must end argv or be followed by whitespace, a quote, or a
// slash (a file inside the worktree). A bare substring match would let a
// per-issue path claim its longer siblings — issue-4 matching issue-42 —
// and any process merely quoting the path as part of a longer word.
func referencesPath(argv, path string) bool {
	for i := 0; i+len(path) <= len(argv); i++ {
		if argv[i:i+len(path)] != path {
			continue
		}
		switch end := i + len(path); {
		case end == len(argv):
			return true
		case argv[end] == ' ', argv[end] == '\t', argv[end] == '"', argv[end] == '\'':
			return true
		case argv[end] == '/':
			return true
		}
	}
	return false
}

// preserveAbortedWork keeps a run that closed without approval resumable:
// the worktree's contents are committed and the in-flight branch renamed to
// the per-issue aborted/ branch that `daedalus continue` builds on. A
// finalized run (its preserved deliverable exists) deletes the in-flight
// branch instead — the deliverable already carries the work. Deletion must
// never outrun preservation: a staging or commit failure here aborts the
// cleanup (a "nothing to commit" tree is fine — there is no work to lose),
// because proceeding would remove a worktree whose contents were never
// saved. Stale state from a crashed run is preserved too (the worktree may
// hold its work).
func preserveAbortedWork(ctx context.Context, input WorktreeInput, worktreePath string) error {
	if input.BranchName == "" {
		return nil
	}
	prefix, err := input.preservedPrefix()
	if err != nil {
		return fmt.Errorf("branch prefix: %w", err)
	}
	preserved, _ := runGit(ctx, "-C", input.RepoPath, "branch", "--list",
		prefix+"/"+pathSegment(input.Flow, input.IssueID)+"-*", "--format=%(refname:short)")
	if strings.TrimSpace(preserved) != "" {
		_, _ = runGit(ctx, "-C", input.RepoPath, "branch", "-D", input.BranchName)
		return nil
	}
	if _, err := os.Stat(worktreePath); err != nil {
		// No worktree — nothing to preserve; the stale-branch sweep below
		// still runs.
		return nil
	}
	aborted, err := AbortedBranchNameFor(input.IssueID, input.Flow)
	if err != nil {
		return err
	}
	if _, err := runGit(ctx, "-C", worktreePath, "add", "-A"); err != nil {
		// A stale index.lock — left by the git of a killed round, the very
		// state cleanup runs after — holds no work and would wedge every
		// later cleanup of this issue. Sweep it and retry once; anything
		// still failing is a genuine stage failure.
		if removed := removeStaleIndexLock(ctx, worktreePath); removed {
			_, err = runGit(ctx, "-C", worktreePath, "add", "-A")
		}
		if err != nil {
			return fmt.Errorf("preserve aborted work (stage): %w", err)
		}
	}
	if out, err := runGit(ctx, append(commitAuthorArgs(input.Authorship),
		"-C", worktreePath, "commit",
		"-m", "daedalus: run closed without approval, work preserved for continue")...); err != nil {
		// A clean tree reports "nothing to commit" — nothing to lose. Any
		// other failure means the work was not saved; abort before any
		// deletion step can run.
		if !strings.Contains(out, "nothing to commit") {
			return fmt.Errorf("preserve aborted work (commit): %w: %s", err, out)
		}
	}
	_, _ = runGit(ctx, "-C", input.RepoPath, "branch", "-D", aborted)
	if _, err := runGit(ctx, "-C", worktreePath, "branch", "-m", aborted); err != nil {
		// The rename can fail with nothing at stake — detached HEAD, or
		// the best-effort delete above left the old aborted ref in place
		// ("already exists"). The commit is what holds the work: point the
		// aborted ref straight at it instead. Only a failure of that too
		// (and of the rename) means the snapshot is not saved.
		if _, ferr := runGit(ctx, "-C", worktreePath, "branch", "-f", aborted, "HEAD"); ferr != nil {
			return fmt.Errorf("preserve aborted work (rename to %s): %w: %w", aborted, err, ferr)
		}
	}
	return nil
}

// removeStaleIndexLock deletes the worktree's index.lock when present,
// reporting whether it did. The lock is resolved through git itself
// (`rev-parse --git-path`) because a linked worktree keeps its state under
// the main repo's .git/worktrees/, not under the worktree. Safe in the
// cleanup context — the round's processes are gone by now — and the
// retry that follows immediately fails loudly if the lock was live after
// all.
func removeStaleIndexLock(ctx context.Context, worktreePath string) bool {
	out, err := runGit(ctx, "-C", worktreePath, "rev-parse", "--git-path", "index.lock")
	if err != nil {
		return false
	}
	lock := strings.TrimSpace(out)
	if lock == "" {
		return false
	}
	if !filepath.IsAbs(lock) {
		lock = filepath.Join(worktreePath, lock)
	}
	if _, err := os.Stat(lock); err != nil {
		return false
	}
	return os.Remove(lock) == nil
}

// deleteStaleBranches sweeps leftover in-flight (feat/) branches of this
// issue — scoped to the flow when one is given — from runs that crashed
// before cleanup. Best-effort: on a listing failure, cleanup still proceeds.
func deleteStaleBranches(ctx context.Context, repoPath, issueID, flow string) {
	if issueID == "" {
		return
	}
	pattern := InFlightBranchPrefix + pathSegment(flow, issueID) + "-*"
	out, err := runGit(ctx, "-C", repoPath, "branch", "--list", pattern, "--format=%(refname:short)")
	if err != nil {
		return
	}
	for name := range strings.FieldsSeq(out) {
		_, _ = runGit(ctx, "-C", repoPath, "branch", "-D", name)
	}
}

// CreateWorktreeActivity creates a fresh git worktree for the issue on a new
// branch, healing any state left behind by a failed previous run first.
func CreateWorktreeActivity(ctx context.Context, input WorktreeInput) (WorktreeOutput, error) {
	worktreePath, err := input.worktreePath()
	if err != nil {
		return WorktreeOutput{}, err
	}

	if err := cleanWorktree(ctx, input, worktreePath, activityLogger(ctx)); err != nil {
		return WorktreeOutput{}, fmt.Errorf("clean stale worktree state: %w", err)
	}

	addArgs := []string{"-C", input.RepoPath, "worktree", "add", worktreePath, "-b", input.BranchName}
	if input.BaseBranch != "" {
		// A continued run resumes the aborted attempt's branch.
		addArgs = append(addArgs, input.BaseBranch)
	}
	if out, err := runGit(ctx, addArgs...); err != nil {
		return WorktreeOutput{}, fmt.Errorf("git worktree add %s: %w: %s", worktreePath, err, out)
	}
	if strings.HasPrefix(input.BaseBranch, abortedPrefix) {
		// The aborted snapshot served its purpose: its commits now live on
		// this run's in-flight branch.
		_, _ = runGit(ctx, "-C", input.RepoPath, "branch", "-D", input.BaseBranch)
	}

	return WorktreeOutput{WorktreePath: worktreePath}, nil
}

// FinalizeWorktreeActivity commits the approved work and renames the run's
// branch from the in-flight feat/ prefix to the run's preserved prefix
// (branch_prefix / --prefix), so the deliverable survives cleanup. The
// workflow returns the preserved branch name to the caller. A run whose
// approved change produced no diff fails here — an approval of nothing is
// an anomaly, not a deliverable.
func FinalizeWorktreeActivity(ctx context.Context, input WorktreeInput) (string, error) {
	worktreePath, err := input.worktreePath()
	if err != nil {
		return "", err
	}
	if _, err := runGit(ctx, "-C", worktreePath, "add", "-A"); err != nil {
		return "", fmt.Errorf("stage approved work: %w", err)
	}
	msg := fmt.Sprintf("daedalus: issue %s", input.IssueID)
	if _, err := runGit(ctx, append(commitAuthorArgs(input.Authorship),
		"-C", worktreePath, "commit", "-m", msg)...); err != nil {
		return "", fmt.Errorf("commit approved work: %w", err)
	}
	if !strings.HasPrefix(input.BranchName, InFlightBranchPrefix) {
		return "", fmt.Errorf("unexpected branch name %q: expected %s prefix", input.BranchName, InFlightBranchPrefix)
	}
	prefix, err := input.preservedPrefix()
	if err != nil {
		return "", fmt.Errorf("branch prefix: %w", err)
	}
	preserved := prefix + "/" + strings.TrimPrefix(input.BranchName, InFlightBranchPrefix)
	if _, err := runGit(ctx, "-C", worktreePath, "branch", "-m", preserved); err != nil {
		return "", fmt.Errorf("rename branch to %s: %w", preserved, err)
	}
	return preserved, nil
}

// CleanupWorktreeActivity removes the issue's worktree, its admin metadata,
// and the run's in-flight branch, leaving no dangling directories or git
// refs behind. Preserved branches are deliberately kept — they are the run's
// deliverable.
func CleanupWorktreeActivity(ctx context.Context, input WorktreeInput) error {
	// Cleanup can run long on a big tree (a full worktree removal is a lot
	// of filesystem work); heartbeats keep Temporal's history showing it
	// as alive rather than indistinguishable from a hang.
	hbDone := make(chan struct{})
	defer close(hbDone)
	go heartbeatLoop(ctx, hbDone)
	worktreePath, err := input.worktreePath()
	if err != nil {
		return err
	}
	return cleanWorktree(ctx, input, worktreePath, activityLogger(ctx))
}
