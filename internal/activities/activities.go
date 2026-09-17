// Package activities implements the Temporal activities that make up a
// Daedalus pipeline: worktree lifecycle, jailed agent and reviewer runs, and
// tests.
package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/template"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
)

// Provider settings (API keys, base URLs, models) reach the jailed agent
// through the worker's environment, exported from config.yaml at startup
// when set there — never through workflow history or activity inputs (both
// would persist them in Temporal events). Nothing here requires them: an
// unset setting simply falls back to whatever the worker inherited.

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
}

// WorktreeOutput reports the location of a created worktree.
type WorktreeOutput struct {
	WorktreePath string
}

// AgentRunInput describes a single jailed agent invocation.
type AgentRunInput struct {
	WorktreePath string
	Prompt       string
	// Agent, set by `run -cli/--cli`, overrides the worker's agent for this
	// run; empty falls back to DAEDALUS_AGENT (see jailedAgentCLI).
	Agent string
	// SessionID, when set, resumes the agent's previous conversation for
	// this worktree (claude -p --resume) instead of starting cold — the
	// round inherits the prior context and its warm provider prompt cache.
	// Empty starts a fresh session; agents without resume support (opencode,
	// amp) ignore it.
	SessionID string
}

// maxAgentOutput bounds the agent text and chain-of-thought each carried in
// the activity result (see maxTestLogs for why results stay small). The
// worker log still records the full untruncated stream.
const maxAgentOutput = 16 * 1024

// AgentRunResult carries the agent's visible text and chain of thought into
// Temporal history, tail-bounded, so the UI and `temporal workflow show`
// expose both per round without reading worker logs.
type AgentRunResult struct {
	Text     string
	Thinking string
	// SessionID identifies the agent conversation the round ran in, so the
	// next round can resume it (see AgentRunInput.SessionID). Empty when the
	// agent does not report one.
	SessionID string
}

// TestResult reports the outcome of a native test run. A failing suite is
// reported via Passed=false (not a system error) so the workflow can feed the
// logs back to the agent. Logs are tail-truncated (see maxTestLogs) because
// activity results are serialized into Temporal history. Command records the
// entrypoint that ran — declared, detected, or AI-discovered — for
// visibility in history.
type TestResult struct {
	Passed  bool
	Logs    string
	Command string
}

// ReviewInput describes a review request for the current state of the
// worktree. Focus says what is being reviewed (e.g. "the implementation",
// "the test suite"); TestLogs optionally carries the latest test output for
// the reviewer to consider.
type ReviewInput struct {
	WorktreePath string
	Focus        string
	TestLogs     string
	// TestsInScope tells the reviewer prompt whether tests are part of
	// this review: false in phase 1 (code review — coverage is a later
	// phase's concern), true in phase 2 (the test-suite review).
	TestsInScope bool
	// Agent, set by `run -cli/--cli`, overrides the worker's agent for
	// this run; empty falls back to DAEDALUS_AGENT (see jailedAgentCLI).
	Agent string
	// SessionID, when set, resumes the reviewer's previous conversation
	// (claude -p --resume) so a re-review verifies its earlier findings
	// with the prior context and warm prompt cache instead of starting
	// cold. Each reviewer role (code review, test review) chains its own
	// session; empty starts a fresh one. Agents without resume support
	// (opencode, amp) ignore it.
	SessionID string
}

// ReviewResult is a reviewer verdict. Comments holds everything the reviewer
// wrote above its verdict line, to be fed back to the implementing agent.
// NeedsMaintainer marks the third verdict, NEEDS_MAINTAINER: the task as
// stated cannot be completed by editing files in this worktree, so the
// workflow parks the run for a maintainer restart instead of requesting
// changes the agent can never satisfy.
type ReviewResult struct {
	Approved        bool
	NeedsMaintainer bool
	Comments        string
	// SessionID identifies the reviewer conversation the round ran in, so
	// the next round of the same review role can resume it (see
	// ReviewInput.SessionID). Empty when the agent reports none.
	SessionID string
}

// Branch name prefixes: feat/ marks in-flight runs (always cleaned up, along
// with any stale feat/ branches from crashed runs); the preserved prefix
// (default daedalus/, configurable via branch_prefix / --prefix) marks
// approved work committed by FinalizeWorktreeActivity, which is kept.
// aborted/ carries a run that closed without approval — committed and kept
// so `daedalus continue` can resume it; each abort replaces the previous
// snapshot.
const (
	inFlightPrefix         = "feat/"
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
// preserved work — the base ref a continued run starts from.
func AbortedBranchName(issueID string) (string, error) {
	if err := validatePathSegment(issueID); err != nil {
		return "", fmt.Errorf("issue id: %w", err)
	}
	return abortedPrefix + "issue-" + issueID, nil
}

// maxTestLogs bounds the test output carried in the activity result (and
// hence Temporal history). The tail is kept — it usually holds the failures.
const maxTestLogs = 16 * 1024

// WorktreePathFor returns the path for a given issue's worktree, scoped by
// task queue so pipelines from different projects or flows sharing one
// worker never collide: ~/.daedalus/worktrees/<taskQueue>/issue-<IssueID>.
// Both segments are validated — they become filesystem path components.
func WorktreePathFor(taskQueue, issueID string) (string, error) {
	root, err := worktreeRootFor(taskQueue)
	if err != nil {
		return "", err
	}
	if err := validatePathSegment(issueID); err != nil {
		return "", fmt.Errorf("issue id: %w", err)
	}
	return filepath.Join(root, "issue-"+issueID), nil
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

// pipeDrainDelay bounds how long Cmd.Wait keeps waiting once the command has
// exited or been killed: a descendant that escaped the process group (setsid)
// can inherit and hold the output pipes, and without this deadline the copy
// goroutines block on it forever — leaking the whole activity goroutine past
// every Temporal timeout.
const pipeDrainDelay = 5 * time.Second

// setProcessGroup puts the subprocess in its own process group and arranges
// for the whole group to be SIGKILLed on context cancellation, so a timeout
// cannot orphan the subprocess's children; pipeDrainDelay keeps Wait from
// hanging on a descendant that slipped out of the group with the pipes.
// Callers pair it with killGroup, which sweeps the group when the round
// finishes (see killGroup for why cancellation alone is not enough).
func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	cmd.WaitDelay = pipeDrainDelay
}

// killGroup SIGKILLs the command's whole process group and is deferred at
// every spawn site so the round leaves no descendants behind, however the
// child exited. Cancellation-only killing is not enough: a child that dies
// or exits on its own — a crashed jail, a test runner that backgrounded
// workers and quit — leaves its children running in the group, orphaned.
// After the direct child is gone the sweep is harmless: the group is empty
// (ESRCH) or holds only descendants, and the pid-recycling window between
// Wait returning and this kill is nanoseconds.
func killGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}

// isWaitDelay reports whether err is exec.ErrWaitDelay, which Wait returns
// only when the child itself exited successfully but a leftover descendant
// was still holding the output pipes when WaitDelay expired. That is a
// success for the round — the collected output stands and killGroup sweeps
// the descendant — not a failure.
func isWaitDelay(err error) bool {
	return errors.Is(err, exec.ErrWaitDelay)
}

// cleanWorktree removes all traces of a worktree from the repository: the
// worktree itself (registered or not), its git admin metadata, the run's
// in-flight branch, stale in-flight branches from crashed runs of the same
// issue, and any leftover directory. It is tolerant by design — remove and
// branch failures such as "not a working tree" or "branch not found" are
// ignored, because a partially-failed previous run must not poison the next
// one — while prune failures and an undeletable directory are surfaced as
// errors. Preserved branches are never touched.
func cleanWorktree(ctx context.Context, input WorktreeInput, worktreePath string) error {
	if err := preserveAbortedWork(ctx, input, worktreePath); err != nil {
		return err
	}

	// Best-effort: fall through to prune and the RemoveAll fallback below.
	_, _ = runGit(ctx, "-C", input.RepoPath, "worktree", "remove", worktreePath, "--force")

	if out, err := runGit(ctx, "-C", input.RepoPath, "worktree", "prune"); err != nil {
		return fmt.Errorf("git worktree prune: %w: %s", err, out)
	}

	deleteStaleBranches(ctx, input.RepoPath, input.IssueID)

	if _, err := os.Stat(worktreePath); err == nil {
		if err := os.RemoveAll(worktreePath); err != nil {
			return fmt.Errorf("remove stale worktree %s: %w", worktreePath, err)
		}
	}
	return nil
}

// preserveAbortedWork keeps a run that closed without approval resumable:
// the worktree's contents are committed and the in-flight branch renamed to
// the per-issue aborted/ branch that `daedalus continue` builds on. A
// finalized run (its preserved deliverable exists) deletes the in-flight
// branch instead — the deliverable already carries the work. Best-effort
// throughout: any git hiccup still lets cleanup proceed, and stale state
// from a crashed run is preserved too (the worktree may hold its work).
func preserveAbortedWork(ctx context.Context, input WorktreeInput, worktreePath string) error {
	if input.BranchName == "" {
		return nil
	}
	prefix, err := input.preservedPrefix()
	if err != nil {
		return fmt.Errorf("branch prefix: %w", err)
	}
	preserved, _ := runGit(ctx, "-C", input.RepoPath, "branch", "--list",
		prefix+"/issue-"+input.IssueID+"-*", "--format=%(refname:short)")
	if strings.TrimSpace(preserved) != "" {
		_, _ = runGit(ctx, "-C", input.RepoPath, "branch", "-D", input.BranchName)
		return nil
	}
	if _, err := os.Stat(worktreePath); err != nil {
		// No worktree — nothing to preserve; the stale-branch sweep below
		// still runs.
		return nil
	}
	aborted, err := AbortedBranchName(input.IssueID)
	if err != nil {
		return err
	}
	_, _ = runGit(ctx, "-C", worktreePath, "add", "-A")
	_, _ = runGit(ctx, "-C", worktreePath,
		"-c", "user.name=daedalus", "-c", "user.email=daedalus@local",
		"commit", "-m", "daedalus: run closed without approval, work preserved for continue")
	_, _ = runGit(ctx, "-C", input.RepoPath, "branch", "-D", aborted)
	_, _ = runGit(ctx, "-C", worktreePath, "branch", "-m", aborted)
	return nil
}

// deleteStaleBranches sweeps leftover in-flight (feat/) branches of this
// issue from runs that crashed before cleanup. Best-effort: on a listing
// failure, cleanup still proceeds.
func deleteStaleBranches(ctx context.Context, repoPath, issueID string) {
	if issueID == "" {
		return
	}
	pattern := inFlightPrefix + "issue-" + issueID + "-*"
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
	worktreePath, err := WorktreePathFor(input.TaskQueue, input.IssueID)
	if err != nil {
		return WorktreeOutput{}, err
	}

	if err := cleanWorktree(ctx, input, worktreePath); err != nil {
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

// jailResult captures the jailed process's output streams separately: the
// verdict parser must see stdout only, so trailing stderr noise cannot flip
// a verdict, while stderr stays available for error reporting.
type jailResult struct {
	Stdout string
	Stderr string
}

// jailedAgentCLI returns the jailed agent CLI, its headless flags, and the
// output-mode flags for full agent rounds. The selection (config.yaml
// `agent`, default claude) travels via the worker's environment like the
// provider settings: DAEDALUS_AGENT, exported at worker startup. A run
// started with `run -cli/--cli` overrides it per run — agent wins over the
// environment here; empty means no override. All three CLIs read the prompt
// from piped stdin; each headless flag set approves every tool call, which
// is only safe inside the jail.
func jailedAgentCLI(agent string) (selected string, headless, output []string) {
	if agent == "" {
		agent = os.Getenv("DAEDALUS_AGENT")
	}
	switch agent {
	case "opencode":
		// opencode reads the prompt from piped stdin just like claude -p;
		// --auto approves everything not explicitly denied. Plain text
		// mode, so Thinking stays empty; capturing it means parsing
		// opencode's --format json event stream.
		return "opencode", []string{"run", "--auto"}, nil
	case "amp":
		// -x is amp's execute mode (single-shot, prompt from stdin);
		// --dangerously-allow-all approves all tool calls. It is absent
		// from amp's --help (only the settings key amp.dangerouslyAllowAll
		// is documented), so its disappearance across updates is the first
		// thing to check if amp rounds start failing on unknown flags.
		// --stream-json-thinking implies --stream-json and adds thinking
		// blocks, which parseAgentStream reads like claude's.
		return "amp", []string{"-x", "--dangerously-allow-all"}, []string{"--stream-json-thinking"}
	default:
		return "claude", []string{"-p", "--dangerously-skip-permissions"}, []string{"--output-format", "json"}
	}
}

// agentConcurrency reads the worker-level cap on concurrent jailed-agent
// rounds, exported at worker startup from config max_concurrent_agent_runs
// (DAEDALUS_MAX_CONCURRENT_AGENT_RUNS). Unset, malformed, or non-positive
// values fall back to the config default.
func agentConcurrency() int {
	if v := os.Getenv("DAEDALUS_MAX_CONCURRENT_AGENT_RUNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return config.DefaultMaxConcurrentAgentRuns
}

// agentLimiter is the process-wide semaphore bounding concurrent jailed
// rounds (agent and reviewer alike — both hit the provider). All of a
// worker's workflows share it: concurrent cold agent sessions compete for
// one provider account, so queuing extras until a slot frees runs every
// round faster than running them all at once.
var (
	limiterOnce sync.Once
	limiter     chan struct{}
)

func agentLimiter() chan struct{} {
	limiterOnce.Do(func() {
		limiter = make(chan struct{}, agentConcurrency())
	})
	return limiter
}

// heartbeatInterval is how often a jailed round heartbeats while it waits
// for a semaphore slot or runs. Heartbeats make Temporal's history show a
// long round as alive (without them, a round thinking for forty minutes is
// indistinguishable from a hung one in the UI) and would feed a future
// HeartbeatTimeout; the interval is far below any plausible timeout.
const heartbeatInterval = 30 * time.Second

// slotWaitTimeout bounds how long a jailed round queues for a concurrency
// slot before reporting ErrAgentSlotsBusy. It sits well under the narrowest
// round's StartToClose (the reviewer's 15 minutes): while queued the round
// produces nothing, so letting the wait run to the budget's end would
// surface as a plain timeout the workflow would count against its timeout
// streak and answer with a continuation prompt for work that never started.
// A var (not a const) so tests can shorten it.
var slotWaitTimeout = 5 * time.Minute

// heartbeatLoop records activity heartbeats every heartbeatInterval until
// done closes or ctx ends. Outside a real activity context (unit tests
// invoke activities directly) the SDK panics by design; like activityLogger,
// the panic is recovered away.
func heartbeatLoop(ctx context.Context, done <-chan struct{}) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				defer func() { recover() }()
				activity.RecordHeartbeat(ctx)
			}()
		}
	}
}

// runJailed runs one autonomous agent invocation inside an ai-jail sandbox
// rooted at the worktree. agent names the jailed CLI to run — a `run
// -cli/--cli` override, empty for the worker's DAEDALUS_AGENT default.
// agentArgs are appended to the agent's fixed argument set before its
// headless flags. The prompt travels via stdin, not argv: argv is visible
// in `ps` and per-argument size limits would make large review prompts
// (which embed the full diff) fail the run. While waiting for a concurrency
// slot and while the agent runs, the round heartbeats so Temporal history
// shows liveness; the slot wait itself is bounded by slotWaitTimeout so a
// queued round cannot burn its whole StartToClose budget without the agent
// ever launching.
func runJailed(ctx context.Context, agent, worktreePath, prompt string, agentArgs ...string) (jailResult, error) {
	selected, headless, _ := jailedAgentCLI(agent)
	lim := agentLimiter()
	hbDone := make(chan struct{})
	defer close(hbDone)
	go heartbeatLoop(ctx, hbDone)
	wait := time.NewTimer(slotWaitTimeout)
	defer wait.Stop()
	select {
	case lim <- struct{}{}:
		defer func() { <-lim }()
	case <-wait.C:
		return jailResult{}, fmt.Errorf("%w: no slot freed within %s", ErrAgentSlotsBusy, slotWaitTimeout)
	case <-ctx.Done():
		return jailResult{}, fmt.Errorf("agent slot: %w", ctx.Err())
	}
	args := []string{"--worktree", "--network"}
	// amp's host login state (~/.config/amp) is not among ai-jail's
	// agent-state dirs and AMP_API_KEY is not in its default env allowlist,
	// so neither documented auth route reaches the jailed child on a stock
	// jail. --env copies the value from this process's environment (set
	// below), making the key route work without operator jail config.
	if selected == "amp" && os.Getenv("AMP_API_KEY") != "" {
		args = append(args, "--env", "AMP_API_KEY")
	}
	// "--" ends ai-jail's own flags: everything after it is the jailed
	// command, verbatim — otherwise ai-jail rejects child flags that
	// resemble its own (e.g. claude's --verbose) as misplaced.
	args = append(args, "--", selected)
	args = append(args, agentArgs...)
	args = append(args, headless...)
	cmd := exec.CommandContext(ctx, "ai-jail", args...)
	setProcessGroup(cmd)
	defer killGroup(cmd)
	cmd.Dir = worktreePath
	// os.Environ() carries the provider settings the worker exported from
	// config.yaml or inherited; pass them through as-is.
	cmd.Env = os.Environ()
	cmd.Stdin = strings.NewReader(prompt)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	res := jailResult{Stdout: stdout.String(), Stderr: stderr.String()}
	if err != nil && !isWaitDelay(err) {
		if out := res.Stdout + res.Stderr; matchesAny(out, apiExhaustionMarkers) {
			return res, fmt.Errorf("%w: %w: %s", ErrAPIExhausted, err, truncateTail(out, 1024))
		}
		return res, fmt.Errorf("ai-jail agent run: %w: %s", err, res.Stdout+res.Stderr)
	}
	return res, nil
}

// apiExhaustionMarkers are the phrases provider APIs (and their CLIs) emit
// when the account is out of quota, rate-limited, or the service is
// overloaded. Matching them labels the halt so operators can tell "resume
// with `daedalus continue` later" apart from a code failure.
var apiExhaustionMarkers = []string{
	"rate limit", "rate_limit", "quota", "credit balance", "insufficient", "usage limit", "402", "429", "overloaded",
}

// ErrAPIExhausted marks an agent or reviewer run that failed because the
// provider API is out of quota or unavailable. Activities do not retry;
// the workflow heartbeats — sleeping an hour and retrying the same round,
// up to five times — and parks the run for a maintainer restart once the
// API is still exhausted past that. Either way the work is preserved on
// the aborted/ branch and the run is resumable via `daedalus continue`.
var ErrAPIExhausted = errors.New("agent api exhausted or unavailable")

// ErrAgentSlotsBusy marks a jailed round that queued for a concurrency
// slot past slotWaitTimeout and gave up without launching the agent. The
// workflow answers it like the quota heartbeat, not like a timeout: back
// off briefly and re-queue the same round unchanged — a timeout would be
// miscounted against the streak and answered with a continuation prompt
// for partial work that never happened. Its text deliberately avoids every
// apiExhaustionMarker so isAPIExhaustion cannot claim it.
var ErrAgentSlotsBusy = errors.New("all jailed-agent slots busy")

// matchesAny reports whether s contains any marker, case-insensitively.
func matchesAny(s string, markers []string) bool {
	s = strings.ToLower(s)
	return slices.ContainsFunc(markers, func(m string) bool {
		return strings.Contains(s, m)
	})
}

// activityLogger returns the activity-scoped logger, falling back to a
// discard logger outside a real activity context (unit tests invoke
// activities directly; the SDK panics in that case by design).
func activityLogger(ctx context.Context) (l log.Logger) {
	l = log.NewStructuredLogger(slog.New(slog.DiscardHandler))
	defer func() { recover() }()
	if al := activity.GetLogger(ctx); al != nil {
		l = al
	}
	return l
}

// RunJailedClaudeActivity runs the jailed agent (Claude Code, opencode, or
// amp, per config) inside an ai-jail sandbox rooted at the worktree. Claude
// runs with json output so its visible text comes back structured and lands
// tail-bounded in the activity result (serialized into Temporal history,
// visible in the UI per round), while the worker log keeps the full
// untruncated output; amp's --stream-json-thinking emits
// Claude-Code-compatible events including thinking blocks, so the same
// parser applies; opencode's plain output is taken as-is (its thinking is
// not captured). stream-json (which also carries the chain of thought via
// --verbose) is blocked for claude for now: ai-jail's flag guard rejects
// --verbose after the command by prefix match, even behind --.
func RunJailedClaudeActivity(ctx context.Context, input AgentRunInput) (AgentRunResult, error) {
	agent, _, agentArgs := jailedAgentCLI(input.Agent)
	// A session id from a previous round resumes that conversation instead
	// of starting cold — the round inherits the prior context and the
	// provider's warm prompt cache for it. Only claude supports --resume;
	// opencode and amp ignore the field and start fresh.
	if input.SessionID != "" && agent == "claude" {
		agentArgs = append([]string{"--resume", input.SessionID}, agentArgs...)
	}
	start := time.Now()
	res, err := runJailed(ctx, input.Agent, input.WorktreePath, input.Prompt, agentArgs...)
	if err != nil {
		return AgentRunResult{}, err
	}
	thinking, text, session := parseAgentStream(res.Stdout)
	if text == "" {
		// Not json/stream-json (a CLI without the flag, or a parse miss):
		// keep whatever the agent did print rather than an empty result.
		text = res.Stdout
	}
	logger := activityLogger(ctx)
	logger.Info("Agent run completed", "Stdout", res.Stdout, "Duration", time.Since(start).Round(time.Second))
	if res.Stderr != "" {
		logger.Info("Agent run stderr", "Stderr", res.Stderr)
	}
	return AgentRunResult{
		Text:      truncateTail(text, maxAgentOutput),
		Thinking:  truncateTail(thinking, maxAgentOutput),
		SessionID: session,
	}, nil
}

// streamMessage is one line of claude --output-format json/stream-json
// output: the json format emits a single result object (carrying the final
// text in Result); stream-json additionally emits assistant messages with
// content blocks (thinking, text).
type streamMessage struct {
	Type   string `json:"type"`
	Result string `json:"result"`
	// SessionID is the conversation id claude reports on every event of a
	// run (and the json format's result object carries); resuming a later
	// round with it continues the same conversation.
	SessionID string `json:"session_id"`
	Message   struct {
		Content []struct {
			Type     string `json:"type"`
			Thinking string `json:"thinking"`
			Text     string `json:"text"`
		} `json:"content"`
	} `json:"message"`
}

// parseAgentStream extracts the agent's chain of thought, visible text, and
// conversation session id from json or stream-json output. Non-JSON or
// uninteresting lines are skipped — the stream also carries system/init and
// delta events. The session id is whatever the last event carrying one
// reported (they all agree within a run; a resumed run keeps its id).
func parseAgentStream(stdout string) (thinking, text, session string) {
	for line := range strings.SplitSeq(stdout, "\n") {
		var m streamMessage
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		if m.SessionID != "" {
			session = m.SessionID
		}
		switch m.Type {
		case "assistant":
			for _, block := range m.Message.Content {
				switch block.Type {
				case "thinking":
					thinking += block.Thinking
				case "text":
					text += block.Text
				}
			}
		case "result":
			// The json format's single object; also the terminal event of
			// a stream-json run, where it restates the final assistant
			// text — overwrite rather than append to avoid duplication.
			if m.Result != "" {
				text = m.Result
			}
		}
	}
	return thinking, text, session
}

// RunJailedReviewerActivity has a jailed reviewer agent review the current
// state of the worktree and return a machine-readable verdict. The diff (and,
// when provided, the latest test output) is collected inside the activity, so
// large payloads stay out of workflow history; only the verdict travels on.
// The reviewer runs with the same structured output mode as the implementing
// agent, so its conversation id comes back for later rounds to resume; the
// verdict is parsed from the agent's visible text, falling back to raw stdout
// for a CLI that printed plain text.
func RunJailedReviewerActivity(ctx context.Context, input ReviewInput) (ReviewResult, error) {
	diff, err := stagedDiff(ctx, input.WorktreePath)
	if err != nil {
		return ReviewResult{}, err
	}
	prompt, err := template.Review(input.Focus, diff, input.TestLogs, input.TestsInScope)
	if err != nil {
		return ReviewResult{}, err
	}
	agent, _, agentArgs := jailedAgentCLI(input.Agent)
	// A session id from a previous round of the same review role resumes
	// that conversation — the re-review verifies its earlier findings with
	// the prior context instead of re-deriving them cold. Only claude
	// supports --resume; opencode and amp start fresh.
	if input.SessionID != "" && agent == "claude" {
		agentArgs = append([]string{"--resume", input.SessionID}, agentArgs...)
	}
	start := time.Now()
	res, err := runJailed(ctx, input.Agent, input.WorktreePath, prompt, agentArgs...)
	if err != nil {
		return ReviewResult{}, fmt.Errorf("review run: %w", err)
	}
	logger := activityLogger(ctx)
	logger.Info("Reviewer run completed", "Duration", time.Since(start).Round(time.Second))
	if res.Stderr != "" {
		logger.Info("Reviewer stderr", "Stderr", res.Stderr)
	}
	_, text, session := parseAgentStream(res.Stdout)
	if text == "" {
		// Plain-text CLI (or a parse miss): the whole stdout is the review.
		text = res.Stdout
	}
	verdict := parseReviewVerdict(text)
	verdict.SessionID = session
	return verdict, nil
}

// stagedDiff returns the full diff of the worktree against HEAD, including
// new files, without leaving the tree staged: intent-to-add (-N) makes new
// files visible to `git diff` while the index stays effectively untouched,
// so the next agent round sees normal `git diff`/`git status` output.
func stagedDiff(ctx context.Context, worktreePath string) (string, error) {
	if _, err := runGit(ctx, "-C", worktreePath, "add", "-N", "-A"); err != nil {
		return "", fmt.Errorf("stage intent-to-add: %w", err)
	}
	out, err := runGit(ctx, "-C", worktreePath, "diff")
	if err != nil {
		return "", fmt.Errorf("collect diff: %w", err)
	}
	return out, nil
}

// FinalizeWorktreeActivity commits the approved work and renames the run's
// branch from the in-flight feat/ prefix to the run's preserved prefix
// (branch_prefix / --prefix), so the deliverable survives cleanup. The
// workflow returns the preserved branch name to the caller. A run whose
// approved change produced no diff fails here — an approval of nothing is
// an anomaly, not a deliverable.
func FinalizeWorktreeActivity(ctx context.Context, input WorktreeInput) (string, error) {
	worktreePath, err := WorktreePathFor(input.TaskQueue, input.IssueID)
	if err != nil {
		return "", err
	}
	if _, err := runGit(ctx, "-C", worktreePath, "add", "-A"); err != nil {
		return "", fmt.Errorf("stage approved work: %w", err)
	}
	msg := fmt.Sprintf("daedalus: issue %s", input.IssueID)
	if _, err := runGit(ctx, "-C", worktreePath,
		"-c", "user.name=daedalus", "-c", "user.email=daedalus@local",
		"commit", "-m", msg); err != nil {
		return "", fmt.Errorf("commit approved work: %w", err)
	}
	if !strings.HasPrefix(input.BranchName, inFlightPrefix) {
		return "", fmt.Errorf("unexpected branch name %q: expected %s prefix", input.BranchName, inFlightPrefix)
	}
	prefix, err := input.preservedPrefix()
	if err != nil {
		return "", fmt.Errorf("branch prefix: %w", err)
	}
	preserved := prefix + "/" + strings.TrimPrefix(input.BranchName, inFlightPrefix)
	if _, err := runGit(ctx, "-C", worktreePath, "branch", "-m", preserved); err != nil {
		return "", fmt.Errorf("rename branch to %s: %w", preserved, err)
	}
	return preserved, nil
}

// parseReviewVerdict extracts the verdict from reviewer output: the last
// non-empty line decides. APPROVED approves; NEEDS_MAINTAINER parks the run
// for a maintainer; CHANGES_REQUESTED (or any other unrecognized line,
// including a missing marker) counts as changes requested, with everything
// above the verdict line — or the whole output, when no marker was found —
// as the comments to feed back to the implementing agent.
func parseReviewVerdict(out string) ReviewResult {
	lines := strings.Split(out, "\n")
	for i, raw := range slices.Backward(lines) {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if line == "APPROVED" || line == "CHANGES_REQUESTED" || line == "NEEDS_MAINTAINER" {
			return ReviewResult{
				Approved:        line == "APPROVED",
				NeedsMaintainer: line == "NEEDS_MAINTAINER",
				Comments:        strings.TrimSpace(strings.Join(lines[:i], "\n")),
			}
		}
		break
	}
	return ReviewResult{Approved: false, Comments: strings.TrimSpace(out)}
}

// truncateLogs bounds the logs carried in the activity result, keeping the
// tail (where test failures usually are) and a truncation marker.
func truncateLogs(logs string) string {
	return truncateTail(logs, maxTestLogs)
}

// truncateTail bounds s to its last max bytes (keeping whole UTF-8 runes)
// with a truncation marker.
func truncateTail(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := len(s) - max
	for cut < len(s) && !utf8.RuneStart(s[cut]) {
		cut++
	}
	return "[... earlier output truncated ...]\n" + s[cut:]
}

// RunNativeTestsActivity runs the repository's own test suite inside the
// worktree, whatever that suite is: the command is resolved per repo — a
// `.daedalus.yaml` declaration first, then static detection, then an
// AI discovery round for repos nothing recognizes (see nativeTestCommand).
// The agent names the run's -cli/--cli override, used only by the discovery
// round; empty falls back to the worker's DAEDALUS_AGENT. A non-zero exit
// is a test failure (Passed=false); any other error (e.g. no test binary on
// PATH) is a system error.
func RunNativeTestsActivity(ctx context.Context, worktreePath, agent string) (TestResult, error) {
	argv, err := nativeTestCommand(ctx, worktreePath, agent)
	if err != nil {
		return TestResult{}, err
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = worktreePath
	setProcessGroup(cmd)
	defer killGroup(cmd)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	res := TestResult{Command: strings.Join(argv, " ")}
	err = cmd.Run()
	if err != nil && !isWaitDelay(err) {
		if _, ok := errors.AsType[*exec.ExitError](err); !ok {
			return TestResult{}, fmt.Errorf("run tests (%s): %w", res.Command, err)
		}
		res.Passed, res.Logs = false, truncateLogs(out.String())
		return res, nil
	}
	res.Passed, res.Logs = true, truncateLogs(out.String())
	return res, nil
}

// nativeTestCommand resolves how to run the worktree's own test suite:
//  1. a `tests:` declaration in the repo's .daedalus.yaml — the repo owner's
//     explicit word, always winning;
//  2. static detection: marker files (go.mod, package.json with a test
//     script, pytest config) and Makefile test-ui/test-api targets;
//  3. an AI discovery round — a short jailed agent run that answers with
//     the command — for repositories none of the above recognize.
//
// agent is the run's -cli/--cli override, honored by the discovery round
// alone; empty falls back to the worker's DAEDALUS_AGENT.
func nativeTestCommand(ctx context.Context, worktreePath, agent string) ([]string, error) {
	if declared, ok := declaredTestCommand(worktreePath); ok {
		return []string{"sh", "-c", declared}, nil
	}
	if argv, ok := detectedTestCommand(worktreePath); ok {
		return argv, nil
	}
	return discoverTestCommand(ctx, worktreePath, agent)
}

// declaredTestCommand reads the repo-owned test declaration.
func declaredTestCommand(worktreePath string) (string, bool) {
	data, err := os.ReadFile(filepath.Join(worktreePath, ".daedalus.yaml"))
	if err != nil {
		return "", false
	}
	var decl struct {
		Tests string `yaml:"tests"`
	}
	if err := yaml.Unmarshal(data, &decl); err != nil {
		return "", false
	}
	if s := strings.TrimSpace(decl.Tests); s != "" {
		return s, true
	}
	return "", false
}

// detectedTestCommand recognizes the common test entrypoints from marker
// files. Order matters only in that go.mod wins over a Makefile — a Go repo
// that also has make targets usually wraps the same suite.
func detectedTestCommand(worktreePath string) ([]string, bool) {
	if _, err := os.Stat(filepath.Join(worktreePath, "go.mod")); err == nil {
		return []string{"go", "test", "./..."}, true
	}
	if pkg, err := os.ReadFile(filepath.Join(worktreePath, "package.json")); err == nil {
		var p struct {
			Scripts struct {
				Test string `json:"test"`
			} `json:"scripts"`
		}
		if json.Unmarshal(pkg, &p) == nil && strings.TrimSpace(p.Scripts.Test) != "" {
			return []string{"npm", "test"}, true
		}
	}
	for _, marker := range []string{"pyproject.toml", "pytest.ini", "setup.cfg"} {
		if _, err := os.Stat(filepath.Join(worktreePath, marker)); err == nil {
			return []string{"pytest", "-q"}, true
		}
	}
	if mk, err := os.ReadFile(filepath.Join(worktreePath, "Makefile")); err == nil {
		var targets []string
		for _, target := range []string{"test-ui", "test-api"} {
			if makefileHasTarget(string(mk), target) {
				targets = append(targets, target)
			}
		}
		if len(targets) > 0 {
			return append([]string{"make"}, targets...), true
		}
	}
	return nil, false
}

// makefileHasTarget reports whether the Makefile declares target as a rule
// ("target:" starting a line).
func makefileHasTarget(mk, target string) bool {
	for line := range strings.SplitSeq(mk, "\n") {
		if strings.HasPrefix(line, target+":") {
			return true
		}
	}
	return false
}

// discoverTestCommand asks a short jailed agent run for the repository's
// test entrypoint — the general, AI-led fallback. agent is the run's
// -cli/--cli override, empty for the worker's default.
func discoverTestCommand(ctx context.Context, worktreePath, agent string) ([]string, error) {
	res, err := runJailed(ctx, agent, worktreePath,
		"Inspect this repository and determine the exact shell command that runs its full test suite. "+
			"Reply with ONLY that command on a single line — no explanation, no code fences.")
	if err != nil {
		return nil, fmt.Errorf("discover test command: %w", err)
	}
	_, text, _ := parseAgentStream(res.Stdout)
	cmd := firstCommandLine(text)
	if cmd == "" {
		return nil, fmt.Errorf("no test command found for %s — declare one in .daedalus.yaml (tests: <command>)", worktreePath)
	}
	return []string{"sh", "-c", cmd}, nil
}

// firstCommandLine extracts a single-line command from an agent reply: the
// first non-empty, non-fence line, stripped of backticks.
func firstCommandLine(text string) string {
	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "```") {
			continue
		}
		line = strings.TrimSpace(strings.Trim(line, "`"))
		if line != "" && len(line) <= 500 {
			return line
		}
	}
	return ""
}

// CleanupWorktreeActivity removes the issue's worktree, its admin metadata,
// and the run's in-flight branch, leaving no dangling directories or git
// refs behind. Preserved branches are deliberately kept — they are the run's
// deliverable.
func CleanupWorktreeActivity(ctx context.Context, input WorktreeInput) error {
	worktreePath, err := WorktreePathFor(input.TaskQueue, input.IssueID)
	if err != nil {
		return err
	}
	return cleanWorktree(ctx, input, worktreePath)
}
