package workflows

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/template"
)

// FlowScope returns the per-issue scope segment a flow's derived names
// (worktree path, in-flight and aborted branches, stale sweep) carry: empty
// for the default flow and pre-field inputs — their legacy unscoped names
// stay untouched, so existing history, `continue`, and recapture keep
// working — the flow name otherwise. The scope is what keeps concurrent
// flows on one issue from sharing a worktree: the second create's cleanup
// would otherwise preserve-and-delete the first run's in-flight state.
func FlowScope(flow string) string {
	if flow == "" || flow == "feature-dev" {
		return ""
	}
	return flow
}

// pipelineRun carries one pipeline run's shared machinery: the activity
// contexts and their timeouts, the worktree lifecycle, and the
// round/retry/park helpers every flow's loop body is assembled from. Each
// workflow function (feature-dev and the flow files) owns only its loop —
// the machinery here is flow-agnostic, so a new flow is a loop body, not a
// fork of the retry logic.
type pipelineRun struct {
	input      PipelineInput
	logger     log.Logger
	ctx        workflow.Context // shared 15-minute ceiling (worktree, finalize, scope)
	agentCtx   workflow.Context // agent rounds (agent_run_timeout)
	reviewCtx  workflow.Context // reviewer rounds (review_timeout)
	cleanupCtx workflow.Context // cleanup (cleanup_timeout)

	// testTimeout is the run's resolved tests_timeout (config default
	// applied); discoverCtx runs test-command discovery under it on this
	// workflow's queue, testExecCtx runs suites and gates on the dedicated
	// test queue (activities.TestTaskQueue).
	testTimeout time.Duration
	// agentRunTimeout is the run's resolved agent_run_timeout — the
	// ceiling named in a cut-off round's continuation prompt.
	agentRunTimeout time.Duration
	discoverCtx     workflow.Context
	testExecCtx     workflow.Context

	guideCh workflow.ReceiveChannel
	// wakeupCh carries `daedalus worker wakeup`: a signal that interrupts
	// the quota heartbeat's sleep so a recovered provider resumes the
	// round immediately. Received only inside heartbeat — mid-activity it
	// stays buffered and merely short-circuits the next heartbeat.
	wakeupCh workflow.ReceiveChannel

	branchName    string
	worktreeInput activities.WorktreeInput
	worktree      activities.WorktreeOutput

	// cover, set by the test-only flow, asks suite executions to record Go
	// statement coverage (TestResult.Coverage) for the reviewer.
	cover bool

	quotaHeartbeats     int
	consecutiveTimeouts int
	reviewTimeouts      int

	devSession, testSession             string
	devReviewSession, testReviewSession string
}

// startRun builds the run's shared machinery and returns it with the
// worktree-cleanup function, which the caller must defer before creating
// the worktree: the defer has to be registered ahead of the create
// activity so that a cancellation during create still cleans up. The
// cleanup runs on a disconnected context so cancelling the workflow does
// not cancel the cleanup itself, and tolerates state that never came to
// exist. It deletes only the in-flight feat/ branch; approved work was
// renamed to the preserved prefix by FinalizeWorktreeActivity and survives.
func startRun(ctx workflow.Context, input PipelineInput) (*pipelineRun, func()) {
	logger := workflow.GetLogger(ctx)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	// Agent rounds get their own, wider ceiling: a whole-repo analysis
	// or slow build legitimately overruns the shared 15 minutes. Same
	// replay-safe zero fallback as TestTimeout below.
	agentTimeout := input.AgentRunTimeout
	if agentTimeout <= 0 {
		agentTimeout = config.DefaultAgentRunTimeout
	}
	agentCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: agentTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	// Reviewer rounds are jailed rounds too — a whole-repo review
	// legitimately runs long — but they historically shared the fixed
	// 15-minute ceiling above, which killed long reviews mid-verdict and
	// burned the whole timeout streak on one slow stage (a review that
	// needs more than one window could never succeed). They get their own
	// configurable ceiling, defaulting to the historical 15 minutes. Same
	// replay-safe zero fallback as the agent timeout; with the reviewer's
	// session resumed across windows (see the activities' session
	// tracking), consecutive timeouts accumulate progress instead of
	// restarting.
	reviewTimeout := input.ReviewTimeout
	if reviewTimeout <= 0 {
		reviewTimeout = config.DefaultReviewTimeout
	}
	reviewCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: reviewTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	// Cleanup gets its own ceiling too: committing the aborted snapshot
	// and removing a large worktree (a build tree can hold hundreds of
	// thousands of files) is filesystem-bound work that legitimately
	// overruns the shared 15 minutes. Same replay-safe zero fallback as
	// the agent timeout.
	cleanupTimeout := input.CleanupTimeout
	if cleanupTimeout <= 0 {
		cleanupTimeout = config.DefaultCleanupTimeout
	}
	cleanupCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: cleanupTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	// The suite and its command discovery share the wider tests_timeout
	// ceiling: command discovery (itself an agent round when nothing
	// statically detects) plus a cold build can legitimately overrun the
	// shared 15 minutes. They run on different queues, though: discovery
	// stays on this workflow's queue (its AI fallback is a jailed round
	// needing the worker's provider environment), while suites execute on
	// the dedicated test queue served by the agent-free test worker
	// (activities.TestTaskQueue) — suite runtime answers only to
	// tests_timeout, outside the agent-slot semaphore, and Get still uses
	// ctx so cancellation propagates normally.
	testTimeout := input.TestTimeout
	if testTimeout <= 0 {
		testTimeout = config.DefaultTestsTimeout
	}
	discoverCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: testTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	testExecCtx := workflow.WithTaskQueue(discoverCtx, activities.TestTaskQueue)

	flowScope := FlowScope(input.Flow)
	// The in-flight branch carries the flow segment too: a flow-scoped run's
	// cleanup sweeps only its own flow's stale branches.
	branchScope := ""
	if flowScope != "" {
		branchScope = flowScope + "-"
	}
	r := &pipelineRun{
		input:           input,
		logger:          logger,
		ctx:             ctx,
		agentCtx:        agentCtx,
		reviewCtx:       reviewCtx,
		cleanupCtx:      cleanupCtx,
		testTimeout:     testTimeout,
		agentRunTimeout: agentTimeout,
		discoverCtx:     discoverCtx,
		testExecCtx:     testExecCtx,
		guideCh:         workflow.GetSignalChannel(ctx, "guide"),
		wakeupCh:        workflow.GetSignalChannel(ctx, "wakeup"),
		branchName:      fmt.Sprintf("feat/%sissue-%s-%d", branchScope, input.IssueID, workflow.Now(ctx).Unix()),
	}
	r.worktreeInput = activities.WorktreeInput{
		RepoPath:     input.RepoPath,
		TaskQueue:    input.TaskQueue,
		IssueID:      input.IssueID,
		BranchName:   r.branchName,
		BranchPrefix: input.BranchPrefix,
		BaseBranch:   input.BaseBranch,
		Flow:         flowScope,
		Authorship:   input.Authorship,
	}
	return r, func() {
		// NewDisconnectedContext returns (Context, CancelFunc) — the second
		// value is not an error. Detach the cancel from this deferred func's
		// lifetime only after the cleanup activity has completed.
		dctx, cancel := workflow.NewDisconnectedContext(cleanupCtx)
		defer cancel()
		if err := workflow.ExecuteActivity(dctx, activities.CleanupWorktreeActivity, r.worktreeInput).Get(dctx, nil); err != nil {
			logger.Error("Failed to clean up worktree", "Error", err)
		}
	}
}

// createWorktree creates the run's worktree, recording its location and
// base commit for the rounds and gates that follow. It runs on the run's
// shared 15-minute context — the caller's bare workflow context carries no
// activity options.
func (r *pipelineRun) createWorktree() error {
	var worktree activities.WorktreeOutput
	if err := workflow.ExecuteActivity(r.ctx, activities.CreateWorktreeActivity, r.worktreeInput).Get(r.ctx, &worktree); err != nil {
		return fmt.Errorf("create worktree: %w", err)
	}
	r.worktree = worktree
	return nil
}

// park fails the run with ErrAwaitingMaintainer carrying the full reason: a
// reviewer NEEDS_MAINTAINER verdict, a structural gate the agent cannot
// satisfy, or anything else only a maintainer can resolve. A park is
// reported as a workflow failure with the reason in the history and FAILED
// in `daedalus list`; the deferred cleanup has already preserved the
// attempt's work on its aborted/ branch, so `daedalus continue` restarts
// from it.
func (r *pipelineRun) park(reason string) error {
	return fmt.Errorf("%w: %s", ErrAwaitingMaintainer, reason)
}

// drainGuidance returns the operator guidance sent since the last round
// (`daedalus guide`), formatted as a prompt prefix; empty when none
// arrived. Draining a signal channel is not a workflow command — adding it
// mid-run is replay-safe.
func (r *pipelineRun) drainGuidance() string {
	var parts []string
	for {
		var g string
		if !r.guideCh.ReceiveAsync(&g) {
			break
		}
		if g != "" {
			parts = append(parts, g)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "OPERATOR GUIDANCE (sent while this pipeline was running — treat as direct instructions from the operator, taking precedence over earlier plan assumptions):\n- " +
		strings.Join(parts, "\n- ")
}

// heartbeat absorbs one API-exhaustion hit: sleep quotaHeartbeatInterval
// and retry the same round, up to maxQuotaHeartbeats times, then give up
// into a park. Any completed round resets the streak, so a later
// exhaustion gets a fresh budget. The sleep ends early on the "wakeup"
// signal (`daedalus worker wakeup`) — the operator's lever for resuming
// the round the moment the provider has actually recovered.
func (r *pipelineRun) heartbeat(err error, stage string) error {
	r.quotaHeartbeats++
	if r.quotaHeartbeats > maxQuotaHeartbeats {
		return r.park(fmt.Sprintf("agent API still exhausted after %d hourly heartbeats (stage %q): %v",
			maxQuotaHeartbeats, stage, err))
	}
	r.logger.Warn("Agent API exhausted; sleeping one hour before retrying the round",
		"Stage", stage, "Heartbeat", r.quotaHeartbeats, "Of", maxQuotaHeartbeats)
	// The sleep races the "wakeup" signal (`daedalus worker wakeup`): the
	// timer branch resumes the round when the hour is out, the signal branch
	// the moment the operator — who has verified the provider recovered —
	// says so. Selector over two channels, not a poll: both branches are
	// single history events, replay-safe. A wakeup arriving mid-activity
	// stays buffered in wakeupCh and only skips the next heartbeat, which is
	// still the operator's intent.
	sel := workflow.NewSelector(r.ctx)
	sel.AddFuture(workflow.NewTimer(r.ctx, quotaHeartbeatInterval),
		func(workflow.Future) {})
	sel.AddReceive(r.wakeupCh, func(c workflow.ReceiveChannel, more bool) {
		var woken string
		c.Receive(r.ctx, &woken)
		r.logger.Info("Wakeup signal received; resuming the round before the heartbeat elapsed",
			"Stage", stage, "Heartbeat", r.quotaHeartbeats, "Of", maxQuotaHeartbeats)
	})
	sel.Select(r.ctx)
	return nil
}

// runAgent executes one autonomous agent round in the given role's session,
// resuming it when set, and returns the round's result — its reply text is
// what a test phase relays to its reviewer. The role also scopes the
// activity's recorded-session fallback, so a retry of a round killed at its
// ceiling resumes that role's own conversation.
func (r *pipelineRun) runAgent(prompt string, stage string, role activities.SessionRole, session *string) (activities.AgentRunResult, error) {
	// One per-call fallback when a resumed round fails outright (e.g. the
	// session no longer exists under the jail's state dir): retry the
	// round fresh rather than failing the whole run over a lost
	// conversation.
	freshFallback := false
	for {
		var result activities.AgentRunResult
		err := workflow.ExecuteActivity(r.agentCtx, activities.RunJailedClaudeActivity, activities.AgentRunInput{
			WorktreePath: r.worktree.WorktreePath,
			Prompt:       prompt,
			Agent:        r.input.Agent,
			SessionID:    *session,
			Role:         role,
		}).Get(r.ctx, &result)
		if err == nil {
			r.consecutiveTimeouts = 0
			r.quotaHeartbeats = 0
			*session = result.SessionID
			r.logger.Info("Agent run completed", "Stage", stage,
				"TextChars", len(result.Text), "ThinkingChars", len(result.Thinking))
			return result, nil
		}
		// The kill classification runs first: its marker is
		// wait-status-derived in the worker (ErrAgentKilled), while
		// the exhaustion and slot markers below match output text an
		// externally killed round may well have printed before it
		// died — a kill must not park the run in the quota heartbeat.
		killed := isAgentKilled(err)
		if !killed && isAPIExhaustion(err) {
			if herr := r.heartbeat(err, stage); herr != nil {
				return activities.AgentRunResult{}, herr
			}
			continue
		}
		if !killed && isSlotWait(err) {
			// The round never launched — re-queue it unchanged, do not
			// count it against the timeout streak (whose continuation
			// prompt would fabricate partial work).
			r.logger.Warn("All jailed-agent slots busy; backing off before re-queuing the round",
				"Stage", stage, "Backoff", slotBackoffInterval)
			if serr := workflow.Sleep(r.ctx, slotBackoffInterval); serr != nil {
				return activities.AgentRunResult{}, fmt.Errorf("slot backoff sleep (stage %q): %w", stage, serr)
			}
			continue
		}
		// An abruptly killed round is as recoverable as a timed-out
		// one — the worktree keeps whatever the agent finished before
		// it died (only daedalus's own timeout paths kill cleanly via
		// context; a signal means something external took the round,
		// e.g. an operator sweep or a host-level kill). It falls
		// through to the timeout handling below: continuation prompt,
		// counted against the same streak so a repeat killer still
		// fails the run.
		if !killed && !temporal.IsTimeoutError(err) {
			if *session != "" && !freshFallback {
				freshFallback = true
				r.logger.Warn("Resumed agent round failed; retrying the round with a fresh session",
					"Stage", stage, "Error", err)
				*session = ""
				continue
			}
			return activities.AgentRunResult{}, err
		}
		r.consecutiveTimeouts++
		cutoff := fmt.Sprintf(
			"the previous attempt was cut off by the round's %s timeout ceiling before it finished",
			r.agentRunTimeout)
		if killed {
			cutoff = "the previous attempt was cut off abruptly before it finished (the agent process was killed mid-round)"
		}
		r.logger.Warn("Agent round cut off; continuing from partial work",
			"Stage", stage, "ConsecutiveTimeouts", r.consecutiveTimeouts,
			"AgentRunTimeout", r.agentRunTimeout, "Killed", killed)
		if r.consecutiveTimeouts >= maxConsecutiveTimeouts {
			return activities.AgentRunResult{}, fmt.Errorf("agent run cut off %d rounds in a row (stage %q): %w",
				r.consecutiveTimeouts, stage, err)
		}
		followUp, ferr := template.Continue(prompt, cutoff)
		if ferr != nil {
			return activities.AgentRunResult{}, fmt.Errorf("build timeout-continuation prompt (stage %q): %w", stage, ferr)
		}
		prompt = followUp
	}
}

// review asks the reviewer for a verdict on the worktree's current state,
// retrying the same round across timeouts — there is no partial work to
// continue, the verdict simply never arrived.
func (r *pipelineRun) review(focus, testLogs string, testsInScope, reproInScope bool, agentReply string, role activities.SessionRole, session *string) (activities.ReviewResult, error) {
	// Same lost-session fallback as the agent rounds: one fresh retry.
	freshFallback := false
	for {
		var result activities.ReviewResult
		err := workflow.ExecuteActivity(r.reviewCtx, activities.RunJailedReviewerActivity, activities.ReviewInput{
			WorktreePath: r.worktree.WorktreePath,
			Focus:        focus,
			TestLogs:     testLogs,
			TestsInScope: testsInScope,
			ReproInScope: reproInScope,
			AgentReply:   agentReply,
			Agent:        r.input.Agent,
			SessionID:    *session,
			Role:         role,
		}).Get(r.ctx, &result)
		if err == nil {
			r.reviewTimeouts = 0
			r.quotaHeartbeats = 0
			*session = result.SessionID
			return result, nil
		}
		// Same kill-first precedence as the agent rounds: a killed
		// reviewer must not park in the quota heartbeat over output
		// text it printed before dying.
		killed := isAgentKilled(err)
		if !killed && isAPIExhaustion(err) {
			if herr := r.heartbeat(err, "review"); herr != nil {
				return result, herr
			}
			continue
		}
		if !killed && isSlotWait(err) {
			// Same re-queue as the agent rounds: the reviewer never
			// launched, so the focus is retried as-is.
			r.logger.Warn("All jailed-agent slots busy; backing off before re-queuing the review",
				"Focus", focus, "Backoff", slotBackoffInterval)
			if serr := workflow.Sleep(r.ctx, slotBackoffInterval); serr != nil {
				return result, fmt.Errorf("slot backoff sleep (review %q): %w", focus, serr)
			}
			continue
		}
		if !killed && !temporal.IsTimeoutError(err) {
			if *session != "" && !freshFallback {
				freshFallback = true
				r.logger.Warn("Resumed reviewer round failed; retrying the review with a fresh session",
					"Focus", focus, "Error", err)
				*session = ""
				continue
			}
			return result, err
		}
		r.reviewTimeouts++
		r.logger.Warn("Reviewer round cut off (timeout or kill); retrying",
			"Focus", focus, "ConsecutiveTimeouts", r.reviewTimeouts)
		if r.reviewTimeouts >= maxConsecutiveTimeouts {
			return result, fmt.Errorf("reviewer timed out %d rounds in a row (focus %q): %w",
				r.reviewTimeouts, focus, err)
		}
	}
}

// resolveTestCommand resolves the worktree's test-suite entrypoint,
// absorbing slot waits and quota exhaustion the same way agent rounds do.
// Anything else — including a discovery timeout — is not suite output a
// reviewer can act on, so the error fails the run instead of feeding the
// fix loop a synthetic red round.
func (r *pipelineRun) resolveTestCommand() (string, error) {
	for {
		var command string
		err := workflow.ExecuteActivity(r.discoverCtx, activities.ResolveTestCommandActivity,
			r.worktree.WorktreePath, r.input.Agent).Get(r.ctx, &command)
		if err == nil {
			return command, nil
		}
		if isSlotWait(err) {
			// Test-command discovery queues on the same semaphore as
			// every other jailed round and can give up on it too.
			r.logger.Warn("All jailed-agent slots busy; backing off before re-queuing test-command discovery",
				"Backoff", slotBackoffInterval)
			if serr := workflow.Sleep(r.ctx, slotBackoffInterval); serr != nil {
				return "", fmt.Errorf("resolve test command: %w", serr)
			}
			continue
		}
		if isAPIExhaustion(err) {
			// Test-command discovery runs a jailed agent round, so it
			// can hit the provider cap too.
			if herr := r.heartbeat(err, "tests"); herr != nil {
				return "", fmt.Errorf("resolve test command: %w", herr)
			}
			continue
		}
		return "", fmt.Errorf("resolve test command: %w", err)
	}
}

// runSuite executes the suite command on the test queue. A timed-out suite
// is a failing round, not a dead run: the fix loop already digests red
// logs, so it hands back a synthetic one describing the timeout.
func (r *pipelineRun) runSuite(command string) (activities.TestResult, error) {
	var result activities.TestResult
	err := workflow.ExecuteActivity(r.testExecCtx, activities.RunTestSuiteActivity,
		activities.TestRunInput{
			WorktreePath: r.worktree.WorktreePath,
			Command:      command,
			Cover:        r.cover,
		}).Get(r.ctx, &result)
	if err == nil {
		// A suite round that ran to completion resets the quota streak
		// like any other completed round — even a red one, since the
		// provider was reachable for it.
		r.quotaHeartbeats = 0
		return result, nil
	}
	if !temporal.IsTimeoutError(err) {
		return activities.TestResult{}, fmt.Errorf("run tests: %w", err)
	}
	r.logger.Warn("Native test suite hit the timeout ceiling; treating as a failing round",
		"TestsTimeout", r.testTimeout)
	return activities.TestResult{
		Passed: false,
		Logs:   fmt.Sprintf("NATIVE TEST SUITE TIMED OUT: the suite did not finish within %s. Cut runtime (parallelism, caching, narrower scope) or raise config tests_timeout.", r.testTimeout),
	}, nil
}

// reproGate runs the bug-fix flow's repro-first gate: the diff's new or
// changed test files must fail on the run's base tree — a test that passes
// there does not capture the bug.
func (r *pipelineRun) reproGate(command string) (activities.ReproResult, error) {
	var res activities.ReproResult
	if err := workflow.ExecuteActivity(r.testExecCtx, activities.ReproFirstGateActivity,
		activities.ReproGateInput{
			RepoPath:     r.input.RepoPath,
			WorktreePath: r.worktree.WorktreePath,
			Command:      command,
		}).Get(r.ctx, &res); err != nil {
		return activities.ReproResult{}, fmt.Errorf("repro-first gate: %w", err)
	}
	return res, nil
}

// checkWriteScope runs the flow's structural write-scope gate: a diff that
// leaves the flow's allowed paths or touches its frozen paths parks the run
// for the maintainer, with the offending paths in the message. Flows
// without a policy (feature-dev, bug-fix) skip the activity entirely.
func (r *pipelineRun) checkWriteScope() error {
	if len(r.input.AllowedPaths) == 0 && len(r.input.FrozenPaths) == 0 {
		return nil
	}
	var res activities.ScopeResult
	if err := workflow.ExecuteActivity(r.ctx, activities.VerifyWriteScopeActivity, activities.ScopeCheckInput{
		WorktreePath: r.worktree.WorktreePath,
		Allowed:      r.input.AllowedPaths,
		Frozen:       r.input.FrozenPaths,
	}).Get(r.ctx, &res); err != nil {
		return fmt.Errorf("write-scope check: %w", err)
	}
	if len(res.Violations) > 0 {
		return r.park("write-scope violation — the diff leaves this flow's write scope, and stripping files silently is not an option:\n" +
			strings.Join(res.Violations, "\n"))
	}
	return nil
}

// finalize commits the approved work and renames the run's branch to its
// preserved prefix, returning the preserved branch name.
func (r *pipelineRun) finalize() (string, error) {
	var preservedBranch string
	if err := workflow.ExecuteActivity(r.ctx, activities.FinalizeWorktreeActivity, r.worktreeInput).Get(r.ctx, &preservedBranch); err != nil {
		return "", fmt.Errorf("finalize worktree: %w", err)
	}
	r.logger.Info("Approved work committed", "Branch", preservedBranch)
	return preservedBranch, nil
}
