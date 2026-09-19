// Package workflows defines the Temporal workflows that orchestrate a
// Daedalus feature-development pipeline.
package workflows

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/template"
)

// PipelineInput is the sole input to FeatureDevWorkflow. It deliberately
// carries no credentials: the worker reads provider settings from its own
// environment (exported from config.yaml), keeping secrets out of workflow
// history.
type PipelineInput struct {
	RepoPath  string
	TaskQueue string
	IssueID   string
	Prompt    string
	// BranchPrefix names the preserved branch that carries the run's
	// approved work (config branch_prefix, `run --prefix`). Empty means
	// the activities' default.
	BranchPrefix string
	// BaseBranch, when set, starts the worktree from an aborted attempt's
	// preserved branch instead of HEAD — a continued run (`daedalus
	// continue`). PriorFeedback is that attempt's last review feedback,
	// folded into the opening prompt.
	BaseBranch    string
	PriorFeedback string
	// Agent, set by `run -cli/--cli`, overrides the config's jailed agent
	// for this run; empty — a run whose input predates the field, replayed
	// by a newer worker — falls back to the worker's DAEDALUS_AGENT.
	Agent string
	// TestTimeout bounds one execution of the native test suite (config
	// tests_timeout). Zero — a run whose input predates the field, replayed
	// by a newer worker — falls back to config.DefaultTestsTimeout.
	TestTimeout time.Duration
	// AgentRunTimeout bounds one jailed-agent round (config
	// agent_run_timeout). Zero — a run whose input predates the field,
	// replayed by a newer worker — falls back to
	// config.DefaultAgentRunTimeout.
	AgentRunTimeout time.Duration
	// CleanupTimeout bounds one CleanupWorktreeActivity (config
	// cleanup_timeout). Same replay-safe zero fallback as AgentRunTimeout:
	// config.DefaultCleanupTimeout.
	CleanupTimeout time.Duration
}

// maxConsecutiveTimeouts caps how many timed-out rounds in a row the
// pipeline absorbs before failing the run: recovery assumes the agent
// makes progress each round, and a run wedged at its ceiling every
// time would otherwise loop forever on the agent budget.
const maxConsecutiveTimeouts = 3

// quotaHeartbeatInterval is how long a run sleeps when the agent API is
// exhausted (hard cap, rate limit, overload) before retrying the same
// round unchanged.
const quotaHeartbeatInterval = time.Hour

// slotBackoffInterval is how long a run sleeps before re-queueing a round
// that gave up waiting for an agent concurrency slot
// (activities.ErrAgentSlotsBusy). Unlike the quota heartbeat it is
// uncapped: a slot frees whenever any running round ends, and every round
// is itself bounded by its StartToClose, so backing off cannot wedge a
// healthy run — a cap would fail legitimately queued ones.
const slotBackoffInterval = time.Minute

// maxQuotaHeartbeats caps the hourly retries; once the API is still
// exhausted after this many heartbeats the run parks itself for a
// maintainer restart instead of failing.
const maxQuotaHeartbeats = 5

// ErrAwaitingMaintainer parks a run instead of failing it with a raw error:
// the API stayed exhausted past every heartbeat, or the reviewer halted with
// NEEDS_MAINTAINER on a task that cannot be completed as stated. A park is
// reported as a workflow failure carrying this error, so the reason lands
// in the workflow history and `daedalus list` shows the run as FAILED —
// the attempt's work is already preserved on its aborted/ branch by the
// deferred cleanup, so `daedalus continue` restarts from it.
var ErrAwaitingMaintainer = errors.New("run parked awaiting maintainer restart")

// FeatureDevWorkflow drives a full issue-development cycle in two
// review-gated phases: (1) implementation ↔ code review until the reviewer
// approves, then (2) tests ↔ test review until the reviewer approves AND the
// native test suite passes. On success the approved work is committed and
// the run's branch renamed to its preserved prefix; the workflow
// returns that branch name. Both loops are intentionally unbounded — they
// run until approval, with no attempt cap; each round is durable, auditable,
// and individually timed-out via activity options. Two conditions park the
// run for a maintainer restart rather than failing it with a raw error (see
// ErrAwaitingMaintainer): the provider API staying exhausted past every
// quota heartbeat, or a reviewer NEEDS_MAINTAINER verdict on a task that
// cannot be completed as stated. A parked run fails with ErrAwaitingMaintainer
// (and so with the reason in the history and FAILED in `daedalus list`);
// the deferred cleanup preserves the attempt's work on its aborted/ branch
// either way.
func FeatureDevWorkflow(ctx workflow.Context, input PipelineInput) (string, error) {
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

	branchName := fmt.Sprintf("feat/issue-%s-%d", input.IssueID, workflow.Now(ctx).Unix())
	worktreeInput := activities.WorktreeInput{
		RepoPath:     input.RepoPath,
		TaskQueue:    input.TaskQueue,
		IssueID:      input.IssueID,
		BranchName:   branchName,
		BranchPrefix: input.BranchPrefix,
		BaseBranch:   input.BaseBranch,
	}

	// Guarantee the workspace is cleaned up on every exit path. The defer is
	// registered before the create activity so that a cancellation during
	// create still cleans up — a Get on a cancelled context fails the
	// workflow before any later defer could be installed. The cleanup runs
	// on a disconnected context so that cancelling the workflow does not
	// cancel the cleanup itself, and tolerates state that never came to
	// exist. It deletes only the in-flight feat/ branch; approved work was
	// renamed to the preserved prefix by FinalizeWorktreeActivity and
	// survives.
	defer func() {
		// NewDisconnectedContext returns (Context, CancelFunc) — the second
		// value is not an error. Detach the cancel from this deferred func's
		// lifetime only after the cleanup activity has completed.
		dctx, cancel := workflow.NewDisconnectedContext(cleanupCtx)
		defer cancel()
		if err := workflow.ExecuteActivity(dctx, activities.CleanupWorktreeActivity, worktreeInput).Get(dctx, nil); err != nil {
			logger.Error("Failed to clean up worktree", "Error", err)
		}
	}()

	var worktree activities.WorktreeOutput
	if err := workflow.ExecuteActivity(ctx, activities.CreateWorktreeActivity, worktreeInput).Get(ctx, &worktree); err != nil {
		return "", fmt.Errorf("create worktree: %w", err)
	}

	// runAgent executes one autonomous implementation round; review asks the
	// reviewer for a verdict on the worktree's current state. The agent's
	// text and chain of thought come back tail-bounded in the activity
	// result — serialized into Temporal history — while the worker log keeps
	// the full stream.
	//
	// guideCh carries operator guidance sent while the pipeline runs (see
	// `daedalus guide`): each fix prompt is prefixed with whatever arrived
	// since the last round, so a human can steer a stuck review loop without
	// restarting the run. Draining a signal channel is not a workflow
	// command — adding it mid-run is replay-safe.
	guideCh := workflow.GetSignalChannel(ctx, "guide")
	drainGuidance := func() string {
		var parts []string
		for {
			var g string
			if !guideCh.ReceiveAsync(&g) {
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
	// Quota exhaustion is a pause, not a failure: when the provider's hard
	// cap is reached, the run heartbeats — sleeping an hour and retrying
	// the same round unchanged — up to maxQuotaHeartbeats times. Still
	// exhausted after that, the run parks itself for the maintainer
	// (ErrAwaitingMaintainer) instead of failing. Any completed round
	// resets the streak, so a later exhaustion gets a fresh budget.
	quotaHeartbeats := 0
	heartbeat := func(err error, stage string) error {
		quotaHeartbeats++
		if quotaHeartbeats > maxQuotaHeartbeats {
			return fmt.Errorf("%w: agent API still exhausted after %d hourly heartbeats (stage %q): %v",
				ErrAwaitingMaintainer, maxQuotaHeartbeats, stage, err)
		}
		logger.Warn("Agent API exhausted; sleeping one hour before retrying the round",
			"Stage", stage, "Heartbeat", quotaHeartbeats, "Of", maxQuotaHeartbeats)
		if serr := workflow.Sleep(ctx, quotaHeartbeatInterval); serr != nil {
			return fmt.Errorf("quota heartbeat sleep (stage %q): %w", stage, serr)
		}
		return nil
	}
	// A timed-out round is recoverable, not fatal: the worktree keeps
	// the attempt's partial work, so the round re-runs as a
	// continuation — the agent inspects what it already did and drives
	// on instead of starting over. Any completed round resets the
	// streak.
	consecutiveTimeouts := 0
	// Four role-scoped sessions chain a run's rounds, each separate from the
	// others: the dev agent (implement + fix rounds), the dev reviewer (code
	// reviews), the test agent (tests + test-fix rounds), and the test
	// reviewer (test reviews). Within a role each round resumes the previous
	// one, inheriting its context and the provider's warm prompt cache; the
	// roles stay separate so, in particular, the test agent writes against
	// the reviewed diff with fresh eyes rather than the implementer's
	// framing, and one reviewer's findings never anchor the other's. Sessions
	// do not cross workflow runs — a continued run starts fresh against its
	// preserved work.
	devSession, testSession := "", ""
	// runAgent runs one agent round in the given role's session, resuming
	// it when set, and returns the round's result — its reply text is what
	// the test phase relays to its reviewer.
	runAgent := func(prompt string, stage string, session *string) (activities.AgentRunResult, error) {
		// One per-call fallback when a resumed round fails outright (e.g. the
		// session no longer exists under the jail's state dir): retry the
		// round fresh rather than failing the whole run over a lost
		// conversation.
		freshFallback := false
		for {
			var result activities.AgentRunResult
			err := workflow.ExecuteActivity(agentCtx, activities.RunJailedClaudeActivity, activities.AgentRunInput{
				WorktreePath: worktree.WorktreePath,
				Prompt:       prompt,
				Agent:        input.Agent,
				SessionID:    *session,
			}).Get(ctx, &result)
			if err == nil {
				consecutiveTimeouts = 0
				quotaHeartbeats = 0
				*session = result.SessionID
				logger.Info("Agent run completed", "Stage", stage,
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
				if herr := heartbeat(err, stage); herr != nil {
					return activities.AgentRunResult{}, herr
				}
				continue
			}
			if !killed && isSlotWait(err) {
				// The round never launched — re-queue it unchanged, do not
				// count it against the timeout streak (whose continuation
				// prompt would fabricate partial work).
				logger.Warn("All jailed-agent slots busy; backing off before re-queuing the round",
					"Stage", stage, "Backoff", slotBackoffInterval)
				if serr := workflow.Sleep(ctx, slotBackoffInterval); serr != nil {
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
					logger.Warn("Resumed agent round failed; retrying the round with a fresh session",
						"Stage", stage, "Error", err)
					*session = ""
					continue
				}
				return activities.AgentRunResult{}, err
			}
			consecutiveTimeouts++
			cutoff := fmt.Sprintf(
				"the previous attempt was cut off by the round's %s timeout ceiling before it finished",
				agentTimeout)
			if killed {
				cutoff = "the previous attempt was cut off abruptly before it finished (the agent process was killed mid-round)"
			}
			logger.Warn("Agent round cut off; continuing from partial work",
				"Stage", stage, "ConsecutiveTimeouts", consecutiveTimeouts,
				"AgentRunTimeout", agentTimeout, "Killed", killed)
			if consecutiveTimeouts >= maxConsecutiveTimeouts {
				return activities.AgentRunResult{}, fmt.Errorf("agent run cut off %d rounds in a row (stage %q): %w",
					consecutiveTimeouts, stage, err)
			}
			followUp, ferr := template.Continue(prompt, cutoff)
			if ferr != nil {
				return activities.AgentRunResult{}, fmt.Errorf("build timeout-continuation prompt (stage %q): %w", stage, ferr)
			}
			prompt = followUp
		}
	}
	// Reviewer timeouts retry the same round unchanged — there is no
	// partial work to continue, the verdict simply never arrived. agentReply
	// quotes the test agent's latest reply for the test reviewer (empty in
	// phase 1).
	reviewTimeouts := 0
	devReviewSession, testReviewSession := "", ""
	review := func(focus, testLogs string, testsInScope bool, agentReply string, session *string) (activities.ReviewResult, error) {
		// Same lost-session fallback as the agent rounds: one fresh retry.
		freshFallback := false
		for {
			var result activities.ReviewResult
			err := workflow.ExecuteActivity(ctx, activities.RunJailedReviewerActivity, activities.ReviewInput{
				WorktreePath: worktree.WorktreePath,
				Focus:        focus,
				TestLogs:     testLogs,
				TestsInScope: testsInScope,
				AgentReply:   agentReply,
				Agent:        input.Agent,
				SessionID:    *session,
			}).Get(ctx, &result)
			if err == nil {
				reviewTimeouts = 0
				quotaHeartbeats = 0
				*session = result.SessionID
				return result, nil
			}
			// Same kill-first precedence as the agent rounds: a killed
			// reviewer must not park in the quota heartbeat over output
			// text it printed before dying.
			killed := isAgentKilled(err)
			if !killed && isAPIExhaustion(err) {
				if herr := heartbeat(err, "review"); herr != nil {
					return result, herr
				}
				continue
			}
			if !killed && isSlotWait(err) {
				// Same re-queue as the agent rounds: the reviewer never
				// launched, so the focus is retried as-is.
				logger.Warn("All jailed-agent slots busy; backing off before re-queuing the review",
					"Focus", focus, "Backoff", slotBackoffInterval)
				if serr := workflow.Sleep(ctx, slotBackoffInterval); serr != nil {
					return result, fmt.Errorf("slot backoff sleep (review %q): %w", focus, serr)
				}
				continue
			}
			if !killed && !temporal.IsTimeoutError(err) {
				if *session != "" && !freshFallback {
					freshFallback = true
					logger.Warn("Resumed reviewer round failed; retrying the review with a fresh session",
						"Focus", focus, "Error", err)
					*session = ""
					continue
				}
				return result, err
			}
			reviewTimeouts++
			logger.Warn("Reviewer round cut off (timeout or kill); retrying",
				"Focus", focus, "ConsecutiveTimeouts", reviewTimeouts)
			if reviewTimeouts >= maxConsecutiveTimeouts {
				return result, fmt.Errorf("reviewer timed out %d rounds in a row (focus %q): %w",
					reviewTimeouts, focus, err)
			}
		}
	}

	// codeReviewLoop runs implementation ↔ code-review rounds until the
	// code reviewer approves, parking on NEEDS_MAINTAINER. Both the first
	// pass and a test-phase REBUILD land here, so the rebuild re-enters the
	// same reviewer conversation instead of starting cold.
	codeReviewLoop := func() error {
		for {
			verdict, err := review("the implementation", "", false, "", &devReviewSession)
			if err != nil {
				return fmt.Errorf("code review: %w", err)
			}
			if verdict.NeedsMaintainer {
				logger.Info("Code review halted the run for maintainer input")
				return fmt.Errorf("%w: code review halted the run — the task cannot be completed as stated: %s",
					ErrAwaitingMaintainer, verdict.Comments)
			}
			if verdict.Approved {
				logger.Info("Code review approved")
				return nil
			}
			logger.Info("Code review requested changes")
			fixPrompt, err := template.ImplementFix(verdict.Comments)
			if err != nil {
				return fmt.Errorf("build implement-fix prompt: %w", err)
			}
			if g := drainGuidance(); g != "" {
				logger.Info("Operator guidance received, folding into fix prompt")
				fixPrompt = g + "\n\n" + fixPrompt
			}
			if _, err := runAgent(fixPrompt, "implement-fix", &devSession); err != nil {
				return fmt.Errorf("agent run after code review: %w", err)
			}
		}
	}

	// rebuild routes a test-review REBUILD finding back through the dev
	// cycle: a tight, finding-only prompt into the dev session, then code
	// review until approved. The test loop resumes afterwards with its own
	// sessions intact.
	rebuild := func(finding string) error {
		prompt, err := template.Rebuild(finding)
		if err != nil {
			return fmt.Errorf("build rebuild prompt: %w", err)
		}
		if _, err := runAgent(prompt, "rebuild", &devSession); err != nil {
			return fmt.Errorf("rebuild agent run: %w", err)
		}
		return codeReviewLoop()
	}

	// Phase 1: implementation ↔ code review, until the reviewer approves.
	// A continued run opens on the aborted attempt's preserved work.
	var initialPrompt string
	var err error
	if input.BaseBranch != "" {
		initialPrompt, err = template.Continue(input.Prompt, input.PriorFeedback)
	} else {
		initialPrompt, err = template.Implement(input.Prompt)
	}
	if err != nil {
		return "", fmt.Errorf("build implement prompt: %w", err)
	}
	if _, err := runAgent(initialPrompt, "implement", &devSession); err != nil {
		return "", fmt.Errorf("initial agent run: %w", err)
	}
	if err := codeReviewLoop(); err != nil {
		return "", err
	}

	// Phase 2: tests ↔ test review, until the reviewer approves AND the
	// native suite passes. The tester's latest reply travels to its
	// reviewer, which alone can verdict REBUILD — routing an
	// implementation-level finding back through the dev cycle (above)
	// before the test loop resumes with both sessions intact.
	testsPrompt, err := template.Tests()
	if err != nil {
		return "", fmt.Errorf("build tests prompt: %w", err)
	}
	testerReply, err := runAgent(testsPrompt, "tests", &testSession)
	if err != nil {
		return "", fmt.Errorf("test-phase agent run: %w", err)
	}
	// The native suite gets a wider ceiling than the shared 15 minutes:
	// command discovery (itself an agent round when nothing statically
	// detects) plus a cold build can legitimately overrun it. The Get still
	// uses ctx so cancellation propagates normally.
	testTimeout := input.TestTimeout
	if testTimeout <= 0 {
		testTimeout = config.DefaultTestsTimeout
	}
	testsCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: testTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	for {
		var result activities.TestResult
		if err := workflow.ExecuteActivity(testsCtx, activities.RunNativeTestsActivity,
			worktree.WorktreePath, input.Agent).Get(ctx, &result); err != nil {
			if isSlotWait(err) {
				// Test-command discovery queues on the same semaphore as
				// every other jailed round and can give up on it too.
				logger.Warn("All jailed-agent slots busy; backing off before re-queuing the suite",
					"Backoff", slotBackoffInterval)
				if serr := workflow.Sleep(ctx, slotBackoffInterval); serr != nil {
					return "", fmt.Errorf("run tests: %w", serr)
				}
				continue
			}
			if isAPIExhaustion(err) {
				// Test-command discovery runs a jailed agent round, so the
				// suite activity can hit the provider cap too.
				if herr := heartbeat(err, "tests"); herr != nil {
					return "", fmt.Errorf("run tests: %w", herr)
				}
				continue
			}
			if !temporal.IsTimeoutError(err) {
				return "", fmt.Errorf("run tests: %w", err)
			}
			// A timed-out suite is a failing round, not a dead run: the
			// fix loop already digests red logs, so hand it a synthetic
			// one describing the timeout.
			logger.Warn("Native test suite hit the timeout ceiling; treating as a failing round",
				"TestsTimeout", testTimeout)
			result = activities.TestResult{
				Passed: false,
				Logs:   fmt.Sprintf("NATIVE TEST SUITE TIMED OUT: the suite did not finish within %s. Cut runtime (parallelism, caching, narrower scope) or raise config tests_timeout.", testTimeout),
			}
		} else {
			// A suite round that ran to completion resets the quota streak
			// like any other completed round — even a red one, since the
			// provider was reachable for it.
			quotaHeartbeats = 0
		}
		verdict, err := review("the test suite", result.Logs, true, testerReply.Text, &testReviewSession)
		if err != nil {
			return "", fmt.Errorf("test review: %w", err)
		}
		if verdict.NeedsMaintainer {
			logger.Info("Test review halted the run for maintainer input")
			return "", fmt.Errorf("%w: test review halted the run — the task cannot be completed as stated: %s",
				ErrAwaitingMaintainer, verdict.Comments)
		}
		if verdict.Rebuild {
			logger.Info("Test review requested a rebuild; returning to the dev cycle")
			if err := rebuild(verdict.Comments); err != nil {
				return "", err
			}
			continue
		}
		if result.Passed && verdict.Approved {
			logger.Info("Tests pass and test review approved")
			var preservedBranch string
			if err := workflow.ExecuteActivity(ctx, activities.FinalizeWorktreeActivity, worktreeInput).Get(ctx, &preservedBranch); err != nil {
				return "", fmt.Errorf("finalize worktree: %w", err)
			}
			logger.Info("Approved work committed", "Branch", preservedBranch)
			return preservedBranch, nil
		}
		testFix := testFixPrompt(result, verdict)
		if g := drainGuidance(); g != "" {
			logger.Info("Operator guidance received, folding into fix prompt")
			testFix = g + "\n\n" + testFix
		}
		fixResult, err := runAgent(testFix, "tests-fix", &testSession)
		if err != nil {
			return "", fmt.Errorf("test-fix agent run: %w", err)
		}
		testerReply = fixResult
	}
}

// testFixPrompt builds the phase-2 fix prompt from whatever failed: test
// output, review comments, or both. Template rendering is deterministic, so
// it is safe to call from workflow code.
func testFixPrompt(result activities.TestResult, verdict activities.ReviewResult) string {
	logs := ""
	if !result.Passed {
		logs = result.Logs
	}
	comments := ""
	if !verdict.Approved {
		comments = verdict.Comments
	}
	prompt, err := template.TestsFix(logs, comments)
	if err != nil {
		// The templates are compile-time-validated (template.Must); an
		// execution failure is a packaging bug. Fail the workflow loudly
		// rather than looping without feedback.
		return fmt.Sprintf("Internal error building the fix prompt: %v", err)
	}
	return prompt
}

// isAPIExhaustion reports whether err is the activities' ErrAPIExhausted.
// Crossing the worker→workflow boundary an activity error survives only as
// the application error's message text, and the activities wrap the
// sentinel with %w (directly in runJailed, nested inside test-command
// discovery), so a plain substring match covers every case.
func isAPIExhaustion(err error) bool {
	return err != nil && strings.Contains(err.Error(), activities.ErrAPIExhausted.Error())
}

// isSlotWait reports whether err is the activities' ErrAgentSlotsBusy — a
// round that queued past the bounded slot wait and gave up without
// launching the agent. Same boundary note as isAPIExhaustion: the
// activities wrap the sentinel with %w (runJailed directly, the reviewer
// and test-command-discovery paths nested), so a substring match covers
// every case.
func isSlotWait(err error) bool {
	return err != nil && strings.Contains(err.Error(), activities.ErrAgentSlotsBusy.Error())
}

// isAgentKilled reports whether err is the activities' ErrAgentKilled —
// a jailed round whose process died to a signal, an abrupt external death
// rather than a timeout or a clean failure. Unlike the exhaustion and
// slot markers (output text), this one is derived from the wait status in
// the worker and wrapped without any agent output, so text an agent
// printed cannot forge it. Same boundary note as isAPIExhaustion: the
// sentinel travels as error text, so a substring match carries it.
func isAgentKilled(err error) bool {
	return err != nil && strings.Contains(err.Error(), activities.ErrAgentKilled.Error())
}
