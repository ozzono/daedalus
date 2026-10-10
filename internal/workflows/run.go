package workflows

import (
	"fmt"
	"os"
	"strings"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/template"
)

// bugDir reads the worker-exported out-of-scope-bug filing folder
// (DAEDALUS_BUG_DIR, exported at worker startup iff bug_filing is enabled;
// absent means off). Config values travel the same worker-process env
// channel they reach activities on — never workflow history or activity
// inputs — and the rendered prompt then becomes a recorded activity input:
// a replay on a differently-configured worker may re-render differently,
// but the recorded input is what executes, so nothing replays differently.
func bugDir() string { return os.Getenv(config.BugDirEnv) }

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
	// workflow's queue, testExecCtx runs suites and gates on the
	// deployment's suite queue (shared "test", or derived "<queue>-test").
	testTimeout time.Duration
	// agentRunTimeout is the run's resolved agent_run_timeout — the
	// ceiling named in a cut-off round's continuation prompt.
	agentRunTimeout time.Duration
	discoverCtx     workflow.Context
	testExecCtx     workflow.Context

	guideCh workflow.ReceiveChannel
	// wakeupCh carries `daedalus worker wakeup`: a signal that interrupts
	// the quota heartbeat's sleep so a recovered provider resumes the
	// round immediately. The channel has two consumers — the quota
	// heartbeat and awaitDependency's dependency gate (a poke ends that
	// wait immediately, granting its grace cadence) — and a signal
	// arriving mid-activity stays buffered, shortening the next wait of
	// whichever is sleeping.
	wakeupCh workflow.ReceiveChannel

	branchName    string
	worktreeInput activities.WorktreeInput
	worktree      activities.WorktreeOutput

	// cover, set by the test-only flow, asks suite executions to record Go
	// statement coverage (TestResult.Coverage) for the reviewer.
	cover bool

	// freshReviews, set by the slim flow, makes every review round a
	// completely fresh reviewer session: no session id is ever stored
	// back and the activity fires no recorded-session fallback, so no
	// review ever resumes another's conversation — the REVIEW role must
	// stay isolated from the WORKER role's accumulated context (a fresh
	// review of the current diff, every time). The role itself still
	// travels (reviewer endpoint, pi guardrail exclusion, tracking), and
	// the caps are unaffected (verdict parsing and the
	// identical-verdict/verdictless caps key on the role, not the
	// session).
	freshReviews bool
	// reviewCriteria, set per sub-task by the slim flow, travels to the
	// reviewer activity: a non-empty set switches the review prompt to
	// the atomic sub-task framing judged against exactly these
	// acceptance criteria.
	reviewCriteria []string

	// vis records whether this execution recorded the visibility version
	// marker; every setStatus/touch is a no-op without it, so a pre-marker
	// run replayed by an upgraded worker never emits a command its history
	// lacks.
	vis bool

	quotaHeartbeats     int
	consecutiveTimeouts int
	reviewTimeouts      int
	// verdictlessReviews counts consecutive review rounds that completed
	// without any verdict marker (see maxVerdictlessReviews). Only a
	// review carrying a real verdict resets it.
	verdictlessReviews int
	// lastVerdictBody and identicalVerdicts track consecutive review
	// rounds returning a whitespace-identical comments body, per review
	// role (see maxIdenticalVerdicts). Only real verdicts participate; an
	// approval from that role resets its streak. Per-role is what keeps
	// the rebuild cycle covered: a test reviewer repeating an identical
	// REBUILD finding has a code-review approval between its rounds, and
	// that other conversation's approval must not erase the churn count.
	lastVerdictBody   map[activities.SessionRole]string
	identicalVerdicts map[activities.SessionRole]int

	devSession, testSession             string
	devReviewSession, testReviewSession string

	// skillDevReviewSessions and skillTestReviewSessions chain each entry
	// of the run's review_skill_list to its own reviewer conversation —
	// one ping-pong per skill per phase, each resuming its own session
	// across rounds exactly like the default reviewer's. Indexed by skill
	// list position; sized at startRun and untouched (all empty) when the
	// run carries no skill list.
	skillDevReviewSessions  []string
	skillTestReviewSessions []string
}

// startRun builds the run's shared machinery and returns it with the
// worktree-cleanup function, which the caller must defer before creating
// the worktree: the defer has to be registered ahead of the create
// activity so that a cancellation during create still cleans up. The
// cleanup runs on a disconnected context so cancelling the workflow does
// not cancel the cleanup itself, and tolerates state that never came to
// exist. It deletes only the in-flight feat/ branch; approved work was
// renamed to the preserved prefix by FinalizeWorktreeActivity and survives.
// A run carrying DependsOn blocks here — ahead of the worktree, the single
// choke point every flow passes through — until its dependency resolves; a
// broken chain is the returned error, with nothing ever scheduled.
func startRun(ctx workflow.Context, input PipelineInput) (*pipelineRun, func(), error) {
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
	// the deployment's suite queue served by the agent-free test worker —
	// suite runtime answers only to tests_timeout, outside the
	// agent-slot semaphore, and Get still uses ctx so cancellation
	// propagates normally. The queue is the fleet-shared "test" when the
	// run's config shares suites (the default, and the pre-field
	// fallback), or the derived "<queue>-test" when it opted out — the
	// worker's pollers follow the same config, so a deployment never
	// schedules on a queue its own workers ignore.
	testTimeout := input.TestTimeout
	if testTimeout <= 0 {
		testTimeout = config.DefaultTestsTimeout
	}
	discoverCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: testTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	suiteQueue := config.ReservedTestTaskQueue
	if input.SharedTestQueue != nil && !*input.SharedTestQueue {
		suiteQueue = config.TestQueueFor(input.TaskQueue)
	}
	testExecCtx := workflow.WithTaskQueue(discoverCtx, suiteQueue)

	flowScope := FlowScope(input.Flow)
	// The in-flight branch carries the flow segment too: a flow-scoped run's
	// cleanup sweeps only its own flow's stale branches.
	branchScope := ""
	if flowScope != "" {
		branchScope = flowScope + "-"
	}
	r := &pipelineRun{
		input:             input,
		logger:            logger,
		ctx:               ctx,
		agentCtx:          agentCtx,
		reviewCtx:         reviewCtx,
		cleanupCtx:        cleanupCtx,
		testTimeout:       testTimeout,
		agentRunTimeout:   agentTimeout,
		discoverCtx:       discoverCtx,
		testExecCtx:       testExecCtx,
		lastVerdictBody:   map[activities.SessionRole]string{},
		identicalVerdicts: map[activities.SessionRole]int{},
		guideCh:           workflow.GetSignalChannel(ctx, "guide"),
		wakeupCh:          workflow.GetSignalChannel(ctx, "wakeup"),
		branchName:        fmt.Sprintf("feat/%sissue-%s-%d", branchScope, input.IssueID, workflow.Now(ctx).Unix()),
		// One reviewer conversation per skill review, per phase.
		skillDevReviewSessions:  make([]string, len(input.ReviewSkillList)),
		skillTestReviewSessions: make([]string, len(input.ReviewSkillList)),
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
		// TaskTouchesJail stays false here: force-staging .ai-jail at
		// finalize is opt-in per flow — only the workflows that unlock
		// jail edits (feature-dev, dev-only) set it, so every other
		// flow keeps the fail-safe discard of any out-of-scope jail
		// edit `git add -A` gives for free.
	}
	r.vis = visibilityEnabled(ctx)
	if input.DependsOn != "" {
		if err := r.awaitDependency(); err != nil {
			return nil, nil, err
		}
	}
	r.setStatus(StatusRunning)
	r.touch()
	return r, func() {
		// NewDisconnectedContext returns (Context, CancelFunc) — the second
		// value is not an error. Detach the cancel from this deferred func's
		// lifetime only after the cleanup activity has completed.
		dctx, cancel := workflow.NewDisconnectedContext(cleanupCtx)
		defer cancel()
		if err := workflow.ExecuteActivity(dctx, activities.CleanupWorktreeActivity, r.worktreeInput).Get(dctx, nil); err != nil {
			logger.Error("Failed to clean up worktree", "Error", err)
		}
	}, nil
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

// park returns the run's park error carrying the full reason: a reviewer
// NEEDS_MAINTAINER verdict, a structural gate the agent cannot satisfy, or
// anything else only a maintainer can resolve. The registration's
// CompleteGreen wrapper turns this error into a green completion whose
// result carries the reason (see ErrAwaitingMaintainer); the deferred
// cleanup has already preserved the attempt's work on its aborted/ branch,
// so `daedalus continue` restarts from it.
func (r *pipelineRun) park(reason string) error {
	return fmt.Errorf("%w: %s", ErrAwaitingMaintainer, reason)
}

// setStatus upserts the DaedalusStatus visibility attribute; a no-op
// without the version marker (see r.vis).
func (r *pipelineRun) setStatus(s RunStatus) {
	if !r.vis {
		return
	}
	setRunStatus(r.ctx, s)
}

// touch stamps the LastActivityAt visibility attribute at each completed
// round; a no-op without the version marker (see r.vis).
func (r *pipelineRun) touch() {
	if !r.vis {
		return
	}
	touchLastActivity(r.ctx)
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
	r.setStatus(StatusWaiting)
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
	r.setStatus(StatusRunning)
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
			Folders:      r.input.Folders,
		}).Get(r.ctx, &result)
		if err == nil {
			r.consecutiveTimeouts = 0
			r.quotaHeartbeats = 0
			r.touch()
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
			r.setStatus(StatusWaiting)
			if serr := workflow.Sleep(r.ctx, slotBackoffInterval); serr != nil {
				return activities.AgentRunResult{}, fmt.Errorf("slot backoff sleep (stage %q): %w", stage, serr)
			}
			r.setStatus(StatusRunning)
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
// continue, the verdict simply never arrived. touchesJail marks the
// phase-1 implementation code review of a task whose text names .ai-jail —
// the only review whose agent can act on jail findings (test, docs, bugfix,
// and refactor rounds pass false: their agents are barred from touching
// .ai-jail, so an audit mandate there would be a demand no round can
// satisfy). The trailing skill carries the skill-review entry, if any: at
// most one is meaningful (the last wins, like every attachment); empty
// keeps the round an ordinary default review.
func (r *pipelineRun) review(focus, testLogs string, testsInScope, reproInScope bool, agentReply string, role activities.SessionRole, session *string, touchesJail bool, skill ...string) (activities.ReviewResult, error) {
	skillEntry := ""
	if len(skill) > 0 {
		skillEntry = skill[len(skill)-1]
	}
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
			// Explicitly threaded from the workflow's submit-time
			// derivation — never the diff (template.TaskTouchesJail
			// documents why), never re-derived here.
			TaskTouchesJail: touchesJail,
			Agent:           r.input.Agent,
			SessionID:       *session,
			Role:            role,
			// Fresh-review mode (slim): the activity starts a brand-new
			// reviewer conversation every round — no session id is ever
			// stored back and no recorded-session fallback fires — while
			// the role itself still selects the configured reviewer
			// endpoint and keeps the pi edit guardrail excluded.
			FreshReview: r.freshReviews,
			// Slim's per-sub-task acceptance criteria; nil elsewhere.
			AcceptanceCriteria: r.reviewCriteria,
			// The skill-review round's entry, if any; empty elsewhere.
			SkillEntry: skillEntry,
		}).Get(r.ctx, &result)
		if err == nil {
			r.reviewTimeouts = 0
			r.quotaHeartbeats = 0
			r.touch()
			if !r.freshReviews {
				*session = result.SessionID
			}
			if result.NoVerdict {
				r.verdictlessReviews++
				// The reviewer's last non-empty line is what a fold and a
				// formatting miss share as their only trace here; without
				// it the workflow log cannot distinguish the two (full
				// output is only in the task log). Recorded untruncated.
				lastLine := ""
				for _, raw := range strings.Split(result.Comments, "\n") {
					if line := strings.TrimSpace(raw); line != "" {
						lastLine = line
					}
				}
				r.logger.Warn("Reviewer round ended without a verdict marker",
					"Focus", focus, "ConsecutiveVerdictless", r.verdictlessReviews,
					"Of", maxVerdictlessReviews, "LastLine", lastLine)
				if r.verdictlessReviews >= maxVerdictlessReviews {
					return result, r.park(fmt.Sprintf("reviewer exited without any verdict %d rounds in a row (focus %q; last line: %q) — either the reviewer's provider channel is folding before a verdict line (e.g. pi at its request timeout) or the review never emitted the exact marker line, and neither hypothesis is resolvable by the implementing agent",
						r.verdictlessReviews, focus, lastLine))
				}
				return result, nil
			}
			r.verdictlessReviews = 0
			if result.Approved {
				// The role's loop ends on approval — progress by definition.
				r.lastVerdictBody[role], r.identicalVerdicts[role] = "", 0
				return result, nil
			}
			// The identical-verdict streak, per role: a repeat of the last
			// real verdict's comments body from the same reviewer means the
			// implementer did not move that review an inch
			// (maxIdenticalVerdicts); any different verdict restarts the
			// count.
			if body := normalizedVerdictBody(result.Comments); body == r.lastVerdictBody[role] {
				r.identicalVerdicts[role]++
			} else {
				r.lastVerdictBody[role] = body
				r.identicalVerdicts[role] = 1
			}
			if r.identicalVerdicts[role] >= maxIdenticalVerdicts {
				return result, r.park(fmt.Sprintf("%d consecutive identical review verdicts (focus %q, role %s) — the implementing agent is not acting on the review comments, so the review loop cannot converge; a maintainer must arbitrate",
					r.identicalVerdicts[role], focus, role))
			}
			return result, nil
		}
		// Same kill-first precedence as the agent rounds: a killed
		// reviewer must not park in the quota heartbeat over output
		// text it printed before dying.
		killed := isAgentKilled(err)
		if !killed && isPromptOverflow(err) {
			// A prompt that cannot fit the serving model's context is a
			// static failure: the same prompt is re-sent by every retry,
			// so neither the fresh-session fallback below nor the quota
			// heartbeat can succeed — return instead of burning them.
			return result, fmt.Errorf("reviewer prompt cannot fit the serving model's context window (focus %q); the prompt size is static, so no retry can succeed: %w", focus, err)
		}
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
			r.setStatus(StatusWaiting)
			if serr := workflow.Sleep(r.ctx, slotBackoffInterval); serr != nil {
				return result, fmt.Errorf("slot backoff sleep (review %q): %w", focus, serr)
			}
			r.setStatus(StatusRunning)
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

// normalizedVerdictBody collapses whitespace in a review's comments body so
// two verdicts differing only in line wrapping count as identical (the
// identical-verdict park cap compares bodies, not bytes). Workflow-code
// string work is deterministic, so this is replay-safe.
func normalizedVerdictBody(comments string) string {
	return strings.Join(strings.Fields(comments), " ")
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
			// A discovery round is an agent round (same slot semaphore, same
			// quota absorption), so its completion refreshes activity like
			// any other.
			r.touch()
			return command, nil
		}
		if isSlotWait(err) {
			// Test-command discovery queues on the same semaphore as
			// every other jailed round and can give up on it too.
			r.logger.Warn("All jailed-agent slots busy; backing off before re-queuing test-command discovery",
				"Backoff", slotBackoffInterval)
			r.setStatus(StatusWaiting)
			if serr := workflow.Sleep(r.ctx, slotBackoffInterval); serr != nil {
				return "", fmt.Errorf("resolve test command: %w", serr)
			}
			r.setStatus(StatusRunning)
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
			OutputDir:    r.input.TestOutputDir,
		}).Get(r.ctx, &result)
	if err == nil {
		// A suite round that ran to completion resets the quota streak
		// like any other completed round — even a red one, since the
		// provider was reachable for it.
		r.quotaHeartbeats = 0
		r.touch()
		// The dump relay: wherever Logs already travels — every fix
		// prompt and reviewer round — a successful dump adds one line
		// naming the file (worktree-relative, so a jailed agent can open
		// it), even under the transport limit. Empty DumpPath (dumping
		// off, or the best-effort write failed) adds nothing.
		if result.DumpPath != "" {
			result.Logs += "\nfull suite output: " + result.DumpPath + "\n"
		}
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

// dependencyReleases reports whether a dependency that stopped in state —
// the gate's probe status, "parked" for a completed workflow carrying the
// park marker — releases the waiting run onto the fallback branch instead
// of failing it. The section must be in release posture (enabled, a
// fallback branch resolved at start — the zero value keeps the historical
// refusal, replay-safe) and the state in the skip set the section's flags
// select. A paused dependency is outside every skip set: it has not
// stopped, so the gate keeps waiting; a vanished id ("not found") is a
// broken submit, never a state to release past.
func dependencyReleases(d config.DependencyConfig, state string) bool {
	if !d.ReleasePosture() {
		return false
	}
	switch state {
	case "parked":
		return d.SkipParkedOrDefault()
	case "failed":
		return d.SkipFailed
	case "timed out":
		return d.SkipStuck
	case "canceled", "terminated":
		return d.SkipCanceled
	}
	return false
}

// awaitDependency is the dependency gate every flow inherits through
// startRun: the run — nothing scheduled yet, no worktree, no branch, no
// round — polls its dependency until the probe reports an approving
// terminal state, then injects the dependency's preserved branch as the
// worktree base (WorktreeInput.BaseBranch only — the input's own BaseBranch
// would flip the opening prompt to a continuation). A dependency that stops
// without approval fails the run before anything was started — an aborted
// chain has nothing to preserve, so the interrupt is a failure, not a park,
// and re-submitting the chain is a human decision — unless the dependency
// section releases that state (dependencyReleases), in which case the run
// starts anyway from the resolved fallback branch, logged loudly so the
// degraded chain is visible in history. A paused dependency is not a stop:
// the probe reports it non-terminal and the gate keeps waiting, with the
// unpause line logged once so a maintainer reading the log knows the run
// is held, not hung. Transient probe errors back off and retry, capped
// like the round timeouts: a permanently failing check (a worker too old
// to know the activity) must fail loudly instead of pending forever. The
// first probe races nothing — a dependency already approved at submit
// resolves here without a single sleep.
//
// Between probes the gate waits on its "wakeup" channel — the same one the
// quota heartbeat races — next to a long fallback timer: the dependency's
// CompleteGreen pokes its dependents the moment it reaches a terminal
// state, so an approved chain starts in seconds and the wait's history
// stays quiet, while an ungraceful dependency death (nothing left to poke)
// still releases — or breaks — the chain on the fallback. A poke
// necessarily precedes the completion event it announces, so the probe it
// triggers usually reads the dependency still running; the gate then
// re-probes at the grace cadence for a bounded stretch (maxPostPokeProbes)
// before surrendering to the fallback. Runs recorded by the old
// fixed-interval loop replay with it, per dependencyWakeupChangeID.
func (r *pipelineRun) awaitDependency() error {
	dep := r.input.DependsOn
	if r.vis {
		workflow.UpsertSearchAttributes(r.ctx, map[string]interface{}{DaedalusDependsOnAttr: dep})
	}
	r.setStatus(StatusPending)
	r.logger.Info("Waiting on dependency", "Dependency", dep)
	wakeup := dependencyWakeupEnabled(r.ctx)
	failures := 0
	pausedSeen := false
	// graceProbes is how many release-grace waits the gate still owes: a
	// poke grants maxPostPokeProbes of them, spent one per wait, so a poke
	// whose probe still finds the dependency running (its completion event
	// not yet visible) — or whose probe transiently errors — keeps the
	// grace cadence instead of dropping to the full fallback on the first
	// miss. Derived only from which selector branch fired, so it replays
	// exactly.
	graceProbes := 0
	for {
		var probe activities.DependencyProbe
		err := workflow.ExecuteActivity(r.ctx, activities.CheckDependencyActivityName, dep).Get(r.ctx, &probe)
		if err == nil {
			failures = 0
			switch {
			case probe.Completed && !IsParkedResult(probe.Result):
				// The wait ends here, exactly once: stamp the run's first
				// moment of real work so the list TIME cells bill runtime
				// from release, not from the dispatch that started the wait.
				if wakeup && r.vis {
					workflow.UpsertSearchAttributes(r.ctx, map[string]interface{}{DaedalusStartedAtAttr: workflow.Now(r.ctx)})
				}
				r.setStatus(StatusRunning)
				r.logger.Info("Dependency approved; starting from its preserved branch",
					"Dependency", dep, "Branch", probe.Result)
				r.worktreeInput.BaseBranch = probe.Result
				return nil
			case probe.Terminal:
				state := probe.Status
				if probe.Completed {
					state = "parked"
				}
				if dependencyReleases(r.input.Dependency, state) {
					r.setStatus(StatusRunning)
					r.logger.Info("Dependency stopped without approval; releasing onto the fallback branch",
						"Dependency", dep, "State", state, "FallbackBranch", r.input.Dependency.FallbackBranch)
					r.worktreeInput.BaseBranch = r.input.Dependency.FallbackBranch
					return nil
				}
				return fmt.Errorf("dependency %s %s — the chain is broken; this run failed before any worktree, branch, or round existed. Resolve the dependency (check the id against \"daedalus list\"; closed sessions resume with `daedalus continue`), then re-submit",
					dep, state)
			case probe.Status == "paused" && !pausedSeen:
				// Logged once, not per poll: the probe repeats every
				// depPollInterval and history does not need the same line
				// for hours.
				pausedSeen = true
				r.logger.Info("dependency paused: the gate waits; unpause with `temporal workflow unpause`",
					"Dependency", dep)
			}
		} else {
			failures++
			if failures >= maxConsecutiveTimeouts {
				return fmt.Errorf("dependency check for %s failed %d probes in a row: %w", dep, failures, err)
			}
			r.logger.Warn("Dependency probe failed; retrying after the wait interval",
				"Dependency", dep, "Failures", failures, "Error", err)
		}
		if !wakeup {
			if serr := workflow.Sleep(r.ctx, depPollInterval); serr != nil {
				return fmt.Errorf("dependency poll (%s): %w", dep, serr)
			}
			continue
		}
		// Both branches are single history events, replay-safe like the
		// quota heartbeat's selector. A wakeup arriving mid-probe stays
		// buffered and only shortens the next wait; the timer branch just
		// waits out whatever cadence was chosen below.
		wait := depFallbackInterval
		if graceProbes > 0 {
			wait = depReleaseGrace
			graceProbes--
		}
		sel := workflow.NewSelector(r.ctx)
		sel.AddFuture(workflow.NewTimer(r.ctx, wait), func(workflow.Future) {})
		sel.AddReceive(r.wakeupCh, func(c workflow.ReceiveChannel, more bool) {
			var woken string
			c.Receive(r.ctx, &woken)
			graceProbes = maxPostPokeProbes
		})
		sel.Select(r.ctx)
	}
}
