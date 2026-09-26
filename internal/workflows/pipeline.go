// Package workflows defines the Temporal workflows that orchestrate a
// Daedalus feature-development pipeline.
package workflows

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/template"
)

// Flow is one entry of the CLI's workflow registry (cmd/daedalus): the
// workflow function plus the per-flow write-scope policy the run carries in
// its input and verifies structurally before finalize. Empty path sets mean
// unrestricted.
type Flow struct {
	Fn func(workflow.Context, PipelineInput) (string, error)
	// AllowedPaths: the run's diff may touch nothing outside these
	// patterns (investigate: docs; test-only: tests and testdata).
	AllowedPaths []string
	// FrozenPaths: the run's diff may touch nothing in these patterns
	// (refactor freezes the test suite it must keep green).
	FrozenPaths []string
}

// PipelineInput is the sole input to every registered workflow. It
// deliberately carries no credentials: the worker reads provider settings
// from its own environment (exported from config.yaml), keeping secrets out
// of workflow history.
type PipelineInput struct {
	RepoPath  string
	TaskQueue string
	IssueID   string
	Prompt    string
	// Flow names the workflow serving this run ("feature-dev",
	// "investigate", …). Empty — a run whose input predates the field,
	// replayed by a newer worker — means feature-dev.
	Flow string
	// AllowedPaths / FrozenPaths are the flow's write-scope policy,
	// verified structurally before finalize (see Flow). Empty — a run
	// whose input predates the policy — means unrestricted, exactly like
	// feature-dev.
	AllowedPaths []string
	FrozenPaths  []string
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
	// ReviewTimeout bounds one jailed reviewer round (config
	// review_timeout). Zero — a run whose input predates the field,
	// replayed by a newer worker — falls back to
	// config.DefaultReviewTimeout.
	ReviewTimeout time.Duration
	// CleanupTimeout bounds one CleanupWorktreeActivity (config
	// cleanup_timeout). Same replay-safe zero fallback as AgentRunTimeout:
	// config.DefaultCleanupTimeout.
	CleanupTimeout time.Duration
	// Authorship commits daedalus's own work as "daedalus
	// <daedalus@local>" (config authorship). False — a run whose input
	// predates the field, or the default — leaves the commits to the
	// worker's git config.
	Authorship bool
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
	run, cleanup := startRun(ctx, input)
	defer cleanup()
	if err := run.createWorktree(); err != nil {
		return "", err
	}
	if err := run.preFlightGate(); err != nil {
		return "", err
	}

	// codeReviewLoop runs implementation ↔ code-review rounds until the
	// code reviewer approves, parking on NEEDS_MAINTAINER. Both the first
	// pass and a test-phase REBUILD land here, so the rebuild re-enters the
	// same reviewer conversation instead of starting cold.
	codeReviewLoop := func() error {
		for {
			verdict, err := run.review("the implementation", "", false, false, "", activities.RoleDevReview, &run.devReviewSession)
			if err != nil {
				return fmt.Errorf("code review: %w", err)
			}
			if verdict.NeedsMaintainer {
				run.logger.Info("Code review halted the run for maintainer input")
				return run.park(fmt.Sprintf("code review halted the run — the task cannot be completed as stated: %s",
					verdict.Comments))
			}
			if verdict.Approved {
				run.logger.Info("Code review approved")
				return nil
			}
			run.logger.Info("Code review requested changes")
			fixPrompt, err := template.ImplementFix(verdict.Comments)
			if err != nil {
				return fmt.Errorf("build implement-fix prompt: %w", err)
			}
			if g := run.drainGuidance(); g != "" {
				run.logger.Info("Operator guidance received, folding into fix prompt")
				fixPrompt = g + "\n\n" + fixPrompt
			}
			if _, err := run.runAgent(fixPrompt, "implement-fix", activities.RoleDev, &run.devSession); err != nil {
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
		if _, err := run.runAgent(prompt, "rebuild", activities.RoleDev, &run.devSession); err != nil {
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
	if _, err := run.runAgent(initialPrompt, "implement", activities.RoleDev, &run.devSession); err != nil {
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
	testerReply, err := run.runAgent(testsPrompt, "tests", activities.RoleTest, &run.testSession)
	if err != nil {
		return "", fmt.Errorf("test-phase agent run: %w", err)
	}
	for {
		command, err := run.resolveTestCommand()
		if err != nil {
			return "", err
		}
		result, err := run.runSuite(command)
		if err != nil {
			return "", err
		}
		verdict, err := run.review("the test suite", result.Logs, true, false, testerReply.Text, activities.RoleTestReview, &run.testReviewSession)
		if err != nil {
			return "", fmt.Errorf("test review: %w", err)
		}
		if verdict.NeedsMaintainer {
			run.logger.Info("Test review halted the run for maintainer input")
			return "", run.park(fmt.Sprintf("test review halted the run — the task cannot be completed as stated: %s",
				verdict.Comments))
		}
		if verdict.Rebuild {
			run.logger.Info("Test review requested a rebuild; returning to the dev cycle")
			if err := rebuild(verdict.Comments); err != nil {
				return "", err
			}
			continue
		}
		if result.Passed && verdict.Approved {
			return run.finalize()
		}
		testFix := testFixPrompt(result, verdict)
		if g := run.drainGuidance(); g != "" {
			run.logger.Info("Operator guidance received, folding into fix prompt")
			testFix = g + "\n\n" + testFix
		}
		fixResult, err := run.runAgent(testFix, "tests-fix", activities.RoleTest, &run.testSession)
		if err != nil {
			return "", fmt.Errorf("test-fix agent run: %w", err)
		}
		testerReply = fixResult
	}
}

// preFlightGate resolves and runs the native suite on the untouched
// worktree before the first agent round. A red baseline parks the run —
// no session may start on a failing suite. Uses the same discovery and
// test-queue execution the test session uses. A discovery concluding the
// repository has no suite at all (activities.ErrNoSuite) passes vacuously:
// nothing can be red when nothing exists — a greenfield repo is not a red
// baseline — and the history records why no suite round ran.
func (r *pipelineRun) preFlightGate() error {
	command, err := r.resolveTestCommand()
	if err != nil {
		if isNoSuite(err) {
			r.logger.Info("No suite discovered; gate passes vacuously")
			return nil
		}
		return err
	}
	result, err := r.runSuite(command)
	if err != nil {
		return err
	}
	if result.Passed {
		r.logger.Info("Preflight suite green; opening the first session", "Command", command)
		return nil
	}
	r.logger.Info("Preflight suite failed; parking before the first session")
	return r.park(fmt.Sprintf("preflight gate failed — the suite must be green before a session starts: %s", command))
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

// isNoSuite reports whether err is the activities' ErrNoSuite. Crossing
// the worker→workflow boundary an activity error survives only as the
// application error's message text (see isAPIExhaustion), so a plain
// substring match covers every case.
func isNoSuite(err error) bool {
	return err != nil && strings.Contains(err.Error(), activities.ErrNoSuite.Error())
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
