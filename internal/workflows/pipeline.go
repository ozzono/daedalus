// Package workflows defines the Temporal workflows that orchestrate a
// Daedalus feature-development pipeline.
package workflows

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
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
	// SharedTestQueue routes suite executions onto the fleet-shared
	// "test" queue when true, or onto this deployment's derived
	// <task_queue>-test queue when false (config shared_test_queue,
	// resolved at start). Nil — a run whose input predates the field,
	// replayed by a newer worker — means shared, the historical routing.
	SharedTestQueue *bool
	// Authorship commits daedalus's own work as "daedalus
	// <daedalus@local>" (config authorship). False — a run whose input
	// predates the field, or the default — leaves the commits to the
	// worker's git config.
	Authorship bool
	// TestOutputDir is the worktree-relative folder each suite run's
	// complete output is dumped into (config test_output, resolved at
	// start). Empty — dumping off, or a run whose input predates the
	// field — means no dump, byte-identical behavior. It rides the
	// pipeline input, not worker env: with a shared test queue a foreign
	// deployment's worker may run the suite, and env would apply the
	// wrong deployment's dir.
	TestOutputDir string
	// Folders are the run's granted host folders (`run -folder/--folder`,
	// plus the -f task file's folder; resolved to cleaned absolute paths at
	// submit). Each is mounted read-write into the implementing rounds'
	// sandbox under <worktree>/.daedalus-folders/<basename>
	// (activities.FolderMounts) and named in a fresh conversation's opening
	// prompt, so brief-mandated bookkeeping outside the repo is executable
	// by the run itself. A continued run keeps the aborted attempt's grants,
	// frozen at start like its write-scope policy; reviewer rounds carry
	// none by design. Empty — no grants, or a run whose input predates the
	// field — mounts nothing, replay-safe like every other input field.
	Folders []string
}

// maxConsecutiveTimeouts caps how many timed-out rounds in a row the
// pipeline absorbs before failing the run: recovery assumes the agent
// makes progress each round, and a run wedged at its ceiling every
// time would otherwise loop forever on the agent budget.
const maxConsecutiveTimeouts = 3

// maxVerdictlessReviews caps how many review rounds in a row may exit
// without any verdict marker before the run parks: a marker-less round
// still counts as changes requested (the standing contract), but a
// reviewer that never emits a verdict at all is failing infrastructure —
// e.g. pi folding every provider request at its request timeout — and the
// changes-requested equivalence would otherwise alternate dev/review
// rounds forever, never converging and never parking.
const maxVerdictlessReviews = 3

// maxIdenticalVerdicts caps how many review rounds in a row — per review
// role, so a code-review approval between a test reviewer's repeated
// REBUILD findings does not reset the count — may return a byte-identical
// comments body (whitespace-normalized) before the run parks: the timeout
// and verdictless streaks above do not cover a steady stream of well-formed
// non-approved verdicts, which is the one remaining unbounded review loop —
// an implementer that cannot act on the feedback at all (e.g. a small
// self-hosted model retrying a hallucinated edit forever) produces reviews
// that differ in no way round to round, so identical verdicts in a row mean
// the loop cannot converge and a maintainer must arbitrate. Only verdicts
// carrying a real marker count; marker-less rounds have
// maxVerdictlessReviews, and an approval ends that role's loop (and resets
// its streak) before it could ever park.
const maxIdenticalVerdicts = 3

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

// maxTestPhaseRebuilds caps how many REBUILD verdicts the green stage may
// absorb before parking the run: each rebuild cycle is a full suite run
// plus two agent rounds on the provider budget, and a loop that cannot
// converge within this many — even with every round legitimate — would
// otherwise cycle until provider quota death takes the deployment's other
// runs down with it. The count is monotonic (no reset when failures drop):
// with the rebuild batching demanding the whole failure inventory per
// cycle, a healthy convergence needs only a handful of cycles, so
// resetting could only ever serve a loop slow enough to be worth a
// maintainer's eyes anyway.
const maxTestPhaseRebuilds = 8

// ErrAwaitingMaintainer parks a run instead of failing it with a raw error:
// the API stayed exhausted past every heartbeat, or the reviewer halted with
// NEEDS_MAINTAINER on a task that cannot be completed as stated. The
// registration wraps every flow in CompleteGreen, so a parked run does not
// fail the workflow — it completes successfully with ParkedResult carrying
// the reason, and the orchestrator's history shows the run green (COMPLETED)
// with the reason in the completion payload, distinguishable from a genuine
// failure (which still fails the workflow). The attempt's work is already
// preserved on its aborted/ branch by the deferred cleanup, so
// `daedalus continue` restarts from it either way.
var ErrAwaitingMaintainer = errors.New("run parked awaiting maintainer restart")

// ParkedResultPrefix marks a completed workflow's result as a park: the
// registration's CompleteGreen wrapper turns a parked run's
// ErrAwaitingMaintainer failure into a successful completion whose result
// carries the marker plus the full reason, so the orchestrator's history
// records the run green with the park reason visible in the completion
// payload. Preserved branch names never carry the prefix, so the marker is
// unambiguous.
const ParkedResultPrefix = "PARKED: "

// ParkedResult renders a park's outcome as the completed workflow's result.
func ParkedResult(reason string) string { return ParkedResultPrefix + reason }

// IsParkedResult reports whether a completed workflow's result string is a
// park — the green completion CompleteGreen produces for ErrAwaitingMaintainer.
func IsParkedResult(result string) bool { return strings.HasPrefix(result, ParkedResultPrefix) }

// The run-visibility search attributes the pipeline upserts (registered on
// the namespace by `make custom-columns`): DaedalusStatus distinguishes a
// parked run from an approved one — both are Completed in ExecutionStatus —
// and waiting from running, while LastActivityAt carries the last completed
// round's time, the liveness signal a quota-heartbeat sleep lacks.
const (
	DaedalusStatusAttr = "DaedalusStatus"
	LastActivityAttr   = "LastActivityAt"
)

// RunStatus is the value space of the DaedalusStatus keyword attribute.
type RunStatus string

const (
	StatusRunning  RunStatus = "running"
	StatusWaiting  RunStatus = "waiting"
	StatusApproved RunStatus = "approved"
	StatusParked   RunStatus = "parked"
	StatusFailed   RunStatus = "failed"
)

// visibilityChangeID gates every upsert behind workflow.GetVersion: replay
// strictly matches upsert commands against recorded history events, so a
// worker upgraded mid-run would wedge every in-flight workflow with a
// nondeterminism failure if the new code emitted upserts unconditionally.
// GetVersion is memoized per execution, so calling it from both CompleteGreen
// and startRun is safe — the marker records once. Pre-marker replays get
// DefaultVersion and skip every upsert; old runs never backfill (history is
// immutable) and fall back to the system status.
const visibilityChangeID = "visibility-search-attrs"

func visibilityEnabled(ctx workflow.Context) bool {
	return workflow.GetVersion(ctx, visibilityChangeID, workflow.DefaultVersion, 1) == 1
}

func setRunStatus(ctx workflow.Context, s RunStatus) {
	workflow.UpsertSearchAttributes(ctx, map[string]interface{}{DaedalusStatusAttr: string(s)})
}

func touchLastActivity(ctx workflow.Context) {
	workflow.UpsertSearchAttributes(ctx, map[string]interface{}{LastActivityAttr: workflow.Now(ctx)})
}

// CompleteGreen wraps a flow function so a parked run completes the
// workflow successfully instead of failing it: an ErrAwaitingMaintainer
// error is logged with its reason and returned as ParkedResult, while every
// other error — a genuine failure — still fails the run, so the failure
// signal is not blanket-suppressed. The worker registers every flow through
// this wrapper under the wrapped function's own type name (WorkflowTypeName),
// keeping the workflow type every past run's history and every start agrees
// on unchanged.
func CompleteGreen(fn func(workflow.Context, PipelineInput) (string, error)) func(workflow.Context, PipelineInput) (string, error) {
	return func(ctx workflow.Context, input PipelineInput) (string, error) {
		// Every flow registers through this wrapper, so this is the one call
		// site whose event-stream position every flow replays identically —
		// the version marker is recorded here for the whole execution.
		vis := visibilityEnabled(ctx)
		branch, err := fn(ctx, input)
		if vis {
			// Terminal status stamped at the single choke point: parks and
			// approvals are both Completed in ExecutionStatus — that gap is
			// why the column exists.
			switch {
			case errors.Is(err, ErrAwaitingMaintainer):
				setRunStatus(ctx, StatusParked)
			case err != nil:
				setRunStatus(ctx, StatusFailed)
			default:
				setRunStatus(ctx, StatusApproved)
			}
		}
		if err == nil || !errors.Is(err, ErrAwaitingMaintainer) {
			return branch, err
		}
		workflow.GetLogger(ctx).Info("Run parked for the maintainer; completing green",
			"Reason", err.Error())
		return ParkedResult(err.Error()), nil
	}
}

// WorkflowTypeName returns the Temporal workflow type name the SDK derives
// for fn — the same runtime.FuncForPC short-naming RegisterWorkflow and
// ExecuteWorkflow apply by default — so a CompleteGreen-wrapped flow can be
// registered and started under the name its unwrapped form always carried.
func WorkflowTypeName(fn func(workflow.Context, PipelineInput) (string, error)) string {
	full := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	elements := strings.Split(full, ".")
	return strings.TrimSuffix(elements[len(elements)-1], "-fm")
}

// FeatureDevWorkflow drives a full issue-development cycle in two
// review-gated phases: (1) implementation ↔ code review until the reviewer
// approves, then (2) tests ↔ test review until the reviewer approves AND the
// native test suite passes. On success the approved work is committed and
// the run's branch renamed to its preserved prefix; the workflow
// returns that branch name. The loops run until approval with no cap on
// ordinary fix rounds — each round is durable, auditable, and individually
// timed-out via activity options — with one bound: the green stage parks
// the run after maxTestPhaseRebuilds REBUILD cycles instead of cycling on
// the provider budget forever. Conditions that park the run for a
// maintainer restart rather than failing it with a raw error (see
// ErrAwaitingMaintainer): the provider API staying exhausted past every
// quota heartbeat, a reviewer NEEDS_MAINTAINER verdict on a task that
// cannot be completed as stated, or the rebuild cap above. The
// registration's CompleteGreen wrapper completes a parked run green with
// the reason as its result; the deferred cleanup preserves the attempt's
// work on its aborted/ branch either way.
func FeatureDevWorkflow(ctx workflow.Context, input PipelineInput) (string, error) {
	run, cleanup := startRun(ctx, input)
	defer cleanup()
	// Submit-time jail carve-out: a task whose own text names .ai-jail
	// gets fix prompts where acting on .ai-jail comments is in scope, and
	// its phase-1 code review audits the jail spec (the flag is threaded
	// into run.review). Setting WorktreeInput.TaskTouchesJail is this
	// flow's opt-in to finalize's .ai-jail force-stage — feature-dev is
	// one of only two flows whose prompts unlock jail edits.
	touchesJail := template.TaskTouchesJail(input.Prompt)
	run.worktreeInput.TaskTouchesJail = touchesJail
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
			verdict, err := run.review("the implementation", "", false, false, "", activities.RoleDevReview, &run.devReviewSession, touchesJail)
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
			fixPrompt, err := template.ImplementFix(verdict.Comments, template.Jail{Touches: touchesJail})
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
		initialPrompt, err = template.Implement(input.Prompt, bugDir())
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
	testsPrompt, err := template.Tests(bugDir())
	if err != nil {
		return "", fmt.Errorf("build tests prompt: %w", err)
	}
	testerReply, err := run.runAgent(testsPrompt, "tests", activities.RoleTest, &run.testSession)
	if err != nil {
		return "", fmt.Errorf("test-phase agent run: %w", err)
	}
	rebuilds := 0
	for {
		command, err := run.resolveTestCommand()
		if err != nil {
			return "", err
		}
		result, err := run.runSuite(command)
		if err != nil {
			return "", err
		}
		verdict, err := run.review("the test suite", result.Logs, true, false, testerReply.Text, activities.RoleTestReview, &run.testReviewSession, false)
		if err != nil {
			return "", fmt.Errorf("test review: %w", err)
		}
		if verdict.NeedsMaintainer {
			run.logger.Info("Test review halted the run for maintainer input")
			return "", run.park(fmt.Sprintf("test review halted the run — the task cannot be completed as stated: %s",
				verdict.Comments))
		}
		if verdict.Rebuild {
			rebuilds++
			if rebuilds > maxTestPhaseRebuilds {
				run.logger.Info("Green stage exceeded the rebuild cap; parking the run")
				return "", run.park(fmt.Sprintf("green stage failed to converge — the test review issued rebuild %d and suite green and review approval never coincided; last finding: %s",
					rebuilds, verdict.Comments))
			}
			run.logger.Info("Test review requested a rebuild; returning to the dev cycle",
				"Rebuilds", rebuilds, "Of", maxTestPhaseRebuilds)
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
