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

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
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
	// BaselineValidated, set by `daedalus continue` when the attempt
	// chain's history shows a green native-suite execution — this link's
	// own (the preflight gate's pass, or a green suite round after it) or
	// an earlier link's flag threaded forward — is the only thing
	// preFlightGate skips on. False — a fresh run, a run whose input
	// predates the field, or the continue of a chain that has never
	// recorded one (parked by the gate, died at or before it, started
	// before the gate existed) — runs the gate: replay-safe in the loud
	// direction, like every zero-value fallback here.
	BaselineValidated bool
	// SkipPreflight, set by `daedalus run --skip-preflight` on a fresh
	// run, downgrades the preflight gate's red-baseline park to a recorded
	// pass: the suite still runs and its red result stays on record (the
	// task log's suite block, the gate's history line), but the run opens
	// on it deliberately — a red baseline is the run's stated premise, and
	// the flow's own suite gating — whatever rounds it takes to reach the
	// green finalize round — is the expected route for the fix it will
	// call for. Finalize is untouched:
	// the done path still requires a green suite round, so baseline debt
	// cannot finalize. False — every run before the field existed, and
	// every run without the flag — keeps the gate's park. The decision is
	// made once, at the fresh run's start, and travels with the run:
	// continue threads the recorded flag into the restarted workflow like
	// its other frozen-at-start policy fields, so a skip chain never
	// re-gates on the baseline it opened on; a pre-field attempt's false
	// re-runs the gate, the loud direction.
	SkipPreflight bool
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
	// by the run itself. A grant inside the repo never reaches this field:
	// the submit-time split (runstart.scopeFolderGrants) turns it into an
	// AllowedPaths extension instead — the worktree already carries its
	// files, so mounting the host copy would bypass the worktree commit
	// model. A continued run keeps the aborted attempt's grants,
	// frozen at start like its write-scope policy; reviewer rounds carry
	// none by design. Empty — no grants, or a run whose input predates the
	// field — mounts nothing, replay-safe like every other input field.
	Folders []string
	// DependsOn chains this run behind another (`run -dep/--depends`,
	// fresh runs only): the run waits — in the pending status, before any
	// worktree, branch, or round exists — until the dependency reaches an
	// approving terminal state, then starts its worktree from the
	// dependency's preserved branch. The base is injected into
	// WorktreeInput.BaseBranch by the gate, never into this struct's
	// BaseBranch: that one would flip the opening prompt to
	// template.Continue, but a dependent is a fresh Implement task. The
	// dependency stopping without approval (failed, parked, canceled,
	// timed out, wiped, never existed) fails the run before anything was
	// started. Empty — no chain, or a run whose input predates the field —
	// skips the gate entirely, replay-safe like every other input field.
	DependsOn string
	// ReviewSkillList is the ordered list of extra skill reviews each
	// review-gated phase of the feature-dev flow appends after its default
	// reviewer approves (config review_skill_list, resolved at submit).
	// One ping-pong per entry, in order — the code phase routes each
	// skill's feedback through the dev session, the test phase through the
	// tester's — each with its own reviewer conversation, the same verdict
	// protocol, and the same park and loop caps as the default reviewer; a
	// test-phase skill may verdict REBUILD like the default reviewer. A
	// "/name" entry resolves to a skill file at round time (the worktree's
	// .claude/skills — project wins — then the worker's ~/.claude/skills);
	// any other entry is hand-written prompt text, passed through
	// verbatim. Empty — no list, or a run whose input predates the field —
	// runs exactly the historical default-only reviews, replay-safe like
	// every other input field.
	ReviewSkillList []string
	// Dependency carries the config dependency section, resolved at start
	// (cmd.startPipelineFolders fills FallbackBranch with the invocation
	// branch when the section is enabled and the key unset). The gate reads
	// it to release a run past a dependency that stopped without approval
	// (dependencyReleases); the fallback lands in WorktreeInput.BaseBranch,
	// same as an approving dependency's preserved branch. The zero value —
	// a run whose input predates the field, a run with no -dep, a section
	// left disabled, or a release that never resolved a branch — keeps the
	// historical refusal on every broken chain: Enabled nil reads true, but
	// the empty FallbackBranch refuses, replay-safe in the loud direction
	// like every zero-value fallback here.
	Dependency config.DependencyConfig
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

// confidentVerdictlessSkip is the repair layer's classification confidence
// at or above which a verdictless review round counts no strike (the
// 2026-10-14 maintainer decision): a classification that confident names
// the verdict the reviewer intended, so the round is a real review with a
// missed marker — the classified verdict and comments deliver to the dev
// exactly as a parsed round's would. It matches the repair layer's own
// promotion threshold (activities.repairPromotionThreshold) in value, not
// in contract: that one promotes an APPROVED classification, this one only
// spares the strike budget.
const confidentVerdictlessSkip = 0.9

// unchangedDiffNote is the re-prompt appended to a review round's comments
// when the worktree's diff is unchanged since the reviewer's previous
// round (the swift path carries it under the standing verdict; the
// unchanged verdictless continue appends it to the marker-less round's
// output). The re-prompt IS the changes-request: the dev is told that
// changes are still requested and that nothing it produced has landed.
const unchangedDiffNote = "\n\n---\nDaedalus note: the worktree has not changed since this reviewer's previous round — nothing produced since has landed. Changes are still requested: the comments above remain the open change requests, and the next round must show them addressed in the diff."

// maxIdenticalVerdicts caps how many review rounds in a row — per
// reviewer (reviewerKey: role plus skill entry), so a code-review
// approval between a test reviewer's repeated REBUILD findings does not
// reset the count, and a skill's standing verdict never answers the
// default reviewer's round — may return a byte-identical
// comments body (whitespace-normalized) before the run parks: the timeout
// and verdictless streaks above do not cover a steady stream of well-formed
// non-approved verdicts, which is the one remaining unbounded review loop —
// an implementer that cannot act on the feedback at all (e.g. a small
// self-hosted model retrying a hallucinated edit forever) produces reviews
// that differ in no way round to round, so identical verdicts in a row mean
// the loop cannot converge and a maintainer must arbitrate. Only verdicts
// carrying a real marker count; marker-less rounds have
// maxVerdictlessReviews, and an approval ends that reviewer's loop (and
// resets its streak) before it could ever park.
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

// depPollInterval is the legacy dependency-gate wait: how often a
// dependent run's gate re-probed its dependency (awaitDependency) before
// the poke-when-done shape. Frozen at the historical minute — a run whose
// history was recorded by the old loop replays with this exact timer, so
// the value can never change again; the live gate waits on
// depFallbackInterval instead.
const depPollInterval = time.Minute

// depFallbackInterval is the poke-when-done gate's fallback wait: how long
// awaitDependency sleeps when no poke arrived. The poke (CompleteGreen's
// NotifyDependentsActivity) releases the chain within seconds of the
// dependency completing, so the fallback only serves ungraceful dependency
// deaths — no wrapper survived to ring — and a half hour of lag there is
// invisible next to the event volume the old minute poll cost.
const depFallbackInterval = 30 * time.Minute

// depReleaseGrace is the short re-probe delay a gate rides after a poke:
// the poke is sent from the dependency's own workflow code, so it must
// precede the completion event the approving probe reads — the probe the
// poke triggers usually finds the dependency still running, and the grace
// probes that follow catch the settled state seconds later.
const depReleaseGrace = 10 * time.Second

// maxPostPokeProbes bounds what a single poke buys: that many grace-wait
// re-probes before the gate surrenders to the full fallback. Without the
// bound, one miss — a completion event still not visible after the first
// grace probe — would drop the wait to the whole fallback on the very next
// round, a worse worst case than the legacy minute poll; with it, a poked
// gate keeps probing at the grace cadence for
// maxPostPokeProbes × depReleaseGrace (~100 seconds) before giving up.
const maxPostPokeProbes = 10

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
// DaedalusDependsOn names a dependent run's dependency workflow id, upserted
// once when the dependency gate engages. DaedalusStartedAt is the run's
// first moment of real work — the gate's release on an approved dependency —
// which the list rendering prefers over the dispatch time so a dependency
// wait is not billed as runtime.
const (
	DaedalusStatusAttr    = "DaedalusStatus"
	LastActivityAttr      = "LastActivityAt"
	DaedalusDependsOnAttr = "DaedalusDependsOn"
	DaedalusStartedAtAttr = "DaedalusStartedAt"
)

// RunStatus is the value space of the DaedalusStatus keyword attribute.
type RunStatus string

const (
	StatusRunning RunStatus = "running"
	StatusWaiting RunStatus = "waiting"
	// pending is a dependent run's live wait for its dependency (the gate
	// in awaitDependency): no work exists yet, nothing is retrying.
	// waiting stays the slot-backoff pause of a run whose rounds are
	// already underway — and, on workers predating the starved stamp, the
	// quota heartbeat's wait too.
	StatusPending RunStatus = "pending"
	// starved is the quota heartbeat's wait: the run's provider API is
	// exhausted (rate limit, cap, overload) and the round retries on the
	// hourly heartbeat — a provider-starved crawl, not a wedge, and the
	// stamp is what lets `daedalus report` and the runs table say so
	// instead of leaving the run looking like a stale heartbeat.
	StatusStarved  RunStatus = "starved"
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

// dependencyWakeupChangeID gates the dependency gate's second generation —
// the poke-when-done wait shape, the DaedalusStartedAt stamp at gate
// release, and the CompleteGreen poke — behind its own version marker, for
// the same replay reason as visibilityChangeID: a run whose history was
// recorded while the gate still slept the fixed poll interval must replay
// with that loop and without the release stamp, or the new commands wedge
// it with a nondeterminism failure at the next worker upgrade.
const dependencyWakeupChangeID = "dependency-gate-wakeup"

func dependencyWakeupEnabled(ctx workflow.Context) bool {
	return workflow.GetVersion(ctx, dependencyWakeupChangeID, workflow.DefaultVersion, 1) == 1
}

// swiftReviewChangeID gates the unchanged-diff swift review's diff probe
// behind its own version marker, for the same replay reason as the
// markers above: the probe dispatches a DiffDigestActivity command, and a
// review round replayed from history recorded before the marker must keep
// issuing exactly the commands that history holds — a pre-marker replay
// gets DefaultVersion and never probes, so those rounds run a full
// reviewer every time; old runs never backfill.
const swiftReviewChangeID = "swift-review-change-probe"

func swiftReviewProbeEnabled(ctx workflow.Context) bool {
	return workflow.GetVersion(ctx, swiftReviewChangeID, workflow.DefaultVersion, 1) == 1
}

// preflightSkipChangeID gates the preflight gate's BaselineValidated skip
// behind its own version marker, for the same replay reason as the two
// markers above: a continued run whose history was recorded before the
// skip (or before the field it keys on) carries the gate's suite execution
// as a recorded command, and replay must keep issuing that command — a
// pre-marker replay gets DefaultVersion, disabling the skip, so the gate
// re-executes the suite exactly as the history recorded it. Post-marker
// replays skip, and any future skip-shaped change must land after this
// marker (or gated behind another of its own) so the marker-carrying
// histories in flight at that release replay coherently. The skip windows
// already shipped unversioned before this marker are outside its reach —
// no marker can distinguish them after the fact.
const preflightSkipChangeID = "preflight-baseline-validated-skip"

func baselineValidatedSkipEnabled(ctx workflow.Context) bool {
	return workflow.GetVersion(ctx, preflightSkipChangeID, workflow.DefaultVersion, 1) == 1
}

// starvedStatusChangeID gates the quota heartbeat's distinct run status
// behind its own version marker, for the same replay reason as the
// markers above: heartbeat upserts are recorded history events, and
// replaying a pre-marker run with "starved" where history recorded
// "waiting" would issue a different command and wedge the run at the
// next worker upgrade. Pre-marker replays keep stamping waiting.
const starvedStatusChangeID = "starved-run-status"

func starvedStatusEnabled(ctx workflow.Context) bool {
	return workflow.GetVersion(ctx, starvedStatusChangeID, workflow.DefaultVersion, 1) == 1
}

// suiteFreshnessChangeID gates the terminal-state suite-freshness gate
// behind its own version marker: the gate emits activity commands at
// park and approve (a forced suite run, or a re-entry into the test
// phase), and a pre-marker run replayed by an upgraded worker would be
// wedged by commands its history lacks. Pre-marker replays reach their
// terminal states ungated, exactly as their history recorded them.
const suiteFreshnessChangeID = "terminal-suite-freshness-gate"

func suiteFreshnessEnabled(ctx workflow.Context) bool {
	return workflow.GetVersion(ctx, suiteFreshnessChangeID, workflow.DefaultVersion, 1) == 1
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
		// the version markers are recorded here for the whole execution.
		vis := visibilityEnabled(ctx)
		wakeup := dependencyWakeupEnabled(ctx)
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
		if wakeup {
			// Poke the dependents waiting on this run — approved and parked
			// terminals alike (a parked dependency still breaks the chain,
			// and the dependent learns it in seconds instead of at the
			// fallback poll). Best-effort: a failed poke leaves the gate's
			// fallback wait as the release path, so the completion must not
			// fail over it. This run's own result is unchanged by the poke.
			nctx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 5 * time.Minute,
				RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
			})
			var poked int
			if nerr := workflow.ExecuteActivity(nctx, activities.NotifyDependentsActivityName,
				workflow.GetInfo(ctx).WorkflowExecution.ID).Get(nctx, &poked); nerr != nil {
				workflow.GetLogger(ctx).Warn("Could not poke dependents; they release on the fallback poll",
					"Error", nerr)
			} else if poked > 0 {
				workflow.GetLogger(ctx).Info("Poked dependents waiting on this run", "Dependents", poked)
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

// skillLabel names a review_skill_list entry for logs and park reasons: a
// /name entry by its name, a hand-written entry by its position — the
// hand-written text itself can be a paragraph, no place for a log line.
func skillLabel(i int, entry string) string {
	if name, ok := strings.CutPrefix(entry, "/"); ok && strings.TrimSpace(name) != "" {
		return strings.TrimSpace(name)
	}
	return fmt.Sprintf("#%d", i+1)
}

// devFixRound feeds one round of review comments back to the dev
// session — the default reviewer's and each skill reviewer's fixes ride
// the same implementing conversation.
func (r *pipelineRun) devFixRound(comments string, touchesJail bool) error {
	fixPrompt, err := template.ImplementFix(comments, template.Jail{Touches: touchesJail})
	if err != nil {
		return fmt.Errorf("build implement-fix prompt: %w", err)
	}
	if g := r.drainGuidance(); g != "" {
		r.logger.Info("Operator guidance received, folding into fix prompt")
		fixPrompt = g + "\n\n" + fixPrompt
	}
	if _, err := r.runAgent(fixPrompt, "implement-fix", activities.RoleDev, &r.devSession); err != nil {
		return fmt.Errorf("agent run after code review: %w", err)
	}
	return nil
}

// taskFocus names a dev review's focus: what is under review plus the
// run's own task text — the review prompt's only copy of what was asked,
// and the anchor its completeness contract enumerates change requests
// from.
func (r *pipelineRun) taskFocus(what string) string {
	return fmt.Sprintf("%s. The task:\n\n%s", what, r.input.Prompt)
}

// codeReviewLoop runs implementation ↔ code-review rounds until the code
// reviewer approves, parking on NEEDS_MAINTAINER — then one ping-pong per
// review_skill_list entry, in order, each a reviewer conversation of its
// own riding the same dev session for its fixes. The phase's gate is the
// whole chain: every skill must approve too. Feature-dev's first pass and
// the test-only flow's post-rebuild review land here, so a rebuild
// re-enters the same reviewer conversations instead of starting cold.
func (r *pipelineRun) codeReviewLoop(touchesJail bool) error {
	for {
		verdict, err := r.review(reviewOpts{focus: r.taskFocus("the implementation"), agentReply: r.devReply, role: activities.RoleDevReview, session: &r.devReviewSession, touchesJail: touchesJail})
		if err != nil {
			return fmt.Errorf("code review: %w", err)
		}
		if verdict.NeedsMaintainer {
			r.logger.Info("Code review halted the run for maintainer input")
			return r.park(fmt.Sprintf("code review halted the run — the task cannot be completed as stated: %s",
				verdict.Comments))
		}
		if verdict.Approved {
			r.logger.Info("Code review approved")
			break
		}
		r.logger.Info("Code review requested changes")
		if err := r.devFixRound(verdict.Comments+verdictNote(verdict), touchesJail); err != nil {
			return err
		}
	}
	for i, entry := range r.input.ReviewSkillList {
		label := skillLabel(i, entry)
		for {
			verdict, err := r.review(reviewOpts{focus: r.taskFocus("the implementation"), agentReply: r.devReply, role: activities.RoleDevReview, session: &r.skillDevReviewSessions[i], touchesJail: touchesJail, skill: entry})
			if err != nil {
				return fmt.Errorf("skill review %s: %w", label, err)
			}
			if verdict.NeedsMaintainer {
				r.logger.Info("Skill review halted the run for maintainer input")
				return r.park(fmt.Sprintf("skill review (%s) halted the run — the task cannot be completed as stated: %s",
					label, verdict.Comments))
			}
			if verdict.Approved {
				r.logger.Info("Skill review approved", "Skill", label)
				break
			}
			r.logger.Info("Skill review requested changes", "Skill", label)
			if err := r.devFixRound(verdict.Comments+verdictNote(verdict), touchesJail); err != nil {
				return err
			}
		}
	}
	return nil
}

// rebuildRound spends one REBUILD against the test phase's shared cap
// (maxTestPhaseRebuilds): a dev-role agent run on the finding — the one
// dev-code path a test-phase flow carries deliberately — then the full
// code review loop gates the rebuilt state before the caller's test loop
// resumes. That gate is codeReviewLoop whole: a flow whose other reviews
// are skill-free (test-only) still re-enters the feature-dev chain
// review_skill_list included, because the rebuild is where the run's
// implementation-review budget is spent — on the path that writes
// application code. reply is the dev round's answer; both callers re-point
// their latest-reply tracking at it, since the test reviewer must next see
// the round that produced the change it is looking at, not the
// pre-rebuild tester. restart reports whether the test phase must start
// over; false is a park (cap spent) or a failed round — err is the
// workflow's return, never dropped.
func (r *pipelineRun) rebuildRound(finding string, touchesJail bool) (reply activities.AgentRunResult, restart bool, err error) {
	r.testRebuilds++
	if r.testRebuilds > maxTestPhaseRebuilds {
		r.logger.Info("Test phase exceeded the rebuild cap; parking the run")
		return activities.AgentRunResult{}, false, r.park(fmt.Sprintf("test phase failed to converge — the test review issued rebuild %d and suite green and review approval never coincided; last finding: %s",
			r.testRebuilds, finding))
	}
	r.logger.Info("Test review requested a rebuild; returning to the dev cycle",
		"Rebuilds", r.testRebuilds, "Of", maxTestPhaseRebuilds)
	prompt, err := template.Rebuild(finding)
	if err != nil {
		return activities.AgentRunResult{}, false, fmt.Errorf("build rebuild prompt: %w", err)
	}
	reply, err = r.runAgent(prompt, "rebuild", activities.RoleDev, &r.devSession)
	if err != nil {
		return activities.AgentRunResult{}, false, fmt.Errorf("rebuild agent run: %w", err)
	}
	if err := r.codeReviewLoop(touchesJail); err != nil {
		return activities.AgentRunResult{}, false, fmt.Errorf("post-rebuild code review: %w", err)
	}
	return reply, true, nil
}

// FeatureDevWorkflow drives a full issue-development cycle in two
// review-gated phases: (1) implementation ↔ code review until the reviewer
// approves, then (2) tests ↔ test review until the reviewer approves AND the
// native test suite passes. When the run carries a review_skill_list, each
// phase's gate extends to one more ping-pong per listed skill after the
// default reviewer approves — the code phase routing a skill's feedback
// through the dev session, the test phase through the tester's, both with
// the same verdict protocol, park handling, and loop caps. On success the
// approved work is committed and
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
	run, cleanup, err := startRun(ctx, input)
	if err != nil {
		return "", err
	}
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
	if _, err := run.preFlightGate(); err != nil {
		return "", err
	}

	// codeReviewLoop runs implementation ↔ code-review rounds until the
	// code reviewer approves — then one ping-pong per review_skill_list
	// entry, in order. Both the first pass and a test-phase REBUILD land
	// here, so the rebuild re-enters the same reviewer conversations
	// instead of starting cold.
	codeReviewLoop := func() error {
		return run.codeReviewLoop(touchesJail)
	}

	// Phase 1: implementation ↔ code review, until the reviewer approves.
	// A continued run opens on the aborted attempt's preserved work.
	var initialPrompt string
	if input.BaseBranch != "" {
		initialPrompt, err = template.Continue(input.Prompt, input.PriorFeedback, true)
	} else {
		initialPrompt, err = template.Implement(input.Prompt, bugDir(), true)
	}
	if err != nil {
		return "", fmt.Errorf("build implement prompt: %w", err)
	}
	implReply, err := run.runAgent(initialPrompt, "implement", activities.RoleDev, &run.devSession)
	if err != nil {
		return "", fmt.Errorf("initial agent run: %w", err)
	}
	// A task that is entirely test-scoped work arrives through the dev
	// session's hand-off marker (implement.md / continue.md): no
	// implementation edit was fabricated, and the code reviewer confirms
	// the claim before the run advances into phase 2 — where the work
	// belongs, and where the tester's opener carries the task text, since
	// there is no implementation diff to infer it from. A rejected claim
	// falls into the normal code-review loop: the reviewer's findings
	// route back through the fix rounds as ever. Parks stay reserved for
	// NEEDS_MAINTAINER.
	handoff := false
	if template.IsTestOnlyHandoff(implReply.Text) {
		run.logger.Info("Dev session declared the task test-only; asking the code reviewer to confirm")
		focus := fmt.Sprintf("the implementing session's test-only hand-off claim. The task:\n\n%s\n\nThe implementing session reports the task is entirely test-scoped work: no implementation change is needed, the diff is empty by design, and the work belongs in the pipeline's test phase. Verify the claim against the repository: if an implementation change is in fact needed for this task, request changes and say exactly what it is; if the task genuinely needs only tests, approve the hand-off.", input.Prompt)
		verdict, err := run.review(reviewOpts{focus: focus, role: activities.RoleDevReview, session: &run.devReviewSession, touchesJail: touchesJail, claimReview: true})
		if err != nil {
			return "", fmt.Errorf("test-only hand-off review: %w", err)
		}
		if verdict.NeedsMaintainer {
			run.logger.Info("Code review halted the run for maintainer input")
			return "", run.park(fmt.Sprintf("code review halted the run — the task cannot be completed as stated: %s",
				verdict.Comments))
		}
		handoff = verdict.Approved
		if handoff {
			run.logger.Info("Test-only hand-off confirmed; advancing to the test phase")
		} else {
			// A rejected hand-off is a review verdict like any other: the
			// reviewer's findings say exactly what implementation change is
			// missing, and they ride the ordinary fix round into the dev
			// session — the edit that produces puts a real diff under the
			// code review loop below. Entering that loop without the fix
			// round would open a default review on the still-empty diff,
			// whose blind-collection contract parks the run instead of
			// acting on the findings.
			run.logger.Info("Code review rejected the test-only hand-off; feeding the findings to the dev session")
			if err := run.devFixRound(verdict.Comments+verdictNote(verdict), touchesJail); err != nil {
				return "", err
			}
		}
	}
	if !handoff {
		if err := codeReviewLoop(); err != nil {
			return "", err
		}
	}

	// Phase 2: tests ↔ test review, until the reviewer approves AND the
	// native suite passes. The tester's latest reply travels to its
	// reviewer, which alone can verdict REBUILD — routing an
	// implementation-level finding back through the dev cycle (above)
	// before the test loop resumes with both sessions intact.
	var testsPrompt string
	if handoff {
		testsPrompt, err = template.TestsForTask(input.Prompt, bugDir())
	} else {
		testsPrompt, err = template.Tests(bugDir())
	}
	if err != nil {
		return "", fmt.Errorf("build tests prompt: %w", err)
	}
	testerReply, err := run.runAgent(testsPrompt, "tests", activities.RoleTest, &run.testSession)
	if err != nil {
		return "", fmt.Errorf("test-phase agent run: %w", err)
	}

	// suiteRound resolves the entrypoint and executes the suite once —
	// the ground truth a review round is put against.
	suiteRound := func() (activities.TestResult, error) {
		command, err := run.resolveTestCommand()
		if err != nil {
			return activities.TestResult{}, err
		}
		return run.runSuite(command)
	}

	// reviewSuite puts one reviewer on an already-collected suite result:
	// the default reviewer when skillIdx is negative, that skill's
	// reviewer otherwise (its conversation, its entry's resolved
	// instructions).
	reviewSuite := func(result activities.TestResult, skillIdx int) (activities.ReviewResult, error) {
		session := &run.testReviewSession
		entry := ""
		if skillIdx >= 0 {
			session = &run.skillTestReviewSessions[skillIdx]
			entry = run.input.ReviewSkillList[skillIdx]
		}
		return run.review(reviewOpts{focus: "the test suite", testLogs: result.Logs, testsInScope: true, agentReply: testerReply.Text, role: activities.RoleTestReview, session: session, skill: entry})
	}

	// testFixRound feeds a failed round back to the tester: the red suite
	// output, the review comments, or both — the default reviewer's and
	// each skill reviewer's fixes ride the same tester conversation.
	testFixRound := func(result activities.TestResult, verdict activities.ReviewResult) error {
		testFix := testFixPrompt(result, verdict)
		if g := run.drainGuidance(); g != "" {
			run.logger.Info("Operator guidance received, folding into fix prompt")
			testFix = g + "\n\n" + testFix
		}
		fixResult, err := run.runAgent(testFix, "tests-fix", activities.RoleTest, &run.testSession)
		if err != nil {
			return fmt.Errorf("test-fix agent run: %w", err)
		}
		testerReply = fixResult
		return nil
	}

	// rebuildRound spends one REBUILD against the test phase's shared cap
	// (pipelineRun.rebuildRound): the dev round rides the dev session, the
	// whole code gate re-reviews its change, and the tester's latest-reply
	// tracking moves to the rebuild round — the reviewer's next look must
	// describe the change it is reviewing.
	rebuildRound := func(finding string) (restart bool, err error) {
		reply, restart, err := run.rebuildRound(finding, touchesJail)
		if !restart {
			return false, err
		}
		testerReply = reply
		return true, nil
	}

	// The gate is green suite + every reviewer's approval: the default
	// reviewer first, then one ping-pong per review_skill_list entry in
	// order, each able to park, request test fixes, or verdict REBUILD
	// exactly like the default reviewer — a skill's rebuild re-enters the
	// whole phase at the top (fresh suite, default reviewer first), the
	// same "re-enter this test phase once it approves" semantics. A
	// skill's first round reviews the suite result already in hand: no
	// agent round ran since it was collected, so the worktree cannot have
	// changed, a re-run is pure spend, and on a flaky suite it could show
	// the skill a failure the diff did not cause. Only a fix round, which
	// moves the tree, triggers a fresh execution.
testPhase:
	for {
		result, err := suiteRound()
		if err != nil {
			return "", err
		}
		verdict, err := reviewSuite(result, -1)
		if err != nil {
			return "", fmt.Errorf("test review: %w", err)
		}
		if verdict.NeedsMaintainer {
			run.logger.Info("Test review halted the run for maintainer input")
			return "", run.park(fmt.Sprintf("test review halted the run — the task cannot be completed as stated: %s",
				verdict.Comments))
		}
		if verdict.Rebuild {
			restart, rerr := rebuildRound(verdict.Comments)
			if !restart {
				return "", rerr
			}
			continue
		}
		if result.Passed && verdict.Approved {
			for i, entry := range run.input.ReviewSkillList {
				label := skillLabel(i, entry)
				for first := true; ; first = false {
					if !first {
						// A fix round moved the tree: this skill's next
						// round reviews a fresh suite execution.
						result, err = suiteRound()
						if err != nil {
							return "", err
						}
					}
					verdict, err := reviewSuite(result, i)
					if err != nil {
						return "", fmt.Errorf("test skill review %s: %w", label, err)
					}
					if verdict.NeedsMaintainer {
						run.logger.Info("Skill test review halted the run for maintainer input")
						return "", run.park(fmt.Sprintf("skill test review (%s) halted the run — the task cannot be completed as stated: %s",
							label, verdict.Comments))
					}
					if verdict.Rebuild {
						restart, rerr := rebuildRound(verdict.Comments)
						if !restart {
							return "", rerr
						}
						continue testPhase
					}
					if result.Passed && verdict.Approved {
						run.logger.Info("Skill test review approved", "Skill", label)
						break
					}
					if err := testFixRound(result, verdict); err != nil {
						return "", err
					}
				}
			}
			// Terminal freshness gate (approve side): agent rounds ran
			// after the last suite execution — the reviews above approved a
			// tree no suite has seen in its final shape. Approving anyway
			// would be the incident this gate closes (a run reaching its
			// terminal state with the final code never validated), so the
			// run re-enters the test phase instead: a fresh suite
			// execution, the default reviewer first. One re-entry, no loop —
			// the suite run at the top of the phase postdates every edit,
			// so a coincident green-plus-approval closes the run.
			if run.suiteGate && run.suiteStale() {
				run.logger.Info("Suite stale at approval (agent rounds ran after the last suite execution); re-entering the test phase")
				continue testPhase
			}
			return run.finalize()
		}
		if err := testFixRound(result, verdict); err != nil {
			return "", err
		}
	}
}

// preFlightGate resolves and runs the native suite on the untouched
// worktree before the first agent round. A red baseline parks the run —
// no session may start on a failing suite — unless the run's input opts
// out (PipelineInput.SkipPreflight, `run --skip-preflight`): the opted-in
// run still executes the suite and records the red result, then opens on
// it deliberately, the flow's own suite gating expected to carry the fix.
// The test-only flow is exempt by identity, not by flag: a `-w test-only`
// run exists to repair the suite, so a red baseline is its input and the
// gate opens on it exactly as the opted-out path does.
// Uses the same discovery and test-queue execution the test session
// uses, and returns the resolved
// suite command for flows that keep executing it (slim re-runs the same
// suite per sub-task; the others ignore it). A discovery concluding the
// repository has no suite at all (activities.ErrNoSuite) passes vacuously:
// nothing can be red when nothing exists — a greenfield repo is not a red
// baseline — and the history records why no suite round ran.
//
// A continued run (`daedalus continue`) skips the suite execution but
// keeps the discovery — and only when its input carries
// BaselineValidated, which `continue` derives from the aborted attempt's
// own history (the gate's pass, or a green suite round after it) or
// threads forward from an earlier link's recorded flag — the skip
// records no suite of its own, so the threading is what keeps a
// validated chain validated across continues. Such a run's worktree
// deliberately carries the attempt's
// mid-flight work, so the old tests measure the in-progress change, not
// the baseline — and an implementation round may legitimately break one
// until the test phase updates it. The unchanged done path still
// requires the full green suite before finalize. A never-validated
// attempt — parked by this gate, died at or before it, or started before
// the gate existed — continues into a re-run gate, the pre-change
// behavior; on a dirty pre-gate-binary worktree that re-run reads
// mid-flight work as the baseline and may re-park: fail-loud, and rarer
// than the silent skip keying on the branch alone would have swallowed
// (backlog/bugs/preflight-skip-continue-unvalidated-baseline.md).
// The skip itself is version-gated (preflightSkipChangeID): a continued
// run in flight across a worker binary swap replays coherently — history
// recorded before the marker keeps issuing the recorded gate suite
// execution, marker-carrying history skips
// (backlog/bugs/preflight-skip-replay-nondeterminism.md).
func (r *pipelineRun) preFlightGate() (string, error) {
	command, err := r.resolveTestCommand()
	if err != nil {
		if isNoSuite(err) {
			r.logger.Info("No suite discovered; gate passes vacuously")
			return "", nil
		}
		return "", err
	}
	if r.input.BaselineValidated && baselineValidatedSkipEnabled(r.ctx) {
		r.logger.Info("Continued run: baseline validated by a green suite round earlier in this chain; skipping the gate", "Command", command)
		return command, nil
	}
	result, err := r.runSuite(command)
	if err != nil {
		return "", err
	}
	if result.Passed {
		r.logger.Info("Preflight suite green; opening the first session", "Command", command)
		return command, nil
	}
	// The opted-in run's premise, on record: the red baseline is the
	// stated reason this run exists, so the gate opens instead of parking.
	// The suite block above is already in the task log and this line in
	// the history — the run's record shows it opened on red, and the
	// flow's own suite gating is what works the baseline's fix into the
	// green round finalize requires. The done path is unchanged: finalize
	// still requires a green suite round, so the debt cannot slip through.
	if r.input.SkipPreflight {
		r.logger.Info("Preflight suite red; the run's input skips the gate — opening on the unvalidated baseline, the flow's own suite gating covers the fix", "Command", command)
		return command, nil
	}
	// A test-only run is a fix-test round by construction: the red suite
	// is the work's input, not its precondition. The flow's identity —
	// recorded on the workflow input like SkipPreflight above, so the
	// branch is replay-stable by the same construction and carries none of
	// the flag's riders — opens the gate instead of parking, with the
	// failure output above in the task log feeding the first round as its
	// target. The done path is unchanged: finalize still requires a green
	// suite round, so the debt cannot slip through.
	if r.input.Flow == "test-only" {
		r.logger.Info("Preflight suite red; the test-only flow opens on it — repairing the suite is the run's task", "Command", command)
		return command, nil
	}
	r.logger.Info("Preflight suite failed; parking before the first session")
	return "", r.park(fmt.Sprintf("preflight gate failed — the suite must be green before a session starts: %s", command))
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
		comments = verdict.Comments + verdictNote(verdict)
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

// promptOverflowMarkers match the static failure of a prompt that cannot
// fit the serving model's context window: claude's result JSON prints the
// first shape, the OpenAI-compatible wire the other two. Crossing the
// worker→workflow boundary an activity error survives only as message
// text (see isAPIExhaustion), so a substring match over the wrapped
// output is the classification. ponytail: the match IS forgeable — round
// output an agent printed can fail a review that would have heartbeated;
// accepted because the sizes are static (the same prompt is re-sent by
// every retry), so the misclassification only skips retries that could
// never succeed. The one refinement over a bare text match is kill
// precedence, refused in isPromptOverflow itself so no call site can
// forget it: a wall-clock kill whose earlier stdout happened to echo a
// marker is a retryable death, not a static failure.
var promptOverflowMarkers = []string{
	"Prompt is too long",
	"context_length_exceeded",
	"maximum context length",
	// The activities' own round-start preflight refusal
	// (activities.ErrPromptOverWindow) rides the same terminal
	// classification: its refusal is static by construction (the prompt is
	// final when the gate fires), so no retry can shrink it. Referenced,
	// not re-literalized, so the sentinel and its workflow-side
	// classification cannot drift apart.
	activities.ErrPromptOverWindow.Error(),
}

// isPromptOverflow reports whether a failed round died because its prompt
// could not fit the serving model's context window. The size is static, so
// the review loop treats the verdict as terminal instead of spending its
// fresh-session fallback or its quota heartbeats on it. The diff-file
// handoff keeps initial reviewer prompts under the window; this is the
// mid-round backstop (a conversation grown past the window by its own tool
// traffic, or test logs the budget does not cover) plus the activities'
// round-start preflight refusal, which is static by construction.
//
// A killed round is refused before the text match: the kill wraps carry no
// output by design, but the refusal keeps a marker echoed into an earlier
// round's stdout from classifying a later wall-clock death terminal. (An
// envelope-only match — markers checked against only the structured
// "round died on an api error" report — was tried here and reverted: the
// plain exhaustion wraps carry no envelope, so real aider/opencode/codex
// overflows and text-faced claude/pi overflows fell through to the quota
// heartbeat and burned its five hourly retries before parking, on a prompt
// whose size is static and unfixable by retry. See
// backlog/bugs/prompt-overflow-envelope-only-misclassifies-text-overflows.md.)
func isPromptOverflow(err error) bool {
	if err == nil || isAgentKilled(err) {
		return false
	}
	msg := err.Error()
	for _, m := range promptOverflowMarkers {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
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

// isAgentStuck reports whether err is the activities' ErrAgentStuck — a
// jailed hands round the harness itself killed as stuck (its structured
// stream's terminal failed event, class "stuck"). Same boundary note as
// isAPIExhaustion: the sentinel travels as error text, so a substring
// match carries it. The genuine classification is structured (the
// activities' handsStuck reads the stream's class field under the
// hands-only gate), but this match runs on the composed error string and
// the sibling wraps embed round output verbatim — the generic death path
// carries stdout+stderr, the exhaustion wraps the same — so a round of
// any agent can plant the sentinel phrase and mint a park here. ponytail:
// accepted, symmetric with the api-marker forge in the activities' exit-0
// classification — the phrase is a daedalus-internal shape an agent has
// no honest reason to print, a signal kill outranks the match, and the
// worst case is a bounded park the operator restarts, never silent wrong
// work. The workflow routes this face to a park — never a retry, never a
// failover.
func isAgentStuck(err error) bool {
	return err != nil && strings.Contains(err.Error(), activities.ErrAgentStuck.Error())
}
