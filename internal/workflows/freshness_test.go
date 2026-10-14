package workflows

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/ozzono/daedalus/internal/activities"
)

// TestSuiteStale pins the terminal freshness gate's predicate: an
// execution of the suite postdating the last agent round is the fresh
// state, anything else is stale, and a run with no completed round at all
// (zero lastEdit) is never stale — nothing needs validating, so a park
// before the first successful round must not force a suite run.
func TestSuiteStale(t *testing.T) {
	baseline := time.Date(2026, 10, 14, 8, 0, 0, 0, time.UTC)
	cases := []struct {
		name      string
		lastEdit  time.Time
		lastSuite time.Time
		want      bool
	}{
		{"no edits and no suite is never stale", time.Time{}, time.Time{}, false},
		{"no edits stays never stale with a suite run", time.Time{}, baseline, false},
		{"an edit after the last suite is stale", baseline.Add(time.Hour), baseline, true},
		{"the first edit after a zero suite is stale", baseline, time.Time{}, true},
		{"a suite after the last edit is fresh", baseline, baseline.Add(time.Hour), false},
		{"equal stamps are fresh", baseline, baseline, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &pipelineRun{lastEdit: c.lastEdit, lastSuite: c.lastSuite}
			if got := r.suiteStale(); got != c.want {
				t.Errorf("suiteStale(lastEdit %v, lastSuite %v) = %v, want %v", c.lastEdit, c.lastSuite, got, c.want)
			}
		})
	}
}

// TestParkRunsSuiteFreshnessGate pins the park side of the terminal
// freshness gate: when agent rounds ran after the last suite execution, the
// park runs the resolved suite once in the run's worktree before parking,
// and the park record names the forced run's outcome — green, RED (the
// final state never passed), a timeout (no verdict — never rendered as
// red), or an execution failure (did not execute, unvalidated). The park
// decision itself is already made either way: every outcome still parks.
//
// The stale state is built on a continued run with BaselineValidated: the
// gate's baseline execution is skipped (its worktree's state was already
// validated when the attempt was preserved), so the implement round's edit
// postdates a never-run suite — structurally stale, independent of the
// test environment's clock, which sits still across back-to-back activity
// completions and would collapse a mainline baseline-plus-round run to
// equal stamps.
func TestParkRunsSuiteFreshnessGate(t *testing.T) {
	cases := []struct {
		name       string
		forced     suiteStep
		wantSuffix string
	}{
		{
			name:       "a green force run parks saying so",
			forced:     suiteStep{result: activities.TestResult{Passed: true, Logs: "ok"}},
			wantSuffix: "(suite freshness run at park: green)",
		},
		{
			name:       "a red force run parks saying the state never passed",
			forced:     suiteStep{result: activities.TestResult{Passed: false, Logs: "--- FAIL: TestBoom"}},
			wantSuffix: "(suite freshness run at park: RED — the final state never passed the suite)",
		},
		{
			name:       "a timed-out force run produced no verdict, not a red one",
			forced:     suiteStep{err: errTimeoutStub},
			wantSuffix: "(suite freshness run at park timed out — it produced no verdict on the final state)",
		},
		{
			name:       "a failed suite execution parks unvalidated",
			forced:     suiteStep{err: errTestsStub},
			wantSuffix: "(suite freshness run at park did not execute — the final state is unvalidated)",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newTestEnv(t)
			env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
				Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil).Once()
			rec := &agentRecorder{env: env}
			rec.record()
			rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
				{NeedsMaintainer: true, Comments: "needs the maintainer's secret"},
			}}
			rev.record()
			_, suiteRec := stubTestPhase(env)
			// No baseline on a validated continue: the park's forced run is
			// the only suite execution, and it takes the scripted outcome.
			suiteRec.script = []suiteStep{c.forced}
			env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

			in := baseInput()
			in.BaseBranch = "aborted/issue-42"
			in.BaselineValidated = true
			env.ExecuteWorkflow(FeatureDevWorkflow, in)
			err := env.GetWorkflowError()
			if err == nil {
				t.Fatal("want the park error")
			}
			for _, want := range []string{
				ErrAwaitingMaintainer.Error(),
				"code review halted the run",
				"needs the maintainer's secret",
				c.wantSuffix,
			} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("park error %q should contain %q", err, want)
				}
			}
			// The forced run executed the resolved command in the run's
			// worktree — exactly one freshness run, nothing else.
			if len(suiteRec.runs) != 1 {
				t.Fatalf("suite ran %d times, want 1 (the park's freshness run alone)", len(suiteRec.runs))
			}
			if got := suiteRec.runs[0]; got.Command != "go test ./..." || got.WorktreePath != "/wt/issue-42" {
				t.Errorf("freshness run = %+v, want the resolved command in the run's worktree", got)
			}
			// The park ends the run: no fix round after the halted review.
			if len(rec.inputs) != 1 {
				t.Errorf("agent ran %d times, want 1 (implement only — the park precedes any fix round)", len(rec.inputs))
			}
			env.AssertExpectations(t)
		})
	}
}

// TestParkWithoutSuiteCommandRecordsUnvalidated pins the gate's no-command
// branch through a flow that never resolves a suite: investigate has no
// test phase, so its docs-review park has nothing to force — no suite
// execution may be discovered or run at the terminal transition, and the
// park record must say the final state is unvalidated rather than pass in
// silence.
func TestParkWithoutSuiteCommandRecordsUnvalidated(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/investigate-issue-42"}, nil).Once()
	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{NeedsMaintainer: true, Comments: "the question is unanswerable as stated"},
	}}
	rev.record()
	disc, suiteRec := stubTestPhase(env)
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	in := baseInput()
	in.Flow = "investigate"
	env.ExecuteWorkflow(InvestigateWorkflow, in)

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want the park error")
	}
	for _, want := range []string{
		ErrAwaitingMaintainer.Error(),
		"docs review halted the run",
		"(suite freshness run at park did not execute — the final state is unvalidated)",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	// Nothing was discovered and nothing ran: the gate must not start
	// spending agent rounds on command discovery at a terminal transition.
	if len(suiteRec.runs) != 0 || len(disc.agents) != 0 {
		t.Errorf("suite ran %d times, discovery %d — want 0 and 0 (nothing to force without a resolved command)", len(suiteRec.runs), len(disc.agents))
	}
	if len(rec.inputs) != 1 {
		t.Errorf("agent ran %d times, want 1 (the investigate round alone)", len(rec.inputs))
	}
	env.AssertExpectations(t)
}
