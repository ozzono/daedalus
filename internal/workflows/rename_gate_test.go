package workflows

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
)

// The rename gate's replay contract, pinned at all three eras. Evaluating
// the gate at the schedule site — deep in the flow, past a continuation
// task's replayed-history boundary — reads the SDK's replay branch and
// schedules the legacy activity name over the new one a fresh execution
// decided on. The hoisted evaluation is safe because it replays inside the
// first workflow task beside the early gates: a fresh execution records the
// marker there and the gate decides the new name, a history holding the
// marker in that task answers the memo hit with the new name, and a
// history holding it later — or lacking it — answers DefaultVersion with
// the legacy name its recorded schedules carry.
//
// The pins hold the facts that make the contract hold: a fresh run decides
// on the new name and that decision reaches the wire verbatim — no
// registration rewrites dispatches (the legacy name is a plain wrapper
// registration, worker_run.go, mirrored in registerPipelineActivities), so
// the scheduled task's type name — the recorded schedule's spelling — is
// the gate's decision, pinned directly; startRun evaluates the gate before
// the run's first await; and rename-era histories (marker 1, recorded
// while the SDK's alias rewrite was live, so their recorded schedules hold
// the legacy spelling) map back to the legacy name their events carry.

// TestRenameGateFreshRunDecidesNewName pins the fresh-run decision:
// jailedAgentActivity on a fresh execution returns the new type name, a
// dispatch of that name resolves against the worker-shape registration,
// and the scheduled history event carries the new spelling — the wire pin
// that would have caught the original alias rewrite.
func TestRenameGateFreshRunDecidesNewName(t *testing.T) {
	env := newTestEnv(t)
	scheduledTypes := captureScheduledActivityTypes(env)
	env.OnGetVersion(jailedAgentActivityChangeID, workflow.DefaultVersion, 2).
		Return(workflow.Version(2))
	env.OnActivity(activities.RunJailedAgentActivity, mock.Anything, mock.Anything).
		Return(activities.AgentRunResult{}, nil).Once()
	env.ExecuteWorkflow(func(ctx workflow.Context, _ struct{}) (string, error) {
		name := jailedAgentActivity(ctx)
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		var res activities.AgentRunResult
		if err := workflow.ExecuteActivity(ctx, name, activities.AgentRunInput{}).Get(ctx, &res); err != nil {
			return "", err
		}
		return name, nil
	}, struct{}{})

	var got string
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if got != "RunJailedAgentActivity" {
		t.Errorf("fresh-run agent activity name = %q, want %q (the rename marker's max version)", got, "RunJailedAgentActivity")
	}
	if types := scheduledTypes(); len(types) != 1 || types[0] != "RunJailedAgentActivity" {
		t.Errorf("scheduled activity types = %v, want exactly [%q] — the fresh spelling must reach the wire verbatim", types, "RunJailedAgentActivity")
	}
	env.AssertExpectations(t)
}

// TestRenameGateMarkerOneSchedulesLegacyActivity pins the rename-era
// branch: marker 1 — recorded while the legacy alias rewrite put the
// legacy spelling on every wire schedule — maps to the legacy name, and a
// dispatch of it resolves against the worker-shape legacy wrapper
// registration. The marker-1 histories hold legacy schedules; replaying
// them with anything else wedges on the rename.
func TestRenameGateMarkerOneSchedulesLegacyActivity(t *testing.T) {
	env := newTestEnv(t)
	scheduledTypes := captureScheduledActivityTypes(env)
	env.OnGetVersion(jailedAgentActivityChangeID, workflow.DefaultVersion, 2).
		Return(workflow.Version(1))
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Return(activities.AgentRunResult{}, nil).Once()
	env.ExecuteWorkflow(func(ctx workflow.Context, _ struct{}) (string, error) {
		name := jailedAgentActivity(ctx)
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		var res activities.AgentRunResult
		if err := workflow.ExecuteActivity(ctx, name, activities.AgentRunInput{}).Get(ctx, &res); err != nil {
			return "", err
		}
		return name, nil
	}, struct{}{})

	var got string
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if got != "RunJailedClaudeActivity" {
		t.Errorf("marker-1 agent activity name = %q, want %q — rename-era histories hold legacy schedules", got, "RunJailedClaudeActivity")
	}
	if types := scheduledTypes(); len(types) != 1 || types[0] != "RunJailedClaudeActivity" {
		t.Errorf("scheduled activity types = %v, want exactly [%q] — the legacy spelling must reach the wire for marker-1 histories", types, "RunJailedClaudeActivity")
	}
	env.AssertExpectations(t)
}

// captureScheduledActivityTypes arms a listener that records the activity
// type name of every task the workflow schedules — the spellings the wire
// carried. The SDK's test environment has no history reader; the started
// task's type is built verbatim from the recorded schedule's attributes,
// so this observes the same fact the history event carries. Call the
// returned func after ExecuteWorkflow to collect the spellings.
func captureScheduledActivityTypes(env *testsuite.TestWorkflowEnvironment) func() []string {
	var names []string
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		names = append(names, info.ActivityType.Name)
	})
	return func() []string { return names }
}

// TestRenameGateStoredNameBeforeFirstAwait pins startRun's evaluation
// order: the gate runs and stores the chosen name on pipelineRun before
// the run's first await — the dependency gate. The GetVersion mock flags
// the gate's evaluation, and the dependency probe's mock snapshots the
// flag at the moment the first await's activity runs: pre-await, the gate
// replays beside the early gates inside the first workflow task, where
// the SDK's marker memo answers deterministically.
func TestRenameGateStoredNameBeforeFirstAwait(t *testing.T) {
	env := newTestEnv(t)
	var gateEvaluated, gateAtProbe atomic.Bool
	env.OnGetVersion(jailedAgentActivityChangeID, workflow.DefaultVersion, 2).
		Return(workflow.Version(2)).
		Run(func(mock.Arguments) { gateEvaluated.Store(true) })
	env.OnActivity(activities.CheckDependencyActivityName, mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { gateAtProbe.Store(gateEvaluated.Load()) }).
		Return(activities.DependencyProbe{Completed: true, Result: "daedalus/issue-42-1"}, nil).Once()
	env.ExecuteWorkflow(func(ctx workflow.Context, _ struct{}) (string, error) {
		r, _, err := startRun(ctx, PipelineInput{DependsOn: "dep-42"})
		if err != nil {
			return "", err
		}
		return r.jailedActivity, nil
	}, struct{}{})

	var got string
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if got != "RunJailedAgentActivity" {
		t.Errorf("startRun stored jailed activity = %q, want %q — runAgent schedules the stored name", got, "RunJailedAgentActivity")
	}
	if !gateAtProbe.Load() {
		t.Error("the rename gate had not been evaluated when the first await's activity ran — the evaluation must precede every await")
	}
	env.AssertExpectations(t)
}
