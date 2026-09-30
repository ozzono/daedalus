package workflows

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
)

// visSnapshot is one reading of the run-visibility search attributes, taken
// inside the workflow execution — the same attributes `daedalus`'s list
// rendering decodes from listed executions. Exported fields: the mid-run
// query result crosses the test environment's codec.
type visSnapshot struct {
	Status    string
	HasStatus bool
	LastAt    time.Time
	HasLast   bool
}

// readVis reads the attributes from the executing workflow's info — the
// raw payloads, decoded with the default converter exactly the way
// `daedalus`'s list rendering decodes a listed execution. The test
// environment merges every UpsertSearchAttributes command into that view,
// so a read reflects the upserts emitted so far. (The typed reader
// GetTypedSearchAttributes stays blind to untyped upserts in the test
// environment: without the server's per-attribute type metadata it drops
// them.)
func readVis(ctx workflow.Context) visSnapshot {
	dc := converter.GetDefaultDataConverter()
	fields := workflow.GetInfo(ctx).SearchAttributes.GetIndexedFields()
	var s visSnapshot
	if p, ok := fields[DaedalusStatusAttr]; ok {
		var v string
		if err := dc.FromPayload(p, &v); err == nil {
			s.Status, s.HasStatus = v, true
		}
	}
	if p, ok := fields[LastActivityAttr]; ok {
		var v time.Time
		if err := dc.FromPayload(p, &v); err == nil {
			s.LastAt, s.HasLast = v, true
		}
	}
	return s
}

// runVisProbe registers a probe workflow around flow — the same execution,
// the way the worker's CompleteGreen registration wraps it — executes it
// with the base input, and captures the attributes read after the flow
// returned. The probe also serves the "vis" query mid-run, so tests can
// observe the attributes while the run sleeps. The flow's own error still
// fails the workflow; the capture is populated regardless.
func runVisProbe(t *testing.T, env *testsuite.TestWorkflowEnvironment, flow func(workflow.Context, PipelineInput) (string, error)) *visSnapshot {
	t.Helper()
	final := new(visSnapshot)
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input PipelineInput) (string, error) {
		if err := workflow.SetQueryHandler(ctx, "vis", func() (visSnapshot, error) {
			return readVis(ctx), nil
		}); err != nil {
			return "", err
		}
		branch, err := flow(ctx, input)
		*final = readVis(ctx)
		return branch, err
	}, workflow.RegisterOptions{Name: "visProbe"})
	env.ExecuteWorkflow("visProbe", baseInput())
	return final
}

// stubWorktree wires the worktree bookkeeping every outcome shares: the
// created worktree and the deferred cleanup. The finalize stub is left to
// each test — a failed or parked flow never reaches it, and a .Once()
// expectation on an unreached call fails AssertExpectations.
func stubWorktree(t *testing.T, env *testsuite.TestWorkflowEnvironment) {
	t.Helper()
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
		Return(nil).Once()
}

// TestCompleteGreenStampsTerminalStatuses pins the terminal stamps at the
// CompleteGreen choke point: parks, approvals, and failures each end with
// their own DaedalusStatus — the gap the attribute exists to fill, since
// parks and approvals are both Completed in the raw Temporal enum — and
// every run carries the LastActivityAt stamp its rounds touched.
func TestCompleteGreenStampsTerminalStatuses(t *testing.T) {
	t.Run("an approved run stamps approved", func(t *testing.T) {
		env := newTestEnv(t)
		stubWorktree(t, env)
		rec := &agentRecorder{env: env}
		rec.record()
		rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
		rev.record()
		stubTestPhase(env)
		env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
			Return("daedalus/issue-42-1", nil).Once()

		final := runVisProbe(t, env, CompleteGreen(FeatureDevWorkflow))

		if err := env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if final.Status != string(StatusApproved) {
			t.Errorf("final DaedalusStatus = %q, want %q", final.Status, StatusApproved)
		}
		if !final.HasLast || final.LastAt.IsZero() {
			t.Errorf("LastActivityAt = %v (present %v), want a stamped round time", final.LastAt, final.HasLast)
		}
		env.AssertExpectations(t)
	})

	t.Run("a parked run stamps parked", func(t *testing.T) {
		env := newTestEnv(t)
		stubWorktree(t, env)
		rec := &agentRecorder{env: env}
		rec.record()
		rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
			{NeedsMaintainer: true, Comments: "needs the maintainer's secret"},
		}}
		rev.record()
		stubTestPhase(env)

		final := runVisProbe(t, env, CompleteGreen(FeatureDevWorkflow))

		if err := env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if final.Status != string(StatusParked) {
			t.Errorf("final DaedalusStatus = %q, want %q", final.Status, StatusParked)
		}
		env.AssertExpectations(t)
	})

	t.Run("a failed run stamps failed", func(t *testing.T) {
		env := newTestEnv(t)
		stubWorktree(t, env)
		rec := &agentRecorder{env: env, stubErr: errAgentStub}
		rec.record()
		stubTestPhase(env)

		final := runVisProbe(t, env, CompleteGreen(FeatureDevWorkflow))

		if err := env.GetWorkflowError(); err == nil {
			t.Fatal("want the workflow to fail on the agent error")
		}
		if final.Status != string(StatusFailed) {
			t.Errorf("final DaedalusStatus = %q, want %q", final.Status, StatusFailed)
		}
		env.AssertExpectations(t)
	})
}

// TestPipelineRunLeavesRunningStatus pins that the terminal stamp belongs
// to the CompleteGreen wrapper alone: a flow registered unwrapped (the
// pre-visibility registration shape, or any non-pipeline registrant)
// finishes with the live-state running attribute startRun stamped, which
// the list rendering must then not let shadow the raw enum of a closed run.
func TestPipelineRunLeavesRunningStatus(t *testing.T) {
	env := newTestEnv(t)
	stubWorktree(t, env)
	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	stubTestPhase(env)
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()

	final := runVisProbe(t, env, FeatureDevWorkflow)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if final.Status != string(StatusRunning) {
		t.Errorf("final DaedalusStatus = %q, want the live-state %q", final.Status, StatusRunning)
	}
	if !final.HasLast || final.LastAt.IsZero() {
		t.Errorf("LastActivityAt = %v (present %v), want a stamped round time", final.LastAt, final.HasLast)
	}
	env.AssertExpectations(t)
}

// TestQuotaHeartbeatStampsWaiting pins the liveness story the TIME column
// tells: the run enters its hourly sleep stamped waiting, a wakeup resumes
// it stamped running again, and the resumed round's completion refreshes
// LastActivityAt past the pre-sleep stamp.
func TestQuotaHeartbeatStampsWaiting(t *testing.T) {
	env := newTestEnv(t)
	stubWorktree(t, env)

	var calls int
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, in activities.AgentRunInput) (activities.AgentRunResult, error) {
			calls++
			if calls == 1 {
				return activities.AgentRunResult{}, errQuotaStub
			}
			return activities.AgentRunResult{Text: "done"}, nil
		})
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	stubTestPhase(env)
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()

	// The first agent call exhausts the API and the run enters its hourly
	// sleep; read the attributes mid-sleep, then wake it 45 minutes in.
	var mid visSnapshot
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow("vis")
		if err != nil {
			t.Errorf("mid-heartbeat vis query: %v", err)
		} else if err := val.Get(&mid); err != nil {
			t.Errorf("decode mid-heartbeat vis snapshot: %v", err)
		}
	}, 30*time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow("wakeup", "") }, 45*time.Minute)

	final := runVisProbe(t, env, FeatureDevWorkflow)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if mid.Status != string(StatusWaiting) {
		t.Errorf("mid-heartbeat DaedalusStatus = %q, want %q", mid.Status, StatusWaiting)
	}
	if !mid.HasLast {
		t.Error("mid-heartbeat LastActivityAt absent, want the run-start stamp")
	}
	if final.Status != string(StatusRunning) {
		t.Errorf("final DaedalusStatus = %q, want %q after the wakeup", final.Status, StatusRunning)
	}
	if !final.LastAt.After(mid.LastAt) {
		t.Errorf("final LastActivityAt %v not after the mid-heartbeat stamp %v — the resumed round must refresh it", final.LastAt, mid.LastAt)
	}
	env.AssertExpectations(t)
}
