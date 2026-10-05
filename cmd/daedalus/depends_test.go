package main

import (
	"context"
	"strings"
	"testing"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/workflows"
)

// fakeDepClient answers only the two client calls preflightDependency
// makes — DescribeWorkflowExecution for the dependency's state and queue,
// GetWorkflow(...).Get for a completed workflow's result. The embedded nil
// interface keeps the rest of client.Client unreachable: a panic there is
// the test failing loudly, not a silent wrong verdict.
type fakeDepClient struct {
	client.Client
	resp       *workflowservice.DescribeWorkflowExecutionResponse
	describeEr error
	result     string
	resultErr  error
}

func (f *fakeDepClient) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return f.resp, f.describeEr
}

func (f *fakeDepClient) GetWorkflow(ctx context.Context, workflowID string, runID string) client.WorkflowRun {
	return preflightRun{f}
}

type preflightRun struct{ f *fakeDepClient }

func (r preflightRun) GetID() string                  { return "" }
func (r preflightRun) GetRunID() string               { return "" }
func (r preflightRun) GetFirstExecutionRunID() string { return "" }

func (r preflightRun) Get(ctx context.Context, value interface{}) error { return r.get(value) }

func (r preflightRun) GetWithOptions(ctx context.Context, value interface{}, _ client.WorkflowRunGetOptions) error {
	return r.get(value)
}

func (r preflightRun) get(value interface{}) error {
	if r.f.resultErr != nil {
		return r.f.resultErr
	}
	if s, ok := value.(*string); ok {
		*s = r.f.result
	}
	return nil
}

func preflightDescribe(status enums.WorkflowExecutionStatus, taskQueue string) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: status, TaskQueue: taskQueue},
	}
}

// TestPreflightDependency pins the submit-time half of the dependency
// gate: a self-dependency is refused before any server call, an in-flight
// dependency on this config's queue dispatches, an already-approved one
// passes straight through (idempotent re-submit), and every already-broken
// shape — foreign queue, failed run, or a completion carrying the park
// marker — is refused here instead of pending forever.
func TestPreflightDependency(t *testing.T) {
	cfg := config.Config{Temporal: config.TemporalConfig{TaskQueue: "daedalus"}}

	t.Run("a run cannot depend on itself", func(t *testing.T) {
		// The self check fires before any client use — nil proves it.
		err := preflightDependency(nil, cfg, "wf-1", "wf-1")
		if err == nil || !strings.Contains(err.Error(), "cannot depend on itself") {
			t.Errorf("preflightDependency(self) = %v, want the self-dependency refusal", err)
		}
	})

	t.Run("an in-flight dependency on this queue dispatches", func(t *testing.T) {
		c := &fakeDepClient{resp: preflightDescribe(enums.WORKFLOW_EXECUTION_STATUS_RUNNING, "daedalus")}
		if err := preflightDependency(c, cfg, "wf-2", "wf-1"); err != nil {
			t.Errorf("preflightDependency(running) = %v, want dispatch", err)
		}
	})

	t.Run("an already-approved dependency passes straight through", func(t *testing.T) {
		c := &fakeDepClient{
			resp:   preflightDescribe(enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, "daedalus"),
			result: "daedalus/issue-41-1",
		}
		if err := preflightDependency(c, cfg, "wf-2", "wf-1"); err != nil {
			t.Errorf("preflightDependency(approved) = %v, want pass-through", err)
		}
	})

	t.Run("a foreign-queue dependency is refused", func(t *testing.T) {
		c := &fakeDepClient{resp: preflightDescribe(enums.WORKFLOW_EXECUTION_STATUS_RUNNING, "other-queue")}
		err := preflightDependency(c, cfg, "wf-2", "wf-1")
		if err == nil || !strings.Contains(err.Error(), "runs on task queue") {
			t.Errorf("preflightDependency(foreign queue) = %v, want the queue-ownership refusal", err)
		}
	})

	t.Run("an already-failed dependency is refused", func(t *testing.T) {
		c := &fakeDepClient{resp: preflightDescribe(enums.WORKFLOW_EXECUTION_STATUS_FAILED, "daedalus")}
		err := preflightDependency(c, cfg, "wf-2", "wf-1")
		if err == nil || !strings.Contains(err.Error(), "already broken") {
			t.Errorf("preflightDependency(failed) = %v, want the already-broken refusal", err)
		}
	})

	t.Run("an already-parked completion is refused", func(t *testing.T) {
		c := &fakeDepClient{
			resp:   preflightDescribe(enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, "daedalus"),
			result: workflows.ParkedResult("needs the maintainer"),
		}
		err := preflightDependency(c, cfg, "wf-2", "wf-1")
		if err == nil || !strings.Contains(err.Error(), "completed parked") {
			t.Errorf("preflightDependency(parked) = %v, want the parked-chain refusal", err)
		}
	})

	t.Run("a non-NotFound describe failure returns plainly", func(t *testing.T) {
		c := &fakeDepClient{describeEr: serviceerror.NewUnavailable("server down")}
		err := preflightDependency(c, cfg, "wf-2", "wf-1")
		if err == nil || !strings.Contains(err.Error(), "describe dependency wf-1") {
			t.Errorf("preflightDependency(unreachable) = %v, want the wrapped describe error", err)
		}
	})
}
