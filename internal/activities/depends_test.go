package activities

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// fakeDepClient answers only the two client calls the dependency probe
// makes — DescribeWorkflowExecution for the state, GetWorkflow(...).Get for
// a completed workflow's result. The embedded nil interface keeps the rest
// of client.Client out of reach: a panic there means the probe grew a new
// dependency, which the test wants loud.
type fakeDepClient struct {
	client.Client
	resp        *workflowservice.DescribeWorkflowExecutionResponse
	describeErr error
	result      string
	resultErr   error
}

func (f *fakeDepClient) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return f.resp, f.describeErr
}

func (f *fakeDepClient) GetWorkflow(ctx context.Context, workflowID string, runID string) client.WorkflowRun {
	return depRun{f}
}

type depRun struct{ f *fakeDepClient }

func (r depRun) GetID() string                  { return "tf-1" }
func (r depRun) GetRunID() string               { return "" }
func (r depRun) GetFirstExecutionRunID() string { return "" }

func (r depRun) Get(ctx context.Context, value interface{}) error {
	return r.get(value)
}

func (r depRun) GetWithOptions(ctx context.Context, value interface{}, _ client.WorkflowRunGetOptions) error {
	return r.get(value)
}

func (r depRun) get(value interface{}) error {
	if r.f.resultErr != nil {
		return r.f.resultErr
	}
	if s, ok := value.(*string); ok {
		*s = r.f.result
	}
	return nil
}

func describeWith(status enums.WorkflowExecutionStatus) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: status},
	}
}

// TestCheckDependencyActivity pins the probe's classification contract: a
// vanished id is a terminal non-error ("not found" — the chain's fate, not
// a probe failure), running states (live and continued-as-new) are
// non-terminal, every closed state is terminal, and only a completed
// workflow's recorded result is read (the branch name or park marker the
// workflow gate decides on).
func TestCheckDependencyActivity(t *testing.T) {
	t.Run("a vanished id is terminal, not an error", func(t *testing.T) {
		c := &fakeDepClient{describeErr: serviceerror.NewNotFound("gone")}
		probe, err := NewCheckDependencyActivity(c)(context.Background(), "tf-1")
		if err != nil {
			t.Fatalf("probe(not found): %v", err)
		}
		if !probe.Terminal || probe.Completed || probe.Status != "not found" {
			t.Errorf("probe = %+v, want Terminal with status \"not found\"", probe)
		}
	})

	t.Run("live states are not terminal", func(t *testing.T) {
		for _, status := range []enums.WorkflowExecutionStatus{
			enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		} {
			c := &fakeDepClient{resp: describeWith(status)}
			probe, err := NewCheckDependencyActivity(c)(context.Background(), "tf-1")
			if err != nil {
				t.Fatalf("probe(%v): %v", status, err)
			}
			if probe.Terminal || probe.Status != "running" {
				t.Errorf("probe(%v) = %+v, want non-terminal with status \"running\"", status, probe)
			}
		}
	})

	t.Run("paused is its own non-terminal state, not running", func(t *testing.T) {
		// Paused is resumable (`temporal workflow unpause`), not stopped: the
		// old default case reported it Terminal, failing every run chained
		// behind a paused dependency, and the gate tells a paused hold from a
		// plain running wait only by this status string.
		c := &fakeDepClient{resp: describeWith(enums.WORKFLOW_EXECUTION_STATUS_PAUSED)}
		probe, err := NewCheckDependencyActivity(c)(context.Background(), "tf-1")
		if err != nil {
			t.Fatalf("probe(paused): %v", err)
		}
		if probe.Terminal || probe.Completed || probe.Status != "paused" {
			t.Errorf("probe(paused) = %+v, want non-terminal with status \"paused\"", probe)
		}
	})

	t.Run("terminal states classify without reading a result", func(t *testing.T) {
		for _, c := range []struct {
			status enums.WorkflowExecutionStatus
			want   string
		}{
			{enums.WORKFLOW_EXECUTION_STATUS_FAILED, "failed"},
			{enums.WORKFLOW_EXECUTION_STATUS_CANCELED, "canceled"},
			{enums.WORKFLOW_EXECUTION_STATUS_TERMINATED, "terminated"},
			{enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, "timed out"},
		} {
			client := &fakeDepClient{resp: describeWith(c.status)}
			probe, err := NewCheckDependencyActivity(client)(context.Background(), "tf-1")
			if err != nil {
				t.Fatalf("probe(%v): %v", c.status, err)
			}
			if !probe.Terminal || probe.Completed || probe.Status != c.want {
				t.Errorf("probe(%v) = %+v, want Terminal with status %q", c.status, probe, c.want)
			}
		}
	})

	t.Run("a completed dependency carries its recorded result", func(t *testing.T) {
		c := &fakeDepClient{
			resp:   describeWith(enums.WORKFLOW_EXECUTION_STATUS_COMPLETED),
			result: "daedalus/issue-41-1",
		}
		probe, err := NewCheckDependencyActivity(c)(context.Background(), "tf-1")
		if err != nil {
			t.Fatalf("probe(completed): %v", err)
		}
		if !probe.Terminal || !probe.Completed || probe.Result != "daedalus/issue-41-1" || probe.Status != "completed" {
			t.Errorf("probe = %+v, want Terminal+Completed with the preserved branch", probe)
		}
	})

	t.Run("an unreadable completion result fails the probe", func(t *testing.T) {
		c := &fakeDepClient{
			resp:      describeWith(enums.WORKFLOW_EXECUTION_STATUS_COMPLETED),
			resultErr: errors.New("history gone"),
		}
		_, err := NewCheckDependencyActivity(c)(context.Background(), "tf-1")
		if err == nil || !strings.Contains(err.Error(), "read result of dependency tf-1") {
			t.Errorf("probe(unreadable result) err = %v, want a wrapped read failure", err)
		}
	})

	t.Run("a non-NotFound describe failure fails the probe", func(t *testing.T) {
		c := &fakeDepClient{describeErr: errors.New("server unreachable")}
		_, err := NewCheckDependencyActivity(c)(context.Background(), "tf-1")
		if err == nil || !strings.Contains(err.Error(), "describe dependency tf-1") {
			t.Errorf("probe(unreachable) err = %v, want a wrapped describe failure", err)
		}
	})
}
