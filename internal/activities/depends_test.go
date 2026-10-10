package activities

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
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

// pokeSignal is one recorded SignalWorkflow call — the dependent addressed
// and the channel it was poked on.
type pokeSignal struct {
	workflowID string
	runID      string
	signalName string
}

// fakePokeClient answers only the two client calls the dependent-release
// poke makes — ListWorkflow for the still-open dependents, SignalWorkflow
// per hit. The embedded nil interface keeps the rest of client.Client out
// of reach: a panic there means the poke grew a new dependency, which the
// test wants loud (the same guard fakeDepClient uses).
type fakePokeClient struct {
	client.Client
	pages   []*workflowservice.ListWorkflowExecutionsResponse
	listErr error
	lists   []*workflowservice.ListWorkflowExecutionsRequest
	signals []pokeSignal
	// signalFail fails the signal to a dependent whose workflow id it
	// names — a dependent that closed between the listing and the signal.
	signalFail map[string]error
}

func (f *fakePokeClient) ListWorkflow(ctx context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	f.lists = append(f.lists, req)
	if f.listErr != nil {
		return nil, f.listErr
	}
	if len(f.pages) == 0 {
		return &workflowservice.ListWorkflowExecutionsResponse{}, nil
	}
	page := f.pages[0]
	f.pages = f.pages[1:]
	return page, nil
}

func (f *fakePokeClient) SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error {
	f.signals = append(f.signals, pokeSignal{workflowID: workflowID, runID: runID, signalName: signalName})
	if err, ok := f.signalFail[workflowID]; ok {
		return err
	}
	return nil
}

// TestNotifyDependentsActivity pins the dependent-release poke's contract
// over its client: it lists the still-open executions whose
// DaedalusDependsOn names the completing workflow, signals each one's
// "wakeup" — the channel the dependency gate races — counts only the
// signals that landed, skips a dependent that closed between the listing
// and the signal (the chain broke meanwhile; the fallback poll loses
// nothing), follows the listing's pages to the end, and turns a failed
// listing into the activity error that leaves every gate on its fallback.
func TestNotifyDependentsActivity(t *testing.T) {
	exec := func(id, runID string) *workflowpb.WorkflowExecutionInfo {
		return &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID},
		}
	}

	t.Run("running dependents are poked by id and run id", func(t *testing.T) {
		c := &fakePokeClient{pages: []*workflowservice.ListWorkflowExecutionsResponse{{
			Executions: []*workflowpb.WorkflowExecutionInfo{exec("tf-dep-1", "run-1"), exec("tf-dep-2", "run-2")},
		}}}
		poked, err := NewNotifyDependentsActivity(c)(context.Background(), "tf-1")
		if err != nil {
			t.Fatalf("poke: %v", err)
		}
		if poked != 2 {
			t.Errorf("poked = %d, want one per listed dependent", poked)
		}
		if len(c.lists) != 1 {
			t.Fatalf("list calls = %d, want one for a single-page listing", len(c.lists))
		}
		if req := c.lists[0]; req.Namespace != "default" {
			t.Errorf("list namespace = %q, want default", req.Namespace)
		} else if want := "DaedalusDependsOn = 'tf-1' AND ExecutionStatus = 'Running'"; req.Query != want {
			t.Errorf("list query = %q, want %q — the dependent's own upsert names this workflow", req.Query, want)
		}
		want := []pokeSignal{
			{workflowID: "tf-dep-1", runID: "run-1", signalName: "wakeup"},
			{workflowID: "tf-dep-2", runID: "run-2", signalName: "wakeup"},
		}
		if !reflect.DeepEqual(c.signals, want) {
			t.Errorf("signals = %v, want one wakeup per listed execution", c.signals)
		}
	})

	t.Run("a dependent that closed before the signal is skipped, not an error", func(t *testing.T) {
		c := &fakePokeClient{
			pages: []*workflowservice.ListWorkflowExecutionsResponse{{
				Executions: []*workflowpb.WorkflowExecutionInfo{exec("tf-dep-gone", "run-gone"), exec("tf-dep-live", "run-live")},
			}},
			signalFail: map[string]error{"tf-dep-gone": errors.New("execution already completed")},
		}
		poked, err := NewNotifyDependentsActivity(c)(context.Background(), "tf-1")
		if err != nil {
			t.Fatalf("poke: %v", err)
		}
		if poked != 1 {
			t.Errorf("poked = %d, want only the signal that landed", poked)
		}
		if len(c.signals) != 2 {
			t.Errorf("signal attempts = %d, want both listed dependents tried", len(c.signals))
		}
	})

	t.Run("a failed listing fails the activity", func(t *testing.T) {
		c := &fakePokeClient{listErr: errors.New("visibility store down")}
		poked, err := NewNotifyDependentsActivity(c)(context.Background(), "tf-1")
		if err == nil || !strings.Contains(err.Error(), "list dependents of tf-1") || !strings.Contains(err.Error(), "visibility store down") {
			t.Errorf("poke(unreachable listing) err = %v, want the listing failure wrapped with the dependency id", err)
		}
		if poked != 0 {
			t.Errorf("poked = %d, want zero on a failed listing", poked)
		}
		if len(c.signals) != 0 {
			t.Errorf("signal attempts = %d, want none without a listing", len(c.signals))
		}
	})

	t.Run("pages are followed until the token runs out", func(t *testing.T) {
		c := &fakePokeClient{pages: []*workflowservice.ListWorkflowExecutionsResponse{
			{
				Executions:    []*workflowpb.WorkflowExecutionInfo{exec("tf-dep-1", "run-1")},
				NextPageToken: []byte("page-2"),
			},
			{
				Executions: []*workflowpb.WorkflowExecutionInfo{exec("tf-dep-2", "run-2"), exec("tf-dep-3", "run-3")},
			},
		}}
		poked, err := NewNotifyDependentsActivity(c)(context.Background(), "tf-1")
		if err != nil {
			t.Fatalf("poke: %v", err)
		}
		if poked != 3 {
			t.Errorf("poked = %d, want every dependent across both pages", poked)
		}
		if len(c.lists) != 2 {
			t.Fatalf("list calls = %d, want one per page", len(c.lists))
		}
		if len(c.lists[0].GetNextPageToken()) != 0 {
			t.Errorf("first page requested with token %q, want empty — the listing starts from the front", c.lists[0].GetNextPageToken())
		}
		if got := string(c.lists[1].GetNextPageToken()); got != "page-2" {
			t.Errorf("second page requested with token %q, want the first page's token verbatim", got)
		}
	})
}
