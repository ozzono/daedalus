package main

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/workflows"
)

// fakeListServer is an in-process stand-in for the Temporal frontend a list
// render talks to: the canned execution rows ListWorkflow pages out, and per
// run ID the close event `closedRunParked`'s outcome probe resolves through
// the SDK's real client (Dial connects eagerly, so the server is listening
// before the config is handed over). A run ID with no canned history fails
// its read — the probe-cannot-read case. Everything else is unreachable for
// a table render and left unimplemented: calling it fails the RPC, which is
// the stub saying so.
type fakeListServer struct {
	workflowservice.UnimplementedWorkflowServiceServer

	rows    []*workflowpb.WorkflowExecutionInfo // listed in order
	history map[string]*historypb.HistoryEvent  // run ID → the run's close event

	// observations: the query each list named, and one record per outcome
	// probe — the execution it targeted and the probe context's remaining
	// deadline at read time.
	queries []string
	probes  []listProbe
}

// listProbe is one outcome probe the table issued.
type listProbe struct {
	workflowID, runID string
	deadline          time.Duration // remaining on the probe's context
}

func (s *fakeListServer) GetSystemInfo(_ context.Context, _ *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{Capabilities: &workflowservice.GetSystemInfoResponse_Capabilities{}}, nil
}

func (s *fakeListServer) ListWorkflowExecutions(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	s.queries = append(s.queries, req.GetQuery())
	return &workflowservice.ListWorkflowExecutionsResponse{Executions: s.rows}, nil
}

func (s *fakeListServer) GetWorkflowExecutionHistory(ctx context.Context, req *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	runID := req.GetExecution().GetRunId()
	p := listProbe{workflowID: req.GetExecution().GetWorkflowId(), runID: runID}
	if deadline, ok := ctx.Deadline(); ok {
		p.deadline = time.Until(deadline)
	}
	s.probes = append(s.probes, p)
	event, ok := s.history[runID]
	if !ok {
		return nil, status.Error(codes.NotFound, "no canned history for run "+runID)
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History: &historypb.History{Events: []*historypb.HistoryEvent{event}},
	}, nil
}

// startListServer serves fake on a loopback port and returns the config
// pointing the list command at it.
func startListServer(t *testing.T, fake *fakeListServer) config.Config {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind the fake temporal frontend: %v", err)
	}
	srv := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return config.Config{Temporal: config.TemporalConfig{Host: lis.Addr().String(), TaskQueue: "listq"}}
}

// closedRow builds one closed run's list entry: ids, the Temporal close
// enum, and an optional DaedalusStatus stamp ("" = none — a pre-marker run,
// or a live-state attribute a close the stamps don't cover left behind).
func closedRow(t *testing.T, workflowID, runID string, closeStatus enums.WorkflowExecutionStatus, stamp workflows.RunStatus) *workflowpb.WorkflowExecutionInfo {
	t.Helper()
	row := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Status:    closeStatus,
		StartTime: timestamppb.New(time.Now().Add(-2 * time.Hour)),
		CloseTime: timestamppb.New(time.Now().Add(-time.Hour)),
	}
	if stamp != "" {
		payload, err := converter.GetDefaultDataConverter().ToPayload(stamp)
		if err != nil {
			t.Fatalf("encode DaedalusStatus %q: %v", stamp, err)
		}
		row.SearchAttributes = &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			workflows.DaedalusStatusAttr: payload,
		}}
	}
	return row
}

// greenClose builds the close event of a workflow that completed with out as
// its result string.
func greenClose(t *testing.T, out string) *historypb.HistoryEvent {
	t.Helper()
	payload, err := converter.GetDefaultDataConverter().ToPayload(out)
	if err != nil {
		t.Fatalf("encode result %q: %v", out, err)
	}
	return &historypb.HistoryEvent{
		EventId:   2,
		EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
				Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}},
			},
		},
	}
}

// redClose builds the close event of a workflow that failed with message as
// the failure text.
func redClose(message string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   2,
		EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
				Failure: &failurepb.Failure{Message: message},
			},
		},
	}
}

// TestListPipelinesClosedRunTaxonomy pins the closed rows' display taxonomy.
// Only an approved run earns completed; parked and aborted (the wipe cancel,
// Canceled here — a live waiting attribute mid-heartbeat included) render as
// paused, because neither is an end, and a failed stamp keeps its raw
// attribute. A run without a terminal stamp classifies from the close enum:
// Canceled is an abort; TimedOut and Terminated keep the raw enum; and a
// green (Completed) or red (FAILED) close can hide a park not carrying the
// stamp — the marker result the current registration completes parks with,
// or the failure message the pre-marker workers parked with — so those two
// closes are recognized by probing the row's own outcome. The probe runs for
// those rows alone, targets the row's own execution (an empty run ID would
// resolve the latest execution and classify an old row from its
// continuation's outcome), stays bounded (tighter than wipe's single probe —
// this runs inside a table render), and one unreadable history degrades to
// the enum's rendering instead of failing the table.
func TestListPipelinesClosedRunTaxonomy(t *testing.T) {
	parkMessage := workflows.ErrAwaitingMaintainer.Error() + ": reviewer halted the run"
	fake := &fakeListServer{
		rows: []*workflowpb.WorkflowExecutionInfo{
			closedRow(t, "wf-approved", "run-approved", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, workflows.StatusApproved),
			closedRow(t, "wf-parked", "run-parked", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, workflows.StatusParked),
			closedRow(t, "wf-failed", "run-failed", enums.WORKFLOW_EXECUTION_STATUS_FAILED, workflows.StatusFailed),
			closedRow(t, "wf-wipe-cancel", "run-wipe-cancel", enums.WORKFLOW_EXECUTION_STATUS_CANCELED, workflows.StatusWaiting),
			closedRow(t, "wf-aborted", "run-aborted", enums.WORKFLOW_EXECUTION_STATUS_CANCELED, ""),
			closedRow(t, "wf-park-green", "run-park-green", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, ""),
			closedRow(t, "wf-green", "run-green", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, ""),
			closedRow(t, "wf-park-old", "run-park-old", enums.WORKFLOW_EXECUTION_STATUS_FAILED, ""),
			closedRow(t, "wf-red", "run-red", enums.WORKFLOW_EXECUTION_STATUS_FAILED, ""),
			closedRow(t, "wf-timed-out", "run-timed-out", enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, ""),
			closedRow(t, "wf-terminated", "run-terminated", enums.WORKFLOW_EXECUTION_STATUS_TERMINATED, ""),
			closedRow(t, "wf-probe-unreadable", "run-probe-unreadable", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, ""),
		},
		history: map[string]*historypb.HistoryEvent{
			"run-park-green": greenClose(t, workflows.ParkedResult("reviewer halted the run")),
			"run-green":      greenClose(t, "aborted/issue-42"),
			"run-park-old":   redClose(parkMessage),
			"run-red":        redClose("activity exploded"),
			// run-probe-unreadable is deliberately absent: its outcome probe
			// cannot read the history and must degrade, not fail the table.
		},
	}
	cfg := startListServer(t, fake)

	var err error
	out := captureStdout(t, func() { err = listPipelines(cfg, 20) })
	if err != nil {
		t.Fatalf("listPipelines: %v", err)
	}

	statusByRow := map[string]string{}
	for _, line := range strings.Split(out, "\n")[1:] { // drop the header row
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		statusByRow[fields[0]] = fields[1]
	}
	want := map[string]string{
		"wf-approved":         "completed",
		"wf-parked":           "paused",
		"wf-failed":           "failed",
		"wf-wipe-cancel":      "paused",
		"wf-aborted":          "paused",
		"wf-park-green":       "paused",
		"wf-green":            "Completed",
		"wf-park-old":         "paused",
		"wf-red":              "Failed",
		"wf-timed-out":        "TimedOut",
		"wf-terminated":       "Terminated",
		"wf-probe-unreadable": "Completed",
	}
	if len(statusByRow) != len(want) {
		t.Fatalf("the table rendered %d closed rows (%q), want %d", len(statusByRow), out, len(want))
	}
	for id, wantStatus := range want {
		if got := statusByRow[id]; got != wantStatus {
			t.Errorf("row %s rendered %q, want %q", id, got, wantStatus)
		}
	}

	// Only the rows whose close can hide a park are probed — five of them —
	// each through the row's own execution, and each bounded.
	probed := map[string]bool{
		"run-park-green":       true,
		"run-green":            true,
		"run-park-old":         true,
		"run-red":              true,
		"run-probe-unreadable": true,
	}
	if len(fake.probes) != len(probed) {
		t.Fatalf("the table issued %d outcome probes (%v), want %d", len(fake.probes), fake.probes, len(probed))
	}
	rowByRun := map[string]string{}
	for _, row := range fake.rows {
		rowByRun[row.GetExecution().GetRunId()] = row.GetExecution().GetWorkflowId()
	}
	for _, p := range fake.probes {
		if !probed[p.runID] {
			t.Errorf("row %s/%s was probed for a park — a stamped, canceled, timed-out, or terminated close must render without one", p.workflowID, p.runID)
		}
		if rowByRun[p.runID] != p.workflowID {
			t.Errorf("probe targeted %s/%s — want the row's own (workflow, run) pair", p.workflowID, p.runID)
		}
		if p.deadline <= 0 || p.deadline > 10*time.Second {
			t.Errorf("probe of %s/%s carried %v of context, want a bounded probe (<=10s, tighter than wipe's single probe)", p.workflowID, p.runID, p.deadline)
		}
	}

	// The table listed this config's queue, not the world.
	if len(fake.queries) != 1 || !strings.Contains(fake.queries[0], "TaskQueue = 'listq'") {
		t.Errorf("list queries %v, want one naming TaskQueue = 'listq'", fake.queries)
	}
}
