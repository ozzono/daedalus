package main

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozzono/daedalus/internal/activities"
)

// fakeCotServer is an in-process stand-in for the Temporal frontend the
// -cot view walks: per workflow id the execution rows listWorkflowRuns
// pages out, per run id that execution's full history, and the live
// describe the tail block reads. Everything else is unreachable for a
// -cot dump and left unimplemented: calling it fails the RPC, which is
// the stub saying so.
type fakeCotServer struct {
	workflowservice.UnimplementedWorkflowServiceServer

	rows    []*workflowpb.WorkflowExecutionInfo  // served to WorkflowId queries
	history map[string][]*historypb.HistoryEvent // run id → the execution's events ("" = the latest-run fallback)
	status  enums.WorkflowExecutionStatus        // the describe's live status

	queries []string // every list query, in order
	reads   []string // every history read, as workflowID/runID
}

func (s *fakeCotServer) GetSystemInfo(_ context.Context, _ *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{Capabilities: &workflowservice.GetSystemInfoResponse_Capabilities{}}, nil
}

func (s *fakeCotServer) ListWorkflowExecutions(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	q := req.GetQuery()
	s.queries = append(s.queries, q)
	var rows []*workflowpb.WorkflowExecutionInfo
	if rest, ok := strings.CutPrefix(q, "WorkflowId = '"); ok {
		id := strings.TrimSuffix(rest, "'")
		for _, row := range s.rows {
			if row.GetExecution().GetWorkflowId() == id {
				rows = append(rows, row)
			}
		}
	}
	return &workflowservice.ListWorkflowExecutionsResponse{Executions: rows}, nil
}

func (s *fakeCotServer) GetWorkflowExecutionHistory(_ context.Context, req *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	ex := req.GetExecution()
	s.reads = append(s.reads, ex.GetWorkflowId()+"/"+ex.GetRunId())
	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History: &historypb.History{Events: s.history[ex.GetRunId()]},
	}, nil
}

func (s *fakeCotServer) DescribeWorkflowExecution(_ context.Context, req *workflowservice.DescribeWorkflowExecutionRequest) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: s.status},
	}, nil
}

// startCotServer serves fake on a loopback port and resolves the -cot
// config lookup (./config.yaml in a fresh working directory) at it. The
// task log dir moves aside too: no task log file exists for the probed id,
// which is what keeps the live section empty and the tail's last-log stamp
// degraded — the completed-round walk is what these tests pin.
func startCotServer(t *testing.T, fake *fakeCotServer) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind the fake temporal frontend: %v", err)
	}
	srv := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	useTaskLogDir(t)
	t.Chdir(t.TempDir())
	cfg := "agent: claude\ntemporal:\n  host: " + lis.Addr().String() + "\n"
	if err := os.WriteFile("config.yaml", []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
}

// cotRound builds one jailed round's scheduled+completed event pair: the
// scheduled input decodes into the section header's role and agent, the
// completed result into the section body.
func cotRound(t *testing.T, dc converter.DataConverter, id int64, when time.Time, typ string, input, result any) []*historypb.HistoryEvent {
	t.Helper()
	inPayload, err := dc.ToPayload(input)
	if err != nil {
		t.Fatalf("encode %s input: %v", typ, err)
	}
	outPayload, err := dc.ToPayload(result)
	if err != nil {
		t.Fatalf("encode %s result: %v", typ, err)
	}
	scheduled := &historypb.HistoryEvent{
		EventId:   id,
		EventTime: timestamppb.New(when),
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId:   strconv.FormatInt(id, 10),
				ActivityType: &commonpb.ActivityType{Name: typ},
				Input:        &commonpb.Payloads{Payloads: []*commonpb.Payload{inPayload}},
			},
		},
	}
	completed := &historypb.HistoryEvent{
		EventId:   id + 1,
		EventTime: timestamppb.New(when.Add(time.Minute)),
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				ScheduledEventId: id,
				Result:           &commonpb.Payloads{Payloads: []*commonpb.Payload{outPayload}},
			},
		},
	}
	return []*historypb.HistoryEvent{scheduled, completed}
}

// seedCotRun registers a two-execution workflow on a fake frontend: the
// older execution holds one dev round, the newer one three reviewer
// rounds — native thinking first, then a Thinking-less round whose inline
// <think> fence must fall back, then a round with neither. The runs list
// carries the newest execution first (as visibility lists them), so the
// oldest-first walk order is on trial.
func seedCotRun(t *testing.T) *fakeCotServer {
	t.Helper()
	dc := converter.GetDefaultDataConverter()
	at := func(mins int) time.Time {
		return time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC).Add(time.Duration(mins) * time.Minute)
	}
	fake := &fakeCotServer{
		rows: []*workflowpb.WorkflowExecutionInfo{
			{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-cot", RunId: "run-new-bb"}, StartTime: timestamppb.New(at(60))},
			{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-cot", RunId: "run-old-aa"}, StartTime: timestamppb.New(at(0))},
		},
		history: map[string][]*historypb.HistoryEvent{
			"run-old-aa": cotRound(t, dc, 1, at(5), "RunJailedClaudeActivity",
				activities.AgentRunInput{Role: activities.RoleDev},
				activities.AgentRunResult{Thinking: "dev reasoning", Text: "dev answer"}),
			"run-new-bb": append(
				cotRound(t, dc, 1, at(65), "RunJailedReviewerActivity",
					activities.ReviewInput{Role: activities.RoleDevReview},
					activities.ReviewResult{Approved: false, Comments: "looks off\n<think>fenced review thought</think>\nCHANGES_REQUESTED", Thinking: "reviewer native reasoning"}),
				cotRound(t, dc, 3, at(95), "RunJailedReviewerActivity",
					activities.ReviewInput{Role: activities.RoleDevReview},
					activities.ReviewResult{Approved: true, Comments: "looks fine\n<think>fenced fallback</think>\nAPPROVED"})...,
			),
		},
		status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	// The third reviewer round: no Thinking, no fence — the no-CoT render.
	fake.history["run-new-bb"] = append(fake.history["run-new-bb"],
		cotRound(t, dc, 5, at(125), "RunJailedReviewerActivity",
			activities.ReviewInput{Role: activities.RoleDevReview},
			activities.ReviewResult{Approved: true, Comments: "plain review\nAPPROVED"})...)
	startCotServer(t, fake)
	return fake
}

// TestTaskLogCotRendersRounds pins the completed-round render end to end,
// in-process against the fake frontend: one section per jailed round across
// every execution of the id, oldest first, each numbered and stamped with
// its run id (the task-log blocks' 8-char-prefix convention); reviewer
// rounds prefer their native Thinking over the inline <think> fence, a
// Thinking-less round falls back to the fence, a round with neither says so
// rather than rendering an empty section; and the dump ends with the
// log-tail block naming the live temporal state.
func TestTaskLogCotRendersRounds(t *testing.T) {
	fake := seedCotRun(t)

	out := captureStdout(t, func() { runTaskLogCot("wf-cot", -1) })

	for _, want := range []string{
		"=== round 1: dev (run run-old-) @",
		"dev reasoning",
		"=== round 2: dev-review (run run-new-) @",
		"reviewer native reasoning",
		"=== round 3: dev-review (run run-new-) @",
		"fenced fallback",
		"=== round 4: dev-review (run run-new-) @",
		"no CoT captured for this round",
		"=== log tail:",
		"temporal state Running: run still going — this dump is not final",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("-cot output should contain %q; got:\n%s", want, out)
		}
	}
	// Native thinking wins over the fenced part of the same round.
	if strings.Contains(out, "fenced review thought") {
		t.Errorf("-cot output rendered the inline fence of a round with native thinking:\n%s", out)
	}
	// Oldest execution first: the dev round's section precedes every
	// reviewer round's.
	if strings.Index(out, "round 1:") > strings.Index(out, "round 2:") {
		t.Errorf("-cot output ordered the newer execution first:\n%s", out)
	}
	// The walk queried by the workflow id.
	if len(fake.queries) != 1 || fake.queries[0] != "WorkflowId = 'wf-cot'" {
		t.Errorf("list queries = %v, want one WorkflowId query", fake.queries)
	}
	// Each execution's history was read under its own run id.
	if strings.Join(fake.reads, "|") != "wf-cot/run-old-aa|wf-cot/run-new-bb" {
		t.Errorf("history reads = %v, want one per execution, oldest first", fake.reads)
	}
}

// TestTaskLogCotTailKeepsLastSections pins -cot-n: the tail keeps whole
// sections only — the last N completed rounds, never a truncation inside
// one — and rounds older than the window print nothing.
func TestTaskLogCotTailKeepsLastSections(t *testing.T) {
	seedCotRun(t)

	out := captureStdout(t, func() { runTaskLogCot("wf-cot", 2) })
	for _, want := range []string{"round 3:", "round 4:"} {
		if !strings.Contains(out, want) {
			t.Errorf("-cot-n 2 output should keep %q; got:\n%s", want, out)
		}
	}
	for _, absent := range []string{"round 1:", "round 2:"} {
		if strings.Contains(out, absent) {
			t.Errorf("-cot-n 2 output should drop %q; got:\n%s", absent, out)
		}
	}
}

// TestTaskLogCotVisibilityLagFallbackHasNoAttribution pins the fallback's
// own shape: when visibility answers no executions (a very fresh run, or a
// lagging backend), the walk reads the latest run without a run id — and
// those sections carry no "(run …)" attribution, the omission that says
// the walk could not see executions rather than a lost field.
func TestTaskLogCotVisibilityLagFallbackHasNoAttribution(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	when := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	fake := &fakeCotServer{
		history: map[string][]*historypb.HistoryEvent{
			"": cotRound(t, dc, 1, when, "RunJailedClaudeActivity",
				activities.AgentRunInput{Role: activities.RoleDev},
				activities.AgentRunResult{Thinking: "latest-run reasoning", Text: "answer"}),
		},
		status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	startCotServer(t, fake)

	out := captureStdout(t, func() { runTaskLogCot("wf-lagging", -1) })

	if !strings.Contains(out, "=== round 1: dev @") || !strings.Contains(out, "latest-run reasoning") {
		t.Errorf("fallback output should render the latest run's round; got:\n%s", out)
	}
	if strings.Contains(out, "(run ") {
		t.Errorf("fallback sections must carry no run attribution; got:\n%s", out)
	}
	if strings.Join(fake.reads, "|") != "wf-lagging/" {
		t.Errorf("history reads = %v, want the empty-run-id read alone", fake.reads)
	}
}
