package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/metricsrelay"
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

// cotDeadRound builds one jailed round's scheduled+terminal event pair —
// term naming how the round died: "failed" (carrying msg as the failure's
// message), "canceled", or "timed_out". No result payload rides a terminal:
// the degraded section the walk must render from these events is the shape
// on trial. The scheduled event sits a minute before the terminal one, so a
// header stamped with the terminal event's time is tellable from one stamped
// with the schedule's.
func cotDeadRound(t *testing.T, dc converter.DataConverter, id int64, when time.Time, typ string, input any, term, msg string) []*historypb.HistoryEvent {
	t.Helper()
	inPayload, err := dc.ToPayload(input)
	if err != nil {
		t.Fatalf("encode %s input: %v", typ, err)
	}
	scheduled := &historypb.HistoryEvent{
		EventId:   id,
		EventTime: timestamppb.New(when.Add(-time.Minute)),
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId:   strconv.FormatInt(id, 10),
				ActivityType: &commonpb.ActivityType{Name: typ},
				Input:        &commonpb.Payloads{Payloads: []*commonpb.Payload{inPayload}},
			},
		},
	}
	var terminal *historypb.HistoryEvent
	switch term {
	case "failed":
		terminal = &historypb.HistoryEvent{
			EventId:   id + 1,
			EventTime: timestamppb.New(when),
			Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
				ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
					ScheduledEventId: id,
					Failure:          &failurepb.Failure{Message: msg},
				},
			},
		}
	case "canceled":
		terminal = &historypb.HistoryEvent{
			EventId:   id + 1,
			EventTime: timestamppb.New(when),
			Attributes: &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
				ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
					ScheduledEventId: id,
				},
			},
		}
	case "timed_out":
		terminal = &historypb.HistoryEvent{
			EventId:   id + 1,
			EventTime: timestamppb.New(when),
			Attributes: &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
				ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
					ScheduledEventId: id,
				},
			},
		}
	default:
		t.Fatalf("cotDeadRound: unknown terminal kind %q", term)
	}
	return []*historypb.HistoryEvent{scheduled, terminal}
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

// TestTaskLogCotRendersDeadRounds pins the degraded-section render end to
// end against a live daemon: a round that died carries a section like any
// other — the header stamped with the terminal event's time, the terminal
// face naming how it died, and the same source-precedence body keyed by the
// activity id that rode the scheduled event. A failure's message renders
// flattened onto the one face line (no truncation), a cancel says plainly
// what is known, and a non-agent activity's failure emits nothing.
func TestTaskLogCotRendersDeadRounds(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	at := func(mins int) time.Time {
		return time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC).Add(time.Duration(mins) * time.Minute)
	}
	// The walk re-materializes event times through protobuf, so the
	// expected stamps render the way the walk itself will see them —
	// zone included — rather than the way the fixtures built them.
	stamp := func(mins int) string {
		return timestamppb.New(at(mins)).AsTime().Format("2006-01-02 15:04:05 MST")
	}
	fake := &fakeCotServer{
		rows: []*workflowpb.WorkflowExecutionInfo{
			{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-dead", RunId: "run-dead"}, StartTime: timestamppb.New(at(0))},
		},
		history: map[string][]*historypb.HistoryEvent{
			"run-dead": append(append(append(
				cotDeadRound(t, dc, 1, at(5), "RunJailedClaudeActivity",
					activities.AgentRunInput{Role: activities.RoleDev},
					"failed", "jailed agent killed by signal: signal: killed\nsecond line\ttabbed tail"),
				cotDeadRound(t, dc, 3, at(8), "RecaptureActivity",
					activities.AgentRunInput{Role: activities.RoleTest},
					"failed", "nonagent boom")...),
				cotDeadRound(t, dc, 5, at(25), "RunJailedClaudeActivity",
					activities.AgentRunInput{Role: activities.RoleDev},
					"canceled", "")...),
				cotDeadRound(t, dc, 7, at(35), "RunJailedReviewerActivity",
					activities.ReviewInput{Role: activities.RoleTest},
					"timed_out", "")...),
		},
		status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	startCotServer(t, fake)

	stub := startCotStubRelay(t, "wf-dead")
	port := stub.srv.Listener.Addr().(*net.TCPAddr).Port
	useMetricsDaemonDir(t, metricsStatusDoc{PID: os.Getpid(), Updated: time.Now().UTC(), RelayPort: port})

	out := captureStdout(t, func() { runTaskLogCot("wf-dead", -1) })

	// Round 1, failed: the face carries the failure message flattened onto
	// one line — every word, none of the newlines — and the body is the
	// round's relay capture, matched by the scheduled event's activity id.
	for _, want := range []string{
		"=== round 1: dev (run run-dead) @ " + stamp(5) + " ===",
		"round failed: jailed agent killed by signal: signal: killed second line tabbed tail\n",
		"source: relay",
		"--- prompt ---\nthe staged prompt",
		"--- response ---\nthe staged answer",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("-cot output should contain %q; got:\n%s", want, out)
		}
	}
	// Round 2, cancelled: no payload rides a cancel, so the face is plain —
	// and the body renders the round's otel usage alone, a dead round having
	// no Temporal text to stamp it over.
	for _, want := range []string{
		"=== round 2: dev (run run-dead) @ " + stamp(25) + " ===",
		"round cancelled\n",
		"source: otel",
		"usage: in 10, out 5, cache read 2/write 1\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("-cot output should contain %q; got:\n%s", want, out)
		}
	}
	// Round 3, timed out: the third terminal renders like any other killed
	// round, degrading to the stamped no-CoT note with neither capture nor
	// ledger entry.
	for _, want := range []string{
		"=== round 3: test (run run-dead) @ " + stamp(35) + " ===",
		"round timed out\n",
		"source: temporal\nno CoT captured for this round\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("-cot output should contain %q; got:\n%s", want, out)
		}
	}
	// The non-agent activity's failure emitted nothing, and nothing
	// doubled: three dead rounds, three sections, three source stamps.
	if n := strings.Count(out, "=== round "); n != 3 {
		t.Errorf("-cot output rendered %d sections, want the 3 dead rounds alone (the non-agent failure silent):\n%s", n, out)
	}
	if n := strings.Count(out, "source: "); n != 3 {
		t.Errorf("every dead section stamps its source once; got:\n%s", out)
	}
	if strings.Contains(out, "nonagent boom") {
		t.Errorf("-cot output rendered a non-agent activity's failure:\n%s", out)
	}
	// Oldest terminal first.
	if i, j := strings.Index(out, "round 1:"), strings.Index(out, "round 2:"); i > j {
		t.Errorf("-cot output ordered the newer terminal first:\n%s", out)
	}
}

// TestTaskLogCotLeavesTheLiveRoundToTheLivePath pins the walk's exclusivity
// face: a scheduled agent activity with no terminal event yet is the round
// in flight — it owns no walk section, or the dash would double-render it
// beside the live view. Only a terminal event emits.
func TestTaskLogCotLeavesTheLiveRoundToTheLivePath(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	when := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	inPayload, err := dc.ToPayload(activities.AgentRunInput{Role: activities.RoleDevReview})
	if err != nil {
		t.Fatalf("encode pending input: %v", err)
	}
	// The round in flight: scheduled, no terminal event after it.
	pending := &historypb.HistoryEvent{
		EventId:   3,
		EventTime: timestamppb.New(when.Add(5 * time.Minute)),
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId:   "3",
				ActivityType: &commonpb.ActivityType{Name: "RunJailedClaudeActivity"},
				Input:        &commonpb.Payloads{Payloads: []*commonpb.Payload{inPayload}},
			},
		},
	}
	fake := &fakeCotServer{
		history: map[string][]*historypb.HistoryEvent{
			"": append(cotRound(t, dc, 1, when, "RunJailedClaudeActivity",
				activities.AgentRunInput{Role: activities.RoleDev},
				activities.AgentRunResult{Thinking: "finished reasoning", Text: "answer"}), pending),
		},
		status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	startCotServer(t, fake)

	out := captureStdout(t, func() { runTaskLogCot("wf-inflight", -1) })

	if !strings.Contains(out, "=== round 1: dev @") || !strings.Contains(out, "finished reasoning") {
		t.Errorf("the completed round should render as the walk's only section; got:\n%s", out)
	}
	if n := strings.Count(out, "=== round "); n != 1 {
		t.Errorf("the scheduled-without-terminal round must own no walk section; got %d sections:\n%s", n, out)
	}
	for _, absent := range []string{"round 2:", "round failed", "round cancelled", "round timed out"} {
		if strings.Contains(out, absent) {
			t.Errorf("the in-flight round must not render as a finished one (%q present):\n%s", absent, out)
		}
	}
}

// cotStubRelay is the in-process stand-in for the live daemon the -cot
// dump reads first: a /capture endpoint holding one round's record (404
// for every other) and a /ledger page carrying one otel-stamped entry.
type cotStubRelay struct {
	srv *httptest.Server
}

// startCotStubRelay mounts the stub; the round-1 capture and the round-2
// otel entry are fixed so the precedence walk has one of each source.
func startCotStubRelay(t *testing.T, task string) *cotStubRelay {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/capture":
			q := r.URL.Query()
			if q.Get("task") == task && q.Get("role") == "dev" && q.Get("round") == "1" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(metricsrelay.CapturedRound{
					At:       time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC),
					Prompt:   "the staged prompt",
					Response: "the staged answer",
				})
				return
			}
			http.Error(w, "metricsrelay: no capture for that round", http.StatusNotFound)
		case "/ledger":
			// The round field is the activity id — the walk's third round
			// (scheduled id 5) is the otel-staged one.
			entries := []metricsrelay.Entry{
				{Source: metricsrelay.SourceOtel, Task: task, Role: "dev", Round: "5",
					InputTokens: 10, OutputTokens: 5, CacheReadTokens: 2, CacheWriteTokens: 1},
				// A relay-era and an external entry: neither may read as
				// the round's otel usage.
				{Task: task, Role: "dev", Round: "5", InputTokens: 999},
				{Source: metricsrelay.SourceOtelExternal, Task: task, Role: "dev", Round: "1", InputTokens: 999},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"total": len(entries), "page": 1, "page_size": len(entries), "entries": entries})
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return &cotStubRelay{srv: srv}
}

// TestTaskLogCotLiveDaemonPrecedence pins the dump's source precedence end
// to end, in-process against a fake frontend and a stub daemon: a keyed
// round's relay capture renders the round's real prompt and response
// instead of the Temporal CoT, a capture-absent round with otel ledger
// usage renders the usage stamp over its Temporal CoT (metadata, not a
// replacement), and a round with neither renders from the walk stamped
// temporal. The live daemon is resolved from the status record, not the
// config.
func TestTaskLogCotLiveDaemonPrecedence(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	at := func(mins int) time.Time {
		return time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC).Add(time.Duration(mins) * time.Minute)
	}
	round := func(id int64, mins int, thinking string) []*historypb.HistoryEvent {
		return cotRound(t, dc, id, at(mins), "RunJailedClaudeActivity",
			activities.AgentRunInput{Role: activities.RoleDev},
			activities.AgentRunResult{Thinking: thinking, Text: "the walk text " + thinking})
	}
	fake := &fakeCotServer{
		rows: []*workflowpb.WorkflowExecutionInfo{
			{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-live", RunId: "run-live"}, StartTime: timestamppb.New(at(0))},
		},
		history: map[string][]*historypb.HistoryEvent{
			"run-live": append(round(1, 5, "capture round"), round(3, 15, "plain round")...),
		},
		status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	fake.history["run-live"] = append(fake.history["run-live"], round(5, 25, "otel round")...)
	startCotServer(t, fake)

	stub := startCotStubRelay(t, "wf-live")
	port := stub.srv.Listener.Addr().(*net.TCPAddr).Port
	useMetricsDaemonDir(t, metricsStatusDoc{PID: os.Getpid(), Updated: time.Now().UTC(), RelayPort: port})

	out := captureStdout(t, func() { runTaskLogCot("wf-live", -1) })

	// Round 1: the relay capture — the walk's own CoT must not surface.
	for _, want := range []string{
		"source: relay",
		"--- prompt ---\nthe staged prompt",
		"--- response ---\nthe staged answer",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("-cot output should contain %q; got:\n%s", want, out)
		}
	}
	if strings.Contains(out, "capture round") {
		t.Errorf("a relay-captured round must render the capture, not the walk's CoT:\n%s", out)
	}
	// Round 2: the otel usage stamp over the Temporal walk's text.
	for _, want := range []string{
		"source: otel",
		"usage: in 10, out 5, cache read 2/write 1",
		"otel round",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("-cot output should contain %q; got:\n%s", want, out)
		}
	}
	// Round 3: the walk itself, stamped temporal — and the external
	// ledger entry staging round 3's usage must not read as otel.
	for _, want := range []string{
		"source: temporal",
		"plain round",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("-cot output should contain %q; got:\n%s", want, out)
		}
	}
	if strings.Count(out, "source: ") != 3 {
		t.Errorf("every live section stamps its source once; got:\n%s", out)
	}
}

// TestCotRoundBody pins the per-round render precedence as a unit: the
// relay capture wins (its empty sides omitted), the otel usage stamp rides
// over the Temporal text, the walk alone stamps temporal, and an off or
// mid-dump-dead daemon degrades the unstamped pre-capture shape byte for
// byte — never an error, never a half-stamped dump.
func TestCotRoundBody(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("round") {
		case "1":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(metricsrelay.CapturedRound{Prompt: "p1", Response: "r1"})
		case "2":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(metricsrelay.CapturedRound{At: time.Now()})
		default:
			http.Error(w, "no capture", http.StatusNotFound)
		}
	}))
	t.Cleanup(stub.Close)

	otel := map[string]otelUsage{
		roundKey("wf", "dev", "4"): {in: 10, out: 5, cacheRead: 2, cacheWrite: 1},
	}

	t.Run("capture wins, empty sides omitted", func(t *testing.T) {
		live := true
		got := cotRoundBody(stub.URL, &live, otel, "wf", "dev", "1", "walk text")
		if want := "source: relay\n--- prompt ---\np1\n--- response ---\nr1\n"; got != want {
			t.Errorf("body = %q, want %q", got, want)
		}
		if !live {
			t.Errorf("a served capture must not flip the dump's liveness off")
		}
		got = cotRoundBody(stub.URL, &live, otel, "wf", "dev", "2", "walk text")
		if got != "source: relay\n" {
			t.Errorf("body = %q, want the bare stamp for a capture with no content", got)
		}
	})

	t.Run("otel usage stamps the walk's text", func(t *testing.T) {
		live := true
		got := cotRoundBody(stub.URL, &live, otel, "wf", "dev", "4", "walk text")
		if want := "source: otel\nusage: in 10, out 5, cache read 2/write 1\nwalk text\n"; got != want {
			t.Errorf("body = %q, want the usage stamp over the walk", got)
		}
		// Without Temporal text the stamp is all there is.
		got = cotRoundBody(stub.URL, &live, otel, "wf", "dev", "4", "")
		if want := "source: otel\nusage: in 10, out 5, cache read 2/write 1\n"; got != want {
			t.Errorf("body = %q, want the usage stamp alone", got)
		}
	})

	t.Run("the walk alone stamps temporal", func(t *testing.T) {
		live := true
		if got, want := cotRoundBody(stub.URL, &live, otel, "wf", "dev", "9", "walk text"), "source: temporal\nwalk text\n"; got != want {
			t.Errorf("body = %q, want %q", got, want)
		}
		if got, want := cotRoundBody(stub.URL, &live, otel, "wf", "dev", "9", ""), "source: temporal\nno CoT captured for this round\n"; got != want {
			t.Errorf("body = %q, want %q", got, want)
		}
	})

	t.Run("a daemon dying mid-dump degrades the rest unstamped", func(t *testing.T) {
		// Unreachability — not an answered error — is the death: a 404 or
		// 500 only degrades that one round to the next source.
		dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		deadURL := dead.URL
		dead.Close()
		live := true
		got := cotRoundBody(deadURL, &live, otel, "wf", "dev", "1", "walk text")
		if got != "walk text\n" {
			t.Errorf("body = %q, want the unstamped walk for the dying daemon", got)
		}
		if live {
			t.Fatalf("the death must flip the dump's liveness off")
		}
		// The next round renders unstamped without another dial.
		if got := cotRoundBody(deadURL, &live, otel, "wf", "dev", "2", ""); got != "no CoT captured for this round\n" {
			t.Errorf("body = %q, want the unstamped no-CoT note after the death", got)
		}
	})

	t.Run("an answered error degrades only that round", func(t *testing.T) {
		flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}))
		defer flaky.Close()
		live := true
		// The failed capture fetch falls to the next source — the otel
		// map (absent here), then the stamped walk — never to the
		// unstamped shape, and never off.
		if got := cotRoundBody(flaky.URL, &live, otel, "wf", "dev", "1", "walk text"); got != "source: temporal\nwalk text\n" {
			t.Errorf("body = %q, want the walk from the next source for the failing round", got)
		}
		if !live {
			t.Errorf("an answered error must not flip the dump's liveness off")
		}
	})

	t.Run("a daemon-less dump is the pre-capture shape", func(t *testing.T) {
		live := false
		if got := cotRoundBody("", &live, otel, "wf", "dev", "1", "walk text"); got != "walk text\n" {
			t.Errorf("body = %q, want the unstamped walk", got)
		}
		if got := cotRoundBody("", &live, otel, "wf", "dev", "1", ""); got != "no CoT captured for this round\n" {
			t.Errorf("body = %q, want the unstamped no-CoT note", got)
		}
	})
}

// TestFetchCotCapture pins the capture fetch's degrade-on shape: a capture
// the daemon does not hold — a 404 as much as any other answered-but-not-
// served status — is found=false but up (the round falls to the next
// source), a served record decodes on the wire shape with its query parts
// escaped, and only an unreachable daemon reports the daemon down.
func TestFetchCotCapture(t *testing.T) {
	var seen url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.URL.Query()
		if r.URL.Query().Get("round") == "1" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(metricsrelay.CapturedRound{Prompt: "p", Response: "r"})
			return
		}
		if r.URL.Query().Get("round") == "5" {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		http.Error(w, "no capture", http.StatusNotFound)
	}))
	defer srv.Close()

	cr, found, up := fetchCotCapture(srv.URL, "wf one", "dev", "1")
	if !found || !up || cr.Prompt != "p" || cr.Response != "r" {
		t.Errorf("fetch = (%+v, %v, %v), want the record found on a live daemon", cr, found, up)
	}
	if seen.Get("task") != "wf one" || seen.Get("role") != "dev" || seen.Get("round") != "1" {
		t.Errorf("query = %v, want the escaped parts landing whole", seen)
	}

	if _, found, up = fetchCotCapture(srv.URL, "wf", "dev", "2"); found || !up {
		t.Errorf("absent capture = (found %v, up %v), want found=false up=true", found, up)
	}
	if _, found, up = fetchCotCapture(srv.URL, "wf", "dev", "5"); found || !up {
		t.Errorf("erroring capture = (found %v, up %v), want found=false up=true", found, up)
	}

	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	if _, _, up = fetchCotCapture(deadURL, "wf", "dev", "1"); up {
		t.Errorf("an unreachable daemon read as up; only unreachability flips the dump's liveness")
	}
}

// TestCotOtelUsageFold pins the ledger's otel face as the dump reads it:
// only SourceOtel entries with a task fold into per-round usage stamps,
// same-round entries sum, and an unreachable, absent, or erroring daemon
// degrades to not-live — the walk then renders unstamped, never an error.
func TestCotOtelUsageFold(t *testing.T) {
	if otel, live := cotOtelUsage(""); otel != nil || live {
		t.Errorf("cotOtelUsage(empty root) = (%v, %v), want nothing live", otel, live)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entries := []metricsrelay.Entry{
			{Source: metricsrelay.SourceOtel, Task: "wf", Role: "dev", Round: "1", InputTokens: 10, OutputTokens: 5},
			{Source: metricsrelay.SourceOtel, Task: "wf", Role: "dev", Round: "1", InputTokens: 3, CacheReadTokens: 2, CacheWriteTokens: 1},
			{Source: metricsrelay.SourceOtel, Task: "wf", Role: "dev", Round: "2", InputTokens: 7},
			// A relay-era entry and an external one: neither is the otel face.
			{Task: "wf", Role: "dev", Round: "3", InputTokens: 999},
			{Source: metricsrelay.SourceOtelExternal, Task: "wf", Role: "dev", Round: "4", InputTokens: 999},
			// An otel row with no task has nothing to file under.
			{Source: metricsrelay.SourceOtel, InputTokens: 999},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"total": len(entries), "page": 1, "page_size": len(entries), "entries": entries})
	}))
	defer srv.Close()

	otel, live := cotOtelUsage(srv.URL)
	if !live {
		t.Fatalf("a serving daemon read as not live")
	}
	if got, want := otel[roundKey("wf", "dev", "1")], (otelUsage{in: 13, out: 5, cacheRead: 2, cacheWrite: 1}); got != want {
		t.Errorf("round 1 usage = %+v, want the same-round entries summed (%+v)", got, want)
	}
	if got := otel[roundKey("wf", "dev", "2")]; got.in != 7 {
		t.Errorf("round 2 usage = %+v, want its own fold", got)
	}
	if n := len(otel); n != 2 {
		t.Errorf("fold holds %d rounds, want 2 (relay, external, and unattributed rows excluded)", n)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer bad.Close()
	if _, live := cotOtelUsage(bad.URL); live {
		t.Errorf("an erroring daemon read as live")
	}

	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	if _, live := cotOtelUsage(deadURL); live {
		t.Errorf("an unreachable daemon read as live")
	}
}

// TestLastRoundExit pins the task log's round-exit stamp parse: the newest
// "jailed … round exited" header's timestamp, later headers winning,
// everything else — started blocks, review verdicts, malformed stamps,
// non-header lines — ignored, and a log with no exited round reading zero.
func TestLastRoundExit(t *testing.T) {
	early := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	late := early.Add(5 * time.Minute)
	log := strings.Join([]string{
		"run started: issue 42",
		"=== " + early.Format(time.RFC3339) + " jailed claude round started: stage=dev worktree=/wt pgid=5 (run 01234567) ===",
		"=== " + early.Format(time.RFC3339) + " jailed claude round exited after 1m0s (run 01234567) ===",
		"=== " + early.Format(time.RFC3339) + " reviewer: CHANGES_REQUESTED (run 01234567) ===",
		"=== " + late.Format(time.RFC3339) + " jailed claude round exited after 2m0s (run 89abcdef) ===",
		"=== not-a-stamp jailed claude round exited after 3m0s (run fedcba98) ===",
		"=== " + late.Add(time.Hour).Format(time.RFC3339) + " jailed claude round exited after 4m0s",
		"garbage line",
	}, "\n")
	if got := lastRoundExit(log); !got.Equal(late) {
		t.Errorf("lastRoundExit = %v, want the newest well-formed exited stamp %v", got, late)
	}
	if got := lastRoundExit("no headers at all"); !got.IsZero() {
		t.Errorf("lastRoundExit(headerless log) = %v, want zero", got)
	}
}

// TestLastCoTAge pins the runs feed's Last-CoT cell: the age of the log's
// newest exited round, and the dash for a run with no readable log —
// relTime renders the zero time as a dash, so a run that never completed a
// round reads the same as one with no log file.
func TestLastCoTAge(t *testing.T) {
	useTaskLogDir(t)
	now := time.Now()
	stamp := now.Add(-5 * time.Minute)
	writeTaskLogFile(t, "wf-aged",
		"=== "+stamp.Format(time.RFC3339)+" jailed claude round exited after 1m0s (run 01234567) ===")
	if got := lastCoTAge("wf-aged", now); got != "5m ago" {
		t.Errorf("lastCoTAge = %q, want %q", got, "5m ago")
	}
	if got := lastCoTAge("wf-missing", now); got != "-" {
		t.Errorf("lastCoTAge(no log) = %q, want the dash", got)
	}
}
