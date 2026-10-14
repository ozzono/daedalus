package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/workflows"
)

// TestParseReportArgs pins the flag surface: --table is the default output
// accepted only for explicitness, --json switches to the machine form, and
// nothing else is a flag — the Q7 rescope dropped the multi-queue selection
// (the report is the invoking config's worker alone), so the old -q/--all
// spellings must fail loud rather than parse into a scope nobody reads.
func TestParseReportArgs(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		want    reportScope
		wantErr string
	}{
		{"no flags defaults to the table and the invoking worker", nil, reportScope{}, ""},
		{"json", []string{"--json"}, reportScope{jsonOut: true}, ""},
		{"table accepted for explicitness", []string{"--table"}, reportScope{}, ""},
		{"json then table stays json", []string{"--json", "--table"}, reportScope{jsonOut: true}, ""},
		{"removed -q is an unknown flag", []string{"-q", "q7"}, reportScope{}, "unknown report flag"},
		{"removed --queue= is an unknown flag", []string{"--queue=q7"}, reportScope{}, "unknown report flag"},
		{"removed --all is an unknown flag", []string{"--all"}, reportScope{}, "unknown report flag"},
		{"-q without a value is an unknown flag", []string{"-q"}, reportScope{}, "unknown report flag"},
		{"--all and -q together are unknown flags", []string{"--all", "-q", "q7"}, reportScope{}, "unknown report flag"},
		{"empty queue name is an unknown flag", []string{"--queue="}, reportScope{}, "unknown report flag"},
		{"injected query text is an unknown flag", []string{"-q", "q7' OR TaskQueue='x"}, reportScope{}, "unknown report flag"},
		{"unknown flag", []string{"--csv"}, reportScope{}, "unknown report flag"},
	}
	for _, c := range cases {
		got, err := parseReportArgs(c.args)
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("%s: parseReportArgs(%v) err = %v, want containing %q", c.name, c.args, err, c.wantErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: parseReportArgs(%v): %v", c.name, c.args, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s: parseReportArgs(%v) = %+v, want %+v", c.name, c.args, got, c.want)
		}
	}
}

// fakeHistoryClient serves canned workflow lists and histories to the
// report's aggregation walk; everything else on client.Client is
// unreachable for it. List queries dispatch on their shape: a WorkflowId
// query serves that id's executions from runs, anything else (the queue
// walk) serves queueRows. History reads key on the run id when the walk
// names one, on the workflow id for the latest-run fallback (the empty
// run id) — exactly the two shapes aggregateSessionUsage issues.
type fakeHistoryClient struct {
	client.Client
	histories    map[string][]*historypb.HistoryEvent // workflow id → latest-run history
	runHistories map[string][]*historypb.HistoryEvent // run id → that execution's history
	runs         map[string][]*workflowpb.WorkflowExecutionInfo
	queueRows    []*workflowpb.WorkflowExecutionInfo

	queries      []string // every list query, in order
	historyReads []string // every history read, as workflowID/runID
}

func (f *fakeHistoryClient) ListWorkflow(ctx context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	q := req.GetQuery()
	f.queries = append(f.queries, q)
	if rest, ok := strings.CutPrefix(q, "WorkflowId = '"); ok {
		return &workflowservice.ListWorkflowExecutionsResponse{
			Executions: f.runs[strings.TrimSuffix(rest, "'")],
		}, nil
	}
	return &workflowservice.ListWorkflowExecutionsResponse{Executions: f.queueRows}, nil
}

func (f *fakeHistoryClient) GetWorkflowHistory(ctx context.Context, workflowID, runID string,
	longPoll bool, filterType enums.HistoryEventFilterType) client.HistoryEventIterator {
	f.historyReads = append(f.historyReads, workflowID+"/"+runID)
	if runID != "" {
		return &fakeHistoryIterator{events: f.runHistories[runID]}
	}
	return &fakeHistoryIterator{events: f.histories[workflowID]}
}

type fakeHistoryIterator struct {
	events []*historypb.HistoryEvent
	i      int
}

func (it *fakeHistoryIterator) HasNext() bool { return it.i < len(it.events) }

func (it *fakeHistoryIterator) Next() (*historypb.HistoryEvent, error) {
	if !it.HasNext() {
		return nil, errors.New("no more history events")
	}
	ev := it.events[it.i]
	it.i++
	return ev, nil
}

func scheduledEvent(id int64, when time.Time, typ string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   id,
		EventTime: timestamppb.New(when),
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId:   strconv.FormatInt(id, 10),
				ActivityType: &commonpb.ActivityType{Name: typ},
			},
		},
	}
}

func completedEvent(t *testing.T, scheduledID int64, when time.Time, result any) *historypb.HistoryEvent {
	t.Helper()
	payload, err := converter.GetDefaultDataConverter().ToPayload(result)
	if err != nil {
		t.Fatalf("encode activity result: %v", err)
	}
	return &historypb.HistoryEvent{
		EventId:   scheduledID + 1,
		EventTime: timestamppb.New(when),
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				ScheduledEventId: scheduledID,
				Result:           &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}},
			},
		},
	}
}

// listRow seeds one execution row of a fake list response: ids, the raw
// Temporal enum, an optional close time (zero = still open), and the
// run-visibility stamps the rows table decodes.
type listRow struct {
	workflowID, runID string
	start             time.Time
	close             time.Time
	status            enums.WorkflowExecutionStatus
	daedalusStatus    string
	lastActivity      time.Time
}

func (r listRow) info(t *testing.T) *workflowpb.WorkflowExecutionInfo {
	t.Helper()
	info := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: r.workflowID, RunId: r.runID},
		Status:    r.status,
		StartTime: timestamppb.New(r.start),
	}
	if !r.close.IsZero() {
		info.CloseTime = timestamppb.New(r.close)
	}
	dc := converter.GetDefaultDataConverter()
	fields := map[string]*commonpb.Payload{}
	if r.daedalusStatus != "" {
		p, err := dc.ToPayload(r.daedalusStatus)
		if err != nil {
			t.Fatalf("encode DaedalusStatus %q: %v", r.daedalusStatus, err)
		}
		fields[workflows.DaedalusStatusAttr] = p
	}
	if !r.lastActivity.IsZero() {
		p, err := dc.ToPayload(r.lastActivity)
		if err != nil {
			t.Fatalf("encode LastActivityAt: %v", err)
		}
		fields[workflows.LastActivityAttr] = p
	}
	if len(fields) > 0 {
		info.SearchAttributes = &commonpb.SearchAttributes{IndexedFields: fields}
	}
	return info
}

// newUsageReport builds the report skeleton every aggregation test fills.
func newUsageReport() *usageReport {
	return &usageReport{
		Sessions: map[string]usageTotals{},
		Days:     map[string]usageTotals{},
		ByWorker: map[string]usageTotals{},
	}
}

// TestRunRowFor pins the live-health verdicts the RUNS section renders:
// the workflow's own stamps name the states a plain list cannot tell apart
// (starved, waiting, pending), a running stamp goes stale — the wedge
// shape — only past runStaleAfter, and a run predating the stamps falls
// back to the raw enum with running verdicts for a running enum only.
// Closed runs are not live health at all.
func TestRunRowFor(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-runStaleAfter).Add(-time.Hour) // age 2.5h past the bound

	cases := []struct {
		name       string
		row        listRow
		wantState  string
		wantRound  string
		wantClosed bool
	}{
		{
			name:       "a closed run is not live health",
			row:        listRow{workflowID: "wf-x", runID: "r1", close: now.Add(-time.Hour), status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED},
			wantClosed: true,
		},
		{
			name:      "a starved stamp renders the provider-starved verdict",
			row:       listRow{workflowID: "wf-x", runID: "r1", status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING, daedalusStatus: string(workflows.StatusStarved), lastActivity: now.Add(-5 * time.Minute)},
			wantState: "provider-starved (quota heartbeat)",
			wantRound: now.Add(-5 * time.Minute).UTC().Format(time.RFC3339),
		},
		{
			name:      "a waiting stamp renders the slot-backoff line",
			row:       listRow{workflowID: "wf-x", runID: "r1", status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING, daedalusStatus: string(workflows.StatusWaiting), lastActivity: now.Add(-5 * time.Minute)},
			wantState: "waiting (slot backoff)",
			wantRound: now.Add(-5 * time.Minute).UTC().Format(time.RFC3339),
		},
		{
			name:      "a pending stamp renders the dependency gate",
			row:       listRow{workflowID: "wf-x", runID: "r1", status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING, daedalusStatus: string(workflows.StatusPending), lastActivity: now.Add(-5 * time.Minute)},
			wantState: "pending (dependency gate)",
			wantRound: now.Add(-5 * time.Minute).UTC().Format(time.RFC3339),
		},
		{
			name:      "a fresh running stamp renders running",
			row:       listRow{workflowID: "wf-x", runID: "r1", status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING, daedalusStatus: string(workflows.StatusRunning), lastActivity: now.Add(-5 * time.Minute)},
			wantState: "running",
			wantRound: now.Add(-5 * time.Minute).UTC().Format(time.RFC3339),
		},
		{
			name:      "quiet exactly to the bound is still running, not stale",
			row:       listRow{workflowID: "wf-x", runID: "r1", status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING, daedalusStatus: string(workflows.StatusRunning), lastActivity: now.Add(-runStaleAfter)},
			wantState: "running",
			wantRound: now.Add(-runStaleAfter).UTC().Format(time.RFC3339),
		},
		{
			name:      "a running stamp quiet past the bound renders the wedge shape",
			row:       listRow{workflowID: "wf-x", runID: "r1", status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING, daedalusStatus: string(workflows.StatusRunning), lastActivity: stale},
			wantState: "stale — no completed round in 2.5h; possible wedge",
			wantRound: stale.UTC().Format(time.RFC3339),
		},
		{
			name:      "a running stamp with no round stamp says no completed round",
			row:       listRow{workflowID: "wf-x", runID: "r1", status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING, daedalusStatus: string(workflows.StatusRunning)},
			wantState: "running (no completed round recorded)",
			wantRound: "unknown",
		},
		{
			name:      "a pre-attribute running run renders the running fallback",
			row:       listRow{workflowID: "wf-x", runID: "r1", status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING},
			wantState: "running (no completed round recorded)",
			wantRound: "unknown",
		},
		{
			name:      "a pre-attribute continued-as-new run keeps its raw enum",
			row:       listRow{workflowID: "wf-x", runID: "r1", status: enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW},
			wantState: "ContinuedAsNew",
			wantRound: "unknown",
		},
		{
			name:      "a live stamp outside the verdict set renders raw",
			row:       listRow{workflowID: "wf-x", runID: "r1", status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING, daedalusStatus: "mystery", lastActivity: now.Add(-time.Minute)},
			wantState: "mystery",
			wantRound: now.Add(-time.Minute).UTC().Format(time.RFC3339),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			row, ok := runRowFor(c.row.info(t), now)
			if ok == c.wantClosed {
				t.Fatalf("ok = %v, want %v", ok, !c.wantClosed)
			}
			if c.wantClosed {
				return
			}
			if row != (runRow{WorkflowID: "wf-x", State: c.wantState, LastRound: c.wantRound}) {
				t.Errorf("row = %+v, want {wf-x, %q, %q}", row, c.wantState, c.wantRound)
			}
		})
	}
}

// TestAggregateQueueUsage pins the queue walk's two products: every live
// execution lands in the RUNS section — a continued session's several
// executions included, closed ones excluded — while the usage aggregation
// runs once per workflow id, not once per listed execution, and closed
// executions still count toward the usage they carried.
func TestAggregateQueueUsage(t *testing.T) {
	start := time.Date(2026, 10, 14, 8, 0, 0, 0, time.UTC)
	round := func(cost float64, in int64) []*historypb.HistoryEvent {
		return []*historypb.HistoryEvent{
			scheduledEvent(1, start, "RunJailedClaudeActivity"),
			completedEvent(t, 1, start, activities.AgentRunResult{Usage: activities.Usage{Worker: "w1", CostUSD: cost, InputTokens: in}}),
		}
	}
	c := &fakeHistoryClient{
		runHistories: map[string][]*historypb.HistoryEvent{
			"run-a": round(0.25, 100),
			"run-b": round(0.25, 100),
			"run-c": round(1.0, 700),
		},
		runs: map[string][]*workflowpb.WorkflowExecutionInfo{
			"wf-1": {listRow{workflowID: "wf-1", runID: "run-a", start: start, status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING}.info(t),
				listRow{workflowID: "wf-1", runID: "run-b", start: start.Add(time.Hour), status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING}.info(t)},
			"wf-2": {listRow{workflowID: "wf-2", runID: "run-c", start: start, status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING}.info(t)},
		},
		queueRows: []*workflowpb.WorkflowExecutionInfo{
			listRow{workflowID: "wf-1", runID: "run-a", start: start, status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING}.info(t),
			listRow{workflowID: "wf-1", runID: "run-b", start: start.Add(time.Hour), status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING}.info(t),
			// wf-2's only execution closed: out of the RUNS section, still
			// in the usage.
			listRow{workflowID: "wf-2", runID: "run-c", start: start, close: start.Add(time.Hour), status: enums.WORKFLOW_EXECUTION_STATUS_FAILED}.info(t),
		},
	}

	rep := newUsageReport()
	rep.Queues = []string{"daedalus"}
	if err := aggregateQueueUsage(c, "daedalus", rep); err != nil {
		t.Fatalf("aggregateQueueUsage: %v", err)
	}

	if len(rep.Runs) != 2 {
		t.Fatalf("RUNS rows = %+v, want the two live wf-1 executions only", rep.Runs)
	}
	for i, row := range rep.Runs {
		if row.WorkflowID != "wf-1" || row.State == "" {
			t.Errorf("RUNS row %d = %+v, want a live wf-1 row with a verdict", i, row)
		}
	}
	// The queue query first, then one id query per distinct workflow —
	// wf-1's two executions share one session walk.
	wantQueries := []string{
		"TaskQueue = 'daedalus'",
		"WorkflowId = 'wf-1'",
		"WorkflowId = 'wf-2'",
	}
	if strings.Join(c.queries, "|") != strings.Join(wantQueries, "|") {
		t.Errorf("list queries = %v, want %v", c.queries, wantQueries)
	}

	// wf-1 aggregated once despite owning two listed executions; wf-2's
	// closed execution still carries its usage.
	if s := rep.Sessions["wf-1"]; s.Rounds != 2 || s.CostUSD != 0.5 {
		t.Errorf("wf-1 session = %+v, want 2 rounds, cost 0.5 (aggregated once, not per execution)", s)
	}
	if s := rep.Sessions["wf-2"]; s.Rounds != 1 || s.CostUSD != 1.0 {
		t.Errorf("wf-2 session = %+v, want the closed execution's round", s)
	}
	if rep.Total.Rounds != 3 || rep.Total.CostUSD != 1.5 {
		t.Errorf("total = %+v, want 3 rounds, cost 1.5", rep.Total)
	}
}

// TestAggregateSessionUsageWalksEveryExecution pins the multi-execution
// walk: a continued or restarted session owns several executions under one
// workflow id, and every one is read, oldest first — the earlier rounds a
// latest-run-only walk loses stay in the report.
func TestAggregateSessionUsageWalksEveryExecution(t *testing.T) {
	earlier := time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	later := earlier.Add(2 * time.Hour)
	c := &fakeHistoryClient{
		runHistories: map[string][]*historypb.HistoryEvent{
			"run-old": {
				scheduledEvent(1, earlier, "RunJailedClaudeActivity"),
				completedEvent(t, 1, earlier, activities.AgentRunResult{Usage: activities.Usage{Worker: "w1", CostUSD: 0.25, InputTokens: 100}}),
			},
			"run-new": {
				scheduledEvent(1, later, "RunJailedReviewerActivity"),
				completedEvent(t, 1, later, activities.ReviewResult{Usage: activities.Usage{Worker: "w2", CostUSD: 0.75, OutputTokens: 50}}),
			},
		},
		runs: map[string][]*workflowpb.WorkflowExecutionInfo{
			"wf-cont": {
				listRow{workflowID: "wf-cont", runID: "run-new", start: later, status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING}.info(t),
				listRow{workflowID: "wf-cont", runID: "run-old", start: earlier, status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED}.info(t),
			},
		},
	}

	rep := newUsageReport()
	if err := aggregateSessionUsage(c, "wf-cont", rep); err != nil {
		t.Fatalf("aggregateSessionUsage: %v", err)
	}

	if rep.Total.Rounds != 2 || rep.Total.CostUSD != 1.0 {
		t.Errorf("total = %+v, want both executions' rounds, cost 1.0", rep.Total)
	}
	// Both executions read, oldest first, under the id's own query.
	wantReads := []string{"wf-cont/run-old", "wf-cont/run-new"}
	if strings.Join(c.historyReads, ",") != strings.Join(wantReads, ",") {
		t.Errorf("history reads = %v, want %v", c.historyReads, wantReads)
	}
	if len(c.queries) != 1 || c.queries[0] != "WorkflowId = 'wf-cont'" {
		t.Errorf("list queries = %v, want one WorkflowId query", c.queries)
	}
}

// TestAggregateSessionUsageQuotedIDDegradesToLatestRun pins the quoted-id
// guard: a workflow id that could not ride inside the visibility query (a
// metachar in a pre-gate id) degrades to the latest-run walk — no list is
// issued and the report never fails, instead of one odd id killing a whole
// report.
func TestAggregateSessionUsageQuotedIDDegradesToLatestRun(t *testing.T) {
	at := time.Date(2026, 10, 14, 8, 0, 0, 0, time.UTC)
	c := &fakeHistoryClient{histories: map[string][]*historypb.HistoryEvent{
		"wf'metachar": {
			scheduledEvent(1, at, "RunJailedClaudeActivity"),
			completedEvent(t, 1, at, activities.AgentRunResult{Usage: activities.Usage{Worker: "w1", CostUSD: 0.25}}),
		},
	}}

	rep := newUsageReport()
	if err := aggregateSessionUsage(c, "wf'metachar", rep); err != nil {
		t.Fatalf("aggregateSessionUsage: %v", err)
	}

	if len(c.queries) != 0 {
		t.Errorf("list queries = %v, want none (the id must not enter a query)", c.queries)
	}
	if strings.Join(c.historyReads, ",") != "wf'metachar/" {
		t.Errorf("history reads = %v, want one latest-run read keyed by the workflow id", c.historyReads)
	}
	if rep.Total.Rounds != 1 {
		t.Errorf("total rounds = %d, want the degraded walk's round", rep.Total.Rounds)
	}
}

// TestAggregateSessionUsageFreshRunFallsBackToLatestRun pins the empty
// visibility fallback: the list answers no executions (a very fresh run, or
// a visibility backend lagging behind) and the walk still reads the latest
// run's history — the pre-fix shape — rather than reporting nothing.
func TestAggregateSessionUsageFreshRunFallsBackToLatestRun(t *testing.T) {
	at := time.Date(2026, 10, 14, 8, 0, 0, 0, time.UTC)
	c := &fakeHistoryClient{histories: map[string][]*historypb.HistoryEvent{
		"wf-fresh": {
			scheduledEvent(1, at, "RunJailedClaudeActivity"),
			completedEvent(t, 1, at, activities.AgentRunResult{Usage: activities.Usage{Worker: "w1", CostUSD: 0.25}}),
		},
	}}

	rep := newUsageReport()
	if err := aggregateSessionUsage(c, "wf-fresh", rep); err != nil {
		t.Fatalf("aggregateSessionUsage: %v", err)
	}

	if len(c.queries) != 1 || c.queries[0] != "WorkflowId = 'wf-fresh'" {
		t.Errorf("list queries = %v, want the id's query attempted", c.queries)
	}
	if strings.Join(c.historyReads, ",") != "wf-fresh/" {
		t.Errorf("history reads = %v, want the fallback's empty-run-id read", c.historyReads)
	}
	if rep.Total.Rounds != 1 {
		t.Errorf("total rounds = %d, want the fallback walk's round", rep.Total.Rounds)
	}
}

func TestAggregateSessionUsage(t *testing.T) {
	d1 := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	d2 := d1.Add(24 * time.Hour)
	// The day buckets are keyed by the event times in local time; derive the
	// expected keys from the same conversion the report performs.
	dayOf := func(ts time.Time) string { return ts.Local().Format("2006-01-02") }

	c := &fakeHistoryClient{histories: map[string][]*historypb.HistoryEvent{
		"wf-1": {
			scheduledEvent(1, d1, "RunJailedClaudeActivity"),
			completedEvent(t, 1, d1, activities.AgentRunResult{Usage: activities.Usage{
				Worker: "w1", CostUSD: 0.25, InputTokens: 100, OutputTokens: 50,
				CacheReadTokens: 10, CacheWriteTokens: 20, Duration: 2 * time.Second,
			}}),
			// The native suite is not an agent round: absent, even though
			// its TestResult deserializes fine.
			scheduledEvent(3, d1, "RunNativeTestsActivity"),
			completedEvent(t, 3, d1, activities.TestResult{Passed: true, Logs: "ok"}),
			scheduledEvent(5, d2, "RunJailedReviewerActivity"),
			completedEvent(t, 5, d2, activities.ReviewResult{Usage: activities.Usage{
				Worker: "w2", CostUSD: 0.75, OutputTokens: 10, Duration: 30 * time.Second,
			}}),
			// A round with no captured Usage (a pre-capture worker): absent
			// from every slice, not zeroed.
			scheduledEvent(7, d2, "RunJailedClaudeActivity"),
			completedEvent(t, 7, d2, activities.AgentRunResult{Text: "plain text"}),
		},
		"wf-2": {
			scheduledEvent(1, d1, "RunJailedClaudeActivity"),
			// An empty worker stamp (a round captured before the field) reads as unknown.
			completedEvent(t, 1, d1, activities.AgentRunResult{Usage: activities.Usage{
				CostUSD: 0.5, InputTokens: 7, Duration: time.Second,
			}}),
		},
	}}

	rep := &usageReport{
		Sessions: map[string]usageTotals{},
		Days:     map[string]usageTotals{},
		ByWorker: map[string]usageTotals{},
	}
	for _, wf := range []string{"wf-1", "wf-2"} {
		if err := aggregateSessionUsage(c, wf, rep); err != nil {
			t.Fatalf("aggregateSessionUsage(%s): %v", wf, err)
		}
	}

	// Totals cover exactly the three rounds that carried Usage.
	if rep.Total.Rounds != 3 {
		t.Errorf("Total.Rounds = %d, want 3 (two agent rounds plus one review)", rep.Total.Rounds)
	}
	if math.Abs(rep.Total.CostUSD-1.5) > 1e-9 {
		t.Errorf("Total.CostUSD = %v, want 1.5", rep.Total.CostUSD)
	}
	if rep.Total.InputTokens != 107 || rep.Total.OutputTokens != 60 {
		t.Errorf("Total tokens = %d in / %d out, want 107 in / 60 out", rep.Total.InputTokens, rep.Total.OutputTokens)
	}
	if rep.Total.CacheReadTokens != 10 || rep.Total.CacheWriteTokens != 20 {
		t.Errorf("Total cache tokens = %d read / %d write, want 10 / 20", rep.Total.CacheReadTokens, rep.Total.CacheWriteTokens)
	}
	if rep.Total.Duration != 33*time.Second {
		t.Errorf("Total.Duration = %v, want 33s", rep.Total.Duration)
	}

	// Sessions stay per workflow.
	if len(rep.Sessions) != 2 {
		t.Fatalf("sessions = %d, want 2", len(rep.Sessions))
	}
	if s := rep.Sessions["wf-1"]; s.Rounds != 2 || math.Abs(s.CostUSD-1.0) > 1e-9 {
		t.Errorf("wf-1 session = %+v, want 2 rounds, cost 1.0", s)
	}
	if s := rep.Sessions["wf-2"]; s.Rounds != 1 || s.InputTokens != 7 || math.Abs(s.CostUSD-0.5) > 1e-9 {
		t.Errorf("wf-2 session = %+v, want 1 round, 7 input tokens, cost 0.5", s)
	}

	// Per worker, with the empty stamp bucketed as unknown.
	if w := rep.ByWorker["w1"]; w.Rounds != 1 || math.Abs(w.CostUSD-0.25) > 1e-9 {
		t.Errorf("worker w1 = %+v, want 1 round, cost 0.25", w)
	}
	if w := rep.ByWorker["w2"]; w.Rounds != 1 || math.Abs(w.CostUSD-0.75) > 1e-9 {
		t.Errorf("worker w2 = %+v, want 1 round, cost 0.75", w)
	}
	if w := rep.ByWorker["unknown"]; w.Rounds != 1 || math.Abs(w.CostUSD-0.5) > 1e-9 {
		t.Errorf("worker unknown = %+v, want the empty stamp's round", w)
	}

	// Days bucket by each round's completion time.
	day1, day2 := dayOf(d1), dayOf(d2)
	if len(rep.Days) != 2 {
		t.Fatalf("days = %v, want the two buckets %q and %q", rep.Days, day1, day2)
	}
	if d := rep.Days[day1]; d.Rounds != 2 || math.Abs(d.CostUSD-0.75) > 1e-9 {
		t.Errorf("day %s = %+v, want 2 rounds, cost 0.75", day1, d)
	}
	if d := rep.Days[day2]; d.Rounds != 1 || math.Abs(d.CostUSD-0.75) > 1e-9 || d.Duration != 30*time.Second {
		t.Errorf("day %s = %+v, want the review round alone", day2, d)
	}
}

// writeStatusFile publishes a worker status file, as the worker daemon does.
func writeStatusFile(t *testing.T, name string, st workerStatus) {
	t.Helper()
	data, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(daemonDir, "worker-"+name+".status"), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestCollectWorkerRows pins the worker roster's trust rules: a status file
// counts as live slot occupancy only when its worker's pid is running and
// the file was written by that pid within the staleness bound — anything
// else shows the worker without occupancy, and a stale file says so.
func TestCollectWorkerRows(t *testing.T) {
	t.Run("fresh status from a live worker shows occupancy", func(t *testing.T) {
		useDaemonDir(t)
		writeLivePidFile(t, "alpha")
		writeStatusFile(t, "alpha", workerStatus{
			PID: os.Getpid(), Updated: time.Now().UTC(), TaskQueue: "daedalus",
			SlotsBusy: 1, SlotsTotal: 2, RoundsWaiting: 3,
		})

		rows := collectWorkerRows()
		if len(rows) != 1 {
			t.Fatalf("rows = %+v, want just alpha", rows)
		}
		want := workerRow{
			Name: "alpha", Running: true, PID: os.Getpid(), TaskQueue: "daedalus",
			SlotsBusy: 1, SlotsTotal: 2, RoundsWaiting: 3, StatusAge: "0s ago",
		}
		if rows[0] != want {
			t.Errorf("row = %+v, want %+v", rows[0], want)
		}
	})

	t.Run("stale status shows as stale without occupancy", func(t *testing.T) {
		useDaemonDir(t)
		writeLivePidFile(t, "alpha")
		writeStatusFile(t, "alpha", workerStatus{
			PID: os.Getpid(), Updated: time.Now().UTC().Add(-statusStaleAfter - time.Second),
			TaskQueue: "daedalus", SlotsBusy: 1, SlotsTotal: 2,
		})

		rows := collectWorkerRows()
		if len(rows) != 1 {
			t.Fatalf("rows = %+v, want just alpha", rows)
		}
		if !rows[0].Running || rows[0].StatusAge != "stale" {
			t.Errorf("row = %+v, want a running worker with a stale status", rows[0])
		}
		if rows[0].SlotsBusy != 0 || rows[0].SlotsTotal != 0 || rows[0].RoundsWaiting != 0 {
			t.Errorf("row = %+v, want no occupancy from a stale file", rows[0])
		}
	})

	t.Run("a worker that published nothing or is dead lists bare", func(t *testing.T) {
		useDaemonDir(t)
		// Live but never published a status file.
		writeLivePidFile(t, "alpha")
		// On record but not running, with a leftover status file from the
		// crashed predecessor: nothing in it is trusted — occupancy stays
		// zero and the age stays unreported.
		writeRecord(t, "beta", writeQueueConfig(t, "daedalus"))
		writeStatusFile(t, "beta", workerStatus{
			PID: os.Getpid(), Updated: time.Now().UTC(), TaskQueue: "daedalus",
			SlotsBusy: 2, SlotsTotal: 2,
		})

		rows := collectWorkerRows()
		if len(rows) != 2 {
			t.Fatalf("rows = %+v, want alpha and beta", rows)
		}
		if !rows[0].Running || rows[0].StatusAge != "" || rows[0].SlotsTotal != 0 {
			t.Errorf("alpha row = %+v, want running with no occupancy (no status file)", rows[0])
		}
		if rows[1].Name != "beta" || rows[1].Running || rows[1].SlotsTotal != 0 || rows[1].StatusAge != "" {
			t.Errorf("beta row = %+v, want a dead worker's file trusted for nothing", rows[1])
		}
	})
}

// TestPrintReportTable pins the table render: every section appears with the
// workers, the live runs' verdicts, and the usage rows, whatever the
// aggregation holds.
func TestPrintReportTable(t *testing.T) {
	rep := &usageReport{
		Generated: time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC),
		Queues:    []string{"daedalus", "q7"},
		Workers:   []workerRow{{Name: "alpha", Running: true, PID: 42, SlotsBusy: 1, SlotsTotal: 2, TaskQueue: "daedalus"}},
		Runs: []runRow{
			{WorkflowID: "wf-live", State: "provider-starved (quota heartbeat)", LastRound: "2026-10-14T08:00:00Z"},
			{WorkflowID: "wf-stale", State: "stale — no completed round in 2.5h; possible wedge", LastRound: "unknown"},
		},
		Sessions: map[string]usageTotals{"wf-1": {Rounds: 2, CostUSD: 1.5, Duration: 90 * time.Second}},
		Days:     map[string]usageTotals{"2026-09-19": {Rounds: 2, CostUSD: 1.5, Duration: 90 * time.Second}},
		ByWorker: map[string]usageTotals{"w1": {Rounds: 2, CostUSD: 1.5}},
		Total:    usageTotals{Rounds: 2, CostUSD: 1.5, Duration: 90 * time.Second},
	}
	out := captureStdout(t, func() { printReportTable(rep) })
	for _, want := range []string{
		"WORKERS ON THIS HOST", "alpha", "running (pid 42)", "1/2",
		"RUNS ON THIS QUEUE",
		"wf-live", "provider-starved (quota heartbeat)", "2026-10-14T08:00:00Z",
		"wf-stale", "stale — no completed round in 2.5h; possible wedge", "unknown",
		"BY SESSION", "BY DAY", "BY WORKER", "TOTAL",
		"wf-1", "2026-09-19", "w1", "all",
		"daedalus, q7",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("table output %q should contain %q", out, want)
		}
	}
}
