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
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ozzono/daedalus/internal/activities"
)

// TestParseReportArgs pins the flag surface: scopes are exclusive, queue
// names go verbatim into a Temporal list query so quotes are refused, and
// --table is the default output accepted only for explicitness.
func TestParseReportArgs(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		want    reportScope
		wantErr string
	}{
		{"no flags defaults to the table and the invoking queue", nil, reportScope{}, ""},
		{"json", []string{"--json"}, reportScope{jsonOut: true}, ""},
		{"table accepted for explicitness", []string{"--table"}, reportScope{}, ""},
		{"queue", []string{"-q", "q7"}, reportScope{queue: "q7", queueSet: true}, ""},
		{"queue equals form", []string{"--queue=q7"}, reportScope{queue: "q7", queueSet: true}, ""},
		{"all queues", []string{"--all"}, reportScope{all: true}, ""},
		{"-q without a value", []string{"-q"}, reportScope{}, "requires a value"},
		{"--all and -q clash", []string{"--all", "-q", "q7"}, reportScope{}, "mutually exclusive"},
		{"empty queue name", []string{"--queue="}, reportScope{}, "wants a plain queue name"},
		{"injected query text", []string{"-q", "q7' OR TaskQueue='x"}, reportScope{}, "wants a plain queue name"},
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

// fakeHistoryClient serves canned workflow histories to the report's
// aggregation walk; everything else on client.Client is unreachable for it.
type fakeHistoryClient struct {
	client.Client
	histories map[string][]*historypb.HistoryEvent
}

func (f *fakeHistoryClient) GetWorkflowHistory(ctx context.Context, workflowID, runID string,
	longPoll bool, filterType enums.HistoryEventFilterType) client.HistoryEventIterator {
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
// workers and the usage rows, whatever the aggregation holds.
func TestPrintReportTable(t *testing.T) {
	rep := &usageReport{
		Generated: time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC),
		Queues:    []string{"daedalus", "q7"},
		Workers:   []workerRow{{Name: "alpha", Running: true, PID: 42, SlotsBusy: 1, SlotsTotal: 2, TaskQueue: "daedalus"}},
		Sessions:  map[string]usageTotals{"wf-1": {Rounds: 2, CostUSD: 1.5, Duration: 90 * time.Second}},
		Days:      map[string]usageTotals{"2026-09-19": {Rounds: 2, CostUSD: 1.5, Duration: 90 * time.Second}},
		ByWorker:  map[string]usageTotals{"w1": {Rounds: 2, CostUSD: 1.5}},
		Total:     usageTotals{Rounds: 2, CostUSD: 1.5, Duration: 90 * time.Second},
	}
	out := captureStdout(t, func() { printReportTable(rep) })
	for _, want := range []string{
		"WORKERS ON THIS HOST", "alpha", "running (pid 42)", "1/2",
		"BY SESSION", "BY DAY", "BY WORKER", "TOTAL",
		"wf-1", "2026-09-19", "w1", "all",
		"daedalus, q7",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("table output %q should contain %q", out, want)
		}
	}
}
