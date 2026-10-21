package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
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

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
	"github.com/ozzono/daedalus/internal/runstart"
	"github.com/ozzono/daedalus/internal/workflows"
)

// TestBadgeClass pins the status → badge mapping: the component kit's four
// colors, with everything unrecognized falling back to the muted one.
func TestBadgeClass(t *testing.T) {
	cases := map[string]string{
		"running":   "b-run",
		"waiting":   "b-wait",
		"pending":   "b-wait",
		"approved":  "b-idle",
		"completed": "b-idle",
		"parked":    "b-fail",
		"paused":    "b-fail",
		"failed":    "b-fail",
		"TimedOut":  "b-idle", // a raw enum the mapping does not know
	}
	for status, want := range cases {
		if got := badgeClass(status); got != want {
			t.Errorf("badgeClass(%q) = %q, want %q", status, got, want)
		}
	}
}

// TestRelTime pins the age rendering: zero is a dash (no fabricated
// epoch), and the four buckets render the way the samples do.
func TestRelTime(t *testing.T) {
	now := time.Now()
	cases := []struct {
		at   time.Time
		want string
	}{
		{time.Time{}, "-"},
		{now.Add(-10 * time.Second), "just now"},
		{now.Add(-5 * time.Minute), "5m ago"},
		{now.Add(-3 * time.Hour), "3h ago"},
		{now.Add(-50 * time.Hour), "2d ago"},
	}
	for _, c := range cases {
		if got := relTime(c.at, now); got != c.want {
			t.Errorf("relTime(%v) = %q, want %q", c.at, got, c.want)
		}
	}
}

// TestClockOrDate pins the table timestamp: today's rows show the wall
// clock, older ones the date, and no attribute is a dash.
func TestClockOrDate(t *testing.T) {
	if got := clockOrDate(time.Time{}); got != "-" {
		t.Errorf("clockOrDate(zero) = %q, want -", got)
	}
	recent := time.Now().Add(-time.Hour)
	if got := clockOrDate(recent); got != recent.Format("15:04:05") {
		t.Errorf("clockOrDate(recent) = %q, want the wall clock %q", got, recent.Format("15:04:05"))
	}
	old := time.Now().Add(-48 * time.Hour)
	if got := clockOrDate(old); got != old.Format("2006-01-02 15:04") {
		t.Errorf("clockOrDate(old) = %q, want the date %q", got, old.Format("2006-01-02 15:04"))
	}
}

// TestTailStr pins the excerpt cap: short text unchanged, longer text cut
// to its last max bytes on a line boundary so the visible half never opens
// mid-line, and a window without a newline degrades to the raw tail.
func TestTailStr(t *testing.T) {
	if got := tailStr("abc", 10); got != "abc" {
		t.Errorf("tailStr(short) = %q, want abc", got)
	}
	if got := tailStr("abcdefgh", 8); got != "abcdefgh" {
		t.Errorf("tailStr(exact) = %q, want unchanged", got)
	}
	if got := tailStr("line1\nline2\nline3", 6); got != "line3" {
		t.Errorf("tailStr(lines) = %q, want line3 (cut on the line boundary)", got)
	}
	if got := tailStr("abcdef", 2); got != "ef" {
		t.Errorf("tailStr(no newline in window) = %q, want ef", got)
	}
}

// TestTemporalUILink pins the outbound link: the host survives the port
// strip, the port is the Temporal UI's 8233, the workflow id is path-
// escaped, and an unparseable host yields no link rather than a broken one.
func TestTemporalUILink(t *testing.T) {
	if got, want := temporalUILink("127.0.0.1:7233", "listq-iss1"),
		"http://127.0.0.1:8233/namespaces/default/workflows/listq-iss1"; got != want {
		t.Errorf("temporalUILink(host:port) = %q, want %q", got, want)
	}
	if got := temporalUILink("hostonly", "w"); got != "http://hostonly:8233/namespaces/default/workflows/w" {
		t.Errorf("temporalUILink(hostonly) = %q", got)
	}
	if got := temporalUILink("127.0.0.1:7233", "wf/x y"); !strings.Contains(got, "wf%2Fx%20y") {
		t.Errorf("temporalUILink(escaping) = %q, want the id path-escaped", got)
	}
	if got := temporalUILink("", "w"); got != "" {
		t.Errorf("temporalUILink(empty) = %q, want no link", got)
	}
}

// TestUsageRow pins the usage-table row: figures ride named fields, the
// cost renders at four decimals, and an all-zero capture is the n/a face —
// not a row of fabricated zeros.
func TestUsageRow(t *testing.T) {
	na := usageRow(1, "implement", "pi", activities.Usage{})
	if !na.NA {
		t.Errorf("usageRow(zero) = %+v, want the n/a face", na)
	}
	u := usageRow(2, "review", "claude", activities.Usage{InputTokens: 3, OutputTokens: 4, Duration: 90 * time.Second, CostUSD: 0.5})
	if u.NA || u.In != 3 || u.Out != 4 || u.Duration != "1m30s" || u.Cost != "0.5000" {
		t.Errorf("usageRow(figures) = %+v, want the round's own numbers", u)
	}
}

// TestReviewVerdictFaces pins the reviewer round's outcome naming: the four
// markers, the marker-less faces (with and without the repair layer's
// likely-verdict annotation), and the default — CHANGES_REQUESTED, the
// standing contract for a malformed-but-real review.
func TestReviewVerdictFaces(t *testing.T) {
	cases := []struct {
		r           activities.ReviewResult
		want, badge string
	}{
		{activities.ReviewResult{Approved: true}, "APPROVED", "b-run"},
		{activities.ReviewResult{NeedsMaintainer: true}, "NEEDS_MAINTAINER", "b-fail"},
		{activities.ReviewResult{Rebuild: true}, "REBUILD", "b-wait"},
		{activities.ReviewResult{NoVerdict: true}, "NO VERDICT", "b-fail"},
		{activities.ReviewResult{NoVerdict: true, LikelyVerdict: "NEEDS_MAINTAINER"}, "NO VERDICT → likely NEEDS_MAINTAINER", "b-fail"},
		{activities.ReviewResult{}, "CHANGES_REQUESTED", "b-wait"},
	}
	for i, c := range cases {
		got, badge := reviewVerdict(c.r)
		if got != c.want || badge != c.badge {
			t.Errorf("case %d: reviewVerdict(%+v) = %q, %q; want %q, %q", i, c.r, got, badge, c.want, c.badge)
		}
	}
}

// TestScheduledAgentRoleDecode pins the scheduled-input decoders: a
// working round's input names the override agent and session role, a
// reviewer's input decodes the same two, and an empty input decodes to
// nothing. roundRole names the role for the rail.
func TestScheduledAgentRoleDecode(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	mustPayload := func(v any) []*commonpb.Payload {
		t.Helper()
		p, err := dc.ToPayload(v)
		if err != nil {
			t.Fatalf("encode %T: %v", v, err)
		}
		return []*commonpb.Payload{p}
	}

	agent, role := scheduledAgent(mustPayload(activities.AgentRunInput{Agent: "pi", Role: activities.RoleTest}), dc), schedRole(mustPayload(activities.AgentRunInput{Agent: "pi", Role: activities.RoleTest}), dc)
	if agent != "pi" || role != activities.RoleTest {
		t.Errorf("agent round decode = %q, %q; want pi, %q", agent, role, activities.RoleTest)
	}
	agent = scheduledAgent(mustPayload(activities.ReviewInput{Agent: "claude", Role: activities.RoleDevReview}), dc)
	role = schedRole(mustPayload(activities.ReviewInput{Agent: "claude", Role: activities.RoleDevReview}), dc)
	if agent != "claude" || role != activities.RoleDevReview {
		t.Errorf("review round decode = %q, %q; want claude, %q", agent, role, activities.RoleDevReview)
	}
	if agent := scheduledAgent(nil, dc); agent != "" {
		t.Errorf("scheduledAgent(empty) = %q, want empty", agent)
	}
	if role := schedRole(nil, dc); role != "" {
		t.Errorf("schedRole(empty) = %q, want empty", role)
	}
	if got := roundRole(activities.RoleDev); got != "implement" {
		t.Errorf("roundRole(dev) = %q, want implement", got)
	}
	if got := roundRole(activities.RoleTest); got != "test" {
		t.Errorf("roundRole(test) = %q, want test", got)
	}
	if got := roundRole(""); got != "agent" {
		t.Errorf("roundRole(unset) = %q, want agent", got)
	}
}

// TestFormFlowsMatchesRegistry pins the New-Task dropdown's flows: exactly
// the submit registry's names, sorted — never a hardcoded list that could
// drift from what the CLI's -w accepts.
func TestFormFlowsMatchesRegistry(t *testing.T) {
	want := make([]string, 0, len(runstart.Registry))
	for name := range runstart.Registry {
		want = append(want, name)
	}
	sort.Strings(want)
	got := formFlows()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("formFlows() = %v, want the registry's sorted names %v", got, want)
	}
	if !strings.Contains(strings.Join(got, ","), "feature-dev") {
		t.Errorf("formFlows() = %v, want the default flow selectable", got)
	}
}

// TestExpandHome pins the form-side ~ resolution (the shell does it for the
// CLI): a bare ~ and a ~/-prefixed path resolve against the home directory;
// everything else passes through untouched.
func TestExpandHome(t *testing.T) {
	t.Setenv("HOME", "/tmp/dd-fakehome")
	cases := []struct{ in, want string }{
		{"~", "/tmp/dd-fakehome"},
		{"~/tasks/iss.md", "/tmp/dd-fakehome/tasks/iss.md"},
		{"/abs/path", "/abs/path"},
		{"rel/path", "rel/path"},
		{"", ""},
	}
	for _, c := range cases {
		if got := expandHome(c.in); got != c.want {
			t.Errorf("expandHome(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// uiFrontend is the dashboard's stand-in Temporal frontend: the embedded
// fakeListServer's list and close-event serving (run-id keyed) plus the
// starts, describes, and empty-run-id history reads the dashboard's walks
// issue. Registered whole, so the overrides dispatch.
type uiFrontend struct {
	*fakeListServer
	mu        sync.Mutex
	starts    []*workflowservice.StartWorkflowExecutionRequest
	describes map[string]*workflowservice.DescribeWorkflowExecutionResponse
	wfHistory map[string][]*historypb.HistoryEvent
	listErr   error
}

func (f *uiFrontend) StartWorkflowExecution(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts = append(f.starts, req)
	return &workflowservice.StartWorkflowExecutionResponse{RunId: "run-1"}, nil
}

func (f *uiFrontend) DescribeWorkflowExecution(_ context.Context, req *workflowservice.DescribeWorkflowExecutionRequest) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp, ok := f.describes[req.GetExecution().GetWorkflowId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "workflow not found: "+req.GetExecution().GetWorkflowId())
	}
	return resp, nil
}

func (f *uiFrontend) ListWorkflowExecutions(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.fakeListServer.ListWorkflowExecutions(context.Background(), req)
}

// GetWorkflowExecutionHistory serves the workflow-id-keyed histories when
// the read names no run id (the dashboard walks history by workflow id) and
// falls back to the embedded run-id-keyed serving — the close-event probe
// the status derivation issues.
func (f *uiFrontend) GetWorkflowExecutionHistory(ctx context.Context, req *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	if req.GetExecution().GetRunId() == "" {
		f.mu.Lock()
		evs, ok := f.wfHistory[req.GetExecution().GetWorkflowId()]
		f.mu.Unlock()
		if !ok {
			return nil, status.Error(codes.NotFound, "no history for "+req.GetExecution().GetWorkflowId())
		}
		return &workflowservice.GetWorkflowExecutionHistoryResponse{
			History: &historypb.History{Events: evs},
		}, nil
	}
	return f.fakeListServer.GetWorkflowExecutionHistory(ctx, req)
}

// uiHarness is one dashboard under test: an isolated daemon dir, a fake
// Temporal frontend, a fresh uiServer on an httptest listener, and the
// relay ledger its metrics view reads.
type uiHarness struct {
	fake *uiFrontend
	cfg  config.Config
	srv  *httptest.Server
	led  *metricsrelay.Ledger
}

func newUIHarness(t *testing.T) *uiHarness {
	t.Helper()
	useDaemonDir(t)
	h := &uiHarness{
		fake: &uiFrontend{
			fakeListServer: &fakeListServer{history: map[string]*historypb.HistoryEvent{}},
			describes:      map[string]*workflowservice.DescribeWorkflowExecutionResponse{},
			wfHistory:      map[string][]*historypb.HistoryEvent{},
		},
		led: &metricsrelay.Ledger{},
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind the fake frontend: %v", err)
	}
	srv := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(srv, h.fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	h.cfg = config.Config{Temporal: config.TemporalConfig{Host: lis.Addr().String(), TaskQueue: "listq"}}
	h.srv = httptest.NewServer(newUIServer(h.cfg, h.led).routes())
	t.Cleanup(h.srv.Close)
	return h
}

func (h *uiHarness) get(t *testing.T, path string) string {
	t.Helper()
	resp, err := http.Get(h.srv.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read GET %s: %v", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d: %s", path, resp.StatusCode, body)
	}
	return string(body)
}

// poll is get for the polled fragments: renderFrag answers 204 No Content
// when a fragment's bytes are unchanged, so the caller sees the status code
// and body instead of get's 200-only contract.
func (h *uiHarness) poll(t *testing.T, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(h.srv.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read GET %s: %v", path, err)
	}
	return resp.StatusCode, string(body)
}

func (h *uiHarness) postForm(t *testing.T, path string, form url.Values) string {
	t.Helper()
	resp, err := http.PostForm(h.srv.URL+path, form)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read POST %s: %v", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s = %d: %s", path, resp.StatusCode, body)
	}
	return string(body)
}

// writeWorkerUIRecord drops a config file and the worker's config record
// pointing at it, and returns the config path.
func writeWorkerUIRecord(t *testing.T, name, configYAML string) string {
	t.Helper()
	conf := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(conf, []byte(configYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(daemonDir, "worker-"+name+".conf"), []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	return conf
}

func uiWorkerConfig(hostPort, queue string) string {
	return "agent: claude\ntemporal:\n  host: " + hostPort + "\n  task_queue: " + queue + "\n"
}

// uiRow builds one listed execution: ids, the Temporal status enum, an
// optional DaedalusStatus stamp ("" = none), the start time, and an
// optional close time (nil = live).
func uiRow(t *testing.T, id, runID string, st enums.WorkflowExecutionStatus, stamp workflows.RunStatus, started time.Time, closed *time.Time) *workflowpb.WorkflowExecutionInfo {
	t.Helper()
	row := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID},
		Status:    st,
		StartTime: timestamppb.New(started),
	}
	if closed != nil {
		row.CloseTime = timestamppb.New(*closed)
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

// uiStartedEvent builds the history's started event carrying in as the
// pipeline input — the run's labels every view decodes.
func uiStartedEvent(t *testing.T, in workflows.PipelineInput) *historypb.HistoryEvent {
	t.Helper()
	payload, err := converter.GetDefaultDataConverter().ToPayload(in)
	if err != nil {
		t.Fatalf("encode pipeline input: %v", err)
	}
	return &historypb.HistoryEvent{
		EventId:   1,
		EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}},
			},
		},
	}
}

// uiScheduled builds an activity-scheduled event naming typ and carrying
// input — the agent/role the rail and usage table attribute the round to.
func uiScheduled(t *testing.T, id int64, when time.Time, typ string, input any) *historypb.HistoryEvent {
	t.Helper()
	attrs := &historypb.ActivityTaskScheduledEventAttributes{
		ActivityId:   strconv.FormatInt(id, 10),
		ActivityType: &commonpb.ActivityType{Name: typ},
	}
	if input != nil {
		payload, err := converter.GetDefaultDataConverter().ToPayload(input)
		if err != nil {
			t.Fatalf("encode scheduled input: %v", err)
		}
		attrs.Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}}
	}
	return &historypb.HistoryEvent{
		EventId:    id,
		EventTime:  timestamppb.New(when),
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: attrs},
	}
}

// uiFailedEvent builds an activity-failed event for the scheduled event id,
// carrying message — the failure the detail rail renders as a failed round
// and the cause card quotes verbatim.
func uiFailedEvent(scheduledID int64, when time.Time, message string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   scheduledID + 1,
		EventTime: timestamppb.New(when),
		Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
			ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
				ScheduledEventId: scheduledID,
				Failure:          &failurepb.Failure{Message: message},
			},
		},
	}
}

// warmProbes drives the roster's async probe cache to quiescence: the
// first GET spawns the config's background probe, later polls pick its
// result up, and the loop re-renders until no cold "probing…" face
// remains — so assertions see landed probes, not the cold-cache answer.
func warmProbes(t *testing.T, h *uiHarness, path string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		out := h.get(t, path)
		if !strings.Contains(out, "probing…") || time.Now().After(deadline) {
			return out
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestUIWorkersPage pins the roster view: an empty host says how to start
// one; a recorded stopped worker renders its name, its not-running face,
// and the queue its config names; the page shell embeds the polled rows;
// and a live worker without provider wiring shows the n/a badge, never a
// fabricated probe.
func TestUIWorkersPage(t *testing.T) {
	t.Run("an empty host says how to start one", func(t *testing.T) {
		h := newUIHarness(t)
		if out := h.get(t, "/workers/rows"); !strings.Contains(out, "No workers on record") {
			t.Errorf("rows = %q, want the start-one note", out)
		}
	})

	t.Run("a recorded stopped worker renders with its queue", func(t *testing.T) {
		h := newUIHarness(t)
		writeWorkerUIRecord(t, "w1", uiWorkerConfig(h.cfg.Temporal.Host, "listq"))
		started := time.Now().Add(-2 * time.Hour)
		h.fake.rows = []*workflowpb.WorkflowExecutionInfo{
			uiRow(t, "listq-live", "run-live", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, workflows.StatusRunning, started, nil),
			uiRow(t, "listq-done", "run-done", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, workflows.StatusApproved, started.Add(-time.Hour), &time.Time{}),
		}

		out := h.get(t, "/workers/rows")
		for _, want := range []string{"w1", "not running", "listq", "New Task"} {
			if !strings.Contains(out, want) {
				t.Errorf("rows output lacks %q:\n%s", want, out)
			}
		}
		// Queue depth counts the live statuses on the worker's queue — the
		// running row alone here (the approved close is not live).
		if !strings.Contains(out, `queue depth <span class="mono text-slate-900 dark:text-slate-100">1</span>`) {
			t.Errorf("rows output lacks the queue-depth count:\n%s", out)
		}

		page := h.get(t, "/")
		if !strings.Contains(page, "Workers on this host") || !strings.Contains(page, "w1") {
			t.Errorf("landing page should embed the same roster")
		}
	})

	t.Run("a live worker without provider wiring shows n/a", func(t *testing.T) {
		h := newUIHarness(t)
		writeWorkerUIRecord(t, "w2", uiWorkerConfig(h.cfg.Temporal.Host, "listq"))
		writeLivePidFile(t, "w2")

		out := warmProbes(t, h, "/workers/rows")
		if !strings.Contains(out, "running · pid ") {
			t.Errorf("rows output lacks the running face:\n%s", out)
		}
		if !strings.Contains(out, "n/a (provider not configured)") {
			t.Errorf("rows output lacks the provider n/a badge:\n%s", out)
		}
	})
}

// TestUIQueueWorkCard pins the in-flight work line: a live worker's card
// derives the queue's current round from the running run's task log — the
// same parser `daedalus log` uses — naming repo, issue, stage, and round.
func TestUIQueueWorkCard(t *testing.T) {
	h := newUIHarness(t)
	writeWorkerUIRecord(t, "w3", uiWorkerConfig(h.cfg.Temporal.Host, "listq"))
	writeLivePidFile(t, "w3")
	h.fake.rows = []*workflowpb.WorkflowExecutionInfo{
		uiRow(t, "listq-iss42", "run-42", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, workflows.StatusRunning, time.Now().Add(-time.Hour), nil),
	}
	h.fake.wfHistory["listq-iss42"] = []*historypb.HistoryEvent{
		uiStartedEvent(t, workflows.PipelineInput{IssueID: "42", RepoPath: "/repo/alpha"}),
	}

	reset := activities.TaskLogDir
	t.Cleanup(func() { activities.TaskLogDir = reset })
	activities.TaskLogDir = t.TempDir()
	log := "=== 2026-10-14T10:00:00Z jailed pi round started: stage=implement (run 1) ===\n"
	if err := os.WriteFile(filepath.Join(activities.TaskLogDir, "listq-iss42.log"), []byte(log), 0o644); err != nil {
		t.Fatal(err)
	}

	out := h.get(t, "/workers/rows")
	for _, want := range []string{
		`<span class="mono">/repo/alpha</span>`,
		`<span class="mono">#42</span>`,
		"implement (round 1)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rows output lacks the in-flight work line's %q:\n%s", want, out)
		}
	}
}

// TestUIRunsViewFilters pins the runs feed: one walk feeds the table, rows
// sort newest-first by start time, the display statuses render through the
// shared derivation, and the server-side queue and status filters keep only
// the matching rows. A failed visibility walk degrades to the feed-down
// note while the roster keeps rendering.
func TestUIRunsViewFilters(t *testing.T) {
	h := newUIHarness(t)
	started := time.Now().Add(-2 * time.Hour)
	closed := time.Now().Add(-30 * time.Minute)
	h.fake.rows = []*workflowpb.WorkflowExecutionInfo{
		uiRow(t, "listq-live", "run-live", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, workflows.StatusRunning, started.Add(time.Hour), nil),
		uiRow(t, "listq-wait", "run-wait", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, workflows.StatusWaiting, started.Add(30*time.Minute), nil),
		uiRow(t, "listq-done", "run-done", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, workflows.StatusApproved, started, &closed),
	}
	h.fake.wfHistory["listq-live"] = []*historypb.HistoryEvent{
		uiStartedEvent(t, workflows.PipelineInput{IssueID: "42", RepoPath: "/repo/alpha"}),
	}

	out := h.get(t, "/runs/rows")
	for _, id := range []string{"listq-live", "listq-wait", "listq-done"} {
		if !strings.Contains(out, id) {
			t.Errorf("feed lacks %q:\n%s", id, out)
		}
	}
	// Newest first — the live run started an hour later than the waiting
	// one, the approved close an hour before that.
	if i, j, k := strings.Index(out, "listq-live"), strings.Index(out, "listq-wait"), strings.Index(out, "listq-done"); !(i < j && j < k) {
		t.Errorf("feed order wrong: live@%d wait@%d done@%d, want newest first", i, j, k)
	}
	// The started input's labels ride the row: repo and issue.
	for _, want := range []string{"/repo/alpha", "#42"} {
		if !strings.Contains(out, want) {
			t.Errorf("feed lacks the run label %q:\n%s", want, out)
		}
	}
	for _, want := range []string{">running</span>", ">waiting</span>", ">completed</span>"} {
		if !strings.Contains(out, want) {
			t.Errorf("feed lacks the %s status badge:\n%s", want, out)
		}
	}

	filtered := h.get(t, "/runs/rows?status=running")
	if strings.Contains(filtered, "listq-wait") || strings.Contains(filtered, "listq-done") || !strings.Contains(filtered, "listq-live") {
		t.Errorf("status=running kept the wrong rows:\n%s", filtered)
	}
	filtered = h.get(t, "/runs/rows?status=completed")
	if !strings.Contains(filtered, "listq-done") || strings.Contains(filtered, "listq-live") {
		t.Errorf("status=completed kept the wrong rows:\n%s", filtered)
	}
	if out := h.get(t, "/runs/rows?status=parked"); !strings.Contains(out, "No runs match.") {
		t.Errorf("status=parked = %q, want the no-match note", out)
	}
	if out := h.get(t, "/runs/rows?queue=otherq"); !strings.Contains(out, "No runs match.") {
		t.Errorf("queue=otherq = %q, want the no-match note", out)
	}
	if out := h.get(t, "/runs/rows?queue=listq"); !strings.Contains(out, "listq-live") || !strings.Contains(out, "listq-done") {
		t.Errorf("queue=listq dropped rows:\n%s", out)
	}
}

// TestUIWorkerProbeTooltips pins the probe badges' hover faces: the title
// carries the probe's own detail plus the TTL-cached caveat, and differs on
// whether the probe reached the provider — the roster's "will the next
// round reach the API?" answer, readable without re-running a probe. The
// fallback badge only exists when the config arms one, and carries the
// fallback probe's own face.
func TestUIWorkerProbeTooltips(t *testing.T) {
	rec := func(t *testing.T, h *uiHarness, name, providerYAML string) {
		t.Helper()
		writeWorkerUIRecord(t, name, uiWorkerConfig(h.cfg.Temporal.Host, "listq")+providerYAML)
		writeLivePidFile(t, name)
	}

	t.Run("a reached primary explains itself", func(t *testing.T) {
		h := newUIHarness(t)
		prov := providerServer(t, http.StatusOK, "{}")
		rec(t, h, "w-ok", "provider:\n  type: anthropic\n  url: "+prov+"\n  key: k\n  model: m\n")
		out := warmProbes(t, h, "/workers/rows")
		if !strings.Contains(out, `title="last probe reached the primary provider (ok (http 200)); TTL-cached, may be stale"`) {
			t.Errorf("rows lack the reached-primary tooltip:\n%s", out)
		}
		if !strings.Contains(out, ">primary ✓</span>") {
			t.Errorf("rows lack the primary ✓ badge:\n%s", out)
		}
	})

	t.Run("a failed primary warns of stall or failover", func(t *testing.T) {
		h := newUIHarness(t)
		prov := providerServer(t, http.StatusUnauthorized, "no")
		rec(t, h, "w-bad", "provider:\n  type: anthropic\n  url: "+prov+"\n  key: k\n  model: m\n")
		out := warmProbes(t, h, "/workers/rows")
		if !strings.Contains(out, `title="last probe failed (401 auth rejected) — next rounds may stall or fail over to the fallback; TTL-cached, may be stale"`) {
			t.Errorf("rows lack the failed-primary tooltip:\n%s", out)
		}
		if !strings.Contains(out, ">primary ✗</span>") {
			t.Errorf("rows lack the primary ✗ badge:\n%s", out)
		}
	})

	t.Run("an armed fallback carries its own face", func(t *testing.T) {
		h := newUIHarness(t)
		main := providerServer(t, http.StatusUnauthorized, "no")
		fb := providerServer(t, http.StatusOK, "{}")
		rec(t, h, "w-fb", "provider:\n  type: anthropic\n  url: "+main+"\n  key: k\n  model: m\n"+
			"fallback:\n  enabled: true\n  url: "+fb+"\n  key: fk\n  model: fm\n")
		out := warmProbes(t, h, "/workers/rows")
		for _, want := range []string{
			`title="last probe reached the fallback provider (ok (http 200)); TTL-cached, may be stale"`,
			">fallback ✓</span>",
			">primary ✗</span>",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("rows lack %q:\n%s", want, out)
			}
		}
	})
}

// TestUIRunsFeedDownNotesTheWalk pins the degraded face: a failing
// visibility walk renders the feed-down note (and empties the table)
// without failing the request, and the workers view keeps its roster.
func TestUIRunsFeedDownNotesTheWalk(t *testing.T) {
	h := newUIHarness(t)
	writeWorkerUIRecord(t, "w1", uiWorkerConfig(h.cfg.Temporal.Host, "listq"))
	h.fake.listErr = status.Error(codes.Unavailable, "visibility down")

	out := h.get(t, "/runs/rows")
	if !strings.Contains(out, "Run feed unavailable:") {
		t.Errorf("runs rows = %q, want the feed-down note", out)
	}
	if workers := h.get(t, "/workers/rows"); !strings.Contains(workers, "w1") {
		t.Errorf("workers rows = %q, want the roster regardless of the walk", workers)
	}
}

// TestUIPolledFragmentsAnswer204WhenUnchanged pins renderFrag through all
// five polled endpoints: a fragment whose fresh bytes equal the last bytes
// sent for that URI answers 204 No Content — htmx counts 204 a successful
// poll but never swaps it, so stable content leaves the DOM (and a click
// mid-poll) alone. Changed data re-sends, the cache keys on the full
// request URI (the runs feed's filter state rides it), and the page shells
// keep answering 200 whole on every poll.
func TestUIPolledFragmentsAnswer204WhenUnchanged(t *testing.T) {
	h := newUIHarness(t)
	started := time.Now().Add(-2 * time.Hour)
	h.fake.rows = []*workflowpb.WorkflowExecutionInfo{
		uiRow(t, "listq-live", "run-live", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, workflows.StatusRunning, started, nil),
	}
	h.fake.wfHistory["listq-live"] = []*historypb.HistoryEvent{
		uiStartedEvent(t, workflows.PipelineInput{IssueID: "42", RepoPath: "", Flow: "feature-dev"}),
	}
	h.fake.describes["listq-live"] = &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: "listq-live", RunId: "run-live"},
			TaskQueue: "listq",
			Status:    enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(started),
		},
	}

	// The workers fragment: first sight sends, the unchanged repeat does
	// not.
	if code, out := h.poll(t, "/workers/rows"); code != http.StatusOK || !strings.Contains(out, "No workers on record") {
		t.Fatalf("first /workers/rows poll = %d, want 200 with the roster", code)
	}
	if code, out := h.poll(t, "/workers/rows"); code != http.StatusNoContent || out != "" {
		t.Errorf("unchanged /workers/rows poll = %d, %q; want 204 with no body", code, out)
	}

	// The runs fragment: the dedupe holds until the feed's bytes change,
	// then the fresh rows ride a 200 again.
	if code, out := h.poll(t, "/runs/rows"); code != http.StatusOK || !strings.Contains(out, "listq-live") {
		t.Fatalf("first /runs/rows poll = %d, want 200 with the feed", code)
	}
	if code, _ := h.poll(t, "/runs/rows"); code != http.StatusNoContent {
		t.Errorf("unchanged /runs/rows poll = %d, want 204", code)
	}
	h.fake.rows = append(h.fake.rows,
		uiRow(t, "listq-two", "run-two", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, workflows.StatusWaiting, started.Add(time.Hour), nil))
	if code, out := h.poll(t, "/runs/rows"); code != http.StatusOK || !strings.Contains(out, "listq-two") {
		t.Errorf("changed /runs/rows poll = %d, want 200 re-sending the new row", code)
	}

	// The key is the request URI: a filter's URI was never sent, so it
	// answers 200 even though the unfiltered feed just deduped — and
	// dedupes on its own repeat.
	if code, out := h.poll(t, "/runs/rows?status=running"); code != http.StatusOK || !strings.Contains(out, "listq-live") {
		t.Errorf("first filtered poll = %d, want 200 (a new URI, not the unfiltered one)", code)
	}
	if code, _ := h.poll(t, "/runs/rows?status=running"); code != http.StatusNoContent {
		t.Errorf("unchanged filtered poll = %d, want 204", code)
	}

	// The run-detail fragment dedupes the same way.
	if code, out := h.poll(t, "/runs/listq-live/body"); code != http.StatusOK || !strings.Contains(out, "listq-live") {
		t.Fatalf("first /runs/listq-live/body poll = %d, want 200 with the detail", code)
	}
	if code, _ := h.poll(t, "/runs/listq-live/body"); code != http.StatusNoContent {
		t.Errorf("unchanged detail poll = %d, want 204", code)
	}

	// The models body fragment: with an empty ledger the read-only face is
	// stable, so the repeat dedupes — and a fresh entry rides a 200. No
	// unchanged-ledger face is pinned past that: with entries the charts
	// re-derive their bucket grid from the render clock, so byte-stability
	// between two renders is timing luck, not behavior — the tick simply
	// keeps the models view current.
	if code, out := h.poll(t, "/models/body"); code != http.StatusOK || !strings.Contains(out, "Usage by model") {
		t.Fatalf("first /models/body poll = %d, want 200 with the models view", code)
	}
	if code, _ := h.poll(t, "/models/body"); code != http.StatusNoContent {
		t.Errorf("unchanged empty /models/body poll = %d, want 204", code)
	}
	h.led.Append(metricsrelay.Entry{At: time.Now(), Worker: "w-m", Wire: metricsrelay.WireOpenAI, Model: "mz",
		InputTokens: 5, OutputTokens: 6, Status: 200})
	if code, out := h.poll(t, "/models/body"); code != http.StatusOK || !strings.Contains(out, "mz") {
		t.Errorf("changed /models/body poll = %d, want 200 re-sending the new model row", code)
	}

	// The metrics body fragment: chartless today (the GoLens embed is
	// absent from this build), so the face is stable between ledger
	// changes and the dedupe holds. The seeded entry's age stays in the
	// milliseconds across the two polls, five orders below the window
	// note's half-second rounding edge, so both renders agree.
	h.led.Append(metricsrelay.Entry{At: time.Now(), Worker: "w-m", InputTokens: 3, OutputTokens: 4, Status: 200})
	if code, out := h.poll(t, "/metrics/body"); code != http.StatusOK || !strings.Contains(out, ">w-m</td>") {
		t.Fatalf("first /metrics/body poll = %d, want 200 with the ledger row", code)
	}
	if code, _ := h.poll(t, "/metrics/body"); code != http.StatusNoContent {
		t.Errorf("unchanged /metrics/body poll = %d, want 204", code)
	}
	h.led.Append(metricsrelay.Entry{At: time.Now(), Worker: "w-n", InputTokens: 1, OutputTokens: 2, Status: 200})
	if code, out := h.poll(t, "/metrics/body"); code != http.StatusOK || !strings.Contains(out, ">w-n</td>") {
		t.Errorf("changed /metrics/body poll = %d, want 200 re-sending the new row", code)
	}

	// Pages are not fragments: they render 200 whole on every request, so
	// the shell's own markup never depends on poll history.
	for i := 0; i < 2; i++ {
		if code, out := h.poll(t, "/"); code != http.StatusOK || !strings.Contains(out, "Workers on this host") {
			t.Errorf("page / poll %d = %d, want 200 with the shell every time", i+1, code)
		}
		if code, out := h.poll(t, "/runs"); code != http.StatusOK || !strings.Contains(out, "listq-live") {
			t.Errorf("page /runs poll %d = %d, want 200 with the shell every time", i+1, code)
		}
	}
}

// TestUIRunDetailErrorFaceHeals pins the polled error face's cache
// membership: runBody routes a failed detail walk through renderFrag under
// the poll's URI, so identical errors dedupe into 204 (the DOM already
// holds the card) and a recovery re-sends 200 — the cached bytes are the
// error, so the restored healthy render can never dedupe against them.
// An uncached error render is exactly the regression this fails on: the
// cache would keep holding the stale healthy bytes from step one, the
// recovery poll would 204, and the error card would stay pinned in the
// DOM. Steps 2-4 cross a dropClient and re-dial.
func TestUIRunDetailErrorFaceHeals(t *testing.T) {
	h := newUIHarness(t)
	started := time.Now().Add(-time.Hour)
	h.fake.wfHistory["listq-live"] = []*historypb.HistoryEvent{
		uiStartedEvent(t, workflows.PipelineInput{IssueID: "42", RepoPath: "", Flow: "feature-dev"}),
	}
	describe := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: "listq-live", RunId: "run-live"},
			TaskQueue: "listq",
			Status:    enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(started),
		},
	}
	h.fake.describes["listq-live"] = describe

	// Healthy: the detail fragment sends.
	if code, out := h.poll(t, "/runs/listq-live/body"); code != http.StatusOK || !strings.Contains(out, "listq-live") {
		t.Fatalf("healthy poll = %d, want 200 with the detail", code)
	}

	// The walk fails: the error face rides a 200 too — it must swap in.
	delete(h.fake.describes, "listq-live")
	if code, out := h.poll(t, "/runs/listq-live/body"); code != http.StatusOK || !strings.Contains(out, "workflow not found: listq-live") {
		t.Fatalf("erroring poll = %d, want 200 with the describe diagnostic", code)
	}
	// The identical error dedupes — harmless, the DOM already holds it.
	if code, out := h.poll(t, "/runs/listq-live/body"); code != http.StatusNoContent || out != "" {
		t.Errorf("same-error poll = %d, %q; want 204 with no body", code, out)
	}

	// Recovery: the healthy bytes differ from the cached error, so the
	// fragment re-sends — never 204 while the DOM holds the error face.
	h.fake.describes["listq-live"] = describe
	if code, out := h.poll(t, "/runs/listq-live/body"); code != http.StatusOK || !strings.Contains(out, "listq-live") {
		t.Fatalf("recovery poll = %d, want 200 re-sending the detail", code)
	}
	if code, _ := h.poll(t, "/runs/listq-live/body"); code != http.StatusNoContent {
		t.Errorf("stable poll = %d, want 204", code)
	}
}

// TestUIFragmentCacheCapDropsWholesale pins the fragment cache's bound:
// past uiCacheCap entries the map drops wholesale, so a URI last seen
// before the drop answers 200 again — a reset costs a re-send, never
// staleness. Exercised at the renderFrag seam: filling the cap through the
// server would re-walk the host once per poll to test a map reset.
func TestUIFragmentCacheCapDropsWholesale(t *testing.T) {
	s := newUIServer(config.Config{}, nil)
	poll := func(uri string) int {
		t.Helper()
		w := httptest.NewRecorder()
		s.renderFrag(w, httptest.NewRequest("GET", uri, nil), "frag_workers", uiSnapshot{})
		return w.Code
	}
	if code := poll("/workers/rows?only"); code != http.StatusOK {
		t.Fatalf("first poll = %d, want 200", code)
	}
	if code := poll("/workers/rows?only"); code != http.StatusNoContent {
		t.Fatalf("unchanged poll = %d, want 204", code)
	}
	for i := 0; i < uiCacheCap; i++ {
		if code := poll("/workers/rows?f=" + strconv.Itoa(i)); code != http.StatusOK {
			t.Fatalf("fill poll %d = %d, want 200", i, code)
		}
	}
	if code := poll("/workers/rows?only"); code != http.StatusOK {
		t.Errorf("pre-drop URI answered %d past the cap, want a re-sending 200 (the drop is wholesale)", code)
	}
}

// TestUIRunDetailPage pins the run-detail walk: the header's labels decode
// from the run's started input and describe answer, the rail orders the
// rounds with their verdicts, the usage table carries each round's
// captured figures and the total, the suite panel shows the command and
// pass state, and the Temporal UI link points off-host at the audit view.
func TestUIRunDetailPage(t *testing.T) {
	h := newUIHarness(t)
	id := "listq-iss9"
	statusPayload, err := converter.GetDefaultDataConverter().ToPayload(workflows.StatusRunning)
	if err != nil {
		t.Fatal(err)
	}
	depPayload, err := converter.GetDefaultDataConverter().ToPayload("listq-dep")
	if err != nil {
		t.Fatal(err)
	}
	h.fake.describes[id] = &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: "run-9"},
			TaskQueue: "listq",
			Status:    enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(time.Now().Add(-time.Hour)),
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				workflows.DaedalusStatusAttr:    statusPayload,
				workflows.DaedalusDependsOnAttr: depPayload,
			}},
		},
	}
	at := time.Now().Add(-30 * time.Minute)
	h.fake.wfHistory[id] = []*historypb.HistoryEvent{
		uiStartedEvent(t, workflows.PipelineInput{IssueID: "9", RepoPath: "", Flow: "feature-dev"}),
		uiScheduled(t, 4, at, "RunJailedClaudeActivity", activities.AgentRunInput{Agent: "pi", Role: activities.RoleDev}),
		completedEvent(t, 4, at.Add(time.Minute), activities.AgentRunResult{Usage: activities.Usage{InputTokens: 100, OutputTokens: 50, CostUSD: 0.25}}),
		uiScheduled(t, 6, at.Add(2*time.Minute), "RunJailedReviewerActivity", activities.ReviewInput{Agent: "claude"}),
		completedEvent(t, 6, at.Add(3*time.Minute), activities.ReviewResult{Approved: true, Comments: "ship it", Usage: activities.Usage{CostUSD: 0.75}}),
		uiScheduled(t, 8, at.Add(4*time.Minute), "RunTestSuiteActivity", nil),
		completedEvent(t, 8, at.Add(5*time.Minute), activities.TestResult{Passed: true, Command: "go test ./...", Logs: "ok\nPASS\n"}),
	}

	out := h.get(t, "/runs/"+id)
	for _, want := range []string{
		id, "run-9", "#9", "feature-dev", "listq-dep", ">running</span>",
		"implement", "review", "pi", "claude",
		"APPROVED", "ship it",
		"suite green", "go test ./...",
		">100</td>", ">50</td>", "1m0s",
		"1.0000",
		"http://127.0.0.1:8233/namespaces/default/workflows/listq-iss9",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("detail page lacks %q", want)
		}
	}

	// The polled body endpoint renders the same fragment directly — the
	// branch page_run's embed exercises through .D, so a regression that
	// hands frag_run the chrome wrapper ships green without this pin (it
	// 500s here: h.get fails on any non-200).
	body := h.get(t, "/runs/"+id+"/body")
	for _, want := range []string{id, ">running</span>", "implement", "APPROVED"} {
		if !strings.Contains(body, want) {
			t.Errorf("polled body lacks %q", want)
		}
	}
}

// TestUIRunDetailUnknownRun pins the degraded detail: a run the frontend
// does not know renders the error panel with the diagnostic — on both the
// page and its polled body endpoint; neither fails the request whole.
func TestUIRunDetailUnknownRun(t *testing.T) {
	h := newUIHarness(t)
	out := h.get(t, "/runs/listq-ghost")
	if !strings.Contains(out, "describe listq-ghost") {
		t.Errorf("unknown-run page = %q, want the describe diagnostic", out)
	}
	if body := h.get(t, "/runs/listq-ghost/body"); !strings.Contains(body, "describe listq-ghost") {
		t.Errorf("unknown-run body = %q, want the describe diagnostic", body)
	}
}

// TestUIRunDetailCauseCard pins the closed-run cause card: a parked run's
// page quotes the park's own reason (the probe's trimmed PARKED result), a
// failed run's quotes the last activity failure's message (the walk is
// chronological, so the proximate failure wins), an aborted run carries
// the resumable hint without a park reason (the reason is what tells park
// and abort apart), and a running run renders no card at all — nothing is
// closed to resume.
func TestUIRunDetailCauseCard(t *testing.T) {
	h := newUIHarness(t)
	started := time.Now().Add(-time.Hour)
	closed := time.Now().Add(-30 * time.Minute)
	build := func(t *testing.T, id, runID string, st enums.WorkflowExecutionStatus, stamp workflows.RunStatus, closedRun bool, hist []*historypb.HistoryEvent) {
		t.Helper()
		info := &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID},
			TaskQueue: "listq",
			Status:    st,
			StartTime: timestamppb.New(started),
		}
		if closedRun {
			info.CloseTime = timestamppb.New(closed)
		}
		if stamp != "" {
			payload, err := converter.GetDefaultDataConverter().ToPayload(stamp)
			if err != nil {
				t.Fatalf("encode DaedalusStatus %q: %v", stamp, err)
			}
			info.SearchAttributes = &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				workflows.DaedalusStatusAttr: payload,
			}}
		}
		h.fake.describes[id] = &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: info}
		h.fake.wfHistory[id] = hist
	}
	labels := workflows.PipelineInput{IssueID: "42", RepoPath: "/repo/alpha", Flow: "feature-dev"}
	agentRound := func(id int64, at time.Time) []*historypb.HistoryEvent {
		return []*historypb.HistoryEvent{
			uiScheduled(t, id, at, "RunJailedClaudeActivity", activities.AgentRunInput{Agent: "pi", Role: activities.RoleDev}),
		}
	}

	t.Run("a parked run quotes the park's own reason", func(t *testing.T) {
		build(t, "listq-hold", "run-hold", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, workflows.StatusParked, true,
			[]*historypb.HistoryEvent{uiStartedEvent(t, labels)})
		h.fake.history["run-hold"] = greenClose(t, workflows.ParkedResult("reviewer halted the run"))

		out := h.get(t, "/runs/listq-hold")
		for _, want := range []string{
			"park reason: reviewer halted the run",
			"daedalus continue listq-hold",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("parked detail lacks %q:\n%s", want, out)
			}
		}
		if strings.Contains(out, "failure: ") {
			t.Errorf("parked detail fabricates a failure cause:\n%s", out)
		}
	})

	t.Run("a failed run quotes the last activity failure", func(t *testing.T) {
		at := time.Now().Add(-45 * time.Minute)
		hist := agentRound(4, at.Add(-5*time.Minute))
		hist = append(hist, uiFailedEvent(4, at, "the provider refused the key"))
		hist = append(hist,
			uiScheduled(t, 6, at.Add(time.Minute), "RunJailedClaudeActivity", activities.AgentRunInput{Agent: "pi", Role: activities.RoleDev}))
		hist = append(hist, uiFailedEvent(6, at.Add(2*time.Minute), "the suite fell over after round 2"))
		build(t, "listq-fell", "run-fell", enums.WORKFLOW_EXECUTION_STATUS_FAILED, workflows.StatusFailed, true, hist)

		out := h.get(t, "/runs/listq-fell")
		for _, want := range []string{
			"failure: the suite fell over after round 2",
			"daedalus continue listq-fell",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("failed detail lacks %q:\n%s", want, out)
			}
		}
		if strings.Contains(out, "the provider refused the key") {
			t.Errorf("failed detail quotes an earlier failure over the proximate one:\n%s", out)
		}
		if strings.Contains(out, "park reason: ") {
			t.Errorf("failed detail fabricates a park reason:\n%s", out)
		}
	})

	t.Run("an aborted run is resumable without a park reason", func(t *testing.T) {
		build(t, "listq-abort", "run-abort", enums.WORKFLOW_EXECUTION_STATUS_CANCELED, "", true,
			[]*historypb.HistoryEvent{uiStartedEvent(t, labels)})

		out := h.get(t, "/runs/listq-abort")
		if !strings.Contains(out, "daedalus continue listq-abort") {
			t.Errorf("aborted detail lacks the resumable hint:\n%s", out)
		}
		if strings.Contains(out, "park reason: ") {
			t.Errorf("an aborted run must not borrow a park reason (the probe cannot read one):\n%s", out)
		}
	})

	t.Run("a running run renders no card", func(t *testing.T) {
		build(t, "listq-open", "run-open", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, "", false,
			[]*historypb.HistoryEvent{uiStartedEvent(t, labels)})

		out := h.get(t, "/runs/listq-open")
		for _, want := range []string{"daedalus continue", "park reason: ", "failure: "} {
			if strings.Contains(out, want) {
				t.Errorf("running detail carries the closed-run face %q:\n%s", want, out)
			}
		}
	})
}

// TestUIPolledPageFaces pins the polling client's decisions as the page
// shells ship them: every polled region gates its poll on a visible tab,
// the runs region re-renders the moment a filter changes, an active
// selection vetoes the swap that would destroy it, the excerpt's scroll
// offset rides the swap both ways, a failed poll announces itself in the
// header, and the every-trigger probe is what hides the countdown ring on
// a page with nothing polling instead of leaving it clamped at zero. The
// scripts ship inside the shells; these pins hold their decision lines so
// a template refactor cannot silently drop them.
func TestUIPolledPageFaces(t *testing.T) {
	h := newUIHarness(t)
	started := time.Now().Add(-time.Hour)
	h.fake.rows = []*workflowpb.WorkflowExecutionInfo{
		uiRow(t, "listq-live", "run-live", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, workflows.StatusRunning, started, nil),
	}
	h.fake.wfHistory["listq-live"] = []*historypb.HistoryEvent{
		uiStartedEvent(t, workflows.PipelineInput{IssueID: "42", RepoPath: "", Flow: "feature-dev"}),
	}
	h.fake.describes["listq-live"] = &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: "listq-live", RunId: "run-live"},
			TaskQueue: "listq",
			Status:    enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(started),
		},
	}
	index := h.get(t, "/")
	runs := h.get(t, "/runs")
	detail := h.get(t, "/runs/listq-live")

	t.Run("background tabs stop polling", func(t *testing.T) {
		if i := strings.Index(index, `id="wlist" hx-get="/workers/rows" hx-trigger="every 5s [!document.hidden]"`); i < 0 {
			t.Errorf("the workers region lost its hidden gate:\n%s", index)
		}
		if !strings.Contains(detail, `hx-trigger="every 5s [!document.hidden]"`) {
			t.Errorf("the run-detail region lost its hidden gate:\n%s", detail)
		}
		if !strings.Contains(runs, `hx-trigger="every 5s [!document.hidden], change from:#fq, change from:#fs"`) {
			t.Errorf("the runs region lost its hidden gate or its filter change triggers:\n%s", runs)
		}
	})

	t.Run("an active selection vetoes the swap it would destroy", func(t *testing.T) {
		for _, want := range []string{
			`if(sel.rangeCount&&!sel.isCollapsed&&t.contains(sel.getRangeAt(0).commonAncestorContainer)){`,
			`ev.detail.shouldSwap=false;`,
		} {
			if !strings.Contains(index, want) {
				t.Errorf("the page shell lacks the selection veto's %q:\n%s", want, index)
			}
		}
	})

	t.Run("the excerpt scroll rides the swap", func(t *testing.T) {
		for _, want := range []string{
			`if(pre)ddScroll=pre.scrollTop;`,
			`if(pre)pre.scrollTop=ddScroll;`,
		} {
			if !strings.Contains(index, want) {
				t.Errorf("the page shell lacks the scroll carry's %q:\n%s", want, index)
			}
		}
	})

	t.Run("a failed poll announces itself", func(t *testing.T) {
		if !strings.Contains(index, `lp.textContent='poll failed';`) {
			t.Errorf("the page shell lacks the poll-failed face:\n%s", index)
		}
	})

	t.Run("the ring hides where nothing polls", func(t *testing.T) {
		for _, want := range []string{
			`var ddLive=false;`,
			`if(!ddLive&&cdWrap)cdWrap.classList.add('hidden');`,
		} {
			if !strings.Contains(index, want) {
				t.Errorf("the page shell lacks the ring's every-trigger probe or its hide branch:\n%s", want)
			}
		}
	})
}

// TestUIPageA11yFaces pins the accessibility faces: the theme toggle
// exposes its pressed state and the script keeps it honest across toggles,
// the mode radios are sr-only (tab-reachable, focus ring on their label),
// every form label names its control, and the tables declare column
// headers.
func TestUIPageA11yFaces(t *testing.T) {
	h := newUIHarness(t)
	writeWorkerUIRecord(t, "w1", uiWorkerConfig(h.cfg.Temporal.Host, "listq"))

	t.Run("the theme toggle exposes its pressed state", func(t *testing.T) {
		page := h.get(t, "/")
		for _, want := range []string{
			`aria-label="toggle theme" aria-pressed="true"`,
			`ddTheme.setAttribute('aria-pressed',document.documentElement.classList.contains('dark'));`,
			`ddTheme.setAttribute('aria-pressed',d);`,
		} {
			if !strings.Contains(page, want) {
				t.Errorf("the theme toggle lacks %q:\n%s", want, page)
			}
		}
	})

	t.Run("the mode radios keep tab order and show their focus", func(t *testing.T) {
		page := h.get(t, "/new")
		for _, want := range []string{
			`<input type="radio" name="mode" value="text" class="sr-only"`,
			`<input type="radio" name="mode" value="file" class="sr-only"`,
			`.tab:has(input:focus-visible){@apply ring-2 ring-emerald-500/60;}`,
		} {
			if !strings.Contains(page, want) {
				t.Errorf("the mode toggle lacks %q:\n%s", want, page)
			}
		}
	})

	t.Run("every form label names its control", func(t *testing.T) {
		page := h.get(t, "/new")
		for _, id := range []string{"nq", "nwf", "nrepo", "nissue", "nprompt", "nprefix"} {
			for _, want := range []string{`for="` + id + `"`, `id="` + id + `"`} {
				if !strings.Contains(page, want) {
					t.Errorf("the form lacks the %q wiring:\n%s", want, page)
				}
			}
		}
		// The agent label ships in its fragment, which only a chosen
		// worker's page embeds.
		pre := h.get(t, "/new?worker=w1")
		if !strings.Contains(pre, `for="nagent"`) || !strings.Contains(pre, `id="nagent"`) {
			t.Errorf("the agent fragment lacks its label wiring:\n%s", pre)
		}
	})

	t.Run("tables declare column headers", func(t *testing.T) {
		runs := h.get(t, "/runs")
		if n := strings.Count(runs, `<th scope="col"`); n != 7 {
			t.Errorf("the runs table declares %d column headers, want 7:\n%s", n, runs)
		}
		metrics := h.get(t, "/metrics")
		if !strings.Contains(metrics, `<th scope="col" class="font-medium pb-2">Queue</th>`) {
			t.Errorf("the sessions table declares no column headers:\n%s", metrics)
		}
	})
}

// TestUIMetricsPage pins the metrics view: the relay ledger's aggregates
// (calls, the last-hour slice, token sums, the non-2xx count, per-worker
// rows ordered by call count), and the live session gauges computed from
// the same walk the runs feed renders.
func TestUIMetricsPage(t *testing.T) {
	h := newUIHarness(t)
	now := time.Now()
	h.led.Append(metricsrelay.Entry{At: now.Add(-2 * time.Minute), Worker: "w-a", InputTokens: 10, OutputTokens: 20, Status: 200})
	h.led.Append(metricsrelay.Entry{At: now.Add(-3 * time.Hour), Worker: "w-a", InputTokens: 5, OutputTokens: 6, Status: 500})
	h.led.Append(metricsrelay.Entry{At: now.Add(-20 * time.Second), Worker: "w-b", InputTokens: 7, OutputTokens: 8, Status: 200})

	started := time.Now().Add(-time.Hour)
	closed := time.Now().Add(-time.Minute)
	h.fake.rows = []*workflowpb.WorkflowExecutionInfo{
		uiRow(t, "listq-r1", "run-r1", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, workflows.StatusRunning, started, nil),
		uiRow(t, "listq-w1", "run-w1", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, workflows.StatusWaiting, started, nil),
		uiRow(t, "listq-p1", "run-p1", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, workflows.StatusPending, started, nil),
		uiRow(t, "listq-d1", "run-d1", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, workflows.StatusApproved, started, &closed),
	}

	out := h.get(t, "/metrics")
	// Per-worker aggregates: w-a leads with 2 calls, 15 in, 26 out, 1
	// error; w-b follows with 1 call, 7 in, 8 out, 0 errors.
	if !strings.Contains(out, `>w-a</td><td>2</td><td>15</td><td>26</td><td>1</td>`) {
		t.Errorf("metrics page lacks w-a's aggregate row:\n%s", out)
	}
	if !strings.Contains(out, `>w-b</td><td>1</td><td>7</td><td>8</td><td>0</td>`) {
		t.Errorf("metrics page lacks w-b's aggregate row:\n%s", out)
	}
	if i, j := strings.Index(out, ">w-a</td>"), strings.Index(out, ">w-b</td>"); !(i >= 0 && i < j) {
		t.Errorf("worker rows out of call-count order: w-a@%d w-b@%d", i, j)
	}
	if !strings.Contains(out, "2 in the last hour") {
		t.Errorf("metrics page lacks the last-hour count:\n%s", out)
	}
	if !strings.Contains(out, "window") {
		t.Errorf("metrics page lacks the ledger window note:\n%s", out)
	}
	// Sessions: one running, two waiting (waiting + pending), four listed.
	if !strings.Contains(out, `>listq</td><td>1</td><td>2</td><td>4</td>`) {
		t.Errorf("metrics page lacks the session gauges:\n%s", out)
	}
}

// TestUIMetricsBodyFragment pins the metrics view's polled body seam: the
// page shell wraps the fragment region in the poller the header countdown
// drives with the fragment's initial render inside it, and /metrics/body
// serves the same aggregates the page embeds — both render through the one
// metricsView walk, so the shell and its polled region can never
// disagree — while the page chrome stays out of the fragment.
func TestUIMetricsBodyFragment(t *testing.T) {
	h := newUIHarness(t)
	h.led.Append(metricsrelay.Entry{At: time.Now().Add(-time.Minute), Worker: "w-f", InputTokens: 1, OutputTokens: 2, Status: 200})

	page := h.get(t, "/metrics")
	if !strings.Contains(page, `hx-get="/metrics/body"`) {
		t.Errorf("the metrics page lost its polled region wrapper:\n%s", page)
	}
	if !strings.Contains(page, ">w-f</td>") {
		t.Errorf("the metrics page lost the fragment's initial render:\n%s", page)
	}

	body := h.get(t, "/metrics/body")
	for _, want := range []string{
		">w-f</td>",   // the per-worker ledger row
		"Relay calls", // the tile grid, the fragment's top
		"The GoLens embed is not built into this binary", // the note closes the region
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the metrics body fragment lacks %q:\n%s", want, body)
		}
	}
	for _, banned := range []string{"<main", "<html"} {
		if strings.Contains(body, banned) {
			t.Errorf("the metrics body fragment grew page chrome (%q):\n%s", banned, body)
		}
	}
}

// TestUINewTaskForm pins the New-Task page: the workers dropdown carries
// every record (a preselected one when named), the flow dropdown is the
// registry, and the agent dropdown derives from the target worker's own
// config — its options are the admitted agents, its default that config's
// agent; a worker without a record degrades to the pick-one note.
func TestUINewTaskForm(t *testing.T) {
	h := newUIHarness(t)
	writeWorkerUIRecord(t, "w1", uiWorkerConfig(h.cfg.Temporal.Host, "listq"))

	out := h.get(t, "/new")
	if !strings.Contains(out, "w1 (idle)") {
		t.Errorf("form lacks the worker option:\n%s", out)
	}
	if !strings.Contains(out, `>bug-fix</option>`) || !strings.Contains(out, `>test-only</option>`) {
		t.Errorf("form lacks the registry flows:\n%s", out)
	}
	if i, j := strings.Index(out, ">bug-fix</option>"), strings.Index(out, ">test-only</option>"); i > j {
		t.Errorf("flows dropdown out of registry sort order")
	}
	// With no worker chosen the agent slot is the pick-one note; the
	// dropdown itself only exists once a worker is named.
	if !strings.Contains(out, "pick a worker with a config record") {
		t.Errorf("agent slot should degrade to the pick-one note before a worker is chosen:\n%s", out)
	}

	pre := h.get(t, "/new?worker=w1")
	if !strings.Contains(pre, `<option value="w1" selected `) {
		t.Errorf("?worker=w1 should preselect the worker:\n%s", pre)
	}
	if !strings.Contains(pre, `<option value="claude" selected>claude</option>`) {
		t.Errorf("agent dropdown should default to the target config's agent:\n%s", pre)
	}

	if out := h.get(t, "/new/agents?worker=ghost"); !strings.Contains(out, "pick a worker with a config record") {
		t.Errorf("agent fragment for a record-less worker = %q, want the pick-one note", out)
	}
	writeWorkerUIRecord(t, "w-bad", ":::: not yaml :::")
	if out := h.get(t, "/new/agents?worker=w-bad"); !strings.Contains(out, "did not load") {
		t.Errorf("agent fragment for an unloadable config = %q, want the load diagnostic", out)
	}
}

// TestUISubmitNew pins the form's write path: every typed refusal lands in
// the result panel without dispatching, and a valid submission goes through
// runstart with the target worker's own config — its queue and host receive
// the run, the form's fields travel verbatim, an empty flow resolves
// through the config's default (the slim reroute included), and the
// branch-prefix override wins over the config's.
func TestUISubmitNew(t *testing.T) {
	newRepo := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		git := func(args ...string) {
			t.Helper()
			cmd := exec.Command("git", args...)
			cmd.Dir = dir
			cmd.Env = runstart.EnvWithoutGitRepoOverrides(os.Environ())
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("git %v: %v: %s", args, err, out)
			}
		}
		git("init", "-b", "main")
		git("config", "user.email", "t@example.com")
		git("config", "user.name", "t")
		if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		git("add", ".")
		git("commit", "-m", "init")
		return dir
	}

	base := func(repo string) url.Values {
		return url.Values{
			"worker":   {"w1"},
			"repo":     {repo},
			"issue":    {"iss1"},
			"mode":     {"text"},
			"prompt":   {"do it"},
			"workflow": {""},
		}
	}
	refused := func(t *testing.T, h *uiHarness, form url.Values, want string) {
		t.Helper()
		out := h.postForm(t, "/new", form)
		if !strings.Contains(out, want) {
			t.Fatalf("submit = %q, want the refusal naming %q", out, want)
		}
		if !strings.Contains(out, "Form state preserved.") {
			t.Errorf("the error panel lost the preserved-state note")
		}
		if n := len(h.fake.starts); n != 0 {
			t.Fatalf("the refusal dispatched %d starts, want 0", n)
		}
	}

	t.Run("the typed refusals", func(t *testing.T) {
		h := newUIHarness(t)
		writeWorkerUIRecord(t, "w1", uiWorkerConfig(h.cfg.Temporal.Host, "listq"))
		repo := newRepo(t)
		// form builds the valid body, then applies one deviation.
		form := func(deviate func(url.Values)) url.Values {
			v := base(repo)
			if deviate != nil {
				deviate(v)
			}
			return v
		}
		refused(t, h, form(func(v url.Values) { v.Del("worker") }), "choose a worker")
		refused(t, h, form(func(v url.Values) { v.Set("worker", "ghost") }), "no config record")
		// a record naming a config file that no longer loads
		if err := os.WriteFile(filepath.Join(daemonDir, "worker-w-dead.conf"), []byte(filepath.Join(t.TempDir(), "gone.yaml")), 0o644); err != nil {
			t.Fatal(err)
		}
		refused(t, h, form(func(v url.Values) { v.Set("worker", "w-dead") }), "did not load")
		refused(t, h, form(func(v url.Values) { v.Del("repo") }), "repo path is required")
		refused(t, h, form(func(v url.Values) { v.Del("issue") }), "issue id is required")
		refused(t, h, form(func(v url.Values) { v.Set("prompt", "   ") }), "provide the task instructions")
		refused(t, h, form(func(v url.Values) { v.Set("mode", "file"); v.Del("prompt") }), "file path is required")
		refused(t, h, form(func(v url.Values) { v.Set("mode", "file"); v.Set("file", "/nonexistent/task.md") }), "read task file")
		refused(t, h, form(func(v url.Values) { v.Set("agent", "bogus") }), "unknown agent")
	})

	t.Run("a dirty repo is refused, not asked about", func(t *testing.T) {
		h := newUIHarness(t)
		writeWorkerUIRecord(t, "w1", uiWorkerConfig(h.cfg.Temporal.Host, "listq"))
		repo := newRepo(t)
		if err := os.WriteFile(filepath.Join(repo, "wip.txt"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		refused(t, h, base(repo), "uncommitted changes")
	})

	t.Run("a valid submission dispatches through the target config", func(t *testing.T) {
		h := newUIHarness(t)
		writeWorkerUIRecord(t, "w1", uiWorkerConfig(h.cfg.Temporal.Host, "listq"))
		repo := newRepo(t)

		form := base(repo)
		form.Set("agent", "pi")
		form.Set("prefix", "px/")
		out := h.postForm(t, "/new", form)
		if !strings.Contains(out, "Task submitted to") || !strings.Contains(out, `/runs/listq-iss1`) {
			t.Fatalf("submit panel = %q, want the confirmation naming the run", out)
		}
		if len(h.fake.starts) != 1 {
			t.Fatalf("the fake served %d starts, want 1", len(h.fake.starts))
		}
		req := h.fake.starts[0]
		if req.GetWorkflowId() != "listq-iss1" {
			t.Errorf("workflow id = %q, want listq-iss1", req.GetWorkflowId())
		}
		if got := req.GetTaskQueue().GetName(); got != "listq" {
			t.Errorf("task queue = %q, want the target worker's listq", got)
		}
		var in workflows.PipelineInput
		if err := converter.GetDefaultDataConverter().FromPayload(req.GetInput().GetPayloads()[0], &in); err != nil {
			t.Fatalf("decode the dispatched input: %v", err)
		}
		if in.IssueID != "iss1" || in.Prompt != "do it" || in.Agent != "pi" || in.BranchPrefix != "px/" {
			t.Errorf("dispatched input = %+v, want the form's own fields", in)
		}
		if in.RepoPath != repo {
			t.Errorf("dispatched RepoPath = %q, want the absolute %q", in.RepoPath, repo)
		}
	})

	t.Run("file mode reads the task file as the prompt", func(t *testing.T) {
		h := newUIHarness(t)
		writeWorkerUIRecord(t, "w1", uiWorkerConfig(h.cfg.Temporal.Host, "listq"))
		repo := newRepo(t)
		task := filepath.Join(t.TempDir(), "task.md")
		if err := os.WriteFile(task, []byte("do the thing\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		form := base(repo)
		form.Set("mode", "file")
		form.Set("file", task)
		form.Del("prompt")
		if out := h.postForm(t, "/new", form); !strings.Contains(out, `/runs/listq-iss1`) {
			t.Fatalf("submit panel = %q, want the confirmation", out)
		}
		var in workflows.PipelineInput
		req := h.fake.starts[len(h.fake.starts)-1]
		if err := converter.GetDefaultDataConverter().FromPayload(req.GetInput().GetPayloads()[0], &in); err != nil {
			t.Fatal(err)
		}
		if in.Prompt != "do the thing\n" {
			t.Errorf("dispatched prompt = %q, want the task file's contents", in.Prompt)
		}
	})

	t.Run("an unset flow resolves through the config's slim default", func(t *testing.T) {
		h := newUIHarness(t)
		slimCfg := "agent: pi\ntemporal:\n  host: " + h.cfg.Temporal.Host + "\n  task_queue: listq\n" +
			"slim:\n  enabled: true\nverdict_repair:\n  url: http://127.0.0.1:1/v1\n  model: tev1:latest\n"
		writeWorkerUIRecord(t, "w-slim", slimCfg)
		repo := newRepo(t)

		form := base(repo)
		form.Set("worker", "w-slim")
		if out := h.postForm(t, "/new", form); !strings.Contains(out, `/runs/listq-slim-iss1`) {
			t.Fatalf("submit panel = %q, want the slim-scoped run", out)
		}
		req := h.fake.starts[len(h.fake.starts)-1]
		if want := workflows.WorkflowTypeName(workflows.SlimWorkflow); req.GetWorkflowType().GetName() != want {
			t.Errorf("workflow type = %q, want %q", req.GetWorkflowType().GetName(), want)
		}
		var in workflows.PipelineInput
		if err := converter.GetDefaultDataConverter().FromPayload(req.GetInput().GetPayloads()[0], &in); err != nil {
			t.Fatal(err)
		}
		if in.Flow != "slim" {
			t.Errorf("dispatched Flow = %q, want the slim reroute", in.Flow)
		}
	})
}

// TestClosedRunParkedMemoizes pins the park probe's memo: an ambiguous
// closed row is probed once per execution per process (the dashboard
// re-renders every poll), a transient read failure is not memoized — the
// next render retries — and a definitive answer is.
func TestClosedRunParkedMemoizes(t *testing.T) {
	fake := &fakeListServer{history: map[string]*historypb.HistoryEvent{}}
	cfg := startListServer(t, fake)
	c, err := newClient(cfg)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	defer c.Close()

	parked := greenClose(t, workflows.ParkedResult("reviewer halted the run"))
	fake.history["run-memo-a"] = parked

	if !closedRunParked(c, "wf-memo-a", "run-memo-a") {
		t.Fatal("a parked close probed false")
	}
	closedRunParked(c, "wf-memo-a", "run-memo-a")
	if n := len(fake.probes); n != 1 {
		t.Fatalf("the memo missed: %d probes for one execution, want 1", n)
	}

	// A failed read is "unknown", not "no": the next render retries.
	if closedRunParked(c, "wf-memo-b", "run-memo-b") {
		t.Fatal("an unreadable close probed true")
	}
	fake.history["run-memo-b"] = parked
	if !closedRunParked(c, "wf-memo-b", "run-memo-b") {
		t.Fatal("the retried read still probed false")
	}
	probesFor := func(runID string) int {
		n := 0
		for _, p := range fake.probes {
			if p.runID == runID {
				n++
			}
		}
		return n
	}
	if n := probesFor("run-memo-b"); n != 2 {
		t.Errorf("run-memo-b probed %d times, want 2 (the failed read must not memoize)", n)
	}
	if n := probesFor("run-memo-a"); n != 1 {
		t.Errorf("run-memo-a probed %d times, want 1 (the definitive answer memoizes)", n)
	}
}

// TestClosedRunParkReasonFaces pins the park probe's reason face: a green
// park yields the PARKED result's own text (prefix trimmed), the old
// workers' awaiting-maintainer failure yields the sentinel alone (the
// wrapped detail is the old workers' own), a plain close yields no reason,
// and the memo serves the reason with the parked answer — a repeat probes
// nothing and answers identically.
func TestClosedRunParkReasonFaces(t *testing.T) {
	fake := &fakeListServer{history: map[string]*historypb.HistoryEvent{}}
	cfg := startListServer(t, fake)
	c, err := newClient(cfg)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	defer c.Close()

	fake.history["run-reason-park"] = greenClose(t, workflows.ParkedResult("reviewer halted the run"))
	fake.history["run-reason-old"] = redClose(workflows.ErrAwaitingMaintainer.Error() + ": reviewer halted the run")
	fake.history["run-reason-green"] = greenClose(t, "aborted/issue-42")

	if parked, reason := closedRunParkReason(c, "wf-reason-park", "run-reason-park"); !parked || reason != "reviewer halted the run" {
		t.Errorf("a parked close probed (%v, %q), want the trimmed park reason", parked, reason)
	}
	if parked, reason := closedRunParkReason(c, "wf-reason-old", "run-reason-old"); !parked || reason != workflows.ErrAwaitingMaintainer.Error() {
		t.Errorf("an awaiting-maintainer close probed (%v, %q), want (%v, %q)", parked, reason, true, workflows.ErrAwaitingMaintainer.Error())
	}
	if parked, reason := closedRunParkReason(c, "wf-reason-green", "run-reason-green"); parked || reason != "" {
		t.Errorf("a plain green close probed (%v, %q), want no park and no reason", parked, reason)
	}

	if parked, reason := closedRunParkReason(c, "wf-reason-park", "run-reason-park"); !parked || reason != "reviewer halted the run" {
		t.Errorf("the memoized repeat probed (%v, %q), want the identical parked answer", parked, reason)
	}
	n := 0
	for _, p := range fake.probes {
		if p.runID == "run-reason-park" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("run-reason-park probed %d times, want 1 (the memo serves the reason too)", n)
	}
}
