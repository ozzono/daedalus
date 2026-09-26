package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"

	"github.com/ozzono/daedalus/internal/activities"
)

// fakeRecaptureClient serves canned workflow list pages to the recapture
// pass and hands out fakeService as the workflow service; everything else
// on client.Client is unreachable for the pass and left nil: calling it
// panics, which is the stub saying so.
type fakeRecaptureClient struct {
	client.Client

	pages [][]string // workflow list pages, in order; entries are workflow ids
	// queries records every list query, for asserting the roster filter.
	queries []string
	service fakeRecaptureService
}

func (f *fakeRecaptureClient) WorkflowService() workflowservice.WorkflowServiceClient {
	return &f.service
}

func (f *fakeRecaptureClient) ListWorkflow(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	f.queries = append(f.queries, req.GetQuery())
	page := 0
	if tok := string(req.GetNextPageToken()); tok != "" {
		page, _ = strconv.Atoi(tok)
	}
	if page >= len(f.pages) {
		return &workflowservice.ListWorkflowExecutionsResponse{}, nil
	}
	resp := &workflowservice.ListWorkflowExecutionsResponse{}
	for _, id := range f.pages[page] {
		resp.Executions = append(resp.Executions, &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: id + "-run"},
		})
	}
	if page+1 < len(f.pages) {
		resp.NextPageToken = []byte(strconv.Itoa(page + 1))
	}
	return resp, nil
}

// fakeRecaptureService serves canned poller rosters and workflow
// descriptions, and records every ghost attempt failed through it.
type fakeRecaptureService struct {
	workflowservice.WorkflowServiceClient

	// pollers maps task queue name → live poller identities.
	pollers map[string][]string
	// describeErrs fails DescribeTaskQueue for a queue by name — the
	// "undecidable, skip" path.
	describeErrs map[string]error
	// describes maps workflow id → its pending activities.
	describes map[string][]*workflowpb.PendingActivityInfo
	// describeWorkflowErrs fails DescribeWorkflowExecution for a workflow
	// id — the poison workflow the queue walk must skip, not mask.
	describeWorkflowErrs map[string]error
	// failed records every RespondActivityTaskFailedById request.
	failed []*workflowservice.RespondActivityTaskFailedByIdRequest
	// failErr, when set, is returned by RespondActivityTaskFailedById.
	failErr error
}

func (s *fakeRecaptureService) DescribeTaskQueue(_ context.Context, req *workflowservice.DescribeTaskQueueRequest, _ ...grpc.CallOption) (*workflowservice.DescribeTaskQueueResponse, error) {
	q := req.GetTaskQueue().GetName()
	if err := s.describeErrs[q]; err != nil {
		return nil, err
	}
	resp := &workflowservice.DescribeTaskQueueResponse{}
	for _, id := range s.pollers[q] {
		resp.Pollers = append(resp.Pollers, &taskqueuepb.PollerInfo{Identity: id})
	}
	return resp, nil
}

func (s *fakeRecaptureService) DescribeWorkflowExecution(_ context.Context, req *workflowservice.DescribeWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if err := s.describeWorkflowErrs[req.GetExecution().GetWorkflowId()]; err != nil {
		return nil, err
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{
		PendingActivities: s.describes[req.GetExecution().GetWorkflowId()],
	}, nil
}

func (s *fakeRecaptureService) RespondActivityTaskFailedById(_ context.Context, req *workflowservice.RespondActivityTaskFailedByIdRequest, _ ...grpc.CallOption) (*workflowservice.RespondActivityTaskFailedByIdResponse, error) {
	s.failed = append(s.failed, req)
	if s.failErr != nil {
		return nil, s.failErr
	}
	return &workflowservice.RespondActivityTaskFailedByIdResponse{}, nil
}

// pendingActivity builds one outstanding activity attempt: activity id, the
// identity of the worker that last held it ("" = never dispatched), its own
// task queue ("" = options unreadable), and the paused flag.
func pendingActivity(id, identity, queue string, paused bool) *workflowpb.PendingActivityInfo {
	pa := &workflowpb.PendingActivityInfo{
		ActivityId:         id,
		LastWorkerIdentity: identity,
		Paused:             paused,
	}
	if queue != "" {
		pa.ActivityOptions = &activitypb.ActivityOptions{TaskQueue: &taskqueuepb.TaskQueue{Name: queue}}
	}
	return pa
}

// TestDetectStuckAttempts pins the ghost detector: an attempt is stuck
// exactly when the identity holding it has no poller on the attempt's OWN
// task queue (or it was never dispatched and nothing polls that queue at
// all — the daemon-never-started, after-a-reboot case); paused attempts,
// attempts whose queue cannot be read, and attempts whose queue cannot be
// described are skipped, never guessed. Every running workflow is examined,
// across list pages, and one un-describable workflow hides none of the
// rest.
func TestDetectStuckAttempts(t *testing.T) {
	fake := &fakeRecaptureClient{
		// w-poison sits before w-late so a later page's ghost is still
		// found despite the poison workflow's describe failing.
		pages: [][]string{{"w-main", "w-test-queue"}, {"w-poison", "w-late"}},
	}
	fake.service = fakeRecaptureService{
		pollers: map[string][]string{
			"daedalus": {"live@host@1"},
		},
		describeErrs: map[string]error{
			"boom": errors.New("describe exploded"),
		},
		describeWorkflowErrs: map[string]error{
			"w-poison": errors.New("workflow just closed"),
		},
		describes: map[string][]*workflowpb.PendingActivityInfo{
			"w-main": {
				pendingActivity("a-live", "live@host@1", "daedalus", false),  // poller live
				pendingActivity("a-ghost", "dead@host@2", "daedalus", false), // ghost, queue alive
				pendingActivity("a-never", "", "daedalus", false),            // never dispatched, pollers exist
			},
			"w-test-queue": {
				pendingActivity("a-paused", "dead@host@3", "daedalus", true), // operator-paused
				pendingActivity("a-opaque", "dead@host@4", "", false),        // queue unreadable
				pendingActivity("a-boom", "dead@host@5", "boom", false),      // queue undescribable
				pendingActivity("suite-0", "", "test", false),                // never dispatched, queue dead
				pendingActivity("suite-1", "dead@host@9", "test", false),     // ghost, whole queue dead
			},
			// Read only if the poison skip regresses; describe must fail
			// before these are ever seen.
			"w-poison": {
				pendingActivity("poison-ghost", "dead@host@6", "daedalus", false),
			},
			"w-late": {
				pendingActivity("late-ghost", "dead@host@7", "daedalus", false), // after the poison workflow
			},
		},
	}

	stuck, err := detectStuckAttempts(context.Background(), fake, "daedalus")
	if err != nil {
		t.Fatalf("detectStuckAttempts: %v", err)
	}

	type key struct{ wf, act string }
	got := map[key]stuckAttempt{}
	for _, s := range stuck {
		got[key{s.workflowID, s.activityID}] = s
	}
	if len(stuck) != 4 {
		t.Errorf("detectStuckAttempts returned %d stuck attempts (%v), want 4", len(stuck), stuck)
	}
	for _, want := range []struct {
		wf, act, queue, identity string
		noLivePoller             bool
	}{
		{"w-main", "a-ghost", "daedalus", "dead@host@2", false},
		{"w-test-queue", "suite-0", "test", "", true},
		{"w-test-queue", "suite-1", "test", "dead@host@9", true},
		{"w-late", "late-ghost", "daedalus", "dead@host@7", false},
	} {
		s, ok := got[key{want.wf, want.act}]
		if !ok {
			t.Errorf("attempt %s of %s not detected as stuck; got %v", want.act, want.wf, stuck)
			continue
		}
		if s.queue != want.queue || s.identity != want.identity || s.noLivePoller != want.noLivePoller {
			t.Errorf("attempt %s of %s = %+v, want queue %s identity %s noLivePoller %v",
				want.act, want.wf, s, want.queue, want.identity, want.noLivePoller)
		}
		if s.runID != want.wf+"-run" {
			t.Errorf("attempt %s of %s carried runID %q, want %q", want.act, want.wf, s.runID, want.wf+"-run")
		}
	}
	for _, skipped := range []struct{ wf, act string }{
		{"w-main", "a-live"}, {"w-main", "a-never"},
		{"w-test-queue", "a-paused"}, {"w-test-queue", "a-opaque"}, {"w-test-queue", "a-boom"},
		{"w-poison", "poison-ghost"}, // its describe failed; it must be skipped, not seen
	} {
		if _, ok := got[key{skipped.wf, skipped.act}]; ok {
			t.Errorf("attempt %s of %s was flagged stuck; it must be skipped", skipped.act, skipped.wf)
		}
	}

	// The roster walk filters to running workflows on the scanned queue.
	if len(fake.queries) == 0 {
		t.Fatal("detectStuckAttempts listed no workflows")
	}
	for _, q := range fake.queries {
		if !strings.Contains(q, "ExecutionStatus = 'Running'") || !strings.Contains(q, "TaskQueue = 'daedalus'") {
			t.Errorf("list query %q must select running workflows on the scanned queue", q)
		}
	}
}

// TestDetectStuckAttemptsUndecidableQueue pins the failure mode when the
// scanned queue itself cannot be described: the whole pass reports the
// error rather than guessing about any attempt.
func TestDetectStuckAttemptsUndecidableQueue(t *testing.T) {
	fake := &fakeRecaptureClient{}
	fake.service.describeErrs = map[string]error{"daedalus": errors.New("temporal exploded")}
	if _, err := detectStuckAttempts(context.Background(), fake, "daedalus"); err == nil {
		t.Error("detectStuckTasks with an undescribable queue = nil error, want the describe failure")
	}
}

// TestFailStuckAttempt pins the unstick call: it fails the attempt by id on
// the right workflow run, signs as the recapture, and carries
// activities.ErrAgentKilled plus the dead identity in the failure message,
// so a recaptured jailed round classifies as killed, not failed.
func TestFailStuckAttempt(t *testing.T) {
	fake := &fakeRecaptureClient{}
	s := stuckAttempt{
		workflowID: "wf-1", runID: "run-1", activityID: "act-1",
		queue: "daedalus", identity: "dead@host@2",
	}
	if err := failStuckAttempt(context.Background(), fake, s); err != nil {
		t.Fatalf("failStuckAttempt: %v", err)
	}
	if len(fake.service.failed) != 1 {
		t.Fatalf("failStuckAttempt issued %d RespondActivityTaskFailedById calls, want 1", len(fake.service.failed))
	}
	req := fake.service.failed[0]
	if req.GetWorkflowId() != "wf-1" || req.GetRunId() != "run-1" || req.GetActivityId() != "act-1" {
		t.Errorf("fail request targeted %s/%s/%s, want wf-1/run-1/act-1",
			req.GetWorkflowId(), req.GetRunId(), req.GetActivityId())
	}
	if req.GetIdentity() != "daedalus-worker-status-recapture" {
		t.Errorf("fail request identity = %q, want the recapture signature", req.GetIdentity())
	}
	msg := req.GetFailure().GetMessage()
	if !strings.Contains(msg, activities.ErrAgentKilled.Error()) || !strings.Contains(msg, "dead@host@2") {
		t.Errorf("failure message %q must carry %q and the dead identity", msg, activities.ErrAgentKilled.Error())
	}

	// A server-side failure surfaces to the caller and no successful fail
	// is implied.
	boom := &fakeRecaptureClient{}
	boom.service.failErr = errors.New("respond refused")
	if err := failStuckAttempt(context.Background(), boom, s); err == nil {
		t.Error("failStuckAttempt with a failing service = nil error, want the service error")
	}
}

// TestAttemptSummary pins the alert rendering: workflow, activity, its own
// queue, and the dead identity, joined for multi-ghost alerts.
func TestAttemptSummary(t *testing.T) {
	got := attemptSummary([]stuckAttempt{
		{workflowID: "wf-1", activityID: "act-1", queue: "daedalus", identity: "dead@host@2"},
		{workflowID: "wf-2", activityID: "suite-1", queue: "test", identity: ""},
	})
	for _, want := range []string{
		"workflow wf-1 activity act-1 on queue daedalus (dead worker dead@host@2)",
		"workflow wf-2 activity suite-1 on queue test",
		"; ",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("attemptSummary = %q, want it to carry %q", got, want)
		}
	}
}

// TestRecaptureQueueWithoutDeadQueues pins the conditional restart: when
// every ghost's own queue still has a poller, the ghosts are failed and no
// worker is restarted or started — the surviving poller serves the
// rescheduled rounds.
func TestRecaptureQueueWithoutDeadQueues(t *testing.T) {
	useDaemonDir(t)
	fake := &fakeRecaptureClient{}
	stuck := []stuckAttempt{{workflowID: "wf-1", runID: "run-1", activityID: "act-1", queue: "daedalus", identity: "dead@h@2"}}
	recaptureQueue(fake, "daedalus", []statusRecord{{name: "w", confPath: "/unused.yaml"}}, stuck)

	if len(fake.service.failed) != 1 || fake.service.failed[0].GetActivityId() != "act-1" {
		t.Errorf("recaptureQueue failed %d attempts, want act-1", len(fake.service.failed))
	}
}

// TestRecaptureQueueAlerts pins the alert contract of the recovery branches
// that need no live daemon interaction. Each case asserts the alert text
// and whether the ghost attempts were failed in the same pass.
func TestRecaptureQueueAlerts(t *testing.T) {
	t.Run("failed restart leaves the ghosts for the next pass", func(t *testing.T) {
		dir := useDaemonDir(t)
		// A live pid plus a record whose config now names a different
		// worker: workerRestartNamed refuses before signaling anything.
		// The live pid is THIS test process — safety rests entirely on
		// the worker-name mismatch check firing before workerStop's
		// SIGTERM; a reorder would kill the test binary loudly here.
		mismatch := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(mismatch, []byte("agent: claude\nworker_id: renamed\ntemporal:\n  task_queue: q7\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		writeRecord(t, "w", mismatch)
		writeLivePidFile(t, "w")

		fake := &fakeRecaptureClient{}
		stuck := []stuckAttempt{{workflowID: "wf-1", runID: "run-1", activityID: "act-1", queue: "w", identity: "dead@h@2", noLivePoller: true}}
		out := captureStdout(t, func() {
			recaptureQueue(fake, "w", []statusRecord{{name: "w", confPath: mismatch}}, stuck)
		})

		if !strings.Contains(out, "restarting worker w") || !strings.Contains(out, "recapture restart failed") {
			t.Errorf("recaptureQueue output %q must alert the restart and its failure", out)
		}
		if len(fake.service.failed) != 0 {
			t.Errorf("a failed restart must leave the ghosts unfailed, not fail them pollerless; failed %v", fake.service.failed)
		}
		if _, err := os.Stat(filepath.Join(dir, "worker-w.log")); err != nil {
			t.Errorf("alerts must reach the worker log %s: %v", dir, err)
		}
	})

	t.Run("no live worker and an unloadable record cannot auto-start", func(t *testing.T) {
		useDaemonDir(t)
		gone := filepath.Join(t.TempDir(), "gone.yaml")
		fake := &fakeRecaptureClient{}
		stuck := []stuckAttempt{{workflowID: "wf-1", runID: "run-1", activityID: "act-1", queue: "w", identity: "dead@h@2", noLivePoller: true}}
		out := captureStdout(t, func() {
			recaptureQueue(fake, "w", []statusRecord{{name: "w", confPath: gone}}, stuck)
		})

		if !strings.Contains(out, "cannot auto-start a worker for queue w") {
			t.Errorf("recaptureQueue output %q must name the blocked auto-start", out)
		}
		if len(fake.service.failed) != 0 {
			t.Errorf("a blocked auto-start must leave the ghosts unfailed; failed %v", fake.service.failed)
		}
	})

	t.Run("start recaptures from the most recent record and then fails the ghosts", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		newest := writeQueueConfig(t, "w")
		older := writeQueueConfig(t, "w")

		fake := &fakeRecaptureClient{}
		stuck := []stuckAttempt{{workflowID: "wf-1", runID: "run-1", activityID: "act-1", queue: "w", identity: "dead@h@2", noLivePoller: true}}
		out := captureStdout(t, func() {
			// Newest record first, as recaptureStuckTasks orders them.
			recaptureQueue(fake, "w", []statusRecord{
				{name: "w", confPath: newest},
				{name: "w-old", confPath: older},
			}, stuck)
		})

		if !strings.Contains(out, "no live worker serves queue w") || !strings.Contains(out, "starting worker w from config "+newest) {
			t.Errorf("recaptureQueue output %q must start from the most recent record", out)
		}
		if len(fake.service.failed) != 1 || fake.service.failed[0].GetActivityId() != "act-1" {
			t.Errorf("recaptureQueue failed %d attempts after the start, want act-1", len(fake.service.failed))
		}
	})
}

// TestAlertRecapture pins the two-channel alert: the [DAEDALUS-ALERT] line
// reaches both the status output and the worker's log file, appended, one
// line per alert.
func TestAlertRecapture(t *testing.T) {
	dir := useDaemonDir(t)
	out := captureStdout(t, func() {
		alertRecapture("w", "failed stuck activity %s of workflow %s", "act-1", "wf-1")
		alertRecapture("w", "second line")
	})

	var lines []string
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "[DAEDALUS-ALERT]") {
			lines = append(lines, l)
		}
	}
	if len(lines) != 2 {
		t.Fatalf("stdout carried %d [DAEDALUS-ALERT] lines (%q), want 2", len(lines), out)
	}
	if !strings.Contains(lines[0], "worker-status: failed stuck activity act-1 of workflow wf-1") {
		t.Errorf("alert line %q must carry the rendered message", lines[0])
	}
	if !strings.Contains(lines[1], "second line") {
		t.Errorf("alert line %q must carry the rendered message", lines[1])
	}

	data, err := os.ReadFile(filepath.Join(dir, "worker-w.log"))
	if err != nil {
		t.Fatalf("read worker log: %v", err)
	}
	logLines := strings.Count(string(data), "[DAEDALUS-ALERT]")
	if logLines != 2 {
		t.Errorf("worker log carried %d alert lines, want 2 (both alerts, appended)", logLines)
	}
}

// TestRecaptureStuckTasksNoopAndDegraded pins recaptureStuckTasks' edges:
// an empty roster is a silent no-op, an unloadable record is skipped
// (already "config error" text in the roster), and an unreachable Temporal
// alerts "not checked" without failing the pass.
func TestRecaptureStuckTasksNoopAndDegraded(t *testing.T) {
	t.Run("empty roster", func(t *testing.T) {
		useDaemonDir(t)
		out := captureStdout(t, func() {
			if err := recaptureStuckTasks(); err != nil {
				t.Errorf("recaptureStuckTasks(empty roster): %v", err)
			}
		})
		if strings.Contains(out, "[DAEDALUS-ALERT]") {
			t.Errorf("an empty roster must be a silent no-op, got %q", out)
		}
	})

	t.Run("unloadable record is skipped", func(t *testing.T) {
		useDaemonDir(t)
		writeRecord(t, "w", filepath.Join(t.TempDir(), "gone.yaml"))
		out := captureStdout(t, func() {
			if err := recaptureStuckTasks(); err != nil {
				t.Errorf("recaptureStuckTasks(unloadable record): %v", err)
			}
		})
		if strings.Contains(out, "[DAEDALUS-ALERT]") {
			t.Errorf("an unloadable record is the roster's config-error row, not a recapture alert: %q", out)
		}
	})

	t.Run("unreachable temporal alerts instead of failing", func(t *testing.T) {
		useDaemonDir(t)
		cfg := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(cfg, []byte("agent: claude\ntemporal:\n  host: 127.0.0.1:1\n  task_queue: q9\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		writeRecord(t, "w", cfg)
		out := captureStdout(t, func() {
			if err := recaptureStuckTasks(); err != nil {
				t.Errorf("recaptureStuckTasks(unreachable temporal): %v", err)
			}
		})
		if !strings.Contains(out, "queue q9 not checked for stuck tasks") {
			t.Errorf("recaptureStuckTasks output %q must alert the skipped check, never silently skip it", out)
		}
	})
}
