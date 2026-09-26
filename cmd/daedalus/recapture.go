// Worker-status recapture: `daedalus worker status` doubles as the
// auto-recovery point for stuck activity attempts. A worker that dies (or
// is restarted) mid-round leaves Temporal holding an attempt whose outcome
// no process will ever report: the server considers the task in-flight for
// the dead worker's identity, redelivers nothing, and — with the Started
// event the timeouts count from possibly never even persisted — no timeout
// fires either. The workflow hangs forever unless something intervenes
// (observed 2026-09-19: a ~30s restart window during a jailed round wedged
// daedalus-issue-status-all until a manual `temporal activity fail`).
//
// Each status call therefore checks every recorded queue for running
// workflows whose outstanding activity attempt is held by a worker
// identity with no live poller, and recaptures it: it fails each ghost
// attempt so the workflow reschedules it, and — only when the attempt's
// queue has no live poller at all — restarts the recorded worker (or
// starts one from the queue's most recent config record, e.g. after a
// reboot). Every automatic action is alerted with a [DAEDALUS-ALERT] line
// to both the status output and the worker log. With nothing stuck the
// whole pass is a no-op, so repeated status calls stay read-only and a
// wrong detection cannot loop.
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
)

// stuckAttempt is one outstanding activity attempt Temporal will never
// redeliver on its own: its last worker identity has no live poller, so
// only an outside resolution (recapture) can move the workflow.
type stuckAttempt struct {
	workflowID string
	runID      string
	activityID string
	// queue is the attempt's own task queue (its activity options'), which
	// may differ from the scanned workflow's queue — suites run on the
	// shared test queue.
	queue string
	// identity is the dead worker's SDK identity (pid@host@…), named in
	// the alerts and in the failure that unsticks the attempt.
	identity string
	// noLivePoller reports that the attempt's own queue has no live
	// activity poller at all — the daemon serving it is dead or zombied,
	// and only then is a restart or start needed. When any poller exists,
	// failing the ghost suffices: the surviving poller serves the
	// rescheduled round.
	noLivePoller bool
}

// statusRecord is one worker's config record: the worker name it files
// under, the config path recorded for it, and the record's mtime (the
// recency tie-break when several records have served one queue).
type statusRecord struct {
	name, confPath string
	modTime        time.Time
}

// recaptureStuckTasks runs the detection-and-recovery pass over every
// queue served by a recorded worker's config. One queue's failure never
// blocks the others: an unloadable record is already "config error" text
// in the roster above, while an unreachable Temporal emits a
// "not checked" [DAEDALUS-ALERT] below — neither is silent.
func recaptureStuckTasks() error {
	names, err := recordedWorkers()
	if err != nil {
		return err
	}
	// Queue → the recorded configs that serve (or served) it, most recent
	// record first. Workers are renamed by editing worker_id, so several
	// records can name one queue over time; the newest is the one a start
	// resurrects the queue from.
	byQueue := map[string][]statusRecord{}
	for _, name := range names {
		conf := recordedConfigPath(name)
		if conf == "" {
			continue
		}
		cfg, err := config.Load(conf)
		if err != nil {
			continue // already shown as "config error" in the roster
		}
		var mod time.Time
		if info, err := os.Stat(conf); err == nil {
			mod = info.ModTime()
		}
		q := cfg.Temporal.TaskQueue
		byQueue[q] = append(byQueue[q], statusRecord{name: name, confPath: conf, modTime: mod})
	}
	queues := make([]string, 0, len(byQueue))
	for q, recs := range byQueue {
		sort.Slice(recs, func(i, j int) bool { return recs[i].modTime.After(recs[j].modTime) })
		queues = append(queues, q)
	}
	sort.Strings(queues)

	for _, q := range queues {
		recs := byQueue[q]
		cfg, err := config.Load(recs[0].confPath)
		if err != nil {
			continue
		}
		c, err := newClient(cfg)
		if err != nil {
			// The roster's API column probes the provider, not Temporal,
			// so nothing above said this: alert the skipped check rather
			// than silently not recovering.
			alertRecapture(recs[0].name, "queue %s not checked for stuck tasks: cannot connect to temporal at %s: %v",
				q, cfg.Temporal.Host, err)
			continue
		}
		stuck, err := detectStuckAttempts(context.Background(), c, q)
		if err != nil {
			alertRecapture(recs[0].name, "queue %s not checked for stuck tasks: %v", q, err)
			c.Close()
			continue
		}
		if len(stuck) == 0 {
			c.Close()
			continue
		}
		recaptureQueue(c, q, recs, stuck)
		c.Close()
	}
	return nil
}

// detectStuckAttempts returns the outstanding activity attempts of queue's
// running workflows that no live poller will ever serve: an attempt whose
// last worker identity has no poller on its own task queue (the ghost a
// dead worker leaves mid-round), or one never dispatched while nothing
// polls that queue at all (a daemon that died or was never started). Each
// attempt is judged against ITS activity options' task queue — suites run
// on the shared test queue, so the workflow's queue is the wrong reference
// in any multi-daemon deployment. The server must populate
// activity_options.task_queue: an attempt whose queue cannot be read is
// undecidable and skipped, never judged against the workflow's queue —
// guessing could flag a live suite on another daemon's test worker, and a
// skipped recovery only waits one more status call.
func detectStuckAttempts(ctx context.Context, c client.Client, queue string) ([]stuckAttempt, error) {
	// Per-queue live poller identities, described on first use and cached:
	// a workflow's pending activities usually share one queue.
	live := map[string]map[string]bool{}
	queueLive := func(q string) (map[string]bool, error) {
		if m, ok := live[q]; ok {
			return m, nil
		}
		desc, err := c.WorkflowService().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:     "default",
			TaskQueue:     &taskqueuepb.TaskQueue{Name: q, Kind: enums.TASK_QUEUE_KIND_NORMAL},
			TaskQueueType: enums.TASK_QUEUE_TYPE_ACTIVITY,
		})
		if err != nil {
			return nil, fmt.Errorf("describe task queue %s: %w", q, err)
		}
		m := make(map[string]bool, len(desc.GetPollers()))
		for _, p := range desc.GetPollers() {
			m[p.GetIdentity()] = true
		}
		live[q] = m
		return m, nil
	}
	if _, err := queueLive(queue); err != nil {
		return nil, err
	}
	var stuck []stuckAttempt
	err := forEachRunningWorkflow(ctx, c, queue, func(exec *commonpb.WorkflowExecution, d *workflowservice.DescribeWorkflowExecutionResponse) {
		for _, pa := range d.GetPendingActivities() {
			if pa.GetPaused() {
				continue // paused by an operator: deliberate, not stuck
			}
			aq := pa.GetActivityOptions().GetTaskQueue().GetName()
			if aq == "" {
				// Undecidable: without the attempt's own queue we cannot
				// judge liveness, and judging it against the workflow's
				// queue could flag a live attempt on another daemon's test
				// worker. Skip — recovery waits one more status call.
				continue
			}
			pollers, err := queueLive(aq)
			if err != nil {
				// Undecidable: a describe failure here must never flag a
				// live attempt as stuck, so skip this activity.
				continue
			}
			identity := pa.GetLastWorkerIdentity()
			var unserved bool
			if identity != "" {
				unserved = !pollers[identity]
			} else {
				unserved = len(pollers) == 0
			}
			if unserved {
				stuck = append(stuck, stuckAttempt{
					workflowID:   exec.GetWorkflowId(),
					runID:        exec.GetRunId(),
					activityID:   pa.GetActivityId(),
					queue:        aq,
					identity:     identity,
					noLivePoller: len(pollers) == 0,
				})
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return stuck, nil
}

// forEachRunningWorkflow describes every running workflow on queue —
// paging through the full list, not a first page whose size would cap
// coverage exactly when a busy deployment is most likely to hold ghosts —
// and calls seen for each described execution. A workflow that cannot be
// described (e.g. it just finished) is skipped: one un-describable
// workflow must not hide the rest of the queue.
func forEachRunningWorkflow(ctx context.Context, c client.Client, queue string, seen func(*commonpb.WorkflowExecution, *workflowservice.DescribeWorkflowExecutionResponse)) error {
	var token []byte
	for {
		resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     "default",
			PageSize:      25,
			NextPageToken: token,
			Query:         fmt.Sprintf("TaskQueue = '%s' AND ExecutionStatus = 'Running'", queue),
		})
		if err != nil {
			return fmt.Errorf("list running workflows on %s: %w", queue, err)
		}
		for _, info := range resp.GetExecutions() {
			exec := info.GetExecution()
			d, err := c.WorkflowService().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: "default",
				Execution: &commonpb.WorkflowExecution{WorkflowId: exec.GetWorkflowId(), RunId: exec.GetRunId()},
			})
			if err != nil {
				continue
			}
			seen(exec, d)
		}
		token = resp.GetNextPageToken()
		if len(token) == 0 {
			return nil
		}
	}
}

// recaptureQueue recaptures a queue's stuck attempts: fail each ghost so
// the workflow reschedules it, and — only when some attempt's own queue
// has no live poller at all (its daemon is dead or zombied) — also
// restart the live recorded worker, or start one from the most recent
// record when none is live. The restart is deliberately conditional: a
// live untyped daemon polls both its workflow queue and the shared test
// queue (activities.TestTaskQueue, which no config record names) — a daemon
// started with -t dev or -t test polls only one, but the untyped daemon a
// recapture brings back is a superset, so when any poller exists on the
// attempt's own queue the rescheduled rounds are already served and
// restarting the healthy daemon would only SIGTERM every OTHER workflow's
// in-flight round. Restart comes before failing so a resurrected poller is
// up before the rescheduled rounds land.
func recaptureQueue(c client.Client, q string, recs []statusRecord, stuck []stuckAttempt) {
	rebuild := false
	for _, s := range stuck {
		if s.noLivePoller {
			rebuild = true
			break
		}
	}
	if !rebuild {
		// Every ghost's queue still has a live poller (e.g. one dead
		// worker among several serving the queue): failing the ghosts
		// alone unsticks the workflows; leave the healthy daemons alone.
		failStuckAttempts(c, recs[0].name, stuck)
		return
	}
	for _, r := range recs {
		pidFile, _, _ := daemonPaths(r.name)
		if pid, ok := readLivePid(pidFile); ok {
			alertRecapture(r.name, "restarting worker %s (pid %d, config %s) to recapture stuck task(s): %s",
				r.name, pid, r.confPath, attemptSummary(stuck))
			if err := workerRestartNamed(r.name); err != nil {
				// The old worker may still be draining and about to resolve
				// the attempts itself; failing them now could duplicate
				// rescheduled rounds with no poller to serve them. Leave
				// the ghosts for the next status call.
				alertRecapture(r.name, "recapture restart failed — stuck task(s) left for the next status call: %v", err)
				return
			}
			failStuckAttempts(c, r.name, stuck)
			return
		}
	}
	// No live worker serves the queue: start one from the most recent
	// record's config — the newest settings this queue was served with —
	// and name the choice in the alert.
	best := recs[0]
	cfg, err := config.Load(best.confPath)
	if err != nil {
		alertRecapture(best.name, "cannot auto-start a worker for queue %s: config %s no longer loads (%v) — stuck: %s",
			q, best.confPath, err, attemptSummary(stuck))
		return
	}
	alertRecapture(best.name, "no live worker serves queue %s — starting worker %s from config %s (most recent record) to recapture stuck task(s): %s",
		q, cfg.WorkerName(), best.confPath, attemptSummary(stuck))
	// Empty workerType: a recapture restart brings the daemon back untyped
	// (both pollers), restoring any missing poller as a side effect.
	if err := workerStart(cfg, best.confPath, ""); err != nil {
		alertRecapture(best.name, "recapture start failed: %v", err)
		return
	}
	failStuckAttempts(c, best.name, stuck)
}

// failStuckAttempts fails each ghost attempt by id — the programmatic
// equivalent of the incident's manual `temporal activity fail`.
func failStuckAttempts(c client.Client, worker string, stuck []stuckAttempt) {
	for _, s := range stuck {
		if err := failStuckAttempt(context.Background(), c, s); err != nil {
			alertRecapture(worker, "could not fail stuck activity %s of workflow %s (held by dead worker %s): %v",
				s.activityID, s.workflowID, s.identity, err)
			continue
		}
		alertRecapture(worker, "failed stuck activity %s of workflow %s (held by dead worker %s) — the workflow reschedules it",
			s.activityID, s.workflowID, s.identity)
	}
}

// failStuckAttempt fails one ghost attempt so the workflow's retry policy
// reschedules it on the live worker. The failure message carries
// activities.ErrAgentKilled so the workflow classifies a recaptured jailed
// round exactly like an abruptly killed one — a continuation from the
// partial work already in the worktree, counted against the same streak —
// instead of failing the run. For non-jailed activities the same text is
// the honest outcome (the attempt's process is gone without reporting) and
// the run closes FAILED — resumable via `daedalus continue` — rather than
// wedged forever. ponytail: the single message cannot classify per
// activity type, so a recaptured test-suite or worktree ghost fails its
// run instead of rescheduling it — strictly better than the wedge, at the
// cost of one resumable rerun.
func failStuckAttempt(ctx context.Context, c client.Client, s stuckAttempt) error {
	_, err := c.WorkflowService().RespondActivityTaskFailedById(ctx, &workflowservice.RespondActivityTaskFailedByIdRequest{
		Namespace:  "default",
		WorkflowId: s.workflowID,
		RunId:      s.runID,
		ActivityId: s.activityID,
		Identity:   "daedalus-worker-status-recapture",
		Failure: &failurepb.Failure{
			Message: fmt.Sprintf("%s: attempt held by dead worker identity %s (recaptured by daedalus worker status)",
				activities.ErrAgentKilled.Error(), s.identity),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					Type: "ApplicationError",
				},
			},
		},
	})
	return err
}

// attemptSummary renders stuck attempts for alerts: workflow, activity,
// its own task queue, and the dead identity holding it.
func attemptSummary(stuck []stuckAttempt) string {
	parts := make([]string, len(stuck))
	for i, s := range stuck {
		parts[i] = fmt.Sprintf("workflow %s activity %s on queue %s (dead worker %s)",
			s.workflowID, s.activityID, s.queue, s.identity)
	}
	return strings.Join(parts, "; ")
}

// alertRecapture emits a recapture alert line to both the status output
// and the worker's log. The [DAEDALUS-ALERT] prefix makes an automatic
// action unmistakable — never confusable with a routine status row, never
// silent.
func alertRecapture(worker, format string, a ...any) {
	line := fmt.Sprintf("[DAEDALUS-ALERT] %s worker-status: %s",
		time.Now().Format(time.RFC3339), fmt.Sprintf(format, a...))
	fmt.Println(line)
	_, logFile, _ := daemonPaths(worker)
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintln(f, line)
}
