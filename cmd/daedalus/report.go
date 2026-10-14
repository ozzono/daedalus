// Report aggregates the raw per-round provider metrics captured in
// workflow history (activities.Usage) into per-session, per-day,
// per-worker, and total slices, lists this host's workers with their live
// slot occupancy, and gives every live run on the queue a health verdict —
// running, provider-starved, waiting, or the stale wedge shape. Scoping
// (Q7): the invoking config's worker — its task queue — alone; there is no
// -q/--all. It reads history through the same multi-execution walk the
// task-log -cot view makes (see listWorkflowRuns); aggregation happens at
// report time, keeping history lossless.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	enums "go.temporal.io/api/enums/v1"
	workflowv1 "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/workflows"
)

// reportScope holds the parsed `daedalus worker report` flags.
type reportScope struct {
	jsonOut bool
}

// parseReportArgs parses report's own flags from the positional arguments
// after the subcommand. --table is the default output and accepted only for
// explicitness.
func parseReportArgs(args []string) (reportScope, error) {
	var sc reportScope
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--json":
			sc.jsonOut = true
		case a == "--table":
		default:
			return sc, fmt.Errorf("unknown report flag %q (--json, --table)", a)
		}
	}
	return sc, nil
}

// runReport gathers and prints the usage report for the invoking config's
// worker — its task queue alone (Q7: single-worker scope, no -q/--all).
func runReport(configPath string, args []string) error {
	scope, err := parseReportArgs(args)
	if err != nil {
		return err
	}
	path, err := resolveConfigPath(configPath)
	if err != nil {
		return err
	}
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	queues := []string{cfg.Temporal.TaskQueue}

	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	rep := &usageReport{
		Generated: time.Now(),
		Queues:    queues,
		Sessions:  make(map[string]usageTotals),
		Days:      make(map[string]usageTotals),
		ByWorker:  make(map[string]usageTotals),
	}
	rep.Workers = collectWorkerRows()
	for _, q := range queues {
		if err := aggregateQueueUsage(c, q, rep); err != nil {
			return err
		}
	}
	if scope.jsonOut {
		data, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	printReportTable(rep)
	return nil
}

// usageTotals sums raw per-round Usage figures. Only the sums are computed
// here — the raw figures stay in history (see activities.Usage).
type usageTotals struct {
	Rounds           int           `json:"rounds"`
	CostUSD          float64       `json:"cost_usd"`
	InputTokens      int64         `json:"input_tokens"`
	OutputTokens     int64         `json:"output_tokens"`
	CacheReadTokens  int64         `json:"cache_read_tokens"`
	CacheWriteTokens int64         `json:"cache_write_tokens"`
	Duration         time.Duration `json:"duration_ns"`
}

func (t *usageTotals) add(u activities.Usage) {
	t.Rounds++
	t.CostUSD += u.CostUSD
	t.InputTokens += u.InputTokens
	t.OutputTokens += u.OutputTokens
	t.CacheReadTokens += u.CacheReadTokens
	t.CacheWriteTokens += u.CacheWriteTokens
	t.Duration += u.Duration
}

// workerRow is one line of the report's host worker roster, from the
// daemon-dir pid and status files.
type workerRow struct {
	Name          string `json:"name"`
	Running       bool   `json:"running"`
	PID           int    `json:"pid,omitempty"`
	TaskQueue     string `json:"task_queue,omitempty"`
	SlotsBusy     int    `json:"slots_busy"`
	SlotsTotal    int    `json:"slots_total"`
	RoundsWaiting int    `json:"rounds_waiting"`
	// StatusAge describes the status file's freshness ("2s ago", "stale",
	// or "" when the worker published none).
	StatusAge string `json:"status_age,omitempty"`
}

// usageReport is the whole report — the JSON document and the source of
// the table sections.
type usageReport struct {
	Generated time.Time              `json:"generated"`
	Queues    []string               `json:"queues"`
	Workers   []workerRow            `json:"workers"`
	Runs      []runRow               `json:"runs,omitempty"`
	Sessions  map[string]usageTotals `json:"sessions"`
	Days      map[string]usageTotals `json:"days"`
	ByWorker  map[string]usageTotals `json:"by_worker"`
	Total     usageTotals            `json:"total"`
}

// statusStaleAfter is how long after a publish a status file is trusted:
// three missed intervals mean the worker stopped writing it (crashed or
// killed) and the file describes nothing live.
const statusStaleAfter = 3 * statusPublishInterval

// runStaleAfter is how long a live run may go without a completed round
// before the RUNS section calls it stale: twice the default agent-round
// ceiling, so even a round at the default maximum has completed by then
// and "running" past it is the wedge shape. ponytail: the run's own
// configured ceilings are not consulted (a per-run history read is not
// report work), so a deployment with raised timeouts can show a healthy
// slow run as stale.
const runStaleAfter = 2 * config.DefaultAgentRunTimeout

// runRow is one live run's health line in the report's RUNS section. The
// state verdict is what makes a provider-starved run (the quota
// heartbeat's hourly crawl under a rate-limited API) distinguishable from
// a merely running one and from a wedged one — the distinction a stale
// heartbeat used to hide. Starved arrives as the workflow's own
// DaedalusStatus stamp (workflows.StatusStarved); a stale-or-older worker
// leaves the heartbeat stamped "waiting", which renders as the (less
// specific) slot-backoff line.
type runRow struct {
	WorkflowID string `json:"workflow_id"`
	// State is the health verdict: "provider-starved (quota heartbeat)",
	// "waiting (slot backoff)", "pending (dependency gate)", "running",
	// the stale wedge shape, or the run's raw status for anything the
	// verdicts above don't cover.
	State string `json:"state"`
	// LastRound is the run's last completed-round stamp
	// (LastActivityAt), RFC3339, "unknown" for runs predating the
	// attribute.
	LastRound string `json:"last_round,omitempty"`
}

// runRowFor derives one run's health row from a listed execution and the
// same visibility decode the runs table makes. ok is false for closed
// runs — the RUNS section is live health only; closed outcomes already
// have `daedalus list`.
func runRowFor(info *workflowv1.WorkflowExecutionInfo, now time.Time) (row runRow, ok bool) {
	if info.GetCloseTime().IsValid() {
		return runRow{}, false
	}
	vis := decodeRunVisibility(info.GetSearchAttributes(), converter.GetDefaultDataConverter())
	row = runRow{WorkflowID: info.GetExecution().GetWorkflowId()}
	if vis.LastAt.IsZero() {
		row.LastRound = "unknown"
	} else {
		row.LastRound = vis.LastAt.UTC().Format(time.RFC3339)
	}
	runningVerdict := func() {
		if vis.LastAt.IsZero() {
			row.State = "running (no completed round recorded)"
		} else if age := now.Sub(vis.LastAt); age > runStaleAfter {
			row.State = fmt.Sprintf("stale — no completed round in %s; possible wedge", dur(age))
		} else {
			row.State = "running"
		}
	}
	switch {
	case workflows.RunStatus(vis.Status) == workflows.StatusStarved:
		row.State = "provider-starved (quota heartbeat)"
	case workflows.RunStatus(vis.Status) == workflows.StatusWaiting:
		row.State = "waiting (slot backoff)"
	case workflows.RunStatus(vis.Status) == workflows.StatusPending:
		row.State = "pending (dependency gate)"
	case workflows.RunStatus(vis.Status) == workflows.StatusRunning:
		runningVerdict()
	case vis.Status == "":
		// A run predating the visibility attributes: the raw enum is all
		// there is — running verdicts only for an actually-running enum
		// (a live continued-as-new execution keeps its own label).
		if info.GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
			runningVerdict()
		} else {
			row.State = info.GetStatus().String()
		}
	default:
		row.State = vis.Status
	}
	return row, true
}

// collectWorkerRows lists this host's workers — every config record plus
// any live stray — with slot occupancy from each worker's published status
// file. Occupancy is trusted only from a running worker whose pid wrote the
// file within the staleness bound; anything else shows as no occupancy.
// The file's task-queue label is display metadata and is shown from any
// parseable file, even a dead worker's leftover.
func collectWorkerRows() []workerRow {
	names, err := recordedWorkers()
	if err != nil {
		names = nil
	}
	onRecord := make(map[string]bool, len(names))
	for _, n := range names {
		onRecord[n] = true
	}
	names = append(names, runningUnrecordedWorkers(onRecord)...)
	sort.Strings(names)
	rows := make([]workerRow, 0, len(names))
	for _, name := range names {
		row := workerRow{Name: name}
		if pid, ok := readLivePid(filepath.Join(daemonDir, "worker-"+name+".pid")); ok {
			row.Running = true
			row.PID = pid
		}
		data, err := os.ReadFile(filepath.Join(daemonDir, "worker-"+name+".status"))
		if err == nil {
			var st workerStatus
			if json.Unmarshal(data, &st) == nil && st.TaskQueue != "" {
				row.TaskQueue = st.TaskQueue
			}
			if row.Running && st.PID == row.PID && time.Since(st.Updated) < statusStaleAfter {
				row.SlotsBusy = st.SlotsBusy
				row.SlotsTotal = st.SlotsTotal
				row.RoundsWaiting = st.RoundsWaiting
				row.StatusAge = time.Since(st.Updated).Round(time.Second).String() + " ago"
			} else if row.Running {
				row.StatusAge = "stale"
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// agentActivityTypes are the jailed-round activities whose results carry
// Usage into history.
var agentActivityTypes = map[string]bool{
	"RunJailedClaudeActivity":   true,
	"RunJailedReviewerActivity": true,
}

// aggregateQueueUsage folds every session's usage on one task queue into
// the report.
func aggregateQueueUsage(c client.Client, queue string, rep *usageReport) error {
	resp, err := c.ListWorkflow(context.Background(), &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: "default",
		PageSize:  100,
		Query:     fmt.Sprintf("TaskQueue = '%s'", queue),
	})
	if err != nil {
		return fmt.Errorf("list workflows on %s: %w", queue, err)
	}
	// ponytail: first page only (the 100 most recent sessions per queue) —
	// paging through whole histories waits for a whole-history need.
	// The list carries one row per execution, and a continued or
	// restarted session owns several under one id — the RUNS section
	// wants every row, but the session walk (aggregateSessionUsage)
	// already reads every execution of the id itself, so each id is
	// aggregated exactly once.
	seen := make(map[string]bool, len(resp.GetExecutions()))
	for _, info := range resp.GetExecutions() {
		// The live-health section rides the same list call the usage
		// aggregation makes — no second query.
		if row, ok := runRowFor(info, time.Now()); ok {
			rep.Runs = append(rep.Runs, row)
		}
		id := info.GetExecution().GetWorkflowId()
		if seen[id] {
			continue
		}
		seen[id] = true
		if err := aggregateSessionUsage(c, id, rep); err != nil {
			return err
		}
	}
	return nil
}

// aggregateSessionUsage walks one workflow's history and adds each jailed
// round's Usage to the session, day, worker, and total slices. Every
// execution of the workflow id is walked, oldest first — `daedalus
// continue` and restart create new executions under the same id, and an
// empty-run-id walk reads only the latest one, so a restarted session's
// earlier rounds used to vanish from the report (the same walk the -cot
// view makes; see listWorkflowRuns). Rounds whose history carries no Usage
// (workers predating capture) contribute nothing — absent from the slices,
// not zeroed.
func aggregateSessionUsage(c client.Client, workflowID string, rep *usageReport) error {
	dc := converter.GetDefaultDataConverter()
	// listWorkflowRuns exits on a quoted id (its filter cannot carry one) —
	// right for the -cot command, fatal for a report over every id on the
	// queue: a pre-gate workflow id carrying a metachar would kill the
	// whole report. Such an id degrades to the latest-run walk instead.
	runs := []string{""}
	if !strings.ContainsAny(workflowID, "'\\") {
		runs = listWorkflowRuns(c, workflowID)
	}
	if len(runs) == 0 {
		// Visibility saw no executions (a very fresh run, or a visibility
		// backend lagging behind): fall back to the latest run's history,
		// the pre-fix walk.
		runs = []string{""}
	}
	for _, runID := range runs {
		if err := aggregateRunUsage(c, workflowID, runID, dc, rep); err != nil {
			return err
		}
	}
	return nil
}

// aggregateRunUsage folds one execution's jailed rounds into the report —
// the per-run body of aggregateSessionUsage.
func aggregateRunUsage(c client.Client, workflowID, runID string, dc converter.DataConverter, rep *usageReport) error {
	scheduled := map[int64]string{}
	iter := c.GetWorkflowHistory(context.Background(), workflowID, runID,
		false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			return fmt.Errorf("read history of %s: %w", workflowID, err)
		}
		a := ev.GetActivityTaskCompletedEventAttributes()
		if a == nil {
			if s := ev.GetActivityTaskScheduledEventAttributes(); s != nil {
				scheduled[ev.GetEventId()] = s.GetActivityType().GetName()
			}
			continue
		}
		typ := scheduled[a.GetScheduledEventId()]
		if !agentActivityTypes[typ] {
			continue
		}
		ps := a.GetResult().GetPayloads()
		if len(ps) == 0 {
			continue
		}
		var u activities.Usage
		if typ == "RunJailedClaudeActivity" {
			var r activities.AgentRunResult
			if dc.FromPayload(ps[0], &r) != nil {
				continue
			}
			u = r.Usage
		} else {
			var r activities.ReviewResult
			if dc.FromPayload(ps[0], &r) != nil {
				continue
			}
			u = r.Usage
		}
		if u == (activities.Usage{}) {
			continue
		}
		worker := u.Worker
		if worker == "" {
			worker = "unknown"
		}
		rep.Total.add(u)
		t := rep.Sessions[workflowID]
		t.add(u)
		rep.Sessions[workflowID] = t
		bw := rep.ByWorker[worker]
		bw.add(u)
		rep.ByWorker[worker] = bw
		day := ev.GetEventTime().AsTime().Local().Format("2006-01-02")
		rep.Days[day] = dayAdd(rep.Days, day, u)
	}
	return nil
}

// dayAdd returns day's totals with u added — map values are not
// addressable, so the increment goes through a copy.
func dayAdd(m map[string]usageTotals, day string, u activities.Usage) usageTotals {
	t := m[day]
	t.add(u)
	return t
}

// printReportTable renders the report as tabwriter sections.
func printReportTable(rep *usageReport) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "AI PROVIDER USAGE — generated %s, queues: %s\n\n",
		rep.Generated.Local().Format("2006-01-02 15:04:05"), strings.Join(rep.Queues, ", "))

	fmt.Fprintln(w, "WORKERS ON THIS HOST")
	fmt.Fprintln(w, "WORKER\tSTATE\tSLOTS\tWAITING\tQUEUE\tSTATUS")
	for _, wr := range rep.Workers {
		state := "not running"
		if wr.Running {
			state = fmt.Sprintf("running (pid %d)", wr.PID)
		}
		fmt.Fprintf(w, "%s\t%s\t%d/%d\t%d\t%s\t%s\n",
			wr.Name, state, wr.SlotsBusy, wr.SlotsTotal, wr.RoundsWaiting, wr.TaskQueue, wr.StatusAge)
	}
	fmt.Fprintln(w, "")

	// Live runs and their health verdicts — the section that tells a
	// provider-starved crawl apart from a wedge (see runRow).
	fmt.Fprintln(w, "RUNS ON THIS QUEUE")
	fmt.Fprintln(w, "WORKFLOW ID\tSTATE\tLAST ROUND")
	for _, row := range rep.Runs {
		fmt.Fprintf(w, "%s\t%s\t%s\n", row.WorkflowID, row.State, row.LastRound)
	}
	fmt.Fprintln(w, "")

	writeUsageSection := func(title string, rows map[string]usageTotals) {
		fmt.Fprintln(w, title)
		fmt.Fprintln(w, "\tROUNDS\tCOST USD\tTOKENS IN\tTOKENS OUT\tCACHE READ\tCACHE WRITE\tTIME")
		for _, key := range sortedUsageKeys(rows) {
			writeTotalsRow(w, key, rows[key])
		}
		fmt.Fprintln(w, "")
	}
	writeUsageSection("BY SESSION", rep.Sessions)
	writeUsageSection("BY DAY", rep.Days)
	writeUsageSection("BY WORKER", rep.ByWorker)
	fmt.Fprintln(w, "TOTAL")
	fmt.Fprintln(w, "\tROUNDS\tCOST USD\tTOKENS IN\tTOKENS OUT\tCACHE READ\tCACHE WRITE\tTIME")
	writeTotalsRow(w, "all", rep.Total)
	w.Flush()
}

func writeTotalsRow(w *tabwriter.Writer, label string, t usageTotals) {
	fmt.Fprintf(w, "%s\t%d\t%.4f\t%d\t%d\t%d\t%d\t%s\n",
		label, t.Rounds, t.CostUSD, t.InputTokens, t.OutputTokens,
		t.CacheReadTokens, t.CacheWriteTokens, t.Duration.Round(time.Second))
}

func sortedUsageKeys(m map[string]usageTotals) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
