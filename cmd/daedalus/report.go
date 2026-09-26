// Report aggregates the raw per-round provider metrics captured in
// workflow history (activities.Usage) into per-session, per-day,
// per-worker, and total slices, and lists this host's workers with their
// live slot occupancy. It reads history through the same walk
// readPriorRun uses; aggregation happens at report time, keeping history
// lossless.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
)

// reportScope holds the parsed `daedalus report` flags.
type reportScope struct {
	queue string
	// queueSet distinguishes "no -q given" (default scope) from an
	// explicitly empty one, and gates the queue-name check below.
	queueSet bool
	all      bool
	jsonOut  bool
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
		case a == "--all":
			sc.all = true
		case a == "-q" || a == "--queue":
			if i+1 >= len(args) {
				return sc, fmt.Errorf("%s requires a value", a)
			}
			i++
			sc.queue = args[i]
			sc.queueSet = true
		case strings.HasPrefix(a, "--queue="):
			sc.queue = strings.TrimPrefix(a, "--queue=")
			sc.queueSet = true
		default:
			return sc, fmt.Errorf("unknown report flag %q (--all, -q/--queue, --json, --table)", a)
		}
	}
	if sc.all && sc.queueSet {
		return sc, errors.New("--all and -q/--queue are mutually exclusive")
	}
	// The queue goes verbatim into a Temporal list query, so a quote could
	// break (or steer) it; a plain token is all any real queue needs.
	if sc.queueSet && (sc.queue == "" || strings.ContainsAny(sc.queue, "'\\")) {
		return sc, fmt.Errorf("-q/--queue wants a plain queue name, got %q", sc.queue)
	}
	return sc, nil
}

// runReport scopes, gathers, and prints the usage report. Scoping: an
// explicit -q queue, --all (every task queue on record), or by default the
// invoking directory's resolved queue (configPath, honoring -c/--config).
// The Temporal address comes from the invoking config when one can be
// found, else the default — so -q/--all work from anywhere.
func runReport(configPath string, args []string) error {
	scope, err := parseReportArgs(args)
	if err != nil {
		return err
	}
	host := config.DefaultTemporalHost
	var queues []string
	path, cfgErr := resolveConfigPath(configPath)
	var cfg config.Config
	if cfgErr == nil {
		cfg, cfgErr = config.Load(path)
	}
	if cfgErr == nil {
		host = cfg.Temporal.Host
		queues = append(queues, cfg.Temporal.TaskQueue)
	} else if !scope.queueSet && !scope.all {
		// The default scope is the invoking config's queue — without a
		// loadable config there is nothing to report on.
		return cfgErr
	}
	switch {
	case scope.queue != "":
		queues = []string{scope.queue}
	case scope.all:
		recorded, err := recordQueues()
		if err != nil {
			return err
		}
		queues = append(queues, recorded...)
		if len(queues) == 0 {
			return fmt.Errorf("no queues on record in %s — pass -q <queue>", daemonDir)
		}
	}
	// Dedupe (the invoking config's queue may also be on record).
	sort.Strings(queues)
	uniq := queues[:0]
	for i, q := range queues {
		if i == 0 || q != queues[i-1] {
			uniq = append(uniq, q)
		}
	}
	queues = uniq

	c, err := newClient(config.Config{Temporal: config.TemporalConfig{Host: host}})
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

// recordQueues lists the distinct task queues named by the daemon-dir
// config records — the roster `report --all` covers, on this host's
// records alone. Unloadable records are skipped: one bad record must not
// hide the rest.
func recordQueues() ([]string, error) {
	names, err := recordedWorkers()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(names))
	var queues []string
	for _, name := range names {
		conf := recordedConfigPath(name)
		if conf == "" {
			continue
		}
		cfg, err := config.Load(conf)
		if err != nil {
			continue
		}
		if !seen[cfg.Temporal.TaskQueue] {
			seen[cfg.Temporal.TaskQueue] = true
			queues = append(queues, cfg.Temporal.TaskQueue)
		}
	}
	sort.Strings(queues)
	return queues, nil
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
	Sessions  map[string]usageTotals `json:"sessions"`
	Days      map[string]usageTotals `json:"days"`
	ByWorker  map[string]usageTotals `json:"by_worker"`
	Total     usageTotals            `json:"total"`
}

// statusStaleAfter is how long after a publish a status file is trusted:
// three missed intervals mean the worker stopped writing it (crashed or
// killed) and the file describes nothing live.
const statusStaleAfter = 3 * statusPublishInterval

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
	for _, info := range resp.GetExecutions() {
		if err := aggregateSessionUsage(c, info.GetExecution().GetWorkflowId(), rep); err != nil {
			return err
		}
	}
	return nil
}

// aggregateSessionUsage walks one workflow's history and adds each jailed
// round's Usage to the session, day, worker, and total slices. Rounds whose
// history carries no Usage (workers predating capture) contribute nothing —
// absent from the slices, not zeroed.
func aggregateSessionUsage(c client.Client, workflowID string, rep *usageReport) error {
	dc := converter.GetDefaultDataConverter()
	scheduled := map[int64]string{}
	iter := c.GetWorkflowHistory(context.Background(), workflowID, "",
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
