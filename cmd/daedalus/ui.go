// The dashboard UI (htmx-ui pt2): the views the metrics daemon serves on
// its LAN listener (metrics.port) — Workers (the `/` landing), Runs,
// Run detail, New Task, the frozen metrics view (the relay ledger's
// aggregates plus the live session counts; the GoLens embed itself is not
// part of this build), and the Models tab — the price table plus the
// round-2 usage surfaces (ui_metrics.go). Everything is
// read-only except the New-Task submit — which goes through
// internal/runstart, the exact path `daedalus run` submits through, so
// the form can never drift from the CLI's gates (Q4 = C) — and the
// Models price save, which writes the daemon dir's metrics-models.json.
// All views read
// the same walks the CLI reads: Workers and Runs ride the Temporal
// visibility list `list` uses (one shared status derivation,
// runDisplayStatus), Run detail's usage table reads the same history
// `daedalus worker report` aggregates (Q7), and current work comes from
// the per-run task logs the `log` command parses. The serving config is
// the daemon's own (the -c the daemon was started with): it names the
// Temporal host for every walk and the fallback queue for the runs feed;
// submissions resolve the target worker's own recorded config (Q5).
package main

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	workflow "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
	"github.com/ozzono/daedalus/internal/runstart"
	"github.com/ozzono/daedalus/internal/workflows"
)

//go:embed ui_templates.html
var uiTemplateFS embed.FS

// uiCacheCap bounds the server's caches — the started-event input cache,
// the provider-probe cache, the polled fragments' last-sent cache, and
// the CLI's closed-run park memo; past it a map drops wholesale. Inputs
// are history (immutable), probes are TTL-bound, fragments re-render from
// the same walks, and parks are immutable once closed, so a reset only
// costs re-reads, never staleness.
const uiCacheCap = 4096

// uiTestExcerptMax caps the test-log excerpt the detail page renders. The
// no-truncation rule governs what the app records (history, task logs,
// transcripts stay complete); the dashboard's excerpt panel is a viewport,
// and the complete log stays one `daedalus log` away.
const uiTestExcerptMax = 64 << 10

// uiServer is the dashboard's state: the serving config, the parsed
// templates, one reusable Temporal client (reset on error — the daemon
// outlives server restarts), the started-event input cache shared by the
// Workers and Runs renderers, the TTL-cached provider probes, the relay
// ledger — the same in-process ledger the relay appends to, read
// here for the metrics views — the operator's model price table (loaded
// from the daemon dir at boot, swapped whole on save, read-only to every
// renderer), and the last-sent fragment cache behind the polled views'
// unchanged-poll 204s.
type uiServer struct {
	cfg    config.Config
	tpl    *template.Template
	ledger *metricsrelay.Ledger

	mu     sync.Mutex
	cl     client.Client
	inputs map[string]workflows.PipelineInput
	probes map[string]uiProbe
	// probing marks the configs with a background probe refresh in
	// flight, so every expiring render does not start its own.
	probing map[string]bool
	frags   map[string][]byte
	prices  map[string]modelPrice
}

func newUIServer(cfg config.Config, ledger *metricsrelay.Ledger) *uiServer {
	tpl := template.Must(template.New("ui").Funcs(template.FuncMap{
		// div100 is the slots bar's integer percentage — busy*100/total,
		// total-proof.
		"div100": func(busy, total int) int {
			if total <= 0 {
				return 0
			}
			return busy * 100 / total
		},
		// tok and usd are the usage tables' token and cost formatters.
		"tok": fmtTokens,
		"usd": fmtUSD,
	}).ParseFS(uiTemplateFS, "ui_templates.html"))
	return &uiServer{cfg: cfg, tpl: tpl, ledger: ledger,
		inputs: map[string]workflows.PipelineInput{},
		probes: map[string]uiProbe{}, frags: map[string][]byte{},
		prices: loadModelPrices()}
}

// temporal returns the cached client, dialing on first use and after any
// error dropped the old one. Close is left to process exit — the daemon's
// lifetime is the server's.
func (s *uiServer) temporal() (client.Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cl != nil {
		return s.cl, nil
	}
	c, err := runstart.NewClient(s.cfg)
	if err != nil {
		return nil, err
	}
	s.cl = c
	return c, nil
}

// dropClient forgets a client that just failed, so the next poll dials
// fresh instead of replaying a dead connection forever.
func (s *uiServer) dropClient() {
	s.mu.Lock()
	s.cl = nil
	s.mu.Unlock()
}

// runInput returns the run's original pipeline input, decoded once from
// the workflow's started event and cached — the repo/issue/flow every
// view labels runs with. Only the map is held under the mutex: the walk
// itself runs unlocked (it can be slow on a loaded server, and s.mu also
// guards the shared Temporal client — holding it here would serialize
// every view behind every walk). The entries are immutable, so concurrent
// misses just decode twice. A failed decode degrades to the zero value
// and is cached (the payload is history — it will not improve on re-read);
// a failed walk is not cached, or one transient error would blank the
// run's labels until the cap reset.
func (s *uiServer) runInput(c client.Client, workflowID string) workflows.PipelineInput {
	s.mu.Lock()
	in, ok := s.inputs[workflowID]
	s.mu.Unlock()
	if ok {
		return in
	}
	var decoded workflows.PipelineInput
	walkErr := false
	dc := converter.GetDefaultDataConverter()
	iter := c.GetWorkflowHistory(context.Background(), workflowID, "",
		false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			walkErr = true
			break
		}
		if att := ev.GetWorkflowExecutionStartedEventAttributes(); att != nil {
			// Only the started event carries the input; stop as soon as it
			// is seen (or undecodable).
			if ps := att.GetInput().GetPayloads(); len(ps) > 0 {
				if dc.FromPayload(ps[0], &decoded) != nil {
					decoded = workflows.PipelineInput{}
				}
			}
			break
		}
	}
	if walkErr {
		return workflows.PipelineInput{}
	}
	s.mu.Lock()
	if len(s.inputs) >= uiCacheCap {
		s.inputs = map[string]workflows.PipelineInput{}
	}
	s.inputs[workflowID] = decoded
	s.mu.Unlock()
	return decoded
}

// routes builds the mux behind the decided route map (Q14). Pages render
// the full shell; the `frag_` endpoints render only the polled regions
// htmx swaps. No CORS headers are set: the decided posture is an
// unauthenticated LAN listener — reachable by direct network peers, not
// by arbitrary web pages the operator's browser visits.
func (s *uiServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.pageWorkers)
	mux.HandleFunc("GET /workers", s.pageWorkers)
	mux.HandleFunc("GET /workers/rows", s.fragWorkers)
	mux.HandleFunc("GET /runs", s.pageRuns)
	mux.HandleFunc("GET /runs/rows", s.fragRuns)
	mux.HandleFunc("GET /runs/{id}", s.pageRun)
	mux.HandleFunc("GET /runs/{id}/body", s.fragRun)
	mux.HandleFunc("GET /runs/{id}/cot", s.fragCot)
	mux.HandleFunc("GET /new", s.pageNew)
	mux.HandleFunc("GET /new/agents", s.fragAgents)
	mux.HandleFunc("POST /new", s.submitNew)
	// The Models tab: the price form (GET) and its upsert-only save
	// (POST, a redirect back to the form).
	mux.HandleFunc("GET /models", s.pageModels)
	mux.HandleFunc("POST /models", s.saveModels)
	// /metrics and everything under it is the metrics view — the paths
	// beneath are the GoLens mount's when the embed exists; today they all
	// render the same aggregates-plus-placeholder page.
	mux.HandleFunc("GET /metrics", s.pageMetrics)
	mux.HandleFunc("GET /metrics/", s.pageMetrics)
	return mux
}

// render executes one template into memory before touching the writer —
// a mid-render error answers with a plain 500 instead of a torn page.
func (s *uiServer) render(w http.ResponseWriter, status int, name string, data any) {
	var b bytes.Buffer
	if err := s.tpl.ExecuteTemplate(&b, name, data); err != nil {
		http.Error(w, "render "+name+": "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(b.Bytes())
}

// renderFrag is render for the three polled fragments, with the polled
// views' click race folded in: a poll whose fresh bytes equal the last
// bytes sent for that URI answers 204 No Content — htmx counts 204 a
// successful poll but never swaps it, so stable content means stable DOM
// nodes and a click can no longer land on a card the poll just replaced.
// The cache is keyed by request URI (the fragments' filter and run-id
// state ride in it) and is per-process, so a second browser tab sharing a
// URI shares entries; that is only visible when content changed between
// this tab's last own swap and another tab's latest poll — one further
// content change and the tab resyncs. ponytail: a single-operator LAN
// tool, so that window is left as is; per-tab byte tracking is the fix
// if it ever matters.
func (s *uiServer) renderFrag(w http.ResponseWriter, req *http.Request, name string, data any) {
	var b bytes.Buffer
	if err := s.tpl.ExecuteTemplate(&b, name, data); err != nil {
		http.Error(w, "render "+name+": "+err.Error(), http.StatusInternalServerError)
		return
	}
	key := req.URL.RequestURI()
	fresh := b.Bytes()
	s.mu.Lock()
	if len(s.frags) >= uiCacheCap {
		s.frags = map[string][]byte{}
	}
	unchanged := bytes.Equal(s.frags[key], fresh)
	s.frags[key] = fresh
	s.mu.Unlock()
	if unchanged {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(fresh)
}

// uiChrome is the shell data every page carries: the active nav tab and
// the document title.
type uiChrome struct {
	Nav   string
	Title string
}

// badgeClass maps a display status onto the component kit's badge colors —
// the claudio mapping, decided: emerald running, amber waiting, muted
// approved, red failed/parked (and paused, the taxonomy's parked face).
func badgeClass(status string) string {
	switch status {
	case "running":
		return "b-run"
	case "waiting", "pending":
		return "b-wait"
	case "approved", "completed":
		return "b-idle"
	case "parked", "paused", "failed":
		return "b-fail"
	}
	return "b-idle"
}

// clockOrDate renders a timestamp for the tables: today's rows show the
// wall clock, older ones the date.
func clockOrDate(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	t = t.Local()
	if time.Since(t) < 24*time.Hour {
		return t.Format("15:04:05")
	}
	return t.Format("2006-01-02 15:04")
}

// relTime renders an age the way the samples do; zero (no attribute) is a
// dash, not a fabricated epoch.
func relTime(t time.Time, now time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := now.Sub(t)
	switch {
	case d < 45*time.Second:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

// uiSnapshot is one pass over the host's queues: the runs feed every view
// renders from, plus the per-queue live depth the worker cards show. One
// walk serves both the Workers and Runs renderers so the two can never
// disagree within a response.
type uiSnapshot struct {
	Generated time.Time
	Queues    []string
	Rows      []uiRunRow
	Workers   []uiWorkerCard
	// WalkErr names a failed visibility walk (the feed-down note the runs
	// view shows); the roster and the probes render regardless.
	WalkErr string
}

// uiRunRow is one runs-feed row, display-ready. startedAt backs the feed's
// sort (the rendered Started string is clock-or-date and would sort every
// older-date row above today's); templates never see it.
type uiRunRow struct {
	WorkflowID string
	Status     string
	Badge      string
	Issue      string
	Repo       string
	Queue      string
	Started    string
	LastAt     string

	startedAt time.Time
}

// uiWork is one in-flight round on a worker's queue.
type uiWork struct {
	WorkflowID string
	Repo       string
	Issue      string
	Stage      string
	Agent      string
	Round      int
}

// uiProbeTTL is how long one worker config's provider probes serve the
// roster: each probe is a real billed completion request on the worker's
// API key, and the CLI earns one per explicit `worker status` — the
// dashboard re-renders every few seconds, so it replays a cached answer
// instead of turning the poll cadence into a paid request stream.
const uiProbeTTL = 2 * time.Minute

// uiProbe is one cached probe outcome and its fetch time.
type uiProbe struct {
	p  providerProbes
	at time.Time
}

// probeFor returns the config's probes, serving uiProbeTTL off the cache.
// An expired entry never blocks the render: probeProvider dials the
// worker's provider — it can block seconds, and once it blocked every
// page render and poll that walked the roster, so a navigation landed
// only after the provider answered. The refresh instead runs in the
// background (one in flight per config — two renders racing one expiry
// just probe twice otherwise) and a later poll picks the fresh probes
// up; the current answer renders stale for one cycle, which is the
// badge's own contract ("TTL-cached, may be stale"). An entry that does
// not exist yet — first render after a daemon start, or after the cache
// cap reset — has no current answer, and the zero struct's !Main.OK
// would paint a failed face for a probe that never ran: the NA contract
// (non-empty means nothing was probed, with the reason) covers it until
// the first probe lands.
func (s *uiServer) probeFor(confPath string) providerProbes {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.probes[confPath]
	if confPath == "" || time.Since(p.at) < uiProbeTTL {
		return p.p
	}
	if s.probing == nil {
		s.probing = map[string]bool{}
	}
	// Mark and spawn before any early return: the goroutine is the only
	// writer of s.probes, so returning first would leave a cold entry
	// probing forever.
	cold := p.at.IsZero()
	if !s.probing[confPath] {
		s.probing[confPath] = true
		go func() {
			fresh := probeProvider(confPath)
			s.mu.Lock()
			defer s.mu.Unlock()
			if len(s.probes) >= uiCacheCap {
				s.probes = map[string]uiProbe{}
			}
			s.probes[confPath] = uiProbe{p: fresh, at: time.Now()}
			delete(s.probing, confPath)
		}()
	}
	if cold {
		return providerProbes{NA: "probing…"}
	}
	return p.p
}

// uiWorkerCard is one roster card, display-ready.
type uiWorkerCard struct {
	Name      string
	Running   bool
	PID       int
	Queue     string
	SlotsBusy int
	SlotsFull int
	Waiting   int
	StatusAge string
	// Probes are filled only for a running worker (a stopped one's
	// provider says nothing about its next round — there isn't one).
	ProbeMain    string
	ProbeMainOK  bool
	ProbeFB      string
	ProbeFBOK    bool
	ProbeFBArmed bool
	ProbeNA      string
	Depth        int
	Work         []uiWork
	HasConf      bool
}

// hostQueues derives the queues the dashboard scopes to: every queue this
// host's workers have been seen on (their status files and config
// records), plus the daemon's own.
func (s *uiServer) hostQueues(rows []workerRow) []string {
	seen := map[string]bool{}
	var qs []string
	add := func(q string) {
		if q != "" && !seen[q] {
			seen[q] = true
			qs = append(qs, q)
		}
	}
	for _, r := range rows {
		add(r.TaskQueue)
		if conf := recordedConfigPath(r.Name); conf != "" {
			if cfg, err := config.Load(conf); err == nil {
				add(cfg.Temporal.TaskQueue)
			}
		}
	}
	add(s.cfg.Temporal.TaskQueue)
	sort.Strings(qs)
	return qs
}

// snapshot walks the host's queues once and assembles everything the
// Workers and Runs renderers need. Failures degrade per piece: a failed
// queue walk notes WalkErr and keeps the rest; a worker with no probe
// target shows n/a; a run without a decodable input renders empty labels.
func (s *uiServer) snapshot() uiSnapshot {
	snap := uiSnapshot{Generated: time.Now()}
	rows := collectWorkerRows()
	queues := s.hostQueues(rows)
	snap.Queues = queues

	byQueue := map[string][]uiRunRow{}
	if c, err := s.temporal(); err != nil {
		snap.WalkErr = err.Error()
	} else {
		dc := converter.GetDefaultDataConverter()
		for _, q := range queues {
			resp, err := c.ListWorkflow(context.Background(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: "default",
				PageSize:  100,
				Query:     fmt.Sprintf("TaskQueue = '%s'", q),
			})
			if err != nil {
				s.dropClient()
				snap.WalkErr = err.Error()
				continue
			}
			for _, info := range resp.GetExecutions() {
				vis := decodeRunVisibility(info.GetSearchAttributes(), dc)
				in := s.runInput(c, info.GetExecution().GetWorkflowId())
				row := uiRunRow{
					WorkflowID: info.GetExecution().GetWorkflowId(),
					Status:     runDisplayStatus(c, info, vis),
					Issue:      in.IssueID,
					Repo:       in.RepoPath,
					Queue:      q,
					Started:    clockOrDate(info.GetStartTime().AsTime()),
				}
				row.startedAt = info.GetStartTime().AsTime()
				if closed := info.GetCloseTime(); closed.IsValid() {
					row.LastAt = relTime(closed.AsTime(), snap.Generated)
				} else {
					row.LastAt = relTime(vis.LastAt, snap.Generated)
				}
				row.Badge = badgeClass(row.Status)
				snap.Rows = append(snap.Rows, row)
				byQueue[q] = append(byQueue[q], row)
			}
		}
	}
	sort.SliceStable(snap.Rows, func(i, j int) bool {
		return snap.Rows[i].startedAt.After(snap.Rows[j].startedAt)
	})

	live := func(q string) int {
		n := 0
		for _, r := range byQueue[q] {
			if r.Status == "running" || r.Status == "waiting" {
				n++
			}
		}
		return n
	}
	for _, r := range rows {
		card := uiWorkerCard{
			Name:      r.Name,
			Running:   r.Running,
			PID:       r.PID,
			Queue:     r.TaskQueue,
			SlotsBusy: r.SlotsBusy,
			SlotsFull: r.SlotsTotal,
			Waiting:   r.RoundsWaiting,
			StatusAge: r.StatusAge,
			HasConf:   recordedConfigPath(r.Name) != "",
		}
		if card.Queue == "" {
			if conf := recordedConfigPath(r.Name); conf != "" {
				if cfg, err := config.Load(conf); err == nil {
					card.Queue = cfg.Temporal.TaskQueue
				}
			}
		}
		if card.Running && card.HasConf {
			p := s.probeFor(recordedConfigPath(r.Name))
			card.ProbeMain, card.ProbeMainOK = p.Main.Detail, p.Main.OK
			card.ProbeFB, card.ProbeFBOK = p.Fb.Detail, p.Fb.OK
			card.ProbeFBArmed = p.FbArmed
			card.ProbeNA = p.NA
		}
		if card.Queue != "" {
			card.Depth = live(card.Queue)
			if card.Running {
				card.Work = s.queueWork(byQueue[card.Queue])
			}
		}
		snap.Workers = append(snap.Workers, card)
	}
	return snap
}

// queueWork derives a queue's in-flight rounds from its running runs' task
// logs — the same parser `daedalus log` uses. The work is attributed to
// the queue, not the worker process: history stamps Usage.Worker only at
// round end and the task-log header carries no worker name, so on a host
// where several live workers share one queue every one of them shows the
// queue's work. ponytail: per-worker attribution waits for a round-start
// stamp that names the worker.
func (s *uiServer) queueWork(rows []uiRunRow) []uiWork {
	c, err := s.temporal()
	if err != nil {
		return nil
	}
	var works []uiWork
	for _, r := range rows {
		if r.Status != "running" && r.Status != "waiting" {
			continue
		}
		path, err := activities.TaskLogPath(r.WorkflowID)
		if err != nil {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			continue // no log, no round in flight
		}
		log := string(data)
		name, stage, _, running := lastRoundState(log)
		if !running {
			continue
		}
		if stage == "" {
			stage = "unknown"
		}
		in := s.runInput(c, r.WorkflowID)
		works = append(works, uiWork{
			WorkflowID: r.WorkflowID,
			Repo:       in.RepoPath,
			Issue:      in.IssueID,
			Stage:      stage,
			Agent:      name,
			Round:      strings.Count(log, " round started: "),
		})
	}
	return works
}

// pageWorkers renders `/` and `/workers` — the dashboard's landing view.
func (s *uiServer) pageWorkers(w http.ResponseWriter, req *http.Request) {
	snap := s.snapshot()
	s.render(w, http.StatusOK, "page_workers", struct {
		Chrome uiChrome
		Snap   uiSnapshot
	}{uiChrome{Nav: "workers", Title: "daedalus — workers"}, snap})
}

// fragWorkers is the workers view's polled region: the cards alone.
func (s *uiServer) fragWorkers(w http.ResponseWriter, req *http.Request) {
	snap := s.snapshot()
	s.renderFrag(w, req, "frag_workers", snap)
}

// uiFilters carries the runs feed's filter state; the selects re-render
// the rows server-side (no client-side filtering to drift).
type uiFilters struct {
	Queues   []string
	Queue    string
	Statuses []string
	Status   string
}

// runStatuses is the filter's option set: every status the shared
// derivation can render — the DaedalusStatus vocabulary (Q6) for live
// runs plus the display taxonomy's closed faces (completed, paused) and
// the pending hold.
var runStatuses = []string{"running", "waiting", "pending", "approved", "parked", "failed", "completed", "paused"}

func (s *uiServer) pageRuns(w http.ResponseWriter, req *http.Request) {
	s.runsView(w, req, "page_runs")
}

// fragRuns is the runs view's polled region: the table body alone, so the
// filter selects are never swapped out from under the operator's cursor.
func (s *uiServer) fragRuns(w http.ResponseWriter, req *http.Request) {
	s.runsView(w, req, "frag_runs")
}

func (s *uiServer) runsView(w http.ResponseWriter, req *http.Request, tpl string) {
	snap := s.snapshot()
	f := uiFilters{
		Queues:   snap.Queues,
		Queue:    req.URL.Query().Get("queue"),
		Statuses: runStatuses,
		Status:   req.URL.Query().Get("status"),
	}
	rows := snap.Rows
	if f.Queue != "" {
		var kept []uiRunRow
		for _, r := range rows {
			if r.Queue == f.Queue {
				kept = append(kept, r)
			}
		}
		rows = kept
	}
	if f.Status != "" {
		var kept []uiRunRow
		for _, r := range rows {
			if r.Status == f.Status {
				kept = append(kept, r)
			}
		}
		rows = kept
	}
	data := struct {
		Chrome  uiChrome
		Filters uiFilters
		Rows    []uiRunRow
		WalkErr string
	}{uiChrome{Nav: "runs", Title: "daedalus — runs"}, f, rows, snap.WalkErr}
	if tpl == "frag_runs" {
		s.renderFrag(w, req, tpl, data)
		return
	}
	s.render(w, http.StatusOK, tpl, data)
}

// uiUsage is one usage-table row — the per-round figures `daedalus worker
// report` aggregates, here raw per the no-truncation stance (the sums are
// GoLens's charts' job). NA renders the literal `n/a` for agents without
// capture.
type uiUsage struct {
	Round    int
	Role     string
	Agent    string
	In       int64
	Out      int64
	Duration string
	Cost     string
	NA       bool
}

func usageRow(round int, role, agent string, u activities.Usage) uiUsage {
	return uiUsage{
		Round:    round,
		Role:     role,
		Agent:    agent,
		In:       u.InputTokens,
		Out:      u.OutputTokens,
		Duration: dur(u.Duration),
		Cost:     fmt.Sprintf("%.4f", u.CostUSD),
		NA:       u == (activities.Usage{}),
	}
}

// uiNode is one timeline-rail entry: an agent round or a test-suite run,
// in history order.
type uiNode struct {
	Kind     string // "round" | "test"
	Role     string
	Agent    string
	Outcome  string
	Badge    string
	Duration string
}

// uiRunDetail is the run-detail page's data. Absolute times only — an
// unchanged run renders byte-identical across polls, so the htmx swap is
// visually silent (the decided behavior).
type uiRunDetail struct {
	WorkflowID string
	RunID      string
	Status     string
	Badge      string
	// ParkCause carries a parked run's own reason (the PARKED: result's
	// text, or the awaiting-maintainer hold on older workers) and
	// FailCause the last failed activity's failure message — the two
	// closed faces whose cause the badge alone drops; both empty when the
	// walk found none.
	ParkCause   string
	FailCause   string
	Issue       string
	Repo        string
	Flow        string
	Branch      string
	Queue       string
	DependsOn   string
	Started     string
	LastAt      string
	TemporalURL string
	Nodes       []uiNode
	Usage       []uiUsage
	TotalCost   string
	// LedgerCost is the run's relay-ledger cost section (ui_metrics.go):
	// per-model rows over the run's own ledger entries, priced models
	// only. LedgerTotal is the rendered sum, or the dash when no prices
	// are saved or the run has no priced keyed calls.
	LedgerCost  []uiRunModelCost
	LedgerTotal string
	TestFound   bool
	TestPassed  bool
	TestCommand string
	TestExcerpt string
	ReviewFound bool
	ReviewVd    string
	ReviewBadge string
	ReviewBody  string
	// LogTail is the run's captured task log, bounded to its tail, with
	// the CLI's freshness line appended (taskLogTail) — the same material
	// `daedalus log` prints, kept out of the page's first paint inside a
	// collapsed details.
	LogTail string
}

// reviewVerdict names a reviewer round's outcome and its badge — the
// four-marker protocol plus the marker-less faces (the workflow's
// CHANGES_REQUESTED equivalence, with the repair layer's likely-verdict
// annotation when it produced one).
func reviewVerdict(r activities.ReviewResult) (string, string) {
	switch {
	case r.Approved:
		return "APPROVED", "b-run"
	case r.NeedsMaintainer:
		return "NEEDS_MAINTAINER", "b-fail"
	case r.Rebuild:
		return "REBUILD", "b-wait"
	case r.NoVerdict:
		if r.LikelyVerdict != "" {
			return "NO VERDICT → likely " + r.LikelyVerdict, "b-fail"
		}
		return "NO VERDICT", "b-fail"
	default:
		return "CHANGES_REQUESTED", "b-wait"
	}
}

// tailStr keeps a panel excerpt's last max bytes, cut on a line boundary
// so the visible half never opens mid-line.
func tailStr(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := s[len(s)-max:]
	if i := strings.IndexByte(cut, '\n'); i >= 0 {
		cut = cut[i+1:]
	}
	return cut
}

// temporalUILink builds the outbound link to the Temporal UI's page for
// the workflow — the audit view the dashboard deliberately does not
// duplicate. Empty when the host does not parse (no strip, not a broken
// link).
func temporalUILink(hostPort, workflowID string) string {
	host := hostPort
	if h, _, err := net.SplitHostPort(hostPort); err == nil {
		host = h
	}
	if host == "" {
		return ""
	}
	return fmt.Sprintf("http://%s:8233/namespaces/default/workflows/%s", host, url.PathEscape(workflowID))
}

// schedAgent decodes the agent override from a scheduled round's input —
// AgentRunInput for the working agent's rounds, ReviewInput for the
// reviewer's. The worker's own agent (the common case: no -cli override)
// is out of reach per round, so the column shows the override or "-".
func scheduledAgent(ps []*commonpb.Payload, dc converter.DataConverter) string {
	if len(ps) == 0 {
		return ""
	}
	var in activities.AgentRunInput
	if dc.FromPayload(ps[0], &in) == nil {
		return in.Agent
	}
	var rv activities.ReviewInput
	if dc.FromPayload(ps[0], &rv) == nil {
		return rv.Agent
	}
	return ""
}

// schedRole decodes the round's session role from a scheduled input (the
// same two payload shapes as scheduledAgent).
func schedRole(ps []*commonpb.Payload, dc converter.DataConverter) activities.SessionRole {
	if len(ps) == 0 {
		return ""
	}
	var in activities.AgentRunInput
	if dc.FromPayload(ps[0], &in) == nil {
		return in.Role
	}
	var rv activities.ReviewInput
	if dc.FromPayload(ps[0], &rv) == nil {
		return rv.Role
	}
	return ""
}

// roundRole names an agent round for the rail and the usage table.
func roundRole(role activities.SessionRole) string {
	switch role {
	case activities.RoleDev:
		return "implement"
	case activities.RoleTest:
		return "test"
	}
	return "agent"
}

// detail walks one run's full history — the same material `daedalus
// worker report` aggregates and `daedalus log` parses — into the page's
// typed view. Any single decode failure skips its piece; the page never
// fails whole over one malformed event.
func (s *uiServer) detail(workflowID string) (uiRunDetail, error) {
	d := uiRunDetail{WorkflowID: workflowID, Branch: "-"}
	c, err := s.temporal()
	if err != nil {
		return d, err
	}
	resp, err := c.DescribeWorkflowExecution(context.Background(), workflowID, "")
	if err != nil {
		s.dropClient()
		return d, fmt.Errorf("describe %s: %w", workflowID, err)
	}
	info := resp.GetWorkflowExecutionInfo()
	d.RunID = info.GetExecution().GetRunId()
	d.Queue = info.GetTaskQueue()
	dc := converter.GetDefaultDataConverter()
	vis := decodeRunVisibility(info.GetSearchAttributes(), dc)
	d.Status = runDisplayStatus(c, info, vis)
	d.Badge = badgeClass(d.Status)
	// The paused face covers park and abort; the park reason is what
	// tells them apart, and it rides the probe runDisplayStatus may have
	// just made (memoized — a stamped park probes once here).
	if d.Status == "paused" {
		if _, reason := closedRunParkReason(c, d.WorkflowID, d.RunID); reason != "" {
			d.ParkCause = reason
		}
	}
	d.DependsOn = vis.DependsOn
	d.Started = clockOrDate(info.GetStartTime().AsTime())
	if closed := info.GetCloseTime(); closed.IsValid() {
		d.LastAt = clockOrDate(closed.AsTime())
	} else {
		d.LastAt = clockOrDate(vis.LastAt)
	}
	in := s.runInput(c, workflowID)
	d.Issue, d.Repo, d.Flow = in.IssueID, in.RepoPath, in.Flow
	d.Branch = s.runBranch(c, info, in)
	d.TemporalURL = temporalUILink(s.cfg.Temporal.Host, workflowID)

	// One history pass builds the rail, the usage rows, and the latest
	// test and review panels.
	type sched struct {
		name  string
		agent string
		role  activities.SessionRole
	}
	scheduled := map[int64]sched{}
	at := map[int64]time.Time{}
	roundN := 0
	total := 0.0

	addRound := func(role, agent, outcome, badge string, span time.Duration, u uiUsage) {
		roundN++
		u.Round = roundN
		u.Role, u.Agent = role, agent
		d.Nodes = append(d.Nodes, uiNode{Kind: "round", Role: role, Agent: agent, Outcome: outcome, Badge: badge, Duration: dur(span)})
		d.Usage = append(d.Usage, u)
	}

	iter := c.GetWorkflowHistory(context.Background(), workflowID, "",
		false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			return d, fmt.Errorf("read history of %s: %w", workflowID, err)
		}
		switch {
		case ev.GetActivityTaskScheduledEventAttributes() != nil:
			a := ev.GetActivityTaskScheduledEventAttributes()
			scheduled[ev.GetEventId()] = sched{
				name:  a.GetActivityType().GetName(),
				agent: scheduledAgent(a.GetInput().GetPayloads(), dc),
				role:  schedRole(a.GetInput().GetPayloads(), dc),
			}
			at[ev.GetEventId()] = ev.GetEventTime().AsTime()
		case ev.GetActivityTaskCompletedEventAttributes() != nil:
			a := ev.GetActivityTaskCompletedEventAttributes()
			sc := scheduled[a.GetScheduledEventId()]
			span := ev.GetEventTime().AsTime().Sub(at[a.GetScheduledEventId()])
			ps := a.GetResult().GetPayloads()
			switch sc.name {
			case "RunJailedClaudeActivity":
				u := uiUsage{NA: true}
				if len(ps) > 0 {
					var r activities.AgentRunResult
					if dc.FromPayload(ps[0], &r) == nil {
						u = usageRow(0, "", "", r.Usage)
						total += r.Usage.CostUSD
					}
				}
				addRound(roundRole(sc.role), sc.agent, "ok", "b-run", span, u)
			case "RunJailedReviewerActivity":
				u := uiUsage{NA: true}
				outcome, badge := "ok", "b-run"
				if len(ps) > 0 {
					var r activities.ReviewResult
					if dc.FromPayload(ps[0], &r) == nil {
						u = usageRow(0, "", "", r.Usage)
						total += r.Usage.CostUSD
						outcome, badge = reviewVerdict(r)
						d.ReviewFound = true
						d.ReviewVd, d.ReviewBadge = outcome, badge
						d.ReviewBody = r.Comments
					}
				}
				addRound("review", sc.agent, outcome, badge, span, u)
			case "RunTestSuiteActivity", "RunNativeTestsActivity":
				tn := uiNode{Kind: "test", Role: "suite", Outcome: "failed", Badge: "b-fail", Duration: dur(span)}
				if len(ps) > 0 {
					var r activities.TestResult
					if dc.FromPayload(ps[0], &r) == nil {
						if r.Passed {
							tn.Outcome, tn.Badge = "pass", "b-run"
						} else {
							tn.Outcome, tn.Badge = "fail", "b-fail"
						}
						d.TestFound = true
						d.TestPassed = r.Passed
						d.TestCommand = r.Command
						d.TestExcerpt = tailStr(r.Logs, uiTestExcerptMax)
					}
				}
				d.Nodes = append(d.Nodes, tn)
			}
		case ev.GetActivityTaskFailedEventAttributes() != nil:
			a := ev.GetActivityTaskFailedEventAttributes()
			// The failure message is the cause the failed badge drops;
			// the walk is chronological, so the last failure is the
			// proximate one.
			d.FailCause = a.GetFailure().GetMessage()
			sc := scheduled[a.GetScheduledEventId()]
			span := ev.GetEventTime().AsTime().Sub(at[a.GetScheduledEventId()])
			switch sc.name {
			case "RunJailedClaudeActivity":
				addRound(roundRole(sc.role), sc.agent, "failed", "b-fail", span, uiUsage{NA: true})
			case "RunJailedReviewerActivity":
				addRound("review", sc.agent, "failed", "b-fail", span, uiUsage{NA: true})
			case "RunTestSuiteActivity", "RunNativeTestsActivity":
				d.Nodes = append(d.Nodes, uiNode{Kind: "test", Role: "suite", Outcome: "failed", Badge: "b-fail", Duration: dur(span)})
			}
		}
	}
	d.TotalCost = fmt.Sprintf("%.4f", total)
	d.LedgerCost, d.LedgerTotal = s.runLedgerCost(workflowID)
	d.LogTail = taskLogTail(workflowID)
	return d, nil
}

// taskLogTail reads the run's captured task log for the detail view — the
// material `daedalus log` prints, bounded to its tail so a long session
// cannot bloat every poll's payload, with the same freshness line the CLI
// appends. A missing or unreadable log degrades to a note: the detail page
// never fails on it.
func taskLogTail(workflowID string) string {
	const uiLogTailBytes = 8192
	path, err := activities.TaskLogPath(workflowID)
	if err != nil {
		return fmt.Sprintf("(no task log: %v)", err)
	}
	// Seek-window read, not ReadFile: the detail body polls every few
	// seconds, so the read is O(window), never O(file) — a tens-of-MB
	// session log made every poll an O(file) read plus two O(file)
	// string conversions for material only the last 8 KiB of which
	// renders (ui-tasklogtail-whole-file-read-per-poll in
	// backlog/bugs/bug-index.md). Only the final round's headers bear on
	// the freshness state, and the window holds them.
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "(no task log yet — the run has not started any rounds)"
		}
		return fmt.Sprintf("(task log unreadable: %v)", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fmt.Sprintf("(task log unreadable: %v)", err)
	}
	start := max(0, info.Size()-uiLogTailBytes)
	buf := make([]byte, info.Size()-start)
	n, err := f.ReadAt(buf, start)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Sprintf("(task log unreadable: %v)", err)
	}
	// n < len(buf) (with io.EOF) means the file shrank between the Stat
	// and the read; rendering the zero tail would print NUL padding.
	tail := string(buf[:n])
	// the byte window can split a rune; continuation bytes say so
	for len(tail) > 0 && tail[0]&0xC0 == 0x80 {
		tail = tail[1:]
	}
	return tail + logTail(workflowID, plainTailState(tail))
}

// runBranch resolves the run's branch line for the detail header: the
// preserved branch for an approved close, the aborted branch for any
// other close, the in-flight feat/ glob for a live run. Anything
// unresolvable renders as "-" — a degraded label, never a guess.
func (s *uiServer) runBranch(c client.Client, info *workflow.WorkflowExecutionInfo, in workflows.PipelineInput) string {
	scope := workflows.FlowScope(in.Flow)
	aborted, err := activities.AbortedBranchNameFor(in.IssueID, scope)
	if err != nil {
		return "-"
	}
	if closed := info.GetCloseTime(); closed.IsValid() {
		if info.GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			return aborted
		}
		pctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var out string
		if err := c.GetWorkflow(pctx, info.GetExecution().GetWorkflowId(), info.GetExecution().GetRunId()).Get(pctx, &out); err != nil {
			return aborted
		}
		if workflows.IsParkedResult(out) {
			return aborted
		}
		return out
	}
	if in.RepoPath == "" {
		return "-"
	}
	pattern := "feat/" + scope + "issue-" + in.IssueID + "-*"
	cmd := exec.Command("git", "-C", in.RepoPath, "branch", "--list", pattern)
	cmd.Env = runstart.EnvWithoutGitRepoOverrides(os.Environ())
	out, err := cmd.Output()
	if err != nil {
		return "-"
	}
	b := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(out)), "* "))
	if b == "" {
		return "-"
	}
	return strings.Fields(b)[0]
}

// pageRun renders the run-detail page shell.
func (s *uiServer) pageRun(w http.ResponseWriter, req *http.Request) {
	s.runBody(w, req, "page_run")
}

// fragRun is the detail page's polled body.
func (s *uiServer) fragRun(w http.ResponseWriter, req *http.Request) {
	s.runBody(w, req, "frag_run")
}

// uiCot is the run detail's last-round CoT box payload.
type uiCot struct {
	Err  string
	Head string
	Body string
}

// fragCot serves the detail page's collapsed CoT box, fetched on first
// open — the walk reads every execution's full history (the same walk the
// CLI's `log -cot` prints), so rendering it into the polled body would
// double every poll's work for a box that is closed almost all of the
// time. It serves the tail-1 view: the last completed round's section,
// with the in-flight live section standing in before any round completes.
func (s *uiServer) fragCot(w http.ResponseWriter, req *http.Request) {
	id := req.PathValue("id")
	// listWorkflowRuns exits on an id its visibility query cannot carry;
	// the daemon must answer, not die, so the guard lands here first.
	if strings.ContainsAny(id, "'\\") {
		s.render(w, http.StatusOK, "frag_cot", uiCot{Err: "workflow id carries characters the CoT walk cannot query"})
		return
	}
	c, err := s.temporal()
	if err != nil {
		s.render(w, http.StatusOK, "frag_cot", uiCot{Err: err.Error()})
		return
	}
	var out uiCot
	runs := listWorkflowRuns(c, id)
	if len(runs) == 0 {
		runs = []string{""}
	}
	sections, err := cotSections(c, id, runs)
	if err != nil {
		s.render(w, http.StatusOK, "frag_cot", uiCot{Err: err.Error()})
		return
	}
	if len(sections) > 0 {
		out.Head, out.Body, _ = strings.Cut(sections[len(sections)-1], "\n")
	}
	if out.Head == "" {
		if live := liveCotSectionWith(id, c); live != "" {
			out.Head, out.Body, _ = strings.Cut(strings.TrimRight(live, "\n"), "\n")
		}
	}
	s.render(w, http.StatusOK, "frag_cot", out)
}

func (s *uiServer) runBody(w http.ResponseWriter, req *http.Request, tpl string) {
	d, err := s.detail(req.PathValue("id"))
	if err != nil {
		// The polled error face joins the fragment cache under the poll's
		// URI — identical errors dedupe harmlessly (the DOM already holds
		// the card), and a recovery whose bytes differ from the cached
		// error re-sends 200. An uncached error render would leave the
		// cache holding the stale healthy bytes, so the recovery poll
		// would dedupe into a 204 and pin the error card in the DOM. The
		// page shell stays on render: a 204 on a navigation renders
		// nothing.
		if tpl == "frag_run" {
			s.renderFrag(w, req, "frag_error", err.Error())
			return
		}
		// The page shell answers with the full document — head, header,
		// nav, script — never the bare fragment. A fragment as the whole
		// page is what a deep-link refresh landed on when the detail could
		// not resolve (the Temporal dial is lazy, so the window includes
		// every daemon restart and blip): a nav-less error card that read
		// as the view being lost. Inside the shell the error is one card
		// among live navigation, and a refresh retries the content.
		s.render(w, http.StatusOK, "page_error", struct {
			Chrome uiChrome
			Err    string
		}{uiChrome{Nav: "runs", Title: "daedalus — error"}, err.Error()})
		return
	}
	// frag_run reads the bare detail — here and where page_run includes it
	// with .D — so it must never be handed the wrapper; only the page
	// shell takes the chrome.
	if tpl == "frag_run" {
		s.renderFrag(w, req, tpl, d)
		return
	}
	s.render(w, http.StatusOK, tpl, struct {
		Chrome uiChrome
		D      uiRunDetail
	}{uiChrome{Nav: "runs", Title: "daedalus — " + d.WorkflowID}, d})
}

// uiQueueSessions is one queue's live session gauges — the counts the
// operator reads as "how much is this queue doing right now".
type uiQueueSessions struct {
	Queue   string
	Running int
	Waiting int
	Total   int
}

// pageMetrics renders the frozen metrics view: the daemon's own
// aggregates — the relay ledger's counters and the live session counts —
// plus the note that the GoLens embed is not part of this build, so the
// mount has no charts to serve where those would render. The counters
// come from the same folds the Models tab reads (foldRelayTotals), so
// the two views can never disagree.
func (s *uiServer) pageMetrics(w http.ResponseWriter, req *http.Request) {
	snap := s.snapshot()
	now := snap.Generated
	sessions := make([]uiQueueSessions, 0, len(snap.Queues))
	for _, q := range snap.Queues {
		qs := uiQueueSessions{Queue: q}
		for _, r := range snap.Rows {
			if r.Queue != q {
				continue
			}
			qs.Total++
			switch r.Status {
			case "running":
				qs.Running++
			case "waiting", "pending":
				qs.Waiting++
			}
		}
		sessions = append(sessions, qs)
	}
	s.render(w, http.StatusOK, "page_metrics", struct {
		Chrome   uiChrome
		Relay    relayTotals
		Sessions []uiQueueSessions
		WalkErr  string
	}{uiChrome{Nav: "metrics", Title: "daedalus — metrics"}, foldRelayTotals(s.ledgerEntries(), now), sessions, snap.WalkErr})
}

// uiWorkerOption is one New-Task worker choice.
type uiWorkerOption struct {
	Name      string
	State     string
	Selected  bool
	Available bool // a config record exists to submit through
}

// uiAgentSelect is the agent dropdown (and its degraded face when the
// target worker has no loadable config).
type uiAgentSelect struct {
	Options  []string
	Selected string
	Note     string
}

// uiFormData backs the New-Task page and the preserved-state error panel.
type uiFormData struct {
	Workers  []uiWorkerOption
	Flows    []string
	Worker   string
	Workflow string
	Repo     string
	Issue    string
	Mode     string
	Prompt   string
	File     string
	Agent    string
	Prefix   string
}

func (s *uiServer) workerOptions(selected string) []uiWorkerOption {
	rows := collectWorkerRows()
	opts := make([]uiWorkerOption, 0, len(rows))
	for _, r := range rows {
		state := "idle"
		if r.Running {
			state = "active"
		}
		opts = append(opts, uiWorkerOption{
			Name:      r.Name,
			State:     state,
			Selected:  r.Name == selected,
			Available: recordedConfigPath(r.Name) != "",
		})
	}
	return opts
}

// formFlows lists the registry's names for the workflow dropdown — the
// same map the CLI's -w validates against, never a hardcoded list.
func formFlows() []string {
	names := make([]string, 0, len(runstart.Registry))
	for name := range runstart.Registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *uiServer) pageNew(w http.ResponseWriter, req *http.Request) {
	sel := req.URL.Query().Get("worker")
	data := struct {
		Chrome uiChrome
		Form   uiFormData
		Agents uiAgentSelect
	}{
		Chrome: uiChrome{Nav: "new", Title: "daedalus — new task"},
		Form: uiFormData{
			Workers: s.workerOptions(sel),
			Flows:   formFlows(),
			Worker:  sel,
			// Workflow stays empty: the dropdown's "default" option is
			// selected, and submitNew resolves it against the target
			// worker's config (the slim reroute included).
		},
		Agents: s.agentSelect(sel),
	}
	s.render(w, http.StatusOK, "page_new", data)
}

// agentSelect derives the agent dropdown from the target worker's resolved
// config (Q5): the options are the agents the config loader admits, the
// default is that config's agent — never a hardcoded list.
func (s *uiServer) agentSelect(worker string) uiAgentSelect {
	conf := recordedConfigPath(worker)
	if conf == "" {
		return uiAgentSelect{Note: "pick a worker with a config record — start it once with `daedalus worker start`"}
	}
	cfg, err := config.Load(conf)
	if err != nil {
		return uiAgentSelect{Note: "worker config did not load: " + err.Error()}
	}
	return uiAgentSelect{Options: config.Agents(), Selected: cfg.Agent}
}

// fragAgents is the agent dropdown htmx re-derives when the queue changes.
func (s *uiServer) fragAgents(w http.ResponseWriter, req *http.Request) {
	s.render(w, http.StatusOK, "frag_agents", s.agentSelect(req.URL.Query().Get("worker")))
}

// uiSubmitResult is the POST response panel: the confirmation (with the
// follow-up actions gemini's sample contributed) or the typed error.
type uiSubmitResult struct {
	OK       bool
	Workflow string
	Queue    string
	Warning  string
	Error    string
}

// submitNew runs the form through runstart — the same gates `daedalus
// run` fires, minus the interactive preflights a form has no terminal for
// (a dirty repo is refused by Start's gate, not asked about), including
// the config's slim default-flow reroute: an empty workflow selection is
// the dropdown's "default" option, resolved the way main.go resolves an
// unset -w. Errors land in the panel typed and red; the form's fields are
// untouched because only the panel swaps.
func (s *uiServer) submitNew(w http.ResponseWriter, req *http.Request) {
	if err := req.ParseForm(); err != nil {
		s.render(w, http.StatusOK, "frag_new_result", uiSubmitResult{Error: "read form: " + err.Error()})
		return
	}
	f := uiFormData{
		Worker:   strings.TrimSpace(req.FormValue("worker")),
		Workflow: strings.TrimSpace(req.FormValue("workflow")),
		Repo:     strings.TrimSpace(req.FormValue("repo")),
		Issue:    strings.TrimSpace(req.FormValue("issue")),
		Mode:     req.FormValue("mode"),
		Prompt:   req.FormValue("prompt"),
		File:     strings.TrimSpace(req.FormValue("file")),
		Agent:    strings.TrimSpace(req.FormValue("agent")),
		Prefix:   strings.TrimSpace(req.FormValue("prefix")),
	}
	if f.Mode != "file" {
		f.Mode = "text"
	}
	fail := func(msg string) {
		s.render(w, http.StatusOK, "frag_new_result", uiSubmitResult{Error: msg})
	}

	// The target worker's recorded config is the submission's config: its
	// queue and Temporal host are where the run lands (Q5's "resolved
	// config"), and its agent default is what an empty agent would mean.
	if f.Worker == "" {
		fail("choose a worker to submit to.")
		return
	}
	conf := recordedConfigPath(f.Worker)
	if conf == "" {
		fail("worker " + f.Worker + " has no config record — start it once with `daedalus worker start`, then submit.")
		return
	}
	cfg, err := config.Load(conf)
	if err != nil {
		fail("worker " + f.Worker + "'s config did not load: " + err.Error())
		return
	}
	if f.Repo == "" {
		fail("repo path is required.")
		return
	}
	if f.Issue == "" {
		fail("issue id is required.")
		return
	}
	prompt := ""
	switch f.Mode {
	case "file":
		if f.File == "" {
			fail("file path is required when the mode is File Path.")
			return
		}
		data, err := os.ReadFile(expandHome(f.File))
		if err != nil {
			fail("read task file: " + err.Error())
			return
		}
		prompt = string(data)
	default:
		if strings.TrimSpace(f.Prompt) == "" {
			fail("provide the task instructions (direct text or a file path).")
			return
		}
		prompt = f.Prompt
	}
	if f.Agent != "" {
		if err := config.ValidateAgent(f.Agent); err != nil {
			fail(err.Error())
			return
		}
	}
	// The flow: an empty selection is the dropdown's "default", resolved
	// exactly as main.go resolves an unset -w — the registry's default
	// flow, rerouted to slim when this config enables it. One gate, shared
	// with the CLI, so the form cannot drift from the CLI's default-flow
	// choice.
	workflow := f.Workflow
	if workflow == "" {
		workflow = defaultWorkflowName
		if cfg.Slim.Enabled {
			workflow = "slim"
		}
	}
	// The slim flow's input gate — shrink and cap before any side effect,
	// exactly as the run path applies it.
	if workflow == "slim" {
		if prompt, err = slimInputGate(prompt, cfg); err != nil {
			fail(err.Error())
			return
		}
	}
	var warn bytes.Buffer
	run, err := runstart.Start(cfg, runstart.Request{
		WorkflowName: workflow,
		RepoPath:     expandHome(f.Repo),
		IssueID:      f.Issue,
		Prompt:       prompt,
		BranchPrefix: resolveBranchPrefix(f.Prefix, cfg.BranchPrefix),
		Agent:        f.Agent,
		Warn:         &warn,
	})
	if err != nil {
		fail(err.Error())
		return
	}
	res := uiSubmitResult{
		OK:       true,
		Workflow: run.WorkflowID,
		Queue:    cfg.Temporal.TaskQueue,
		Warning:  strings.TrimSpace(warn.String()),
	}
	s.render(w, http.StatusOK, "frag_new_result", res)
}

// expandHome resolves a leading ~ for the two paths a form submit takes
// from the browser verbatim (the shell does this for the CLI).
func expandHome(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, strings.TrimPrefix(strings.TrimPrefix(p, "~"), "/"))
		}
	}
	return p
}
