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

	"go.temporal.io/api/workflowservice/v1"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/provider"
)

// workerStatusAll lists every worker this deployment has on record — plus
// any live stray running without one — with its running pid, the config it
// was started with, the code path its task queue is currently working, and
// its log path. Record-driven like `restart all`, so it reports the whole
// roster, not just the worker the invoking directory happens to resolve to.
func workerStatusAll() error {
	names, err := recordedWorkers()
	if err != nil {
		return err
	}
	onRecord := make(map[string]bool, len(names))
	for _, name := range names {
		onRecord[name] = true
	}
	names = append(names, runningUnrecordedWorkers(onRecord)...)
	if len(names) == 0 {
		fmt.Printf("no workers on record in %s — start one first\n", daemonDir)
		return nil
	}
	sort.Strings(names)
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "WORKER\tSTATE\tVERSION\tAPI\tCONFIG\tCODE PATH\tLOG")
	for _, name := range names {
		pidFile, logFile, _ := daemonPaths(name)
		state := "not running"
		pid, live := readLivePid(pidFile)
		if live {
			state = fmt.Sprintf("running (pid %d)", pid)
		}
		conf := recordedConfigPath(name)
		shown := conf
		if conf == "" {
			shown = "(no config record)"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", name, state, workerVersion(name, pid, live), providerStatus(conf), shown, workerCodePath(conf), logFile)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	// The status pass is not purely observational: after the roster, check
	// every recorded queue for stuck activity attempts — work held by a
	// dead worker identity that Temporal will never redeliver — and bring
	// each back (restart or start a worker, fail the ghost attempt), with
	// a [DAEDALUS-ALERT] line for every action. Nothing stuck: this is a
	// no-op, so repeated status calls stay read-only.
	return recaptureStuckTasks()
}

// workerVersion reports the version of the binary a running worker is
// executing, from the version field of its published status file — the
// worker names its own build, so after a CLI upgrade a row still shows the
// older version actually running, next to what `daedalus version` prints.
// Trusted on pid match alone: a wedged worker's stale file still names the
// binary it started from, and a new worker's first publish overwrites any
// predecessor's. "(unknown)" when nothing readable confirms it —
// no file, a pre-version file from a worker started before this field
// existed, or a pid mismatch. Not running: nothing to report.
func workerVersion(name string, pid int, live bool) string {
	if !live {
		return "n/a (not running)"
	}
	data, err := os.ReadFile(filepath.Join(daemonDir, "worker-"+name+".status"))
	if err != nil {
		return "(unknown)"
	}
	var st workerStatus
	if json.Unmarshal(data, &st) != nil || st.PID != pid || st.Version == "" {
		return "(unknown)"
	}
	return st.Version
}

// workerCodePath reports the code a worker is reading to accomplish its
// current task: the repo path of every pipeline run currently executing on
// the worker's task queue, decoded from each run's history input (the same
// walk `continue` uses to recover a run's original input). "idle" when the
// queue has no running run. As in providerStatus, values that cannot be
// determined are reported as display text rather than failures, so one
// unreachable dependency never hides the rest of the roster.
func workerCodePath(confPath string) string {
	if confPath == "" {
		return "n/a (no config record)"
	}
	cfg, err := config.Load(confPath)
	if err != nil {
		return "config error"
	}
	c, err := newClient(cfg)
	if err != nil {
		return "temporal unreachable"
	}
	defer c.Close()
	resp, err := c.ListWorkflow(context.Background(), &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: "default",
		PageSize:  10,
		Query:     fmt.Sprintf("TaskQueue = '%s' AND ExecutionStatus = 'Running'", cfg.Temporal.TaskQueue),
	})
	if err != nil {
		return "temporal unreachable"
	}
	var paths []string
	seen := make(map[string]bool, len(resp.GetExecutions()))
	for _, info := range resp.GetExecutions() {
		prev, _, err := readPriorRun(c, info.GetExecution().GetWorkflowId())
		if err != nil || prev.RepoPath == "" || seen[prev.RepoPath] {
			continue
		}
		seen[prev.RepoPath] = true
		paths = append(paths, prev.RepoPath)
	}
	if len(paths) == 0 {
		return "idle"
	}
	return strings.Join(paths, ", ")
}

// providerStatus live-probes the provider a worker's recorded config
// names, so a row answers "will this worker's next round reach the API?"
// The main provider is probed; when it is not ok and the config arms a
// fallback, the fallback is probed too and reported as the active one —
// mirroring the worker's own failover choice. Values that cannot be
// determined (no record, unloadable config, provider not fully
// configured) are reported as such rather than as failures.
func providerStatus(confPath string) string {
	if confPath == "" {
		return "n/a (no config record)"
	}
	cfg, err := config.Load(confPath)
	if err != nil {
		return "config error"
	}
	if cfg.Anthropic.URL == "" || cfg.Anthropic.Key == "" {
		return "n/a (provider not configured)"
	}
	spec := provider.Spec{
		URL:   cfg.Anthropic.URL,
		Key:   cfg.Anthropic.Key,
		Model: heartbeatOrDefault(cfg.Anthropic.HeartbeatModel, cfg.Anthropic.Model),
		Style: provider.StyleAnthropic,
	}
	main := provider.Probe(context.Background(), spec)
	if !cfg.Fallback.Active() || main.OK {
		return main.Detail
	}
	// The fallback's type picks its probe's wire style, matching what
	// failover rounds speak; anthropic is the default (also for a Type
	// built without Load, which cannot happen here but costs nothing).
	style := provider.StyleAnthropic
	if cfg.Fallback.Type == config.FallbackTypeOpenAI {
		style = provider.StyleOpenAI
	}
	fb := provider.Probe(context.Background(), provider.Spec{
		URL:   cfg.Fallback.URL,
		Key:   cfg.Fallback.Key,
		Model: heartbeatOrDefault(cfg.Fallback.HeartbeatModel, cfg.Fallback.Model),
		Style: style,
	})
	active := "fallback"
	if !fb.OK {
		active = "none"
	}
	return fmt.Sprintf("main: %s → fallback: %s [active: %s]", main.Detail, fb.Detail, active)
}

// heartbeatOrDefault picks the model a cheap request should name: the
// heartbeat (small/fast) model when configured, else the main model.
func heartbeatOrDefault(heartbeat, main string) string {
	if heartbeat != "" {
		return heartbeat
	}
	return main
}
