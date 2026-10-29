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
		// A boot marker with a live pid and budget left outranks both: the
		// delayed boot is the one state an operator is actively staring at
		// (a restart issued inside the relay's registration TTL), and the
		// pid file alone misreports it — a spawned daemon as a bare
		// "running" with no pollers, a supervised one as a false "not
		// running". The booting pid serves the version lookup too, so the
		// row never says "booting (pid N)" and "n/a (not running)" at once.
		versionPid, versionLive := pid, live
		if bpid, left, booting := workerBootingPhase(name); booting {
			state = fmt.Sprintf("booting (pid %d, metrics relay registration retry, ~%s left)", bpid, left.Round(time.Second))
			versionPid, versionLive = bpid, true
		}
		conf := recordedConfigPath(name)
		shown := conf
		if conf == "" {
			shown = "(no config record)"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", name, state, workerVersion(name, versionPid, versionLive), providerStatus(conf), shown, workerCodePath(conf), logFile)
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

// workerBootingPhase reports a worker's boot-retry state from its boot
// marker (worker-<name>.booting): the pid and the remaining retry budget
// while the marker names a live process with budget left. ok is false when
// there is no marker, its process is gone, or its budget has lapsed — a
// crash's leftover marker expires itself (workerBootState), so a dead
// worker never shows as booting.
func workerBootingPhase(name string) (pid int, remaining time.Duration, ok bool) {
	data, err := os.ReadFile(bootMarkerPath(name))
	if err != nil {
		return 0, 0, false
	}
	var bs workerBootState
	if json.Unmarshal(data, &bs) != nil || bs.PID <= 0 || !pidAlive(bs.PID) {
		return 0, 0, false
	}
	remaining = time.Until(bs.Deadline)
	if remaining <= 0 {
		return 0, 0, false
	}
	return bs.PID, remaining, true
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
		prev, _, _, err := readPriorRun(c, info.GetExecution().GetWorkflowId())
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

// providerProbes is the structured outcome of probing one worker's
// provider wiring: what `daedalus worker status` renders as text and the
// dashboard renders as badges, without either parsing the other's output.
// NA non-empty means nothing was probed — the same reason text the CLI
// row shows; FbArmed means the fallback was probed after a missed main.
type providerProbes struct {
	Main    provider.Result
	Fb      provider.Result
	FbArmed bool
	NA      string
}

// probeProvider live-probes the provider a worker's recorded config
// names, so a row answers "will this worker's next round reach the API?"
// The main provider is probed — the provider: section's endpoint in the
// wire style its type names when set, the legacy anthropic: section, or
// the legacy openai: section when the anthropic side is unset (the served
// channel of an openai-only config) — and the probe's wire shape is the
// section's heartbeat format; unset, the non-token check matching the
// probed wire's style (count_tokens on the anthropic wire, model list on
// the openai wire). When the main probe is not ok and the config arms a
// fallback, the fallback is probed too — mirroring the worker's own
// failover choice, under the fallback's own heartbeat format. Values that
// cannot be determined (no record, unloadable config, provider not fully
// configured) come back as NA text rather than as failures.
func probeProvider(confPath string) providerProbes {
	if confPath == "" {
		return providerProbes{NA: "n/a (no config record)"}
	}
	cfg, err := config.Load(confPath)
	if err != nil {
		return providerProbes{NA: "config error"}
	}
	anth, open := cfg.ProviderChannels()
	url, key, model := anth.URL, anth.Key, heartbeatOrDefault(anth.HeartbeatModel, anth.Model)
	style := provider.StyleAnthropic
	// The heartbeat format is the provider section's alone (the legacy
	// sections carry none); empty resolves at the probe to the non-token
	// check matching the probed wire's style.
	var format string
	if cfg.Provider != nil {
		format = cfg.Provider.Heartbeat
	}
	if cfg.Provider != nil && cfg.Provider.Type == config.FallbackTypeOpenAI {
		// The openai-typed provider is the primary: probe its endpoint in
		// its own wire style (heartbeat_model cannot be set on it — load
		// rejects the combination — so the probe names Model).
		url, key, model, style = open.URL, open.Key, open.Model, provider.StyleOpenAI
	} else if url == "" || key == "" {
		// No anthropic-wire endpoint to probe: a legacy-only config whose
		// openai section stands alone serves its rounds on the OPENAI_*
		// channel, so that channel is the primary and the thing to probe —
		// reporting n/a here would call a working worker unconfigured.
		url, key, model, style = open.URL, open.Key, open.Model, provider.StyleOpenAI
	}
	if url == "" || key == "" {
		return providerProbes{NA: "n/a (provider not configured)"}
	}
	p := providerProbes{Main: provider.Probe(context.Background(), provider.Spec{
		URL:    url,
		Key:    key,
		Model:  model,
		Style:  style,
		Format: format,
	})}
	if !cfg.Fallback.Active() || p.Main.OK {
		return p
	}
	// The fallback's type picks its probe's wire style, matching what
	// failover rounds speak, and its own heartbeat names the format;
	// anthropic is the default (also for a Type built without Load, which
	// cannot happen here but costs nothing).
	fbStyle := provider.StyleAnthropic
	if cfg.Fallback.Type == config.FallbackTypeOpenAI {
		fbStyle = provider.StyleOpenAI
	}
	p.Fb = provider.Probe(context.Background(), provider.Spec{
		URL:    cfg.Fallback.URL,
		Key:    cfg.Fallback.Key,
		Model:  heartbeatOrDefault(cfg.Fallback.HeartbeatModel, cfg.Fallback.Model),
		Style:  fbStyle,
		Format: cfg.Fallback.Heartbeat,
	})
	p.FbArmed = true
	return p
}

// providerStatus renders the probes as the roster row's text: the main
// provider's detail alone, or the failover arrow when a fallback was
// probed — mirroring the worker's own failover choice.
func providerStatus(confPath string) string {
	p := probeProvider(confPath)
	if p.NA != "" {
		return p.NA
	}
	if !p.FbArmed {
		return p.Main.Detail
	}
	active := "fallback"
	if !p.Fb.OK {
		active = "none"
	}
	return fmt.Sprintf("main: %s → fallback: %s [active: %s]", p.Main.Detail, p.Fb.Detail, active)
}

// heartbeatOrDefault picks the model a cheap request should name: the
// heartbeat (small/fast) model when configured, else the main model.
func heartbeatOrDefault(heartbeat, main string) string {
	if heartbeat != "" {
		return heartbeat
	}
	return main
}
