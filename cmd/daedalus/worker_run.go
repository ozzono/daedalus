package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
	"github.com/ozzono/daedalus/internal/toolrelay"
	"github.com/ozzono/daedalus/internal/version"
	"github.com/ozzono/daedalus/internal/workflows"
)

// workerStopGrace is the daemon's graceful-drain window (SDK
// WorkerStopTimeout): how long in-flight activities may keep finishing
// after SIGTERM before the SDK cancels them — see the worker construction
// in runWorker. It bounds every restart at workerStop's own 30s CLI wait,
// inside which activities' child processes get the first
// activities.ShutdownDrainGrace and then fail cleanly.
const workerStopGrace = 30 * time.Second

// wantsPipelineWorker reports whether a daemon of the given run type starts
// the main-queue poller (jailed dev and reviewer rounds): every type except
// test-only. The empty type (unset) starts both pollers.
func wantsPipelineWorker(workerType string) bool { return workerType != workerTypeTest }

// wantsTestWorker reports whether a daemon of the given run type starts the
// shared test-queue poller (suite executions plus the repro-first gate):
// every type except dev-only.
func wantsTestWorker(workerType string) bool { return workerType != workerTypeDev }

// runWorker registers and runs the pollers the daemon's run type selects
// (workerType: dev, test, or empty for both) and blocks running them. Any
// prompt overrides were resolved and validated by the caller (the one
// `worker foreground` dispatch, which knows the config file's directory)
// before this runs, so a bad override fails the start before any poller
// registers.
func runWorker(cfg config.Config, workerType string) error {
	// The self-managed interrupt channel the Runs and the signal
	// forwarders share, declared before anything that fires halt:
	// whichever Run exits first halts the others, so an asymmetric failure
	// (say, the test worker dying alone on an untyped daemon) stops the
	// survivors too instead of leaving a half-dead daemon with a live pid
	// file and an error nobody reports. Each Run only returns after its
	// in-flight activity outcomes have been reported, and returning on
	// whichever finishes first without halting the others would exit
	// mid-drain — the orphaned-round wedge this shutdown path exists to
	// prevent.
	interrupt := make(chan interface{})
	var haltOnce sync.Once
	halt := func() { haltOnce.Do(func() { close(interrupt) }) }
	// The daemon's drain-restart lever (`worker restart` on a live
	// daemon): SIGHUP sets the restart intent and fires the same halt, so
	// every Run drains its in-flight activities first — the join at the
	// bottom returns only when the worker is idle by construction — and
	// the intent then re-execs the daemon on its config record instead of
	// returning. Armed before the client dial so the whole multi-second
	// boot is covered, and gated on the daemon marker, so a foreground run
	// keeps SIGHUP's default disposition (terminate) — a closed terminal
	// still takes the process down. ponytail: two windows remain where a
	// SIGHUP still kills at default disposition or is dropped — this
	// dispatch's config load before runWorker is entered (covering it
	// means arming in the dispatch and threading the intent down, not
	// worth the plumbing), and after the restart-intent check below but
	// before the process exits (the scheduling CLI has already reported
	// the restart; inherent to check-and-exit, and closing it means
	// blocking the exit on a grace period).
	var restartPending atomic.Bool
	if os.Getenv(daemonEnv) == "1" {
		sighup := make(chan os.Signal, 1)
		signal.Notify(sighup, syscall.SIGHUP)
		go func() {
			for range sighup {
				restartPending.Store(true)
				halt()
			}
		}()
	}
	// Export provider settings into the worker's environment — but only the
	// ones actually set in the config. A missing key is not an error: the
	// jailed agent may authenticate through the worker's inherited
	// environment or its own login instead. Values never travel through
	// workflow history, activity inputs, or argv.
	for _, kv := range cfg.AgentEnv() {
		if key, value, ok := strings.Cut(kv, "="); ok {
			if err := os.Setenv(key, value); err != nil {
				return fmt.Errorf("set %s: %w", key, err)
			}
		}
	}
	// The reviewer section's model override rides the same channel as the
	// endpoint override above, but as literal Setenv sites here rather
	// than an AgentEnv key family: AgentEnv's key-site scan is a pinned
	// exact count, so a new family would fail it. Set only when configured
	// — absent means reviewers request the serving section's model — and
	// unset symmetrically (the thinking toggles' shape below), so a stale
	// shell export cannot retarget a config that says nothing even on the
	// foreground path, where no daemon spawn scrub
	// (config.ProviderEnvVars) runs.
	if m := cfg.Reviewer.Model; m != "" {
		if err := os.Setenv("DAEDALUS_REVIEWER_MODEL", m); err != nil {
			return fmt.Errorf("set DAEDALUS_REVIEWER_MODEL: %w", err)
		}
	} else if err := os.Unsetenv("DAEDALUS_REVIEWER_MODEL"); err != nil {
		return fmt.Errorf("unset DAEDALUS_REVIEWER_MODEL: %w", err)
	}
	// The jailed agent selection travels the same channel: activities read
	// DAEDALUS_AGENT when building the ai-jail command line.
	if err := os.Setenv("DAEDALUS_AGENT", cfg.Agent); err != nil {
		return fmt.Errorf("set DAEDALUS_AGENT: %w", err)
	}
	// So does the concurrency cap: activities read
	// DAEDALUS_MAX_CONCURRENT_AGENT_RUNS when sizing the semaphore that
	// bounds concurrent jailed-agent rounds on this worker.
	if err := os.Setenv("DAEDALUS_MAX_CONCURRENT_AGENT_RUNS", strconv.Itoa(cfg.MaxConcurrentAgentRuns)); err != nil {
		return fmt.Errorf("set DAEDALUS_MAX_CONCURRENT_AGENT_RUNS: %w", err)
	}
	// Likewise the test-suite cap for the shared test queue's semaphore.
	if err := os.Setenv("DAEDALUS_MAX_CONCURRENT_TESTS", strconv.Itoa(cfg.MaxConcurrentTests)); err != nil {
		return fmt.Errorf("set DAEDALUS_MAX_CONCURRENT_TESTS: %w", err)
	}
	// Slim mode travels the same env channel: activities read DAEDALUS_SLIM
	// when building a jailed aider round's environment (weak/editor model
	// pinning). Like the concurrency var it is set here from the recorded
	// config; the unset branch matters as much as the set one — both vars
	// are in the daemon spawn scrub (config.AllWorkerEnvVars), so a stale
	// export in the invoking shell is dropped at spawn and the re-export
	// here (or the unset below) is the only channel a value returns on:
	// the daemon's own behavior stays config-driven. Foreground runs keep
	// the inherit semantics by design.
	if cfg.Slim.Enabled {
		if err := os.Setenv("DAEDALUS_SLIM", "1"); err != nil {
			return fmt.Errorf("set DAEDALUS_SLIM: %w", err)
		}
	} else if err := os.Unsetenv("DAEDALUS_SLIM"); err != nil {
		return fmt.Errorf("unset DAEDALUS_SLIM: %w", err)
	}
	// The metrics relay (config metrics.enabled): the worker's rounds stage
	// their provider calls onto the host metrics daemon's relay
	// (internal/metricsrelay), which keeps the usage ledger the `daedalus
	// metrics` surface reads. Two things happen here: the relay roots are
	// exported on the env channel (per-round staging in activities reads
	// them; symmetric unset plus the daemon-spawn scrub
	// (config.AllWorkerEnvVars) keep a stale export from staging a
	// metrics-disabled worker), and this worker's
	// upstream mapping — base URLs and real API keys per wire, the same
	// values AgentEnv exported above — is registered with the daemon and
	// refreshed on a heartbeat well inside the registration TTL. workerStart
	// gated on the relay already; the probe here covers a bare `worker
	// foreground` too, so a metrics-enabled start fails the same way on
	// either path.
	var metricsRoot, metricsToken string
	if cfg.MetricsEnabled() {
		if err := metricsGateCheck(cfg); err != nil {
			return err
		}
		metricsRoot = fmt.Sprintf("http://127.0.0.1:%d", cfg.Metrics.RelayPort)
		// One mint per process: boot and every heartbeat present the same
		// secret, so a heartbeat refreshes the live registration instead of
		// colliding with it — a fresh mint per call would be refused, the
		// registration would lapse at the TTL, and the post-lapse re-mint
		// would rotate the secret out from under rounds staged before it.
		metricsToken = metricsrelay.NewToken()
		if metricsToken == "" {
			return fmt.Errorf("mint %s: crypto/rand failed", config.MetricsRelayTokenEnv)
		}
	}
	if metricsRoot != "" {
		if err := os.Setenv(config.MetricsRelayAnthropicEnv, metricsRoot); err != nil {
			return fmt.Errorf("set %s: %w", config.MetricsRelayAnthropicEnv, err)
		}
		if err := os.Setenv(config.MetricsRelayOpenAIEnv, metricsRoot); err != nil {
			return fmt.Errorf("set %s: %w", config.MetricsRelayOpenAIEnv, err)
		}
		// The OTLP endpoint rides its own var, staged whenever metrics is
		// on — one the override paths never strip: an otel-staged round
		// dials the daemon directly and needs no relay URL, so fallback
		// and override rounds keep telemetry coverage (see the var's
		// config comment). Per-round selection — a capable claude/codex
		// takes the agent-native path, everything else the relay — reads
		// this var's presence.
		if err := os.Setenv(config.MetricsOtelEndpointEnv, metricsRoot+"/v1/metrics"); err != nil {
			return fmt.Errorf("set %s: %w", config.MetricsOtelEndpointEnv, err)
		}
		// Boot retries a name conflict instead of exiting: a restart inside
		// the registration TTL hits the relay's refusal while the dead
		// process's entry lingers, and the retry budget outlives that window
		// (metricsRegisterBootRetry). A failed boot registration is otherwise
		// fatal here, the wedge the restart-TTL incident rode.
		if err := metricsRegisterBootRetry(cfg, metricsRoot, metricsToken); err != nil {
			return fmt.Errorf("register with the metrics relay: %w", err)
		}
		stopReg := make(chan struct{})
		defer close(stopReg)
		go func() {
			ticker := time.NewTicker(metricsHeartbeatInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stopReg:
					return
				case <-ticker.C:
					// A failed refresh only ages the registration toward its
					// TTL; the next tick retries, so log and continue.
					if err := metricsRegister(cfg, metricsRoot, metricsToken); err != nil {
						fmt.Printf("metrics relay: registration refresh failed: %v\n", err)
					}
				}
			}
		}()
	} else {
		for _, name := range []string{config.MetricsRelayAnthropicEnv, config.MetricsRelayOpenAIEnv, config.MetricsRelayTokenEnv, config.MetricsOtelEndpointEnv} {
			if err := os.Unsetenv(name); err != nil {
				return fmt.Errorf("unset %s: %w", name, err)
			}
		}
	}
	// The worker's provider relay: a loopback reverse proxy between jailed
	// pi rounds and the openai channel's upstream (internal/toolrelay). It
	// starts for either of two jobs — never both required at once: lifting
	// pi's text-encoded tool calls into the native tool_calls wire (slim
	// enabled with a parser model), or pacing the upstream's completions
	// traffic to max_requests_per_minute (a request-counted API tier; the
	// window is shared by agent and reviewer rounds alike, the accounting
	// the tier actually enforces). Both gate on an openai-channel upstream
	// — the provider section's url when its type is openai, else the
	// legacy openai section's — and absent every trigger the relay never
	// starts and the env var below stays unset, byte-identical to the
	// pre-relay behavior. The staged URL (relay address plus the upstream's
	// path prefix) travels the env channel like every other knob; unset
	// symmetrically, and scrubbed from the daemon spawn via
	// config.AllWorkerEnvVars, so a stale export can never point a new worker's
	// rounds at a dead relay.
	var relayURL string
	_, open := cfg.ProviderChannels()
	relayParser := cfg.Slim.Enabled && cfg.Slim.ParserModel != ""
	if open.URL != "" && (relayParser || cfg.MaxRequestsPerMinute > 0) {
		// With metrics on, the relay forwards into the metrics relay's
		// staged openai path — pi → relay → metrics relay → upstream —
		// so the round's calls land in the ledger attributed to the
		// round: the synthetic attribution key rides through both relays
		// untouched.
		upstream := open.URL
		if metricsRoot != "" {
			if staged := metricsrelay.StagedBase(metricsRoot, cfg.WorkerName(), metricsrelay.WireOpenAI, open.URL); staged != "" {
				upstream = staged
			}
		}
		r, err := toolrelay.Start(upstream, cfg.Slim.ParserModel, cfg.MaxRequestsPerMinute)
		if err != nil {
			return fmt.Errorf("start tool relay: %w", err)
		}
		defer r.Close()
		relayURL = r.URL()
	}
	if relayURL != "" {
		if err := os.Setenv(config.ToolRelayURLEnv, relayURL); err != nil {
			return fmt.Errorf("set %s: %w", config.ToolRelayURLEnv, err)
		}
	} else if err := os.Unsetenv(config.ToolRelayURLEnv); err != nil {
		return fmt.Errorf("unset %s: %w", config.ToolRelayURLEnv, err)
	}
	// The thinking toggle: only an explicit thinking: false is exported
	// (as DAEDALUS_THINKING=off, the agent-lever thinking signal —
	// translated per agent to pi/aider/claude's own off lever); unset or
	// true means no agent gets any thinking signal and each follows its
	// own default. The wire-level per-role signals below
	// (DAEDALUS_OPENAI_THINKING / DAEDALUS_REVIEWER_THINKING, from the
	// openai and reviewer sections' disable_thinking) are the staged-onto-
	// the-request counterpart. Unset symmetrically, for the same
	// stale-export reason as DAEDALUS_SLIM above.
	if cfg.ThinkingDisabled() {
		if err := os.Setenv("DAEDALUS_THINKING", "off"); err != nil {
			return fmt.Errorf("set DAEDALUS_THINKING: %w", err)
		}
	} else if err := os.Unsetenv("DAEDALUS_THINKING"); err != nil {
		return fmt.Errorf("unset DAEDALUS_THINKING: %w", err)
	}
	// The wire-level thinking toggles, one per role (literal Setenv sites
	// for the same pinned-scan reason as DAEDALUS_REVIEWER_MODEL above):
	// only an explicit disable_thinking: true is exported, as "off" —
	// unset means that role's rounds send no thinking signal and the
	// backend's own default stands. The openai-side value reads the
	// projected openai channel (ProviderChannels), not cfg.OpenAI, so a
	// provider-typed section's knob governs. The jailed-round builder
	// resolves the per-role value into the round env
	// (DAEDALUS_WIRE_THINKING), the var the pi and aider stagings read.
	// Unset symmetrically like DAEDALUS_THINKING above.
	if open.DisableThinking {
		if err := os.Setenv("DAEDALUS_OPENAI_THINKING", "off"); err != nil {
			return fmt.Errorf("set DAEDALUS_OPENAI_THINKING: %w", err)
		}
	} else if err := os.Unsetenv("DAEDALUS_OPENAI_THINKING"); err != nil {
		return fmt.Errorf("unset DAEDALUS_OPENAI_THINKING: %w", err)
	}
	if cfg.Reviewer.DisableThinking {
		if err := os.Setenv("DAEDALUS_REVIEWER_THINKING", "off"); err != nil {
			return fmt.Errorf("set DAEDALUS_REVIEWER_THINKING: %w", err)
		}
	} else if err := os.Unsetenv("DAEDALUS_REVIEWER_THINKING"); err != nil {
		return fmt.Errorf("unset DAEDALUS_REVIEWER_THINKING: %w", err)
	}
	// The per-role provider overrides (the dev and final_reviewer sections,
	// plus the reviewer section's context window) ride the same env-only
	// channel, one loop for the whole family — a literal site per var would
	// only restate config.RoleOverrideEnvVars. Every var is exported only
	// when its field is set (the unset case is as load-bearing as the set
	// one: absent means that role's rounds resolve down the fallback chain
	// — final reviewer → reviewer → serving — exactly as before), and unset
	// symmetrically with the rest of the scrub set, so a stale shell export
	// can never retarget a config that says nothing. The jailed-round
	// builder resolves per round (activities.reviewerEnv / devEnv /
	// wireThinkingEnv).
	exported := make(map[string]bool)
	for _, kv := range cfg.RoleOverrideEnv() {
		key, value, ok := strings.Cut(kv, "=")
		if !ok {
			return fmt.Errorf("role override export %q: malformed key=value", kv)
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("set %s: %w", key, err)
		}
		exported[key] = true
	}
	for _, name := range config.RoleOverrideEnvVars() {
		if exported[name] {
			continue
		}
		if err := os.Unsetenv(name); err != nil {
			return fmt.Errorf("unset %s: %w", name, err)
		}
	}
	// The streaming toggle: exported (as DAEDALUS_STREAM=on/off) only when
	// explicitly set — absent means every agent follows its own streaming
	// default. The jailed-round builder translates the value per agent
	// (aider --stream/--no-stream). Unset symmetrically, for the same
	// stale-export reason as DAEDALUS_THINKING above.
	if v, ok := cfg.StreamSetting(); ok {
		if err := os.Setenv("DAEDALUS_STREAM", v); err != nil {
			return fmt.Errorf("set DAEDALUS_STREAM: %w", err)
		}
	} else if err := os.Unsetenv("DAEDALUS_STREAM"); err != nil {
		return fmt.Errorf("unset DAEDALUS_STREAM: %w", err)
	}
	// The out-of-scope-bug filing toggle: DAEDALUS_BUG_DIR (the effective
	// bug_filing.dir, default applied) is exported only when filing is
	// enabled — absent means off, and the absence is load-bearing, like
	// DAEDALUS_THINKING's only-off export above. Unset symmetrically (and
	// scrubbed from the daemon spawn via config.AllWorkerEnvVars) so a stale export
	// in the invoking shell can never re-enable filing for a config that
	// says off. The prompt-render sites read it in the worker process. Like
	// DAEDALUS_SLIM and DAEDALUS_WORKER_NAME it passes through to jailed
	// rounds wholesale via os.Environ() — benign on exactly those rounds,
	// since it is exported only when the round's prompt already instructs
	// filing into that folder; the off case is the one that must stay clean,
	// and the symmetric unset plus the scrub above keep it that way.
	if dir := cfg.BugFilingDir(); dir != "" {
		if err := os.Setenv(config.BugDirEnv, dir); err != nil {
			return fmt.Errorf("set %s: %w", config.BugDirEnv, err)
		}
	} else if err := os.Unsetenv(config.BugDirEnv); err != nil {
		return fmt.Errorf("unset %s: %w", config.BugDirEnv, err)
	}
	// The host mirror dirs (config bug_filing.mirror / test_output.mirror,
	// resolved through config.ResolveMirror): each section's worker copies
	// its filed files there after the round that wrote them. Exported only
	// when the section is enabled and a mirror configured — absent means
	// off, the same load-bearing absence as DAEDALUS_BUG_DIR, with the
	// same symmetric unset and daemon-spawn scrub (config.AllWorkerEnvVars). They
	// travel worker env, not pipeline input, because a mirror is a host
	// path: a shared test queue may run a suite on a foreign deployment's
	// worker, and each deployment mirrors onto its own host.
	bugMirror := ""
	if dir := cfg.BugFilingDir(); dir != "" && cfg.BugFiling.Mirror != "" {
		m, err := config.ResolveMirror("bug_filing mirror", cfg.BugFiling.Mirror)
		if err != nil {
			return fmt.Errorf("resolve bug_filing.mirror: %w", err)
		}
		bugMirror = m
	}
	if bugMirror != "" {
		if err := os.Setenv(config.BugMirrorEnv, bugMirror); err != nil {
			return fmt.Errorf("set %s: %w", config.BugMirrorEnv, err)
		}
	} else if err := os.Unsetenv(config.BugMirrorEnv); err != nil {
		return fmt.Errorf("unset %s: %w", config.BugMirrorEnv, err)
	}
	testMirror := ""
	if dir := cfg.TestOutputDir(); dir != "" && cfg.TestOutput.Mirror != "" {
		m, err := config.ResolveMirror("test_output mirror", cfg.TestOutput.Mirror)
		if err != nil {
			return fmt.Errorf("resolve test_output.mirror: %w", err)
		}
		testMirror = m
	}
	if testMirror != "" {
		if err := os.Setenv(config.TestOutputMirrorEnv, testMirror); err != nil {
			return fmt.Errorf("set %s: %w", config.TestOutputMirrorEnv, err)
		}
	} else if err := os.Unsetenv(config.TestOutputMirrorEnv); err != nil {
		return fmt.Errorf("unset %s: %w", config.TestOutputMirrorEnv, err)
	}
	// The worker's name is stamped into every round's captured Usage (so
	// `daedalus worker report` aggregates per worker) and keys the status
	// file published below.
	if err := os.Setenv("DAEDALUS_WORKER_NAME", cfg.WorkerName()); err != nil {
		return fmt.Errorf("set DAEDALUS_WORKER_NAME: %w", err)
	}

	if err := activities.PreflightWorktreeRoot(cfg.Temporal.TaskQueue); err != nil {
		return fmt.Errorf("worktree preflight: %w", err)
	}

	c, err := newClient(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	// Each poller gets the same graceful-drain window so a SIGTERM (worker
	// stop or restart) waits for in-flight activities to report their
	// outcome before the SDK cancels anything. Activities give their child
	// processes the first activities.ShutdownDrainGrace of the window to
	// end on their own and then fail cleanly, so the drain always fits —
	// and it matches workerStop's 30s CLI wait, which must not outlive it.
	// Without this bound the SDK's default (0s) makes Stop return without
	// waiting at all: the process exits mid-round, the jailed agent dies
	// unreported, and Temporal holds a ghost attempt no worker will ever
	// serve — the wedge that hung daedalus-issue-status-all.
	opts := worker.Options{WorkerStopTimeout: workerStopGrace}
	var workers []worker.Worker
	if wantsPipelineWorker(workerType) {
		w := worker.New(c, cfg.Temporal.TaskQueue, opts)
		// Each flow registers under its unwrapped function's own type name
		// (WorkflowTypeName), wrapped so a parked run completes green
		// (workflows.CompleteGreen) — the name keeps every past run's
		// history and every start (by the same name) in agreement.
		for _, spec := range workflowRegistry {
			w.RegisterWorkflowWithOptions(workflows.CompleteGreen(spec.Fn),
				workflow.RegisterOptions{Name: workflows.WorkflowTypeName(spec.Fn)})
		}
		w.RegisterActivity(activities.CreateWorktreeActivity)
		w.RegisterActivity(activities.RunJailedClaudeActivity)
		w.RegisterActivity(activities.RunJailedReviewerActivity)
		w.RegisterActivity(activities.RunNativeTestsActivity)
		w.RegisterActivity(activities.ResolveTestCommandActivity)
		w.RegisterActivity(activities.FinalizeWorktreeActivity)
		w.RegisterActivity(activities.CleanupWorktreeActivity)
		w.RegisterActivity(activities.VerifyWriteScopeActivity)
		w.RegisterActivity(activities.ReproFirstGateActivity)
		// The dependency probe is a closure over this daemon's client (c is
		// dialed above, before registration) — no second dial, no env
		// channel. Its name is pinned as a string on both ends. The
		// dependent-release poke shares the closure shape and the client.
		w.RegisterActivityWithOptions(activities.NewCheckDependencyActivity(c),
			activity.RegisterOptions{Name: activities.CheckDependencyActivityName})
		w.RegisterActivityWithOptions(activities.NewNotifyDependentsActivity(c),
			activity.RegisterOptions{Name: activities.NotifyDependentsActivityName})
		workers = append(workers, w)
	}

	// The test worker serves this deployment's suite queue: it runs only
	// the suite activities — dumb command runners needing no provider
	// environment — so this deployment's suites share one queue and one
	// max_concurrent_tests budget, whatever workers created them. The
	// repro-first gate belongs here too: it runs the suite command in a
	// throwaway base checkout. With shared_test_queue off, the queue is
	// this deployment's own derived "<queue>-test": its suites are then
	// schedulable only by workers started from this config, so restarting
	// this deployment's daemon is sufficient to change which binary
	// executes them (a stale worker of any other deployment can no longer
	// pick them up). With sharing on — the default — it is the Temporal-
	// wide "test" queue, preserving fleet-wide suite borrowing.
	if wantsTestWorker(workerType) {
		suiteQueue := config.ReservedTestTaskQueue
		if !cfg.SharesTestQueue() {
			suiteQueue = config.TestQueueFor(cfg.Temporal.TaskQueue)
		}
		tw := worker.New(c, suiteQueue, opts)
		tw.RegisterActivity(activities.RunTestSuiteActivity)
		tw.RegisterActivity(activities.ReproFirstGateActivity)
		workers = append(workers, tw)
	}

	// Publish slot occupancy next to the pid file for `daedalus worker report`.
	stopPublish := make(chan struct{})
	defer close(stopPublish)
	go publishWorkerStatus(cfg.WorkerName(), cfg.Temporal.TaskQueue, stopPublish)

	// The worker log is opened in append mode by workerStart, so this start
	// record — timestamped, versioned — separates restarts in one file and
	// says which build served each stretch. Without the version, a worker
	// running an unidentified local build is indistinguishable from the
	// checked-out code it should match.
	fmt.Printf("daedalus worker %s starting %s on task queue %q (temporal %s, UI %s)\n",
		version.String(), time.Now().Format(time.RFC3339),
		cfg.Temporal.TaskQueue, cfg.Temporal.Host, cfg.UIURL())
	// SIGINT/SIGTERM still reach the workers: the forwarder drains the
	// signal channel and halts the shared channel declared (with the
	// SIGHUP lever) at the top of runWorker — its close every Run
	// receives — no send, so a signal racing halt's close cannot panic the
	// forwarder on a closed channel.
	go func() { <-worker.InterruptCh(); halt() }()
	errs := make(chan error, len(workers))
	for _, wk := range workers {
		go func(w worker.Worker) { err := w.Run(interrupt); halt(); errs <- err }(wk)
	}
	joined := make([]error, 0, len(workers))
	for range workers {
		joined = append(joined, <-errs)
	}
	// Restart intent wins over returning: every poller has joined, every
	// in-flight activity outcome is reported — the worker is idle, and the
	// re-exec brings it back on the (possibly just re-recorded) config,
	// same pid, same log. Only an Exec failure adds to the joined errors.
	if restartPending.Load() {
		if err := restartSelf(cfg.WorkerName()); err != nil {
			return errors.Join(append(joined, err)...)
		}
	}
	return errors.Join(joined...)
}

// restartSelf re-executes this binary as a fresh daemon from the worker's
// config record (restartArgv) — the second half of the drain restart,
// reached only after every poller has joined. Same pid (exec replaces the
// image in place), so the pid file stays valid; stdin/stdout/stderr still
// point at the daemon log, so the new start's banner continues the same
// file. The environment is scrubbed of every worker-exported variable
// (envWithoutWorkerVars, the spawnDaemon shape) with the daemon marker
// re-appended: this process's own environ carries the previous config's
// re-exports, and a plain passthrough would let any value the fresh
// config stops exporting — a rotated-out key, a retracted fallback —
// survive the exec and keep serving rounds from stale memory, exactly
// what the spawn-time scrub exists to prevent. The defers above (client
// close, relay and status goroutines) do not run — exec never returns on
// success — and need not: the drains they guard have completed, and the
// kernel closes what is left.
func restartSelf(name string) error {
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}
	argv := restartArgv(self, name)
	fmt.Printf("drain restart: re-executing %q\n", strings.Join(argv[1:], " "))
	return syscall.Exec(self, argv, append(envWithoutWorkerVars(os.Environ()), daemonEnv+"=1"))
}

// restartArgv builds the re-exec argv from the worker's config record —
// the config path and run type the scheduling `worker restart` just wrote,
// not this process's original invocation (config is re-read at startup, so
// both a CLI re-record and a hand edit take effect). A worker with no
// readable record — started before records existed — re-execs as what it
// is: its own invocation.
func restartArgv(self, name string) []string {
	path, typ := readConfigRecord(name)
	if path == "" {
		return append([]string{self}, os.Args[1:]...)
	}
	argv := []string{self, "worker", "foreground", "-c", path}
	if typ != "" {
		argv = append(argv, "-t", typ)
	}
	return argv
}

// statusPublishInterval is how often the worker rewrites its status file.
const statusPublishInterval = 5 * time.Second

// workerStatus is the JSON document published to worker-<name>.status: the
// worker's live slot state (activities.SlotStats) plus its pid, update
// time, and the version of the binary it is executing, so a reader
// (`daedalus worker report`, `daedalus worker status`) can spot a stale
// file left
// by a crashed worker and one running a build older than the CLI.
type workerStatus struct {
	PID           int       `json:"pid"`
	Updated       time.Time `json:"updated"`
	Version       string    `json:"version,omitempty"`
	TaskQueue     string    `json:"task_queue"`
	SlotsBusy     int       `json:"slots_busy"`
	SlotsTotal    int       `json:"slots_total"`
	RoundsWaiting int       `json:"rounds_waiting"`
}

// publishWorkerStatus rewrites the worker's status file every
// statusPublishInterval (and once up front) until stop closes. Best-effort:
// a failed write only ages the file toward its staleness bound, never
// blocks the daemon.
func publishWorkerStatus(name, taskQueue string, stop <-chan struct{}) {
	if err := os.MkdirAll(daemonDir, 0o755); err != nil {
		return
	}
	statusFile := filepath.Join(daemonDir, "worker-"+name+".status")
	write := func() {
		busy, total, waiting := activities.SlotStats()
		data, err := json.Marshal(workerStatus{
			PID:           os.Getpid(),
			Updated:       time.Now().UTC(),
			Version:       version.String(),
			TaskQueue:     taskQueue,
			SlotsBusy:     busy,
			SlotsTotal:    total,
			RoundsWaiting: waiting,
		})
		if err != nil {
			return
		}
		// Write-then-rename so a reader never sees a torn file.
		tmp := statusFile + ".tmp"
		if err := os.WriteFile(tmp, data, 0o644); err != nil {
			return
		}
		_ = os.Rename(tmp, statusFile)
	}
	write()
	ticker := time.NewTicker(statusPublishInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			write()
		}
	}
}
