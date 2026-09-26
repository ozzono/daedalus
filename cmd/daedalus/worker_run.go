package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/worker"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/version"
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
// (workerType: dev, test, or empty for both) and blocks running them.
func runWorker(cfg config.Config, workerType string) error {
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
	// config; the unset branch matters as much as the set one — neither var
	// is in the daemon spawn scrub, so a stale export in the invoking shell
	// would otherwise survive into the daemon and silently beat a config
	// that says off.
	if cfg.Slim {
		if err := os.Setenv("DAEDALUS_SLIM", "1"); err != nil {
			return fmt.Errorf("set DAEDALUS_SLIM: %w", err)
		}
	} else if err := os.Unsetenv("DAEDALUS_SLIM"); err != nil {
		return fmt.Errorf("unset DAEDALUS_SLIM: %w", err)
	}
	// The thinking toggle: only an explicit thinking: false is exported
	// (as DAEDALUS_THINKING=off, the sole thinking signal daedalus ever
	// sends); unset or true means no agent gets any thinking signal and
	// each follows its own default. The jailed-round builder translates
	// the signal per agent (pi --thinking off, aider --thinking-tokens 0,
	// claude MAX_THINKING_TOKENS=0). Unset symmetrically, for the same
	// stale-export reason as DAEDALUS_SLIM above.
	if cfg.ThinkingDisabled() {
		if err := os.Setenv("DAEDALUS_THINKING", "off"); err != nil {
			return fmt.Errorf("set DAEDALUS_THINKING: %w", err)
		}
	} else if err := os.Unsetenv("DAEDALUS_THINKING"); err != nil {
		return fmt.Errorf("unset DAEDALUS_THINKING: %w", err)
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
	// The worker's name is stamped into every round's captured Usage (so
	// `daedalus report` aggregates per worker) and keys the status file
	// published below.
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
		for _, spec := range workflowRegistry {
			w.RegisterWorkflow(spec.Fn)
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
		workers = append(workers, w)
	}

	// The test worker serves the dedicated test queue: it runs only the
	// suite activities — dumb command runners needing no provider
	// environment — so this host's suites share one queue and one
	// max_concurrent_tests budget, whatever workers created them. The
	// repro-first gate belongs here too: it runs the suite command in a
	// throwaway base checkout.
	if wantsTestWorker(workerType) {
		tw := worker.New(c, activities.TestTaskQueue, opts)
		tw.RegisterActivity(activities.RunTestSuiteActivity)
		tw.RegisterActivity(activities.ReproFirstGateActivity)
		workers = append(workers, tw)
	}

	// Publish slot occupancy next to the pid file for `daedalus report`.
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
	// The selected workers run against a self-managed interrupt channel:
	// whichever Run exits first halts the others, so an asymmetric failure
	// (say, the test worker dying alone on an untyped daemon) stops the
	// survivors too instead of leaving a half-dead daemon with a live pid
	// file and an error nobody reports.
	// Each Run only returns after its in-flight activity outcomes have been
	// reported, and returning on whichever finishes first without halting
	// the others would exit mid-drain — the orphaned-round wedge this
	// shutdown path exists to prevent.
	interrupt := make(chan interface{})
	var haltOnce sync.Once
	halt := func() { haltOnce.Do(func() { close(interrupt) }) }
	// SIGINT/SIGTERM still reach the workers: the forwarder drains the
	// signal channel and halts the shared channel, whose close every Run
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
	return errors.Join(joined...)
}

// statusPublishInterval is how often the worker rewrites its status file.
const statusPublishInterval = 5 * time.Second

// workerStatus is the JSON document published to worker-<name>.status: the
// worker's live slot state (activities.SlotStats) plus its pid, update
// time, and the version of the binary it is executing, so a reader
// (`daedalus report`, `daedalus worker status`) can spot a stale file left
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
