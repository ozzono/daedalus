package workflows

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
)

// depProbeStep is one scripted dependency-probe outcome.
type depProbeStep struct {
	probe activities.DependencyProbe
	err   error
}

// depProbeRecorder captures every CheckDependencyActivity call — the
// workflow gate's one poll per round — and plays its script in order, the
// last entry repeating once exhausted (a wait-then-approve script needs
// only its terminal outcome written once).
type depProbeRecorder struct {
	env    *testsuite.TestWorkflowEnvironment
	ids    []string
	script []depProbeStep
}

func (r *depProbeRecorder) record() {
	// The probe is registered under its pinned string name (the worker's
	// registration shape) before the mock wraps it — the test environment
	// refuses an OnActivity by name for an unregistered activity.
	r.env.RegisterActivityWithOptions(activities.NewCheckDependencyActivity(nil),
		activity.RegisterOptions{Name: activities.CheckDependencyActivityName})
	// Maybe: a non-dependent run must never probe at all, and the tests
	// pin the call counts explicitly off ids.
	r.env.OnActivity(activities.CheckDependencyActivityName, mock.Anything, mock.Anything).Maybe().
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if id, ok := a.(string); ok {
					r.ids = append(r.ids, id)
				}
			}
		}).
		Return(func(ctx context.Context, id string) (activities.DependencyProbe, error) {
			if len(r.script) == 0 {
				return activities.DependencyProbe{}, errors.New("no scripted probes")
			}
			step := r.script[0]
			if len(r.script) > 1 {
				r.script = r.script[1:]
			}
			return step.probe, step.err
		})
}

// depVisSnapshot is the run-visibility reading a dependent-run probe takes:
// the gate's pending stamp, the DaedalusDependsOn upsert (readVis in
// visibility_test.go predates the third attribute), and the
// DaedalusStartedAt release stamp — zero when absent (legacy replays, runs
// still waiting).
type depVisSnapshot struct {
	Status    string
	DependsOn string
	StartedAt time.Time
}

// readDepVis reads the attributes from the executing workflow's info — the
// raw payloads, decoded with the default converter exactly the way
// `daedalus`'s list rendering decodes a listed execution.
func readDepVis(ctx workflow.Context) depVisSnapshot {
	dc := converter.GetDefaultDataConverter()
	fields := workflow.GetInfo(ctx).SearchAttributes.GetIndexedFields()
	var s depVisSnapshot
	if p, ok := fields[DaedalusStatusAttr]; ok {
		_ = dc.FromPayload(p, &s.Status)
	}
	if p, ok := fields[DaedalusDependsOnAttr]; ok {
		_ = dc.FromPayload(p, &s.DependsOn)
	}
	if p, ok := fields[DaedalusStartedAtAttr]; ok {
		_ = dc.FromPayload(p, &s.StartedAt)
	}
	return s
}

// runDepVisProbe executes flow behind a "vis" query handler and captures
// the attributes read after the flow returned — the same shape as
// runVisProbe, with the dependency attributes included.
func runDepVisProbe(t *testing.T, env *testsuite.TestWorkflowEnvironment, flow func(workflow.Context, PipelineInput) (string, error), in PipelineInput) depVisSnapshot {
	t.Helper()
	final := new(depVisSnapshot)
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input PipelineInput) (string, error) {
		if err := workflow.SetQueryHandler(ctx, "vis", func() (depVisSnapshot, error) {
			return readDepVis(ctx), nil
		}); err != nil {
			return "", err
		}
		_, err := flow(ctx, input)
		*final = readDepVis(ctx)
		return "", err
	}, workflow.RegisterOptions{Name: "depVisProbe"})
	env.ExecuteWorkflow("depVisProbe", in)
	return *final
}

// runPokeProbe executes flow behind a wrapper that captures the run's own
// workflow id, the flow's returned branch, and the final visibility
// snapshot — the id the CompleteGreen release poke must name.
func runPokeProbe(t *testing.T, env *testsuite.TestWorkflowEnvironment, flow func(workflow.Context, PipelineInput) (string, error), in PipelineInput) (string, string, depVisSnapshot) {
	t.Helper()
	selfID := ""
	branch := ""
	final := new(depVisSnapshot)
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input PipelineInput) (string, error) {
		selfID = workflow.GetInfo(ctx).WorkflowExecution.ID
		b, err := flow(ctx, input)
		branch = b
		*final = readDepVis(ctx)
		return "", err
	}, workflow.RegisterOptions{Name: "pokeProbe"})
	env.ExecuteWorkflow("pokeProbe", in)
	return selfID, branch, *final
}

// depPokeRecorder captures every NotifyDependentsActivity call — the
// dependent-release poke CompleteGreen fires once a run reaches a terminal
// state — and answers each with a successful single-dependent poke.
// Maybe: the legacy replay path and gate-only runs never poke, and the
// tests pin the call counts explicitly off ids.
type depPokeRecorder struct {
	env     *testsuite.TestWorkflowEnvironment
	ids     []string
	stubErr error
}

// register pins the poke's registered name (the worker's registration
// shape) — the test environment refuses an OnActivity by name for an
// unregistered activity, and refuses the registration itself once any
// activity mock exists, so a test mixing recorders registers everything
// before mocking anything.
func (r *depPokeRecorder) register() {
	r.env.RegisterActivityWithOptions(activities.NewNotifyDependentsActivity(nil),
		activity.RegisterOptions{Name: activities.NotifyDependentsActivityName})
}

// record wires the recording mock. register must have run already.
func (r *depPokeRecorder) record() {
	r.env.OnActivity(activities.NotifyDependentsActivityName, mock.Anything, mock.Anything).Maybe().
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if id, ok := a.(string); ok {
					r.ids = append(r.ids, id)
				}
			}
		}).
		Return(func(ctx context.Context, id string) (int, error) {
			return 1, r.stubErr
		})
}

// stubDependentHappyPath wires the full feature-dev happy path, capturing
// the created worktree input (what the gate injected into it) and the
// agent rounds (the opener it produced).
func stubDependentHappyPath(t *testing.T, env *testsuite.TestWorkflowEnvironment) (*activities.WorktreeInput, *agentRecorder) {
	t.Helper()
	// Pointer: the capture fills in after this helper returns (the test
	// env executes the workflow on ExecuteWorkflow), so the caller must
	// read through the reference, not a zero copy.
	created := new(activities.WorktreeInput)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.WorktreeInput); ok {
					*created = in
				}
			}
		}).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil).Once()
	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	stubTestPhase(env)
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	return created, rec
}

// TestDependentRunStartsFromDependencyBranch pins the approving path: the
// gate probes the named dependency verbatim, injects the dependency's
// preserved branch as the worktree base (BaseBranch alone — the input's
// own BaseBranch would flip the opener to a continuation), and the run
// opens as a fresh Implement task, not a Continue.
func TestDependentRunStartsFromDependencyBranch(t *testing.T) {
	env := newTestEnv(t)
	probe := &depProbeRecorder{env: env, script: []depProbeStep{
		{probe: activities.DependencyProbe{Terminal: true, Completed: true, Result: "daedalus/issue-41-1", Status: "completed"}},
	}}
	probe.record()
	created, rec := stubDependentHappyPath(t, env)

	in := baseInput()
	in.DependsOn = "tf-daeadalus-issue-41"
	env.ExecuteWorkflow(FeatureDevWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(probe.ids) != 1 || probe.ids[0] != in.DependsOn {
		t.Errorf("probe ids = %v, want exactly one probe of %q", probe.ids, in.DependsOn)
	}
	if created.BaseBranch != "daedalus/issue-41-1" {
		t.Errorf("worktree BaseBranch = %q, want the dependency's preserved branch", created.BaseBranch)
	}
	if len(rec.inputs) == 0 {
		t.Fatal("no agent round ran")
	}
	// Fresh task, not a continuation: the opener is the implement framing,
	// never the continue template's "continuing a previous attempt".
	prompt := rec.inputs[0].Prompt
	if !strings.Contains(prompt, "Implement the change") || strings.Contains(prompt, "continuing a previous attempt") {
		t.Errorf("opening prompt = %.120q..., want the fresh Implement framing", prompt)
	}
	env.AssertExpectations(t)
}

// TestDependentRunWaitsForRunningDependency pins the live-wait path: a
// non-terminal probe keeps the run pending (one sleep per poll — the test
// clock fires the timers), and the approving terminal probe ends the wait
// with the branch injected.
func TestDependentRunWaitsForRunningDependency(t *testing.T) {
	env := newTestEnv(t)
	probe := &depProbeRecorder{env: env, script: []depProbeStep{
		{probe: activities.DependencyProbe{Status: "running"}},
		{probe: activities.DependencyProbe{Terminal: true, Completed: true, Result: "daedalus/issue-41-1", Status: "completed"}},
	}}
	probe.record()
	created, _ := stubDependentHappyPath(t, env)

	in := baseInput()
	in.DependsOn = "tf-issue-41"
	env.ExecuteWorkflow(FeatureDevWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(probe.ids) != 2 {
		t.Errorf("probe calls = %d (%v), want one running poll then the approving one", len(probe.ids), probe.ids)
	}
	if created.BaseBranch != "daedalus/issue-41-1" {
		t.Errorf("worktree BaseBranch = %q, want the dependency's preserved branch", created.BaseBranch)
	}
	env.AssertExpectations(t)
}

// stubDormantWorktree stubs both worktree calls with counters, for tests
// whose run must fail before either ever fires (a broken chain preserves
// nothing): the caller asserts both counts read zero.
func stubDormantWorktree(env *testsuite.TestWorkflowEnvironment) (created, cleaned *int) {
	created, cleaned = new(int), new(int)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).Maybe().
		Run(func(mock.Arguments) { *created++ }).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Maybe().
		Run(func(mock.Arguments) { *cleaned++ }).
		Return(nil)
	return created, cleaned
}

// TestDependentRunFailsOnBrokenDependency pins the broken-chain table: a
// dependency that stops without approval — failed, canceled, terminated,
// timed out, or completed parked — fails the run before anything was
// started: no worktree is created, no cleanup ever runs, and the error
// names both the dependency and the state that broke the chain.
func TestDependentRunFailsOnBrokenDependency(t *testing.T) {
	for _, c := range []struct {
		name  string
		step  depProbeStep
		state string
	}{
		{"failed", depProbeStep{probe: activities.DependencyProbe{Terminal: true, Status: "failed"}}, "failed"},
		{"canceled", depProbeStep{probe: activities.DependencyProbe{Terminal: true, Status: "canceled"}}, "canceled"},
		{"terminated", depProbeStep{probe: activities.DependencyProbe{Terminal: true, Status: "terminated"}}, "terminated"},
		{"timed out", depProbeStep{probe: activities.DependencyProbe{Terminal: true, Status: "timed out"}}, "timed out"},
		{"parked", depProbeStep{probe: activities.DependencyProbe{Terminal: true, Completed: true, Result: ParkedResult("needs the maintainer"), Status: "completed"}}, "parked"},
	} {
		t.Run(c.name, func(t *testing.T) {
			env := newTestEnv(t)
			probe := &depProbeRecorder{env: env, script: []depProbeStep{c.step}}
			probe.record()
			created, cleaned := stubDormantWorktree(env)

			in := baseInput()
			in.DependsOn = "tf-issue-41"
			env.ExecuteWorkflow(FeatureDevWorkflow, in)

			err := env.GetWorkflowError()
			if err == nil {
				t.Fatal("want the workflow to fail on the broken dependency")
			}
			if !strings.Contains(err.Error(), "chain is broken") || !strings.Contains(err.Error(), c.state) {
				t.Errorf("error = %v, want the broken-chain diagnostic naming %q", err, c.state)
			}
			if *created != 0 || *cleaned != 0 {
				t.Errorf("worktree created %d times, cleaned %d times — a broken chain must fail before anything was started", *created, *cleaned)
			}
		})
	}
}

// TestDependentRunCapsFailingProbes pins the transient-error ceiling: a
// permanently failing probe retries once per poll interval until the
// consecutive-failure cap, then fails the run loudly instead of pending
// forever.
func TestDependentRunCapsFailingProbes(t *testing.T) {
	env := newTestEnv(t)
	probe := &depProbeRecorder{env: env, script: []depProbeStep{
		{err: errors.New("probe exploded")},
	}}
	probe.record()
	created, cleaned := stubDormantWorktree(env)

	in := baseInput()
	in.DependsOn = "tf-issue-41"
	env.ExecuteWorkflow(FeatureDevWorkflow, in)

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want the workflow to fail at the probe-failure cap")
	}
	want := fmt.Sprintf("failed %d probes in a row", maxConsecutiveTimeouts)
	if !strings.Contains(err.Error(), want) {
		t.Errorf("error = %v, want the cap diagnostic %q", err, want)
	}
	if len(probe.ids) != maxConsecutiveTimeouts {
		t.Errorf("probe calls = %d, want exactly the cap (%d)", len(probe.ids), maxConsecutiveTimeouts)
	}
	if *created != 0 || *cleaned != 0 {
		t.Errorf("worktree created %d times, cleaned %d times — a capped-out gate fails before anything was started", *created, *cleaned)
	}
}

// TestDependentRunStampsPending pins the visibility contract of the wait:
// the gate upserts DaedalusDependsOn (what `daedalus list` renders) and
// stamps DaedalusStatus pending while it waits — the value that says a run
// is live but owns no work. The probe here observes only the gate's own
// upserts, so pending is the last value it sees even on a broken chain:
// production registers every flow through CompleteGreen, which stamps the
// terminal status failed over this pending once the gate's error
// propagates.
func TestDependentRunStampsPending(t *testing.T) {
	env := newTestEnv(t)
	probe := &depProbeRecorder{env: env, script: []depProbeStep{
		{probe: activities.DependencyProbe{Terminal: true, Status: "failed"}},
	}}
	probe.record()
	stubDormantWorktree(env)

	in := baseInput()
	in.DependsOn = "tf-issue-41"
	final := runDepVisProbe(t, env, FeatureDevWorkflow, in)

	if err := env.GetWorkflowError(); err == nil || !strings.Contains(err.Error(), "chain is broken") {
		t.Fatalf("workflow error = %v, want the broken-chain failure", err)
	}
	if final.Status != string(StatusPending) {
		t.Errorf("final DaedalusStatus = %q, want %q — the gate's during-wait stamp, which CompleteGreen's failed overrides once the error lands", final.Status, StatusPending)
	}
	if final.DependsOn != in.DependsOn {
		t.Errorf("DaedalusDependsOn = %q, want %q", final.DependsOn, in.DependsOn)
	}
}

// TestNonDependentRunNeverProbes pins that an empty DependsOn skips the
// gate entirely: no CheckDependencyActivity call, and the worktree's
// BaseBranch stays empty — a plain fresh run's shape, unchanged.
func TestNonDependentRunNeverProbes(t *testing.T) {
	env := newTestEnv(t)
	probe := &depProbeRecorder{env: env, script: []depProbeStep{
		{probe: activities.DependencyProbe{Terminal: true, Status: "failed"}},
	}}
	probe.record()
	created, _ := stubDependentHappyPath(t, env)

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(probe.ids) != 0 {
		t.Errorf("probe calls = %v, want none — the gate is skipped without a dependency", probe.ids)
	}
	if created.BaseBranch != "" {
		t.Errorf("worktree BaseBranch = %q, want empty — a fresh run starts from HEAD", created.BaseBranch)
	}
	env.AssertExpectations(t)
}

// TestDependencyReleases pins the release table the gate reads: only a
// section in release posture (enabled, a fallback branch resolved at
// start) can release, and only past a stopped state its skip flags select.
// Paused is outside every skip set — it has not stopped — and a vanished
// id ("not found") is a broken submit, never a state to release past.
func TestDependencyReleases(t *testing.T) {
	allSkips := config.DependencyConfig{
		FallbackBranch: "release-base",
		SkipFailed:     true,
		SkipStuck:      true,
		SkipCanceled:   true,
	}
	disabled := false
	disabledWithBranch := allSkips
	disabledWithBranch.Enabled = &disabled
	noParked := config.DependencyConfig{FallbackBranch: "release-base", SkipFailed: true, SkipStuck: true, SkipCanceled: true}
	noParked.SkipParked = &disabled

	for _, c := range []struct {
		name  string
		cfg   config.DependencyConfig
		state string
		want  bool
	}{
		{"zero value refuses every state", config.DependencyConfig{}, "parked", false},
		{"zero value refuses failed", config.DependencyConfig{}, "failed", false},
		{"fallback only releases parked (the default)", config.DependencyConfig{FallbackBranch: "b"}, "parked", true},
		{"fallback only refuses failed", config.DependencyConfig{FallbackBranch: "b"}, "failed", false},
		{"fallback only refuses timed out", config.DependencyConfig{FallbackBranch: "b"}, "timed out", false},
		{"fallback only refuses canceled", config.DependencyConfig{FallbackBranch: "b"}, "canceled", false},
		{"fallback only refuses terminated", config.DependencyConfig{FallbackBranch: "b"}, "terminated", false},
		{"explicit skip_parked false refuses parked", noParked, "parked", false},
		{"all skips release failed", allSkips, "failed", true},
		{"all skips release timed out", allSkips, "timed out", true},
		{"all skips release canceled", allSkips, "canceled", true},
		{"all skips release terminated", allSkips, "terminated", true},
		{"paused is outside every skip set", allSkips, "paused", false},
		{"a vanished id is outside every skip set", allSkips, "not found", false},
		{"an unknown status refuses", allSkips, "completed", false},
		{"a disabled section refuses parked", disabledWithBranch, "parked", false},
		{"a disabled section refuses failed", disabledWithBranch, "failed", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := dependencyReleases(c.cfg, c.state); got != c.want {
				t.Errorf("dependencyReleases(%+v, %q) = %v, want %v", c.cfg, c.state, got, c.want)
			}
		})
	}
}

// TestDependentRunReleasesPastParkedDependency pins the release path end to
// end: a dependency that stopped in a skip-listed state does not fail the
// run — the gate starts it anyway from the resolved fallback branch
// (injected as the worktree's BaseBranch, exactly like an approving
// dependency's preserved branch), and the dependent starts its work rather
// than failing.
func TestDependentRunReleasesPastParkedDependency(t *testing.T) {
	env := newTestEnv(t)
	probe := &depProbeRecorder{env: env, script: []depProbeStep{
		{probe: activities.DependencyProbe{Terminal: true, Completed: true, Result: ParkedResult("needs the maintainer"), Status: "completed"}},
	}}
	probe.record()
	created, rec := stubDependentHappyPath(t, env)

	in := baseInput()
	in.DependsOn = "tf-issue-41"
	in.Dependency = config.DependencyConfig{FallbackBranch: "release-base"}
	env.ExecuteWorkflow(FeatureDevWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v — a skip-listed park must release, not fail", err)
	}
	if len(probe.ids) != 1 {
		t.Errorf("probe calls = %v, want exactly one", probe.ids)
	}
	if created.BaseBranch != "release-base" {
		t.Errorf("worktree BaseBranch = %q, want the resolved fallback branch", created.BaseBranch)
	}
	if len(rec.inputs) == 0 {
		t.Fatal("no agent round ran — a released run starts its work")
	}
	env.AssertExpectations(t)
}

// TestDependentRunWaitsOutPausedDependency pins that paused is not a stop:
// the gate keeps polling a paused dependency — even with every skip flag
// set, since paused is outside every skip set — until it resumes and
// approves, and the run then starts from the dependency's preserved branch.
func TestDependentRunWaitsOutPausedDependency(t *testing.T) {
	env := newTestEnv(t)
	probe := &depProbeRecorder{env: env, script: []depProbeStep{
		{probe: activities.DependencyProbe{Status: "paused"}},
		{probe: activities.DependencyProbe{Status: "running"}},
		{probe: activities.DependencyProbe{Terminal: true, Completed: true, Result: "daedalus/issue-41-1", Status: "completed"}},
		{probe: activities.DependencyProbe{Terminal: true, Completed: true, Result: "daedalus/issue-41-1", Status: "completed"}},
	}}
	probe.record()
	created, _ := stubDependentHappyPath(t, env)

	in := baseInput()
	in.DependsOn = "tf-issue-41"
	in.Dependency = config.DependencyConfig{
		FallbackBranch: "release-base",
		SkipFailed:     true,
		SkipStuck:      true,
		SkipCanceled:   true,
	}
	env.ExecuteWorkflow(FeatureDevWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v — a paused dependency must be waited out, not released past or failed", err)
	}
	if len(probe.ids) != 3 {
		t.Errorf("probe calls = %d (%v), want paused polled, then running, then the approval", len(probe.ids), probe.ids)
	}
	if created.BaseBranch != "daedalus/issue-41-1" {
		t.Errorf("worktree BaseBranch = %q, want the dependency's preserved branch once it approved", created.BaseBranch)
	}
	env.AssertExpectations(t)
}

// TestDependentRunPokeReleasesGate pins the poke-when-done release: the
// wakeup signal the dependency's CompleteGreen sends lands seconds into
// the wait — long before any fallback-scale timer — the gate re-probes at
// once and starts, stamping DaedalusStartedAt as the run's first moment of
// real work.
func TestDependentRunPokeReleasesGate(t *testing.T) {
	env := newTestEnv(t)
	probe := &depProbeRecorder{env: env, script: []depProbeStep{
		{probe: activities.DependencyProbe{Status: "running"}},
		{probe: activities.DependencyProbe{Terminal: true, Completed: true, Result: "daedalus/issue-41-1", Status: "completed"}},
	}}
	probe.record()
	stubDependentHappyPath(t, env)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow("wakeup", "") }, 5*time.Second)

	in := baseInput()
	in.DependsOn = "tf-issue-41"
	start := env.Now()
	final := runDepVisProbe(t, env, FeatureDevWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	// Under a minute: the signal plus one release-grace wait — not the
	// 30-minute fallback, and not even the legacy minute poll.
	if elapsed := env.Now().Sub(start); elapsed >= time.Minute {
		t.Errorf("run released after %v, want the poke's grace cadence (signal, then one re-probe)", elapsed)
	}
	if len(probe.ids) != 2 {
		t.Errorf("probe calls = %d (%v), want the initial poll then the poke-triggered re-probe", len(probe.ids), probe.ids)
	}
	if final.StartedAt.IsZero() {
		t.Error("DaedalusStartedAt absent, want the release stamp — runtime bills from the gate's release, not the dispatch")
	}
	env.AssertExpectations(t)
}

// TestDependentRunPokeGraceIsBounded pins what a poke buys: one signal
// grants the grace cadence for a bounded stretch — the poked gate keeps
// re-probing through misses, but once the grant runs out the wait falls
// back to the long timer instead of re-probing forever.
func TestDependentRunPokeGraceIsBounded(t *testing.T) {
	env := newTestEnv(t)
	// The grant covers the poke's own re-probe plus maxPostPokeProbes
	// grace waits' worth after it; the approving probe lands one wait
	// later, on the fallback.
	script := make([]depProbeStep, 0, maxPostPokeProbes+3)
	for i := 0; i < maxPostPokeProbes+2; i++ {
		script = append(script, depProbeStep{probe: activities.DependencyProbe{Status: "running"}})
	}
	script = append(script, depProbeStep{probe: activities.DependencyProbe{
		Terminal: true, Completed: true, Result: "daedalus/issue-41-1", Status: "completed"}})
	probe := &depProbeRecorder{env: env, script: script}
	probe.record()
	stubDependentHappyPath(t, env)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow("wakeup", "") }, 5*time.Second)

	in := baseInput()
	in.DependsOn = "tf-issue-41"
	start := env.Now()
	runDepVisProbe(t, env, FeatureDevWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	// The initial poll, maxPostPokeProbes+1 probes at the grace cadence
	// while the dependency stays running, then — the grant spent — the
	// approving probe on the fallback timer.
	if len(probe.ids) != maxPostPokeProbes+3 {
		t.Errorf("probe calls = %d (%v), want the initial poll, %d grace probes, and the approving fallback probe", len(probe.ids), probe.ids, maxPostPokeProbes+1)
	}
	if elapsed := env.Now().Sub(start); elapsed < depFallbackInterval || elapsed >= 2*depFallbackInterval {
		t.Errorf("run released after %v, want exactly one fallback wait after the grant runs out (%d grace probes, then the approving fallback probe)", elapsed, maxPostPokeProbes)
	}
	env.AssertExpectations(t)
}

// TestDependentRunFallbackReleasesGate pins the no-poke path: an
// ungraceful dependency death leaves nothing to signal, and the gate still
// releases on the long fallback wait instead of pending forever.
func TestDependentRunFallbackReleasesGate(t *testing.T) {
	env := newTestEnv(t)
	probe := &depProbeRecorder{env: env, script: []depProbeStep{
		{probe: activities.DependencyProbe{Status: "running"}},
		{probe: activities.DependencyProbe{Terminal: true, Completed: true, Result: "daedalus/issue-41-1", Status: "completed"}},
	}}
	probe.record()
	stubDependentHappyPath(t, env)

	in := baseInput()
	in.DependsOn = "tf-issue-41"
	start := env.Now()
	runDepVisProbe(t, env, FeatureDevWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if elapsed := env.Now().Sub(start); elapsed < depFallbackInterval {
		t.Errorf("run released after %v, want the fallback wait — nothing poked the gate", elapsed)
	}
	if len(probe.ids) != 2 {
		t.Errorf("probe calls = %d (%v), want one poll then the fallback re-probe", len(probe.ids), probe.ids)
	}
	env.AssertExpectations(t)
}

// TestDependentRunLegacyReplayGate pins the replay contract of the second
// generation: with the dependency-gate-wakeup marker absent from the
// recorded history — GetVersion hands back the default — the gate waits
// out the frozen minute poll, the wrapper never pokes, and no
// DaedalusStartedAt stamp exists, so an in-flight run replaying on an
// upgraded worker neither wedges on new commands nor changes its clock.
func TestDependentRunLegacyReplayGate(t *testing.T) {
	env := newTestEnv(t)
	poke := &depPokeRecorder{env: env}
	poke.register()
	probe := &depProbeRecorder{env: env, script: []depProbeStep{
		{probe: activities.DependencyProbe{Status: "running"}},
		{probe: activities.DependencyProbe{Terminal: true, Completed: true, Result: "daedalus/issue-41-1", Status: "completed"}},
	}}
	probe.record()
	poke.record()
	stubDependentHappyPath(t, env)

	// Ordering is forced here rather than through runDepVisProbe: the
	// environment refuses workflow registrations once a workflow mock
	// exists, so the wrapper registers first and the version mock — the
	// recorded history carrying no dependency-gate-wakeup marker, GetVersion
	// hands back the default — lands between registration and execution.
	final := new(depVisSnapshot)
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input PipelineInput) (string, error) {
		_, err := CompleteGreen(FeatureDevWorkflow)(ctx, input)
		*final = readDepVis(ctx)
		return "", err
	}, workflow.RegisterOptions{Name: "legacyDepProbe"})
	env.OnGetVersion(dependencyWakeupChangeID, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)

	in := baseInput()
	in.DependsOn = "tf-issue-41"
	start := env.Now()
	env.ExecuteWorkflow("legacyDepProbe", in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	elapsed := env.Now().Sub(start)
	if elapsed < depPollInterval || elapsed >= depFallbackInterval {
		t.Errorf("run released after %v, want the legacy minute poll (>= %v) and not the new fallback (< %v)", elapsed, depPollInterval, depFallbackInterval)
	}
	if len(probe.ids) != 2 {
		t.Errorf("probe calls = %d (%v), want one poll then the legacy re-probe", len(probe.ids), probe.ids)
	}
	if len(poke.ids) != 0 {
		t.Errorf("poke calls = %v, want none — a legacy replay runs without the release poke", poke.ids)
	}
	if !final.StartedAt.IsZero() {
		t.Errorf("DaedalusStartedAt = %v, want absent — a legacy replay records no release stamp", final.StartedAt)
	}
}

// TestCompleteGreenPokesDependents pins the release poke at the
// CompleteGreen choke point: a run reaching a terminal state — approved
// and parked alike — pokes the dependents waiting on it, naming this run's
// own workflow id (what their DaedalusDependsOn attribute matches), and a
// failed poke leaves the run's own outcome and stamps untouched, the
// fallback poll being the release path it falls back to.
func TestCompleteGreenPokesDependents(t *testing.T) {
	t.Run("an approved run pokes its dependents", func(t *testing.T) {
		env := newTestEnv(t)
		poke := &depPokeRecorder{env: env}
		poke.register()
		stubWorktree(t, env)
		rec := &agentRecorder{env: env}
		rec.record()
		rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
		rev.record()
		stubTestPhase(env)
		env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
			Return("daedalus/issue-42-1", nil).Once()
		poke.record()

		selfID, branch, final := runPokeProbe(t, env, CompleteGreen(FeatureDevWorkflow), baseInput())

		if err := env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if branch != "daedalus/issue-42-1" {
			t.Errorf("workflow branch = %q, want the preserved branch", branch)
		}
		if len(poke.ids) != 1 || poke.ids[0] != selfID {
			t.Errorf("poke ids = %v, want exactly one naming this run (%q)", poke.ids, selfID)
		}
		if final.Status != string(StatusApproved) {
			t.Errorf("final DaedalusStatus = %q, want %q — the poke sits beside the terminal stamp, not over it", final.Status, StatusApproved)
		}
		env.AssertExpectations(t)
	})

	t.Run("a parked run pokes its dependents too", func(t *testing.T) {
		env := newTestEnv(t)
		poke := &depPokeRecorder{env: env}
		poke.register()
		stubWorktree(t, env)
		rec := &agentRecorder{env: env}
		rec.record()
		rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
			{NeedsMaintainer: true, Comments: "needs the maintainer's secret"},
		}}
		rev.record()
		stubTestPhase(env)
		poke.record()

		selfID, branch, _ := runPokeProbe(t, env, CompleteGreen(FeatureDevWorkflow), baseInput())

		if err := env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if !IsParkedResult(branch) {
			t.Errorf("workflow branch = %q, want a green completion carrying the park marker", branch)
		}
		if len(poke.ids) != 1 || poke.ids[0] != selfID {
			t.Errorf("poke ids = %v, want exactly one naming this run (%q) — a parked dependency still breaks the chain, and the dependent learns it in seconds", poke.ids, selfID)
		}
		env.AssertExpectations(t)
	})

	t.Run("a failed poke leaves the run's outcome untouched", func(t *testing.T) {
		env := newTestEnv(t)
		poke := &depPokeRecorder{env: env, stubErr: errors.New("visibility store down")}
		poke.register()
		stubWorktree(t, env)
		rec := &agentRecorder{env: env}
		rec.record()
		rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
		rev.record()
		stubTestPhase(env)
		env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
			Return("daedalus/issue-42-1", nil).Once()
		poke.record()

		_, branch, final := runPokeProbe(t, env, CompleteGreen(FeatureDevWorkflow), baseInput())

		if err := env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow error: %v, want the poke failure swallowed — the gate's fallback poll is the release path it drops back to", err)
		}
		if branch != "daedalus/issue-42-1" {
			t.Errorf("workflow branch = %q, want the preserved branch unchanged by the failed poke", branch)
		}
		if final.Status != string(StatusApproved) {
			t.Errorf("final DaedalusStatus = %q, want %q", final.Status, StatusApproved)
		}
		env.AssertExpectations(t)
	})
}
