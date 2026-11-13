package activities

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

// plantJSONLAt drops a .jsonl transcript carrying the given mtime into an
// explicit transcript dir (claude's project dir or pi's session dir),
// creating the dir — the fixture every transcript-freshness scan reads.
// Claude-dir files can reuse plantTranscript plus Chtimes; this one covers
// the dirs that helper does not.
func plantJSONLAt(t *testing.T, dir, name string, mtime time.Time) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	if err := os.Chtimes(p, mtime, mtime); err != nil {
		t.Fatalf("set mtime: %v", err)
	}
}

// TestNewestTranscriptMtime pins the newest-wins scan the cot-progress loop
// follows: the newest mtime across claude's, pi's, and codex's host
// transcripts wins — no source has priority, and a previous round's stale
// file on one side never shadows a live writer's newer write on another —
// while non-transcript files and codex rollouts of other worktrees never
// count. A worktree whose sources hold nothing reports not-ok: missing
// dirs are just sources without transcripts, not errors.
func TestNewestTranscriptMtime(t *testing.T) {
	fakeHome(t)
	cxHome := fakeCodexHome(t)
	wt := t.TempDir()

	if mt, ok := NewestTranscriptMtime(wt); ok || !mt.IsZero() {
		t.Errorf("NewestTranscriptMtime with no transcripts = %v, %v; want zero and not-ok", mt, ok)
	}

	old := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	mid := time.Now().Add(-time.Hour).Truncate(time.Second)
	new := time.Now().Add(-30 * time.Minute).Truncate(time.Second)
	claudeDir, err := claudeProjectDir(wt)
	if err != nil {
		t.Fatalf("claudeProjectDir: %v", err)
	}
	piDir, err := piSessionsDir(wt)
	if err != nil {
		t.Fatalf("piSessionsDir: %v", err)
	}

	claude := filepath.Join(claudeDir, "old-round.jsonl")
	plantTranscript(t, wt, "old-round")
	if err := os.Chtimes(claude, old, old); err != nil {
		t.Fatal(err)
	}
	plantJSONLAt(t, piDir, "mid-round.jsonl", mid)
	codex := plantCodexRollout(t, cxHome, "2026/10/05", codexUUIDA, wt)
	if err := os.Chtimes(codex, new, new); err != nil {
		t.Fatal(err)
	}

	// Codex is scanned last, so its newest rollout beating claude's older
	// transcript proves no source has priority...
	if mt, ok := NewestTranscriptMtime(wt); !ok || !mt.Equal(new) {
		t.Errorf("NewestTranscriptMtime = %v, %v; want codex's %v — the newest write wins regardless of source", mt, ok, new)
	}

	// ...and claude overtaking codex proves the converse.
	live := time.Now().Add(-15 * time.Minute).Truncate(time.Second)
	if err := os.Chtimes(claude, live, live); err != nil {
		t.Fatal(err)
	}
	if mt, ok := NewestTranscriptMtime(wt); !ok || !mt.Equal(live) {
		t.Errorf("NewestTranscriptMtime = %v, %v; want claude's %v once it is the newest write", mt, ok, live)
	}

	// Non-transcript files and foreign-worktree rollouts carry no stamp for
	// this worktree, however fresh their mtimes.
	notes := filepath.Join(claudeDir, "notes.txt")
	if err := os.WriteFile(notes, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	fresh := time.Now()
	if err := os.Chtimes(notes, fresh, fresh); err != nil {
		t.Fatal(err)
	}
	foreign := plantCodexRollout(t, cxHome, "2026/10/06", codexUUIDB, t.TempDir())
	if err := os.Chtimes(foreign, fresh, fresh); err != nil {
		t.Fatal(err)
	}
	if mt, ok := NewestTranscriptMtime(wt); !ok || !mt.Equal(live) {
		t.Errorf("NewestTranscriptMtime = %v, %v with fresh non-transcript noise; want claude's %v still", mt, ok, live)
	}
}

// cotSignalCall is one captured SignalWorkflow dispatch, flattened to the
// fields the cot-progress contract speaks.
type cotSignalCall struct {
	workflowID, runID, signal, payload string
}

// cotSignalRecorder stands in for the worker's Temporal client and captures
// every SignalWorkflow call the cot-progress loop makes. The embedded nil
// client pins the interface and panics on any other call — the loop must
// send signals and nothing else. failNext makes exactly one send fail (the
// degraded-freshness path), recorded in the failure count.
type cotSignalRecorder struct {
	client.Client
	mu       sync.Mutex
	failNext bool
	failures int
	calls    []cotSignalCall
}

func (r *cotSignalRecorder) SignalWorkflow(_ context.Context, workflowID, runID, signalName string, arg interface{}) error {
	payload, _ := arg.(string)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, cotSignalCall{workflowID, runID, signalName, payload})
	if r.failNext {
		r.failNext = false
		r.failures++
		return errors.New("relay down")
	}
	return nil
}

func (r *cotSignalRecorder) snapshot() []cotSignalCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]cotSignalCall(nil), r.calls...)
}

func (r *cotSignalRecorder) failedSends() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failures
}

func (r *cotSignalRecorder) failNextSend() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failNext = true
}

// installCotProgressRecorder wires a recorder in as the loop's client and
// shortens the poll interval, restoring both at cleanup — the loop's
// client and cadence are shared package state every jailed round in this
// test binary reads.
func installCotProgressRecorder(t *testing.T, interval time.Duration) *cotSignalRecorder {
	t.Helper()
	rec := &cotSignalRecorder{}
	prevClient, prevInterval := cotProgressClient, cotProgressInterval
	cotProgressClient = rec
	cotProgressInterval = interval
	t.Cleanup(func() { cotProgressClient, cotProgressInterval = prevClient, prevInterval })
	return rec
}

// waitForCotCalls polls the recorder until it holds at least n calls or the
// deadline passes — the loop signals on its own ticker, so the arrival time
// is the interval's, not the test's.
func waitForCotCalls(rec *cotSignalRecorder, n int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if len(rec.snapshot()) >= n {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return len(rec.snapshot()) >= n
}

// cotRoundIdentity is the activity identity a probe activity reports, so a
// test can pin that the loop's signal addresses the round's own workflow.
type cotRoundIdentity struct {
	WorkflowID string
	RunID      string
}

// TestCotProgressLoopSignalsTranscriptAdvances pins the loop's contract in
// a real activity context: the first scan only sets the baseline (a
// previous round's stale transcript never signals, and a still transcript
// produces nothing however long the round runs), each advance past the
// baseline sends exactly one signal — never a per-tick stream — whose
// payload is the RFC3339 mtime and whose target is the running activity's
// own workflow, a failed send costs nothing (the loop carries on and
// stamps the next advance), and closing done ends the loop for good.
func TestCotProgressLoopSignalsTranscriptAdvances(t *testing.T) {
	fakeHome(t)
	fakeCodexHome(t) // keep the codex leg off the host's real sessions tree
	wt := t.TempDir()
	claudeDir, err := claudeProjectDir(wt)
	if err != nil {
		t.Fatalf("claudeProjectDir: %v", err)
	}
	// One shared, second-aligned base keeps every fixture mtime and the
	// payload assertions on the same clock: no wall-clock drift between the
	// choreography and the checks.
	base := time.Now().Truncate(time.Hour)
	// A previous round's transcript, stale before this round starts.
	plantTranscript(t, wt, "live")
	live := filepath.Join(claudeDir, "live.jsonl")
	if err := os.Chtimes(live, base.Add(-time.Hour), base.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	rec := installCotProgressRecorder(t, 25*time.Millisecond)
	interval := 25 * time.Millisecond
	settle := func() { time.Sleep(2*interval + 10*time.Millisecond) }

	probe := func(ctx context.Context, wt string) (cotRoundIdentity, error) {
		id, runID := taskLogIdentity(ctx)
		done := make(chan struct{})
		var once sync.Once
		stop := func() { once.Do(func() { close(done) }) }
		defer stop()
		go cotProgressLoop(ctx, wt, done)

		// Baseline-first: three poll windows over a still transcript must
		// send nothing.
		time.Sleep(3 * interval)
		if calls := rec.snapshot(); len(calls) != 0 {
			return cotRoundIdentity{id, runID}, fmt.Errorf("a still transcript produced %d signals, want 0 — the first scan only sets the baseline", len(calls))
		}

		advance := func(at time.Time) error { return os.Chtimes(live, at, at) }

		// One advance, exactly one signal — the settle proves the loop does
		// not keep signaling on the unchanged transcript every tick.
		t1 := base.Add(-30 * time.Minute)
		if err := advance(t1); err != nil {
			return cotRoundIdentity{id, runID}, err
		}
		if !waitForCotCalls(rec, 1, 2*time.Second) {
			return cotRoundIdentity{id, runID}, fmt.Errorf("the transcript advance produced %d signals, want 1", len(rec.snapshot()))
		}
		settle()
		if calls := rec.snapshot(); len(calls) != 1 {
			return cotRoundIdentity{id, runID}, fmt.Errorf("after settling, the advance holds %d signals, want exactly 1", len(calls))
		}

		// A failed send is logged and dropped: the attempt is recorded, then
		// the loop carries on and stamps the next advance.
		rec.failNextSend()
		t2 := base.Add(-25 * time.Minute)
		if err := advance(t2); err != nil {
			return cotRoundIdentity{id, runID}, err
		}
		if !waitForCotCalls(rec, 2, 2*time.Second) {
			return cotRoundIdentity{id, runID}, fmt.Errorf("the advance whose send failed produced %d signals, want 2 — a failed send must still have been attempted", len(rec.snapshot()))
		}
		t3 := base.Add(-20 * time.Minute)
		if err := advance(t3); err != nil {
			return cotRoundIdentity{id, runID}, err
		}
		if !waitForCotCalls(rec, 3, 2*time.Second) {
			return cotRoundIdentity{id, runID}, fmt.Errorf("the loop did not recover from a failed send: %d signals, want 3", len(rec.snapshot()))
		}
		settle()
		if calls := rec.snapshot(); len(calls) != 3 {
			return cotRoundIdentity{id, runID}, fmt.Errorf("after settling, the round holds %d signals, want exactly 3", len(calls))
		}

		// Closing done ends the loop for good: an advance after the drain
		// window must stay unstamped. The drain first lets any in-flight
		// tick — one that selected the ticker before done closed — land.
		stop()
		settle()
		t4 := base.Add(-15 * time.Minute)
		if err := advance(t4); err != nil {
			return cotRoundIdentity{id, runID}, err
		}
		settle()
		if calls := rec.snapshot(); len(calls) != 3 {
			return cotRoundIdentity{id, runID}, fmt.Errorf("after done closed, the round holds %d signals, want 3 — the loop must have exited", len(calls))
		}
		return cotRoundIdentity{id, runID}, nil
	}

	s := &testsuite.WorkflowTestSuite{}
	env := s.NewTestActivityEnvironment()
	env.RegisterActivity(probe)

	v, err := env.ExecuteActivity(probe, wt)
	if err != nil {
		t.Fatalf("ExecuteActivity: %v", err)
	}
	var id cotRoundIdentity
	if err := v.Get(&id); err != nil {
		t.Fatalf("decode identity: %v", err)
	}
	if id.WorkflowID == "" || id.RunID == "" {
		t.Fatalf("identity = %+v, want the activity's own workflow identity", id)
	}

	if rec.failedSends() != 1 {
		t.Errorf("the recorder saw %d failed sends, want exactly 1 (the degradation path must have been exercised)", rec.failedSends())
	}
	calls := rec.snapshot()
	for i, c := range calls {
		if c.signal != CotProgressSignal {
			t.Errorf("signal %d name = %q, want %q", i, c.signal, CotProgressSignal)
		}
		if c.workflowID != id.WorkflowID || c.runID != id.RunID {
			t.Errorf("signal %d targets %s/%s, want the round's own %s/%s", i, c.workflowID, c.runID, id.WorkflowID, id.RunID)
		}
		got, err := time.Parse(time.RFC3339, c.payload)
		if err != nil {
			t.Errorf("signal %d payload %q does not parse as RFC3339: %v", i, c.payload, err)
			continue
		}
		var want time.Time
		switch i {
		case 0:
			want = base.Add(-30 * time.Minute)
		case 1:
			want = base.Add(-25 * time.Minute)
		case 2:
			want = base.Add(-20 * time.Minute)
		}
		if !got.Equal(want) {
			t.Errorf("signal %d stamps %v, want the %v mtime that triggered it", i, got, want)
		}
	}
	if len(calls) != 3 {
		t.Errorf("the round sent %d signals, want 3 (advance, failed-send advance, post-failure advance)", len(calls))
	}
}

// TestJailedRoundCotProgressFollowsTranscriptAgents pins the round-side
// gate: a jailed transcript-writing round (pi stands in for
// claude/pi/codex) watches its worktree and signals its first mid-round
// transcript write — even when no transcript existed at round start, the
// loop's baseline being the zero time — while a round on an agent that
// keeps no stat-able host transcript (opencode) sends nothing even as the
// same file appears mid-round. Each signal names the wire signal and
// addresses the round's own workflow with an RFC3339 mtime — the shape the
// run workflow's receive loop decodes.
func TestJailedRoundCotProgressFollowsTranscriptAgents(t *testing.T) {
	cases := []struct {
		name        string
		agent       string
		stub        string
		roundEnv    func() []string
		wantSignals int
	}{
		{
			name:  "a pi round signals its first mid-round transcript write",
			agent: "pi",
			stub: `printf '%s\n' '{"type":"session","id":"sess-cot"}' '{"type":"message_end","message":{"content":[{"type":"text","text":"cot ok"}]}}'
sleep 0.3
exit 0`,
			roundEnv:    jailedPiRoundEnv,
			wantSignals: 1,
		},
		{
			name:  "an opencode round sends nothing",
			agent: "opencode",
			stub: `sleep 0.3
echo opencode done
exit 0`,
			roundEnv:    func() []string { return os.Environ() },
			wantSignals: 0,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			newStubLog(t)
			fakeHome(t)
			fakeCodexHome(t)
			wt := t.TempDir()
			rec := installCotProgressRecorder(t, 10*time.Millisecond)
			stubBin(t, "ai-jail", c.stub)

			// The worktree holds no transcript at round start: the loop's
			// baseline is the zero time, so the file the round's agent
			// creates mid-round is its first advance. The write lands 100ms
			// in, well inside the stub round's 300ms lifetime; the channel
			// keeps the goroutine's error off t until the test is back on
			// the main goroutine.
			wrote := make(chan error, 1)
			go func() {
				time.Sleep(100 * time.Millisecond)
				var err error
				if dir, e := claudeProjectDir(wt); e != nil {
					err = e
				} else if e := os.MkdirAll(dir, 0o755); e != nil {
					err = e
				} else {
					err = os.WriteFile(filepath.Join(dir, "live.jsonl"), []byte("{}\n"), 0o644)
				}
				wrote <- err
			}()

			probe := func(ctx context.Context, agent, wt string) (cotRoundIdentity, error) {
				id, runID := taskLogIdentity(ctx)
				_, err := runJailedRound(ctx, c.roundEnv(), "", agent, wt, "round prompt")
				return cotRoundIdentity{id, runID}, err
			}
			s := &testsuite.WorkflowTestSuite{}
			env := s.NewTestActivityEnvironment()
			env.RegisterActivity(probe)
			v, err := env.ExecuteActivity(probe, c.agent, wt)
			if err != nil {
				t.Fatalf("ExecuteActivity: %v", err)
			}
			var id cotRoundIdentity
			if err := v.Get(&id); err != nil {
				t.Fatalf("decode identity: %v", err)
			}
			if err := <-wrote; err != nil {
				t.Fatalf("the mid-round transcript write failed: %v", err)
			}

			// The pi leg waits out the loop's own cadence for its one
			// signal; the opencode leg grants the loop several windows past
			// the round to wrongly fire before the count is final.
			if c.wantSignals > 0 && !waitForCotCalls(rec, c.wantSignals, 2*time.Second) {
				t.Fatalf("the %s round sent %d signals, want %d", c.agent, len(rec.snapshot()), c.wantSignals)
			}
			time.Sleep(200 * time.Millisecond)
			calls := rec.snapshot()
			if len(calls) != c.wantSignals {
				t.Fatalf("the %s round sent %d signals, want %d", c.agent, len(calls), c.wantSignals)
			}
			for i, call := range calls {
				if call.signal != CotProgressSignal {
					t.Errorf("signal %d name = %q, want %q", i, call.signal, CotProgressSignal)
				}
				if call.workflowID != id.WorkflowID || call.runID != id.RunID {
					t.Errorf("signal %d targets %s/%s, want the round's own %s/%s", i, call.workflowID, call.runID, id.WorkflowID, id.RunID)
				}
				if _, err := time.Parse(time.RFC3339, call.payload); err != nil {
					t.Errorf("signal %d payload %q does not parse as RFC3339: %v", i, call.payload, err)
				}
			}
		})
	}
}
