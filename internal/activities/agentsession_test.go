package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestPathSlug pins the transcript-directory encoding: every character
// outside [A-Za-z0-9] becomes '-', matching claude's own project-dir
// encoding so the activities look exactly where claude writes.
func TestPathSlug(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"/home/u/.repo", "-home-u--repo"},
		{"plain", "plain"},
		{"a b/c", "a-b-c"},
		{"", ""},
	} {
		if got := pathSlug(c.in); got != c.want {
			t.Errorf("pathSlug(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestSessionStateRoleAccessors pins the role-to-field mapping: each of the
// run's four conversations lands in (and reads back from) its own field, so
// one role's id can never be served to another.
func TestSessionStateRoleAccessors(t *testing.T) {
	ids := map[SessionRole]string{
		RoleDev:        "d",
		RoleTest:       "t",
		RoleDevReview:  "dr",
		RoleTestReview: "tr",
	}
	var st sessionState
	for role, id := range ids {
		st.set(role, id)
	}
	for role, want := range ids {
		if got := st.role(role); got != want {
			t.Errorf("role(%s) = %q, want %q", role, got, want)
		}
	}
}

// TestRecordedSessionRoundTrip pins the record contract: an id recorded for
// a worktree and role reads back for that role only — never for a sibling
// role in the same worktree, and never from another worktree's record.
func TestRecordedSessionRoundTrip(t *testing.T) {
	fakeHome(t)
	ctx := context.Background()
	wt := t.TempDir()

	recordSession(ctx, wt, RoleTest, "sess-t")
	plantTranscript(t, wt, "sess-t")
	id, stale := recordedSession(ctx, wt, RoleTest)
	if id != "sess-t" || stale {
		t.Fatalf("recordedSession = %q (stale=%v), want sess-t live", id, stale)
	}
	for _, role := range []SessionRole{RoleDev, RoleDevReview, RoleTestReview} {
		if id, _ := recordedSession(ctx, wt, role); id != "" {
			t.Errorf("role %s resumed %q; one role's session must not serve another", role, id)
		}
	}
	if id, _ := recordedSession(ctx, t.TempDir(), RoleTest); id != "" {
		t.Errorf("another worktree resumed %q; records must not cross trees", id)
	}
}

// TestRecordedSessionOtherRunNeverResumes pins the run scoping: a record
// left by another workflow run never resumes, even with its transcript
// still on disk — sessions do not cross runs. (Unit tests run outside a
// real activity context, so the caller's run id is the empty string.)
func TestRecordedSessionOtherRunNeverResumes(t *testing.T) {
	fakeHome(t)
	ctx := context.Background()
	wt := t.TempDir()

	path, err := sessionStatePath(wt)
	if err != nil {
		t.Fatalf("sessionStatePath: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(sessionState{Worktree: wt, RunID: "run-1", Dev: "sess-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	plantTranscript(t, wt, "sess-1")

	if id, stale := recordedSession(ctx, wt, RoleDev); id != "" || stale {
		t.Errorf("recordedSession = %q (stale=%v), want no resume across runs", id, stale)
	}
}

// TestRecordedSessionStaleWhenTranscriptGone pins the stale signal: a
// session WAS on record but claude pruned its transcript — the caller gets
// stale=true so it can start fresh knowingly instead of resuming blindly.
func TestRecordedSessionStaleWhenTranscriptGone(t *testing.T) {
	fakeHome(t)
	ctx := context.Background()
	wt := t.TempDir()

	recordSession(ctx, wt, RoleDevReview, "sess-gone")
	id, stale := recordedSession(ctx, wt, RoleDevReview)
	if id != "" || !stale {
		t.Errorf("recordedSession = %q (stale=%v), want empty with stale=true", id, stale)
	}
}

// TestRecordedSessionEmptyRoleSkipsFallback pins the one-shot guard: an
// empty role (test-command discovery and every caller that does not chain)
// never resumes, even with a live record on disk.
func TestRecordedSessionEmptyRoleSkipsFallback(t *testing.T) {
	fakeHome(t)
	ctx := context.Background()
	wt := t.TempDir()

	recordSession(ctx, wt, RoleDev, "sess-1")
	plantTranscript(t, wt, "sess-1")
	if id, stale := recordedSession(ctx, wt, ""); id != "" || stale {
		t.Errorf("recordedSession with empty role = %q (stale=%v), want no fallback", id, stale)
	}
}

// TestRecordSessionCollisionResets pins the slug-collision arbitration: a
// record file whose stored worktree differs from the caller's is restarted,
// not merged — two trees' sessions must never mix in one file.
func TestRecordSessionCollisionResets(t *testing.T) {
	fakeHome(t)
	ctx := context.Background()
	wt := t.TempDir()

	path, err := sessionStatePath(wt)
	if err != nil {
		t.Fatalf("sessionStatePath: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path,
		[]byte(`{"worktree":"/somewhere/else","dev":"x","test":"y"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	recordSession(ctx, wt, RoleDev, "sess-mine")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	var st sessionState
	if err := json.Unmarshal(data, &st); err != nil {
		t.Fatalf("parse record: %v", err)
	}
	if st.Worktree != wt || st.Dev != "sess-mine" || st.Test != "" {
		t.Errorf("record after collision = %+v, want a fresh record for %s holding only sess-mine", st, wt)
	}
}

// TestTranscriptIDs pins the snapshot: jsonl transcripts are listed, other
// entries ignored, a missing project dir is a valid empty snapshot (claude
// creates it itself), and an unreadable one disables detection rather than
// risking an old session being mistaken for this round's.
func TestTranscriptIDs(t *testing.T) {
	fakeHome(t)
	wt := t.TempDir()

	ids, ok := transcriptIDs(wt)
	if !ok || len(ids) != 0 {
		t.Errorf("transcriptIDs without a project dir = %v, %v; want an empty snapshot", ids, ok)
	}

	dir, err := claudeProjectDir(wt)
	if err != nil {
		t.Fatalf("claudeProjectDir: %v", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"sess-a.jsonl", "sess-b.jsonl", "notes.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ids, ok = transcriptIDs(wt)
	if !ok || !ids["sess-a"] || !ids["sess-b"] || len(ids) != 2 {
		t.Errorf("transcriptIDs = %v, %v; want exactly sess-a and sess-b", ids, ok)
	}

	t.Setenv("HOME", "")
	if ids, ok = transcriptIDs(t.TempDir()); ok || ids != nil {
		t.Errorf("transcriptIDs without a home = %v, %v; want detection off", ids, ok)
	}
}

// TestRecordNewTranscript pins the recording rule: only a transcript the
// round's snapshot does not already know — the round's own session — gets
// recorded.
func TestRecordNewTranscript(t *testing.T) {
	fakeHome(t)
	ctx := context.Background()
	wt := t.TempDir()
	plantTranscript(t, wt, "sess-old")

	known := map[string]bool{"sess-old": true}
	if recordNewTranscript(ctx, known, wt, RoleDev) {
		t.Error("recordNewTranscript recorded a known transcript")
	}
	if id, _ := recordedSession(ctx, wt, RoleDev); id != "" {
		t.Errorf("record holds %q, want empty", id)
	}

	plantTranscript(t, wt, "sess-new")
	if !recordNewTranscript(ctx, known, wt, RoleDev) {
		t.Error("recordNewTranscript missed the round's new transcript")
	}
	if id, stale := recordedSession(ctx, wt, RoleDev); id != "sess-new" || stale {
		t.Errorf("recorded session = %q (stale=%v), want sess-new live", id, stale)
	}
}

// TestWatchTranscriptsRecordsNewSession pins the polling loop: a transcript
// appearing mid-round is recorded and ends the watch.
func TestWatchTranscriptsRecordsNewSession(t *testing.T) {
	fakeHome(t)
	ctx := context.Background()
	wt := t.TempDir()
	old := transcriptScanInterval
	transcriptScanInterval = 5 * time.Millisecond
	t.Cleanup(func() { transcriptScanInterval = old })

	done := make(chan struct{})
	go func() {
		defer close(done)
		watchTranscripts(ctx, map[string]bool{}, wt, RoleTest, make(chan struct{}))
	}()

	// The transcript lands after the watch is already polling — claude
	// creates it moments after spawn.
	time.Sleep(20 * time.Millisecond)
	plantTranscript(t, wt, "sess-late")

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchTranscripts kept polling after the round's transcript appeared")
	}
	if id, _ := recordedSession(ctx, wt, RoleTest); id != "sess-late" {
		t.Errorf("recorded session = %q, want sess-late", id)
	}
}

// TestWatchTranscriptsStopsWhenClosed pins the round-end exit: a closed
// stop channel ends the watch without recording anything.
func TestWatchTranscriptsStopsWhenClosed(t *testing.T) {
	fakeHome(t)
	ctx := context.Background()
	wt := t.TempDir()
	stop := make(chan struct{})
	close(stop)

	done := make(chan struct{})
	go func() {
		defer close(done)
		watchTranscripts(ctx, map[string]bool{}, wt, RoleTest, stop)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchTranscripts ignored the stop channel")
	}
	if id, _ := recordedSession(ctx, wt, RoleTest); id != "" {
		t.Errorf("record holds %q after a stop with no transcript, want empty", id)
	}
}

// TestBrokenResume pins the fresh-start classification: a plain failure is
// worth one fresh start, while everything that says nothing about the
// conversation — a killed round (including the ceiling kill), an exhausted
// provider, a busy slot, a round that never launched, a cancelled or
// expired context — keeps the round as it is.
func TestBrokenResume(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain failure", errors.New("no conversation found with session ID: sess-1"), true},
		{"killed round", fmt.Errorf("wrap: %w", ErrAgentKilled), false},
		{"api exhausted", fmt.Errorf("wrap: %w", ErrAPIExhausted), false},
		{"slots busy", fmt.Errorf("wrap: %w", ErrAgentSlotsBusy), false},
		{"agent never launched", fmt.Errorf("%w: exec: not found", errAgentStart), false},
		{"cancelled", fmt.Errorf("wrap: %w", context.Canceled), false},
		{"deadline exceeded", context.DeadlineExceeded, false},
	}
	for _, c := range cases {
		if got := brokenResume(c.err); got != c.want {
			t.Errorf("brokenResume(%s: %v) = %v, want %v", c.name, c.err, got, c.want)
		}
	}
}

// plantTranscript fakes claude's own record of a session: the transcript
// file under the worktree's project dir, which the activities watch.
func plantTranscript(t *testing.T, worktree, id string) {
	t.Helper()
	dir, err := claudeProjectDir(worktree)
	if err != nil {
		t.Fatalf("claudeProjectDir: %v", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, id+".jsonl"),
		[]byte("{\"type\":\"system\",\"subtype\":\"init\"}\n"), 0o644); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
}

// shortTranscriptScan makes the transcript watcher poll fast enough for
// tests to observe it; restored on cleanup.
func shortTranscriptScan(t *testing.T) {
	t.Helper()
	old := transcriptScanInterval
	transcriptScanInterval = 20 * time.Millisecond
	t.Cleanup(func() { transcriptScanInterval = old })
}

// TestRunJailedClaudeActivityRecordsKilledRoundSession pins the incident
// this feature exists for: a round killed at its ceiling never returns a
// result carrying a session id, but its conversation is still on record by
// the time the round dies — sourced from claude's own transcript file, not
// from anything the round printed.
func TestRunJailedClaudeActivityRecordsKilledRoundSession(t *testing.T) {
	fakeHome(t)
	wt := t.TempDir()
	slug := pathSlug(wt)
	newStubLog(t)
	shortTranscriptScan(t)
	// The stub plays claude: it creates its transcript at session start,
	// works a while, then dies to a signal mid-round.
	stubBin(t, "ai-jail", `mkdir -p "$HOME/.claude/projects/`+slug+`"
printf '%s\n' '{"type":"system","subtype":"init"}' > "$HOME/.claude/projects/`+slug+`/sess-att-1.jsonl"
sleep 0.3
kill -9 $$`)

	_, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "implement",
		Role:         RoleDev,
	})
	if !errors.Is(err, ErrAgentKilled) {
		t.Fatalf("error = %v, want ErrAgentKilled", err)
	}
	id, stale := recordedSession(context.Background(), wt, RoleDev)
	if id != "sess-att-1" || stale {
		t.Errorf("recorded session = %q (stale=%v), want sess-att-1 live — the killed round's conversation must be resumable", id, stale)
	}
}

// TestRunJailedClaudeActivityResumesRecordedSession pins the retry path: a
// round whose workflow-provided SessionID is empty — the retry after a
// round killed at its ceiling — resumes the recorded conversation instead
// of restarting from zero.
func TestRunJailedClaudeActivityResumesRecordedSession(t *testing.T) {
	fakeHome(t)
	wt := t.TempDir()
	slug := pathSlug(wt)
	log := newStubLog(t)
	shortTranscriptScan(t)
	// Round one: killed mid-flight, but its transcript outlives it.
	stubBin(t, "ai-jail", `mkdir -p "$HOME/.claude/projects/`+slug+`"
printf '%s\n' '{"type":"system","subtype":"init"}' > "$HOME/.claude/projects/`+slug+`/sess-att-1.jsonl"
kill -9 $$`)
	if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "implement",
		Role:         RoleDev,
	}); !errors.Is(err, ErrAgentKilled) {
		t.Fatalf("first round error = %v, want ErrAgentKilled", err)
	}

	// Round two: the retry, with no workflow-provided id.
	stubBin(t, "ai-jail", `printf '%s\n' '{"type":"result","subtype":"success","result":"continued","session_id":"sess-att-2"}'; exit 0`)
	res, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "continue the work",
		Role:         RoleDev,
	})
	if err != nil {
		t.Fatalf("retry round: %v", err)
	}
	if res.SessionID != "sess-att-2" {
		t.Errorf("res.SessionID = %q, want sess-att-2", res.SessionID)
	}

	calls := readCalls(t, log)
	if len(calls) != 2 {
		t.Fatalf("ai-jail called %d times, want 2 (killed round, retry)", len(calls))
	}
	if contains(calls[0].Args, "--resume") {
		t.Errorf("killed round args %v must not resume anything (no id existed yet)", calls[0].Args)
	}
	if !contains(calls[1].Args, "--resume") || !contains(calls[1].Args, "sess-att-1") {
		t.Errorf("retry args %v should resume the recorded session sess-att-1", calls[1].Args)
	}
}

// TestRunJailedClaudeActivityFallbackGuards pins the two guards on the
// recorded-session fallback: an empty role (one-shot rounds) never resumes,
// and an agent without --resume support never resumes.
func TestRunJailedClaudeActivityFallbackGuards(t *testing.T) {
	for _, tc := range []struct {
		name  string
		role  SessionRole
		agent string
	}{
		{"empty role skips the fallback", "", ""},
		{"non-claude agent skips the fallback", RoleDev, "opencode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeHome(t)
			wt := t.TempDir()
			log := newStubLog(t)
			stubBin(t, "ai-jail", "echo OUTPUT; exit 0")
			if tc.agent != "" {
				t.Setenv("DAEDALUS_AGENT", tc.agent)
			}
			// A live record is on disk for the dev conversation.
			recordSession(context.Background(), wt, RoleDev, "sess-1")
			plantTranscript(t, wt, "sess-1")

			if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
				WorktreePath: wt,
				Prompt:       "discover the test command",
				Role:         tc.role,
			}); err != nil {
				t.Fatalf("RunJailedClaudeActivity: %v", err)
			}

			calls := readCalls(t, log)
			if len(calls) != 1 {
				t.Fatalf("ai-jail called %d times, want 1", len(calls))
			}
			if contains(calls[0].Args, "--resume") || contains(calls[0].Args, "sess-1") {
				t.Errorf("%s: round resumed a recorded session: %v", tc.name, calls[0].Args)
			}
		})
	}
}

// TestRunJailedClaudeActivityStaleRecordStartsFresh pins the stale guard: a
// record whose transcript claude pruned is not resumed — and does not fail
// the round; it starts fresh instead.
func TestRunJailedClaudeActivityStaleRecordStartsFresh(t *testing.T) {
	fakeHome(t)
	wt := t.TempDir()
	log := newStubLog(t)
	stubBin(t, "ai-jail", `printf '%s\n' '{"type":"result","subtype":"success","result":"fresh","session_id":"sess-2"}'; exit 0`)
	// On record, but its transcript is gone (claude pruned it).
	recordSession(context.Background(), wt, RoleDev, "sess-gone")

	res, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "retry",
		Role:         RoleDev,
	})
	if err != nil {
		t.Fatalf("stale record must not fail the round, got %v", err)
	}
	if res.Text != "fresh" {
		t.Errorf("res.Text = %q, want the fresh round's output", res.Text)
	}
	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
	}
	if contains(calls[0].Args, "--resume") || contains(calls[0].Args, "sess-gone") {
		t.Errorf("stale record was resumed: %v", calls[0].Args)
	}
}

// TestRunJailedClaudeActivityBrokenResumeRetriesFresh pins the fresh-start
// fallback: when the resumed conversation itself is dead anyway (a miss the
// pre-flight check could not catch), the round retries once without
// --resume instead of failing — and the record is left for the fresh
// round's own transcript to overwrite.
func TestRunJailedClaudeActivityBrokenResumeRetriesFresh(t *testing.T) {
	fakeHome(t)
	wt := t.TempDir()
	log := newStubLog(t)
	stubBin(t, "ai-jail", `for a in "$@"; do
  if [ "$a" = "sess-dead" ]; then
    echo 'claude: no conversation found with session ID sess-dead' >&2
    exit 1
  fi
done
printf '%s\n' '{"type":"result","subtype":"success","result":"fresh ok","session_id":"sess-fresh"}'; exit 0`)
	// Live-looking record: the pre-flight passes, claude rejects the resume.
	recordSession(context.Background(), wt, RoleDev, "sess-dead")
	plantTranscript(t, wt, "sess-dead")

	res, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "retry",
		Role:         RoleDev,
	})
	if err != nil {
		t.Fatalf("broken resume must get one fresh start, got %v", err)
	}
	if res.Text != "fresh ok" {
		t.Errorf("res.Text = %q, want the fresh retry's output", res.Text)
	}

	calls := readCalls(t, log)
	if len(calls) != 2 {
		t.Fatalf("ai-jail called %d times, want 2 (failed resume, fresh retry)", len(calls))
	}
	if !contains(calls[0].Args, "sess-dead") {
		t.Errorf("first attempt args %v should resume sess-dead", calls[0].Args)
	}
	if contains(calls[1].Args, "--resume") || contains(calls[1].Args, "sess-dead") {
		t.Errorf("fresh retry args %v must start clean", calls[1].Args)
	}
	// The record is deliberately untouched: only the fresh round's own
	// transcript overwrites the dead id.
	if id, _ := recordedSession(context.Background(), wt, RoleDev); id != "sess-dead" {
		t.Errorf("record holds %q after the fresh retry, want the dead id left alone", id)
	}
}

// TestRunJailedClaudeActivityKilledResumeNotRetriedFresh pins the exclusion:
// a resumed round killed by a signal is not given a fresh start — the kill
// (including the activity-ceiling kill) says nothing about the conversation,
// and the workflow's timeout accounting depends on the kill staying a kill.
func TestRunJailedClaudeActivityKilledResumeNotRetriedFresh(t *testing.T) {
	fakeHome(t)
	wt := t.TempDir()
	log := newStubLog(t)
	stubBin(t, "ai-jail", `for a in "$@"; do
  if [ "$a" = "sess-live" ]; then kill -9 $$; fi
done
echo SHOULD-NOT-RUN; exit 0`)
	recordSession(context.Background(), wt, RoleDev, "sess-live")
	plantTranscript(t, wt, "sess-live")

	_, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "retry",
		Role:         RoleDev,
	})
	if !errors.Is(err, ErrAgentKilled) {
		t.Fatalf("error = %v, want ErrAgentKilled", err)
	}
	if calls := readCalls(t, log); len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1 — a killed resume must not be retried fresh", len(calls))
	}
}

// TestRunJailedReviewerActivityResumesRecordedSession pins the reviewer
// counterpart: a review with no workflow-provided id resumes the recorded
// conversation for its own role — the long review killed at its ceiling
// used to restart from zero and never deliver a verdict.
func TestRunJailedReviewerActivityResumesRecordedSession(t *testing.T) {
	fakeHome(t)
	wt := t.TempDir()
	log := newStubLog(t)
	stubBin(t, "git", `if [ "$3" = "diff" ]; then printf 'M foo.go\n'; fi
exit 0`)
	stubBin(t, "ai-jail", `printf '%s\n' '{"type":"result","subtype":"success","result":"Fine.\nAPPROVED","session_id":"rev-2"}'; exit 0`)
	// A previous review attempt was cut off at its ceiling, leaving its
	// transcript behind.
	recordSession(context.Background(), wt, RoleTestReview, "rev-cut")
	plantTranscript(t, wt, "rev-cut")

	res, err := RunJailedReviewerActivity(context.Background(), ReviewInput{
		WorktreePath: wt,
		Focus:        "the test suite",
		Role:         RoleTestReview,
	})
	if err != nil {
		t.Fatalf("RunJailedReviewerActivity: %v", err)
	}
	if !res.Approved {
		t.Error("Approved = false, want true")
	}

	calls := readCalls(t, log)
	if len(calls) != 3 {
		t.Fatalf("%d subprocess calls, want 3 (git add, git diff, ai-jail)", len(calls))
	}
	if !contains(calls[2].Args, "--resume") || !contains(calls[2].Args, "rev-cut") {
		t.Errorf("review args %v should resume the recorded reviewer session rev-cut", calls[2].Args)
	}
}

// TestRunJailedReviewerActivityBrokenResumeRetriesFresh pins the reviewer's
// copy of the fresh-start fallback: a rejected resume gets one fresh review
// round instead of failing the review.
func TestRunJailedReviewerActivityBrokenResumeRetriesFresh(t *testing.T) {
	fakeHome(t)
	wt := t.TempDir()
	log := newStubLog(t)
	stubBin(t, "git", `if [ "$3" = "diff" ]; then printf 'M foo.go\n'; fi
exit 0`)
	stubBin(t, "ai-jail", `for a in "$@"; do
  if [ "$a" = "rev-dead" ]; then
    echo 'claude: no conversation found with session ID rev-dead' >&2
    exit 1
  fi
done
printf '%s\n' '{"type":"result","subtype":"success","result":"Looks good.\nAPPROVED","session_id":"rev-fresh"}'; exit 0`)
	recordSession(context.Background(), wt, RoleDevReview, "rev-dead")
	plantTranscript(t, wt, "rev-dead")

	res, err := RunJailedReviewerActivity(context.Background(), ReviewInput{
		WorktreePath: wt,
		Focus:        "the implementation",
		Role:         RoleDevReview,
	})
	if err != nil {
		t.Fatalf("broken resume must get one fresh start, got %v", err)
	}
	if !res.Approved {
		t.Error("Approved = false, want true from the fresh retry")
	}

	calls := readCalls(t, log)
	// 2 failed rounds (git add, git diff, ai-jail) + 1 fresh round's jail
	// call: the fresh round re-runs git too, but only the jail calls carry
	// --resume; count them.
	var jailCalls []stubCall
	for _, c := range calls {
		if len(c.Args) > 0 && c.Args[0] == "--worktree" {
			jailCalls = append(jailCalls, c)
		}
	}
	if len(jailCalls) != 2 {
		t.Fatalf("ai-jail called %d times, want 2 (failed resume, fresh retry)", len(jailCalls))
	}
	if !contains(jailCalls[0].Args, "rev-dead") {
		t.Errorf("first review args %v should resume rev-dead", jailCalls[0].Args)
	}
	if contains(jailCalls[1].Args, "--resume") || contains(jailCalls[1].Args, "rev-dead") {
		t.Errorf("fresh review args %v must start clean", jailCalls[1].Args)
	}
}

// TestCleanupWorktreeActivityRemovesSessionRecord pins the litter control:
// the worktree's session record goes away with the worktree it is keyed to.
func TestCleanupWorktreeActivityRemovesSessionRecord(t *testing.T) {
	home := fakeHome(t)
	newStubLog(t)
	stubBin(t, "git", "exit 0")
	wt := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")
	recordSession(context.Background(), wt, RoleDev, "sess-1")

	if err := CleanupWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-7",
	}); err != nil {
		t.Fatalf("CleanupWorktreeActivity: %v", err)
	}

	path, err := sessionStatePath(wt)
	if err != nil {
		t.Fatalf("sessionStatePath: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("session record survived the worktree's cleanup (stat err = %v)", err)
	}
}

// plantPiSession fakes pi's own record of a session: the session file under
// the worktree's per-directory session dir, which the activities watch. The
// file name follows pi's <timestamp>_<session-id>.jsonl layout.
func plantPiSession(t *testing.T, worktree, id string) {
	t.Helper()
	dir, err := piSessionsDir(worktree)
	if err != nil {
		t.Fatalf("piSessionsDir: %v", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir sessions dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "20260920_000000_"+id+".jsonl"),
		[]byte("{\"type\":\"session\"}\n"), 0o644); err != nil {
		t.Fatalf("write session file: %v", err)
	}
}

// TestPiSessionsDir pins the session-dir encoding: pi's own per-working-
// directory layout — leading separator stripped, '/', '\', and ':' each
// replaced with '-', wrapped in dashes — under the home's sessions root.
// Without a home the dir cannot be resolved.
func TestPiSessionsDir(t *testing.T) {
	home := fakeHome(t)
	dir, err := piSessionsDir("/tmp/a:b/c")
	if err != nil {
		t.Fatalf("piSessionsDir: %v", err)
	}
	want := filepath.Join(home, ".pi", "agent", "sessions", "--tmp-a-b-c--")
	if dir != want {
		t.Errorf("piSessionsDir = %q, want %q", dir, want)
	}

	t.Setenv("HOME", "")
	if _, err := piSessionsDir("/tmp/x"); err == nil {
		t.Error("piSessionsDir without a home = nil error, want one")
	}
}

// TestPiSessionID pins the id extraction from pi's session file names: the
// last underscore-separated field of the base name; non-jsonl entries and
// names without an underscore hold no id.
func TestPiSessionID(t *testing.T) {
	for _, c := range []struct {
		name string
		want string
		ok   bool
	}{
		{"20260920_103000_sess-1.jsonl", "sess-1", true},
		{"a_b_c.jsonl", "c", true},
		{"nounderscore.jsonl", "", false},
		{"notes.txt", "", false},
	} {
		got, ok := piSessionID(c.name)
		if got != c.want || ok != c.ok {
			t.Errorf("piSessionID(%q) = %q, %v; want %q, %v", c.name, got, ok, c.want, c.ok)
		}
	}
}

// TestPiTranscriptIDs pins the pi snapshot: session files are listed by
// their extracted id, other entries ignored, a missing sessions dir is a
// valid empty snapshot (pi creates it itself), and an unreadable one
// disables detection rather than risking an old session being mistaken for
// this round's.
func TestPiTranscriptIDs(t *testing.T) {
	fakeHome(t)
	wt := t.TempDir()

	ids, ok := piTranscriptIDs(wt)
	if !ok || len(ids) != 0 {
		t.Errorf("piTranscriptIDs without a sessions dir = %v, %v; want an empty snapshot", ids, ok)
	}

	plantPiSession(t, wt, "sess-a")
	plantPiSession(t, wt, "sess-b")
	dir, err := piSessionsDir(wt)
	if err != nil {
		t.Fatalf("piSessionsDir: %v", err)
	}
	for _, name := range []string{"notes.txt", "noid.jsonl"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ids, ok = piTranscriptIDs(wt)
	if !ok || !ids["sess-a"] || !ids["sess-b"] || len(ids) != 2 {
		t.Errorf("piTranscriptIDs = %v, %v; want exactly sess-a and sess-b", ids, ok)
	}

	t.Setenv("HOME", "")
	if ids, ok = piTranscriptIDs(t.TempDir()); ok || ids != nil {
		t.Errorf("piTranscriptIDs without a home = %v, %v; want detection off", ids, ok)
	}
}

// TestRecordedPiSession pins the pi fallback: a recorded id reads back live
// while pi holds its session file, other roles get nothing, a pruned file
// reports stale, and the per-agent dispatch routes pi to pi's session dir —
// not claude's transcripts.
func TestRecordedPiSession(t *testing.T) {
	fakeHome(t)
	ctx := context.Background()
	wt := t.TempDir()

	recordSession(ctx, wt, RoleDev, "sess-pi")
	plantPiSession(t, wt, "sess-pi")

	id, stale := recordedPiSession(ctx, wt, RoleDev)
	if id != "sess-pi" || stale {
		t.Fatalf("recordedPiSession = %q (stale=%v), want sess-pi live", id, stale)
	}
	if id, _ := recordedPiSession(ctx, wt, RoleTest); id != "" {
		t.Errorf("role %s resumed %q; one role's session must not serve another", RoleTest, id)
	}

	if id, stale := recordedAgentSession(ctx, "pi", wt, RoleDev); id != "sess-pi" || stale {
		t.Errorf("recordedAgentSession(pi) = %q (stale=%v), want sess-pi live", id, stale)
	}
	// Claude's dispatch looks in claude's own transcripts, where this
	// session does not exist — the record reads stale, never resumable.
	if id, stale := recordedAgentSession(ctx, "claude", wt, RoleDev); id != "" || !stale {
		t.Errorf("recordedAgentSession(claude) = %q (stale=%v), want empty and stale", id, stale)
	}

	if err := os.Remove(func() string {
		dir, err := piSessionsDir(wt)
		if err != nil {
			t.Fatalf("piSessionsDir: %v", err)
		}
		return filepath.Join(dir, "20260920_000000_sess-pi.jsonl")
	}()); err != nil {
		t.Fatal(err)
	}
	if id, stale := recordedPiSession(ctx, wt, RoleDev); id != "" || !stale {
		t.Errorf("recordedPiSession after pruning = %q (stale=%v), want empty with stale=true", id, stale)
	}
}

// TestRecordNewPiTranscript pins the pi recording rule: only a session file
// the round's snapshot does not already know — the round's own session —
// gets recorded.
func TestRecordNewPiTranscript(t *testing.T) {
	fakeHome(t)
	ctx := context.Background()
	wt := t.TempDir()
	plantPiSession(t, wt, "sess-old")

	known := map[string]bool{"sess-old": true}
	if recordNewPiTranscript(ctx, known, wt, RoleDev) {
		t.Error("recordNewPiTranscript recorded a known session file")
	}
	if id, _ := recordedPiSession(ctx, wt, RoleDev); id != "" {
		t.Errorf("record holds %q, want empty", id)
	}

	plantPiSession(t, wt, "sess-new")
	if !recordNewPiTranscript(ctx, known, wt, RoleDev) {
		t.Error("recordNewPiTranscript missed the round's new session file")
	}
	if id, stale := recordedPiSession(ctx, wt, RoleDev); id != "sess-new" || stale {
		t.Errorf("recorded session = %q (stale=%v), want sess-new live", id, stale)
	}
}

// TestRunJailedClaudeActivityPiRecordsKilledRoundSessionAndResumes pins the
// pi path end to end: a killed pi round's conversation is on record from
// pi's own session dir by the time the round dies, and the retry — with no
// workflow-provided id — resumes it via pi's --session flag, not claude's
// --resume.
func TestRunJailedClaudeActivityPiRecordsKilledRoundSessionAndResumes(t *testing.T) {
	fakeHome(t)
	wt := t.TempDir()
	sessionsDir, err := piSessionsDir(wt)
	if err != nil {
		t.Fatalf("piSessionsDir: %v", err)
	}
	log := newStubLog(t)
	shortTranscriptScan(t)
	t.Setenv("DAEDALUS_AGENT", "pi")
	// Round one: killed mid-flight, but pi's session file outlives it.
	stubBin(t, "ai-jail", `mkdir -p '`+sessionsDir+`'
printf '%s\n' '{"type":"session","id":"sess-pi-1"}' > '`+sessionsDir+`/20260920_000000_sess-pi-1.jsonl'
kill -9 $$`)
	if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "implement",
		Role:         RoleDev,
	}); !errors.Is(err, ErrAgentKilled) {
		t.Fatalf("first round error = %v, want ErrAgentKilled", err)
	}
	if id, stale := recordedPiSession(context.Background(), wt, RoleDev); id != "sess-pi-1" || stale {
		t.Fatalf("recorded session = %q (stale=%v), want sess-pi-1 live — the killed round's conversation must be resumable", id, stale)
	}

	// Round two: the retry, with no workflow-provided id.
	stubBin(t, "ai-jail", `printf '%s\n' `+
		`'{"type":"session","id":"sess-pi-2"}' `+
		`'{"type":"message_end","message":{"content":[{"type":"text","text":"continued"}]}}'
printf '%s\n' '{"type":"session"}' > '`+sessionsDir+`/20260920_000001_sess-pi-2.jsonl'
exit 0`)
	res, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "continue the work",
		Role:         RoleDev,
	})
	if err != nil {
		t.Fatalf("retry round: %v", err)
	}
	if res.SessionID != "sess-pi-2" {
		t.Errorf("res.SessionID = %q, want sess-pi-2", res.SessionID)
	}
	if res.Text != "continued" {
		t.Errorf("res.Text = %q, want %q", res.Text, "continued")
	}

	calls := readCalls(t, log)
	if len(calls) != 2 {
		t.Fatalf("ai-jail called %d times, want 2 (killed round, retry)", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{
		"--worktree",
		"--network",
		"--mask",
		".claude/settings.json",
		"--mask",
		".claude/settings.local.json",
		"--",
		"pi",
		"--mode",
		"json",
		"-p",
	}, "killed pi round")
	if !contains(calls[1].Args, "--session") || !contains(calls[1].Args, "sess-pi-1") {
		t.Errorf("retry args %v should resume the recorded session sess-pi-1 via --session", calls[1].Args)
	}
	if contains(calls[1].Args, "--resume") {
		t.Errorf("retry args %v must not use claude's --resume", calls[1].Args)
	}
}
