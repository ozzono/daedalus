package activities

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.temporal.io/sdk/log"

	"github.com/ozzono/daedalus/internal/config"
)

// stubCall records one invocation of a stubbed binary: its working directory,
// the arguments it was called with, what it received on stdin, and select
// environment variables.
type stubCall struct {
	Cwd   string
	Args  []string
	Stdin string
	Env   map[string]string
}

// newStubLog creates a fresh invocation log and points $STUB_LOG at it.
func newStubLog(t *testing.T) string {
	t.Helper()
	log := filepath.Join(t.TempDir(), "stub.log")
	t.Setenv("STUB_LOG", log)
	return log
}

// stubBin installs an executable shell script named name earlier on PATH.
// Each invocation appends "=== CALL ===", "CWD=<pwd>", one "ARG:<arg>" line
// per argument (newlines escaped as \x1e, so multi-line arguments like agent
// prompts survive the line-based log), one "STDIN:<data>" line (same
// escaping), and one "ENV:<key>=<value>" line per interesting variable to
// $STUB_LOG, then runs the extra shell body.
func stubBin(t *testing.T, name, body string) {
	t.Helper()
	dir := t.TempDir()
	script := `#!/bin/sh
echo "=== CALL ===" >> "$STUB_LOG"
echo "CWD=$(pwd)" >> "$STUB_LOG"
for a in "$@"; do
  printf 'ARG:' >> "$STUB_LOG"
  printf '%s' "$a" | tr '\n' '\036' >> "$STUB_LOG"
  printf '\n' >> "$STUB_LOG"
done
printf 'STDIN:' >> "$STUB_LOG"
cat | tr '\n' '\036' >> "$STUB_LOG"
printf '\n' >> "$STUB_LOG"
echo "ENV:ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY" >> "$STUB_LOG"
echo "ENV:ANTHROPIC_BASE_URL=$ANTHROPIC_BASE_URL" >> "$STUB_LOG"
echo "ENV:ANTHROPIC_MODEL=$ANTHROPIC_MODEL" >> "$STUB_LOG"
echo "ENV:ANTHROPIC_DEFAULT_HAIKU_MODEL=$ANTHROPIC_DEFAULT_HAIKU_MODEL" >> "$STUB_LOG"
echo "ENV:AMP_API_KEY=$AMP_API_KEY" >> "$STUB_LOG"
` + body + "\n"
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write stub %s: %v", name, err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// readCalls parses a stub invocation log written by stubBin.
func readCalls(t *testing.T, log string) []stubCall {
	t.Helper()
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatalf("read stub log: %v", err)
	}
	var calls []stubCall
	for line := range strings.SplitSeq(string(data), "\n") {
		switch {
		case line == "=== CALL ===":
			calls = append(calls, stubCall{Env: map[string]string{}})
		case strings.HasPrefix(line, "CWD="):
			if n := len(calls); n > 0 {
				calls[n-1].Cwd = strings.TrimPrefix(line, "CWD=")
			}
		case strings.HasPrefix(line, "ARG:"):
			if n := len(calls); n > 0 {
				arg := strings.TrimPrefix(line, "ARG:")
				calls[n-1].Args = append(calls[n-1].Args, strings.ReplaceAll(arg, "\x1e", "\n"))
			}
		case strings.HasPrefix(line, "STDIN:"):
			if n := len(calls); n > 0 {
				calls[n-1].Stdin = strings.ReplaceAll(strings.TrimPrefix(line, "STDIN:"), "\x1e", "\n")
			}
		case strings.HasPrefix(line, "ENV:"):
			if n := len(calls); n > 0 {
				kv := strings.SplitN(strings.TrimPrefix(line, "ENV:"), "=", 2)
				calls[n-1].Env[kv[0]] = kv[1]
			}
		}
	}
	return calls
}

// TestSetProcessGroup pins the child-process containment: own process group
// (so a cancel kills the whole tree, not just the direct child), a Cancel
// hook, and a WaitDelay bound (so a descendant that escaped the group with
// the output pipes cannot hang Wait forever).
func TestSetProcessGroup(t *testing.T) {
	cmd := exec.CommandContext(context.Background(), "true")
	setProcessGroup(cmd)
	if !cmd.SysProcAttr.Setpgid {
		t.Error("Setpgid = false, want the child in its own process group")
	}
	if cmd.Cancel == nil {
		t.Error("Cancel = nil, want a group-kill cancel hook")
	}
	if cmd.WaitDelay != pipeDrainDelay {
		t.Errorf("WaitDelay = %v, want %v", cmd.WaitDelay, pipeDrainDelay)
	}
}

// TestRunJailedSweepsDescendantsAfterCleanExit pins the post-round sweep: a
// signalsPermitted reports whether this process may signal its own children.
// Sandboxed environments may forbid kill(2) outright (EPERM on signal-0
// probes of fresh children), which makes any group-sweep verification
// impossible — the sweep's own SIGKILL bounces off the same restriction.
func signalsPermitted() bool {
	// Short-lived even when the cleanup kill bounces (signal-forbidding
	// sandboxes deny it too), so a skip costs a second, not the child's
	// full lifetime.
	cmd := exec.Command("sleep", "1")
	if err := cmd.Start(); err != nil {
		return false
	}
	defer func() {
		_ = syscall.Kill(cmd.Process.Pid, syscall.SIGKILL)
		_, _ = cmd.Process.Wait()
	}()
	return syscall.Kill(cmd.Process.Pid, 0) == nil
}

// TestRunJailedSweepsDescendantsAfterCleanExit pins the post-round sweep: a
// child that exits cleanly while leaving a same-group descendant running
// (a crashed jail, a test runner that backgrounded workers) must not orphan
// it — the round kills the whole group once the child is gone. The
// descendant inherits and holds the output pipes, so this also pins that
// the round still succeeds (ErrWaitDelay is a success) with the output
// printed before the child exited. Liveness is probed via a heartbeat file
// the descendant appends to, not kill(pid, 0) — signal permissions vary
// between environments, file growth does not.
func TestRunJailedSweepsDescendantsAfterCleanExit(t *testing.T) {
	if !signalsPermitted() {
		t.Skip("environment forbids signals; the group sweep cannot be verified here")
	}
	newStubLog(t)
	beat := os.Getenv("STUB_LOG") + ".beat"
	// The stub backgrounds a heartbeat loop (same process group, outlives
	// the stub's clean exit, holds the pipes open) and exits 0.
	stubBin(t, "ai-jail", `while true; do echo x >> "`+beat+`"; sleep 0.1; done &
echo done; exit 0`)

	res, err := runJailed(context.Background(), "", t.TempDir(), "do things")
	if err != nil {
		t.Fatalf("runJailed: %v", err)
	}
	if res.Stdout != "done\n" {
		t.Errorf("Stdout = %q, want the child's pre-exit output despite the pipe-holding descendant", res.Stdout)
	}

	size := func() int64 {
		info, err := os.Stat(beat)
		if err != nil {
			t.Fatalf("stat heartbeat file: %v", err)
		}
		return info.Size()
	}
	// killGroup runs before runJailed returns; give the dead loop a moment
	// to prove it stays dead, then require the heartbeat to have stopped.
	before := size()
	time.Sleep(1 * time.Second)
	if after := size(); after != before {
		t.Errorf("descendant heartbeat still growing after the round ended (%d -> %d bytes): the group sweep missed it",
			before, after)
	}
}

func fakeHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	return home
}

// samePath compares two paths up to symlinks: macOS temp directories are
// reachable as both /var/folders/... and /private/var/folders/..., and a
// subprocess may report either form as its working directory.
func samePath(a, b string) bool {
	ra, erra := filepath.EvalSymlinks(a)
	rb, errb := filepath.EvalSymlinks(b)
	if erra != nil || errb != nil {
		return a == b
	}
	return ra == rb
}

// assertArgs reports a strict element-by-element comparison of a recorded
// call's arguments.
func assertArgs(t *testing.T, got, want []string, context string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s args = %v, want %v", context, got, want)
	}
}

// contains reports whether any argument in args contains substr.
func contains(args []string, substr string) bool {
	for _, a := range args {
		if strings.Contains(a, substr) {
			return true
		}
	}
	return false
}

func TestWorktreePathFor(t *testing.T) {
	home := fakeHome(t)

	got, err := WorktreePathFor("daedalus", "42")
	if err != nil {
		t.Fatalf("WorktreePathFor: %v", err)
	}
	if want := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42"); got != want {
		t.Errorf("WorktreePathFor(daedalus, 42) = %q, want %q", got, want)
	}
}

// TestWorktreePathForRejectsBadSegments guards against path traversal: both
// segments become filesystem components and are later passed to os.RemoveAll.
func TestWorktreePathForRejectsBadSegments(t *testing.T) {
	fakeHome(t)
	for _, bad := range []string{"..", ".", "a/b", `a\b`, ""} {
		if _, err := WorktreePathFor("daedalus", bad); err == nil {
			t.Errorf("issue id %q should be rejected", bad)
		}
		if _, err := WorktreePathFor(bad, "42"); err == nil {
			t.Errorf("task queue %q should be rejected", bad)
		}
	}
}

// TestPreflightWorktreeRoot verifies the worker-startup probe creates the
// worktree root and leaves no probe file behind.
func TestPreflightWorktreeRoot(t *testing.T) {
	home := fakeHome(t)

	if err := PreflightWorktreeRoot("daedalus"); err != nil {
		t.Fatalf("PreflightWorktreeRoot: %v", err)
	}
	root := filepath.Join(home, ".daedalus", "worktrees", "daedalus")
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		t.Errorf("worktree root %s missing after preflight (err=%v)", root, err)
	}
	if _, err := os.Stat(filepath.Join(root, ".write-probe")); err == nil {
		t.Errorf("probe file left behind in %s", root)
	}
}

// TestPreflightWorktreeRootRejectsBadQueue mirrors the path-traversal guard
// on task queue segments.
func TestPreflightWorktreeRootRejectsBadQueue(t *testing.T) {
	fakeHome(t)
	if err := PreflightWorktreeRoot(".."); err == nil {
		t.Error("task queue .. should be rejected")
	}
}

func TestCreateWorktreeActivity(t *testing.T) {
	home := fakeHome(t)
	log := newStubLog(t)
	stubBin(t, "git", "exit 0")

	worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(worktreePath, "stale.txt")
	if err := os.WriteFile(stale, []byte("leftover"), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err := CreateWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-123",
	})
	if err != nil {
		t.Fatalf("CreateWorktreeActivity: %v", err)
	}
	if out.WorktreePath != worktreePath {
		t.Errorf("WorktreePath = %q, want %q", out.WorktreePath, worktreePath)
	}

	// The stale directory must be gone before git worktree add ran.
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale worktree dir still present (stat err = %v)", err)
	}

	calls := readCalls(t, log)
	if len(calls) != 9 {
		t.Fatalf("git called %d times, want 9 (preserve list, add, commit, drop aborted, rename, remove, prune, stale sweep, worktree add)", len(calls))
	}
	// A stale worktree from a crashed run is preserved, not discarded: its
	// contents are committed and its branch renamed to aborted/issue-42.
	assertArgs(t, calls[0].Args, []string{"-C", "/repo", "branch", "--list", "daedalus/issue-42-*", "--format=%(refname:short)"}, "finalized check")
	assertArgs(t, calls[1].Args, []string{"-C", worktreePath, "add", "-A"}, "preserve stage")
	assertArgs(t, calls[2].Args, []string{"-C", worktreePath,
		"-c", "user.name=daedalus", "-c", "user.email=daedalus@local",
		"commit", "-m", "daedalus: run closed without approval, work preserved for continue"}, "preserve commit")
	assertArgs(t, calls[3].Args, []string{"-C", "/repo", "branch", "-D", "aborted/issue-42"}, "drop previous aborted")
	assertArgs(t, calls[4].Args, []string{"-C", worktreePath, "branch", "-m", "aborted/issue-42"}, "preserve rename")
	assertArgs(t, calls[5].Args, []string{"-C", "/repo", "worktree", "remove", worktreePath, "--force"}, "pre-clean remove")
	assertArgs(t, calls[6].Args, []string{"-C", "/repo", "worktree", "prune"}, "pre-clean prune")
	assertArgs(t, calls[7].Args, []string{"-C", "/repo", "branch", "--list", "feat/issue-42-*", "--format=%(refname:short)"}, "pre-clean stale branch sweep")
	assertArgs(t, calls[8].Args, []string{"-C", "/repo", "worktree", "add", worktreePath, "-b", "feat/issue-42-123"}, "worktree add")
}

// TestCreateWorktreeFromAbortedBase pins the continued-run path: the new
// worktree branches from the preserved aborted/ branch, which is then
// deleted — its commits live on the new in-flight branch.
func TestCreateWorktreeFromAbortedBase(t *testing.T) {
	home := fakeHome(t)
	log := newStubLog(t)
	stubBin(t, "git", "exit 0")
	worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")

	if _, err := CreateWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-123",
		BaseBranch: "aborted/issue-42",
	}); err != nil {
		t.Fatalf("CreateWorktreeActivity: %v", err)
	}

	calls := readCalls(t, log)
	if len(calls) != 6 {
		t.Fatalf("git called %d times, want 6 (preserve list, remove, prune, stale sweep, add, drop base)", len(calls))
	}
	assertArgs(t, calls[4].Args, []string{"-C", "/repo", "worktree", "add", worktreePath, "-b", "feat/issue-42-123", "aborted/issue-42"}, "worktree add from base")
	assertArgs(t, calls[5].Args, []string{"-C", "/repo", "branch", "-D", "aborted/issue-42"}, "drop consumed base")
}

func TestCreateWorktreeActivityHealsStaleRegistration(t *testing.T) {
	home := fakeHome(t)
	log := newStubLog(t)
	// worktree remove fails ("not a working tree") but prune and add succeed.
	// argv is: -C <repo> worktree <subcommand> ... — the subcommand is $4.
	stubBin(t, "git", `if [ "$4" = "remove" ]; then echo "fatal: not a working tree" >&2; exit 1; fi
exit 0`)

	worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatal(err)
	}

	out, err := CreateWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-123",
	})
	if err != nil {
		t.Fatalf("CreateWorktreeActivity must tolerate stale registration: %v", err)
	}
	if out.WorktreePath != worktreePath {
		t.Errorf("WorktreePath = %q, want %q", out.WorktreePath, worktreePath)
	}

	calls := readCalls(t, log)
	if len(calls) != 9 {
		t.Fatalf("git called %d times, want 9", len(calls))
	}
	if _, err := os.Stat(worktreePath); !os.IsNotExist(err) {
		t.Errorf("stale directory should be removed even when git remove failed (stat err = %v)", err)
	}
}

func TestCreateWorktreeActivityGitFailure(t *testing.T) {
	fakeHome(t)
	newStubLog(t)
	stubBin(t, "git", `if [ "$4" = "add" ]; then echo 'fatal: bad repo' >&2; exit 3; fi
exit 0`)

	_, err := CreateWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42",
	})
	if err == nil {
		t.Fatal("want error when git worktree add fails")
	}
	if !strings.Contains(err.Error(), "git worktree add") {
		t.Errorf("error %q should mention the failing command", err)
	}
}

// cleanWorktreeSweepOutput shows what deleteStaleBranches does with listed
// stale branches: each is deleted. The daedalus/ finalized check lists empty
// and the worktree is absent, so nothing is preserved.
func TestCleanupWorktreeActivitySweepsStaleBranches(t *testing.T) {
	fakeHome(t)
	log := newStubLog(t)
	// branch --list reports two stale in-flight branches from crashed runs;
	// argv here is: -C repo branch --list <pattern> — the pattern is $5.
	stubBin(t, "git", `case "$5" in
  feat/*) printf 'feat/issue-42-111\nfeat/issue-42-222\n'; exit 0 ;;
esac
exit 0`)

	err := CleanupWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-7",
	})
	if err != nil {
		t.Fatalf("CleanupWorktreeActivity: %v", err)
	}

	calls := readCalls(t, log)
	// finalized check, remove, prune, list, -D, -D
	if len(calls) != 6 {
		t.Fatalf("git called %d times, want 6 (finalized check, remove, prune, list, 2 stale deletes)", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{"-C", "/repo", "branch", "--list", "daedalus/issue-42-*", "--format=%(refname:short)"}, "finalized check")
	assertArgs(t, calls[3].Args, []string{"-C", "/repo", "branch", "--list", "feat/issue-42-*", "--format=%(refname:short)"}, "stale branch sweep")
	assertArgs(t, calls[4].Args, []string{"-C", "/repo", "branch", "-D", "feat/issue-42-111"}, "stale branch delete 1")
	assertArgs(t, calls[5].Args, []string{"-C", "/repo", "branch", "-D", "feat/issue-42-222"}, "stale branch delete 2")
}

// TestCleanupPreservesAbortedWork pins the preservation contract: without a
// finalized daedalus/ branch, the worktree's contents are committed and the
// in-flight branch renamed to aborted/issue-42 for `daedalus continue`.
func TestCleanupPreservesAbortedWork(t *testing.T) {
	home := fakeHome(t)
	log := newStubLog(t)
	stubBin(t, "git", "exit 0")
	worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := CleanupWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-7",
	}); err != nil {
		t.Fatalf("CleanupWorktreeActivity: %v", err)
	}

	calls := readCalls(t, log)
	if len(calls) != 8 {
		t.Fatalf("git called %d times, want 8 (list, add, commit, drop aborted, rename, remove, prune, sweep)", len(calls))
	}
	assertArgs(t, calls[1].Args, []string{"-C", worktreePath, "add", "-A"}, "preserve stage")
	if !contains(calls[2].Args, "work preserved for continue") {
		t.Errorf("commit %v should carry the preservation message", calls[2].Args)
	}
	assertArgs(t, calls[3].Args, []string{"-C", "/repo", "branch", "-D", "aborted/issue-42"}, "drop previous aborted")
	assertArgs(t, calls[4].Args, []string{"-C", worktreePath, "branch", "-m", "aborted/issue-42"}, "preserve rename")
	if _, err := os.Stat(worktreePath); !os.IsNotExist(err) {
		t.Errorf("worktree dir should be removed after preservation (stat err = %v)", err)
	}
}

// TestRunJailedAPIExhaustion pins the halt labeling: when the agent CLI
// fails with quota/rate-limit output, the error names ErrAPIExhausted so the
// failure reads as "resume later" rather than a code bug.
func TestRunJailedAPIExhaustion(t *testing.T) {
	newStubLog(t)
	stubBin(t, "ai-jail", "echo 'credit balance too low' >&2; exit 1")

	_, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "do things",
	})
	if err == nil {
		t.Fatal("want error when the agent api is exhausted")
	}
	if !errors.Is(err, ErrAPIExhausted) {
		t.Errorf("error %v should wrap ErrAPIExhausted", err)
	}
	if !strings.Contains(err.Error(), "credit balance too low") {
		t.Errorf("error %q should keep the provider's message", err)
	}
}

// TestRunJailedKilledClassifiedByWaitStatus pins that a process death by
// signal is classified from the wait status, ahead of any output-text
// marker: a killed round that printed exhaustion phrases mid-run must not
// park the run in the quota heartbeat, and the classified error must carry
// no output — agent text must not be able to forge or bury the verdict.
func TestRunJailedKilledClassifiedByWaitStatus(t *testing.T) {
	newStubLog(t)
	stubBin(t, "ai-jail", `echo 'usage limit reached, 429'; kill -9 $$`)

	_, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "do things",
	})
	if err == nil {
		t.Fatal("want error when the agent is killed")
	}
	if !errors.Is(err, ErrAgentKilled) {
		t.Errorf("error %v should wrap ErrAgentKilled", err)
	}
	if errors.Is(err, ErrAPIExhausted) {
		t.Errorf("signal death must outrank output-text exhaustion markers: %v", err)
	}
	if strings.Contains(err.Error(), "usage limit reached") {
		t.Errorf("classified kill error must not embed agent output: %q", err)
	}
}

// TestRunJailedKillMarkerInOutputNotForged is the flip side: a round that
// merely *prints* the kill sentinel's text and fails normally must not be
// classified as killed — the classification comes from the wait status,
// never from output.
func TestRunJailedKillMarkerInOutputNotForged(t *testing.T) {
	newStubLog(t)
	stubBin(t, "ai-jail", "echo '"+ErrAgentKilled.Error()+" (quoted in a log)'; exit 1")

	_, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "do things",
	})
	if err == nil {
		t.Fatal("want error from the failing agent")
	}
	if errors.Is(err, ErrAgentKilled) {
		t.Errorf("output text must not forge a kill classification: %v", err)
	}
	if !strings.Contains(err.Error(), ErrAgentKilled.Error()+" (quoted in a log)") {
		t.Errorf("generic failure should still carry the output: %q", err)
	}
}

// TestReferencesPath pins the straggler matcher's boundary rule: the
// per-issue worktree path matches itself, its files, and argument
// boundaries — never a longer sibling (issue-4 vs issue-42) or a word that
// merely contains the path as a substring.
func TestReferencesPath(t *testing.T) {
	const wt = "/wt/arete/issue-4"
	cases := []struct {
		argv string
		want bool
	}{
		{wt, true},
		{"vim " + wt, true},
		{"vim " + wt + "/sub/dir/file.go", true},
		{"tail -f " + wt + "/log", true},
		{`grep "` + wt + `" -r .`, true},
		{"/wt/arete/issue-42", false},              // sibling: longer id
		{"/wt/arete/issue-42/sub", false},          // sibling's file
		{"vim /wt/arete/issue-4xyz", false},        // path inside a longer word
		{"echo /wt/arete/issue-4-and-more", false}, // path prefix of a longer token
		{"flutter test", false},                    // no reference at all
		{"/backup/copy-of" + wt, true},             // pinned as-is: the matcher has an end boundary but
		// no start boundary, so a path merely *ending* with the worktree
		// string (a copy filed under another root) still matches —
		// contrived given home-anchored worktree paths, but that is the
		// current semantics, not an accident of this table.
		{"", false},
	}
	for _, c := range cases {
		if got := referencesPath(c.argv, wt); got != c.want {
			t.Errorf("referencesPath(%q, %q) = %v, want %v", c.argv, wt, got, c.want)
		}
	}
}

// TestCleanupPreserveRecoversFromStaleIndexLock pins that a stale
// index.lock — the state a killed round's git leaves behind — is swept and
// staging retried, instead of wedging every later cleanup of the issue.
func TestCleanupPreserveRecoversFromStaleIndexLock(t *testing.T) {
	home := fakeHome(t)
	log := newStubLog(t)
	worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(t.TempDir(), "index.lock")
	if err := os.WriteFile(lock, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	counter := filepath.Join(t.TempDir(), "adds")
	// argv: -C <wt> <sub> …; add fails exactly once (the lock), then the
	// retry after the sweep succeeds. rev-parse hands back the lock's
	// path so the sweep can remove it.
	stubBin(t, "git", `case "$3" in
  rev-parse) printf '%s\n' "`+lock+`"; exit 0 ;;
  add) n=$(cat "`+counter+`" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "`+counter+`"; [ "$n" -gt 1 ] && exit 0; exit 1 ;;
esac
exit 0`)

	if err := CleanupWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-7",
	}); err != nil {
		t.Fatalf("CleanupWorktreeActivity: %v", err)
	}

	var adds int
	for _, c := range readCalls(t, log) {
		if len(c.Args) >= 4 && c.Args[2] == "add" {
			adds++
		}
	}
	if adds != 2 {
		t.Fatalf("git add called %d times, want 2 (locked, then retried after sweep)", adds)
	}
	if _, err := os.Stat(lock); !os.IsNotExist(err) {
		t.Errorf("stale index.lock should be removed (stat err = %v)", err)
	}
}

// TestCleanupPreserveRenameFallback pins that a rename failure with
// nothing at stake (detached HEAD, or the old aborted ref survived its
// best-effort delete) falls back to pointing the aborted ref at HEAD — the
// commit holds the work — instead of wedging the issue's cleanup.
func TestCleanupPreserveRenameFallback(t *testing.T) {
	home := fakeHome(t)
	newStubLog(t)
	worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatal(err)
	}
	// argv: -C <path> branch <flag> …; every rename (-m) fails, forcing
	// the -f fallback.
	stubBin(t, "git", `case "$3" in
  branch) [ "$4" = "-m" ] && exit 1 ;;
esac
exit 0`)

	if err := CleanupWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-7",
	}); err != nil {
		t.Fatalf("CleanupWorktreeActivity: %v", err)
	}
}

// TestCleanupPreserveFailureAbortsBeforeDeletion pins the ordering
// guarantee "deletion must never outrun preservation": a staging failure
// (retry already exhausted, no stale lock to sweep) or a commit failure
// that is not a clean "nothing to commit" tree aborts the cleanup before
// any removal step — no `worktree remove`, no prune, and the worktree
// directory itself is left on disk — so unsaved work survives for the next
// run instead of being deleted.
func TestCleanupPreserveFailureAbortsBeforeDeletion(t *testing.T) {
	cases := []struct {
		name    string
		gitStub string
		wantErr string
	}{
		{
			name: "stage failure",
			// add fails and rev-parse reports no lock (empty output), so
			// there is nothing to sweep and no retry: a genuine failure.
			gitStub: `case "$3" in
  add) exit 1 ;;
esac
exit 0`,
			wantErr: "preserve aborted work (stage)",
		},
		{
			name: "commit failure",
			// The commit fails with output that is not "nothing to
			// commit": the work was not saved. (The commit subcommand sits
			// at $7, behind the two -c identity flags.)
			gitStub: `case "$3" in
  -c) [ "$7" = commit ] && { echo 'error: unable to write object'; exit 1; } ;;
esac
exit 0`,
			wantErr: "preserve aborted work (commit)",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			home := fakeHome(t)
			log := newStubLog(t)
			worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")
			if err := os.MkdirAll(worktreePath, 0o755); err != nil {
				t.Fatal(err)
			}
			stubBin(t, "git", c.gitStub)

			err := CleanupWorktreeActivity(context.Background(), WorktreeInput{
				RepoPath:   "/repo",
				TaskQueue:  "daedalus",
				IssueID:    "42",
				BranchName: "feat/issue-42-7",
			})
			if err == nil {
				t.Fatal("want cleanup to abort when preservation fails")
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("error %q should name the failed preserve step %q", err, c.wantErr)
			}
			for _, call := range readCalls(t, log) {
				if len(call.Args) >= 3 && call.Args[2] == "worktree" {
					t.Errorf("removal ran despite failed preservation: %v", call.Args)
				}
			}
			if _, err := os.Stat(worktreePath); err != nil {
				t.Errorf("worktree dir must survive a failed preservation (stat err = %v)", err)
			}
		})
	}
}

// TestCleanupPreserveToleratesNothingToCommit pins the one commit failure
// that is not an abort: a clean tree ("nothing to commit") holds no work to
// lose, so preservation proceeds — the rename still runs and the cleanup
// completes, removing the worktree directory.
func TestCleanupPreserveToleratesNothingToCommit(t *testing.T) {
	home := fakeHome(t)
	log := newStubLog(t)
	// The commit subcommand sits at $7, behind the two -c identity flags.
	stubBin(t, "git", `case "$3" in
  -c) [ "$7" = commit ] && { echo 'nothing to commit, working tree clean'; exit 1; } ;;
esac
exit 0`)
	worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := CleanupWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-7",
	}); err != nil {
		t.Fatalf("CleanupWorktreeActivity: %v", err)
	}
	var renamed bool
	for _, call := range readCalls(t, log) {
		if contains(call.Args, "aborted/issue-42") && contains(call.Args, "-m") {
			renamed = true
		}
	}
	if !renamed {
		t.Errorf("clean tree should still be renamed to the aborted branch")
	}
	if _, err := os.Stat(worktreePath); !os.IsNotExist(err) {
		t.Errorf("worktree dir should be removed after cleanup (stat err = %v)", err)
	}
}

// TestStragglersInClassifiesTableEntries pins which processes the sweep
// targets: of a stubbed ps table, exactly those whose command line
// references the worktree at a path boundary — never a longer sibling's
// path, a longer word around the path, or processes elsewhere entirely.
func TestStragglersInClassifiesTableEntries(t *testing.T) {
	newStubLog(t)
	wt := t.TempDir()
	stubBin(t, "ps", `printf '%s\n' \
" 111 /bin/sh `+wt+`/repo/tool --watch" \
" 112 `+wt+`/repo/tool" \
" 12345 tail -f `+wt+`2/build.log" \
" 12346 vim /backup/copy`+wt+`x" \
" 12347 make -C /elsewhere" \
" not-a-pid command `+wt+`"`)

	lines, pids := stragglersIn(log.NewStructuredLogger(
		slog.New(slog.NewTextHandler(io.Discard, nil))), wt)

	if len(pids) != 2 || pids[0] != 111 || pids[1] != 112 {
		t.Errorf("pids = %v, want [111 112] (the watcher and the bare worktree command)", pids)
	}
	if len(lines) != len(pids) {
		t.Errorf("lines (%d) and pids (%d) should stay paired", len(lines), len(pids))
	}
}

// TestKillWorktreeStragglersSweepsReferencingProcesses pins the sweep's
// observable effect: a live process whose command line references the
// worktree path is SIGKILLed — a real one, listed by a stubbed ps table
// (the real ps is not usable from every environment the tests run in).
func TestKillWorktreeStragglersSweepsReferencingProcesses(t *testing.T) {
	if !signalsPermitted() {
		t.Skip("environment forbids signals; the sweep's kill cannot be verified here")
	}
	newStubLog(t)
	wt := t.TempDir()
	// A real, disposable process for the sweep to kill.
	straggler := exec.Command("/bin/sleep", "60")
	if err := straggler.Start(); err != nil {
		t.Fatal(err)
	}
	pid := straggler.Process.Pid
	// Death is observed through the wait status, not kill(pid, 0): an
	// unreaped child stays a zombie and the probe keeps succeeding long
	// after the SIGKILL landed. reap waits the child exactly once on
	// every path (assertion, timeout, and cleanup).
	waited := make(chan error, 1)
	var reapOnce sync.Once
	reap := func() {
		reapOnce.Do(func() { waited <- straggler.Wait() })
	}
	t.Cleanup(func() {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		reap()
	})
	stubBin(t, "ps", `printf '%s\n' " `+strconv.Itoa(pid)+` /bin/sh `+wt+`/repo/tool --watch"`)

	killWorktreeStragglers(log.NewStructuredLogger(
		slog.New(slog.NewTextHandler(io.Discard, nil))), wt)

	go reap()
	select {
	case err := <-waited:
		if sig := deathSignal(err); sig == "" {
			t.Errorf("straggler %d did not die to a signal (wait err: %v)", pid, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("straggler %d still alive after the sweep", pid)
	}
}

func TestRunJailedClaudeActivity(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "ai-jail", "echo AGENT-OUTPUT; exit 0")
	t.Setenv("ANTHROPIC_API_KEY", "sk-test")

	worktree := t.TempDir()
	result, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: worktree,
		Prompt:       "fix the bug",
	})
	if err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}
	// Plain (non stream-json) output falls back to the raw stdout.
	if result.Text != "AGENT-OUTPUT\n" {
		t.Errorf("result.Text = %q, want the raw fallback output", result.Text)
	}

	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
	}
	// The prompt must travel via stdin, not argv; the agent runs with json
	// output so its text comes back structured (stream-json needs
	// --verbose, which ai-jail's flag guard rejects).
	assertArgs(t, calls[0].Args, []string{
		"--worktree",
		"--network",
		"--",
		"claude",
		"--output-format",
		"json",
		"-p",
		"--dangerously-skip-permissions",
	}, "ai-jail")
	if calls[0].Stdin != "fix the bug" {
		t.Errorf("ai-jail stdin = %q, want the prompt", calls[0].Stdin)
	}
	if !samePath(calls[0].Cwd, worktree) {
		t.Errorf("ai-jail cwd = %q, want %q", calls[0].Cwd, worktree)
	}
	if got := calls[0].Env["ANTHROPIC_API_KEY"]; got != "sk-test" {
		t.Errorf("ai-jail ANTHROPIC_API_KEY = %q, want injected via environment", got)
	}
	for _, arg := range calls[0].Args {
		if strings.Contains(arg, "sk-test") {
			t.Errorf("API key leaked into argv: %q", arg)
		}
	}
}

// TestRunJailedClaudeActivityResume pins the session-continuity path: a set
// SessionID makes the claude round resume that conversation (--resume before
// the headless flags, after the agent's own flags), and the reported
// session_id flows back through AgentRunResult for the next round.
func TestRunJailedClaudeActivityResume(t *testing.T) {
	log := newStubLog(t)
	script := `printf '%s\n' '{"type":"result","subtype":"success","result":"resumed work","session_id":"sess-7"}'; exit 0`
	stubBin(t, "ai-jail", script)

	result, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "address the review comments",
		SessionID:    "sess-7",
	})
	if err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}
	if result.Text != "resumed work" {
		t.Errorf("result.Text = %q, want %q", result.Text, "resumed work")
	}
	if result.SessionID != "sess-7" {
		t.Errorf("result.SessionID = %q, want %q", result.SessionID, "sess-7")
	}

	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{
		"--worktree",
		"--network",
		"--",
		"claude",
		"--resume",
		"sess-7",
		"--output-format",
		"json",
		"-p",
		"--dangerously-skip-permissions",
	}, "ai-jail")
}

// TestRunJailedClaudeActivityResumeOtherAgents pins that opencode and amp
// rounds ignore a set SessionID: neither CLI takes --resume here, so the
// round must start fresh rather than pass an unknown flag.
func TestRunJailedClaudeActivityResumeOtherAgents(t *testing.T) {
	for _, agent := range []string{"opencode", "amp"} {
		t.Run(agent, func(t *testing.T) {
			log := newStubLog(t)
			stubBin(t, "ai-jail", "echo OUTPUT; exit 0")
			t.Setenv("DAEDALUS_AGENT", agent)

			_, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
				WorktreePath: t.TempDir(),
				Prompt:       "fix the bug",
				SessionID:    "sess-7",
			})
			if err != nil {
				t.Fatalf("RunJailedClaudeActivity: %v", err)
			}
			calls := readCalls(t, log)
			if len(calls) != 1 {
				t.Fatalf("ai-jail called %d times, want 1", len(calls))
			}
			if contains(calls[0].Args, "--resume") || contains(calls[0].Args, "sess-7") {
				t.Errorf("%s round passed resume state: %v", agent, calls[0].Args)
			}
		})
	}
}

// TestAgentConcurrency pins the env parsing behind the concurrency cap:
// positive values pass through; unset, malformed, and non-positive values
// fall back to the config default.
func TestAgentConcurrency(t *testing.T) {
	cases := []struct {
		env  string
		set  bool
		want int
	}{
		{"", false, config.DefaultMaxConcurrentAgentRuns},
		{"1", true, 1},
		{"4", true, 4},
		{"not-a-number", true, config.DefaultMaxConcurrentAgentRuns},
		{"0", true, config.DefaultMaxConcurrentAgentRuns},
		{"-3", true, config.DefaultMaxConcurrentAgentRuns},
	}
	for _, tc := range cases {
		if tc.set {
			t.Setenv("DAEDALUS_MAX_CONCURRENT_AGENT_RUNS", tc.env)
		} else {
			t.Setenv("DAEDALUS_MAX_CONCURRENT_AGENT_RUNS", "")
		}
		if got := agentConcurrency(); got != tc.want {
			t.Errorf("agentConcurrency() with env %q = %d, want %d", tc.env, got, tc.want)
		}
	}
}

// TestRunJailedSlotWaitBounded pins the bounded slot wait: a round queued
// behind a full limiter gives up after slotWaitTimeout with
// ErrAgentSlotsBusy — never by burning its StartToClose budget — and the
// agent is not launched at all.
func TestRunJailedSlotWaitBounded(t *testing.T) {
	old := slotWaitTimeout
	slotWaitTimeout = 100 * time.Millisecond
	t.Cleanup(func() { slotWaitTimeout = old })

	// Occupy every slot the process-wide limiter has. agentLimiter is a
	// sync.Once, so the cap is whatever the first caller in this test
	// binary initialized; cap(lim) is the truth either way.
	lim := agentLimiter()
	for i := 0; i < cap(lim); i++ {
		lim <- struct{}{}
	}
	t.Cleanup(func() {
		for i := 0; i < cap(lim); i++ {
			<-lim
		}
	})

	// The stub would record a call if the bound failed and the round
	// launched; the assertions below catch that via the error text.
	log := newStubLog(t)
	stubBin(t, "ai-jail", "echo SHOULD-NOT-RUN; exit 0")

	done := make(chan error, 1)
	go func() {
		_, err := runJailed(context.Background(), "", t.TempDir(), "do things")
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrAgentSlotsBusy) {
			t.Fatalf("runJailed error = %v, want ErrAgentSlotsBusy", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runJailed did not give up within the bounded slot wait")
	}
	// The stub log only exists once ai-jail has run at all; absent is the
	// expected zero-call outcome.
	if _, err := os.Stat(log); err == nil {
		if calls := readCalls(t, log); len(calls) != 0 {
			t.Fatalf("ai-jail ran while no slot was free: %d call(s)", len(calls))
		}
	}
}

// TestRunJailedClaudeActivityOpenCode pins the opencode path: DAEDALUS_AGENT
// switches the jailed CLI, the headless flags replace claude's, no
// stream-json is requested, and the plain output is taken as-is.
func TestRunJailedClaudeActivityOpenCode(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "ai-jail", "echo OPENCODE-OUTPUT; exit 0")
	t.Setenv("DAEDALUS_AGENT", "opencode")

	worktree := t.TempDir()
	result, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: worktree,
		Prompt:       "fix the bug",
	})
	if err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}
	if result.Text != "OPENCODE-OUTPUT\n" {
		t.Errorf("result.Text = %q, want the raw plain-mode output", result.Text)
	}
	if result.Thinking != "" {
		t.Errorf("result.Thinking = %q, want empty (plain output has no thinking)", result.Thinking)
	}

	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{
		"--worktree",
		"--network",
		"--",
		"opencode",
		"run",
		"--auto",
	}, "ai-jail")
	if calls[0].Stdin != "fix the bug" {
		t.Errorf("ai-jail stdin = %q, want the prompt", calls[0].Stdin)
	}
}

// TestRunJailedClaudeActivityAgentOverride pins the -cli precedence: a run
// whose Agent is set (run -cli/--cli) picks the jailed CLI over the worker's
// DAEDALUS_AGENT — here the worker runs claude while the run asks for
// opencode, and opencode's headless flag set is what reaches ai-jail.
func TestRunJailedClaudeActivityAgentOverride(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "ai-jail", "echo OPENCODE-OUTPUT; exit 0")
	t.Setenv("DAEDALUS_AGENT", "claude")

	if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "fix the bug",
		Agent:        "opencode",
	}); err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}

	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{
		"--worktree",
		"--network",
		"--",
		"opencode",
		"run",
		"--auto",
	}, "ai-jail")
}

// TestRunJailedClaudeActivityAmp pins the amp path: DAEDALUS_AGENT switches
// to amp's headless flags and --stream-json-thinking output mode (whose
// events parse like claude's), and a set AMP_API_KEY reaches the jail via
// --env, by name only — never in argv. Without a key no --env is passed.
func TestRunJailedClaudeActivityAmp(t *testing.T) {
	log := newStubLog(t)
	script := `printf '%s\n' ` +
		`'{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"plan via amp"}]}}' ` +
		`'{"type":"assistant","message":{"content":[{"type":"text","text":"did it via amp"}]}}'; exit 0`
	stubBin(t, "ai-jail", script)
	t.Setenv("DAEDALUS_AGENT", "amp")

	// No key: the agent still runs, just without the forwarded credential.
	t.Setenv("AMP_API_KEY", "")
	if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "fix the bug",
	}); err != nil {
		t.Fatalf("RunJailedClaudeActivity without a key: %v", err)
	}

	// Key set: it travels by name for ai-jail to copy from the environment.
	t.Setenv("AMP_API_KEY", "amp-test-key")
	result, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "fix the bug",
	})
	if err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}
	if result.Thinking != "plan via amp" {
		t.Errorf("result.Thinking = %q, want the stream's thinking block", result.Thinking)
	}
	if result.Text != "did it via amp" {
		t.Errorf("result.Text = %q, want the stream's text block", result.Text)
	}

	calls := readCalls(t, log)
	if len(calls) != 2 {
		t.Fatalf("ai-jail called %d times, want 2", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{
		"--worktree",
		"--network",
		"--",
		"amp",
		"--stream-json-thinking",
		"-x",
		"--dangerously-allow-all",
	}, "ai-jail without a key")
	assertArgs(t, calls[1].Args, []string{
		"--worktree",
		"--network",
		"--env",
		"AMP_API_KEY",
		"--",
		"amp",
		"--stream-json-thinking",
		"-x",
		"--dangerously-allow-all",
	}, "ai-jail with a key")
	if calls[1].Env["AMP_API_KEY"] != "amp-test-key" {
		t.Errorf("ai-jail AMP_API_KEY = %q, want carried via the environment", calls[1].Env["AMP_API_KEY"])
	}
	for _, call := range calls {
		if contains(call.Args, "amp-test-key") {
			t.Errorf("API key leaked into argv: %v", call.Args)
		}
		if call.Stdin != "fix the bug" {
			t.Errorf("ai-jail stdin = %q, want the prompt", call.Stdin)
		}
	}
}

// TestRunJailedClaudeActivityWithoutKey pins the no-key behavior: an unset
// API key is not an error — the agent is simply launched and left to
// authenticate on its own (worker-inherited env or its own login).
func TestRunJailedClaudeActivityWithoutKey(t *testing.T) {
	newStubLog(t)
	stubBin(t, "ai-jail", "exit 0")
	t.Setenv("ANTHROPIC_API_KEY", "")

	if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "do things",
	}); err != nil {
		t.Fatalf("agent run without a key must not fail, got %v", err)
	}
}

func TestRunJailedClaudeActivityFailure(t *testing.T) {
	newStubLog(t)
	stubBin(t, "ai-jail", "echo 'jail rejected' >&2; exit 1")

	_, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "do things",
	})
	if err == nil {
		t.Fatal("want error when ai-jail fails")
	}
	if !strings.Contains(err.Error(), "jail rejected") {
		t.Errorf("error %q should contain stderr on failure", err)
	}
}

// TestRunJailedClaudeActivityStreamJSON pins the stream-json path: thinking
// and text blocks from assistant messages land in the activity result, other
// stream lines are ignored.
func TestRunJailedClaudeActivityStreamJSON(t *testing.T) {
	newStubLog(t)
	script := `printf '%s\n' ` +
		`'{"type":"system","subtype":"init"}' ` +
		`'{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"plan the change"}]}}' ` +
		`'{"type":"assistant","message":{"content":[{"type":"text","text":"did the change"}]}}' ` +
		`'{"type":"result","subtype":"success","result":"did the change"}'; exit 0`
	stubBin(t, "ai-jail", script)

	result, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "fix the bug",
	})
	if err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}
	if result.Thinking != "plan the change" {
		t.Errorf("result.Thinking = %q, want %q", result.Thinking, "plan the change")
	}
	if result.Text != "did the change" {
		t.Errorf("result.Text = %q, want %q", result.Text, "did the change")
	}
}

// TestParseAgentStreamJSONFormat pins the --output-format json path: the
// single result object's Result field becomes the text and its session_id
// is reported for later rounds to resume.
func TestParseAgentStreamJSONFormat(t *testing.T) {
	thinking, text, session := parseAgentStream(
		`{"type":"result","subtype":"success","result":"did the change","session_id":"abc123"}`)
	if thinking != "" {
		t.Errorf("thinking = %q, want empty", thinking)
	}
	if text != "did the change" {
		t.Errorf("text = %q, want %q", text, "did the change")
	}
	if session != "abc123" {
		t.Errorf("session = %q, want %q", session, "abc123")
	}
}

// TestParseAgentStreamNoise pins tolerance: non-JSON lines and non-assistant
// events are skipped rather than fatal.
func TestParseAgentStreamNoise(t *testing.T) {
	stdout := "not json at all\n" +
		`{"type":"stream_event","event":{"type":"content_block_delta"}}` + "\n" +
		`{"type":"assistant","message":{"content":[{"type":"text","text":"ok"}]}}` + "\n"
	thinking, text, session := parseAgentStream(stdout)
	if thinking != "" {
		t.Errorf("thinking = %q, want empty", thinking)
	}
	if text != "ok" {
		t.Errorf("text = %q, want %q", text, "ok")
	}
	if session != "" {
		t.Errorf("session = %q, want empty (no event carried one)", session)
	}
}

// TestParseReviewVerdict pins the verdict protocol: the last non-empty line
// decides, NEEDS_MAINTAINER parks for a maintainer, and anything not exactly
// APPROVED counts as changes requested.
func TestParseReviewVerdict(t *testing.T) {
	cases := []struct {
		name            string
		out             string
		approved        bool
		needsMaintainer bool
		comments        string
	}{
		{"approved", "Looks good.\nAPPROVED\n", true, false, "Looks good."},
		{"changes requested", "Do X.\nCHANGES_REQUESTED", false, false, "Do X."},
		{"needs maintainer", "Need a secret.\nNEEDS_MAINTAINER", false, true, "Need a secret."},
		{"trailing blank lines", "fine\nAPPROVED\n\n\n", true, false, "fine"},
		{"no marker keeps whole output", "the error path is untested", false, false, "the error path is untested"},
		{"lowercase is not approved", "fine\napproved", false, false, "fine\napproved"},
		{"lowercase is not a maintainer halt", "fine\nneeds_maintainer", false, false, "fine\nneeds_maintainer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseReviewVerdict(tc.out)
			if got.Approved != tc.approved {
				t.Errorf("Approved = %v, want %v", got.Approved, tc.approved)
			}
			if got.NeedsMaintainer != tc.needsMaintainer {
				t.Errorf("NeedsMaintainer = %v, want %v", got.NeedsMaintainer, tc.needsMaintainer)
			}
			if got.Comments != tc.comments {
				t.Errorf("Comments = %q, want %q", got.Comments, tc.comments)
			}
		})
	}
}

func TestRunJailedReviewerActivityApproved(t *testing.T) {
	log := newStubLog(t)
	// git -C <wt> add -N -A, then git -C <wt> diff prints the diff.
	// argv: -C <wt> <subcmd> ... — the subcommand is $3.
	stubBin(t, "git", `if [ "$3" = "diff" ]; then printf 'M foo.go\n'; fi
exit 0`)
	stubBin(t, "ai-jail", `printf 'Looks good.\nAPPROVED\n'; exit 0`)

	worktree := t.TempDir()
	res, err := RunJailedReviewerActivity(context.Background(), ReviewInput{
		WorktreePath: worktree,
		Focus:        "the implementation",
	})
	if err != nil {
		t.Fatalf("RunJailedReviewerActivity: %v", err)
	}
	if !res.Approved {
		t.Error("Approved = false, want true")
	}
	if res.Comments != "Looks good." {
		t.Errorf("Comments = %q, want %q", res.Comments, "Looks good.")
	}

	calls := readCalls(t, log)
	if len(calls) != 3 {
		t.Fatalf("%d subprocess calls, want 3 (git add, git diff, ai-jail)", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{"-C", worktree, "add", "-N", "-A"}, "git add")
	assertArgs(t, calls[1].Args, []string{"-C", worktree, "diff"}, "git diff")

	// The reviewer prompt must carry the focus, the diff, and the verdict
	// instructions — via stdin, like every prompt.
	jailed := calls[2]
	for _, want := range []string{"the implementation", "M foo.go", "APPROVED", "CHANGES_REQUESTED"} {
		if !strings.Contains(jailed.Stdin, want) {
			t.Errorf("reviewer prompt should mention %q", want)
		}
	}
	if strings.Contains(jailed.Stdin, "Latest test run output") {
		t.Error("reviewer prompt should not mention test output when none was given")
	}
}

func TestRunJailedReviewerActivityChangesRequested(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "git", `if [ "$3" = "diff" ]; then printf 'A foo_test.go\n'; fi
exit 0`)
	stubBin(t, "ai-jail", `printf 'Cover the error path.\nCHANGES_REQUESTED\n'; exit 0`)

	res, err := RunJailedReviewerActivity(context.Background(), ReviewInput{
		WorktreePath: t.TempDir(),
		Focus:        "the test suite",
		TestLogs:     "--- FAIL: TestBoom",
	})
	if err != nil {
		t.Fatalf("RunJailedReviewerActivity: %v", err)
	}
	if res.Approved {
		t.Error("Approved = true, want false")
	}
	if res.Comments != "Cover the error path." {
		t.Errorf("Comments = %q, want %q", res.Comments, "Cover the error path.")
	}

	calls := readCalls(t, log)
	if len(calls) != 3 {
		t.Fatalf("%d subprocess calls, want 3", len(calls))
	}
	// Test logs given to the reviewer must reach its prompt.
	for _, want := range []string{"Latest test run output", "--- FAIL: TestBoom"} {
		if !strings.Contains(calls[2].Stdin, want) {
			t.Errorf("reviewer prompt should mention %q", want)
		}
	}
}

// TestRunJailedReviewerActivityAgentOverride pins the reviewer-side -cli
// plumbing: ReviewInput.Agent picks the jailed CLI for the review round too,
// over the worker's DAEDALUS_AGENT.
func TestRunJailedReviewerActivityAgentOverride(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "git", `if [ "$3" = "diff" ]; then printf 'M foo.go\n'; fi
exit 0`)
	stubBin(t, "ai-jail", `printf 'Looks good.\nAPPROVED\n'; exit 0`)
	t.Setenv("DAEDALUS_AGENT", "claude")

	if _, err := RunJailedReviewerActivity(context.Background(), ReviewInput{
		WorktreePath: t.TempDir(),
		Focus:        "the implementation",
		Agent:        "opencode",
	}); err != nil {
		t.Fatalf("RunJailedReviewerActivity: %v", err)
	}

	calls := readCalls(t, log)
	if len(calls) != 3 {
		t.Fatalf("%d subprocess calls, want 3 (git add, git diff, ai-jail)", len(calls))
	}
	assertArgs(t, calls[2].Args, []string{
		"--worktree",
		"--network",
		"--",
		"opencode",
		"run",
		"--auto",
	}, "review jail")
}

// TestRunJailedReviewerActivityStderrNoise pins the stdout-only verdict rule:
// stderr written after a genuine APPROVED must not flip the verdict.
func TestRunJailedReviewerActivityStderrNoise(t *testing.T) {
	newStubLog(t)
	stubBin(t, "git", "exit 0")
	stubBin(t, "ai-jail", `printf 'Fine.\nAPPROVED\n'; printf 'some jail progress noise\n' >&2; exit 0`)

	res, err := RunJailedReviewerActivity(context.Background(), ReviewInput{
		WorktreePath: t.TempDir(),
		Focus:        "the implementation",
	})
	if err != nil {
		t.Fatalf("RunJailedReviewerActivity: %v", err)
	}
	if !res.Approved {
		t.Error("Approved = false, want true — stderr must not affect the verdict")
	}
	if res.Comments != "Fine." {
		t.Errorf("Comments = %q, want %q", res.Comments, "Fine.")
	}
}

// TestRunJailedReviewerActivityResume pins the reviewer's session continuity:
// a set SessionID resumes the prior review conversation (--resume before the
// headless flags), the reviewer runs with the structured output mode (so the
// verdict is parsed from the json result's text), and the session_id comes
// back on the verdict for the next round of the same role.
func TestRunJailedReviewerActivityResume(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "git", `if [ "$3" = "diff" ]; then printf 'M foo.go\n'; fi
exit 0`)
	script := `printf '%s\n' '{"type":"result","subtype":"success","result":"Addressed.\nAPPROVED","session_id":"rev-9"}'; exit 0`
	stubBin(t, "ai-jail", script)

	res, err := RunJailedReviewerActivity(context.Background(), ReviewInput{
		WorktreePath: t.TempDir(),
		Focus:        "the implementation",
		SessionID:    "rev-9",
	})
	if err != nil {
		t.Fatalf("RunJailedReviewerActivity: %v", err)
	}
	if !res.Approved {
		t.Error("Approved = false, want true (verdict parsed from the json result text)")
	}
	if res.Comments != "Addressed." {
		t.Errorf("Comments = %q, want %q", res.Comments, "Addressed.")
	}
	if res.SessionID != "rev-9" {
		t.Errorf("SessionID = %q, want %q", res.SessionID, "rev-9")
	}

	calls := readCalls(t, log)
	if len(calls) != 3 {
		t.Fatalf("%d subprocess calls, want 3 (git add, git diff, ai-jail)", len(calls))
	}
	assertArgs(t, calls[2].Args, []string{
		"--worktree",
		"--network",
		"--",
		"claude",
		"--resume",
		"rev-9",
		"--output-format",
		"json",
		"-p",
		"--dangerously-skip-permissions",
	}, "ai-jail")
}

func TestFinalizeWorktreeActivity(t *testing.T) {
	home := fakeHome(t)
	log := newStubLog(t)
	stubBin(t, "git", "exit 0")
	worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")

	preserved, err := FinalizeWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-123",
	})
	if err != nil {
		t.Fatalf("FinalizeWorktreeActivity: %v", err)
	}
	if preserved != "daedalus/issue-42-123" {
		t.Errorf("preserved branch = %q, want daedalus/issue-42-123", preserved)
	}

	calls := readCalls(t, log)
	if len(calls) != 3 {
		t.Fatalf("git called %d times, want 3 (add, commit, branch -m)", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{"-C", worktreePath, "add", "-A"}, "finalize add")
	if !contains(calls[1].Args, "commit") || !contains(calls[1].Args, "daedalus: issue 42") {
		t.Errorf("finalize commit args = %v, want a commit with the issue message", calls[1].Args)
	}
	assertArgs(t, calls[2].Args, []string{"-C", worktreePath, "branch", "-m", "daedalus/issue-42-123"}, "branch rename")
}

// TestFinalizeWorktreeActivityCustomPrefix pins the per-run prefix: the
// deliverable branch lands under BranchPrefix instead of the default
// daedalus/ namespace.
func TestFinalizeWorktreeActivityCustomPrefix(t *testing.T) {
	home := fakeHome(t)
	log := newStubLog(t)
	stubBin(t, "git", "exit 0")
	worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")

	preserved, err := FinalizeWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:     "/repo",
		TaskQueue:    "daedalus",
		IssueID:      "42",
		BranchName:   "feat/issue-42-123",
		BranchPrefix: "team/ship",
	})
	if err != nil {
		t.Fatalf("FinalizeWorktreeActivity: %v", err)
	}
	if preserved != "team/ship/issue-42-123" {
		t.Errorf("preserved branch = %q, want team/ship/issue-42-123", preserved)
	}

	calls := readCalls(t, log)
	if len(calls) != 3 {
		t.Fatalf("git called %d times, want 3 (add, commit, branch -m)", len(calls))
	}
	assertArgs(t, calls[2].Args, []string{"-C", worktreePath, "branch", "-m", "team/ship/issue-42-123"}, "branch rename")
}

// TestCleanupFinalizedCheckUsesRunPrefix pins that the is-this-run-finalized
// check looks under the run's own prefix: a deliverable preserved under a
// custom prefix still suppresses the aborted-work snapshot.
func TestCleanupFinalizedCheckUsesRunPrefix(t *testing.T) {
	home := fakeHome(t)
	log := newStubLog(t)
	// branch --list under the run's prefix reports the preserved deliverable;
	// argv: -C repo branch --list <pattern> — the pattern is $5.
	stubBin(t, "git", `case "$5" in
  team/ship/*) printf 'team/ship/issue-42-1\n'; exit 0 ;;
esac
exit 0`)
	// The real worktree path CleanupWorktreeActivity stats, so the preserve
	// sequence is genuinely reachable and only the prefix check can suppress it.
	worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := CleanupWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:     "/repo",
		TaskQueue:    "daedalus",
		IssueID:      "42",
		BranchName:   "feat/issue-42-7",
		BranchPrefix: "team/ship",
	}); err != nil {
		t.Fatalf("CleanupWorktreeActivity: %v", err)
	}

	calls := readCalls(t, log)
	// Finalized check, in-flight delete, remove, prune, stale sweep — and
	// crucially no preserve commit / aborted rename, despite the worktree
	// directory still existing.
	if len(calls) != 5 {
		t.Fatalf("git called %d times, want 5 (finalized check, drop in-flight, remove, prune, sweep)", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{"-C", "/repo", "branch", "--list", "team/ship/issue-42-*", "--format=%(refname:short)"}, "finalized check")
	assertArgs(t, calls[1].Args, []string{"-C", "/repo", "branch", "-D", "feat/issue-42-7"}, "drop in-flight branch")
}

// TestCleanupFinalizedCheckIgnoresOtherPrefixes is the mirror of
// TestCleanupFinalizedCheckUsesRunPrefix: a deliverable finalized under a
// different prefix must not suppress this run's aborted-work snapshot.
func TestCleanupFinalizedCheckIgnoresOtherPrefixes(t *testing.T) {
	home := fakeHome(t)
	log := newStubLog(t)
	// branch --list reports a deliverable only under daedalus/, not the run's
	// team/ship prefix; argv: -C repo branch --list <pattern> — $5.
	stubBin(t, "git", `case "$5" in
  daedalus/*) printf 'daedalus/issue-42-1\n'; exit 0 ;;
esac
exit 0`)
	// The real worktree path, so the preserve sequence is genuinely reachable
	// and only the prefix check could suppress it.
	worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := CleanupWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:     "/repo",
		TaskQueue:    "daedalus",
		IssueID:      "42",
		BranchName:   "feat/issue-42-7",
		BranchPrefix: "team/ship",
	}); err != nil {
		t.Fatalf("CleanupWorktreeActivity: %v", err)
	}

	calls := readCalls(t, log)
	if len(calls) != 8 {
		t.Fatalf("git called %d times, want 8 (list, add, commit, drop aborted, rename, remove, prune, sweep)", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{"-C", "/repo", "branch", "--list", "team/ship/issue-42-*", "--format=%(refname:short)"}, "finalized check")
	assertArgs(t, calls[4].Args, []string{"-C", worktreePath, "branch", "-m", "aborted/issue-42"}, "preserve rename")
}

// TestCleanupRejectsReservedBranchPrefix pins the trust boundary: a workflow
// input whose BranchPrefix collides with a reserved namespace fails loudly
// before any git runs, so it can neither suppress the aborted-work snapshot
// nor delete the in-flight branch.
func TestCleanupRejectsReservedBranchPrefix(t *testing.T) {
	fakeHome(t)
	log := newStubLog(t)
	stubBin(t, "git", "exit 0")

	err := CleanupWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:     "/repo",
		TaskQueue:    "daedalus",
		IssueID:      "42",
		BranchName:   "feat/issue-42-7",
		BranchPrefix: "feat",
	})
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("want reserved-namespace rejection, got %v", err)
	}
	// The stub log only exists once git runs; its absence is the assertion.
	if _, err := os.Stat(log); !os.IsNotExist(err) {
		t.Errorf("git ran despite the reserved prefix (stat err = %v)", err)
	}
}

// TestFinalizeWorktreeActivityNothingToCommit pins the anomaly path: an
// approved change with an empty diff fails finalize instead of preserving a
// meaningless branch.
func TestFinalizeWorktreeActivityNothingToCommit(t *testing.T) {
	fakeHome(t)
	newStubLog(t)
	stubBin(t, "git", `case " $* " in *" commit "*) echo 'nothing to commit, working tree clean' >&2; exit 1 ;; esac
exit 0`)

	_, err := FinalizeWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-123",
	})
	if err == nil || !strings.Contains(err.Error(), "commit approved work") {
		t.Fatalf("want nothing-to-commit error, got %v", err)
	}
}

// goWorktree returns a temp worktree carrying a go.mod marker so the static
// detection resolves `go test ./...`.
func goWorktree(t *testing.T) string {
	t.Helper()
	wt := t.TempDir()
	if err := os.WriteFile(filepath.Join(wt, "go.mod"), []byte("module example.com/x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return wt
}

// TestNativeTestsDeclaredCommand pins the precedence: a .daedalus.yaml
// declaration wins over every detection.
func TestNativeTestsDeclaredCommand(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "sh", "echo 'suite green'; exit 0")

	wt := t.TempDir()
	if err := os.WriteFile(filepath.Join(wt, "go.mod"), []byte("module x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wt, ".daedalus.yaml"), []byte("tests: pnpm test --filter ui\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := RunNativeTestsActivity(context.Background(), wt, "")
	if err != nil {
		t.Fatalf("RunNativeTestsActivity: %v", err)
	}
	if !result.Passed {
		t.Error("Passed = false, want true")
	}
	if result.Command != "sh -c pnpm test --filter ui" {
		t.Errorf("Command = %q, want the declared command", result.Command)
	}
	calls := readCalls(t, log)
	if len(calls) != 1 || !contains(calls[0].Args, "pnpm") {
		t.Errorf("sh calls = %+v, want the declared command", calls)
	}
}

// TestNativeTestsMakefileTargets pins the Makefile fallback: whichever of
// test-ui / test-api the Makefile declares, in that order.
func TestNativeTestsMakefileTargets(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "make", "echo 'ui+api green'; exit 0")

	wt := t.TempDir()
	mk := "build:\n\tgo build ./...\n\ntest-ui:\n\tnpx vitest run\n\ntest-api:\n\tnpx pytest api\n"
	if err := os.WriteFile(filepath.Join(wt, "Makefile"), []byte(mk), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := RunNativeTestsActivity(context.Background(), wt, "")
	if err != nil {
		t.Fatalf("RunNativeTestsActivity: %v", err)
	}
	if !result.Passed {
		t.Error("Passed = false, want true")
	}
	if result.Command != "make test-ui test-api" {
		t.Errorf("Command = %q, want make test-ui test-api", result.Command)
	}
	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("make called %d times, want 1", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{"test-ui", "test-api"}, "make")

	// A Makefile with only test-api runs only that target.
	wt2 := t.TempDir()
	if err := os.WriteFile(filepath.Join(wt2, "Makefile"), []byte("test-api:\n\tpytest api\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	argv, ok := detectedTestCommand(wt2)
	if !ok || strings.Join(argv, " ") != "make test-api" {
		t.Errorf("detectedTestCommand = %v, %v; want make test-api", argv, ok)
	}
}

// TestNativeTestsAIDiscovery pins the general fallback: with no static
// markers, a short jailed agent round names the command.
func TestNativeTestsAIDiscovery(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "ai-jail",
		`printf '%s\n' '{"type":"result","subtype":"success","result":"make test-ui test-api"}'; exit 0`)
	stubBin(t, "sh", "echo 'suite green'; exit 0")

	result, err := RunNativeTestsActivity(context.Background(), t.TempDir(), "")
	if err != nil {
		t.Fatalf("RunNativeTestsActivity: %v", err)
	}
	if !result.Passed {
		t.Error("Passed = false, want true")
	}
	if result.Command != "sh -c make test-ui test-api" {
		t.Errorf("Command = %q, want the AI-discovered command", result.Command)
	}
	calls := readCalls(t, log)
	if len(calls) != 2 {
		t.Fatalf("called %d times, want 2 (discovery + test run)", len(calls))
	}
}

// TestNativeTestsAIDiscoveryAgentOverride pins that the run's -cli selection
// reaches the discovery round too: with no static markers, the discovery jail
// runs the overridden agent — not the worker's DAEDALUS_AGENT claude.
func TestNativeTestsAIDiscoveryAgentOverride(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "ai-jail",
		`printf '%s\n' '{"type":"result","subtype":"success","result":"make test"}'; exit 0`)
	stubBin(t, "sh", "echo 'suite green'; exit 0")
	t.Setenv("DAEDALUS_AGENT", "claude")

	if _, err := RunNativeTestsActivity(context.Background(), t.TempDir(), "opencode"); err != nil {
		t.Fatalf("RunNativeTestsActivity: %v", err)
	}

	calls := readCalls(t, log)
	if len(calls) != 2 {
		t.Fatalf("called %d times, want 2 (discovery + test run)", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{
		"--worktree",
		"--network",
		"--",
		"opencode",
		"run",
		"--auto",
	}, "discovery jail")
}

// TestFirstCommandLine pins the reply hygiene: fences and backticks are
// stripped, the first usable line wins.
func TestFirstCommandLine(t *testing.T) {
	for _, c := range []struct {
		in, want string
	}{
		{"make test-ui", "make test-ui"},
		{"```sh\nmake test-ui\n```", "make test-ui"},
		{"`npm test`", "npm test"},
		{"\n\npytest -q\nplus more", "pytest -q"},
		{"```\nnothing usable\n", "nothing usable"},
	} {
		if got := firstCommandLine(c.in); got != c.want {
			t.Errorf("firstCommandLine(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	if got := firstCommandLine(""); got != "" {
		t.Errorf("firstCommandLine(empty) = %q, want empty", got)
	}
}

func TestRunNativeTestsActivityPass(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "go", "echo 'ok all packages'; exit 0")

	worktree := goWorktree(t)
	result, err := RunNativeTestsActivity(context.Background(), worktree, "")
	if err != nil {
		t.Fatalf("RunNativeTestsActivity: %v", err)
	}
	if !result.Passed {
		t.Error("Passed = false, want true on exit 0")
	}
	if !strings.Contains(result.Logs, "ok all packages") {
		t.Errorf("logs %q should contain test output", result.Logs)
	}

	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("go called %d times, want 1", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{"test", "./..."}, "go")
	if !samePath(calls[0].Cwd, worktree) {
		t.Errorf("go cwd = %q, want %q", calls[0].Cwd, worktree)
	}
}

func TestRunNativeTestsActivityFail(t *testing.T) {
	newStubLog(t)
	stubBin(t, "go", "echo 'FAIL: TestBoom'; exit 1")

	result, err := RunNativeTestsActivity(context.Background(), goWorktree(t), "")
	if err != nil {
		t.Fatalf("test failure must not be a system error, got %v", err)
	}
	if result.Passed {
		t.Error("Passed = true, want false on non-zero exit")
	}
	if !strings.Contains(result.Logs, "FAIL: TestBoom") {
		t.Errorf("logs %q should contain failure output", result.Logs)
	}
}

// TestRunNativeTestsActivityTruncates pins the history-diet behavior: huge
// test output is tail-truncated with a marker, never carried in full.
func TestRunNativeTestsActivityTruncates(t *testing.T) {
	newStubLog(t)
	stubBin(t, "go", `head -c 100000 /dev/zero | tr '\0' 'x'; echo 'FAIL: at the end'; exit 1`)

	result, err := RunNativeTestsActivity(context.Background(), goWorktree(t), "")
	if err != nil {
		t.Fatalf("RunNativeTestsActivity: %v", err)
	}
	if result.Passed {
		t.Error("Passed = true, want false")
	}
	if !strings.Contains(result.Logs, "[... earlier output truncated ...]") {
		t.Error("logs should carry the truncation marker")
	}
	if !strings.Contains(result.Logs, "FAIL: at the end") {
		t.Error("logs should keep the tail (where failures live)")
	}
	if len(result.Logs) > maxTestLogs+1024 {
		t.Errorf("logs length %d exceeds the bound by too much", len(result.Logs))
	}
}

func TestRunNativeTestsActivitySystemError(t *testing.T) {
	// A non-executable `go` that is the ONLY go on PATH makes exec itself
	// fail (not an exit status) — that is a system error, not "tests failed".
	// PATH must contain nothing else: LookPath skips non-executable entries
	// and would otherwise find the real toolchain further down.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte("not executable"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)

	result, err := RunNativeTestsActivity(context.Background(), goWorktree(t), "")
	if err == nil {
		t.Fatal("want system error when go cannot be executed")
	}
	if result.Passed || result.Logs != "" {
		t.Errorf("result = %+v, want zero value on system error", result)
	}
}

func TestCleanupWorktreeActivity(t *testing.T) {
	home := fakeHome(t)
	log := newStubLog(t)
	stubBin(t, "git", "exit 0")

	err := CleanupWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-7",
	})
	if err != nil {
		t.Fatalf("CleanupWorktreeActivity: %v", err)
	}

	worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")
	calls := readCalls(t, log)
	// No worktree dir and no finalized branch: nothing to preserve, so the
	// flow is finalized check, remove, prune, stale sweep.
	if len(calls) != 4 {
		t.Fatalf("git called %d times, want 4 (finalized check, remove, prune, stale sweep)", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{"-C", "/repo", "branch", "--list", "daedalus/issue-42-*", "--format=%(refname:short)"}, "finalized check")
	assertArgs(t, calls[1].Args, []string{"-C", "/repo", "worktree", "remove", worktreePath, "--force"}, "remove")
	assertArgs(t, calls[2].Args, []string{"-C", "/repo", "worktree", "prune"}, "prune")
	assertArgs(t, calls[3].Args, []string{"-C", "/repo", "branch", "--list", "feat/issue-42-*", "--format=%(refname:short)"}, "stale branch sweep")
}

func TestCleanupWorktreeActivityToleratesUnregistered(t *testing.T) {
	fakeHome(t)
	log := newStubLog(t)
	// remove fails ("is not a working tree"); prune and branch must still run.
	stubBin(t, "git", `if [ "$4" = "remove" ]; then echo "fatal: is not a working tree" >&2; exit 1; fi
exit 0`)

	err := CleanupWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-7",
	})
	if err != nil {
		t.Fatalf("cleanup must tolerate an unregistered worktree: %v", err)
	}

	calls := readCalls(t, log)
	if len(calls) != 4 {
		t.Fatalf("git called %d times, want 4 (finalized check, remove attempted, prune and sweep still run)", len(calls))
	}
	assertArgs(t, calls[2].Args, []string{"-C", "/repo", "worktree", "prune"}, "prune after failed remove")
}

func TestCleanupWorktreeActivityPruneFailure(t *testing.T) {
	fakeHome(t)
	newStubLog(t)
	stubBin(t, "git", `if [ "$4" = "prune" ]; then echo "fatal: locked" >&2; exit 1; fi
exit 0`)

	err := CleanupWorktreeActivity(context.Background(), WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-7",
	})
	if err == nil || !strings.Contains(err.Error(), "worktree prune") {
		t.Fatalf("want prune failure surfaced, got %v", err)
	}
}
