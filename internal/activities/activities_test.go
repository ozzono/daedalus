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
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"

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
echo "ENV:OPENAI_BASE_URL=$OPENAI_BASE_URL" >> "$STUB_LOG"
echo "ENV:OPENAI_API_BASE=$OPENAI_API_BASE" >> "$STUB_LOG"
echo "ENV:OPENAI_MODEL=$OPENAI_MODEL" >> "$STUB_LOG"
echo "ENV:AIDER_MODEL=$AIDER_MODEL" >> "$STUB_LOG"
echo "ENV:AIDER_WEAK_MODEL=$AIDER_WEAK_MODEL" >> "$STUB_LOG"
echo "ENV:AIDER_EDITOR_MODEL=$AIDER_EDITOR_MODEL" >> "$STUB_LOG"
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
	// Authorship defaults to false: the preserve commit carries the
	// worker's git config, not a forced identity.
	assertArgs(t, calls[2].Args, []string{"-C", worktreePath,
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

// TestAuthorshipIdentityForced pins the authorship opt-in: with
// Authorship true, both commits daedalus makes on its own behalf — the
// aborted-work preserve commit and the approved-deliverable commit — run
// with the forced "daedalus <daedalus@local>" identity, passed before the
// subcommand so git applies it as config overrides.
func TestAuthorshipIdentityForced(t *testing.T) {
	home := fakeHome(t)
	log := newStubLog(t)
	stubBin(t, "git", "exit 0")
	worktreePath := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "issue-42")
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatal(err)
	}
	identity := []string{"-c", "user.name=daedalus", "-c", "user.email=daedalus@local"}

	in := WorktreeInput{
		RepoPath:   "/repo",
		TaskQueue:  "daedalus",
		IssueID:    "42",
		BranchName: "feat/issue-42-123",
		Authorship: true,
	}
	// A stale worktree makes the create path run the preserve commit.
	if _, err := CreateWorktreeActivity(context.Background(), in); err != nil {
		t.Fatalf("CreateWorktreeActivity: %v", err)
	}
	calls := readCalls(t, log)
	// commitAuthorArgs comes first in the argv — global git options are
	// order-independent ahead of the subcommand.
	assertArgs(t, calls[2].Args, append(append([]string{}, identity...),
		"-C", worktreePath,
		"commit", "-m", "daedalus: run closed without approval, work preserved for continue"), "preserve commit")

	if _, err := FinalizeWorktreeActivity(context.Background(), in); err != nil {
		t.Fatalf("FinalizeWorktreeActivity: %v", err)
	}
	calls = readCalls(t, log)
	assertArgs(t, calls[len(calls)-2].Args, append(append([]string{}, identity...),
		"-C", worktreePath, "commit", "-m", "daedalus: issue 42"), "finalize commit")
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
			// commit": the work was not saved. (Authorship defaults to
			// false, so the commit subcommand sits at $3, right after -C.)
			gitStub: `case "$3" in
  commit) echo 'error: unable to write object'; exit 1 ;;
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
	// Authorship defaults to false, so the commit subcommand sits at $3,
	// right after -C.
	stubBin(t, "git", `case "$3" in
  commit) echo 'nothing to commit, working tree clean'; exit 1 ;;
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
		"--mask",
		".claude/settings.json",
		"--mask",
		".claude/settings.local.json",
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
		"--mask",
		".claude/settings.json",
		"--mask",
		".claude/settings.local.json",
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
		"--mask",
		".claude/settings.json",
		"--mask",
		".claude/settings.local.json",
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
		"--mask",
		".claude/settings.json",
		"--mask",
		".claude/settings.local.json",
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
		"--mask",
		".claude/settings.json",
		"--mask",
		".claude/settings.local.json",
		"--",
		"amp",
		"--stream-json-thinking",
		"-x",
		"--dangerously-allow-all",
	}, "ai-jail without a key")
	assertArgs(t, calls[1].Args, []string{
		"--worktree",
		"--network",
		"--mask",
		".claude/settings.json",
		"--mask",
		".claude/settings.local.json",
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
		rebuild         bool
		comments        string
	}{
		{"approved", "Looks good.\nAPPROVED\n", true, false, false, "Looks good."},
		{"changes requested", "Do X.\nCHANGES_REQUESTED", false, false, false, "Do X."},
		{"needs maintainer", "Need a secret.\nNEEDS_MAINTAINER", false, true, false, "Need a secret."},
		{"rebuild", "handler drops the error path.\nREBUILD", false, false, true, "handler drops the error path."},
		{"trailing blank lines", "fine\nAPPROVED\n\n\n", true, false, false, "fine"},
		{"no marker keeps whole output", "the error path is untested", false, false, false, "the error path is untested"},
		{"lowercase is not approved", "fine\napproved", false, false, false, "fine\napproved"},
		{"lowercase is not a maintainer halt", "fine\nneeds_maintainer", false, false, false, "fine\nneeds_maintainer"},
		{"lowercase is not a rebuild", "fine\nrebuild", false, false, false, "fine\nrebuild"},
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
			if got.Rebuild != tc.rebuild {
				t.Errorf("Rebuild = %v, want %v", got.Rebuild, tc.rebuild)
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
		"--mask",
		".claude/settings.json",
		"--mask",
		".claude/settings.local.json",
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
		"--mask",
		".claude/settings.json",
		"--mask",
		".claude/settings.local.json",
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
		"--mask",
		".claude/settings.json",
		"--mask",
		".claude/settings.local.json",
		"--",
		"opencode",
		"run",
		"--auto",
	}, "discovery jail")
}

// TestFirstCommandLine pins the reply hygiene: fences and backticks are
// stripped, the first command-shaped line wins, and banner chrome
// (decorative rules, bullets) is skipped rather than ending the scan.
func TestFirstCommandLine(t *testing.T) {
	for _, c := range []struct {
		in, want string
	}{
		{"make test-ui", "make test-ui"},
		{"```sh\nmake test-ui\n```", "make test-ui"},
		{"`npm test`", "npm test"},
		{"\n\npytest -q\nplus more", "pytest -q"},
		{"```\nnothing usable\n", "nothing usable"},
		// Banner chrome precedes the reply in raw stdout: decorative
		// rules and bullet lines are skipped, the reply after them wins.
		{"──────\nmake test-ui", "make test-ui"},
		{"======\ngo test ./...", "go test ./..."},
		{"* nothing detected\nmake test", "make test"},
		// Shell-plausible punctuation starters stay commands.
		{"./gradlew check", "./gradlew check"},
		{"/usr/bin/make -k test", "/usr/bin/make -k test"},
		{"~/bin/run-tests", "~/bin/run-tests"},
		{"$RUN_ALL", "$RUN_ALL"},
		{"$(make test)", "$(make test)"},
		// A decorative rule alone is not a command.
		{"──────", ""},
	} {
		if got := firstCommandLine(c.in); got != c.want {
			t.Errorf("firstCommandLine(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	if got := firstCommandLine(""); got != "" {
		t.Errorf("firstCommandLine(empty) = %q, want empty", got)
	}
}

// TestNativeTestsAIDiscoveryNone pins the NONE contract: an agent round
// answering NONE — bare, case-folded, sentence-punctuated, dressed with
// prose, or preceded by banner chrome — means the repository has no test
// suite at all, so discovery fails with ErrNoSuite (the distinct
// classification the preflight gate passes vacuously) instead of turning
// the word into a suite command, and no suite round runs.
func TestNativeTestsAIDiscoveryNone(t *testing.T) {
	for _, reply := range []string{
		"NONE",
		"none",
		"NONE.",
		"NONE — no test suite",
		// Banner chrome preceding the verdict in the reply itself. The
		// newline is JSON-escaped so the stubbed stdout stays valid JSON.
		"──────\\nNONE",
	} {
		log := newStubLog(t)
		stubBin(t, "ai-jail",
			`printf '%s\n' '{"type":"result","subtype":"success","result":"`+reply+`"}'; exit 0`)
		stubBin(t, "sh", "echo 'suite would run'; exit 0")

		_, err := RunNativeTestsActivity(context.Background(), t.TempDir(), "")
		if !errors.Is(err, ErrNoSuite) {
			t.Errorf("reply %q: err = %v, want ErrNoSuite", reply, err)
		}
		if calls := readCalls(t, log); len(calls) != 1 {
			t.Errorf("reply %q: %d stub calls, want 1 (discovery alone — no suite run)", reply, len(calls))
		}
	}
}

// TestLastAiderReply pins the chat-history reply extraction: the text after
// the LAST `#### ` heading wins (the file accumulates one heading + reply
// pair per round), the `> …` chrome lines (banner echo, the Tokens tally)
// are dropped, and the result is trimmed. An unparsable file — no heading
// at all, or a heading followed only by chrome — yields "".
func TestLastAiderReply(t *testing.T) {
	for _, c := range []struct {
		name, in, want string
	}{
		{
			name: "latest round wins, chrome dropped, trimmed",
			in: "#### first prompt\n" +
				"> Aider v0.86.2\n" +
				"make old\n" +
				"#### second prompt\n" +
				"> Aider v0.86.2\n" +
				"> Tokens: 1.2k sent, 42 received\n" +
				"\n" +
				"make check\n" +
				"\n",
			want: "make check",
		},
		{
			name: "no heading at all",
			in:   "just prose\nmake check\n",
			want: "",
		},
		{
			name: "heading followed only by chrome",
			in:   "#### a prompt\n> Aider v0.86.2\n> Tokens: 5 sent\n",
			want: "",
		},
		{
			name: "empty file",
			in:   "",
			want: "",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := lastAiderReply(c.in); got != c.want {
				t.Errorf("lastAiderReply(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestNativeTestsAIDiscoveryAiderHistory pins the aider reply channel on
// discovery rounds: aider's raw stdout is all banner chrome whose
// command-shaped lines ("Aider v0.86.2") win the shape scan, shadowing the
// reply (wa-termo 2026-09-26) — so the reply is taken from the appended
// bytes of .daedalus-aider/chat.history.md instead. A repo-planted history
// file must not forge the reply: only bytes the round appended count.
func TestNativeTestsAIDiscoveryAiderHistory(t *testing.T) {
	t.Run("reply comes from the appended history, not the banner", func(t *testing.T) {
		fakeAiderInstall(t, "tree")
		newStubLog(t)
		// The jail stub runs with the worktree as its cwd; appending to the
		// history file here is what aider itself does mid-round.
		stubBin(t, "ai-jail",
			`mkdir -p .daedalus-aider
printf '%s\n' '#### Inspect this repository...' '> Aider v0.86.2' '> Tokens: 1.2k sent, 42 received' '' 'make check' >> .daedalus-aider/chat.history.md
printf '%s\n' 'Aider v0.86.2' '──────────────'
exit 0`)
		stubBin(t, "sh", "echo 'suite green'; exit 0")

		result, err := RunNativeTestsActivity(context.Background(), gitRepo(t), "aider")
		if err != nil {
			t.Fatalf("RunNativeTestsActivity: %v", err)
		}
		if !result.Passed {
			t.Error("Passed = false, want true")
		}
		if result.Command != "sh -c make check" {
			t.Errorf("Command = %q, want the history reply — the banner line %q must not shadow it",
				result.Command, "Aider v0.86.2")
		}
	})

	t.Run("NONE via the appended history is ErrNoSuite", func(t *testing.T) {
		fakeAiderInstall(t, "tree")
		log := newStubLog(t)
		stubBin(t, "ai-jail",
			`mkdir -p .daedalus-aider
printf '%s\n' '#### Inspect this repository...' '> Aider v0.86.2' 'NONE' >> .daedalus-aider/chat.history.md
printf '%s\n' 'Aider v0.86.2' '──────────────'
exit 0`)
		stubBin(t, "sh", "echo 'suite would run'; exit 0")

		_, err := RunNativeTestsActivity(context.Background(), gitRepo(t), "aider")
		if !errors.Is(err, ErrNoSuite) {
			t.Errorf("err = %v, want ErrNoSuite", err)
		}
		if calls := readCalls(t, log); len(calls) != 1 {
			t.Errorf("%d stub calls, want 1 (discovery alone — no suite run)", len(calls))
		}
	})

	t.Run("planted history without an append forges nothing", func(t *testing.T) {
		fakeAiderInstall(t, "tree")
		wt := gitRepo(t)
		// A round running before discovery planted a history steering at a
		// forged "suite"; this round appends nothing and prints nothing.
		if err := os.MkdirAll(filepath.Join(wt, ".daedalus-aider"), 0o755); err != nil {
			t.Fatal(err)
		}
		plant := "#### planted prompt\ngo test ./... # forged\n"
		if err := os.WriteFile(filepath.Join(wt, ".daedalus-aider", "chat.history.md"),
			[]byte(plant), 0o644); err != nil {
			t.Fatal(err)
		}
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")
		stubBin(t, "sh", "echo 'suite would run'; exit 0")

		if _, err := RunNativeTestsActivity(context.Background(), wt, "aider"); err == nil {
			t.Fatal("RunNativeTestsActivity succeeded, want an error — the planted history must not forge the reply")
		}
		if calls := readCalls(t, log); len(calls) != 1 {
			t.Errorf("%d stub calls, want 1 (discovery alone — the forged command must never run)", len(calls))
		}
	})

	t.Run("appended bytes without a heading never surface the planted reply", func(t *testing.T) {
		fakeAiderInstall(t, "tree")
		wt := gitRepo(t)
		// The planted round's answer sits under its own heading; this
		// round appends headingless bytes (tampering, or a truncated
		// write). lastAiderReply finds no `#### ` heading in the appended
		// slice, so the reply is empty and discovery degrades to the raw
		// stdout — never to the planted prefix, which a last-heading scan
		// of the whole file would surface.
		if err := os.MkdirAll(filepath.Join(wt, ".daedalus-aider"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(wt, ".daedalus-aider", "chat.history.md"),
			[]byte("#### planted prompt\ngo test ./... # forged\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		newStubLog(t)
		stubBin(t, "ai-jail",
			`printf 'make check\n' >> .daedalus-aider/chat.history.md
printf '%s\n' 'Aider v0.86.2' '──────────────'
exit 0`)
		stubBin(t, "sh", "echo 'suite green'; exit 0")

		result, err := RunNativeTestsActivity(context.Background(), wt, "aider")
		if err != nil {
			t.Fatalf("RunNativeTestsActivity: %v", err)
		}
		if result.Command != "sh -c Aider v0.86.2" {
			t.Errorf("Command = %q, want the degraded stdout fallback — the planted reply must not surface", result.Command)
		}
	})

	t.Run("no history file falls back to stdout", func(t *testing.T) {
		fakeAiderInstall(t, "tree")
		newStubLog(t)
		// An older aider run (or any parse miss): no reply channel, so the
		// raw stdout is the reply.
		stubBin(t, "ai-jail", "echo 'make check'; exit 0")
		stubBin(t, "sh", "echo 'suite green'; exit 0")

		result, err := RunNativeTestsActivity(context.Background(), gitRepo(t), "aider")
		if err != nil {
			t.Fatalf("RunNativeTestsActivity: %v", err)
		}
		if result.Command != "sh -c make check" {
			t.Errorf("Command = %q, want the stdout fallback", result.Command)
		}
	})

	t.Run("opencode ignores the history channel", func(t *testing.T) {
		newStubLog(t)
		// opencode has no reply channel wired (ponytail): even with an
		// appended history sitting there, its reply is the raw stdout.
		stubBin(t, "ai-jail",
			`mkdir -p .daedalus-aider
printf '%s\n' '#### a prompt' 'make from-history' >> .daedalus-aider/chat.history.md
echo 'cargo test'
exit 0`)
		stubBin(t, "sh", "echo 'suite green'; exit 0")

		result, err := RunNativeTestsActivity(context.Background(), t.TempDir(), "opencode")
		if err != nil {
			t.Fatalf("RunNativeTestsActivity: %v", err)
		}
		if result.Command != "sh -c cargo test" {
			t.Errorf("Command = %q, want the stdout reply — the history channel is aider's alone", result.Command)
		}
	})
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

// TestRunNativeTestsActivityCarriesFullLogs pins the no-truncation
// contract: huge test output reaches the activity result complete — no
// truncation marker, no byte bound — so a tests-fix round always digests
// the whole failure, head and tail alike.
func TestRunNativeTestsActivityCarriesFullLogs(t *testing.T) {
	newStubLog(t)
	stubBin(t, "go", `head -c 100000 /dev/zero | tr '\0' 'x'; echo 'FAIL: at the end'; exit 1`)

	result, err := RunNativeTestsActivity(context.Background(), goWorktree(t), "")
	if err != nil {
		t.Fatalf("RunNativeTestsActivity: %v", err)
	}
	if result.Passed {
		t.Error("Passed = true, want false")
	}
	if strings.Contains(result.Logs, "[... earlier output truncated ...]") {
		t.Error("logs carry a truncation marker; output must travel complete")
	}
	if !strings.Contains(result.Logs, "FAIL: at the end") {
		t.Error("logs should keep the tail (where failures live)")
	}
	if len(result.Logs) < 100000 {
		t.Errorf("logs length %d lost the bulk of a 100000-byte run", len(result.Logs))
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

// TestParseAgentUsage pins the raw capture contract: the terminal result
// event's cost, token counts, and duration_ms land in Usage untouched (raw
// figures only — aggregation computes deltas); a stream without a result
// event (opencode's plain text) reports a zero Usage; a result without
// metrics does too, leaving the caller's wall-clock fallback in charge of
// Duration.
func TestParseAgentUsage(t *testing.T) {
	resultEvent := `{"type":"result","subtype":"success","result":"done",` +
		`"total_cost_usd":0.25,"duration_ms":42000,` +
		`"usage":{"input_tokens":100,"output_tokens":50,` +
		`"cache_read_input_tokens":10,"cache_creation_input_tokens":20}}`
	want := Usage{
		CostUSD: 0.25, Duration: 42 * time.Second,
		InputTokens: 100, OutputTokens: 50,
		CacheReadTokens: 10, CacheWriteTokens: 20,
	}
	for _, c := range []struct {
		name   string
		stdout string
		want   Usage
	}{
		{"json result event", resultEvent + "\n", want},
		{"stream-json noise then result", "{\"type\":\"system\",\"subtype\":\"init\"}\n" + resultEvent + "\n", want},
		{"plain text, no result event", "AGENT-OUTPUT\n", Usage{}},
		{"result without metrics", "{\"type\":\"result\",\"subtype\":\"success\",\"result\":\"done\"}\n", Usage{}},
	} {
		if got := parseAgentUsage(c.stdout); got != c.want {
			t.Errorf("%s: parseAgentUsage = %+v, want %+v", c.name, got, c.want)
		}
	}
}

// TestRunJailedClaudeActivityCapturesUsage pins the round-level stamping:
// the CLI-reported metrics land in the result's Usage with the worker's
// name, so history aggregates per worker.
func TestRunJailedClaudeActivityCapturesUsage(t *testing.T) {
	newStubLog(t)
	stubBin(t, "ai-jail", `printf '%s\n' '{"type":"result","subtype":"success","result":"done","total_cost_usd":0.25,"duration_ms":42000,"usage":{"input_tokens":100,"output_tokens":50,"cache_read_input_tokens":10,"cache_creation_input_tokens":20}}'; exit 0`)
	t.Setenv("ANTHROPIC_API_KEY", "sk-test")
	t.Setenv("DAEDALUS_WORKER_NAME", "worker-a")

	result, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "fix the bug",
	})
	if err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}
	want := Usage{
		Worker:           "worker-a",
		CostUSD:          0.25,
		Duration:         42 * time.Second,
		InputTokens:      100,
		OutputTokens:     50,
		CacheReadTokens:  10,
		CacheWriteTokens: 20,
	}
	if result.Usage != want {
		t.Errorf("Usage = %+v, want %+v", result.Usage, want)
	}
}

// TestRunJailedClaudeActivityUsageFallsBackToWallClock pins the fallback: a
// CLI without a result event (plain text) still reports the worker's
// measured wall time and name — zero cost and tokens, never a zero
// duration.
func TestRunJailedClaudeActivityUsageFallsBackToWallClock(t *testing.T) {
	newStubLog(t)
	stubBin(t, "ai-jail", "echo AGENT-OUTPUT; exit 0")
	t.Setenv("ANTHROPIC_API_KEY", "sk-test")
	t.Setenv("DAEDALUS_WORKER_NAME", "worker-a")

	result, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "fix the bug",
	})
	if err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}
	u := result.Usage
	if u.Worker != "worker-a" {
		t.Errorf("Usage.Worker = %q, want worker-a", u.Worker)
	}
	if u.Duration <= 0 {
		t.Errorf("Usage.Duration = %v, want the measured wall time", u.Duration)
	}
	if u.CostUSD != 0 || u.InputTokens != 0 || u.OutputTokens != 0 {
		t.Errorf("Usage = %+v, want zero cost and tokens for a CLI without a result event", u)
	}
}

// TestRunJailedReviewerActivityCapturesUsage pins the review-round
// counterpart: the reviewer's verdict carries the same raw Usage.
func TestRunJailedReviewerActivityCapturesUsage(t *testing.T) {
	newStubLog(t)
	// git -C <wt> add -N -A, then git -C <wt> diff prints the diff.
	stubBin(t, "git", `if [ "$3" = "diff" ]; then printf 'M foo.go\n'; fi
exit 0`)
	stubBin(t, "ai-jail", `printf '%s\n' '{"type":"result","subtype":"success","result":"Looks good.\nAPPROVED","total_cost_usd":0.75,"duration_ms":30000,"usage":{"input_tokens":10,"output_tokens":5}}'; exit 0`)
	t.Setenv("ANTHROPIC_API_KEY", "sk-test")
	t.Setenv("DAEDALUS_WORKER_NAME", "worker-b")

	verdict, err := RunJailedReviewerActivity(context.Background(), ReviewInput{
		WorktreePath: t.TempDir(),
		Focus:        "the implementation",
	})
	if err != nil {
		t.Fatalf("RunJailedReviewerActivity: %v", err)
	}
	if !verdict.Approved {
		t.Error("Approved = false, want true")
	}
	want := Usage{
		Worker:       "worker-b",
		CostUSD:      0.75,
		Duration:     30 * time.Second,
		InputTokens:  10,
		OutputTokens: 5,
	}
	if verdict.Usage != want {
		t.Errorf("Usage = %+v, want %+v", verdict.Usage, want)
	}
}

// TestResolveTestCommandActivityQuotesCommand pins the handoff to the test
// queue: the resolved argv is re-quoted element-wise, so the test worker's
// outer shell (`sh -c "cd <wt> && <command>"`) runs the inner command
// nativeTestCommand meant — including one with a space or single quote —
// rather than splitting it.
func TestResolveTestCommandActivityQuotesCommand(t *testing.T) {
	log := newStubLog(t)
	// The payload command must survive the outer shell it is re-run under,
	// so a stub pnpm records how the inner command was split.
	stubBin(t, "pnpm", "exit 0")
	wt := t.TempDir()
	if err := os.WriteFile(filepath.Join(wt, ".daedalus.yaml"),
		[]byte("tests: pnpm test --filter 'ui suite'\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	command, err := ResolveTestCommandActivity(context.Background(), wt, "")
	if err != nil {
		t.Fatalf("ResolveTestCommandActivity: %v", err)
	}
	want := `'sh' '-c' 'pnpm test --filter '\''ui suite'\'''`
	if command != want {
		t.Errorf("command = %q, want %q", command, want)
	}
	// Re-running the command the way the test worker does must reach the
	// inner `sh -c` and split its payload into the intended words — a bare
	// join would run `pnpm` with sh's own flags instead.
	cmd := exec.Command("sh", "-c", "cd "+shellQuote(wt)+" && "+command)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("re-running the quoted command: %v: %s", err, out)
	}
	calls := readCalls(t, log)
	pnpm := calls[len(calls)-1]
	assertArgs(t, pnpm.Args, []string{"test", "--filter", "ui suite"}, "inner command words")
	if !samePath(pnpm.Cwd, wt) {
		t.Errorf("inner command cwd = %q, want %q", pnpm.Cwd, wt)
	}
}

// TestRunTestSuiteActivity pins the dumb-runner contract: the command runs
// in the worktree, a passing suite is green, a failing suite is a red round
// (never a system error) with its output captured, and an empty command is
// refused before anything runs.
func TestRunTestSuiteActivity(t *testing.T) {
	t.Run("pass runs in the worktree", func(t *testing.T) {
		newStubLog(t)
		wt := t.TempDir()
		if err := os.WriteFile(filepath.Join(wt, "marker.txt"), []byte("here"), 0o644); err != nil {
			t.Fatal(err)
		}
		res, err := RunTestSuiteActivity(context.Background(), TestRunInput{
			WorktreePath: wt,
			Command:      "test -f marker.txt && echo 'suite green'",
		})
		if err != nil {
			t.Fatalf("RunTestSuiteActivity: %v", err)
		}
		if !res.Passed {
			t.Error("Passed = false, want true on exit 0")
		}
		if res.Command != "test -f marker.txt && echo 'suite green'" {
			t.Errorf("Command = %q, want the input command echoed back", res.Command)
		}
		if !strings.Contains(res.Logs, "suite green") {
			t.Errorf("logs %q should contain suite output", res.Logs)
		}
	})

	t.Run("failing suite is a red round, not an error", func(t *testing.T) {
		newStubLog(t)
		res, err := RunTestSuiteActivity(context.Background(), TestRunInput{
			WorktreePath: t.TempDir(),
			Command:      "echo 'FAIL: TestBoom' >&2; exit 3",
		})
		if err != nil {
			t.Fatalf("test failure must not be a system error, got %v", err)
		}
		if res.Passed {
			t.Error("Passed = true, want false on non-zero exit")
		}
		if !strings.Contains(res.Logs, "FAIL: TestBoom") {
			t.Errorf("logs %q should contain failure output", res.Logs)
		}
	})

	t.Run("empty command is a system error", func(t *testing.T) {
		newStubLog(t)
		if _, err := RunTestSuiteActivity(context.Background(), TestRunInput{
			WorktreePath: t.TempDir(),
			Command:      "   ",
		}); err == nil || !strings.Contains(err.Error(), "empty test command") {
			t.Fatalf("want empty-command error, got %v", err)
		}
	})
}

// TestTestConcurrency pins the env parsing behind the test-suite cap:
// positive values pass through; unset, malformed, and non-positive values
// fall back to the config default.
func TestTestConcurrency(t *testing.T) {
	cases := []struct {
		env  string
		set  bool
		want int
	}{
		{"", false, config.DefaultMaxConcurrentTests},
		{"1", true, 1},
		{"4", true, 4},
		{"not-a-number", true, config.DefaultMaxConcurrentTests},
		{"0", true, config.DefaultMaxConcurrentTests},
		{"-3", true, config.DefaultMaxConcurrentTests},
	}
	for _, tc := range cases {
		if tc.set {
			t.Setenv("DAEDALUS_MAX_CONCURRENT_TESTS", tc.env)
		} else {
			t.Setenv("DAEDALUS_MAX_CONCURRENT_TESTS", "")
		}
		if got := testConcurrency(); got != tc.want {
			t.Errorf("testConcurrency() with env %q = %d, want %d", tc.env, got, tc.want)
		}
	}
}

// waitCmdActivity starts the argv command with the round's usual process
// containment and waits through waitCommand — the exact shape the jailed
// round and both test activities use, exposed so the drain can be driven
// from the activity test environment.
func waitCmdActivity(ctx context.Context, argv []string) error {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	setProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		return err
	}
	return waitCommand(ctx, cmd)
}

// executeWithStop runs waitCmdActivity inside a real activity context whose
// worker stop channel is pre-closed (the state a restarting worker's
// activities observe), with shutdownDrainGrace set to grace.
func executeWithStop(t *testing.T, grace time.Duration, argv []string) (time.Duration, error) {
	t.Helper()
	old := shutdownDrainGrace
	shutdownDrainGrace = grace
	t.Cleanup(func() { shutdownDrainGrace = old })

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestActivityEnvironment()
	stop := make(chan struct{})
	close(stop) // the worker is already stopping when the activity starts
	env.SetWorkerStopChannel(stop)
	env.RegisterActivity(waitCmdActivity)

	start := time.Now()
	_, err := env.ExecuteActivity(waitCmdActivity, argv)
	return time.Since(start), err
}

// TestWaitCommandPlain pins waitCommand outside a real activity context —
// the unit-test path where the SDK declines to report a stop channel: it is
// exactly cmd.Wait, success and failure alike.
func TestWaitCommandPlain(t *testing.T) {
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start true: %v", err)
	}
	if err := waitCommand(context.Background(), cmd); err != nil {
		t.Errorf("waitCommand(clean exit) = %v, want nil", err)
	}

	cmd = exec.Command("sh", "-c", "exit 3")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sh: %v", err)
	}
	err := waitCommand(context.Background(), cmd)
	if _, ok := errors.AsType[*exec.ExitError](err); !ok {
		t.Errorf("waitCommand(exit 3) = %v, want an *exec.ExitError", err)
	}
}

// TestWaitCommandContextCancelReaps pins the activity-cancellation branch:
// when the round's context is cancelled (an activity timeout, a server
// cancel) CommandContext's group kill lands and waitCommand reaps the exit
// immediately — it must not block on the child's full lifetime.
func TestWaitCommandContextCancelReaps(t *testing.T) {
	if !signalsPermitted() {
		t.Skip("environment forbids signals; the cancel kill cannot be verified here")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sleep", "30")
	setProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	start := time.Now()
	err := waitCommand(ctx, cmd)
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("waitCommand(canceled ctx) returned after %v — the cancel kill did not reap the child", elapsed)
	}
	if _, ok := errors.AsType[*exec.ExitError](err); !ok {
		t.Errorf("waitCommand(canceled ctx) = %v, want the reaped ExitError from the cancel kill", err)
	}
}

// TestWaitCommandDrainsOnWorkerStop pins the shutdown drain: once the
// activity worker begins stopping, a round finishing inside the drain grace
// still reports success, and a round that exceeds it is group-killed and
// its death reported through the normal wait status — never a silent exit
// leaving Temporal a ghost attempt.
func TestWaitCommandDrainsOnWorkerStop(t *testing.T) {
	if !signalsPermitted() {
		t.Skip("environment forbids signals; the drain kill cannot be verified here")
	}

	t.Run("a round finishing inside the grace still succeeds", func(t *testing.T) {
		elapsed, err := executeWithStop(t, 5*time.Second, []string{"sleep", "0.3"})
		if err != nil {
			t.Errorf("waitCommand(stop, finish within grace) = %v, want nil", err)
		}
		if elapsed >= 5*time.Second {
			t.Errorf("round took %v — it should have finished on its own, not been killed at the grace", elapsed)
		}
	})

	t.Run("a round exceeding the grace is killed and reported", func(t *testing.T) {
		elapsed, err := executeWithStop(t, 250*time.Millisecond, []string{"sleep", "30"})
		if err == nil {
			t.Fatal("waitCommand(stop, grace expired) = nil, want the kill's ExitError")
		}
		if !strings.Contains(err.Error(), "killed") {
			t.Errorf("waitCommand(stop, grace expired) = %v, want a signal-death status", err)
		}
		if elapsed < 200*time.Millisecond {
			t.Errorf("round returned after %v — earlier than the drain grace, so the kill fired without waiting", elapsed)
		}
		if elapsed > 10*time.Second {
			t.Errorf("round returned after %v — the grace kill did not fire; the drain outlived the worker window", elapsed)
		}
	})
}

// TestSlotStats pins the worker slot state the daemon publishes next to its
// pid file and `daedalus report` surfaces: busy counts held semaphore slots,
// total is the process-wide cap, and waiting counts rounds queued for a
// slot.
func TestSlotStats(t *testing.T) {
	lim := agentLimiter()

	busy, total, waiting := SlotStats()
	if busy != len(lim) {
		t.Errorf("SlotStats busy = %d, want %d", busy, len(lim))
	}
	if total != cap(lim) {
		t.Errorf("SlotStats total = %d, want the limiter cap %d", total, cap(lim))
	}
	if waiting != 0 {
		t.Errorf("SlotStats waiting with no queued rounds = %d, want 0", waiting)
	}

	// A held slot shows up as busy and a queued round as waiting.
	lim <- struct{}{}
	t.Cleanup(func() { <-lim })
	agentWaiters.Add(1)
	t.Cleanup(func() { agentWaiters.Add(-1) })

	busy, _, waiting = SlotStats()
	if busy != len(lim) {
		t.Errorf("SlotStats busy with one held slot = %d, want %d", busy, len(lim))
	}
	if waiting != 1 {
		t.Errorf("SlotStats waiting with one queued round = %d, want 1", waiting)
	}
}

// gitRepo turns a temp dir into a minimal git repository — the stand-in for
// a real worktree (aider's scratch-dir exclusion needs one).
func gitRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if out, err := exec.Command("git", "-C", dir, "init", "-q").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	return dir
}

// TestRunJailedClaudeActivityPi pins the pi path: DAEDALUS_AGENT switches to
// pi's headless flags and --mode json output mode (whose event schema is
// parsed by its own branch — thinking, text, session id, and usage all
// captured from pi's event shapes).
func TestRunJailedClaudeActivityPi(t *testing.T) {
	log := newStubLog(t)
	script := `printf '%s\n' ` +
		`'{"type":"session","id":"sess-pi-9"}' ` +
		`'{"type":"message_update","usage":{"input":100,"output":20,"cacheRead":5,"cacheWrite":7,"cost":{"total":0.42}}}' ` +
		`'{"type":"message_end","message":{"content":[{"type":"thinking","thinking":"plan via pi"},{"type":"text","text":"did it via pi"}]}}'; exit 0`
	stubBin(t, "ai-jail", script)
	t.Setenv("DAEDALUS_AGENT", "pi")

	result, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "fix the bug",
	})
	if err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}
	if result.Thinking != "plan via pi" {
		t.Errorf("result.Thinking = %q, want the stream's thinking block", result.Thinking)
	}
	if result.Text != "did it via pi" {
		t.Errorf("result.Text = %q, want the stream's text block", result.Text)
	}
	if result.SessionID != "sess-pi-9" {
		t.Errorf("result.SessionID = %q, want the session header's id", result.SessionID)
	}
	if result.Usage.CostUSD != 0.42 || result.Usage.InputTokens != 100 || result.Usage.OutputTokens != 20 ||
		result.Usage.CacheReadTokens != 5 || result.Usage.CacheWriteTokens != 7 {
		t.Errorf("result.Usage = %+v, want pi's message_update figures", result.Usage)
	}

	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
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
	}, "ai-jail")
	if calls[0].Stdin != "fix the bug" {
		t.Errorf("ai-jail stdin = %q, want the prompt", calls[0].Stdin)
	}
}

// TestRunJailedClaudeActivityAider pins the aider path: its doc-verified
// flag set reaches ai-jail, the prompt rides a staged --message-file (aider
// has no stdin mode) whose content is the prompt and which is cleaned up
// after the round, stdin still carries the prompt, a set SessionID is
// ignored (aider has no session-id mechanism), plain chat output is
// taken as-is, and the resolved uv install is mapped into the jail before
// the "--" separator. With no OPENAI_MODEL in the round env, no model
// wiring rides along: no --model flags and no model files staged.
func TestRunJailedClaudeActivityAider(t *testing.T) {
	binDir, venv, pyTree := fakeAiderInstall(t, "tree")
	t.Setenv("OPENAI_MODEL", "")
	log := newStubLog(t)
	stubBin(t, "ai-jail", `prev=
for a in "$@"; do
  [ "$prev" = "--message-file" ] && cat "$a"
  prev=$a
done
exit 0`)
	t.Setenv("DAEDALUS_AGENT", "aider")
	wt := gitRepo(t)

	result, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "fix the bug",
		SessionID:    "sess-7",
	})
	if err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}
	if result.Text != "fix the bug" {
		t.Errorf("result.Text = %q, want the staged prompt file's content", result.Text)
	}

	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
	}
	if calls[0].Stdin != "fix the bug" {
		t.Errorf("ai-jail stdin = %q, want the prompt (the stdin transport is unchanged)", calls[0].Stdin)
	}
	if contains(calls[0].Args, "sess-7") {
		t.Errorf("aider round passed resume state it cannot use: %v", calls[0].Args)
	}
	// The install mounts ride ai-jail's own flags, before the "--"
	// separator — source and destination are identical (the venv's
	// absolute paths are baked into its scripts, so the jail must see the
	// install where the host has it): the launcher's bin dir, the tool
	// venv, and the uv python tree the venv interpreter symlinks into.
	sep := slices.Index(calls[0].Args, "--")
	if sep < 0 {
		t.Fatalf("ai-jail args carry no -- separator: %v", calls[0].Args)
	}
	var maps []string
	for i, a := range calls[0].Args {
		if a == "--map" {
			if i > sep {
				t.Errorf("--map at %d sits after the -- separator at %d", i, sep)
			}
			maps = append(maps, calls[0].Args[i+1])
		}
	}
	pyRoot := filepath.Dir(pyTree)
	wantMaps := []string{binDir + ":" + binDir, venv + ":" + venv, pyRoot + ":" + pyRoot}
	if !slices.Equal(maps, wantMaps) {
		t.Errorf("mount args = %v, want %v", maps, wantMaps)
	}
	if calls[0].Env["OPENAI_API_BASE"] != "" {
		t.Errorf("round env carries OPENAI_API_BASE=%q with no openai model configured", calls[0].Env["OPENAI_API_BASE"])
	}
	mi := slices.Index(calls[0].Args, "--message-file")
	if mi < 0 {
		t.Fatalf("aider args %v carry no --message-file", calls[0].Args)
	}
	promptFile := calls[0].Args[mi+1]
	if !strings.HasPrefix(promptFile, filepath.Join(wt, ".daedalus-aider", "prompt-")) {
		t.Errorf("--message-file = %q, want a staged file under %s", promptFile, filepath.Join(wt, ".daedalus-aider"))
	}
	if _, err := os.Stat(promptFile); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("staged prompt file %q survived the round, want it removed", promptFile)
	}
	assertArgs(t, calls[0].Args[mi:], []string{
		"--message-file", promptFile,
		"--yes-always",
		"--no-auto-commits",
		"--no-gitignore",
		"--chat-history-file", ".daedalus-aider/chat.history.md",
		"--input-history-file", ".daedalus-aider/input.history",
	}, "aider flag set")
	for _, name := range []string{"model.metadata.json", "model.settings.yml"} {
		if _, err := os.Stat(filepath.Join(wt, ".daedalus-aider", name)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("model file %s staged with no OPENAI_MODEL configured (err %v)", name, err)
		}
	}
}

// TestRunJailedPiThinkingFlag pins the thinking toggle's argv seam: only
// DAEDALUS_THINKING=off — what an explicit thinking: false exports at
// worker startup — appends --thinking off to a jailed pi round, after the
// headless flags; unset (or any other value) sends no thinking flag so pi
// follows its host default, and no other agent ever sees the flag.
func TestRunJailedPiThinkingFlag(t *testing.T) {
	for _, c := range []struct {
		name     string
		thinking string
		set      bool
		agent    string
		wantFlag bool
	}{
		{"off appends the flag", "off", true, "pi", true},
		{"on stays on the host default", "on", true, "pi", false},
		{"unset stays on the host default", "", false, "pi", false},
		{"garbage value ignored", "sometimes", true, "pi", false},
		{"off does not leak to claude", "off", true, "claude", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.set {
				t.Setenv("DAEDALUS_THINKING", c.thinking)
			}
			t.Setenv("DAEDALUS_AGENT", c.agent)
			log := newStubLog(t)
			stubBin(t, "ai-jail", "echo OUTPUT; exit 0")

			_, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
				WorktreePath: t.TempDir(),
				Prompt:       "fix the bug",
			})
			if err != nil {
				t.Fatalf("RunJailedClaudeActivity: %v", err)
			}

			calls := readCalls(t, log)
			if len(calls) != 1 {
				t.Fatalf("ai-jail called %d times, want 1", len(calls))
			}
			args := calls[0].Args
			if !c.wantFlag {
				if slices.Contains(args, "--thinking") {
					t.Errorf("args %v carry a thinking flag, want the agent's host default", args)
				}
				return
			}
			n := len(args)
			if n < 2 || args[n-2] != "--thinking" || args[n-1] != "off" {
				t.Errorf("thinking-off args = %v, want --thinking off appended after the headless flags", args)
			}
		})
	}
}

// TestRunJailedClaudeActivityAiderSlim pins the slim wiring on a live
// round: DAEDALUS_SLIM=1 (what runWorker exports for slim: true) makes an
// aider round's child environment carry AIDER_WEAK_MODEL/AIDER_EDITOR_MODEL
// pinned to AIDER_MODEL, while a claude round under the same exports is
// untouched — the pinning is aider's wiring alone.
func TestRunJailedClaudeActivityAiderSlim(t *testing.T) {
	t.Setenv("DAEDALUS_SLIM", "1")
	t.Setenv("AIDER_MODEL", "glm-small")
	t.Setenv("AIDER_WEAK_MODEL", "")
	t.Setenv("AIDER_EDITOR_MODEL", "")
	t.Setenv("DAEDALUS_AGENT", "aider")
	// The fake uv-tools install keeps the test off the host's real aider —
	// only the env pinning is under test, and the suite must not require
	// aider to be installed (see backlog/bugs/aider-slim-test-needs-host-aider.md).
	fakeAiderInstall(t, "tree")
	log := newStubLog(t)
	stubBin(t, "ai-jail", "echo OUTPUT; exit 0")

	if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: gitRepo(t),
		Prompt:       "fix the bug",
	}); err != nil {
		t.Fatalf("RunJailedClaudeActivity (aider): %v", err)
	}
	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
	}
	if got := calls[0].Env["AIDER_WEAK_MODEL"]; got != "glm-small" {
		t.Errorf("slim aider AIDER_WEAK_MODEL = %q, want pinned to AIDER_MODEL", got)
	}
	if got := calls[0].Env["AIDER_EDITOR_MODEL"]; got != "glm-small" {
		t.Errorf("slim aider AIDER_EDITOR_MODEL = %q, want pinned to AIDER_MODEL", got)
	}

	// A claude round under the same exports gets no pinning.
	t.Setenv("DAEDALUS_AGENT", "claude")
	log = newStubLog(t)
	stubBin(t, "ai-jail", "echo OUTPUT; exit 0")
	if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "fix the bug",
	}); err != nil {
		t.Fatalf("RunJailedClaudeActivity (claude): %v", err)
	}
	calls = readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
	}
	if got := calls[0].Env["AIDER_WEAK_MODEL"]; got != "" {
		t.Errorf("claude round AIDER_WEAK_MODEL = %q, want untouched by slim", got)
	}
	if got := calls[0].Env["AIDER_EDITOR_MODEL"]; got != "" {
		t.Errorf("claude round AIDER_EDITOR_MODEL = %q, want untouched by slim", got)
	}
}

// TestExcludeAiderArtifacts pins the scratch-dir exclusion: .daedalus-aider/
// is created and hidden from git via info/exclude — never a tracked
// .gitignore entry — with the pattern appended exactly once, and a pre-
// existing exclude file without a trailing newline is not corrupted.
func TestExcludeAiderArtifacts(t *testing.T) {
	excludeFile := func(wt string) string {
		t.Helper()
		out, err := exec.Command("git", "-C", wt, "rev-parse", "--git-common-dir").CombinedOutput()
		if err != nil {
			t.Fatalf("rev-parse --git-common-dir: %v: %s", err, out)
		}
		dir := strings.TrimSpace(string(out))
		if !filepath.IsAbs(dir) {
			dir = filepath.Join(wt, dir)
		}
		return filepath.Join(dir, "info", "exclude")
	}

	wt := gitRepo(t)
	if err := excludeAiderArtifacts(wt); err != nil {
		t.Fatalf("excludeAiderArtifacts: %v", err)
	}
	if st, err := os.Stat(filepath.Join(wt, ".daedalus-aider")); err != nil || !st.IsDir() {
		t.Fatalf(".daedalus-aider dir = %v, %v; want a directory", st, err)
	}
	want := ".daedalus-aider/\n"
	data, err := os.ReadFile(excludeFile(wt))
	if err != nil {
		t.Fatalf("read exclude: %v", err)
	}
	if !strings.HasSuffix(string(data), want) || strings.Count(string(data), want) != 1 {
		t.Fatalf("exclude file = %q, want the pattern appended exactly once", data)
	}
	// Idempotent: a second round in the same worktree appends nothing.
	if err := excludeAiderArtifacts(wt); err != nil {
		t.Fatalf("second excludeAiderArtifacts: %v", err)
	}
	data, err = os.ReadFile(excludeFile(wt))
	if err != nil {
		t.Fatalf("reread exclude: %v", err)
	}
	if !strings.HasSuffix(string(data), want) || strings.Count(string(data), want) != 1 {
		t.Errorf("exclude file after a second call = %q, want still the one pattern", data)
	}
	// The exclusion is effective: git ignores the staged prompts.
	if out, err := exec.Command("git", "-C", wt, "check-ignore", "-q",
		filepath.Join(".daedalus-aider", "prompt-1.md")).CombinedOutput(); err != nil {
		t.Errorf("git check-ignore .daedalus-aider/prompt-1.md failed: %v: %s", err, out)
	}

	// A pre-existing exclude entry without a trailing newline gets one
	// before the pattern is appended.
	wt2 := gitRepo(t)
	excl := excludeFile(wt2)
	if err := os.MkdirAll(filepath.Dir(excl), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(excl, []byte("other-artifact/"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := excludeAiderArtifacts(wt2); err != nil {
		t.Fatalf("excludeAiderArtifacts: %v", err)
	}
	data, err = os.ReadFile(excl)
	if err != nil {
		t.Fatalf("read exclude: %v", err)
	}
	if string(data) != "other-artifact/\n.daedalus-aider/\n" {
		t.Errorf("exclude file = %q, want both entries newline-separated", data)
	}
}

// TestParsePiStream pins pi's --mode json parser: the session header line
// carries the conversation id, and the last message_end carrying content is
// the round's final message — intermediate narration and empty turns do not
// dilute it. Non-JSON lines are skipped like the claude parser's.
func TestParsePiStream(t *testing.T) {
	thinking, text, session := parsePiStream(strings.Join([]string{
		`not json at all`,
		`{"type":"session","id":"sess-9"}`,
		`{"type":"message_end","message":{"content":[{"type":"text","text":"draft step"}]}}`,
		`{"type":"message_end","message":{"content":[{"type":"thinking","thinking":"why"},{"type":"text","text":"final answer"}]}}`,
		`{"type":"message_end","message":{"content":[]}}`,
		`{"type":"other"}`,
	}, "\n"))
	if session != "sess-9" {
		t.Errorf("session = %q, want the session header's id", session)
	}
	if thinking != "why" {
		t.Errorf("thinking = %q, want the last message_end's thinking", thinking)
	}
	if text != "final answer" {
		t.Errorf("text = %q, want the last message_end carrying content", text)
	}
}

// TestParsePiUsage pins the usage extraction: usage rides message_update
// events cumulatively, so the last one seen is the round's final figure; a
// stream without message_update events reports a zero Usage.
func TestParsePiUsage(t *testing.T) {
	usage := parsePiUsage(strings.Join([]string{
		`{"type":"session","id":"sess-9"}`,
		`{"type":"message_update","usage":{"input":10,"output":2,"cost":{"total":0.01}}}`,
		`{"type":"message_end","message":{"content":[{"type":"text","text":"hi"}]}}`,
		`{"type":"message_update","usage":{"input":110,"output":22,"cacheRead":5,"cacheWrite":7,"cost":{"total":0.42}}}`,
	}, "\n"))
	if usage.CostUSD != 0.42 || usage.InputTokens != 110 || usage.OutputTokens != 22 ||
		usage.CacheReadTokens != 5 || usage.CacheWriteTokens != 7 {
		t.Errorf("parsePiUsage = %+v, want the last message_update's figures", usage)
	}

	if got := parsePiUsage(`{"type":"message_end","message":{"content":[{"type":"text","text":"hi"}]}}`); got != (Usage{}) {
		t.Errorf("parsePiUsage without message_update = %+v, want the zero Usage", got)
	}
}

// TestParseRoundOutput pins the per-CLI dispatch: pi reads its own event
// schema, claude and amp share the claude schema, and the plain-text CLIs
// (opencode, aider) yield nothing — the caller's cue to take the raw
// stdout.
func TestParseRoundOutput(t *testing.T) {
	piOut := `{"type":"session","id":"sess-p"}` + "\n" +
		`{"type":"message_update","usage":{"input":3,"output":4,"cost":{"total":0.5}}}` + "\n" +
		`{"type":"message_end","message":{"content":[{"type":"text","text":"pi reply"}]}}`
	thinking, text, session, usage := parseRoundOutput("pi", piOut)
	if text != "pi reply" || session != "sess-p" || thinking != "" || usage.CostUSD != 0.5 {
		t.Errorf("parseRoundOutput(pi) = %q, %q, %q, %+v; want pi's reply, session, and cost", thinking, text, session, usage)
	}

	thinking, text, session, usage = parseRoundOutput("claude",
		`{"type":"result","subtype":"success","result":"claude reply","session_id":"sess-c","total_cost_usd":0.2,"duration_ms":900}`)
	if text != "claude reply" || session != "sess-c" || thinking != "" || usage.CostUSD != 0.2 {
		t.Errorf("parseRoundOutput(claude) = %q, %q, %q, %+v; want claude's reply, session, and cost", thinking, text, session, usage)
	}

	for _, agent := range []string{"opencode", "aider"} {
		thinking, text, session, usage = parseRoundOutput(agent, "plain markdown reply\n")
		if thinking != "" || text != "" || session != "" || usage != (Usage{}) {
			t.Errorf("parseRoundOutput(%s) = %q, %q, %q, %+v; want all empty (raw stdout is the text)", agent, thinking, text, session, usage)
		}
	}
}

// TestNativeTestsAIDiscoveryPi pins that a pi-default worker's discovery
// round resolves to pi — the jailed CLI, not claude — and that the command
// is extracted from the round's plain output (discovery rounds run without
// output-mode flags, so the raw-stdout fallback is what carries the reply).
func TestNativeTestsAIDiscoveryPi(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "ai-jail", "echo 'make test'; exit 0")
	stubBin(t, "sh", "echo 'suite green'; exit 0")
	t.Setenv("DAEDALUS_AGENT", "pi")

	result, err := RunNativeTestsActivity(context.Background(), t.TempDir(), "")
	if err != nil {
		t.Fatalf("RunNativeTestsActivity: %v", err)
	}
	if !result.Passed {
		t.Error("Passed = false, want true")
	}
	if result.Command != "sh -c make test" {
		t.Errorf("Command = %q, want the pi-discovered command", result.Command)
	}
	calls := readCalls(t, log)
	if len(calls) != 2 {
		t.Fatalf("called %d times, want 2 (discovery + test run)", len(calls))
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
		"-p",
	}, "discovery jail")
}

// TestNativeTestsAIDiscoveryPlainText pins the plain-text fallback: a CLI
// whose output has no structured events (opencode) still gets its raw
// stdout extracted as the discovered command.
func TestNativeTestsAIDiscoveryPlainText(t *testing.T) {
	newStubLog(t)
	stubBin(t, "ai-jail", "echo 'make test'; exit 0")
	stubBin(t, "sh", "echo 'suite green'; exit 0")
	t.Setenv("DAEDALUS_AGENT", "opencode")

	result, err := RunNativeTestsActivity(context.Background(), t.TempDir(), "")
	if err != nil {
		t.Fatalf("RunNativeTestsActivity: %v", err)
	}
	if !result.Passed {
		t.Error("Passed = false, want true")
	}
	if result.Command != "sh -c make test" {
		t.Errorf("Command = %q, want the raw reply as the discovered command", result.Command)
	}
}
