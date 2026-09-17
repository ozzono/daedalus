package activities

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
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
// single result object's Result field becomes the text.
func TestParseAgentStreamJSONFormat(t *testing.T) {
	thinking, text := parseAgentStream(`{"type":"result","subtype":"success","result":"did the change"}`)
	if thinking != "" {
		t.Errorf("thinking = %q, want empty", thinking)
	}
	if text != "did the change" {
		t.Errorf("text = %q, want %q", text, "did the change")
	}
}

// TestParseAgentStreamNoise pins tolerance: non-JSON lines and non-assistant
// events are skipped rather than fatal.
func TestParseAgentStreamNoise(t *testing.T) {
	stdout := "not json at all\n" +
		`{"type":"stream_event","event":{"type":"content_block_delta"}}` + "\n" +
		`{"type":"assistant","message":{"content":[{"type":"text","text":"ok"}]}}` + "\n"
	thinking, text := parseAgentStream(stdout)
	if thinking != "" {
		t.Errorf("thinking = %q, want empty", thinking)
	}
	if text != "ok" {
		t.Errorf("text = %q, want %q", text, "ok")
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
