package activities

import (
	"context"
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
	if len(calls) != 5 {
		t.Fatalf("git called %d times, want 5 (remove, prune, branch -D, branch --list, add)", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{"-C", "/repo", "worktree", "remove", worktreePath, "--force"}, "pre-clean remove")
	assertArgs(t, calls[1].Args, []string{"-C", "/repo", "worktree", "prune"}, "pre-clean prune")
	assertArgs(t, calls[2].Args, []string{"-C", "/repo", "branch", "-D", "feat/issue-42-123"}, "pre-clean branch delete")
	assertArgs(t, calls[3].Args, []string{"-C", "/repo", "branch", "--list", "feat/issue-42-*", "--format=%(refname:short)"}, "pre-clean stale branch sweep")
	assertArgs(t, calls[4].Args, []string{"-C", "/repo", "worktree", "add", worktreePath, "-b", "feat/issue-42-123"}, "worktree add")
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
	if len(calls) != 5 {
		t.Fatalf("git called %d times, want 5", len(calls))
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
// stale branches: each is deleted, in addition to the run's own branch.
func TestCleanupWorktreeActivitySweepsStaleBranches(t *testing.T) {
	fakeHome(t)
	log := newStubLog(t)
	// branch --list reports two stale in-flight branches from crashed runs.
	stubBin(t, "git", `case "$4" in
  remove) exit 0 ;;
  --list) printf 'feat/issue-42-111\nfeat/issue-42-222\n'; exit 0 ;;
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
	// remove, prune, branch -D, list, -D, -D
	if len(calls) != 6 {
		t.Fatalf("git called %d times, want 6 (remove, prune, branch -D, list, 2 stale deletes)", len(calls))
	}
	assertArgs(t, calls[2].Args, []string{"-C", "/repo", "branch", "-D", "feat/issue-42-7"}, "branch delete")
	assertArgs(t, calls[4].Args, []string{"-C", "/repo", "branch", "-D", "feat/issue-42-111"}, "stale branch delete 1")
	assertArgs(t, calls[5].Args, []string{"-C", "/repo", "branch", "-D", "feat/issue-42-222"}, "stale branch delete 2")
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
	// The prompt must travel via stdin, not argv; the agent runs with
	// stream-json output so thinking and text come back structured.
	assertArgs(t, calls[0].Args, []string{
		"--worktree",
		"--network",
		"claude",
		"--output-format",
		"stream-json",
		"--verbose",
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
// decides, and anything not exactly APPROVED counts as changes requested.
func TestParseReviewVerdict(t *testing.T) {
	cases := []struct {
		name     string
		out      string
		approved bool
		comments string
	}{
		{"approved", "Looks good.\nAPPROVED\n", true, "Looks good."},
		{"changes requested", "Do X.\nCHANGES_REQUESTED", false, "Do X."},
		{"trailing blank lines", "fine\nAPPROVED\n\n\n", true, "fine"},
		{"no marker keeps whole output", "the error path is untested", false, "the error path is untested"},
		{"lowercase is not approved", "fine\napproved", false, "fine\napproved"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseReviewVerdict(tc.out)
			if got.Approved != tc.approved {
				t.Errorf("Approved = %v, want %v", got.Approved, tc.approved)
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

func TestRunNativeTestsActivityPass(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "go", "echo 'ok all packages'; exit 0")

	worktree := t.TempDir()
	result, err := RunNativeTestsActivity(context.Background(), worktree)
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

	result, err := RunNativeTestsActivity(context.Background(), t.TempDir())
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

	result, err := RunNativeTestsActivity(context.Background(), t.TempDir())
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

	result, err := RunNativeTestsActivity(context.Background(), t.TempDir())
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
	if len(calls) != 4 {
		t.Fatalf("git called %d times, want 4 (remove, prune, branch -D, stale sweep)", len(calls))
	}
	assertArgs(t, calls[0].Args, []string{"-C", "/repo", "worktree", "remove", worktreePath, "--force"}, "remove")
	assertArgs(t, calls[1].Args, []string{"-C", "/repo", "worktree", "prune"}, "prune")
	assertArgs(t, calls[2].Args, []string{"-C", "/repo", "branch", "-D", "feat/issue-42-7"}, "branch delete")
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
		t.Fatalf("git called %d times, want 4 (remove attempted, prune and branch still run)", len(calls))
	}
	assertArgs(t, calls[1].Args, []string{"-C", "/repo", "worktree", "prune"}, "prune after failed remove")
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
