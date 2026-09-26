package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ozzono/daedalus/internal/activities"
)

// useTaskLogDir points activities.TaskLogDir — the source daemonDir and the
// task-log paths both resolve through — at a fresh temp dir for the test's
// duration and returns it, so no test reads or writes the real /tmp/daedalus.
func useTaskLogDir(t *testing.T) string {
	t.Helper()
	reset := activities.TaskLogDir
	t.Cleanup(func() { activities.TaskLogDir = reset })
	activities.TaskLogDir = t.TempDir()
	return activities.TaskLogDir
}

// writeTaskLogFile seeds the task log for id with lines, creating it in the
// (already redirected) task log dir.
func writeTaskLogFile(t *testing.T, id string, lines ...string) {
	t.Helper()
	path, err := activities.TaskLogPath(id)
	if err != nil {
		t.Fatalf("TaskLogPath(%s): %v", id, err)
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestRunningRound pins the current-round derivation from a task log: the
// last "round started" block without a matching "round exited" names the
// running agent (its stage) and worktree, an exit block drops the state back
// to idle, and nothing else can — test-suite blocks embed an arbitrary test
// command in the header, and escaped (writer-prefixed) body lines still carry
// header-shaped text, so both must be inert (see
// backlog/bugs/tasklog-test-command-round-marker-corruption.md and
// backlog/bugs/tasklog-brief-forged-header-lines.md).
func TestRunningRound(t *testing.T) {
	const startedDev = "=== 2026-09-20T10:00:00Z jailed claude round started: stage=dev worktree=/home/x/wt pgid=4242 (run abcd1234) ==="
	const exited = "=== 2026-09-20T10:05:00Z jailed claude round exited after 5m0s (run abcd1234) ==="

	cases := []struct {
		name         string
		log          []string
		wantAgent    string
		wantWorktree string
	}{
		{
			name:         "empty log is idle with no worktree",
			wantAgent:    "idle",
			wantWorktree: "",
		},
		{
			name:         "a started block names the running round",
			log:          []string{startedDev},
			wantAgent:    "dev",
			wantWorktree: "/home/x/wt",
		},
		{
			name: "an exited block returns to idle",
			log:  []string{startedDev, "stdout of the round", exited},
			// The worktree of the finished round is kept: the brief keeps
			// reporting last-update lines for an idle agent instead of
			// silently dropping them.
			wantAgent:    "idle",
			wantWorktree: "/home/x/wt",
		},
		{
			name: "the latest started block wins",
			log: []string{startedDev, exited,
				"=== 2026-09-20T10:06:00Z jailed claude round started: stage=test-review worktree=/home/x/wt pgid=4243 (run abcd1234) ==="},
			wantAgent:    "test-review",
			wantWorktree: "/home/x/wt",
		},
		{
			name: "a test-suite header with a round-shaped command is inert",
			log: []string{startedDev,
				"=== 2026-09-20T10:01:00Z test suite exited after 5s: echo jailed claude round started: stage=evil worktree=/evil pgid=1 (run abcd1234) ===",
				"=== 2026-09-20T10:02:00Z test suite exited after 5s: go test ./... (run abcd1234) ==="},
			wantAgent:    "dev",
			wantWorktree: "/home/x/wt",
		},
		{
			name: "an escaped body header line is inert",
			log: []string{startedDev,
				" === 2026-09-20T10:03:00Z jailed amp round started: stage=evil worktree=/evil pgid=9 (run deadbeef) ==="},
			wantAgent:    "dev",
			wantWorktree: "/home/x/wt",
		},
		{
			name:         "plain output lines are inert",
			log:          []string{"some agent stdout", "=== just a section header in output", "more output"},
			wantAgent:    "idle",
			wantWorktree: "",
		},
		{
			name: "an empty stage reads as unknown",
			log:  []string{"=== 2026-09-20T10:00:00Z jailed claude round started: stage= worktree=/wt pgid=1 (run abcd1234) ==="},
			// The agent falls back; the worktree is still derived.
			wantAgent:    "unknown",
			wantWorktree: "/wt",
		},
		{
			name:         "a non-jailed event never sets the state",
			log:          []string{"=== 2026-09-20T10:00:00Z test suite exited after 5s: go test ./... (run abcd1234) ==="},
			wantAgent:    "idle",
			wantWorktree: "",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			agent, worktree := runningRound(strings.Join(c.log, "\n"))
			if agent != c.wantAgent {
				t.Errorf("runningRound agent = %q, want %q", agent, c.wantAgent)
			}
			if worktree != c.wantWorktree {
				t.Errorf("runningRound worktree = %q, want %q", worktree, c.wantWorktree)
			}
		})
	}
}

// TestDigestLastEntry pins the transcript digest: the last parseable json
// line renders as "<type> <tool name>" for tool_use content, "<type>
// <snippet>" for text content, and the bare type when the shape is not
// recognized; a trailing partial line (an entry mid-write) is skipped; a
// file with nothing parseable at all renders as <unreadable transcript>.
func TestDigestLastEntry(t *testing.T) {
	longText := `{"type":"assistant","message":{"content":[{"type":"text","text":"` +
		strings.Repeat("a", 85) + `"}]}}`

	cases := []struct {
		name, transcript, want string
	}{
		{
			name:       "empty file",
			transcript: "",
			want:       "<unreadable transcript>",
		},
		{
			name:       "nothing parseable",
			transcript: "not json at all",
			want:       "<unreadable transcript>",
		},
		{
			name:       "tool_use names the tool",
			transcript: `{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{}}]}}`,
			want:       "assistant Bash",
		},
		{
			name:       "text content is snippeted",
			transcript: `{"type":"assistant","message":{"content":[{"type":"text","text":"hello there"}]}}`,
			want:       "assistant hello there",
		},
		{
			name:       "string content is snippeted",
			transcript: `{"type":"user","message":{"content":"a plain string"}}`,
			want:       "user a plain string",
		},
		{
			name:       "long text truncates at 80 runes",
			transcript: longText,
			want:       "assistant " + strings.Repeat("a", 80) + "…",
		},
		{
			name:       "newlines and tabs collapse to one line",
			transcript: `{"type":"assistant","message":{"content":"first line\nsecond\tline"}}`,
			want:       "assistant first line second line",
		},
		{
			name:       "trailing partial entry is skipped",
			transcript: `{"type":"assistant","message":{"content":"complete"}}` + "\n" + `{"type":"assi`,
			want:       "assistant complete",
		},
		{
			name:       "unrecognized shape falls back to the bare type",
			transcript: `{"type":"system"}`,
			want:       "system",
		},
		{
			name:       "array without tool_use takes the first item with text",
			transcript: `{"type":"assistant","message":{"content":[{"type":"tool_result","text":"first"},{"type":"text","text":"second"}]}}`,
			want:       "assistant first",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := digestLastEntry(c.transcript); got != c.want {
				t.Errorf("digestLastEntry(%q) = %q, want %q", c.transcript, got, c.want)
			}
		})
	}
}

// TestLastTranscriptUpdate pins the transcript lookup: the newest *.jsonl in
// the directory wins (other extensions ignored), and a missing or
// transcript-free directory reports ok=false so the brief says so instead of
// guessing.
func TestLastTranscriptUpdate(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string, mtime time.Time) {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	new := time.Now().Add(-time.Hour).Truncate(time.Second)
	write("a.jsonl", `{"type":"assistant","message":{"content":"older transcript"}}`, old)
	write("b.jsonl", `{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash"}]}}`, new)
	write("c.txt", "not a transcript, and newer still", new)

	mtime, digest, ok := lastTranscriptUpdate(dir)
	if !ok {
		t.Fatal("lastTranscriptUpdate = !ok, want the newest .jsonl")
	}
	if !mtime.Equal(new) {
		t.Errorf("mtime = %v, want the newest .jsonl's %v", mtime, new)
	}
	if digest != "assistant Bash" {
		t.Errorf("digest = %q, want the newest transcript's last entry", digest)
	}

	// A directory without transcripts — and a missing one — report no data.
	empty := t.TempDir()
	if _, _, ok := lastTranscriptUpdate(empty); ok {
		t.Error("lastTranscriptUpdate(empty dir) = ok, want !ok")
	}
	if _, _, ok := lastTranscriptUpdate(filepath.Join(dir, "missing")); ok {
		t.Error("lastTranscriptUpdate(missing dir) = ok, want !ok")
	}
}

// TestTaskStatusBrief pins the brief's render from a log plus transcripts:
// the running round's agent, the newest transcript's mtime (UTC RFC3339) and
// digest — and the degraded lines when the transcript side has nothing to
// say. The claude projects dir and pi sessions dir are both faked, so the
// brief is checked purely as a function of its file inputs.
func TestTaskStatusBrief(t *testing.T) {
	fakeProjectsDir := func(t *testing.T, dir string, err error) {
		t.Helper()
		reset := claudeProjectsDir
		t.Cleanup(func() { claudeProjectsDir = reset })
		claudeProjectsDir = func(string) (string, error) { return dir, err }
	}
	fakePiSessionsDir := func(t *testing.T, dir string, err error) {
		t.Helper()
		reset := piSessionsDir
		t.Cleanup(func() { piSessionsDir = reset })
		piSessionsDir = func(string) (string, error) { return dir, err }
	}
	// seedTranscript writes a one-entry transcript named name into dir with
	// the given mtime; the digest the brief must report is derived from the
	// same content.
	seedTranscript := func(t *testing.T, dir, name, digest string, mtime time.Time) {
		t.Helper()
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, name)
		content := `{"type":"assistant","message":{"content":"` + digest + `"}}`
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("running round with transcripts", func(t *testing.T) {
		dir := t.TempDir()
		mtime := time.Now().Add(-time.Minute).Truncate(time.Second)
		transcript := filepath.Join(dir, "session.jsonl")
		if err := os.WriteFile(transcript, []byte(`{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash"}]}}`), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(transcript, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		fakeProjectsDir(t, dir, nil)
		fakePiSessionsDir(t, t.TempDir(), nil)

		out := captureStdout(t, func() {
			taskStatusBrief("=== 2026-09-20T10:00:00Z jailed claude round started: stage=dev worktree=/wt pgid=1 (run abcd1234) ===\n")
		})
		want := "current agent: dev\nlast update: " + mtime.UTC().Format(time.RFC3339) + "\nlast update text: assistant Bash\n"
		if out != want {
			t.Errorf("brief = %q, want %q", out, want)
		}
	})

	t.Run("idle log with no worktree stops at the agent line", func(t *testing.T) {
		out := captureStdout(t, func() { taskStatusBrief("") })
		if out != "current agent: idle\n" {
			t.Errorf("brief = %q, want only the idle agent line", out)
		}
	})

	t.Run("transcript dir unreadable", func(t *testing.T) {
		fakeProjectsDir(t, "", os.ErrPermission)
		fakePiSessionsDir(t, t.TempDir(), nil)
		out := captureStdout(t, func() {
			taskStatusBrief("=== 2026-09-20T10:00:00Z jailed claude round started: stage=dev worktree=/wt pgid=1 (run abcd1234) ===\n")
		})
		if out != "current agent: dev\nlast update: unknown\n" {
			t.Errorf("brief = %q, want the unknown-update degraded line", out)
		}
	})

	t.Run("no transcripts yet", func(t *testing.T) {
		fakeProjectsDir(t, t.TempDir(), nil)
		fakePiSessionsDir(t, t.TempDir(), nil)
		out := captureStdout(t, func() {
			taskStatusBrief("=== 2026-09-20T10:00:00Z jailed claude round started: stage=dev worktree=/wt pgid=1 (run abcd1234) ===\n")
		})
		if out != "current agent: dev\nlast update: no transcripts yet\n" {
			t.Errorf("brief = %q, want the no-transcripts degraded line", out)
		}
	})

	// The pi fallback: the brief reports whichever agent's transcript was
	// written last, so a stale transcript on one side never shadows a live
	// session on the other.
	const briefLog = "=== 2026-09-20T10:00:00Z jailed claude round started: stage=dev worktree=/wt pgid=1 (run abcd1234) ===\n"
	t.Run("newer pi session wins over a stale claude transcript", func(t *testing.T) {
		claudeDir, piDir := t.TempDir(), t.TempDir()
		old := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
		new := time.Now().Add(-time.Hour).Truncate(time.Second)
		seedTranscript(t, claudeDir, "claude.jsonl", "stale claude entry", old)
		seedTranscript(t, piDir, "pi.jsonl", "live pi entry", new)
		fakeProjectsDir(t, claudeDir, nil)
		fakePiSessionsDir(t, piDir, nil)

		out := captureStdout(t, func() { taskStatusBrief(briefLog) })
		want := "current agent: dev\nlast update: " + new.UTC().Format(time.RFC3339) + "\nlast update text: assistant live pi entry\n"
		if out != want {
			t.Errorf("brief = %q, want the pi session's mtime and digest", out)
		}
	})

	t.Run("newer claude transcript wins over a stale pi session", func(t *testing.T) {
		claudeDir, piDir := t.TempDir(), t.TempDir()
		old := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
		new := time.Now().Add(-time.Hour).Truncate(time.Second)
		seedTranscript(t, claudeDir, "claude.jsonl", "live claude entry", new)
		seedTranscript(t, piDir, "pi.jsonl", "stale pi entry", old)
		fakeProjectsDir(t, claudeDir, nil)
		fakePiSessionsDir(t, piDir, nil)

		out := captureStdout(t, func() { taskStatusBrief(briefLog) })
		want := "current agent: dev\nlast update: " + new.UTC().Format(time.RFC3339) + "\nlast update text: assistant live claude entry\n"
		if out != want {
			t.Errorf("brief = %q, want the claude transcript's mtime and digest", out)
		}
	})

	t.Run("pi session reports when claude has no transcripts", func(t *testing.T) {
		piDir := t.TempDir()
		mtime := time.Now().Add(-time.Minute).Truncate(time.Second)
		seedTranscript(t, piDir, "pi.jsonl", "pi only entry", mtime)
		fakeProjectsDir(t, t.TempDir(), nil)
		fakePiSessionsDir(t, piDir, nil)

		out := captureStdout(t, func() { taskStatusBrief(briefLog) })
		want := "current agent: dev\nlast update: " + mtime.UTC().Format(time.RFC3339) + "\nlast update text: assistant pi only entry\n"
		if out != want {
			t.Errorf("brief = %q, want the pi session's line instead of the no-transcripts degraded line", out)
		}
	})

	t.Run("unresolvable pi dir is inert", func(t *testing.T) {
		claudeDir := t.TempDir()
		mtime := time.Now().Add(-time.Minute).Truncate(time.Second)
		seedTranscript(t, claudeDir, "claude.jsonl", "claude entry", mtime)
		fakeProjectsDir(t, claudeDir, nil)
		fakePiSessionsDir(t, "", os.ErrPermission)

		out := captureStdout(t, func() { taskStatusBrief(briefLog) })
		want := "current agent: dev\nlast update: " + mtime.UTC().Format(time.RFC3339) + "\nlast update text: assistant claude entry\n"
		if out != want {
			t.Errorf("brief = %q, want the claude transcript's line with the pi side ignored", out)
		}
	})
}

// TestRunTaskLogPrintsFile pins the raw mode in-process (its success path
// never exits): the log file's bytes verbatim on stdout.
func TestRunTaskLogPrintsFile(t *testing.T) {
	useTaskLogDir(t)
	content := "=== 2026-09-20T10:00:00Z jailed claude round started: stage=dev worktree=/wt pgid=1 (run abcd1234) ===\nround output\n"
	writeTaskLogFile(t, "wf-raw", strings.TrimSuffix(content, "\n"))

	out := captureStdout(t, func() { runTaskLog("wf-raw", false) })
	if out != content {
		t.Errorf("daedalus log printed %q, want the file verbatim %q", out, content)
	}
}

// TestMainLogDispatch pins the `daedalus log` dispatch wiring in-process:
// log is config-exempt (it runs with no config resolvable anywhere) and
// --status switches to the brief. The task log dir is redirected, so the
// real /tmp/daedalus is never touched.
func TestMainLogDispatch(t *testing.T) {
	useTaskLogDir(t)
	writeTaskLogFile(t, "wf-dispatch",
		"=== 2026-09-20T10:00:00Z jailed claude round started: stage=dev worktree=/wt pgid=1 (run abcd1234) ===")

	t.Setenv("HOME", t.TempDir()) // no ~/.config/daedalus/config.yaml fallback
	t.Chdir(t.TempDir())          // no ./config.yaml

	t.Run("raw mode prints the file", func(t *testing.T) {
		args := os.Args
		t.Cleanup(func() { os.Args = args })
		os.Args = []string{"daedalus", "log", "wf-dispatch"}
		out := captureStdout(t, main)
		if !strings.Contains(out, "round started") {
			t.Errorf("daedalus log output %q should carry the log's blocks", out)
		}
	})

	t.Run("--status prints the brief", func(t *testing.T) {
		// No transcripts on disk: the brief's degraded line proves the
		// dispatch reached the brief, not the raw printer.
		reset := claudeProjectsDir
		t.Cleanup(func() { claudeProjectsDir = reset })
		claudeProjectsDir = func(string) (string, error) { return t.TempDir(), nil }
		resetPi := piSessionsDir
		t.Cleanup(func() { piSessionsDir = resetPi })
		piSessionsDir = func(string) (string, error) { return t.TempDir(), nil }

		args := os.Args
		t.Cleanup(func() { os.Args = args })
		os.Args = []string{"daedalus", "log", "wf-dispatch", "--status"}
		out := captureStdout(t, main)
		if !strings.HasPrefix(out, "current agent: dev\nlast update: no transcripts yet\n") {
			t.Errorf("daedalus log --status output %q, want the status brief", out)
		}
	})
}

// TestMainTaskLogMissingExits pins the not-started error through the CLI
// (subprocess: exitf calls os.Exit): a workflow id with no log file exits 1
// with the not-started diagnostic — the same either way, --status included.
// The id is chosen so no real run can hold it; nothing is written.
func TestMainTaskLogMissingExits(t *testing.T) {
	for _, args := range [][]string{
		{"log", "daedalus-tasklog-missing-probe"},
		{"log", "daedalus-tasklog-missing-probe", "--status"},
	} {
		stdout, stderr, code := runMainIn(t, "", args...)
		if code != 1 {
			t.Errorf("daedalus %v exit code = %d, want 1", args, code)
		}
		if stdout != "" {
			t.Errorf("daedalus %v stdout = %q, want empty", args, stdout)
		}
		want := "no task log for daedalus-tasklog-missing-probe at /tmp/daedalus/daedalus-tasklog-missing-probe.log"
		if !strings.HasPrefix(stderr, want) {
			t.Errorf("daedalus %v stderr = %q, want the not-started diagnostic", args, stderr)
		}
		if strings.Contains(stderr, "Usage:") {
			t.Errorf("daedalus %v must not append the usage text, got %q", args, stderr)
		}
	}
}

// TestMainTaskLogArgCount pins the argument-count contract (subprocess:
// usageFail exits): `log` takes exactly one workflow id.
func TestMainTaskLogArgCount(t *testing.T) {
	for _, args := range [][]string{{"log"}, {"log", "a", "b"}} {
		stdout, stderr, code := runMainIn(t, "", args...)
		if code != 1 {
			t.Errorf("daedalus %v exit code = %d, want 1", args, code)
		}
		if stdout != "" {
			t.Errorf("daedalus %v stdout = %q, want empty", args, stdout)
		}
		if !strings.HasPrefix(stderr, "log takes <workflow-id>\n\n") || !strings.HasSuffix(stderr, usage) {
			t.Errorf("daedalus %v stderr = %q, want the usage-fail shape (diagnostic, blank line, usage)", args, stderr)
		}
	}
}

// TestInlineCoT pins the <think>-fence extraction behind -cot's inline-CoT
// view: only fenced reasoning is split from the visible text, whitespace is
// trimmed, and an unclosed fence still holds what the round thought. Rounds
// that reasoned inline without tags render as no CoT — tagless text cannot
// be told apart from the answer, so it is never guessed at.
func TestInlineCoT(t *testing.T) {
	cases := []struct {
		name, text, want string
	}{
		{
			name: "fenced reasoning is split from the answer",
			text: "Working on it.\n<think>\nneed to check the diff first\n</think>\nDone, here is the summary.",
			want: "need to check the diff first",
		},
		{
			name: "whitespace around the fence body is trimmed",
			text: "<think>\n  spaced out  \n</think>",
			want: "spaced out",
		},
		{
			name: "an unclosed fence still holds the reasoning",
			text: "prefix text <think>cut off mid-thought",
			want: "cut off mid-thought",
		},
		{
			name: "an empty fence renders as no CoT",
			text: "<think></think>answer",
			want: "",
		},
		{
			name: "no fence at all renders as no CoT",
			text: "a plain answer",
			want: "",
		},
		{
			name: "tagless inline reasoning is not guessed at",
			text: "Let me reason step by step. First the diff, then the tests. The answer is 4.",
			want: "",
		},
		{
			name: "a stray close tag before the open one does not end the fence",
			text: "</think>earlier text<think>the thought",
			want: "the thought",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := inlineCoT(c.text); got != c.want {
				t.Errorf("inlineCoT(%q) = %q, want %q", c.text, got, c.want)
			}
		})
	}
}

// TestTaskLogCotFlags pins -cot's parse-time surface: both spellings are
// accepted on `daedalus log <id>`, -cot is rejected anywhere else with the
// same wording --status gets, and one invocation prints one view — --status
// and -cot together are refused.
func TestTaskLogCotFlags(t *testing.T) {
	f, _, err := parseFlags([]string{"log", "wf-1", "-cot"})
	if err != nil || !f.cot {
		t.Errorf("parseFlags(log wf-1 -cot) = %v, %v, want cot set with no error", f, err)
	}

	f, _, err = parseFlags([]string{"log", "wf-1", "--cot"})
	if err != nil || !f.cot {
		t.Errorf("parseFlags(log wf-1 --cot) = %v, %v, want cot set with no error", f, err)
	}

	if _, _, err := parseFlags([]string{"-cot", "worker", "status"}); err == nil ||
		err.Error() != "-cot only applies to daedalus log <workflow-id>" {
		t.Errorf("parseFlags(-cot worker status) err = %v, want the cot-only-on-log rejection", err)
	}

	if _, _, err := parseFlags([]string{"log", "wf-1", "--status", "-cot"}); err == nil ||
		err.Error() != "--status and -cot are mutually exclusive" {
		t.Errorf("parseFlags(log wf-1 --status -cot) err = %v, want the mutual-exclusion rejection", err)
	}
}

// TestMainLogCotDispatch pins the `-cot` dispatch through the CLI
// (subprocess: runTaskLogCot dials Temporal, and exitf exits): with a
// resolved config pointing at a dead host, the invocation fails with that
// host's connect error — proving -cot routed to the CoT view and took its
// Temporal address from the resolved config file, rather than falling
// through to the raw printer (which never dials and would report the
// missing task log instead).
func TestMainLogCotDispatch(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfg, []byte("agent: claude\ntemporal:\n  host: 127.0.0.1:1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runMainIn(t, dir, "log", "daedalus-cot-dial-probe", "-cot")
	if code != 1 {
		t.Errorf("daedalus log <id> -cot exit code = %d, want 1", code)
	}
	if stdout != "" {
		t.Errorf("daedalus log <id> -cot stdout = %q, want empty", stdout)
	}
	if !strings.HasPrefix(stderr, "connect to temporal at 127.0.0.1:1: ") {
		t.Errorf("daedalus log <id> -cot stderr = %q, want the config-host connect error", stderr)
	}
	if strings.Contains(stderr, "no task log") {
		t.Errorf("daedalus log <id> -cot stderr = %q, must not fall through to the raw log", stderr)
	}
}

// TestLastRoundState pins the extended round walk behind both --status and
// the -cot live section: alongside the stage and worktree it keeps the
// jailed agent's name and whether the round is still in flight — a start
// block sets running, a matching exit drops it, the latest block wins, and
// the same inert lines as runningRound can never set the state. The stage
// is returned raw (an empty "stage=" stays empty); the "unknown" fallback
// is each caller's render decision.
func TestLastRoundState(t *testing.T) {
	const startedClaude = "=== 2026-09-25T10:00:00Z jailed claude round started: stage=dev worktree=/home/x/wt pgid=4242 (run abcd1234) ==="
	const exitedClaude = "=== 2026-09-25T10:05:00Z jailed claude round exited after 5m0s (run abcd1234) ==="

	cases := []struct {
		name              string
		log               []string
		wantName, wantStg string
		wantWorktree      string
		wantRunning       bool
	}{
		{
			name: "empty log has no round state",
		},
		{
			name:         "a started block keeps the agent name",
			log:          []string{startedClaude},
			wantName:     "claude",
			wantStg:      "dev",
			wantWorktree: "/home/x/wt",
			wantRunning:  true,
		},
		{
			name:         "an exit block drops running but keeps the round",
			log:          []string{startedClaude, exitedClaude},
			wantName:     "claude",
			wantStg:      "dev",
			wantWorktree: "/home/x/wt",
		},
		{
			name: "the latest started block wins, name included",
			log: []string{startedClaude, exitedClaude,
				"=== 2026-09-25T10:06:00Z jailed pi round started: stage=test-review worktree=/home/x/wt2 pgid=4243 (run abcd1234) ==="},
			wantName:     "pi",
			wantStg:      "test-review",
			wantWorktree: "/home/x/wt2",
			wantRunning:  true,
		},
		{
			name:         "an empty stage is returned raw",
			log:          []string{"=== 2026-09-25T10:00:00Z jailed aider round started: stage= worktree=/wt pgid=1 (run abcd1234) ==="},
			wantName:     "aider",
			wantWorktree: "/wt",
			wantRunning:  true,
		},
		{
			name: "forged and escaped header lines are inert",
			log: []string{startedClaude,
				"=== 2026-09-25T10:01:00Z test suite exited after 5s: echo jailed pi round started: stage=evil worktree=/evil pgid=1 (run abcd1234) ===",
				" === 2026-09-25T10:02:00Z jailed amp round started: stage=evil worktree=/evil pgid=9 (run deadbeef) ==="},
			wantName:     "claude",
			wantStg:      "dev",
			wantWorktree: "/home/x/wt",
			wantRunning:  true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			name, stage, worktree, running := lastRoundState(strings.Join(c.log, "\n"))
			if name != c.wantName || stage != c.wantStg || worktree != c.wantWorktree || running != c.wantRunning {
				t.Errorf("lastRoundState = (%q, %q, %q, %v), want (%q, %q, %q, %v)",
					name, stage, worktree, running, c.wantName, c.wantStg, c.wantWorktree, c.wantRunning)
			}
		})
	}
}

// TestNewestTranscript pins the newest-wins transcript lookup behind --status
// and the live section: claude's project dir and pi's session dir are both
// consulted and the newest file wins with its agent's name — a stale
// transcript on one side must not shadow a live session on the other — and
// an unresolvable claude dir is an error, while an unresolvable pi dir is
// simply ignored.
func TestNewestTranscript(t *testing.T) {
	fakeDirs := func(t *testing.T, claudeDir, piDir string, claudeErr, piErr error) {
		t.Helper()
		resetC := claudeProjectsDir
		t.Cleanup(func() { claudeProjectsDir = resetC })
		claudeProjectsDir = func(string) (string, error) { return claudeDir, claudeErr }
		resetP := piSessionsDir
		t.Cleanup(func() { piSessionsDir = resetP })
		piSessionsDir = func(string) (string, error) { return piDir, piErr }
	}
	seed := func(t *testing.T, dir, name string, mtime time.Time) string {
		t.Helper()
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("{}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		return path
	}
	old := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	new := time.Now().Add(-time.Hour).Truncate(time.Second)

	t.Run("newest file wins with its agent", func(t *testing.T) {
		claudeDir, piDir := t.TempDir(), t.TempDir()
		want := seed(t, piDir, "pi.jsonl", new)
		seed(t, claudeDir, "claude.jsonl", old)
		fakeDirs(t, claudeDir, piDir, nil, nil)

		path, agent, mtime, ok, err := newestTranscript("/wt")
		if err != nil || !ok || path != want || agent != "pi" || !mtime.Equal(new) {
			t.Errorf("newestTranscript = (%q, %q, %v, %v, %v), want the newer pi file", path, agent, mtime, ok, err)
		}
	})

	t.Run("pi-only report when claude has no transcripts", func(t *testing.T) {
		piDir := t.TempDir()
		want := seed(t, piDir, "pi.jsonl", new)
		fakeDirs(t, t.TempDir(), piDir, nil, nil)

		path, agent, _, ok, err := newestTranscript("/wt")
		if err != nil || !ok || path != want || agent != "pi" {
			t.Errorf("newestTranscript = (%q, %q, %v, %v), want the pi file", path, agent, ok, err)
		}
	})

	t.Run("no transcripts anywhere is !ok with no error", func(t *testing.T) {
		fakeDirs(t, t.TempDir(), t.TempDir(), nil, nil)
		// agent is only meaningful when ok — no consumer reads it otherwise.
		path, _, _, ok, err := newestTranscript("/wt")
		if err != nil || ok || path != "" {
			t.Errorf("newestTranscript = (%q, _, _, %v, %v), want !ok with no error", path, ok, err)
		}
	})

	t.Run("unresolvable claude dir is an error", func(t *testing.T) {
		fakeDirs(t, "", "", os.ErrPermission, nil)
		if _, _, _, _, err := newestTranscript("/wt"); err == nil {
			t.Error("newestTranscript err = nil, want the claude dir error")
		}
	})

	t.Run("unresolvable pi dir is inert", func(t *testing.T) {
		claudeDir := t.TempDir()
		want := seed(t, claudeDir, "claude.jsonl", new)
		fakeDirs(t, claudeDir, "", nil, os.ErrPermission)

		path, agent, _, ok, err := newestTranscript("/wt")
		if err != nil || !ok || path != want || agent != "claude" {
			t.Errorf("newestTranscript = (%q, %q, _, %v, %v), want the claude file with pi ignored", path, agent, ok, err)
		}
	})
}

// TestTranscriptCoT pins the live CoT render from one transcript file: the
// assistant's thinking and text blocks in transcript order, joined by blank
// lines and never truncated; tool calls, results, and non-assistant entries
// are omitted; unparsable (mid-write) lines are skipped; and a file with no
// assistant output at all reports found=false so the caller can say so.
func TestTranscriptCoT(t *testing.T) {
	write := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "transcript.jsonl")
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}

	t.Run("claude assistant blocks in order", func(t *testing.T) {
		path := write(t, strings.Join([]string{
			`{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"check the diff"}]}}`,
			`{"type":"user","message":{"content":"tool result the agent must not surface"}}`,
			`{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{}}]}}`,
			`{"type":"assistant","message":{"content":[{"type":"text","text":"here is the answer"}]}}`,
		}, "\n"))

		cot, found := transcriptCoT(path, "claude")
		if !found {
			t.Fatal("transcriptCoT found = false, want the assistant blocks")
		}
		want := "check the diff\n\nhere is the answer\n"
		if cot != want {
			t.Errorf("transcriptCoT = %q, want %q", cot, want)
		}
	})

	t.Run("pi entries are picked by the message role", func(t *testing.T) {
		path := write(t, strings.Join([]string{
			`{"type":"message","message":{"role":"user","content":[{"type":"text","text":"the prompt"}]}}`,
			`{"type":"message","message":{"role":"assistant","content":[{"type":"thinking","thinking":"pi reasoning"},{"type":"text","text":"pi answer"}]}}`,
		}, "\n"))

		cot, found := transcriptCoT(path, "pi")
		if !found {
			t.Fatal("transcriptCoT found = false, want the assistant entry")
		}
		want := "pi reasoning\n\npi answer\n"
		if cot != want {
			t.Errorf("transcriptCoT = %q, want %q", cot, want)
		}
	})

	t.Run("a trailing partial entry is skipped", func(t *testing.T) {
		path := write(t, `{"type":"assistant","message":{"content":[{"type":"text","text":"complete"}]}}`+"\n"+`{"type":"assi`)

		cot, found := transcriptCoT(path, "claude")
		if !found || cot != "complete\n" {
			t.Errorf("transcriptCoT = (%q, %v), want the complete entry only", cot, found)
		}
	})

	t.Run("no assistant output is not found", func(t *testing.T) {
		path := write(t, `{"type":"user","message":{"content":"just the prompt"}}`+"\n"+"\n")

		if cot, found := transcriptCoT(path, "claude"); found || cot != "" {
			t.Errorf("transcriptCoT = (%q, %v), want not found", cot, found)
		}
	})

	t.Run("a missing file is not found", func(t *testing.T) {
		if _, found := transcriptCoT(filepath.Join(t.TempDir(), "missing.jsonl"), "claude"); found {
			t.Error("transcriptCoT found = true for a missing file, want false")
		}
	})
}

// TestLiveCotSection pins the -cot live section as a function of its file
// inputs (the redirected task log plus faked transcript dirs): a section
// only exists while a round is in flight, non-transcript agents get the
// one-line appears-when-complete note, claude and pi get their live CoT,
// and the empty-transcript states degrade to their one-liners — never a
// guess and never a duplicate of a completed round.
func TestLiveCotSection(t *testing.T) {
	fakeDirs := func(t *testing.T, claudeDir, piDir string, claudeErr, piErr error) {
		t.Helper()
		resetC := claudeProjectsDir
		t.Cleanup(func() { claudeProjectsDir = resetC })
		claudeProjectsDir = func(string) (string, error) { return claudeDir, claudeErr }
		resetP := piSessionsDir
		t.Cleanup(func() { piSessionsDir = resetP })
		piSessionsDir = func(string) (string, error) { return piDir, piErr }
	}
	seedTranscript := func(t *testing.T, dir, agent, thinking, text string, mtime time.Time) {
		t.Helper()
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		name := map[string]string{"claude": "claude.jsonl", "pi": "pi.jsonl"}[agent]
		entry := `{"type":"` + map[string]string{"claude": "assistant", "pi": "message"}[agent] +
			`","message":{"role":"assistant","content":[{"type":"thinking","thinking":"` + thinking +
			`"},{"type":"text","text":"` + text + `"}]}}`
		if agent == "claude" {
			entry = `{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"` + thinking +
				`"},{"type":"text","text":"` + text + `"}]}}`
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(entry+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	startedLine := func(agent, stage string) string {
		return "=== 2026-09-25T10:00:00Z jailed " + agent + " round started: stage=" + stage +
			" worktree=/wt pgid=1 (run abcd1234) ==="
	}

	t.Run("no task log is no section", func(t *testing.T) {
		useTaskLogDir(t)
		if got := liveCotSection("wf-live-none"); got != "" {
			t.Errorf("liveCotSection = %q, want empty with no log file", got)
		}
	})

	t.Run("idle log is no section", func(t *testing.T) {
		useTaskLogDir(t)
		writeTaskLogFile(t, "wf-live-idle", startedLine("claude", "dev"),
			"=== 2026-09-25T10:05:00Z jailed claude round exited after 5m0s (run abcd1234) ===")
		if got := liveCotSection("wf-live-idle"); got != "" {
			t.Errorf("liveCotSection = %q, want empty for a finished round", got)
		}
	})

	t.Run("non-transcript agent gets the one-line note", func(t *testing.T) {
		useTaskLogDir(t)
		writeTaskLogFile(t, "wf-live-aider", startedLine("aider", "dev"))
		fakeDirs(t, t.TempDir(), t.TempDir(), nil, nil)

		want := "=== in-flight round (stage=dev, live) ===\n" +
			"aider keeps no host transcript; CoT appears here when the round completes\n\n"
		if got := liveCotSection("wf-live-aider"); got != want {
			t.Errorf("liveCotSection = %q, want %q", got, want)
		}
	})

	t.Run("an empty stage reads as unknown in the header", func(t *testing.T) {
		useTaskLogDir(t)
		writeTaskLogFile(t, "wf-live-unknown", startedLine("aider", ""))
		fakeDirs(t, t.TempDir(), t.TempDir(), nil, nil)

		out := liveCotSection("wf-live-unknown")
		if !strings.HasPrefix(out, "=== in-flight round (stage=unknown, live) ===\n") {
			t.Errorf("liveCotSection = %q, want the unknown-stage header", out)
		}
	})

	t.Run("claude round with a live transcript renders its CoT", func(t *testing.T) {
		useTaskLogDir(t)
		writeTaskLogFile(t, "wf-live-claude", startedLine("claude", "dev"))
		claudeDir := t.TempDir()
		fakeDirs(t, claudeDir, t.TempDir(), nil, nil)
		seedTranscript(t, claudeDir, "claude", "the reasoning", "the answer", time.Now())

		want := "=== in-flight round (stage=dev, live) ===\nthe reasoning\n\nthe answer\n\n"
		if got := liveCotSection("wf-live-claude"); got != want {
			t.Errorf("liveCotSection = %q, want %q", got, want)
		}
	})

	t.Run("a newer pi session wins over a stale claude transcript", func(t *testing.T) {
		useTaskLogDir(t)
		writeTaskLogFile(t, "wf-live-pi", startedLine("pi", "test-review"))
		old := time.Now().Add(-2 * time.Hour)
		new := time.Now().Add(-time.Hour)
		seedTranscript(t, t.TempDir(), "claude", "stale", "stale", old)
		piDir := t.TempDir()
		seedTranscript(t, piDir, "pi", "live pi reasoning", "live pi answer", new)
		fakeDirs(t, t.TempDir(), piDir, nil, nil)

		want := "=== in-flight round (stage=test-review, live) ===\n" +
			"live pi reasoning\n\nlive pi answer\n\n"
		if got := liveCotSection("wf-live-pi"); got != want {
			t.Errorf("liveCotSection = %q, want %q", got, want)
		}
	})

	t.Run("running with no transcript yet degrades to one line", func(t *testing.T) {
		useTaskLogDir(t)
		writeTaskLogFile(t, "wf-live-notyet", startedLine("claude", "dev"))
		fakeDirs(t, t.TempDir(), t.TempDir(), nil, nil)

		want := "=== in-flight round (stage=dev, live) ===\nno transcript yet — no assistant output has landed\n\n"
		if got := liveCotSection("wf-live-notyet"); got != want {
			t.Errorf("liveCotSection = %q, want %q", got, want)
		}
	})

	t.Run("a transcript without assistant output says so", func(t *testing.T) {
		useTaskLogDir(t)
		writeTaskLogFile(t, "wf-live-empty", startedLine("claude", "dev"))
		claudeDir := t.TempDir()
		path := filepath.Join(claudeDir, "claude.jsonl")
		if err := os.WriteFile(path, []byte(`{"type":"user","message":{"content":"the prompt"}}`+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		fakeDirs(t, claudeDir, t.TempDir(), nil, nil)

		want := "=== in-flight round (stage=dev, live) ===\nno assistant output in the transcript yet\n\n"
		if got := liveCotSection("wf-live-empty"); got != want {
			t.Errorf("liveCotSection = %q, want %q", got, want)
		}
	})
}

// TestTaskLogFlags pins `log`'s option surface at parse time: --status is
// accepted on (and only on) `daedalus log <id>`, and -c/--config is rejected
// there with log's own wording — the log is read by name alone, while the
// worker commands keep their shared rejection.
func TestTaskLogFlags(t *testing.T) {
	f, rest, err := parseFlags([]string{"log", "wf-1"})
	if err != nil || f.status {
		t.Errorf("parseFlags(log wf-1) = %v, %v, want status unset with no error", f, err)
	}
	if !isTaskLog(rest) {
		t.Errorf("rest = %v, want isTaskLog", rest)
	}

	f, _, err = parseFlags([]string{"log", "wf-1", "--status"})
	if err != nil || !f.status {
		t.Errorf("parseFlags(log wf-1 --status) = %v, %v, want status set with no error", f, err)
	}

	if _, _, err := parseFlags([]string{"-c", "/no/such/cfg.yaml", "log", "wf-1"}); err == nil ||
		err.Error() != "-c/--config does not apply to daedalus log" {
		t.Errorf("parseFlags(-c ... log) err = %v, want log's own config rejection", err)
	}

	if _, _, err := parseFlags([]string{"--status", "worker", "status"}); err == nil ||
		err.Error() != "--status only applies to daedalus log <workflow-id>" {
		t.Errorf("parseFlags(--status worker status) err = %v, want the status-only-on-log rejection", err)
	}

	if isTaskLog([]string{"log"}) || isTaskLog([]string{"log", "a", "b"}) || isTaskLog([]string{"logs", "a"}) {
		t.Error("isTaskLog accepted a non `log <id>` argument list")
	}
}
