package activities

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
)

// useTaskLogDir points TaskLogDir at a fresh temp dir for the test's
// duration and returns it, so block-format tests never read or write the
// real /tmp/daedalus.
func useTaskLogDir(t *testing.T) string {
	t.Helper()
	reset := TaskLogDir
	t.Cleanup(func() { TaskLogDir = reset })
	TaskLogDir = t.TempDir()
	return TaskLogDir
}

// TestTaskLogPath pins the task log's file layout and id validation: a
// valid workflow id maps to <dir>/<id>.log, and an id that could escape the
// daemon directory — empty, relative, or carrying a separator — is rejected
// before any path is built, since the id embeds the operator-supplied issue
// id.
func TestTaskLogPath(t *testing.T) {
	dir := useTaskLogDir(t)

	path, err := TaskLogPath("daedalus-issue-42")
	if err != nil {
		t.Fatalf("TaskLogPath(daedalus-issue-42): %v", err)
	}
	if want := filepath.Join(dir, "daedalus-issue-42.log"); path != want {
		t.Errorf("TaskLogPath(daedalus-issue-42) = %q, want %q", path, want)
	}

	for _, id := range []string{"", ".", "..", "a/b", `a\b`, "../escape"} {
		if _, err := TaskLogPath(id); err == nil || !strings.Contains(err.Error(), "not a valid path segment") {
			t.Errorf("TaskLogPath(%q) error = %v, want a not-a-valid-path-segment rejection", id, err)
		}
	}
}

// TestWriteTaskLogBlockFormat pins the on-disk block shape: one header line
// "<=== RFC3339> <event> (run <first-8-of-run-id>) ===", the body verbatim,
// one trailing newline — and appends: a second write concatenates, never
// truncating.
func TestWriteTaskLogBlockFormat(t *testing.T) {
	dir := useTaskLogDir(t)

	if err := writeTaskLog("wf-1", "0123456789abcdef", "jailed claude round started: stage=dev worktree=/wt pgid=5", ""); err != nil {
		t.Fatalf("writeTaskLog: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "wf-1.log"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) != 3 || lines[2] != "" {
		t.Fatalf("block after empty body = %q, want header line, blank line, final newline", string(data))
	}
	header := lines[0]
	if !strings.HasPrefix(header, "=== ") || !strings.HasSuffix(header, " ===") {
		t.Fatalf("header = %q, want the === delimited block header", header)
	}
	fields := strings.Split(header, " ")
	ts, err := time.Parse(time.RFC3339, fields[1])
	if err != nil {
		t.Fatalf("header timestamp %q is not RFC3339: %v", fields[1], err)
	}
	if time.Since(ts) > time.Minute {
		t.Errorf("header timestamp %q is not ~now", fields[1])
	}
	wantHeader := "=== " + fields[1] + " jailed claude round started: stage=dev worktree=/wt pgid=5 (run 01234567) ==="
	if header != wantHeader {
		t.Errorf("header = %q, want %q (event verbatim, run id truncated to 8)", header, wantHeader)
	}

	// A run id of 8 characters or fewer travels untruncated, and a second
	// write appends to the file.
	if err := writeTaskLog("wf-1", "abcd", "test suite exited after 5s: go test ./...", "out line"); err != nil {
		t.Fatalf("writeTaskLog (short run id): %v", err)
	}
	data, err = os.ReadFile(filepath.Join(dir, "wf-1.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "(run abcd) ===\nout line\n") {
		t.Errorf("appended block missing or wrong in %q", string(data))
	}
}

// TestWriteTaskLogEscapesHeaderLines pins the header-escape contract: body
// lines that would look like a block header are prefixed with one space —
// leading "=== " and every embedded "\n=== " — so a parser only ever sees
// writer-produced headers, while all other bodies pass through byte-for-byte
// (trailing newlines trimmed, though; the block supplies the final one).
func TestWriteTaskLogEscapesHeaderLines(t *testing.T) {
	dir := useTaskLogDir(t)

	cases := []struct {
		name, body, want string
	}{
		{
			name: "leading header line",
			body: "=== forged header\nnot a header",
			want: " === forged header\nnot a header",
		},
		{
			name: "embedded header line",
			body: "before\n=== mid\nafter",
			want: "before\n === mid\nafter",
		},
		{
			name: "plain body untouched",
			body: "just output\nmore output",
			want: "just output\nmore output",
		},
		{
			name: "trailing newlines trimmed",
			body: "output\n\n\n",
			want: "output",
		},
	}
	// One workflow id per case, so each file holds exactly one block.
	for i, c := range cases {
		id := "wf-esc-" + strconv.Itoa(i)
		if err := writeTaskLog(id, "abcd1234", "event "+c.name, c.body); err != nil {
			t.Fatalf("writeTaskLog(%s): %v", c.name, err)
		}
		data, err := os.ReadFile(filepath.Join(dir, id+".log"))
		if err != nil {
			t.Fatal(err)
		}
		got := string(data)
		if !strings.HasPrefix(got, "=== ") || !strings.Contains(got, " (run abcd1234) ===\n") {
			t.Fatalf("block for %s = %q, want a well-formed header first", c.name, got)
		}
		// The block carries one trailing newline of its own (pinned in
		// TestWriteTaskLogBlockFormat); strip it so cases compare the body.
		if body := strings.TrimSuffix(got[strings.Index(got, "===\n")+len("===\n"):], "\n"); body != c.want {
			t.Errorf("block %s body = %q, want %q", c.name, body, c.want)
		}
	}
}

// TestWriteTaskLogRejectsBadID pins that a workflow id which cannot validate
// fails the write instead of producing a path outside the daemon dir.
func TestWriteTaskLogRejectsBadID(t *testing.T) {
	useTaskLogDir(t)
	if err := writeTaskLog("../escape", "abcd", "event", "body"); err == nil {
		t.Error("writeTaskLog(../escape) = nil error, want a path-segment rejection")
	}
}

// appendTaskLogProbeActivity is a test-only activity wrapping appendTaskLog,
// so it can run inside a real activity context via the SDK's test
// environment — the path-return contract only applies there.
func appendTaskLogProbeActivity(ctx context.Context, event, body string) (string, error) {
	return appendTaskLog(ctx, event, body), nil
}

// TestAppendTaskLogNoopOutsideActivity pins the unit-test contract of
// appendTaskLog: outside a real activity context there is no workflow id, so
// nothing is written, no file is created, and it returns "" — activities
// invoked directly from tests stay side-effect free and their callers get no
// path to point at.
func TestAppendTaskLogNoopOutsideActivity(t *testing.T) {
	dir := useTaskLogDir(t)
	if got := appendTaskLog(context.Background(), "jailed claude round started: stage=dev worktree=/wt pgid=1", ""); got != "" {
		t.Errorf("appendTaskLog outside an activity context = %q, want \"\"", got)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("appendTaskLog outside an activity context created %v, want nothing", entries)
	}
}

// TestAppendTaskLogReturnsPathInsideActivity pins the path-return contract
// inside a real activity context: the returned path is the task log that was
// just written — it resolves inside the configured dir, opens, and carries
// the record (event and body).
func TestAppendTaskLogReturnsPathInsideActivity(t *testing.T) {
	dir := useTaskLogDir(t)
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(appendTaskLogProbeActivity)

	v, err := env.ExecuteActivity(appendTaskLogProbeActivity,
		"test suite exited after 5s: go test ./...", "out line")
	if err != nil {
		t.Fatalf("ExecuteActivity: %v", err)
	}
	var path string
	if err := v.Get(&path); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if path == "" {
		t.Fatal(`appendTaskLog returned "" inside an activity context, want the written log's path`)
	}
	if filepath.Dir(path) != dir {
		t.Errorf("path %q is outside the configured dir %q", path, dir)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the returned path %q does not name a readable log: %v", path, err)
	}
	if got := string(data); !strings.Contains(got, "test suite exited after 5s") || !strings.Contains(got, "out line\n") {
		t.Errorf("the task log at the returned path lacks the record: %q", got)
	}
}

// TestAppendTaskLogFailedWriteReturnsEmpty pins the other half of the
// path-return contract: a failed write returns "" — the returned path must
// never name a file that lacks the record.
func TestAppendTaskLogFailedWriteReturnsEmpty(t *testing.T) {
	reset := TaskLogDir
	t.Cleanup(func() { TaskLogDir = reset })
	TaskLogDir = filepath.Join(t.TempDir(), "does-not-exist")

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(appendTaskLogProbeActivity)

	v, err := env.ExecuteActivity(appendTaskLogProbeActivity, "test suite exited after 5s: go test ./...", "out line")
	if err != nil {
		t.Fatalf("ExecuteActivity: %v (a task-log write failure is best-effort, never an activity error)", err)
	}
	var path string
	if err := v.Get(&path); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if path != "" {
		t.Errorf("appendTaskLog on a failed write = %q, want \"\"", path)
	}
}
