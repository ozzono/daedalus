package activities

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writePiTranscript fakes pi's own session transcript for a session id: the
// given JSONL lines under the worktree's pi session dir, in pi's
// <timestamp>_<session-id>.jsonl layout.
func writePiTranscript(t *testing.T, worktree, id string, lines ...string) {
	t.Helper()
	dir, err := piSessionsDir(worktree)
	if err != nil {
		t.Fatalf("piSessionsDir: %v", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir sessions dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "20260929_120000_"+id+".jsonl"),
		[]byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write session file: %v", err)
	}
}

// piAssistantToolCall is one assistant turn holding a single edit toolCall.
func piAssistantToolCall(id, path, oldText string) string {
	return `{"type":"message","message":{"role":"assistant","content":[{"type":"toolCall","id":"` + id + `","arguments":{"path":"` + path + `","edits":[{"oldText":"` + oldText + `"}]}}]}}`
}

// piAssistantFlatToolCall is the legacy flat edit shape (top-level oldText).
func piAssistantFlatToolCall(id, path, oldText string) string {
	return `{"type":"message","message":{"role":"assistant","content":[{"type":"toolCall","id":"` + id + `","arguments":{"path":"` + path + `","oldText":"` + oldText + `"}}]}}`
}

// piToolResult is one toolResult turn.
func piToolResult(id, tool string, isError bool) string {
	return `{"type":"message","message":{"role":"toolResult","toolCallId":"` + id + `","toolName":"` + tool + `","isError":` + boolStr(isError) + `}}`
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// TestEditIdentity pins the loop identity: the target path plus the
// exact-match text(s), in either wire schema — so a legacy flat retry and a
// modern edits[] retry of the same edit compare equal, while a different
// oldText or a different target file does not.
func TestEditIdentity(t *testing.T) {
	flat := func(path, old string) piEditArgs {
		return piEditArgs{Path: path, OldText: old}
	}
	edits := func(path string, olds ...string) piEditArgs {
		a := piEditArgs{Path: path}
		for _, o := range olds {
			a.Edits = append(a.Edits, struct {
				OldText string `json:"oldText"`
			}{o})
		}
		return a
	}
	for _, c := range []struct {
		name      string
		a, b      piEditArgs
		wantEqual bool
	}{
		{"flat same", flat("a.go", "X"), flat("a.go", "X"), true},
		{"edits[] same", edits("a.go", "X", "Y"), edits("a.go", "X", "Y"), true},
		{"cross-shape same edit", flat("a.go", "X"), edits("a.go", "X"), true},
		{"different oldText", flat("a.go", "X"), flat("a.go", "Y"), false},
		{"different edits content", edits("a.go", "X"), edits("a.go", "X", "Y"), false},
		{"different path", flat("a.go", "X"), flat("b.go", "X"), false},
	} {
		if got := editIdentity(c.a) == editIdentity(c.b); got != c.wantEqual {
			t.Errorf("%s: editIdentity equal = %v, want %v", c.name, got, c.wantEqual)
		}
	}
}

// TestScanPiSession pins the transcript scan's health verdicts: the poison
// signature (assistant text embedding tool-call JSON), the failed-edit loop
// (a trailing run of identical failed `edit` results, in either schema),
// and everything that must read as a clean bill — missing or malformed
// files, prose, single failures, recovered loops, differently-argued
// retries, orphaned tool results, and non-edit tool failures.
func TestScanPiSession(t *testing.T) {
	fakeHome(t)
	wt := t.TempDir()

	editLoop := []string{
		`{"type":"message","message":{"role":"user","content":"fix it"}}`,
		piAssistantToolCall("t1", "a.go", "X"),
		piToolResult("t1", "edit", true),
		piAssistantToolCall("t2", "a.go", "X"),
		piToolResult("t2", "edit", true),
	}
	for _, c := range []struct {
		name         string
		plantID      string // "" plants nothing: no session file
		lines        []string
		wantPoison   bool
		wantEditLoop bool
	}{
		{"no session file", "absent", nil, false, false},
		{
			"plain prose",
			"prose",
			[]string{
				`{"type":"message","message":{"role":"user","content":"why did the edit fail"}}`,
				`{"type":"message","message":{"role":"assistant","content":[{"type":"text","text":"the oldText argument was not found in the file"}]}}`,
			},
			false, false,
		},
		{
			"assistant text embeds tool-call JSON",
			"poison",
			[]string{
				piAssistantToolCall("t1", "a.go", "X"),
				piToolResult("t1", "edit", true),
				`{"type":"message","message":{"role":"assistant","content":[{"type":"text","text":"retrying {\"path\":\"a.go\",\"edits\":[{\"oldText\":\"X\"}]} again"}]}}`,
			},
			true, false,
		},
		{"modern edits[] loop", "loop", editLoop, false, true},
		{
			"legacy flat loop",
			"flatloop",
			[]string{
				piAssistantFlatToolCall("t1", "a.go", "X"),
				piToolResult("t1", "edit", true),
				piAssistantFlatToolCall("t2", "a.go", "X"),
				piToolResult("t2", "edit", true),
			},
			false, true,
		},
		{
			"cross-shape loop",
			"crossloop",
			[]string{
				piAssistantFlatToolCall("t1", "a.go", "X"),
				piToolResult("t1", "edit", true),
				piAssistantToolCall("t2", "a.go", "X"),
				piToolResult("t2", "edit", true),
			},
			false, true,
		},
		{"single failed edit", "single", editLoop[:4], false, false},
		{
			"recovered loop",
			"recovered",
			append(append([]string{}, editLoop...),
				piAssistantToolCall("t3", "a.go", "X"),
				piToolResult("t3", "edit", false),
			),
			false, false,
		},
		{
			"differently-argued failure breaks the run",
			"varied",
			[]string{
				piAssistantToolCall("t1", "a.go", "X"),
				piToolResult("t1", "edit", true),
				piAssistantToolCall("t2", "a.go", "Y"),
				piToolResult("t2", "edit", true),
			},
			false, false,
		},
		{
			"loop through reads is still a loop",
			"throughreads",
			[]string{
				piAssistantToolCall("t1", "a.go", "X"),
				piToolResult("t1", "edit", true),
				piAssistantToolCall("t3", "a.go", ""),
				piToolResult("t3", "read", false),
				piAssistantToolCall("t2", "a.go", "X"),
				piToolResult("t2", "edit", true),
			},
			false, true,
		},
		{
			"orphaned failed edit breaks the run",
			"orphan",
			[]string{
				piToolResult("t1", "edit", true),
				piToolResult("t2", "edit", true),
			},
			false, false,
		},
		{
			"non-edit failures ignored",
			"reads",
			[]string{
				piAssistantToolCall("t1", "a.go", ""),
				piToolResult("t1", "read", true),
				piAssistantToolCall("t2", "a.go", ""),
				piToolResult("t2", "read", true),
			},
			false, false,
		},
		{
			"malformed lines skipped",
			"malformed",
			[]string{
				"not json at all",
				`{"type":"message","message":{"role":"assistant","content":[{"type":"text","text":"{\"oldText\":1}"}]}}`,
			},
			true, false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.plantID != "absent" {
				writePiTranscript(t, wt, c.plantID, c.lines...)
			}
			h := scanPiSession(wt, c.plantID)
			if h.embeddedToolJSON != c.wantPoison || h.editLoop != c.wantEditLoop {
				t.Errorf("scanPiSession = %+v, want poison=%v loop=%v", h, c.wantPoison, c.wantEditLoop)
			}
		})
	}
}

// TestRunJailedClaudeActivityPiDropsPoisonedResume pins the resume gate: a
// pi session whose transcript embeds tool-call JSON as assistant text is
// not resumed — the round starts fresh (no --session flag), carrying the
// prompt and the edit guardrail but no steering.
func TestRunJailedClaudeActivityPiDropsPoisonedResume(t *testing.T) {
	scrubBugFilingEnv(t)
	fakeHome(t)
	wt := t.TempDir()
	log := newStubLog(t)
	stubBin(t, "ai-jail", `printf '%s\n' '{"type":"session","id":"sess-fresh"}' '{"type":"message_end","message":{"content":[{"type":"text","text":"started over"}]}}'; exit 0`)
	writePiTranscript(t, wt, "sess-poison",
		`{"type":"message","message":{"role":"assistant","content":[{"type":"text","text":"{\"oldText\":\"X\"}"}]}}`)

	res, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "fix the bug",
		Agent:        "pi",
		SessionID:    "sess-poison",
		Role:         RoleDev,
	})
	if err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}
	if res.SessionID != "sess-fresh" {
		t.Errorf("res.SessionID = %q, want the fresh session", res.SessionID)
	}

	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
	}
	if contains(calls[0].Args, "--session") || contains(calls[0].Args, "sess-poison") {
		t.Errorf("poisoned session args %v must not be resumed", calls[0].Args)
	}
	if calls[0].Stdin != "fix the bug"+piEditGuardrail {
		t.Errorf("ai-jail stdin = %q, want the prompt with the guardrail and no steering", calls[0].Stdin)
	}
}

// TestRunJailedClaudeActivityPiSteersEditLoopResume pins the steering: a
// pi session ending in a failed-edit loop keeps its session (still resumed
// via --session) and gets piEditSteer prepended to the round's prompt.
func TestRunJailedClaudeActivityPiSteersEditLoopResume(t *testing.T) {
	scrubBugFilingEnv(t)
	fakeHome(t)
	wt := t.TempDir()
	log := newStubLog(t)
	stubBin(t, "ai-jail", `printf '%s\n' '{"type":"session","id":"sess-loop"}' '{"type":"message_end","message":{"content":[{"type":"text","text":"steered"}]}}'; exit 0`)
	writePiTranscript(t, wt, "sess-loop",
		piAssistantToolCall("t1", "a.go", "X"),
		piToolResult("t1", "edit", true),
		piAssistantToolCall("t2", "a.go", "X"),
		piToolResult("t2", "edit", true),
	)

	res, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "fix the bug",
		Agent:        "pi",
		SessionID:    "sess-loop",
		Role:         RoleDev,
	})
	if err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}
	if res.Text != "steered" {
		t.Errorf("res.Text = %q, want the round's output", res.Text)
	}

	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
	}
	if !contains(calls[0].Args, "--session") || !contains(calls[0].Args, "sess-loop") {
		t.Errorf("looping session args %v must still be resumed", calls[0].Args)
	}
	if calls[0].Stdin != piEditSteer+"\n\nfix the bug"+piEditGuardrail {
		t.Errorf("ai-jail stdin = %q, want steering + prompt + guardrail", calls[0].Stdin)
	}
}

// TestRunJailedReviewerActivityPiHygiene pins the reviewer's copy of the
// hygiene gate: a poisoned transcript is not resumed, while a failed-edit
// loop neither drops the session nor steers — reviewers do not edit.
func TestRunJailedReviewerActivityPiHygiene(t *testing.T) {
	stubBin(t, "git", `if [ "$3" = "diff" ]; then printf 'M foo.go\n'; fi
exit 0`)
	stubBin(t, "ai-jail", `printf '%s\n' '{"type":"session","id":"rev-out"}' '{"type":"message_end","message":{"content":[{"type":"text","text":"Fine.\nAPPROVED"}]}}'; exit 0`)

	t.Run("poisoned transcript drops the resume", func(t *testing.T) {
		scrubBugFilingEnv(t)
		fakeHome(t)
		wt := t.TempDir()
		log := newStubLog(t)
		writePiTranscript(t, wt, "rev-poison",
			`{"type":"message","message":{"role":"assistant","content":[{"type":"text","text":"{\"oldText\":\"X\"}"}]}}`)

		if _, err := RunJailedReviewerActivity(context.Background(), ReviewInput{
			WorktreePath: wt,
			Focus:        "the implementation",
			Agent:        "pi",
			SessionID:    "rev-poison",
			Role:         RoleDevReview,
		}); err != nil {
			t.Fatalf("RunJailedReviewerActivity: %v", err)
		}

		var jail stubCall
		for _, c := range readCalls(t, log) {
			if len(c.Args) > 0 && c.Args[0] == "--worktree" {
				jail = c
			}
		}
		if jail.Args == nil {
			t.Fatal("ai-jail was never called")
		}
		if contains(jail.Args, "--session") || contains(jail.Args, "rev-poison") {
			t.Errorf("poisoned reviewer session args %v must not be resumed", jail.Args)
		}
		if strings.Contains(jail.Stdin, "EDIT DISCIPLINE") || strings.Contains(jail.Stdin, "EDIT LOOP") {
			t.Errorf("reviewer prompt %q must carry neither guardrail nor steering", jail.Stdin)
		}
	})

	t.Run("edit loop neither drops nor steers", func(t *testing.T) {
		scrubBugFilingEnv(t)
		fakeHome(t)
		wt := t.TempDir()
		log := newStubLog(t)
		writePiTranscript(t, wt, "rev-loop",
			piAssistantToolCall("t1", "a.go", "X"),
			piToolResult("t1", "edit", true),
			piAssistantToolCall("t2", "a.go", "X"),
			piToolResult("t2", "edit", true),
		)

		if _, err := RunJailedReviewerActivity(context.Background(), ReviewInput{
			WorktreePath: wt,
			Focus:        "the implementation",
			Agent:        "pi",
			SessionID:    "rev-loop",
			Role:         RoleDevReview,
		}); err != nil {
			t.Fatalf("RunJailedReviewerActivity: %v", err)
		}

		var jail stubCall
		for _, c := range readCalls(t, log) {
			if len(c.Args) > 0 && c.Args[0] == "--worktree" {
				jail = c
			}
		}
		if jail.Args == nil {
			t.Fatal("ai-jail was never called")
		}
		if !contains(jail.Args, "--session") || !contains(jail.Args, "rev-loop") {
			t.Errorf("looping reviewer session args %v must still be resumed", jail.Args)
		}
		if strings.Contains(jail.Stdin, "EDIT DISCIPLINE") || strings.Contains(jail.Stdin, "EDIT LOOP") {
			t.Errorf("reviewer prompt %q must carry neither guardrail nor steering", jail.Stdin)
		}
	})
}
