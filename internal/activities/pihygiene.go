package activities

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// pi round discipline for self-hosted models. pi rounds served by small
// local models (qwen3:8b via ollama/litellm over pi's openai-completions
// bridge) degenerate in a pattern flagship CLIs do not: the model retries a
// failing exact-match `edit` with byte-identical arguments without ever
// re-reading the file, then abandons the tool channel and prints the
// tool-call JSON as fenced assistant text — which pi records as plain
// prose, so the degenerate pattern sits in the resumed transcript as an
// example the model imitates (wa-termo-bootstrap 01a0ecd2, 2026-09-29).
// Two daedalus-side counters, both best-effort and both gated to pi: a
// resumed session whose transcript already embeds tool-call JSON is not
// resumed at all (the transcript is the infection vector), and a resumed
// session whose tail is a failed-edit loop gets the next round's prompt
// prefixed with steering. Every read/parse failure returns a clean bill —
// a lost inspection costs the old behavior, never a failed round.

// piEditLoopThreshold is the number of consecutive failed `edit` calls with
// identical arguments that marks a loop: the observed spiral retried the
// same call 26 times in one round, so steering at the second attempt
// precedes the compounding.
const piEditLoopThreshold = 2

// piEditSteer is prepended to the next round's prompt when the resumed
// session's transcript ends in a failed-edit loop. Resuming with --session
// and a new prompt appends this as a user message — the closest pi headless
// gets to harness auto-steering.
const piEditSteer = "EDIT LOOP: your recent `edit` calls failed because `oldText` was not found — the file does not contain that exact text. Call `read` on the target file now, then retry the `edit` with oldText copied exactly from the read output. Never retry an edit with the same oldText that already failed."

// piEditGuardrail is appended to every pi round's prompt (implementing
// rounds only — reviewers do not edit). Cheap insurance for small models;
// delivered on every round so it is in-context when an edit is attempted.
const piEditGuardrail = "\n\nEDIT DISCIPLINE: before any `edit` tool call, `read` the target file in this round first. If an edit fails, `read` the file again before retrying — never retry an edit with the same `oldText` that already failed."

// piSessionHealth is what a session-file scan found. embeddedToolJSON marks
// the self-reinforcing degenerate pattern (assistant text embedding
// tool-call JSON) — poison worth dropping the whole session over. editLoop
// marks a trailing run of failed `edit` calls with identical arguments —
// worth steering, not dropping.
type piSessionHealth struct {
	embeddedToolJSON bool
	editLoop         bool
}

// piSessionFile locates a session's transcript file in the worktree's pi
// session dir (files are <timestamp>_<session-id>.jsonl), reporting ok=false
// when there is none — a session pi never wrote, or an unreadable dir.
func piSessionFile(worktree, id string) (string, bool) {
	dir, err := piSessionsDir(worktree)
	if err != nil {
		return "", false
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", false
	}
	for _, e := range entries {
		if pid, ok := piSessionID(e.Name()); ok && pid == id {
			return filepath.Join(dir, e.Name()), true
		}
	}
	return "", false
}

// piSessionEntry is one line of pi's session JSONL (pi.dev/docs/latest/
// session-format, version 3): every entry carries a type, and message
// entries carry the wire message — user/assistant messages with a content
// array of typed blocks, and toolResult messages naming the tool, its call
// id, and isError.
type piSessionEntry struct {
	Message struct {
		Role    string `json:"role"`
		Content piBlocks `json:"content"`
		// ToolCallID and ToolName ride toolResult messages; IsError marks
		// the tool's failure.
		ToolCallID string `json:"toolCallId"`
		ToolName   string `json:"toolName"`
		IsError    bool   `json:"isError"`
	} `json:"message"`
}

// piBlocks tolerates both content shapes: assistant/toolResult messages
// carry a block array, user messages a plain string — the wrong shape just
// fails to fill the array.
type piBlocks []piBlock

func (b *piBlocks) UnmarshalJSON(data []byte) error {
	var arr []piBlock
	if json.Unmarshal(data, &arr) == nil {
		*b = arr
	}
	return nil
}

// piBlock is one assistant content block (pi.dev/docs/latest/
// message-types): text, thinking, or a toolCall whose arguments are a
// parsed object.
type piBlock struct {
	Type      string     `json:"type"`
	Text      string     `json:"text"`
	ID        string     `json:"id"`
	Arguments piEditArgs `json:"arguments"`
}

// piEditArgs is an `edit` call's arguments as pi's session records them —
// the assistant's raw wire arguments, in whichever schema the model sent:
// the current edit tool (packages/coding-agent/src/core/tools/edit.ts)
// carries the exact-match texts in edits[], while the legacy flat shape
// carries a top-level oldText that pi normalizes on execution. The
// transcript holds the raw form, so the loop identity (editIdentity) must
// accept both.
type piEditArgs struct {
	Path    string `json:"path"`
	OldText string `json:"oldText"`
	Edits   []struct {
		OldText string `json:"oldText"`
	} `json:"edits"`
}

// editIdentity reduces an edit call's arguments to the comparable identity
// of a failing retry: the target path plus the exact-match text(s) — the
// top-level oldText when the legacy flat shape carries one, the edits[]
// oldTexts otherwise. Everything else (a new text, whitespace in either)
// differing does not make a retry progress — the failure is oldText not
// matching the file — while genuinely different edits content must not
// compare equal, or a model that re-read and deliberately changed its edit
// between failures would be steered for nothing.
func editIdentity(args piEditArgs) string {
	if args.OldText != "" {
		return args.Path + "\x00" + args.OldText
	}
	olds := make([]string, 0, len(args.Edits))
	for _, e := range args.Edits {
		olds = append(olds, e.OldText)
	}
	return args.Path + "\x00" + strings.Join(olds, "\x00")
}

// scanPiSession reads a pi session's transcript and reports its health.
// embeddedToolJSON fires on any assistant text block embedding tool-call
// JSON — the `"oldText":` key signature. A serialized edit call (either
// schema) always carries it, and prose discussing an edit does not, but
// source code quoting an oldText key does — a pi round editing such a file
// can get a healthy session dropped on the next resume, at the documented
// best-effort cost of lost conversation context, never a failed round.
// editLoop reports whether the transcript's trailing run of failed `edit`
// calls holds piEditLoopThreshold or more with identical editIdentity
// (path + exact-match text, either schema); a successful edit or a
// differently-argued failure ends the run — a loop the model already
// recovered from steers no one — while intervening tool calls (the reads
// steering asks for) do not: a loop that continues through reads is still
// a loop.
func scanPiSession(worktree, id string) (h piSessionHealth) {
	path, ok := piSessionFile(worktree, id)
	if !ok {
		return h
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return h
	}
	// Edit-call identity per tool-call id, so a failing toolResult can be
	// compared with the failing one before it.
	identities := map[string]string{}
	streak := ""
	count := 0
	for line := range strings.SplitSeq(string(data), "\n") {
		var e piSessionEntry
		if json.Unmarshal([]byte(line), &e) != nil {
			continue
		}
		switch e.Message.Role {
		case "assistant":
			for _, b := range e.Message.Content {
				switch b.Type {
				case "text":
					if strings.Contains(b.Text, `"oldText":`) {
						h.embeddedToolJSON = true
					}
				case "toolCall":
					identities[b.ID] = editIdentity(b.Arguments)
				}
			}
		case "toolResult":
			if e.Message.ToolName != "edit" {
				continue
			}
			if !e.Message.IsError {
				// A successful edit ends any open run — the trailing state
				// is clean, so a loop the model recovered from steers no one.
				streak, count = "", 0
				h.editLoop = false
				continue
			}
			id, known := identities[e.Message.ToolCallID]
			switch {
			case !known:
				// The call's arguments were never seen (its assistant turn
				// is not on file) — identity is unknowable, run broken.
				streak, count = "", 0
			case id == streak:
				count++
			default:
				streak, count = id, 1
			}
			h.editLoop = count >= piEditLoopThreshold
		}
	}
	return h
}
