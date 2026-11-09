package activities

import (
	"encoding/json"
	"fmt"
	"testing"
)

// piToolExecLine renders one pi tool_execution_start stream line: the
// generated call id, the tool name, and the raw args object — the shape
// piToolExecEvent parses (pi.dev/docs/latest/json).
func piToolExecLine(id, tool, args string) string {
	return fmt.Sprintf(`{"type":"tool_execution_start","id":%q,"toolName":%q,"args":%s}`, id, tool, args)
}

// TestCanonicalJSONIdentity pins the loop identity's normalization: two
// emissions of the same call encode equal canonical strings regardless of
// key order or spacing, any value or type difference stays distinct —
// number literals verbatim, so adjacent integers above float64's precision
// never alias onto one identity — and bytes that do not re-encode fall
// through raw so byte-identical repeats still compare equal.
func TestCanonicalJSONIdentity(t *testing.T) {
	cases := []struct {
		name      string
		a, b      string
		wantEqual bool
	}{
		{"key order does not matter", `{"command":"ls","path":"."}`, `{"path":".","command":"ls"}`, true},
		{"spacing does not matter", `{ "command" : "ls" }`, `{"command":"ls"}`, true},
		{"nested keys sort too", `{"o":{"x":1,"y":2}}`, `{"o":{"y":2,"x":1}}`, true},
		{"a value difference stays distinct", `{"command":"ls"}`, `{"command":"ls -la"}`, false},
		{"a type difference stays distinct", `{"n":"1"}`, `{"n":1}`, false},
		{"adjacent big integers stay distinct", `{"n":1234567890123456789}`, `{"n":1234567890123456788}`, false},
		{"trailing data falls through raw", `{"a":1} junk`, `{"a":1} junk`, true},
		{"non-json bytes fall through raw", `not json`, `not json`, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotEqual := canonicalJSON(json.RawMessage(c.a)) == canonicalJSON(json.RawMessage(c.b))
			if gotEqual != c.wantEqual {
				t.Errorf("canonicalJSON(%s) == canonicalJSON(%s) = %v, want %v", c.a, c.b, gotEqual, c.wantEqual)
			}
		})
	}
}

// writeStream feeds one completed line per write, asserting the writer
// contract each time: the tee is transparent — full length accepted, no
// error — whatever the bytes carry.
func writeStream(t *testing.T, w *piToolLoopWatcher, lines ...string) {
	t.Helper()
	for _, line := range lines {
		p := []byte(line + "\n")
		n, err := w.Write(p)
		if err != nil || n != len(p) {
			t.Fatalf("Write(%q) = (%d, %v), want (%d, nil)", line, n, err, len(p))
		}
	}
}

// TestPiToolLoopWatcher pins the kill decision on the parsed stream:
// toolLoopThreshold consecutive identical tool calls — identity = tool
// name plus canonicalized arguments, the generated call id excluded (every
// line below carries a fresh id) — fire the kill exactly once, while two
// identical calls, a distinct call between repeats, a differing argument,
// and interleaved non-tool or malformed lines never do. The interleaved
// case is the real-stream shape: pi folds message events between tool
// calls, so a streak broken by them would never fire at all.
func TestPiToolLoopWatcher(t *testing.T) {
	identical := func(n int) []string {
		lines := make([]string, n)
		for i := range lines {
			lines[i] = piToolExecLine(fmt.Sprintf("call-%d", i), "bash", `{"command":"ls"}`)
		}
		return lines
	}
	noise := []string{
		`{"type":"message_update","message":{"role":"assistant"}}`,
		`this line is not json at all`,
	}
	cases := []struct {
		name      string
		lines     []string
		wantFired bool
	}{
		{"third identical call fires", identical(toolLoopThreshold), true},
		{"two identical calls stay under the threshold", identical(toolLoopThreshold - 1), false},
		{"five identical calls still kill exactly once", identical(5), true},
		{"a distinct tool resets the streak", append(append(identical(2),
			piToolExecLine("call-x", "edit", `{"path":"a.go"}`)), identical(2)...), false},
		{"a differing argument stays distinct", []string{
			piToolExecLine("c1", "bash", `{"command":"ls"}`),
			piToolExecLine("c2", "bash", `{"command":"ls"}`),
			piToolExecLine("c3", "bash", `{"command":"ls -la"}`),
		}, false},
		{"key order and spacing do not change the identity", []string{
			piToolExecLine("c1", "bash", `{"command":"ls","path":"."}`),
			piToolExecLine("c2", "bash", `{ "path": ".", "command": "ls" }`),
			piToolExecLine("c3", "bash", `{"path":".","command":"ls"}`),
		}, true},
		{"non-tool and malformed lines never reset the streak",
			append(append(append(append(append(append([]string{},
				noise...), identical(1)...), noise...), identical(1)...), noise...), identical(1)...), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			kills := 0
			w := &piToolLoopWatcher{kill: func() { kills++ }}
			writeStream(t, w, c.lines...)
			if w.fired != c.wantFired {
				t.Errorf("fired = %v, want %v", w.fired, c.wantFired)
			}
			wantKills := 0
			if c.wantFired {
				wantKills = 1
			}
			if kills != wantKills {
				t.Errorf("kill called %d times, want %d", kills, wantKills)
			}
		})
	}
}

// TestPiToolLoopWatcherPartialLines pins the carry between writes: an
// event split across writes counts once, when its line completes — a
// partial write is never a call, and a dropped remainder never loses one.
func TestPiToolLoopWatcherPartialLines(t *testing.T) {
	kills := 0
	w := &piToolLoopWatcher{kill: func() { kills++ }}
	// The first call arrives split across two writes; the second rides the
	// write that completes the first. A phantom count from the partial
	// would reach the threshold on the second real call.
	parts := []string{
		`{"type":"tool_exec`,
		`ution_start","id":"c1","toolName":"bash","args":{"command":"ls"}}` + "\n" +
			piToolExecLine("c2", "bash", `{"command":"ls"}`) + "\n",
	}
	for _, p := range parts {
		if n, err := w.Write([]byte(p)); err != nil || n != len(p) {
			t.Fatalf("Write(%q) = (%d, %v), want (%d, nil)", p, n, err, len(p))
		}
	}
	if w.fired {
		t.Fatal("two real identical calls fired the kill: a partial write was counted as a call")
	}
	writeStream(t, w, piToolExecLine("c3", "bash", `{"command":"ls"}`))
	if !w.fired || kills != 1 {
		t.Errorf("after the third identical call: fired = %v, kills = %d, want the kill fired once", w.fired, kills)
	}
}

// TestHandsStuckClassify pins handsStuck's classification table: only the
// failed event's class field decides — "stuck" lands with its reason in the
// composite, which is never empty on a match (a missing reason must not
// launder the class back into the generic failure path), every other class
// stays untracked (retriable belongs to agentAPIError, the rest to the
// generic path), and last-wins is total past any later event, mirroring
// agentAPIError's stream-terminal semantics.
func TestHandsStuckClassify(t *testing.T) {
	cases := []struct {
		name   string
		stdout string
		want   string
	}{
		{"stuck kill with reason", `{"kind":"failed","payload":{"class":"stuck","reason":"no wire events for 14m0s"}}`,
			"class stuck: no wire events for 14m0s"},
		{"stuck without a reason is never empty", `{"kind":"failed","payload":{"class":"stuck"}}`,
			"class stuck: "},
		{"last stuck wins", `{"kind":"failed","payload":{"class":"stuck","reason":"first"}}` + "\n" +
			`{"kind":"failed","payload":{"class":"stuck","reason":"second"}}`,
			"class stuck: second"},
		{"last-wins is total past any later event", `{"kind":"failed","payload":{"class":"stuck","reason":"first"}}` + "\n" +
			`{"kind":"started","payload":{"session":"s-9"}}` + "\n" +
			`{"kind":"delivered","payload":{"payload":{"summary":"done"}}}`,
			"class stuck: first"},
		{"retriable belongs to the api-error face", `{"kind":"failed","payload":{"class":"retriable","reason":"connection refused"}}`, ""},
		{"other classes stay generic", `{"kind":"failed","payload":{"class":"no-delivery","reason":"loop guard"}}` + "\n" +
			`{"kind":"failed","payload":{"class":"terminal","reason":"unusable response"}}`, ""},
		{"clean stream", `{"kind":"started","payload":{"session":"s"}}` + "\n" +
			`{"kind":"delivered","payload":{"payload":{"summary":"done"}}}`, ""},
		{"non-json lines ignored", "not json", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := handsStuck(c.stdout); got != c.want {
				t.Errorf("handsStuck(%q) = %q, want %q", c.stdout, got, c.want)
			}
		})
	}
}

// TestHandsNoDeliveryClassify pins handsNoDelivery's classification table:
// only the failed event's class field decides — "no-delivery" (the deliver
// contract's watchdog kill) lands with its reason in the composite, which is
// never empty on a match, every other class stays untracked (stuck belongs
// to handsStuck, retriable to agentAPIError, the rest to the generic path),
// and last-wins is total past any later event. The faces are mutually
// exclusive in a real stream (one terminal failed event, one class per
// event), so the sibling parsers must stay deaf to each other's class —
// pinned from both directions on the same streams.
func TestHandsNoDeliveryClassify(t *testing.T) {
	stuckStream := `{"kind":"failed","payload":{"class":"stuck","reason":"no wire events for 14m0s"}}`
	noDeliveryStream := `{"kind":"failed","payload":{"class":"no-delivery","reason":"2 strikes with no delivery tool call"}}`
	cases := []struct {
		name   string
		stdout string
		want   string
	}{
		{"no-delivery kill with reason", noDeliveryStream,
			"class no-delivery: 2 strikes with no delivery tool call"},
		{"no-delivery without a reason is never empty", `{"kind":"failed","payload":{"class":"no-delivery"}}`,
			"class no-delivery: "},
		{"last no-delivery wins", `{"kind":"failed","payload":{"class":"no-delivery","reason":"first"}}` + "\n" +
			`{"kind":"failed","payload":{"class":"no-delivery","reason":"second"}}`,
			"class no-delivery: second"},
		{"last-wins is total past any later event", `{"kind":"failed","payload":{"class":"no-delivery","reason":"first"}}` + "\n" +
			`{"kind":"started","payload":{"session":"s-9"}}` + "\n" +
			`{"kind":"delivered","payload":{"payload":{"summary":"done"}}}`,
			"class no-delivery: first"},
		{"stuck belongs to the stuck face", stuckStream, ""},
		{"retriable belongs to the api-error face", `{"kind":"failed","payload":{"class":"retriable","reason":"connection refused"}}`, ""},
		{"other classes stay generic", `{"kind":"failed","payload":{"class":"terminal","reason":"unusable response"}}`, ""},
		{"clean stream", `{"kind":"started","payload":{"session":"s"}}` + "\n" +
			`{"kind":"delivered","payload":{"payload":{"summary":"done"}}}`, ""},
		{"non-json lines ignored", "not json", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := handsNoDelivery(c.stdout); got != c.want {
				t.Errorf("handsNoDelivery(%q) = %q, want %q", c.stdout, got, c.want)
			}
		})
	}
	// Mutual exclusivity, both directions: a stuck class never mints the
	// no-delivery face and a no-delivery class never mints the stuck face.
	if got := handsStuck(noDeliveryStream); got != "" {
		t.Errorf("handsStuck(no-delivery stream) = %q, want empty", got)
	}
	if got := handsNoDelivery(stuckStream); got != "" {
		t.Errorf("handsNoDelivery(stuck stream) = %q, want empty", got)
	}
}

// TestHandsTextEvents pins the salvage capture: the text of a hands round's
// last non-empty text event — the prose a deliver-contract kill strands in
// the stream. The payload decodes through both observed shapes (the
// {"text": ...} object, then a bare JSON string), a shape drift degrades to
// "" rather than a wrong plan, non-text events and malformed lines are
// ignored, and last-non-empty-wins is never a concatenation — the watchdog
// strikes regenerate byte-identical prose, so joining them would hand the
// consumer a duplicated plan.
func TestHandsTextEvents(t *testing.T) {
	cases := []struct {
		name   string
		stdout string
		want   string
	}{
		{"object payload shape", `{"kind":"text","payload":{"payload":{"text":"the plan"}}}`, "the plan"},
		{"bare string payload shape", `{"kind":"text","payload":{"payload":"the plan"}}`, "the plan"},
		{"last text wins, never concatenated", `{"kind":"text","payload":{"payload":{"text":"first strike"}}}` + "\n" +
			`{"kind":"text","payload":{"payload":{"text":"first strike"}}}`,
			"first strike"},
		{"empty text events skipped, earlier non-empty wins", `{"kind":"text","payload":{"payload":{"text":"the plan"}}}` + "\n" +
			`{"kind":"text","payload":{"payload":{"text":""}}}`,
			"the plan"},
		{"last non-empty wins over an earlier one", `{"kind":"text","payload":{"payload":{"text":"old"}}}` + "\n" +
			`{"kind":"text","payload":{"payload":{"text":"new"}}}`,
			"new"},
		{"non-text kinds ignored", `{"kind":"started","payload":{"session":"s"}}` + "\n" +
			`{"kind":"delivered","payload":{"payload":{"summary":"done"}}}` + "\n" +
			`{"kind":"text","payload":{"payload":{"text":"the plan"}}}`,
			"the plan"},
		{"failed events ignored", `{"kind":"failed","payload":{"class":"no-delivery","reason":"killed"}}` + "\n" +
			`{"kind":"text","payload":{"payload":{"text":"the plan"}}}`,
			"the plan"},
		{"malformed lines ignored", "not json" + "\n" +
			`{"kind":"text","payload":{"payload":{"text":"the plan"}}}`,
			"the plan"},
		{"payload shape drift degrades to empty", `{"kind":"text","payload":{"payload":{"nope":1}}}`, ""},
		{"no text events at all", `{"kind":"started","payload":{"session":"s"}}`, ""},
		{"plain text lines ignored", "hands said something", ""},
		{"empty stream", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := handsTextEvents(c.stdout); got != c.want {
				t.Errorf("handsTextEvents(%q) = %q, want %q", c.stdout, got, c.want)
			}
		})
	}
}
