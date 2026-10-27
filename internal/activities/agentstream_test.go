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
