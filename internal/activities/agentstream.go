package activities

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"strings"
	"time"
)

// Usage carries the raw per-invocation provider metrics of one jailed
// round, exactly as the agent CLI reported them (claude's result event:
// total_cost_usd, usage token counts, duration_ms; pi's message_update
// usage objects). Raw figures only —
// deltas are computed at aggregation time — so per-round token attribution
// and tokens/sec stay lossless if provider semantics change. CLIs without
// a structured result (opencode's, codex's, and aider's plain text) contribute
// nothing.
type Usage struct {
	// Worker names the worker process that ran the round (config
	// worker_id, else its task queue, stamped from the worker's
	// DAEDALUS_WORKER_NAME), so history aggregates per worker. Empty in
	// rounds run by workers that predate the field.
	Worker string
	// CostUSD is the CLI-reported total cost of the invocation.
	CostUSD float64
	// InputTokens and OutputTokens are the invocation's prompt and
	// completion token counts.
	InputTokens  int64
	OutputTokens int64
	// CacheReadTokens and CacheWriteTokens are claude's
	// cache_read_input_tokens and cache_creation_input_tokens.
	CacheReadTokens  int64
	CacheWriteTokens int64
	// Duration is the CLI-reported duration_ms when it reported one, else
	// the worker's measured wall time for the round.
	Duration time.Duration
}

// streamMessage is one line of claude --output-format json/stream-json
// output: the json format emits a single result object (carrying the final
// text in Result); stream-json additionally emits assistant messages with
// content blocks (thinking, text). The result object also carries the raw
// provider metrics captured per round (Usage).
type streamMessage struct {
	Type   string `json:"type"`
	Result string `json:"result"`
	// TerminalReason rides the result event when the run ended on a
	// provider error: terminal_reason "api_error" marks a request the
	// API rejected outright (the img-upload incident — a healthy
	// 167-turn round ending in a single 400, exit 1), distinct from a
	// genuine task-level failure. Read only by agentAPIError.
	TerminalReason string `json:"terminal_reason"`
	// SessionID is the conversation id claude reports on every event of a
	// run (and the json format's result object carries); resuming a later
	// round with it continues the same conversation.
	SessionID string `json:"session_id"`
	// TotalCostUSD, DurationMS, and Usage ride the result event: the
	// round's cost, wall duration in ms, and token counts, captured raw
	// into Usage.
	TotalCostUSD float64 `json:"total_cost_usd"`
	DurationMS   int64   `json:"duration_ms"`
	Usage        struct {
		InputTokens              int64 `json:"input_tokens"`
		OutputTokens             int64 `json:"output_tokens"`
		CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
		CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
	} `json:"usage"`
	Message struct {
		Content []struct {
			Type     string `json:"type"`
			Thinking string `json:"thinking"`
			Text     string `json:"text"`
		} `json:"content"`
	} `json:"message"`
}

// parseAgentStream extracts the agent's chain of thought, visible text, and
// conversation session id from json or stream-json output. Non-JSON or
// uninteresting lines are skipped — the stream also carries system/init and
// delta events. The session id is whatever the last event carrying one
// reported (they all agree within a run; a resumed run keeps its id).
func parseAgentStream(stdout string) (thinking, text, session string) {
	for line := range strings.SplitSeq(stdout, "\n") {
		var m streamMessage
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		if m.SessionID != "" {
			session = m.SessionID
		}
		switch m.Type {
		case "assistant":
			for _, block := range m.Message.Content {
				switch block.Type {
				case "thinking":
					thinking += block.Thinking
				case "text":
					text += block.Text
				}
			}
		case "result":
			// The json format's single object; also the terminal event of
			// a stream-json run, where it restates the final assistant
			// text — overwrite rather than append to avoid duplication.
			if m.Result != "" {
				text = m.Result
			}
		}
	}
	return thinking, text, session
}

// parseAgentUsage extracts the raw provider metrics (Usage) from json or
// stream-json output — the terminal result event's cost, token counts, and
// duration_ms, captured raw per the maintainer decision so aggregation
// computes deltas. A stream without a result event (opencode's, codex's,
// and aider's plain text) reports a zero Usage.
func parseAgentUsage(stdout string) (usage Usage) {
	for line := range strings.SplitSeq(stdout, "\n") {
		var m streamMessage
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		if m.Type != "result" {
			continue
		}
		usage.CostUSD = m.TotalCostUSD
		usage.InputTokens = m.Usage.InputTokens
		usage.OutputTokens = m.Usage.OutputTokens
		usage.CacheReadTokens = m.Usage.CacheReadInputTokens
		usage.CacheWriteTokens = m.Usage.CacheCreationInputTokens
		if m.DurationMS > 0 {
			usage.Duration = time.Duration(m.DurationMS) * time.Millisecond
		}
	}
	return usage
}

// piUsage is pi's provider-reported usage object (pi.dev/docs/latest/json,
// schema from pi's packages/ai types): token counts per side plus cost
// totals.
type piUsage struct {
	Input      int64 `json:"input"`
	Output     int64 `json:"output"`
	CacheRead  int64 `json:"cacheRead"`
	CacheWrite int64 `json:"cacheWrite"`
	Cost       struct {
		Total float64 `json:"total"`
	} `json:"cost"`
}

// piStreamMessage is one line of pi --mode json output — pi's own schema,
// not claude's: a first-line session header carrying the session id,
// message_update delta events (whose top-level usage is the latest
// cumulative provider-reported usage), and message_end events carrying the
// final authoritative message.
type piStreamMessage struct {
	Type string `json:"type"`
	// ID is the session id on the "session" header line.
	ID string `json:"id"`
	// Usage rides message_update events; it may remain zero when a
	// provider only reports usage at completion.
	Usage   piUsage `json:"usage"`
	Message struct {
		Content []struct {
			Type     string `json:"type"`
			Text     string `json:"text"`
			Thinking string `json:"thinking"`
		} `json:"content"`
		// StopReason "error" + ErrorMessage mark a message pi folded on a
		// provider/API failure — pi still exits 0, so without reading this
		// the round would count as a success carrying an error-shaped
		// body. Read only by agentAPIError.
		StopReason   string `json:"stopReason"`
		ErrorMessage string `json:"errorMessage"`
	} `json:"message"`
}

// parsePiStream is parseAgentStream for pi's --mode json event stream: the
// session header line carries the conversation id, and the last
// message_end carrying content is the round's final authoritative message
// — intermediate narration and tool-call turns do not dilute it.
// Non-JSON lines are skipped like the claude parser's.
func parsePiStream(stdout string) (thinking, text, session string) {
	for line := range strings.SplitSeq(stdout, "\n") {
		var m piStreamMessage
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		switch m.Type {
		case "session":
			session = m.ID
		case "message_end":
			var th, tx strings.Builder
			for _, block := range m.Message.Content {
				switch block.Type {
				case "thinking":
					th.WriteString(block.Thinking)
				case "text":
					tx.WriteString(block.Text)
				}
			}
			if th.Len() > 0 {
				thinking = th.String()
			}
			if tx.Len() > 0 {
				text = tx.String()
			}
		}
	}
	return thinking, text, session
}

// parsePiUsage is parseAgentUsage for pi: usage rides message_update
// events cumulatively, so the last one seen is the round's final figure.
// A stream without message_update events (a provider reporting only at
// completion) reports a zero Usage — the worker's measured wall time still
// lands in Duration at the caller.
func parsePiUsage(stdout string) (usage Usage) {
	for line := range strings.SplitSeq(stdout, "\n") {
		var m piStreamMessage
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		if m.Type != "message_update" {
			continue
		}
		usage.CostUSD = m.Usage.Cost.Total
		usage.InputTokens = m.Usage.Input
		usage.OutputTokens = m.Usage.Output
		usage.CacheReadTokens = m.Usage.CacheRead
		usage.CacheWriteTokens = m.Usage.CacheWrite
	}
	return usage
}

// handsEvent is one line of `hands round --format json` output — hands's
// own protocol, the first agent schema in this package owned end-to-end by
// the agent itself (shape probe-pinned live 2026-10-25 against the
// installed binary; no dialect folklore): an envelope carrying the round
// id, a monotonic seq, a kind, and a kind-shaped payload. Observed kinds:
// started (payload.session — the conversation UUID hands's own recovery
// pin and daedalus's resume key, minted by hands before any model turn,
// so nothing the model says can forge it), usage (one per provider
// request), tool_call, tool_result, delivered / partial (payload.payload —
// the forced delivery tool's arguments), and failed (class retriable /
// no-delivery / terminal + reason). partial's exact payload shape is
// probe-owed (never observed live); it decodes through the same delivery
// struct, and a shape drift degrades to an empty summary — the caller's
// raw-stdout fallback — never a wrong one.
type handsEvent struct {
	RoundID string `json:"round_id"`
	Seq     int    `json:"seq"`
	Kind    string `json:"kind"`
	Payload struct {
		Session string `json:"session"`
		Class   string `json:"class"`
		Reason  string `json:"reason"`
		// Payload is the kind-shaped body: the session id under
		// started, class/reason under failed, the delivery arguments
		// under delivered/partial at this one nested level — decoded
		// per kind. The usage figures ride flat, siblings of these
		// keys (probed 2026-10-25: usage carries no nested object), so
		// they sit directly on this struct below.
		Payload json.RawMessage `json:"payload"`

		InputTokens      int64   `json:"input_tokens"`
		OutputTokens     int64   `json:"output_tokens"`
		CacheReadTokens  int64   `json:"cache_read_tokens"`
		CacheWriteTokens int64   `json:"cache_write_tokens"`
		CostDollars      float64 `json:"cost_dollars"`
	} `json:"payload"`
}

// handsDelivery is the forced delivery tool's argument schema, fixed in
// this hands build (summary required, files_changed optional — probed
// live; the fit review's caller-supplied-schema shape did not land), so a
// round always ends in a structured deliverable instead of free prose.
type handsDelivery struct {
	Summary      string   `json:"summary"`
	FilesChanged []string `json:"files_changed"`
}

// parseHandsStream is parseAgentStream for hands's --format json event
// stream: the started event carries the conversation session, and the
// terminal delivered/partial event's delivery summary is the round's
// visible text — the forced deliverable, not whatever narration preceded
// it. hands emits no thinking events (the wire thinking knob stages
// through its config), so thinking stays empty.
func parseHandsStream(stdout string) (thinking, text, session string) {
	for line := range strings.SplitSeq(stdout, "\n") {
		var e handsEvent
		if json.Unmarshal([]byte(line), &e) != nil {
			continue
		}
		switch e.Kind {
		case "started":
			session = e.Payload.Session
		case "delivered", "partial":
			var d handsDelivery
			if json.Unmarshal(e.Payload.Payload, &d) == nil && d.Summary != "" {
				text = d.Summary
			}
		}
	}
	return "", text, session
}

// parseHandsUsage is parseAgentUsage for hands: usage events ride per
// provider request, so the round's raw figure is the sum across them (the
// same total claude's single result event carries). Duration stays zero —
// hands reports none — and the caller stamps the measured wall time.
func parseHandsUsage(stdout string) (usage Usage) {
	for line := range strings.SplitSeq(stdout, "\n") {
		var e handsEvent
		if json.Unmarshal([]byte(line), &e) != nil {
			continue
		}
		if e.Kind != "usage" {
			continue
		}
		usage.CostUSD += e.Payload.CostDollars
		usage.InputTokens += e.Payload.InputTokens
		usage.OutputTokens += e.Payload.OutputTokens
		usage.CacheReadTokens += e.Payload.CacheReadTokens
		usage.CacheWriteTokens += e.Payload.CacheWriteTokens
	}
	return usage
}

// parseRoundOutput extracts a jailed round's chain of thought, visible
// text, conversation session id, and raw usage metrics from its stdout,
// dispatching on the CLI: claude and amp share the claude event schema
// (parseAgentStream); pi emits its own JSON-lines schema (parsePiStream);
// hands emits its own typed event protocol (parseHandsStream); opencode,
// codex, and aider print plain text and yield empty fields — the
// caller's cue to take the raw stdout as the text.
func parseRoundOutput(agent, stdout string) (thinking, text, session string, usage Usage) {
	if agent == "pi" {
		thinking, text, session = parsePiStream(stdout)
		return thinking, text, session, parsePiUsage(stdout)
	}
	if agent == "hands" {
		thinking, text, session = parseHandsStream(stdout)
		return thinking, text, session, parseHandsUsage(stdout)
	}
	thinking, text, session = parseAgentStream(stdout)
	return thinking, text, session, parseAgentUsage(stdout)
}

// agentAPIError reports the API-level failure a round's structured output
// declares, "" when none: claude's terminal result event carries
// terminal_reason "api_error" when the CLI died on a provider error
// (shape verified against the img-upload incident stdout), and pi's
// json stream folds the same failures into a message whose stopReason is
// "error" — with pi still exiting 0, so without reading it the round
// would count as a success carrying an error-shaped body (shape verified
// against pi's published source; no pi binary on this host to probe
// live). The classification is the CLI's own machine-readable report, not
// chat text, so the caller can route it into the ErrAPIExhausted rescue
// (immediate provider failover, then the workflow's hourly heartbeat)
// wherever the CLI chose to put the report. amp rides the claude branch
// on the assumption its reused schema carries the same field — unverifiable
// here (no binary) and the diff declares amp unmaintained, so it stays an
// assumption: if the shape differs, amp's face degrades to "" (inert).
// Agents without a structured stream (opencode, codex, aider) report ""
// here; their plain-text face is matched separately (apiErrorTextMarkers).
// hands reports through its terminal failed event, and only the class
// "retriable" is the API-level report (probe-pinned 2026-10-25: a failed
// event is always the stream's last line — the process exits 1 right
// after it — because an internally retried provider failure, two attempts
// per request under a persistent 500 and under a mid-stream connection
// drop alike, emits no failed event at all; a refused connection or an
// exhausted-retry 500 surfaces as class retriable; the other observed
// classes are round-level — "no-delivery", the loop guard, and
// "terminal", a response hands could not use — and stay on the generic
// failure path).
func agentAPIError(agent, stdout string) string {
	switch agent {
	case "hands":
		apiErr := ""
		for line := range strings.SplitSeq(stdout, "\n") {
			var e handsEvent
			if json.Unmarshal([]byte(line), &e) != nil {
				continue
			}
			// Last retriable wins — a totality guard, not a pi-mirror
			// standing rule: a failed event is stream-terminal (probe-
			// pinned 2026-10-25 — hands's internal retries are silent, so
			// nothing ever follows a failed event, least of all a
			// delivery), so no later clean event can exist here to clear
			// it. The other classes are untracked — they stay on the
			// generic failure path by design.
			if e.Kind == "failed" && e.Payload.Class == "retriable" {
				apiErr = "class retriable: " + e.Payload.Reason
			}
		}
		return apiErr
	case "pi":
		// The last message_end decides, mirroring parsePiStream's own
		// authority rule: pi writes an error-stopReason message_end the
		// moment one completion fails — before its own auto-retry has had
		// its chance — so the first hit must not decide; a later
		// message_end (any stopReason but error, the empty shape of a
		// clean end included) clears it. Only an error still standing at
		// the end of the stream means the round folded.
		errMsg := ""
		for line := range strings.SplitSeq(stdout, "\n") {
			var m piStreamMessage
			if json.Unmarshal([]byte(line), &m) != nil {
				continue
			}
			if m.Type != "message_end" {
				continue
			}
			if m.Message.StopReason == "error" {
				if m.Message.ErrorMessage != "" {
					errMsg = m.Message.ErrorMessage
				} else {
					errMsg = "stopReason error"
				}
			} else {
				errMsg = ""
			}
		}
		return errMsg
	case "claude", "amp":
		for line := range strings.SplitSeq(stdout, "\n") {
			var m streamMessage
			if json.Unmarshal([]byte(line), &m) != nil {
				continue
			}
			if m.Type == "result" && m.TerminalReason == "api_error" {
				if m.Result != "" {
					return "terminal_reason api_error: " + m.Result
				}
				return "terminal_reason api_error"
			}
		}
	}
	return ""
}

// handsStuck reports a hands round's structured stream declaring its own
// death stuck, "" when none: the failed event carrying class "stuck" —
// hands's watchdog verdict that the round went silent, zero wire events
// for its whole silence window (prompt-prefill time included), with the
// window in the reason. Returns the "class stuck: <reason>" composite, so
// a hit is never empty even when the event carries no reason — the caller
// branches on non-emptiness, and a bare-reason return would let a missing
// payload field launder the class match back into the generic failure
// path (the same shape agentAPIError's retriable branch returns). Parsed
// from the event's class field exactly like that branch reads — the
// harness's machine report, never raw text — and last-wins for the same
// totality reason: a failed event is stream-terminal (the process exits
// right after it), so no later clean event can exist to clear it. Every
// other class stays untracked here: retriable belongs to agentAPIError,
// the rest to the generic failure path.
func handsStuck(stdout string) string {
	reason := ""
	for line := range strings.SplitSeq(stdout, "\n") {
		var e handsEvent
		if json.Unmarshal([]byte(line), &e) != nil {
			continue
		}
		if e.Kind == "failed" && e.Payload.Class == "stuck" {
			reason = "class stuck: " + e.Payload.Reason
		}
	}
	return reason
}

// toolLoopThreshold is the consecutive-identical-call count at which a pi
// round's loop watcher kills the round: three — one legitimate retry of a
// flaky command is a single repeat, so two identical calls stay under the
// threshold, and a third in a row is the degenerate livelock (every call
// succeeds, heartbeats stay fresh, and only the round budget would ever
// end it). Named so the kill's wrap can cite the count it fired at.
const toolLoopThreshold = 3

// piToolExecEvent is the one pi json event the loop watcher consumes
// (pi.dev/docs/latest/json): tool_execution_start fires once per executed
// tool call, naming the tool and carrying its arguments object. The
// generated call id is deliberately not captured — a re-issued call gets
// a new id every time, so the loop identity excludes it by construction.
type piToolExecEvent struct {
	Type     string          `json:"type"`
	ToolName string          `json:"toolName"`
	Args     json.RawMessage `json:"args"`
}

// canonicalJSON re-encodes raw json so equal values render equal strings:
// decoding into any and marshaling again sorts object keys and strips
// whitespace, while UseNumber keeps every number literal verbatim — plain
// float64 decoding collapses adjacent integers above 2^53 (snowflake ids,
// unix-nano timestamps) onto one float, which would alias distinct calls
// into one loop identity. Two emissions of the same call compare equal
// regardless of key order or spacing; any argument difference stays
// distinct. Raw bytes fall through when they are not exactly one json
// value — byte-identical repeats still compare equal.
func canonicalJSON(raw json.RawMessage) string {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return string(raw)
	}
	// Unmarshal's own strictness: the input must be exactly one value, or
	// the raw bytes stay the identity.
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return string(raw)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return string(raw)
	}
	return string(b)
}

// piToolLoopWatcher watches one pi round's json stream live for the
// identical-tool-call livelock small models fall into: every round-trip
// re-issues the call, every call executes successfully, and the round
// looks healthy to everything but its budget while it produces nothing.
// It implements io.Writer and rides the round's stdout tee (an
// io.MultiWriter beside the collection buffer, which keeps its existing
// role), carrying partial lines between writes, and counts consecutive
// tool_execution_start identities — tool name plus canonicalized
// arguments. At toolLoopThreshold identical calls it kills the round
// through kill (the round's process-group kill) and stays fired; a call
// with a different identity resets the streak, so interleaved distinct
// calls, tool-name changes, and argument changes never fire it. The
// comparison runs on parsed structured events — the same machine-readable
// surface agentAPIError reads post-hoc — never on raw text markers, so
// nothing the agent prints can forge the kill or its classification.
// Writes arrive on exec's stdout-copy goroutine and all complete before
// cmd.Wait returns (Wait reaps the copiers), so the caller reads fired
// after Wait without a lock.
type piToolLoopWatcher struct {
	// kill ends the round once fired. Called on exec's stdout-copy
	// goroutine.
	kill  func()
	buf   []byte // partial line carried between writes
	last  string // identity of the previous tool call
	count int    // consecutive calls matching last
	fired bool
}

// Write tees the stream through the line scanner. It never fails: the tee
// must stay transparent, and an error here would corrupt the round's own
// output collection.
func (w *piToolLoopWatcher) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.scanLine(w.buf[:i])
		w.buf = w.buf[i+1:]
	}
	return len(p), nil
}

// scanLine folds one completed stream line into the streak, killing the
// round on the threshold hit.
func (w *piToolLoopWatcher) scanLine(line []byte) {
	var e piToolExecEvent
	if json.Unmarshal(line, &e) != nil || e.Type != "tool_execution_start" {
		return
	}
	id := e.ToolName + "\x00" + canonicalJSON(e.Args)
	if id == w.last {
		w.count++
	} else {
		w.last, w.count = id, 1
	}
	if w.count >= toolLoopThreshold && !w.fired {
		w.fired = true
		w.kill()
	}
}

// verdictMarkers are the four daedalus review verdict markers — the set
// parseReviewVerdict exact-matches and the NoVerdict repair's classifier
// chooses among, declared once so the two cannot drift.
var verdictMarkers = []string{"APPROVED", "CHANGES_REQUESTED", "NEEDS_MAINTAINER", "REBUILD"}

// parseReviewVerdict extracts the verdict from reviewer output: the last
// non-empty line decides. APPROVED approves; NEEDS_MAINTAINER parks the run
// for a maintainer; REBUILD (offered only to the test reviewer) routes the
// finding back to the implementation cycle; CHANGES_REQUESTED (or any other
// unrecognized line, including a missing marker) counts as changes
// requested, with everything above the verdict line — or the whole output,
// when no marker was found — as the comments to feed back to the
// implementing agent. A missing marker additionally reports NoVerdict, so
// the workflow can count marker-less exits against its strike budget; a
// marker-less round is still a changes-requested round (the contract above
// is unchanged), just a distinctly-counted one.
func parseReviewVerdict(out string) ReviewResult {
	lines := strings.Split(out, "\n")
	for i, raw := range slices.Backward(lines) {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if slices.Contains(verdictMarkers, line) {
			return ReviewResult{
				Approved:        line == "APPROVED",
				NeedsMaintainer: line == "NEEDS_MAINTAINER",
				Rebuild:         line == "REBUILD",
				Comments:        strings.TrimSpace(strings.Join(lines[:i], "\n")),
			}
		}
		break
	}
	return ReviewResult{Approved: false, NoVerdict: true, Comments: strings.TrimSpace(out)}
}
