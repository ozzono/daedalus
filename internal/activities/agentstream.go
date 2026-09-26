package activities

import (
	"encoding/json"
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
// a structured result (opencode's and aider's plain text) contribute
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
// computes deltas. A stream without a result event (opencode's and aider's
// plain text) reports a zero Usage.
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

// parseRoundOutput extracts a jailed round's chain of thought, visible
// text, conversation session id, and raw usage metrics from its stdout,
// dispatching on the CLI: claude and amp share the claude event schema
// (parseAgentStream); pi emits its own JSON-lines schema (parsePiStream);
// opencode and aider print plain text and yield empty fields — the
// caller's cue to take the raw stdout as the text.
func parseRoundOutput(agent, stdout string) (thinking, text, session string, usage Usage) {
	if agent == "pi" {
		thinking, text, session = parsePiStream(stdout)
		return thinking, text, session, parsePiUsage(stdout)
	}
	thinking, text, session = parseAgentStream(stdout)
	return thinking, text, session, parseAgentUsage(stdout)
}

// parseReviewVerdict extracts the verdict from reviewer output: the last
// non-empty line decides. APPROVED approves; NEEDS_MAINTAINER parks the run
// for a maintainer; REBUILD (offered only to the test reviewer) routes the
// finding back to the implementation cycle; CHANGES_REQUESTED (or any other
// unrecognized line, including a missing marker) counts as changes
// requested, with everything above the verdict line — or the whole output,
// when no marker was found — as the comments to feed back to the
// implementing agent.
func parseReviewVerdict(out string) ReviewResult {
	lines := strings.Split(out, "\n")
	for i, raw := range slices.Backward(lines) {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		switch line {
		case "APPROVED", "CHANGES_REQUESTED", "NEEDS_MAINTAINER", "REBUILD":
			return ReviewResult{
				Approved:        line == "APPROVED",
				NeedsMaintainer: line == "NEEDS_MAINTAINER",
				Rebuild:         line == "REBUILD",
				Comments:        strings.TrimSpace(strings.Join(lines[:i], "\n")),
			}
		}
		break
	}
	return ReviewResult{Approved: false, Comments: strings.TrimSpace(out)}
}
