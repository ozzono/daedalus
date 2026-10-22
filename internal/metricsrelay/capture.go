// Per-round content capture — the tee's second retention, beside the usage
// ledger. Every keyed, inspected round's request body (the prompt side —
// for a chained CLI the accumulated conversation its last provider call
// carried) and the assistant text extracted from its response stream are
// kept here, last write wins per round, under the ledger's own discipline:
// in-memory only, dropped on daemon restart, plain eviction. The capture is
// keyed-only by construction — an unkeyed round has no per-round
// attribution to file it under. `daedalus log <wf> -cot` reads it as its
// primary source (see cmd/daedalus/tasklog.go) over the /capture endpoint;
// durable roll-ups stay the golens-metrics embed's concern.
package metricsrelay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// contentCap bounds each captured side of one round's record — the
// per-entry byte bound the content-bearing entries forced (the ledger's
// entry-count ring alone cannot bound content). Sized to the largest
// plausible single round: a full-window conversation plus its answer stays
// two orders of magnitude under it.
const contentCap = 1 << 20

// captureCap bounds how many rounds the capture tracks; past it the oldest
// round's record is evicted — plain eviction, the ring's own rule.
const captureCap = 2048

// captureKey is one jailed round's attribution: the parts the synthetic
// attribution key encodes, split by the relay's parse.
type captureKey struct{ task, role, round string }

// CapturedRound is one keyed round's wire record. Prompt is the request
// body of the round's last captured provider call; Response is the
// assistant text extracted from that call's stream — the material `-cot`
// renders. A capture that hit its byte cap says so inline (the truncation
// marker), never silently.
type CapturedRound struct {
	At       time.Time `json:"at"`
	Prompt   string    `json:"prompt,omitempty"`
	Response string    `json:"response,omitempty"`
}

// Captures is the in-memory capture store: last write wins per round, so a
// round's record always holds its most complete provider call (a chained
// CLI's last request carries everything before it). Zero value ready.
type Captures struct {
	mu    sync.Mutex
	m     map[captureKey]CapturedRound
	order []captureKey
}

// Put records one round's capture. An existing record for the round is
// replaced, not appended; rounds past captureCap evict oldest-first.
func (c *Captures) Put(task, role, round string, cr CapturedRound) {
	if task == "" || round == "" {
		return // no per-round attribution, nothing to file the capture under
	}
	k := captureKey{task, role, round}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, seen := c.m[k]; !seen {
		if c.m == nil {
			c.m = map[captureKey]CapturedRound{}
		}
		c.m[k] = cr
		c.order = append(c.order, k)
		if len(c.order) > captureCap {
			delete(c.m, c.order[0])
			c.order = c.order[1:]
		}
		return
	}
	c.m[k] = cr
}

// Get returns one round's record; ok is false when the capture holds none
// (never captured, aged out, or the daemon restarted).
func (c *Captures) Get(task, role, round string) (CapturedRound, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cr, ok := c.m[captureKey{task, role, round}]
	return cr, ok
}

// serveCapture answers one round's record for the -cot reader. 404 — not
// an error shape — is the capture-absent case the caller degrades on.
func (r *Relay) serveCapture(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	cr, ok := r.captures.Get(q.Get("task"), q.Get("role"), q.Get("round"))
	if !ok {
		http.Error(w, "metricsrelay: no capture for that round", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cr)
}

// cappedBuffer is an io.Writer that keeps its first cap bytes and discards
// the rest, remembering that it did — the tee must never fail a forward
// over capture bookkeeping, and a truncated capture must say so.
type cappedBuffer struct {
	buf      bytes.Buffer
	cap      int
	overflow bool
}

func newCappedBuffer(cap int) *cappedBuffer { return &cappedBuffer{cap: cap} }

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if room := c.cap - c.buf.Len(); room > 0 {
		if len(p) > room {
			c.buf.Write(p[:room])
			c.overflow = true
		} else {
			c.buf.Write(p)
		}
	} else if len(p) > 0 {
		c.overflow = true
	}
	return len(p), nil // always claim the full write: the tee never errors
}

// String renders the captured bytes, appending one truncation marker when
// the cap bit — the reader can tell a bounded capture from a whole one.
func (c *cappedBuffer) String() string {
	s := c.buf.String()
	if c.overflow {
		s += fmt.Sprintf("\n[daedalus: capture truncated at %d bytes]", c.cap)
	}
	return s
}

// assistantText extracts the assistant-visible text — thinking plus answer
// — from a buffered response body, either wire's plain-JSON shape or its
// SSE chunk stream: the same bodies applyUsage reads the usage block from.
// A shape it does not recognize extracts to "" (an error page carries no
// assistant text to find). ponytail: only the four documented shapes are
// walked — the wires' canonical completions shapes; anything else was
// never assistant text.
func assistantText(body []byte) string {
	var pieces []string
	add := func(s string) {
		if s != "" {
			pieces = append(pieces, s)
		}
	}
	// Plain JSON first, either wire.
	var plain struct {
		Content []struct {
			Type     string `json:"type"`
			Text     string `json:"text"`
			Thinking string `json:"thinking"`
		} `json:"content"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if json.Unmarshal(body, &plain) == nil {
		for _, b := range plain.Content {
			add(b.Text)
			add(b.Thinking)
		}
		for _, c := range plain.Choices {
			add(c.Message.Content)
		}
		if len(pieces) > 0 {
			return strings.Join(pieces, "\n\n")
		}
	}
	// SSE: anthropic reports text and thinking in content_block_delta
	// events; openai streams choices[].delta.content.
	for _, line := range strings.Split(string(body), "\n") {
		payload, ok := strings.CutPrefix(strings.TrimSpace(line), "data:")
		if !ok {
			continue
		}
		payload = strings.TrimSpace(payload)
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var chunk struct {
			Type  string `json:"type"`
			Delta struct {
				Type     string `json:"type"`
				Text     string `json:"text"`
				Thinking string `json:"thinking"`
				Content  string `json:"content"`
			} `json:"delta"`
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(payload), &chunk) != nil {
			continue
		}
		if chunk.Type == "content_block_delta" {
			add(chunk.Delta.Text)
			add(chunk.Delta.Thinking)
		}
		for _, c := range chunk.Choices {
			add(c.Delta.Content)
		}
	}
	return strings.Join(pieces, "\n\n")
}
