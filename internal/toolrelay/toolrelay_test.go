package toolrelay

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordedRequest is one request a stub upstream received: path, headers,
// and body — every upstream-side assertion starts from what actually
// arrived on the wire.
type recordedRequest struct {
	Path   string
	Header http.Header
	Body   []byte
}

// recorder collects stub-upstream requests; the relay serves them
// concurrently with the test's assertions, so access is mutex-guarded.
type recorder struct {
	mu   sync.Mutex
	reqs []recordedRequest
}

func (rec *recorder) add(r *http.Request, body []byte) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.reqs = append(rec.reqs, recordedRequest{Path: r.URL.Path, Header: r.Header.Clone(), Body: body})
}

func (rec *recorder) all() []recordedRequest {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]recordedRequest(nil), rec.reqs...)
}

// startStubRelay starts a relay whose upstream is srv plus the /v1 path
// prefix the openai section carries, closed at test cleanup.
func startStubRelay(t *testing.T, srv *httptest.Server, parserModel string) *Relay {
	t.Helper()
	r, err := Start(srv.URL+"/v1", parserModel)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

// stubUpstream serves the two legs the relay dials: the forwarded
// completions request (answered with the test's forward bytes) and the
// relay's parser-model call — the only request shape carrying ollama's
// "format" field — answered with the parser output as its content.
func stubUpstream(rec *recorder, forward []byte, forwardStatus int, parserOut string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.add(r, body)
		var top map[string]json.RawMessage
		if json.Unmarshal(body, &top) == nil {
			if _, ok := top["format"]; ok {
				w.Header().Set("Content-Type", "application/json")
				w.Write(completionReply(&parserOut, ""))
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(forwardStatus)
		w.Write(forward)
	}))
}

// completionReply marshals a plain (non-streaming) chat completion whose
// answer is content and which carries toolCalls (raw JSON, "" for none) —
// both the upstream stub's replies and the parser reply take this shape.
func completionReply(content *string, toolCalls string) []byte {
	msg := map[string]any{"role": "assistant", "content": content}
	if toolCalls != "" {
		msg["tool_calls"] = json.RawMessage(toolCalls)
	}
	b, err := json.Marshal(map[string]any{"choices": []any{map[string]any{"message": msg}}})
	if err != nil {
		panic(err) // unreachable: map of marshalable values
	}
	return b
}

// sseReply renders content fragments as a chat-completions SSE chunk
// stream, [DONE]-terminated — the streamed shape a stream:true upstream
// reply takes.
func sseReply(fragments ...string) []byte {
	var b bytes.Buffer
	for _, f := range fragments {
		chunk, err := json.Marshal(map[string]any{
			"choices": []any{map[string]any{"delta": map[string]any{"content": f}}},
		})
		if err != nil {
			panic(err) // unreachable: map of marshalable values
		}
		b.WriteString("data: " + string(chunk) + "\n\n")
	}
	b.WriteString("data: [DONE]\n\n")
	return b.Bytes()
}

// toolsRequestBody marshals a chat-completions request offering one tool,
// read_file(path), the name a liftable content must carry.
func toolsRequestBody(t *testing.T, stream bool) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"model":  "qwen2.5-coder:3b",
		"stream": stream,
		"tools": []any{map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        "read_file",
				"description": "read a file",
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{"path": map[string]any{"type": "string"}},
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return b
}

// fenced wraps s in a ```json fence, the shape qwen2.5-coder emits about
// half the time.
func fenced(s string) string { return "```json\n" + s + "\n```" }

// strPtr exists so table literals can take a content pointer inline.
func strPtr(s string) *string { return &s }

// syntheticChunk is one SSE chunk of a streamed synthetic reply.
type syntheticChunk struct {
	Choices []struct {
		Delta struct {
			Content   *string `json:"content"`
			ToolCalls []struct {
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
}

// parseChunks splits an SSE body into its data payloads, failing the test
// when a non-[DONE] data line is unparseable or carries other than one
// choice.
func parseChunks(t *testing.T, body []byte) []syntheticChunk {
	t.Helper()
	var out []syntheticChunk
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") || line == "data: [DONE]" {
			continue
		}
		var ch syntheticChunk
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ch); err != nil {
			t.Fatalf("parse chunk %q: %v", line, err)
		}
		if len(ch.Choices) != 1 {
			t.Fatalf("chunk %q carries %d choices, want 1", line, len(ch.Choices))
		}
		out = append(out, ch)
	}
	return out
}

// postCompletions POSTs body to the relay's completions path (the staged
// URL already carries the /v1 prefix) and returns the status with the
// fully-read body.
func postCompletions(t *testing.T, relayURL string, body []byte) (int, []byte) {
	t.Helper()
	resp, err := http.Post(relayURL+"/chat/completions", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST relay: %v", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read relay response: %v", err)
	}
	return resp.StatusCode, b
}

// decodedSynthetic is the slice of the synthetic tool_calls completion the
// lift contract is asserted on.
type decodedSynthetic struct {
	Choices []struct {
		Message struct {
			Content   *string `json:"content"`
			ToolCalls []struct {
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
}

// TestCloseIsQuiet pins the designed shutdown's face: Close goes through
// the server, so Serve exits on the http.ErrServerClosed path and logs
// nothing — with the listener closed underneath it, Serve exits with the
// raw accept error instead, and every worker shutdown logs a bogus
// "toolrelay: serve:" line for the designed close. Serve's exit is not
// observable from outside, so the quietness is bounded-checked: a wrong-path
// exit logs within moments of the listener closing, and the check fails
// fast on the line, passing only after a short clean window.
func TestCloseIsQuiet(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer up.Close()
	r, err := Start(up.URL+"/v1", "parser")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Serve must be up and answering before the close — otherwise the
	// quietness would be vacuous (nothing was ever serving).
	status, body := postCompletions(t, r.URL(), []byte(`{}`))
	if status != http.StatusOK {
		t.Fatalf("relay not serving before Close: status %d, body %s", status, body)
	}

	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The relay must actually be down: the designed shutdown stops the
	// listener, so the next dial fails rather than hanging or answering.
	if resp, err := http.Post(r.URL()+"/chat/completions", "application/json", strings.NewReader(`{}`)); err == nil {
		resp.Body.Close()
		t.Error("relay still answering after Close, want the listener down")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if msg := logBuf.String(); msg != "" {
			t.Fatalf("designed shutdown logged %q, want a quiet close (Serve exited off the http.ErrServerClosed path)", msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestPinsAcceptEncodingIdentity pins the wire invariant both legs rely on:
// the forwarded round trip and the parser call travel with
// Accept-Encoding: identity — a transport asked to negotiate gzip would
// re-add the header itself and transparently unwrap the reply, so the plain
// bytes the JSON inspection reads would hold only for transports with that
// auto-gzip behavior, not by the pin. The stub upstream negotiates for
// real (gzip asked, gzip answered; identity asked, plain answered), and pi's
// own request carries Accept-Encoding: gzip, so a drop instead of a set
// would let the transport backfill gzip and show on the wire.
func TestPinsAcceptEncodingIdentity(t *testing.T) {
	const raw = `{"name":"read_file","arguments":{"path":"a.txt"}}`
	forward := completionReply(strPtr(fenced(raw)), "")
	parserOut := `{"path":"a.txt"}`
	// negotiate replies gzip-encoded when the request asks for gzip and
	// plain otherwise — an upstream honoring the wire's negotiation.
	negotiate := func(w http.ResponseWriter, r *http.Request, body []byte) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Type", "application/json")
			w.Write(body)
			return
		}
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(body); err != nil {
			panic(err) // unreachable: in-memory writer
		}
		if err := zw.Close(); err != nil {
			panic(err) // unreachable: in-memory writer
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		w.Write(buf.Bytes())
	}
	rec := &recorder{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.add(r, body)
		var top map[string]json.RawMessage
		if json.Unmarshal(body, &top) == nil {
			if _, ok := top["format"]; ok {
				negotiate(w, r, completionReply(&parserOut, ""))
				return
			}
		}
		negotiate(w, r, forward)
	}))
	defer up.Close()
	relay := startStubRelay(t, up, "parser-model")

	// pi's dial negotiates gzip — the header the relay must overwrite, not
	// merely drop, on both of its own dials.
	req, err := http.NewRequest(http.MethodPost, relay.URL()+"/chat/completions",
		bytes.NewReader(toolsRequestBody(t, false)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST relay: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read relay response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	// The inspected body was plain enough to lift: the answer became a
	// native tool call, so the identity pin held end to end.
	var got decodedSynthetic
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("parse relay reply %s: %v", body, err)
	}
	if len(got.Choices) != 1 || len(got.Choices[0].Message.ToolCalls) != 1 ||
		got.Choices[0].Message.ToolCalls[0].Function.Name != "read_file" {
		t.Errorf("relay reply %s, want the lifted read_file tool call (the inspection saw plain bytes)", body)
	}

	reqs := rec.all()
	if len(reqs) != 2 {
		t.Fatalf("upstream got %d requests, want 2 (forward + parser)", len(reqs))
	}
	for _, leg := range reqs {
		if got := leg.Header.Get("Accept-Encoding"); got != "identity" {
			t.Errorf("leg %s Accept-Encoding = %q, want identity pinned on the wire", leg.Path, got)
		}
	}
}

// TestStartRejectsUnusableUpstream pins the boot guard: an upstream URL
// without a scheme or host cannot be forwarded to, and Start refuses
// instead of serving a relay that 502s every request.
func TestStartRejectsUnusableUpstream(t *testing.T) {
	for _, upstream := range []string{"127.0.0.1:11434", "http:///v1", "://bad"} {
		r, err := Start(upstream, "parser")
		if err == nil {
			r.Close()
			t.Errorf("Start(%q) = nil error, want rejection", upstream)
		}
	}
}

// TestRelayURLCarriesUpstreamPath pins the staged URL's shape: the relay's
// own loopback address plus the upstream's path prefix, so pi's request
// paths land on the relay exactly as they would land on the upstream.
func TestRelayURLCarriesUpstreamPath(t *testing.T) {
	r, err := Start("http://upstream.example:11434/v1", "parser")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	u, err := url.Parse(r.URL())
	if err != nil {
		t.Fatalf("parse relay URL: %v", err)
	}
	if u.Scheme != "http" || u.Hostname() != "127.0.0.1" {
		t.Errorf("relay URL %q, want a loopback http address", r.URL())
	}
	if u.Path != "/v1" {
		t.Errorf("relay URL %q, want the upstream's /v1 prefix preserved", r.URL())
	}
}

// TestNonCompletionsPassThroughVerbatim pins the inspection gate: only
// chat-completions POSTs are tool-bearing — anything else (model pulls,
// other endpoints) streams through the reverse proxy untouched, upstream
// path included.
func TestNonCompletionsPassThroughVerbatim(t *testing.T) {
	rec := &recorder{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.add(r, body)
		w.Write([]byte("upstream-reply"))
	}))
	defer up.Close()
	relay := startStubRelay(t, up, "parser")

	for _, c := range []struct {
		name string
		do   func() (*http.Response, error)
	}{
		{"GET model list", func() (*http.Response, error) {
			return http.Get(relay.URL() + "/models")
		}},
		{"POST embeddings", func() (*http.Response, error) {
			return http.Post(relay.URL()+"/embeddings", "application/json", strings.NewReader(`{"input":"x"}`))
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			resp, err := c.do()
			if err != nil {
				t.Fatalf("relay request: %v", err)
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if string(b) != "upstream-reply" {
				t.Errorf("body = %q, want the upstream's bytes verbatim", b)
			}
		})
	}
}

// TestLiftsTextEncodedToolCall pins the relay's core lift: an upstream
// answer that is exactly one JSON object naming one of the request's own
// tools becomes a native tool_calls completion for pi — non-streaming as
// one body, streaming as a single-delta SSE chunk stream — with the
// parser model normalizing the arguments against the tool's schema on a
// second dial to the same upstream.
func TestLiftsTextEncodedToolCall(t *testing.T) {
	const raw = `{"name":"read_file","arguments":{"path":"a.txt"}}`
	for _, c := range []struct {
		name      string
		stream    bool
		forward   []byte
		parserOut string
	}{
		{
			name:      "fenced content, plain request",
			forward:   completionReply(strPtr(fenced(raw)), ""),
			parserOut: `{"path":"a.txt"}`,
		},
		{
			name:      "bare JSON content, fenced parser output",
			forward:   completionReply(strPtr(raw), ""),
			parserOut: fenced(`{"path":"a.txt"}`),
		},
		{
			name:      "streamed content fragments",
			stream:    true,
			forward:   sseReply("```json\n", `{"name":"read`, `_file","arguments":`, `{"path":"a.txt"}}`, "\n```"),
			parserOut: `{"path":"a.txt"}`,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := &recorder{}
			up := stubUpstream(rec, c.forward, http.StatusOK, c.parserOut)
			defer up.Close()
			relay := startStubRelay(t, up, "parser-model")

			status, body := postCompletions(t, relay.URL(), toolsRequestBody(t, c.stream))
			if status != http.StatusOK {
				t.Fatalf("status = %d, body %s", status, body)
			}
			if !c.stream {
				var got decodedSynthetic
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatalf("parse synthetic reply %s: %v", body, err)
				}
				if len(got.Choices) != 1 {
					t.Fatalf("choices = %d, want 1 (body %s)", len(got.Choices), body)
				}
				msg := got.Choices[0].Message
				if msg.Content != nil {
					t.Errorf("message content = %v, want nil (the answer became a tool call)", *msg.Content)
				}
				if len(msg.ToolCalls) != 1 {
					t.Fatalf("tool_calls = %d, want 1 (body %s)", len(msg.ToolCalls), body)
				}
				tc := msg.ToolCalls[0]
				if tc.Type != "function" || tc.Function.Name != "read_file" {
					t.Errorf("tool call = %s/%s, want function/read_file", tc.Type, tc.Function.Name)
				}
				var args map[string]any
				if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
					t.Fatalf("arguments %q are not a JSON object: %v", tc.Function.Arguments, err)
				}
				if args["path"] != "a.txt" {
					t.Errorf("arguments = %v, want the parser-normalized path", args)
				}
				if got.Choices[0].FinishReason != "tool_calls" {
					t.Errorf("finish_reason = %q, want tool_calls", got.Choices[0].FinishReason)
				}
			} else {
				if !strings.HasSuffix(string(body), "data: [DONE]\n\n") {
					t.Errorf("stream does not end with the [DONE] sentinel:\n%s", body)
				}
				chunks := parseChunks(t, body)
				if len(chunks) != 2 {
					t.Fatalf("stream has %d chunks, want 2 (the tool-call delta and the closer):\n%s", len(chunks), body)
				}
				tc := chunks[0].Choices[0].Delta.ToolCalls
				if len(tc) != 1 || tc[0].Function.Name != "read_file" {
					t.Fatalf("first delta tool call = %+v, want one for read_file", tc)
				}
				var args map[string]any
				if err := json.Unmarshal([]byte(tc[0].Function.Arguments), &args); err != nil {
					t.Fatalf("delta arguments %q are not a JSON object: %v", tc[0].Function.Arguments, err)
				}
				if args["path"] != "a.txt" {
					t.Errorf("delta arguments = %v, want the parser-normalized path", args)
				}
				if fr := chunks[1].Choices[0].FinishReason; fr == nil || *fr != "tool_calls" {
					t.Errorf("closer finish_reason = %v, want tool_calls", fr)
				}
			}

			// Both legs hit the same upstream: the forwarded round trip,
			// then the parser call on the incoming completions path.
			reqs := rec.all()
			if len(reqs) != 2 {
				t.Fatalf("upstream got %d requests, want 2 (forward + parser)", len(reqs))
			}
			if reqs[0].Path != "/v1/chat/completions" {
				t.Errorf("forwarded path = %q, want /v1/chat/completions", reqs[0].Path)
			}
			if string(reqs[0].Body) != string(toolsRequestBody(t, c.stream)) {
				t.Errorf("forwarded body was not replayed verbatim:\n%s", reqs[0].Body)
			}
			var preq struct {
				Model    string `json:"model"`
				Stream   bool   `json:"stream"`
				Format   any    `json:"format"`
				Messages []struct {
					Content string `json:"content"`
				} `json:"messages"`
			}
			if err := json.Unmarshal(reqs[1].Body, &preq); err != nil {
				t.Fatalf("parse parser request %s: %v", reqs[1].Body, err)
			}
			if preq.Model != "parser-model" || preq.Stream {
				t.Errorf("parser request model/stream = %q/%v, want parser-model/false", preq.Model, preq.Stream)
			}
			if preq.Format == nil {
				t.Error("parser request carries no structured-output format")
			}
			if len(preq.Messages) != 1 ||
				!strings.Contains(preq.Messages[0].Content, "read_file") ||
				!strings.Contains(preq.Messages[0].Content, `"path":"a.txt"`) {
				t.Errorf("parser prompt %q, want the tool name and the raw arguments", preq.Messages)
			}
		})
	}
}

// TestLiftFailurePassesThrough pins fail-open: every lift refusal — no
// tools offered, a native tool_calls answer, prose, an unknown tool,
// non-object arguments, a parser failure — replays the upstream bytes
// byte-identically with a 200, and a non-200 upstream replays its own
// status. No case may reach the parser (except the parser-failure one,
// which must reach exactly it), so the refusal happens before the second
// dial.
func TestLiftFailurePassesThrough(t *testing.T) {
	const raw = `{"name":"read_file","arguments":{"path":"a.txt"}}`
	toolsNoStream := func(t *testing.T) []byte { return toolsRequestBody(t, false) }
	prose := completionReply(strPtr("APPROVED — ship it."), "")
	for _, c := range []struct {
		name          string
		req           func(t *testing.T) []byte
		forward       []byte
		forwardStatus int
		parserStatus  int
		wantStatus    int
		wantParser    bool
	}{
		{
			name:       "request offers no tools",
			req:        func(t *testing.T) []byte { return []byte(`{"model":"m","stream":false,"messages":[]}`) },
			forward:    prose,
			wantStatus: http.StatusOK,
		},
		{
			name:       "native tool_calls answer",
			req:        toolsNoStream,
			forward:    completionReply(nil, `[{"id":"c1","type":"function","function":{"name":"read_file","arguments":"{}"}}]`),
			wantStatus: http.StatusOK,
		},
		{
			name:       "prose content",
			req:        toolsNoStream,
			forward:    prose,
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty content",
			req:        toolsNoStream,
			forward:    completionReply(nil, ""),
			wantStatus: http.StatusOK,
		},
		{
			name:       "unknown tool name",
			req:        toolsNoStream,
			forward:    completionReply(strPtr(fenced(`{"name":"rm_rf","arguments":{}}`)), ""),
			wantStatus: http.StatusOK,
		},
		{
			name:       "arguments are a string",
			req:        toolsNoStream,
			forward:    completionReply(strPtr(`{"name":"read_file","arguments":"a.txt"}`), ""),
			wantStatus: http.StatusOK,
		},
		{
			name:       "prose plus a fenced block",
			req:        toolsNoStream,
			forward:    completionReply(strPtr("Let me check.\n"+fenced(raw)), ""),
			wantStatus: http.StatusOK,
		},
		{
			name:         "parser model fails",
			req:          toolsNoStream,
			forward:      completionReply(strPtr(fenced(raw)), ""),
			parserStatus: http.StatusInternalServerError,
			wantStatus:   http.StatusOK,
			wantParser:   true,
		},
		{
			name:          "upstream error status",
			req:           toolsNoStream,
			forward:       []byte(`{"error":{"message":"rate limited"}}`),
			forwardStatus: http.StatusTooManyRequests,
			wantStatus:    http.StatusTooManyRequests,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.forwardStatus == 0 {
				c.forwardStatus = http.StatusOK
			}
			rec := &recorder{}
			up := stubUpstream(rec, c.forward, c.forwardStatus, `{"path":"a.txt"}`)
			// A non-200 parser leg needs its own stub: the shared one always
			// answers the parser leg 200.
			if c.parserStatus != http.StatusOK {
				up.Close()
				up = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, _ := io.ReadAll(r.Body)
					rec.add(r, body)
					var top map[string]json.RawMessage
					if json.Unmarshal(body, &top) == nil {
						if _, ok := top["format"]; ok {
							w.WriteHeader(c.parserStatus)
							return
						}
					}
					w.WriteHeader(c.forwardStatus)
					w.Write(c.forward)
				}))
			}
			defer up.Close()
			relay := startStubRelay(t, up, "parser-model")

			status, body := postCompletions(t, relay.URL(), c.req(t))
			if status != c.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", status, c.wantStatus, body)
			}
			if string(body) != string(c.forward) {
				t.Errorf("body not replayed verbatim:\ngot  %q\nwant %q", body, c.forward)
			}
			wantReq := 1
			if c.wantParser {
				wantReq = 2
			}
			if got := len(rec.all()); got != wantReq {
				t.Errorf("upstream got %d requests, want %d (parser dialed: %v)", got, wantReq, c.wantParser)
			}
		})
	}
}

// TestUpstreamUnreachableServes502 pins the dead-upstream face: the relay
// answers 502 with its own body rather than hanging or panicking.
func TestUpstreamUnreachableServes502(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	up.Close() // take the port away: the relay's upstream is dead

	relay, err := Start(up.URL+"/v1", "parser")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer relay.Close()
	status, body := postCompletions(t, relay.URL(), toolsRequestBody(t, false))
	if status != http.StatusBadGateway {
		t.Errorf("status = %d, want 502 (body %s)", status, body)
	}
	if !strings.Contains(string(body), "toolrelay") {
		t.Errorf("body = %q, want the relay's own error body", body)
	}
}

// TestExtractAnswerShapes pins the buffered-answer decoder directly: both
// wire shapes are recognized, SSE fragments reassemble in order, native
// tool_calls are flagged in either shape, and bytes that are neither shape
// (an error body, a choices-less completion) report not-ok so the response
// passes through unexamined.
func TestExtractAnswerShapes(t *testing.T) {
	for _, c := range []struct {
		name    string
		body    string
		content *string
		native  bool
		ok      bool
	}{
		{"plain completion", string(completionReply(strPtr("hello"), "")), strPtr("hello"), false, true},
		{"native tool_calls message", string(completionReply(nil, `[{"type":"function","function":{"name":"f","arguments":"{}"}}]`)), nil, true, true},
		{"explicit null tool_calls", string(completionReply(strPtr("hi"), "null")), strPtr("hi"), false, true},
		{
			"SSE fragments reassemble in order",
			string(sseReply("He", "llo ", "world")),
			strPtr("Hello world"), false, true,
		},
		{
			"SSE native tool_calls delta",
			"data: " + `{"choices":[{"delta":{"tool_calls":[{"type":"function","function":{"name":"f","arguments":"{}"}}]}}]}` + "\n\ndata: [DONE]\n\n",
			strPtr(""), true, true,
		},
		{"error body is not a chat shape", "upstream exploded", nil, false, false},
		{"choices-less completion is not a chat shape", `{"object":"chat.completion","choices":[]}`, nil, false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			content, native, ok := extractAnswer([]byte(c.body))
			if ok != c.ok {
				t.Fatalf("ok = %v, want %v", ok, c.ok)
			}
			if !ok {
				return
			}
			if native != c.native {
				t.Errorf("native = %v, want %v", native, c.native)
			}
			switch {
			case c.content == nil && content != nil:
				t.Errorf("content = %q, want nil", *content)
			case c.content != nil && (content == nil || *content != *c.content):
				t.Errorf("content = %v, want %q", content, *c.content)
			}
		})
	}
}

// TestStripFence pins the fence strip: one optional code fence with its
// info string comes off, content that does not open with a fence is only
// trimmed, and shapes the strip cannot make single-object (prose plus a
// block, two blocks) stay unparseable as one object — the downstream
// unmarshal fails them and the answer passes through.
func TestStripFence(t *testing.T) {
	for _, c := range []struct {
		name       string
		in         string
		want       string
		notOneJSON bool
	}{
		{"fenced with info string", fenced(`{"a":1}`), `{"a":1}`, false},
		{"bare fence", "```\n{\"a\":1}\n```", `{"a":1}`, false},
		{"no fence, trimmed", "  plain  ", "plain", false},
		{"two blocks stay unparseable", fenced(`{"a":1}`) + "\n" + fenced(`{"b":2}`), "", true},
		{"fence without a newline", "```json without closing newline", "json without closing newline", false},
	} {
		got := stripFence(c.in)
		if c.notOneJSON {
			var obj map[string]any
			if err := json.Unmarshal([]byte(got), &obj); err == nil {
				t.Errorf("stripFence(%q) = %q, want it unparseable as a single object", c.in, got)
			}
			continue
		}
		if got != c.want {
			t.Errorf("stripFence(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
