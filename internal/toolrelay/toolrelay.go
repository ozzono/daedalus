// Package toolrelay implements the worker-owned loopback reverse proxy
// that lifts text-encoded tool calls into the native tool_calls wire pi
// executes. pi 0.87.1 runs tools only from provider-native tool_calls (no
// text fallback), while qwen2.5-coder-class models emit them as fenced
// JSON in the message content with tool_calls null — so served rounds
// complete with zero file changes. The relay sits between the jailed pi
// round and the openai section's upstream: a tool-bearing
// POST .../chat/completions is buffered and inspected; every other
// request is streamed through verbatim. When the upstream's answer is a
// single fenced-or-bare JSON object naming one of the request's own tools
// with an object arguments value, the parser model (slim.parser_model,
// ollama structured output, 60s bound) normalizes the arguments against
// the tool's schema and pi receives a synthetic single-delta tool_calls
// stream. Everything else replays the upstream bytes untouched, and every
// lift failure degrades to that passthrough — fail-open, no new workflow
// failure path. The relay is stateless: no metrics store, no persistence;
// one structured log line per inspected response (tool name only, never
// argument bodies).
package toolrelay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

// parserTimeout bounds one parser-model call; a slower parser fails the
// lift and the response passes through as inert text.
const parserTimeout = 60 * time.Second

// Relay is the loopback proxy for one worker. Start it with Start; it
// serves until Close or process exit.
type Relay struct {
	upstream *url.URL
	// parserModel is slim.parser_model — the model asked to normalize
	// lifted arguments, resolved against the same upstream pi dials.
	parserModel string
	transport   http.RoundTripper
	proxy       *httputil.ReverseProxy
	srv         *http.Server
	listener    net.Listener
}

// Start parses the upstream URL (the openai section's url — scheme and
// host are what the relay forwards to; the path prefix is preserved
// end-to-end, see URL) and serves the relay on a fresh 127.0.0.1 port.
func Start(upstreamURL, parserModel string) (*Relay, error) {
	u, err := url.Parse(upstreamURL)
	if err != nil {
		return nil, fmt.Errorf("parse upstream url: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("upstream url %q has no scheme/host", upstreamURL)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	r := &Relay{
		upstream:    u,
		parserModel: parserModel,
		transport:   http.DefaultTransport,
		listener:    ln,
		proxy: &httputil.ReverseProxy{Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)
			// Keep the request path verbatim: pi's staged base URL
			// already carries the upstream's path prefix (see URL), so
			// SetURL's path join would double it.
			pr.Out.URL.Path = pr.In.URL.Path
			pr.Out.URL.RawPath = pr.In.URL.RawPath
		}},
	}
	// The server lives on the Relay so Close goes through it — closing the
	// listener alone would exit Serve with a raw accept error instead of
	// http.ErrServerClosed, logging a bogus error for the designed
	// shutdown.
	r.srv = &http.Server{Handler: r, ReadHeaderTimeout: 30 * time.Second}
	go func() {
		if err := r.srv.Serve(ln); err != http.ErrServerClosed {
			log.Printf("toolrelay: serve: %v", err)
		}
	}()
	return r, nil
}

// Close stops the relay's server (and with it the listener); in-flight
// requests are not drained — the worker daemon owns the lifecycle and
// exits whole. Closing through the server is what keeps Serve's exit on
// the http.ErrServerClosed path, so the designed shutdown logs nothing.
func (r *Relay) Close() error { return r.srv.Close() }

// URL is the staged base URL pi dials: the relay's own loopback address
// plus the upstream's path prefix. Request paths arriving on it are
// identical to the paths the upstream would have received directly, so
// the relay can forward them verbatim — and the parser call reuses the
// incoming completions path against the upstream host.
func (r *Relay) URL() string {
	u := url.URL{Scheme: "http", Host: r.listener.Addr().String(), Path: r.upstream.Path}
	return u.String()
}

func (r *Relay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Only chat-completions POSTs can be tool-bearing; everything else —
	// other methods, model pulls, /api routes — streams through verbatim.
	if req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/chat/completions") {
		r.serveCompletions(w, req)
		return
	}
	r.proxy.ServeHTTP(w, req)
}

// chatRequest is the slice of the OpenAI chat-completions request the lift
// rule reads; everything else in the body is never touched — passthrough
// replays the original bytes.
type chatRequest struct {
	Model  string     `json:"model"`
	Stream bool       `json:"stream"`
	Tools  []toolSpec `json:"tools"`
}

// toolSpec is one entry of the request's tools list; the lift may only
// produce a tool the request itself offered, and the tool's parameters
// schema feeds the parser's structured output.
type toolSpec struct {
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function"`
}

// chatResponse is the slice of a chat completion the lift rule reads; it
// serves both the upstream's plain-JSON shape and the parser model's
// reply.
type chatResponse struct {
	Choices []struct {
		Message struct {
			Content   *string         `json:"content"`
			ToolCalls json.RawMessage `json:"tool_calls"`
		} `json:"message"`
	} `json:"choices"`
}

// extractAnswer pulls the assistant's answer out of a buffered upstream
// response in either wire shape: a plain JSON chat completion (non-
// streaming requests) or the SSE chunk stream a streamed request gets —
// whose content fragments are concatenated in order, the way pi's
// accumulator would. native reports provider-native tool_calls anywhere
// in the answer (a message field or any chunk delta); ok is false when
// the bytes are neither shape (an error body, /v1/models output) and the
// response must pass through unexamined.
func extractAnswer(body []byte) (content *string, native bool, ok bool) {
	var completion chatResponse
	if err := json.Unmarshal(body, &completion); err == nil && len(completion.Choices) > 0 {
		msg := completion.Choices[0].Message
		native = len(msg.ToolCalls) > 0 && string(msg.ToolCalls) != "null"
		return msg.Content, native, true
	}
	var acc string
	seen := false
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			seen = true
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content   *string         `json:"content"`
					ToolCalls json.RawMessage `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			continue
		}
		seen = true
		for _, ch := range chunk.Choices {
			if ch.Delta.Content != nil {
				acc += *ch.Delta.Content
			}
			if len(ch.Delta.ToolCalls) > 0 && string(ch.Delta.ToolCalls) != "null" {
				native = true
			}
		}
	}
	if seen {
		return &acc, native, true
	}
	return nil, false, false
}

func (r *Relay) serveCompletions(w http.ResponseWriter, req *http.Request) {
	// The request is buffered so it can be replayed on both the inspect
	// and the passthrough leg; a mid-body read failure leaves nothing
	// replayable and the client sees the error directly.
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "toolrelay: read request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := r.roundTrip(req, body)
	if err != nil {
		log.Printf("toolrelay: passthrough reason=%q", fmt.Sprintf("upstream unreachable: %v", err))
		http.Error(w, "toolrelay: upstream unreachable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	// Non-200s carry no tool calls to lift — replay verbatim (status,
	// headers, and the error body pi's own error handling expects).
	if resp.StatusCode != http.StatusOK {
		upBody, _ := io.ReadAll(resp.Body)
		r.replay(w, resp, upBody)
		return
	}
	upBody, err := io.ReadAll(resp.Body)
	if err != nil {
		// Best effort: relay whatever arrived before the failure — the
		// truncated stream is today's behavior degraded, not a new one.
		log.Printf("toolrelay: passthrough reason=%q", fmt.Sprintf("read upstream body: %v", err))
		r.replay(w, resp, upBody)
		return
	}

	var creq chatRequest
	if err := json.Unmarshal(body, &creq); err != nil || len(creq.Tools) == 0 {
		r.passthrough(w, resp, upBody, "request offers no tools", "")
		return
	}
	content, native, ok := extractAnswer(upBody)
	if !ok {
		r.passthrough(w, resp, upBody, "response is not a chat completion", "")
		return
	}
	if native {
		r.passthrough(w, resp, upBody, "native tool_calls", "")
		return
	}
	tools := map[string]toolSpec{}
	for _, t := range creq.Tools {
		tools[t.Function.Name] = t
	}
	tool, args, reason := candidateCall(content, tools)
	if reason != "" {
		r.passthrough(w, resp, upBody, reason, tool.Function.Name)
		return
	}
	// The raw bytes are replayed from here on whatever the parser does —
	// from the first parser byte on, fail-open means passthrough.
	lifted, err := r.parserCall(req, req.URL.RequestURI(), tool, args)
	if err != nil {
		r.passthrough(w, resp, upBody, fmt.Sprintf("parser: %v", err), tool.Function.Name)
		return
	}
	log.Printf("toolrelay: lifted tool=%s model=%s parser=%s", tool.Function.Name, creq.Model, r.parserModel)
	writeSynthetic(w, creq.Model, tool.Function.Name, lifted, creq.Stream)
}

// roundTrip forwards the buffered completions request to the upstream —
// same method, same path (pi's staged base URL carries the upstream's
// path prefix, so the incoming path is the upstream's own), same headers.
// Accept-Encoding is pinned to identity so the inspected response arrives
// uncompressed: the transport does not re-add the header when the request
// carries one, so the invariant holds for any transport — dropping the
// header alone would not, since a transport asked to negotiate gzip then
// transparently unwraps the body itself (the plain bytes arriving here
// today are that unwrap, not the drop).
func (r *Relay) roundTrip(req *http.Request, body []byte) (*http.Response, error) {
	up, err := http.NewRequestWithContext(req.Context(), req.Method,
		r.upstream.Scheme+"://"+r.upstream.Host+req.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	up.Header = req.Header.Clone()
	up.Header.Set("Accept-Encoding", "identity")
	return r.transport.RoundTrip(up)
}

// candidateCall extracts a single text-encoded tool call from assistant
// content: after trimming (and stripping one optional code fence), the
// content must be exactly one JSON object whose name is one of the
// request's own tools and whose arguments is a JSON object. Anything else
// — prose, multi-block content, unknown tool names, non-object arguments
// — is never converted (reviewer verdicts are content) and comes back as
// a non-empty reason; an empty reason means the call may be lifted.
func candidateCall(content *string, tools map[string]toolSpec) (tool toolSpec, args json.RawMessage, reason string) {
	if content == nil {
		return tool, nil, "empty content"
	}
	s := stripFence(*content)
	var call struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal([]byte(s), &call); err != nil {
		return tool, nil, "content is not a single JSON object"
	}
	t, known := tools[call.Name]
	if !known {
		// Name the attempted tool in the log line by returning a spec
		// carrying just its name.
		t.Function.Name = call.Name
		return t, nil, fmt.Sprintf("unknown tool name %q", call.Name)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(call.Arguments, &obj); err != nil {
		return t, nil, "arguments are not a JSON object"
	}
	return t, call.Arguments, ""
}

// stripFence trims content and, when it opens with a code fence, strips
// the fence and its info string — qwen2.5-coder fences its JSON about
// half the time. Shapes the fence strip cannot make single-object (prose
// plus a block, two blocks) fail the downstream unmarshal and pass
// through.
func stripFence(content string) string {
	s := strings.TrimSpace(content)
	if !strings.HasPrefix(s, "```") {
		return s
	}
	s = strings.TrimPrefix(s, "```")
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[i+1:]
	} else {
		return s
	}
	if i := strings.LastIndex(s, "```"); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

// parserCall asks the parser model (served by the same upstream, on the
// same completions path pi used) to turn the lifted call's raw arguments
// into arguments conforming to the tool's parameter schema, via ollama's
// structured-output "format" field. Bounded at parserTimeout; any failure
// fails the lift.
func (r *Relay) parserCall(req *http.Request, completionsPath string, tool toolSpec, args json.RawMessage) (json.RawMessage, error) {
	schema := tool.Function.Parameters
	if len(schema) == 0 || string(schema) == "null" {
		schema = json.RawMessage(`{"type":"object"}`)
	}
	prompt := fmt.Sprintf("Tool: %s\n%s\n\nConvert the raw JSON below into arguments for this tool, conforming to the tool's schema. Reply with only the JSON object of arguments.\n\n%s",
		tool.Function.Name, tool.Function.Description, args)
	reqBody, err := json.Marshal(map[string]any{
		"model":    r.parserModel,
		"messages": []map[string]string{{"role": "user", "content": prompt}},
		"stream":   false,
		"format":   schema,
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(req.Context(), parserTimeout)
	defer cancel()
	up, err := http.NewRequestWithContext(ctx, http.MethodPost,
		r.upstream.Scheme+"://"+r.upstream.Host+completionsPath, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	// The parser dial is authenticated exactly like the forwarded leg:
	// the request's own headers carry the key pi sent (a keyed upstream
	// would 401 the parser call and silently disable every lift), and
	// Accept-Encoding is pinned to identity for the same
	// any-transport-invariant reason as roundTrip.
	up.Header = req.Header.Clone()
	up.Header.Set("Content-Type", "application/json")
	up.Header.Set("Accept-Encoding", "identity")
	resp, err := r.transport.RoundTrip(up)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var presp chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&presp); err != nil {
		return nil, err
	}
	if len(presp.Choices) == 0 || presp.Choices[0].Message.Content == nil {
		return nil, fmt.Errorf("no content returned")
	}
	out := stripFence(*presp.Choices[0].Message.Content)
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(out), &obj); err != nil {
		return nil, fmt.Errorf("output is not a JSON object")
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(out)); err != nil {
		return nil, err
	}
	return json.RawMessage(compact.Bytes()), nil
}

// writeSynthetic answers the client with a minimal tool_calls completion:
// the streaming shape is a single delta carrying the complete arguments
// followed by a closing chunk with finish_reason "tool_calls" (both legal
// per the OpenAI chunk schema; pi's accumulator concatenates argument
// fragments by index, and one whole fragment needs no stitching); the
// non-streaming twin carries the same message in one body.
func writeSynthetic(w http.ResponseWriter, model, name string, args json.RawMessage, stream bool) {
	id := fmt.Sprintf("chatcmpl-toolrelay-%d", time.Now().UnixNano())
	created := time.Now().Unix()
	call := map[string]any{
		"index": 0,
		"id":    strings.Replace(id, "chatcmpl", "call", 1),
		"type":  "function",
		"function": map[string]any{
			"name":      name,
			"arguments": string(args),
		},
	}
	if !stream {
		body, err := json.Marshal(map[string]any{
			"id": id, "object": "chat.completion", "created": created, "model": model,
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role": "assistant", "content": nil, "tool_calls": []any{call},
				},
				"finish_reason": "tool_calls",
			}},
		})
		if err != nil { // unreachable: map of marshalable values
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
		return
	}
	chunkOf := func(delta map[string]any, finish any) []byte {
		b, err := json.Marshal(map[string]any{
			"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
			"choices": []map[string]any{{"index": 0, "delta": delta, "finish_reason": finish}},
		})
		if err != nil { // unreachable: map of marshalable values
			return nil
		}
		return b
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	first := chunkOf(map[string]any{
		"role": "assistant", "tool_calls": []any{call},
	}, nil)
	last := chunkOf(map[string]any{}, "tool_calls")
	for _, b := range [][]byte{first, last} {
		if b == nil {
			continue
		}
		w.Write(append(append([]byte("data: "), b...), '\n', '\n'))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
	w.Write([]byte("data: [DONE]\n\n"))
}

// passthrough replays the upstream bytes untouched — the fail-open
// endpoint of every lift decision — after one structured log line naming
// the reason and, when known, the tool (never argument bodies).
func (r *Relay) passthrough(w http.ResponseWriter, resp *http.Response, body []byte, reason, tool string) {
	if tool != "" {
		log.Printf("toolrelay: passthrough tool=%s reason=%q", tool, reason)
	} else {
		log.Printf("toolrelay: passthrough reason=%q", reason)
	}
	r.replay(w, resp, body)
}

// replay copies an upstream response to the client verbatim: the buffered
// body is the exact upstream bytes, so Content-Length is dropped and the
// transfer re-chunked rather than trusted.
func (r *Relay) replay(w http.ResponseWriter, resp *http.Response, body []byte) {
	hdr := w.Header()
	for k, vv := range resp.Header {
		if k == "Content-Length" {
			continue
		}
		for _, v := range vv {
			hdr.Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}
