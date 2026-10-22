// Package metricsrelay implements the metrics daemon's loopback reverse
// proxy and its per-request usage ledger. With metrics enabled, the worker
// stages the relay's URL into every jailed round's provider environment
// (config.MetricsRelayAnthropicEnv / MetricsRelayOpenAIEnv), so a round
// that would dial its provider dials the relay instead: the staged path is
// /relay/<worker-name>/<wire><upstream-path>, the relay strips the prefix,
// resolves the upstream from its registration map (worker-name → upstream
// per wire, registered by the worker at boot and TTL-refreshed by its
// heartbeat), and forwards. POSTs ending in /chat/completions (openai
// wire) or /v1/messages (anthropic wire) are streamed through a tee that
// buffers the body while it forwards, so the client keeps its SSE stream
// and the ledger reads the usage block at EOF — structurally uniform
// across both wires, whatever CLI produced the round. A keyed round's
// inspected call additionally tees its request body and the response's
// assistant text into the in-memory per-round capture (capture.go) — the
// primary source `daedalus log <wf> -cot` renders. Every other request
// proxies verbatim. The relay reads no request or response content beyond
// that capture, the usage blocks, and the attribution encoding the worker
// attached (the
// synthetic per-round key in the credential slot —
// task-<id>-role-<role>-round-<n>, staged with the registration's secret
// token as its -rk- tail; the token gates key restoration, anything else
// attributes at worker level). ponytail: the ledger is in-memory only — a daemon restart drops
// its rows, and a 10k-entry ring bounds memory; the durable roll-ups are
// the golens-metrics embed's sqlite store (cmd/daedalus/golens.go), fed
// per append, not this package's concern. Cost stays render-time: the
// operator's price
// table (the dashboard's models tab) prices entries when the dashboard
// reads them, and CostUSD is never written.
package metricsrelay

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Wire names — the path segment the staged URL carries and the registry's
// per-wire upstream key.
const (
	WireAnthropic = "anthropic"
	WireOpenAI    = "openai"
)

// prefix is the staged path root: /relay/<worker>/<wire>/…
const prefix = "/relay/"

// StagedBase joins a relay root, the staged prefix (/relay/<worker>/<wire>)
// and a provider base URL's path prefix — the base URL a caller stages so
// its requests arrive carrying the prefix (the CLI-appended suffix
// included) and the relay can reproduce the path verbatim upstream. "" when
// either URL does not parse.
func StagedBase(root, worker, wire, base string) string {
	r, err := url.Parse(strings.TrimSuffix(root, "/"))
	if err != nil {
		return ""
	}
	b, err := url.Parse(base)
	if err != nil {
		return ""
	}
	staged := *r
	staged.Path = prefix + worker + "/" + wire + strings.TrimSuffix(b.Path, "/")
	return staged.String()
}

// ledgerCap bounds the in-memory ledger; appending past it drops the
// oldest entries.
const ledgerCap = 10_000

// Entry is one relayed provider request's record. Task/Role/Round are
// parsed from the synthetic attribution key when the round carried one;
// empty means an unkeyed round attributed at worker level. Alias is the
// worker's registered display label for the key the wire dialed
// (key_alias on the provider section) — the per-key dimension metrics
// group by; empty for an unaliased wire.
type Entry struct {
	At               time.Time `json:"at"`
	Worker           string    `json:"worker"`
	Queue            string    `json:"queue,omitempty"`
	Alias            string    `json:"alias,omitempty"`
	Wire             string    `json:"wire"`
	Task             string    `json:"task,omitempty"`
	Role             string    `json:"role,omitempty"`
	Round            string    `json:"round,omitempty"`
	Model            string    `json:"model,omitempty"`
	InputTokens      int64     `json:"input_tokens"`
	OutputTokens     int64     `json:"output_tokens"`
	CacheReadTokens  int64     `json:"cache_read_tokens"`
	CacheWriteTokens int64     `json:"cache_write_tokens"`
	// Reserved and never written: cost is computed at render time from
	// the operator's price table (see the package comment).
	CostUSD    float64 `json:"cost_usd"`
	DurationMS int64   `json:"duration_ms"`
	Status     int     `json:"status"`
	// Source stamps the capture path: "" is the relay's own observation
	// (the ordinary shape), SourceOtel/SourceOtelExternal mark usage
	// folded from an OTLP export by the receiver (otel.go) — those rows
	// carry no per-request status or duration.
	Source string `json:"source,omitempty"`
}

// Ledger is the relay's in-memory usage record. Safe for concurrent
// append and query; the daemon process is its only writer and reader.
type Ledger struct {
	mu       sync.Mutex
	entries  []Entry
	onAppend func(Entry)
}

// SetOnAppend installs fn as the ledger's append observer — the daemon
// feeds its golens-metrics registry this way. fn runs after the entry is
// stored, once per Append, on the relay's serving goroutine; the invoke
// happens outside the ledger lock, so fn may call back into the ledger
// (Query, Len) without deadlocking, though two concurrent appends may
// deliver out of order. Unset is the ordinary shape — the CLI reads the
// ledger by polling, no observer.
func (l *Ledger) SetOnAppend(fn func(Entry)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onAppend = fn
}

// Append records one entry, dropping the oldest past the cap.
func (l *Ledger) Append(e Entry) {
	l.mu.Lock()
	l.entries = append(l.entries, e)
	if len(l.entries) > ledgerCap {
		l.entries = l.entries[len(l.entries)-ledgerCap:]
	}
	fn := l.onAppend
	l.mu.Unlock()
	// Observed outside the lock, so an observer that reads the ledger
	// cannot deadlock against the append it belongs to; two concurrent
	// appends may deliver to the observer out of order, which no fold of
	// the samples cares about.
	if fn != nil {
		fn(e)
	}
}

// Len reports the ledger's current size.
func (l *Ledger) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// Query returns the newest entries first, one page's worth: page counts
// from 1, pageSize bounds it (below 1 means the 50 default, above the
// ledger cap clamps to the cap — the ledger cannot serve more; the
// effective value is returned, so callers report what was served, not
// what was asked), worker filters to one worker's entries when non-empty.
// total is the filtered count across all pages; a page past the last one
// — an overflowed page number included, whose offset would leave int's
// range in the multiply below — is empty.
func (l *Ledger) Query(worker string, page, pageSize int) (entries []Entry, total, size int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Newest first: walk the append-ordered slice backwards.
	var filtered []Entry
	for i := len(l.entries) - 1; i >= 0; i-- {
		e := l.entries[i]
		if worker == "" || e.Worker == worker {
			filtered = append(filtered, e)
		}
	}
	total = len(filtered)
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 50
	}
	// Clamped before the ceil below, whose total + pageSize - 1 would
	// wrap for a page_size near MaxInt.
	if pageSize > ledgerCap {
		pageSize = ledgerCap
	}
	size = pageSize
	if last := (total + pageSize - 1) / pageSize; page > last {
		return nil, total, size
	}
	start := (page - 1) * pageSize
	end := min(start+pageSize, total)
	return filtered[start:end], total, size
}

// registration is one worker's upstream mapping, refreshed by its
// heartbeat. Expired entries serve nothing: a worker that stopped
// refreshing is gone, and its staged URLs describe upstreams only it
// should reach. Keys are the real API keys per wire — what a keyed
// round's synthetic attribution key displaced from the credential slot;
// a wire registered without one forwards the inbound credential
// untouched (OAuth pass-through). Aliases are the per-wire display labels
// (the config's key_alias) stamped onto ledger entries beside Queue —
// metadata about the credential, not a credential. Token is the worker's
// credential-restoration secret, minted worker-side (NewToken) and
// presented at every registration and by every keyed round.
type registration struct {
	Queue     string
	Upstreams map[string]*url.URL
	Keys      map[string]string
	Aliases   map[string]string
	Token     string
	Expires   time.Time
}

// Registry holds the live registrations. TTL-refreshed: the worker
// re-registers well inside the TTL, so an expired entry means a dead
// worker.
type Registry struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]registration
	now func() time.Time
}

// NewRegistry returns a registry whose entries live ttl past their last
// registration.
func NewRegistry(ttl time.Duration) *Registry {
	return &Registry{ttl: ttl, m: map[string]registration{}, now: time.Now}
}

// Register (re-)arms one worker's mapping. Token is the worker's own
// restoration secret — the worker mints it (NewToken), stages it into its
// rounds' synthetic attribution keys, and presents it here; the relay
// stores it and never returns it, so no code path hands the secret to a
// caller that did not present it. A live registration is replaced only by
// a caller presenting its current secret (the heartbeat does); a name
// collision without it is refused, so a loopback process can neither
// harvest a worker's token by re-registering its name nor quietly
// redirect its upstreams, and a name squatted before the worker boots
// fails the worker's registration loudly instead of adopting the
// squatter's secret. Expired entries are pruned in the same pass, so a
// worker that stopped heartbeating is dropped by the next registration
// from anywhere and the map cannot grow without bound.
func (r *Registry) Register(worker, queue string, upstreams map[string]*url.URL, keys, aliases map[string]string, token string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	for name, reg := range r.m {
		if now.After(reg.Expires) {
			delete(r.m, name)
		}
	}
	if reg, live := r.m[worker]; live && !tokenPresented(token, reg.Token) {
		return fmt.Errorf("worker %q is already registered", worker)
	}
	r.m[worker] = registration{Queue: queue, Upstreams: upstreams, Keys: keys, Aliases: aliases, Token: token, Expires: now.Add(r.ttl)}
	return nil
}

// NewToken mints a worker's registration secret: 128 bits of crypto/rand,
// hex. "" on rand failure — the minting caller refuses a tokenless
// registration rather than register one nothing could ever match.
func NewToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		log.Printf("metricsrelay: mint registration token: %v", err)
		return ""
	}
	return hex.EncodeToString(b)
}

// tokenPresented reports whether presented is a non-empty match for the
// registered secret. Constant-time: both sides of this comparison arrive
// from untrusted on-host processes.
func tokenPresented(presented, registered string) bool {
	return presented != "" && subtle.ConstantTimeCompare([]byte(presented), []byte(registered)) == 1
}

// Resolve is the forward path's one atomic read: the worker's live
// registration resolves to its upstream, queue, registered alias, key, and
// restoration token in a single lock, so a concurrent re-registration can
// never pair one entry's upstream with another entry's credential. ok is
// false when the worker has no live registration (never registered, or
// its heartbeat lapsed past the TTL) or no upstream for that wire; key is
// "" for a wire registered keyless (OAuth pass-through — the inbound
// credential, the round's own, forwards untouched); alias is "" for a wire
// registered without a label.
func (r *Registry) Resolve(worker, wire string) (upstream *url.URL, queue, alias, key, token string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reg, known := r.m[worker]
	if !known || r.now().After(reg.Expires) {
		return nil, "", "", "", "", false
	}
	u, known := reg.Upstreams[wire]
	return u, reg.Queue, reg.Aliases[wire], reg.Keys[wire], reg.Token, known
}

// Live reports how many workers currently hold an unexpired registration.
func (r *Registry) Live() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	now := r.now()
	for _, reg := range r.m {
		if !now.After(reg.Expires) {
			n++
		}
	}
	return n
}

// Relay is the loopback proxy: the metrics daemon owns its lifecycle.
type Relay struct {
	ledger   *Ledger
	registry *Registry
	captures Captures
	started  time.Time
	srv      *http.Server
	listener net.Listener
}

// Start binds the relay on 127.0.0.1:port and serves it until Close. The
// bind keeps the relay unreachable off-host; on-host it is not an
// authentication — credential restoration additionally requires the
// registration's token (see Register), so a loopback peer cannot spend a
// registered worker's key with a fabricated attribution key.
func Start(port int, ledger *Ledger, registry *Registry) (*Relay, error) {
	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("listen on 127.0.0.1:%d: %w", port, err)
	}
	r := &Relay{
		ledger:   ledger,
		registry: registry,
		started:  time.Now(),
		listener: ln,
	}
	// Close goes through the server so Serve exits on http.ErrServerClosed
	// (the designed shutdown) rather than a raw accept error.
	r.srv = &http.Server{Handler: r, ReadHeaderTimeout: 30 * time.Second}
	go func() {
		if err := r.srv.Serve(ln); err != http.ErrServerClosed {
			log.Printf("metricsrelay: serve: %v", err)
		}
	}()
	return r, nil
}

// Close stops the relay; in-flight requests are not drained — the metrics
// daemon owns the lifecycle and exits whole.
func (r *Relay) Close() error { return r.srv.Close() }

func (r *Relay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == "/register" && req.Method == http.MethodPost:
		r.serveRegister(w, req)
	case req.URL.Path == "/status" && req.Method == http.MethodGet:
		r.serveStatus(w)
	case req.URL.Path == "/ledger" && req.Method == http.MethodGet:
		r.serveLedger(w, req)
	case req.URL.Path == "/v1/metrics" && req.Method == http.MethodPost:
		r.serveOTLP(w, req)
	// The exporters pointed at this listener deliver traces and logs to
	// stubs: acknowledged and discarded (see otel.go's package comment).
	case (req.URL.Path == "/v1/traces" || req.URL.Path == "/v1/logs") && req.Method == http.MethodPost:
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
	case req.URL.Path == "/capture" && req.Method == http.MethodGet:
		r.serveCapture(w, req)
	case strings.HasPrefix(req.URL.Path, prefix):
		r.serveProxy(w, req)
	default:
		http.Error(w, "metricsrelay: unknown endpoint", http.StatusNotFound)
	}
}

// registerRequest is one worker's boot/heartbeat body: the worker's name
// (the staged path prefix's worker segment) and its serving upstreams —
// one per wire it actually stages, each the provider URL its rounds would
// dial without the relay.
type registerRequest struct {
	Worker    string            `json:"worker"`
	Queue     string            `json:"queue,omitempty"`
	Upstreams map[string]string `json:"upstreams"`
	// Keys are the real API keys per wire, restoring the credential slot a
	// keyed round's synthetic attribution key displaced. A wire registered
	// without a key forwards the inbound credential untouched (OAuth
	// pass-through).
	Keys map[string]string `json:"keys,omitempty"`
	// Aliases are the per-wire display labels (the config's key_alias)
	// stamped onto ledger entries beside Queue — metadata about the
	// credential, not a credential, carried here because the registration
	// is the one worker→relay channel.
	Aliases map[string]string `json:"aliases,omitempty"`
	// Token is the worker's own restoration secret (worker-minted — see
	// Register). Stored, compared, never returned: the response carries no
	// body.
	Token string `json:"token"`
}

func (r *Relay) serveRegister(w http.ResponseWriter, req *http.Request) {
	var rr registerRequest
	if err := json.NewDecoder(req.Body).Decode(&rr); err != nil {
		http.Error(w, "metricsrelay: bad registration body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if rr.Worker == "" || len(rr.Upstreams) == 0 {
		http.Error(w, "metricsrelay: registration needs worker and at least one upstream", http.StatusBadRequest)
		return
	}
	if rr.Token == "" {
		http.Error(w, "metricsrelay: registration needs a restoration token", http.StatusBadRequest)
		return
	}
	upstreams := map[string]*url.URL{}
	for wire, raw := range rr.Upstreams {
		if wire != WireAnthropic && wire != WireOpenAI {
			http.Error(w, "metricsrelay: unknown wire "+wire, http.StatusBadRequest)
			return
		}
		u, err := url.Parse(raw)
		if err != nil || u.Scheme == "" || u.Host == "" {
			http.Error(w, fmt.Sprintf("metricsrelay: upstream %s %q has no scheme/host", wire, raw), http.StatusBadRequest)
			return
		}
		upstreams[wire] = u
	}
	keys := map[string]string{}
	for wire, key := range rr.Keys {
		// Keys for unstaged wires can never be looked up (forwarding only
		// consults staged wires) — dropped, not an error.
		if _, staged := upstreams[wire]; staged {
			keys[wire] = key
		}
	}
	aliases := map[string]string{}
	for wire, alias := range rr.Aliases {
		// Same drop rule as keys: unstaged wires are never looked up.
		if _, staged := upstreams[wire]; staged {
			aliases[wire] = alias
		}
	}
	if err := r.registry.Register(rr.Worker, rr.Queue, upstreams, keys, aliases, rr.Token); err != nil {
		http.Error(w, "metricsrelay: "+err.Error(), http.StatusConflict)
		return
	}
	// No body: the secret came from the caller and goes nowhere.
	w.WriteHeader(http.StatusNoContent)
}

func (r *Relay) serveStatus(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":             "ok",
		"uptime_seconds":     int64(time.Since(r.started) / time.Second),
		"entries":            r.ledger.Len(),
		"live_registrations": r.registry.Live(),
	})
}

func (r *Relay) serveLedger(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	pageSize, _ := strconv.Atoi(q.Get("page_size"))
	entries, total, size := r.ledger.Query(q.Get("worker"), page, pageSize)
	if entries == nil {
		entries = []Entry{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"total":     total,
		"page":      max(page, 1),
		"page_size": size,
		"entries":   entries,
	})
}

// serveProxy is the whole forward path: strip /relay/<worker>/<wire>,
// resolve the upstream, forward, and — for the two inspected POST shapes —
// tee the response body so the usage block lands in the ledger while the
// client's bytes stream through unharmed.
func (r *Relay) serveProxy(w http.ResponseWriter, req *http.Request) {
	rest, ok := strings.CutPrefix(req.URL.Path, prefix)
	if !ok {
		http.Error(w, "metricsrelay: bad relay path", http.StatusNotFound)
		return
	}
	worker, remainder, found := strings.Cut(rest, "/")
	if !found || worker == "" || remainder == "" {
		http.Error(w, "metricsrelay: relay path needs /relay/<worker>/<wire>/…", http.StatusNotFound)
		return
	}
	wire, upstreamPath, found := strings.Cut(remainder, "/")
	// The staged base URL always carries at least the upstream's own path
	// prefix, so the CLI's appended suffix follows a real slash; when the
	// upstream has no path prefix, upstreamPath is what the CLI appended
	// after the slash this Cut consumed (possibly empty for a bare-base
	// request).
	if !found {
		http.Error(w, "metricsrelay: relay path needs /relay/<worker>/<wire>/…", http.StatusNotFound)
		return
	}
	if wire != WireAnthropic && wire != WireOpenAI {
		http.Error(w, "metricsrelay: unknown wire "+wire, http.StatusNotFound)
		return
	}
	upstream, queue, alias, cred, regToken, ok := r.registry.Resolve(worker, wire)
	if !ok {
		http.Error(w, "metricsrelay: no live registration for worker "+worker+" on wire "+wire, http.StatusBadGateway)
		return
	}
	// The remainder — the upstream's own path prefix plus whatever the CLI
	// appended — is forwarded verbatim: the staged URL reproduced the full
	// path the request would have had without the relay. The registered
	// upstream contributes scheme and host only.
	forward := upstream.Scheme + "://" + upstream.Host + "/" + upstreamPath
	if req.URL.RawQuery != "" {
		forward += "?" + req.URL.RawQuery
	}
	// The attribution encoding rides the credential slot; parsed once here
	// — it decides the credential handling, the ledger entry, and whether
	// the request side joins the content capture. Parsed before the
	// upstream request is built, so the tee below can consult it.
	task, role, round, token, keyed := parseAttribution(attributionKey(req, wire))
	// Inspect exactly the two provider-call shapes, whichever upstream
	// path prefix the staged URL carried.
	inspect := req.Method == http.MethodPost &&
		(strings.HasSuffix("/"+upstreamPath, "/chat/completions") ||
			strings.HasSuffix("/"+upstreamPath, "/v1/messages"))
	// A keyed, inspected round's request body tees into the capture on its
	// way upstream — the prompt side -cot renders. The tee never errors
	// the forward: cappedBuffer discards past its bound.
	var reqCap *cappedBuffer
	var body io.Reader = req.Body
	if inspect && keyed {
		reqCap = newCappedBuffer(contentCap)
		body = io.TeeReader(req.Body, reqCap)
	}
	up, err := http.NewRequestWithContext(req.Context(), req.Method, forward, body)
	if err != nil {
		http.Error(w, "metricsrelay: build upstream request: "+err.Error(), http.StatusBadGateway)
		return
	}
	// http.NewRequestWithContext leaves ContentLength unset for a body it
	// did not size itself; without it the forward goes out chunked and a
	// strict upstream may refuse the length-less request.
	up.ContentLength = req.ContentLength
	up.Header = cloneHeaders(req.Header)
	// A transport asked to negotiate gzip transparently unwraps the body —
	// pin identity so the bytes tee'd into the ledger are the client's
	// bytes (same invariant as the tool relay's roundTrip).
	up.Header.Set("Accept-Encoding", "identity")
	// Restore-only and token-gated: the registration's real key goes back
	// into the credential slot exactly when the request presents the
	// registration's secret alongside the synthetic attribution key. The
	// shape alone is forgeable by any on-host process — jailed rounds are
	// exactly that, with loopback access by design — and an ungated
	// restore would let any of them spend any registered worker's key. A
	// credential-less request forwards credential-less: nothing is minted
	// for anyone. A wire registered keyless (OAuth pass-through) forwards
	// the inbound credential untouched once the token checks.
	if keyed {
		if !tokenPresented(token, regToken) {
			http.Error(w, "metricsrelay: keyed round presents no valid registration token", http.StatusUnauthorized)
			return
		}
		if cred != "" {
			setCredential(up.Header, wire, cred)
		}
	}

	start := time.Now()
	resp, err := http.DefaultTransport.RoundTrip(up)
	if err != nil {
		log.Printf("metricsrelay: upstream unreachable worker=%s wire=%s: %v", worker, wire, err)
		http.Error(w, "metricsrelay: upstream unreachable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	hdr := w.Header()
	for k, vv := range resp.Header {
		if inspect && (k == "Content-Length" || k == "Connection") {
			// The tee re-chunks (it cannot know the length up front); a
			// trusted-then-violated Content-Length would truncate the
			// client's body.
			continue
		}
		if !inspect && (k == "Connection" || k == "Keep-Alive") {
			continue
		}
		for _, v := range vv {
			hdr.Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	var buf bytes.Buffer
	if inspect {
		stream(w, io.TeeReader(resp.Body, &buf))
	} else {
		stream(w, resp.Body)
	}
	// Only the inspected provider-call shapes are recorded — the ledger is
	// the usage ledger behind `metrics list`'s pages, and every model-list
	// GET or health probe staged through the relay would land as a bare
	// zero-usage row diluting it. Inspected calls are recorded with
	// failures included: Status carries the outcome, and a flood of
	// failures during a provider flap is exactly what this ledger exists
	// to show. Everything else proxies verbatim, unrecorded.
	if inspect {
		e := Entry{
			At:         start,
			Worker:     worker,
			Queue:      queue,
			Alias:      alias,
			Wire:       wire,
			DurationMS: time.Since(start).Milliseconds(),
			Status:     resp.StatusCode,
		}
		// Keyed rounds carry the worker's chosen attribution (parsed above);
		// unkeyed ones left the round's own credentials in place — never
		// second-guessed.
		if keyed {
			e.Task, e.Role, e.Round = task, role, round
		}
		applyUsage(&e, buf.Bytes())
		r.ledger.Append(e)
		log.Printf("metricsrelay: recorded worker=%s wire=%s model=%s in=%d out=%d status=%d",
			e.Worker, e.Wire, e.Model, e.InputTokens, e.OutputTokens, e.Status)
		// Keyed rounds capture content beside usage: the round's last
		// provider call carries the whole conversation its -cot renders.
		if keyed {
			r.captures.Put(task, role, round, CapturedRound{
				At:       start,
				Prompt:   reqCap.String(),
				Response: assistantText(buf.Bytes()),
			})
		}
	}
}

// stream copies body to w, flushing after every chunk so SSE arrives as
// it is produced.
func stream(w http.ResponseWriter, body io.Reader) {
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			return
		}
	}
}

// hopHeaders are stripped from the forwarded request (RFC 7230 §6.1):
// they shape one connection, not the message.
var hopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "TE", "Trailer",
	"Transfer-Encoding", "Upgrade",
}

func cloneHeaders(h http.Header) http.Header {
	out := h.Clone()
	for _, k := range hopHeaders {
		out.Del(k)
	}
	return out
}

// attributionKey reads the credential slot the wire puts its API key in:
// x-api-key on the anthropic wire, the Bearer token on the openai wire.
// An OAuth bearer (unkeyed round) arrives here too and simply never parses
// as the synthetic shape.
func attributionKey(req *http.Request, wire string) string {
	if wire == WireAnthropic {
		return req.Header.Get("X-Api-Key")
	}
	auth := req.Header.Get("Authorization")
	if bearer, ok := strings.CutPrefix(auth, "Bearer "); ok {
		return strings.TrimSpace(bearer)
	}
	return auth
}

// setCredential writes key into the credential slot the wire reads — the
// restore half of attributionKey.
func setCredential(h http.Header, wire, key string) {
	if wire == WireAnthropic {
		h.Set("X-Api-Key", key)
		return
	}
	h.Set("Authorization", "Bearer "+key)
}

// parseAttribution splits the worker's synthetic per-round key
// (task-<id>-role-<role>-round-<n>, optionally -rk-<token> staged by the
// worker) into its parts; keyed is false for anything that is not that
// shape — an OAuth bearer, a plain API key, garbage. The task id's
// sanitization (worker-side) excludes "-" for exactly this parse: the
// first "-role-" always starts the role part. The token is the split-off
// tail, restore-gating in serveProxy; the round part stays clean of it
// for the ledger. Last split: the token (hex) never carries the marker,
// so an activity id containing "-rk-" still parses.
func parseAttribution(key string) (task, role, round, token string, keyed bool) {
	rest, ok := strings.CutPrefix(key, "task-")
	if !ok {
		return "", "", "", "", false
	}
	taskPart, rest2, ok := strings.Cut(rest, "-role-")
	if !ok {
		return "", "", "", "", false
	}
	rolePart, roundPart, ok := strings.Cut(rest2, "-round-")
	if !ok || taskPart == "" || rolePart == "" || roundPart == "" {
		return "", "", "", "", false
	}
	if i := strings.LastIndex(roundPart, "-rk-"); i >= 0 {
		token, roundPart = roundPart[i+len("-rk-"):], roundPart[:i]
	}
	return taskPart, rolePart, roundPart, token, true
}

// wireUsage is either wire's usage block: the anthropic field names are
// primary, the openai prompt/completion names map onto the same slots.
// Pointers, not ints, so an absent field is distinguishable from a zero
// one — a message_delta carrying only output_tokens must not zero the
// input side message_start already set.
type wireUsage struct {
	InputTokens              *int64 `json:"input_tokens"`
	OutputTokens             *int64 `json:"output_tokens"`
	CacheReadInputTokens     *int64 `json:"cache_read_input_tokens"`
	CacheCreationInputTokens *int64 `json:"cache_creation_input_tokens"`
	PromptTokens             *int64 `json:"prompt_tokens"`
	CompletionTokens         *int64 `json:"completion_tokens"`
	PromptTokensDetails      *struct {
		CachedTokens *int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
}

// present reports whether the block carried any token count at all — a
// plain-JSON body without a usage block (a model list, an error object)
// must not be mistaken for one.
func (u *wireUsage) present() bool {
	if u.PromptTokensDetails != nil && u.PromptTokensDetails.CachedTokens != nil {
		return true
	}
	return u.InputTokens != nil || u.OutputTokens != nil ||
		u.CacheReadInputTokens != nil || u.CacheCreationInputTokens != nil ||
		u.PromptTokens != nil || u.CompletionTokens != nil
}

// applyUsage extracts the usage block from a buffered response body —
// either wire's plain-JSON shape or its SSE chunk stream — into e. A body
// that is neither shape (an error page) leaves e untouched.
func applyUsage(e *Entry, body []byte) {
	// Plain JSON first: a non-streaming response on either wire.
	var plain struct {
		Model string    `json:"model"`
		Usage wireUsage `json:"usage"`
	}
	if json.Unmarshal(body, &plain) == nil && plain.Usage.present() {
		e.Model = plain.Model
		setTokens(e, &plain.Usage)
		return
	}
	// SSE: walk the stream's data payloads. Anthropic reports the input
	// side in message_start's message.usage and the output side in each
	// message_delta's (cumulative) usage; openai reports a whole usage
	// block on the (single) chunk that carries one, and every chunk names
	// its model.
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
			Model   string `json:"model"`
			Message *struct {
				Model string    `json:"model"`
				Usage wireUsage `json:"usage"`
			} `json:"message"`
			Usage *wireUsage `json:"usage"`
		}
		if json.Unmarshal([]byte(payload), &chunk) != nil {
			continue
		}
		if chunk.Message != nil {
			if chunk.Message.Model != "" {
				e.Model = chunk.Message.Model
			}
			setTokens(e, &chunk.Message.Usage)
		}
		if chunk.Model != "" {
			e.Model = chunk.Model
		}
		if chunk.Usage != nil {
			setTokens(e, chunk.Usage)
		}
	}
}

// setTokens maps a usage block onto the entry, openai's prompt/completion
// naming included; the pointer-nil discipline in wireUsage keeps an absent
// field from zeroing one an earlier chunk set.
func setTokens(e *Entry, u *wireUsage) {
	if u.InputTokens != nil {
		e.InputTokens = *u.InputTokens
	}
	if u.PromptTokens != nil {
		e.InputTokens = *u.PromptTokens
	}
	if u.OutputTokens != nil {
		e.OutputTokens = *u.OutputTokens
	}
	if u.CompletionTokens != nil {
		e.OutputTokens = *u.CompletionTokens
	}
	if u.CacheReadInputTokens != nil {
		e.CacheReadTokens = *u.CacheReadInputTokens
	}
	if u.CacheCreationInputTokens != nil {
		e.CacheWriteTokens = *u.CacheCreationInputTokens
	}
	if u.PromptTokensDetails != nil && u.PromptTokensDetails.CachedTokens != nil {
		e.CacheReadTokens = *u.PromptTokensDetails.CachedTokens
	}
}
