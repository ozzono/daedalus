package metricsrelay

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordedRequest is one request a stub upstream received: path, query,
// headers, and body — every upstream-side assertion starts from what
// actually arrived on the wire.
type recordedRequest struct {
	Path     string
	RawQuery string
	Header   http.Header
	Body     []byte
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
	rec.reqs = append(rec.reqs, recordedRequest{Path: r.URL.Path, RawQuery: r.URL.RawQuery, Header: r.Header.Clone(), Body: body})
}

func (rec *recorder) all() []recordedRequest {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]recordedRequest(nil), rec.reqs...)
}

// startRelay starts the real relay on a kernel-chosen loopback port, closed
// at test cleanup, and returns it with its root URL — the address staged
// URLs are built from.
func startRelay(t *testing.T, ledger *Ledger, registry *Registry) (*Relay, string) {
	t.Helper()
	r, err := Start(0, ledger, registry)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	root := "http://" + r.listener.Addr().String()
	return r, root
}

// postRelay POSTs body to url and returns the status with the fully-read
// body — the one leg every proxy test starts from.
func postRelay(t *testing.T, rawURL string, header http.Header, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header = header
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", rawURL, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, b
}

// staged joins a relay root with the staged path for worker on wire over
// the upstream base URL — the URL a round's environment would carry.
func staged(root, worker, wire, base string) string {
	return StagedBase(root, worker, wire, base)
}

// TestStagedBase pins the staged-URL composition: the relay root's scheme
// and host carry the /relay/<worker>/<wire> prefix plus the upstream base's
// own path prefix, so a CLI appending its usual suffix lands on the relay
// exactly where it would have landed on the upstream. Trailing slashes on
// either side are joined away; an unparseable URL stages nothing.
func TestStagedBase(t *testing.T) {
	for _, c := range []struct {
		name         string
		root, base   string
		worker, wire string
		want         string
	}{
		{
			name: "plain join keeps the upstream path prefix",
			root: "http://127.0.0.1:7778", worker: "w", wire: WireOpenAI,
			base: "http://u.example/v1",
			want: "http://127.0.0.1:7778/relay/w/openai/v1",
		},
		{
			name: "anthropic wire and slashless base",
			root: "http://127.0.0.1:7778", worker: "wrkr", wire: WireAnthropic,
			base: "https://api.example",
			want: "http://127.0.0.1:7778/relay/wrkr/anthropic",
		},
		{
			name: "trailing slash on the root is trimmed",
			root: "http://127.0.0.1:7778/", worker: "w", wire: WireOpenAI,
			base: "http://u.example/v1",
			want: "http://127.0.0.1:7778/relay/w/openai/v1",
		},
		{
			name: "trailing slash on the base is trimmed",
			root: "http://127.0.0.1:7778", worker: "w", wire: WireOpenAI,
			base: "http://u.example/v1/",
			want: "http://127.0.0.1:7778/relay/w/openai/v1",
		},
		{
			name: "unparseable base stages nothing",
			root: "http://127.0.0.1:7778", worker: "w", wire: WireOpenAI,
			base: "://bad",
			want: "",
		},
	} {
		if got := StagedBase(c.root, c.worker, c.wire, c.base); got != c.want {
			t.Errorf("%s: StagedBase = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestLedgerMemoryModeKeepsEverything pins the memory ledger's shape after
// the ring's retirement: the zero-value ledger is the no-disk posture and
// unbounded by design — appending past the retired 10k cap keeps every
// entry, still served newest-first, with the oldest record surviving at the
// bottom of the ascending read.
func TestLedgerMemoryModeKeepsEverything(t *testing.T) {
	l := &Ledger{}
	const n = 10_001 // just past the retired ledgerCap
	for i := 0; i < n; i++ {
		l.Append(Entry{Task: "t" + strconv.Itoa(i)})
	}
	if l.Len() != n {
		t.Fatalf("Len = %d, want every one of the %d appends (memory mode is unbounded)", l.Len(), n)
	}
	entries, total, _ := l.Query("", 1, 5)
	if total != n || len(entries) != 5 {
		t.Fatalf("Query = %d entries total %d, want the first page of five over %d", len(entries), total, n)
	}
	if entries[0].Task != "t"+strconv.Itoa(n-1) {
		t.Errorf("newest entry = %q, want t%d", entries[0].Task, n-1)
	}
	asc := l.Ascending()
	if len(asc) != n || asc[0].Task != "t0" || asc[n-1].Task != "t"+strconv.Itoa(n-1) {
		t.Errorf("Ascending = %d entries (%q..%q), want all %d oldest-first", len(asc), asc[0].Task, asc[n-1].Task, n)
	}
}

// TestLedgerMemoryAscendingCopies pins the memory read's shape: Ascending
// hands the entries back in append order as a fresh slice — mutating the
// result must not reach into the ledger.
func TestLedgerMemoryAscendingCopies(t *testing.T) {
	l := &Ledger{}
	l.Append(Entry{Task: "a"})
	l.Append(Entry{Task: "b"})
	got := l.Ascending()
	if len(got) != 2 || got[0].Task != "a" || got[1].Task != "b" {
		t.Fatalf("Ascending = %+v, want append order", got)
	}
	got[0].Task = "mutated"
	if fresh := l.Ascending(); fresh[0].Task != "a" {
		t.Errorf("mutating the returned slice leaked into the ledger: %q", fresh[0].Task)
	}
}

// TestLedgerQuery pins the paging contract: newest first, page counts from
// 1, an unset page size is the 50 default, a worker filter narrows without
// changing the ordering, an overflowed (or MaxInt) page serves nothing but
// still reports the total, and the effective page size comes back so
// callers report what was served.
func TestLedgerQuery(t *testing.T) {
	l := &Ledger{}
	for i := 0; i < 5; i++ {
		l.Append(Entry{Task: "t", Worker: "w1"})
		l.Append(Entry{Task: "t", Worker: "w2"})
	}

	entries, total, size := l.Query("", 1, 3)
	if total != 10 || size != 3 || len(entries) != 3 {
		t.Fatalf("Query(page 1) = %d entries total %d size %d, want 3/10/3", len(entries), total, size)
	}
	if entries[0].Worker != "w2" || entries[1].Worker != "w1" || entries[2].Worker != "w2" {
		t.Errorf("newest-first ordering broken: %v", entries)
	}

	entries, total, _ = l.Query("w1", 0, 0)
	if total != 5 || len(entries) != 5 {
		t.Fatalf("Query(w1, unset page/size) = %d entries total %d, want all five w1 entries", len(entries), total)
	}
	for _, e := range entries {
		if e.Worker != "w1" {
			t.Errorf("filter let a %q entry through", e.Worker)
		}
	}

	entries, total, size = l.Query("", 2, 4)
	if total != 10 || size != 4 || len(entries) != 4 {
		t.Fatalf("Query(page 2) = %d entries total %d size %d, want 4/10/4", len(entries), total, size)
	}

	entries, total, _ = l.Query("", 4, 4)
	if entries != nil || total != 10 {
		t.Errorf("overflowed page = %d entries, want nil with total %d", len(entries), total)
	}

	// A page number whose offset multiply would overflow int must serve
	// empty, not panic or wrap into a live offset — and the oversized page
	// size comes back clamped to the request-side cap, the effective value
	// callers report.
	entries, total, size = l.Query("", math.MaxInt, math.MaxInt)
	if entries != nil || total != 10 {
		t.Errorf("MaxInt page = %d entries total %d, want nil/10 (clamped, no wrap)", len(entries), total)
	}
	if size != ledgerPageSizeCap {
		t.Errorf("MaxInt page size = %d, want the page cap %d", size, ledgerPageSizeCap)
	}
}

// TestLedgerAppendObserver pins the append observer — the daemon's
// golens-metrics feed hangs off it: each Append delivers its entry exactly
// once, to an observer installed mid-stream (earlier appends stay
// unobserved — the unset observer is the ordinary shape), and the invoke
// happens outside the ledger lock, so an observer that reads the ledger
// back (Query, Len) cannot deadlock against the append it belongs to.
func TestLedgerAppendObserver(t *testing.T) {
	t.Run("delivers each append once, outside the lock", func(t *testing.T) {
		done := make(chan struct{})
		go func() {
			defer close(done)
			l := &Ledger{}
			var seen []Entry
			// observe reads the ledger back on every delivery — as the
			// golens feed's neighbors do — which must not deadlock against
			// the append in flight; only the installed-late observer
			// records, so the pre-install append stays unobserved.
			observe := func(record bool) func(Entry) {
				return func(e Entry) {
					_, _, _ = l.Query("", 1, 10)
					l.Len()
					if record {
						seen = append(seen, e)
					}
				}
			}
			l.SetOnAppend(observe(false))
			l.Append(Entry{Task: "pre"})
			l.SetOnAppend(observe(true))

			l.Append(Entry{Task: "a"})
			l.Append(Entry{Task: "b"})
			if len(seen) != 2 || seen[0].Task != "a" || seen[1].Task != "b" {
				t.Errorf("observed %d entries (%v), want a then b, once each", len(seen), seen)
			}
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the observed append deadlocked — the observer ran inside the ledger lock")
		}
	})
}

// TestRegistryTokenLifecycle pins the registration's secret economy, which
// is worker-minted: the worker presents its own secret (NewToken's shape is
// pinned here too), a live heartbeat presenting that same secret is
// accepted, a live collision presenting anything else — a wrong token or
// nothing — is refused, and a lapsed name re-registers freely (the dead
// process's entry cannot be distinguished from a squatter's, and the TTL
// is what clears both).
func TestRegistryTokenLifecycle(t *testing.T) {
	reg := NewRegistry(time.Minute)
	up := map[string]*url.URL{"openai": {Scheme: "https", Host: "u.example"}}
	tok := NewToken()
	if tok == "" || len(tok) != 32 || strings.Trim(tok, "0123456789abcdef") != "" {
		t.Fatalf("NewToken = %q, want 128 bits of hex", tok)
	}
	if err := reg.Register("w", "q", up, nil, nil, tok); err != nil {
		t.Fatalf("Register with the worker's own secret: %v", err)
	}
	if err := reg.Register("w", "q", up, nil, nil, tok); err != nil {
		t.Errorf("live re-registration with the presented secret: %v, want accepted (heartbeat)", err)
	}
	if err := reg.Register("w", "q", up, nil, nil, tok+"0"); err == nil {
		t.Error("live collision with a wrong token accepted")
	}
	if err := reg.Register("w", "q", up, nil, nil, ""); err == nil {
		t.Error("live collision with no token accepted")
	}

	// Past the TTL the entry is gone: the name re-registers from anywhere,
	// under the old or a fresh secret — the TTL, not the secret, clears a
	// dead registration.
	base := time.Now()
	reg.now = func() time.Time { return base.Add(2 * time.Minute) }
	if err := reg.Register("w", "q", up, nil, nil, "fresh-secret"); err != nil {
		t.Errorf("lapsed re-registration: %v, want accepted", err)
	}
	if _, _, _, _, got, ok := reg.Resolve("w", "openai"); !ok || got != "fresh-secret" {
		t.Errorf("Resolve after the lapsed re-registration = (ok %v, token %q), want the fresh secret stored", ok, got)
	}
}

// TestRegistryRegisterPrunesExpired pins the sweep: a registration past its
// TTL is dropped by the next Register from anywhere, so the map cannot grow
// without bound and a dead worker's staged paths serve nothing.
func TestRegistryRegisterPrunesExpired(t *testing.T) {
	reg := NewRegistry(time.Minute)
	base := time.Now()
	reg.now = func() time.Time { return base }
	up := map[string]*url.URL{"openai": {Scheme: "https", Host: "u.example"}}
	reg.Register("a", "q", up, nil, nil, NewToken())
	reg.Register("b", "q", up, nil, nil, NewToken())
	if reg.Live() != 2 {
		t.Fatalf("Live = %d, want 2", reg.Live())
	}

	base = base.Add(2 * time.Minute)
	reg.Register("c", "q", up, nil, nil, NewToken())
	if got := reg.Live(); got != 1 {
		t.Errorf("Live = %d after the sweep, want 1 (only the fresh registration)", got)
	}
	if _, _, _, _, _, ok := reg.Resolve("a", "openai"); ok {
		t.Error("expired worker a still resolves an upstream")
	}
	if _, _, _, _, _, ok := reg.Resolve("c", "openai"); !ok {
		t.Error("survivor lost its upstream")
	}
}

// TestRegistryResolveAndTokenGate pins the atomic read and the constant-time
// match: Resolve hands back the registration's upstream, queue, per-wire
// alias, per-wire key, and restoration token in one answer — a keyless wire
// resolves an empty key (OAuth pass-through), an unaliased wire an empty
// alias — expiry kills the whole entry, and tokenPresented matches only a
// non-empty exact presented secret.
func TestRegistryResolveAndTokenGate(t *testing.T) {
	reg := NewRegistry(time.Minute)
	base := time.Now()
	reg.now = func() time.Time { return base }
	up := map[string]*url.URL{
		"anthropic": {Scheme: "https", Host: "a.example"},
		"openai":    {Scheme: "https", Host: "o.example"},
	}
	tok := "abcdef0123456789abcdef0123456789"
	reg.Register("w", "q", up, map[string]string{"anthropic": "sk-real"}, nil, tok)

	gotUp, gotQueue, gotAlias, gotKey, gotTok, ok := reg.Resolve("w", "anthropic")
	if !ok || gotUp.Host != "a.example" || gotQueue != "q" || gotAlias != "" || gotKey != "sk-real" || gotTok != tok {
		t.Errorf("Resolve(anthropic) = (%v, %q, alias %q, %q, %q, %v), want the full registration", gotUp, gotQueue, gotAlias, gotKey, gotTok, ok)
	}
	if _, _, gotAlias, gotKey, _, ok := reg.Resolve("w", "openai"); !ok || gotKey != "" || gotAlias != "" {
		t.Errorf("Resolve(keyless openai) = (alias %q, key %q, ok %v), want both empty (pass-through)", gotAlias, gotKey, ok)
	}
	if _, _, _, _, _, ok := reg.Resolve("other", "anthropic"); ok {
		t.Error("an unregistered worker resolved")
	}

	base = base.Add(2 * time.Minute)
	if _, _, _, _, _, ok := reg.Resolve("w", "anthropic"); ok {
		t.Error("an expired registration still resolves")
	}

	if !tokenPresented(tok, tok) {
		t.Error("tokenPresented rejected the exact secret")
	}
	if tokenPresented("", tok) || tokenPresented(tok+"0", tok) || tokenPresented("other", tok) {
		t.Error("tokenPresented matched an empty, extended, or foreign secret")
	}
	if tokenPresented(tok, "") {
		t.Error("tokenPresented matched against an empty registration (nothing is registered)")
	}
}

// TestRegistryDeregister pins the graceful exit's half of the registration
// handshake: the registered secret clears the name, so the next boot of the
// same worker registers on its first attempt instead of waiting out the
// ghost's TTL (the 409 delayed boot); a wrong or missing secret refuses
// without touching the live registration — a ghost from a SIGKILLed process
// presents nothing and still ages out through the TTL sweep alone; and an
// unknown name deregisters nothing and succeeds, idempotent for the
// shutdown paths that can hand the token over twice.
func TestRegistryDeregister(t *testing.T) {
	reg := NewRegistry(time.Minute)
	up := map[string]*url.URL{"openai": {Scheme: "https", Host: "u.example"}}
	tok := NewToken()
	if err := reg.Register("w", "q7", up, nil, nil, tok); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Nothing presented on the loopback can clear a live registration —
	// not an empty secret, not a near-miss, not a guess.
	for _, presented := range []string{"", tok + "0", "guessed"} {
		if err := reg.Deregister("w", presented); err == nil {
			t.Errorf("Deregister(presented %q) = nil, want the refusal", presented)
		}
	}
	if _, _, _, _, _, ok := reg.Resolve("w", "openai"); !ok {
		t.Fatal("a refused deregistration removed the live registration")
	}
	if err := reg.Deregister("never-registered", tok); err != nil {
		t.Errorf("Deregister(unknown worker) = %v, want nil", err)
	}

	// The secret itself clears the name — and the second handover is a
	// no-op success, the idempotency the double shutdown paths lean on.
	if err := reg.Deregister("w", tok); err != nil {
		t.Fatalf("Deregister with the registered secret: %v", err)
	}
	if _, _, _, _, _, ok := reg.Resolve("w", "openai"); ok {
		t.Error("the deregistered registration still resolves an upstream")
	}
	if err := reg.Deregister("w", tok); err != nil {
		t.Errorf("the second dereg = %v, want nil (idempotent)", err)
	}

	// The window this closes: the successor boot registers on its first
	// attempt, under a fresh secret.
	if err := reg.Register("w", "q7", up, nil, nil, "fresh-secret"); err != nil {
		t.Errorf("the successor boot's registration = %v, want accepted on the first attempt", err)
	}
	if _, _, _, _, got, ok := reg.Resolve("w", "openai"); !ok || got != "fresh-secret" {
		t.Errorf("Resolve after the successor boot = (ok %v, token %q), want the fresh secret stored", ok, got)
	}
}

// stubUpstream records every request and answers status with body.
func stubUpstream(rec *recorder, status int, body []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		rec.add(r, b)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write(body)
	}))
}

// testToken is the stand-in registration secret the tests present at
// registration and in keyed rounds' attribution keys — the worker-minted
// economy's caller-side stand-in for NewToken.
const testToken = "abcdef0123456789abcdef0123456789"

// registerRelay registers worker w with the relay over the /register
// endpoint — the worker-side path, presenting its own secret. The relay
// stores the secret and returns no body: the secret never travels back.
func registerRelay(t *testing.T, root, worker string, upstreams, keys, aliases map[string]string) {
	t.Helper()
	body, err := json.Marshal(registerRequest{Worker: worker, Queue: "q7", Upstreams: upstreams, Keys: keys, Aliases: aliases, Token: testToken})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(root+"/register", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("register status %d: %s", resp.StatusCode, b)
	}
}

// openaiCompletionReply marshals a plain chat completion with the usage
// block the openai wire reports.
func openaiCompletionReply() []byte {
	return []byte(`{"model":"glm-4","usage":{"prompt_tokens":10,"completion_tokens":5,` +
		`"prompt_tokens_details":{"cached_tokens":4}}}`)
}

// TestProxyForwardsVerbatim pins the forward path: the staged prefix is
// stripped and the remainder — the upstream's own path prefix plus the
// CLI's suffix — forwarded verbatim with the query intact, hop headers
// stripped, and Accept-Encoding pinned to identity (a transport negotiating
// gzip would tee unwrapped bytes into the ledger, not the client's bytes).
func TestProxyForwardsVerbatim(t *testing.T) {
	rec := &recorder{}
	up := stubUpstream(rec, http.StatusOK, openaiCompletionReply())
	defer up.Close()

	_, root := startRelay(t, &Ledger{}, NewRegistry(time.Minute))
	registerRelay(t, root, "w", map[string]string{"openai": up.URL + "/v1"}, nil, nil)

	stagedURL := staged(root, "w", WireOpenAI, up.URL+"/v1")
	if stagedURL == "" {
		t.Fatal("staged URL is empty")
	}
	// An unkeyed round: a plain credential that never parses as the
	// synthetic shape, so the forward is exercised without the restore
	// gate (pinned separately in TestProxyTokenGate).
	h := http.Header{}
	h.Set("Authorization", "Bearer sk-plain-round-key")
	h.Set("Connection", "close")
	h.Set("Accept-Encoding", "gzip")
	status, body := postRelay(t, stagedURL+"/chat/completions?api_version=2", h, []byte(`{"model":"m"}`))
	if status != http.StatusOK || !bytes.Equal(body, openaiCompletionReply()) {
		t.Fatalf("relayed reply = (%d) %s, want the upstream's bytes verbatim", status, body)
	}

	reqs := rec.all()
	if len(reqs) != 1 {
		t.Fatalf("upstream saw %d requests, want 1", len(reqs))
	}
	r := reqs[0]
	if r.Path != "/v1/chat/completions" || r.RawQuery != "api_version=2" {
		t.Errorf("forward = %s?%s, want /v1/chat/completions?api_version=2", r.Path, r.RawQuery)
	}
	if got := r.Header.Get("Accept-Encoding"); got != "identity" {
		t.Errorf("forward Accept-Encoding = %q, want identity", got)
	}
	if got := r.Header.Get("Connection"); got != "" {
		t.Errorf("forward carries hop header Connection: %q", got)
	}
}

// TestProxyPathShapes pins the staged-path grammar and the registry's
// reachability: every malformed prefix, unknown wire, and worker without a
// live registration fails before any upstream is dialed.
func TestProxyPathShapes(t *testing.T) {
	rec := &recorder{}
	up := stubUpstream(rec, http.StatusOK, openaiCompletionReply())
	defer up.Close()

	_, root := startRelay(t, &Ledger{}, NewRegistry(time.Minute))
	registerRelay(t, root, "w", map[string]string{"openai": up.URL + "/v1"}, nil, nil)

	for _, c := range []struct {
		name, path string
		want       int
	}{
		{"no worker segment", root + "/relay/", http.StatusNotFound},
		{"worker but no wire", root + "/relay/w", http.StatusNotFound},
		{"wire but no path", root + "/relay/w/openai", http.StatusNotFound},
		{"unknown wire", root + "/relay/w/bogus/v1/chat/completions", http.StatusNotFound},
		{"unknown worker", root + "/relay/ghost/openai/v1/chat/completions", http.StatusBadGateway},
		{"registered wire not staged by this worker", root + "/relay/w/anthropic/v1/messages", http.StatusBadGateway},
	} {
		status, body := postRelay(t, c.path, http.Header{}, []byte(`{}`))
		if status != c.want {
			t.Errorf("%s: status = %d, want %d (body %s)", c.name, status, c.want, body)
		}
	}
	if got := len(rec.all()); got != 0 {
		t.Errorf("upstream saw %d requests across the failing shapes, want 0", got)
	}
}

// TestProxyTokenGate pins the restore gate: a keyed round — the synthetic
// attribution shape in the credential slot — forwards only with the
// registration's secret token on its tail. A wrong or missing token is a
// 401 before any upstream dial and leaves nothing in the ledger; a valid
// one restores the registered key into the credential slot the wire reads.
func TestProxyTokenGate(t *testing.T) {
	rec := &recorder{}
	up := stubUpstream(rec, http.StatusOK, openaiCompletionReply())
	defer up.Close()

	ledger := &Ledger{}
	registry := NewRegistry(time.Minute)
	_, root := startRelay(t, ledger, registry)
	tok := testToken
	registry.Register("w", "q7", map[string]*url.URL{
		"openai":    mustURL(t, up.URL+"/v1"),
		"anthropic": mustURL(t, up.URL),
	}, map[string]string{"openai": "sk-real-openai", "anthropic": "sk-real-anthropic"}, nil, tok)

	stagedOpenAI := staged(root, "w", WireOpenAI, up.URL+"/v1")

	t.Run("missing token is refused before the dial", func(t *testing.T) {
		h := http.Header{}
		h.Set("Authorization", "Bearer task-w1-role-dev-round-3")
		status, body := postRelay(t, stagedOpenAI+"/chat/completions", h, []byte(`{}`))
		if status != http.StatusUnauthorized {
			t.Fatalf("status = %d (%s), want 401", status, body)
		}
		if got := len(rec.all()); got != 0 {
			t.Fatalf("upstream saw %d requests, want 0", got)
		}
		if ledger.Len() != 0 {
			t.Errorf("ledger recorded %d entries for a refused round, want 0", ledger.Len())
		}
	})

	t.Run("wrong token is refused", func(t *testing.T) {
		h := http.Header{}
		h.Set("Authorization", "Bearer task-w1-role-dev-round-3-rk-deadbeef")
		if status, _ := postRelay(t, stagedOpenAI+"/chat/completions", h, []byte(`{}`)); status != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", status)
		}
	})

	t.Run("valid token restores the registered key", func(t *testing.T) {
		h := http.Header{}
		h.Set("Authorization", "Bearer task-w1-role-dev-round-3-rk-"+tok)
		if status, _ := postRelay(t, stagedOpenAI+"/chat/completions", h, []byte(`{}`)); status != http.StatusOK {
			t.Fatalf("status = %d, want the forward through", status)
		}
		reqs := rec.all()
		if len(reqs) != 1 {
			t.Fatalf("upstream saw %d requests, want 1", len(reqs))
		}
		if got := reqs[0].Header.Get("Authorization"); got != "Bearer sk-real-openai" {
			t.Errorf("forwarded credential = %q, want the registered key restored", got)
		}
	})

	t.Run("token expired with the registration is refused", func(t *testing.T) {
		registry.Register("gone", "q7", map[string]*url.URL{"openai": mustURL(t, up.URL+"/v1")}, map[string]string{"openai": "sk-gone"}, nil, testToken)
		stagedGone := staged(root, "gone", WireOpenAI, up.URL+"/v1")
		h := http.Header{}
		h.Set("Authorization", "Bearer task-g1-role-dev-round-1-rk-"+testToken)
		registry.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
		defer func() { registry.now = time.Now }()
		if status, _ := postRelay(t, stagedGone+"/chat/completions", h, []byte(`{}`)); status != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502 (the registration lapsed before the token was even checked)", status)
		}
	})
}

// TestProxyTokenGateAnthropicSlot pins the same gate on the anthropic
// wire's credential slot: the token-gated restore lands in X-Api-Key.
func TestProxyTokenGateAnthropicSlot(t *testing.T) {
	rec := &recorder{}
	up := stubUpstream(rec, http.StatusOK, []byte(`{"model":"m","usage":{"input_tokens":1}}`))
	defer up.Close()

	registry := NewRegistry(time.Minute)
	_, root := startRelay(t, &Ledger{}, registry)
	registry.Register("w", "q7", map[string]*url.URL{"anthropic": mustURL(t, up.URL)}, map[string]string{"anthropic": "sk-real-anthropic"}, nil, testToken)

	stagedAnthropic := staged(root, "w", WireAnthropic, up.URL)
	h := http.Header{}
	h.Set("X-Api-Key", "task-w1-role-test-round-7-rk-"+testToken)
	if status, _ := postRelay(t, stagedAnthropic+"/v1/messages", h, []byte(`{}`)); status != http.StatusOK {
		t.Fatalf("status = %d, want the forward through", status)
	}
	reqs := rec.all()
	if len(reqs) != 1 {
		t.Fatalf("upstream saw %d requests, want 1", len(reqs))
	}
	if got := reqs[0].Header.Get("X-Api-Key"); got != "sk-real-anthropic" {
		t.Errorf("forwarded X-Api-Key = %q, want the registered key restored", got)
	}
}

// TestProxyUnkeyedAndKeylessRounds pins the no-restore paths: a credential
// that is not the synthetic shape (a plain API key) forwards untouched and
// attributes at worker level; a wire registered keyless forwards the
// inbound credential even for a token-valid keyed round (OAuth
// pass-through); a credential-less request forwards credential-less —
// nothing is minted for anyone.
func TestProxyUnkeyedAndKeylessRounds(t *testing.T) {
	rec := &recorder{}
	up := stubUpstream(rec, http.StatusOK, openaiCompletionReply())
	defer up.Close()

	ledger := &Ledger{}
	registry := NewRegistry(time.Minute)
	_, root := startRelay(t, ledger, registry)
	registry.Register("w", "q7", map[string]*url.URL{"openai": mustURL(t, up.URL+"/v1")}, nil, nil, testToken) // keyless wire

	stagedOpenAI := staged(root, "w", WireOpenAI, up.URL+"/v1")

	t.Run("plain key forwards untouched, worker-level attribution", func(t *testing.T) {
		h := http.Header{}
		h.Set("Authorization", "Bearer sk-plain-oauth")
		if status, _ := postRelay(t, stagedOpenAI+"/chat/completions", h, []byte(`{}`)); status != http.StatusOK {
			t.Fatalf("status = %d, want the forward through", status)
		}
		if got := rec.all()[0].Header.Get("Authorization"); got != "Bearer sk-plain-oauth" {
			t.Errorf("forwarded credential = %q, want the round's own untouched", got)
		}
		entries, _, _ := ledger.Query("w", 1, 10)
		if len(entries) != 1 {
			t.Fatalf("ledger holds %d entries, want 1", len(entries))
		}
		if entries[0].Task != "" || entries[0].Role != "" || entries[0].Round != "" {
			t.Errorf("unkeyed entry = %+v, want worker-level attribution only", entries[0])
		}
		if entries[0].InputTokens != 10 || entries[0].OutputTokens != 5 || entries[0].Model != "glm-4" {
			t.Errorf("unkeyed entry usage = %+v, want the reply's usage block still read", entries[0])
		}
	})

	t.Run("keyed round on a keyless wire forwards its own credential", func(t *testing.T) {
		h := http.Header{}
		h.Set("Authorization", "Bearer oauth-token-of-the-round-rk-NOT-a-shape")
		h.Set("X-OAuth", "keep")
		// keyed=false: the value above is not the synthetic shape, so no
		// token check runs and no restore fires.
		before := ledger.Len()
		if status, _ := postRelay(t, stagedOpenAI+"/chat/completions", h, []byte(`{}`)); status != http.StatusOK {
			t.Fatalf("status = %d, want the forward through", status)
		}
		if got := rec.all()[1].Header.Get("Authorization"); got != "Bearer oauth-token-of-the-round-rk-NOT-a-shape" {
			t.Errorf("forwarded credential = %q, want the inbound OAuth bearer untouched", got)
		}
		if ledger.Len() != before+1 {
			t.Errorf("ledger grew %d, want one entry for the forwarded call", ledger.Len()-before)
		}
	})

	t.Run("credential-less round forwards credential-less", func(t *testing.T) {
		before := ledger.Len()
		if status, _ := postRelay(t, stagedOpenAI+"/chat/completions", http.Header{}, []byte(`{}`)); status != http.StatusOK {
			t.Fatalf("status = %d, want the forward through", status)
		}
		r := rec.all()[2]
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("forwarded Authorization = %q, want none minted", got)
		}
		if ledger.Len() != before+1 {
			t.Errorf("ledger grew %d, want one entry", ledger.Len()-before)
		}
	})
}

// TestProxyLedgerShapes pins what gets recorded: exactly the two inspected
// POST shapes — POSTs ending in /chat/completions or /v1/messages, whatever
// path prefix the staged URL carried — land in the ledger with usage,
// model, wire, queue, duration, and the upstream's status, and a provider
// failure (an HTTP 500) is recorded too — a flap is exactly what the
// ledger exists to show. Everything else proxies verbatim and unrecorded:
// a model-list GET or POST is forwarded (the upstream saw it) but must not
// dilute the usage ledger with a bare zero-usage row.
func TestProxyLedgerShapes(t *testing.T) {
	rec := &recorder{}
	up := stubUpstream(rec, http.StatusOK, openaiCompletionReply())
	defer up.Close()

	fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		rec.add(r, b)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":{"message":"overloaded"}}`))
	}))
	defer fail.Close()

	ledger := &Ledger{}
	registry := NewRegistry(time.Minute)
	_, root := startRelay(t, ledger, registry)
	registry.Register("w", "q7", map[string]*url.URL{
		"openai":    mustURL(t, up.URL+"/v1"),
		"anthropic": mustURL(t, fail.URL),
	}, nil, nil, testToken)

	stagedOpenAI := staged(root, "w", WireOpenAI, up.URL+"/v1")
	stagedAnthropic := staged(root, "w", WireAnthropic, fail.URL)

	// 1: inspected openai POST.
	postRelay(t, stagedOpenAI+"/chat/completions", http.Header{}, []byte(`{}`))
	// 2: inspected anthropic POST — a 500, recorded with its status.
	postRelay(t, stagedAnthropic+"/v1/messages", http.Header{}, []byte(`{}`))

	// Newest first: the anthropic 500, then the openai 200 with its usage
	// block and model read (usage extraction only runs on a body that
	// carries a usage block; an error object is not mistaken for one).
	entries, total, _ := ledger.Query("", 1, 10)
	if total != 2 || len(entries) != 2 {
		t.Fatalf("ledger = %d entries (total %d), want only the two inspected provider calls recorded", len(entries), total)
	}
	failed, openai := entries[0], entries[1]
	if failed.Wire != WireAnthropic || failed.Status != http.StatusInternalServerError || failed.Queue != "q7" {
		t.Errorf("failed-provider entry = %+v, want the 500 recorded on the anthropic wire with its queue", failed)
	}
	if failed.Task != "" || failed.InputTokens != 0 {
		t.Errorf("failed entry = %+v, want no usage read out of an error object and no attribution (keyless)", failed)
	}
	if openai.Wire != WireOpenAI || openai.Status != http.StatusOK ||
		openai.InputTokens != 10 || openai.OutputTokens != 5 || openai.Model != "glm-4" {
		t.Errorf("openai entry = %+v, want the usage block and model read", openai)
	}
	if openai.DurationMS < 0 || failed.DurationMS < 0 {
		t.Errorf("negative duration")
	}

	// Uninspected shapes forward — the upstream answers both — but record
	// nothing: the ledger stays at the two provider calls.
	if resp, err := http.Get(stagedOpenAI + "/chat/completions"); err != nil {
		t.Fatalf("GET through relay: %v", err)
	} else {
		resp.Body.Close()
	}
	postRelay(t, stagedOpenAI+"/models", http.Header{}, []byte(`{}`))
	if got := len(rec.all()); got != 4 {
		t.Fatalf("upstream saw %d requests, want all four forwarded", got)
	}
	if entries, total, _ := ledger.Query("", 1, 10); total != 2 || len(entries) != 2 {
		t.Errorf("ledger grew to %d (total %d) on uninspected shapes, want the two provider calls only", len(entries), total)
	}
}

// TestAliasRidesRegistrationToLedger pins the alias dimension end to end:
// a worker registered with a key_alias resolves it beside the key, and
// every ledger entry its wire produces carries the label — an unkeyed
// round included (the label describes the credential, not the round's
// attribution); a worker registered without one produces unlabeled
// entries, and the ledger JSON omits the alias field for them.
func TestAliasRidesRegistrationToLedger(t *testing.T) {
	rec := &recorder{}
	up := stubUpstream(rec, http.StatusOK, openaiCompletionReply())
	defer up.Close()

	ledger := &Ledger{}
	registry := NewRegistry(time.Minute)
	_, root := startRelay(t, ledger, registry)
	registry.Register("labeled", "q7", map[string]*url.URL{"openai": mustURL(t, up.URL+"/v1")},
		map[string]string{"openai": "sk-real"}, map[string]string{"openai": "quota-a"}, testToken)
	registry.Register("plain", "q7", map[string]*url.URL{"openai": mustURL(t, up.URL+"/v1")},
		nil, nil, testToken)

	for _, worker := range []string{"labeled", "plain"} {
		stagedURL := staged(root, worker, WireOpenAI, up.URL+"/v1")
		if status, _ := postRelay(t, stagedURL+"/chat/completions", http.Header{}, []byte(`{}`)); status != http.StatusOK {
			t.Fatalf("%s: status = %d, want the forward through", worker, status)
		}
	}
	entries, total, _ := ledger.Query("", 1, 10)
	if total != 2 || len(entries) != 2 {
		t.Fatalf("ledger = %d entries (total %d), want one per worker", len(entries), total)
	}
	// Newest first: plain proxied last, so it opens the page.
	if got := entries[0].Worker + "/" + entries[0].Alias; got != "plain/" {
		t.Errorf("newest entry = %q, want the unaliased worker unlabeled", got)
	}
	if got := entries[1].Worker + "/" + entries[1].Alias; got != "labeled/quota-a" {
		t.Errorf("older entry = %q, want the registered alias stamped", got)
	}

	// The wire shape: an unaliased entry omits the alias field entirely —
	// a metrics surface grouping on it must see absence, not an empty
	// string.
	raw, err := json.Marshal(entries[0])
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if _, present := doc["alias"]; present {
		t.Errorf("unaliased entry JSON = %s, want no alias key (omitempty)", raw)
	}
}

// TestProxyUpstreamUnreachable pins the dial-failure face: a 502 with the
// diagnostic, and no ledger entry — nothing was forwarded, so there is no
// usage to record.
func TestProxyUpstreamUnreachable(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	ledger := &Ledger{}
	registry := NewRegistry(time.Minute)
	_, root := startRelay(t, ledger, registry)
	registry.Register("w", "q7", map[string]*url.URL{"openai": mustURL(t, deadURL+"/v1")}, nil, nil, testToken)

	stagedOpenAI := staged(root, "w", WireOpenAI, deadURL+"/v1")
	status, body := postRelay(t, stagedOpenAI+"/chat/completions", http.Header{}, []byte(`{}`))
	if status != http.StatusBadGateway || !strings.Contains(string(body), "upstream unreachable") {
		t.Fatalf("unreachable upstream served (%d) %s, want the 502 diagnostic", status, body)
	}
	if ledger.Len() != 0 {
		t.Errorf("ledger recorded %d entries, want 0", ledger.Len())
	}
}

// TestProxyStreamPassthrough pins the client's stream arriving intact while
// the ledger reads it: a [DONE]-terminated openai SSE body is relayed
// byte-for-byte and the usage chunk still lands in the ledger.
func TestProxyStreamPassthrough(t *testing.T) {
	sse := "data: " + `{"model":"glm-4","choices":[{"delta":{"content":"hi"}}]}` + "\n\n" +
		"data: " + `{"model":"glm-4","choices":[],"usage":{"prompt_tokens":11,"completion_tokens":6}}` + "\n\n" +
		"data: [DONE]\n\n"
	rec := &recorder{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		rec.add(r, b)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(sse))
	}))
	defer up.Close()

	ledger := &Ledger{}
	registry := NewRegistry(time.Minute)
	_, root := startRelay(t, ledger, registry)
	registry.Register("w", "q7", map[string]*url.URL{"openai": mustURL(t, up.URL+"/v1")}, nil, nil, testToken)

	stagedOpenAI := staged(root, "w", WireOpenAI, up.URL+"/v1")
	status, body := postRelay(t, stagedOpenAI+"/chat/completions", http.Header{}, []byte(`{}`))
	if status != http.StatusOK || string(body) != sse {
		t.Fatalf("client's stream = (%d) %q, want the SSE bytes verbatim", status, body)
	}
	entries, _, _ := ledger.Query("", 1, 5)
	if len(entries) != 1 || entries[0].InputTokens != 11 || entries[0].OutputTokens != 6 || entries[0].Model != "glm-4" {
		t.Fatalf("ledger entry = %+v, want the usage chunk read off the stream", entries)
	}
}

// TestServeRegister pins the registration endpoint's face: a valid body
// stores the mapping against the worker-presented secret and answers 204
// with no body — the secret came from the caller and goes nowhere; a bad
// body, a missing worker or upstreams, a missing token, an unknown wire,
// or a scheme-less upstream is a 400 naming the problem; a live name
// collision without the current secret is a 409; keys and aliases for
// unstaged wires are dropped rather than stored (they can never be looked
// up).
func TestServeRegister(t *testing.T) {
	registry := NewRegistry(time.Minute)
	_, root := startRelay(t, &Ledger{}, registry)

	post := func(body string) (int, string) {
		resp, err := http.Post(root+"/register", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST /register: %v", err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	tok := "1234abcdef0123456789abcdef01234567"
	status, body := post(`{"worker":"w","upstreams":{"openai":"https://u.example/v1"},"keys":{"openai":"sk"},"token":"` + tok + `"}`)
	if status != http.StatusNoContent || body != "" {
		t.Fatalf("valid registration = (%d) %q, want 204 with no body — the secret is never echoed", status, body)
	}
	up, _, _, key, storedTok, ok := registry.Resolve("w", "openai")
	if !ok || up.Host != "u.example" || key != "sk" || storedTok != tok {
		t.Errorf("stored registration = (%v, key %q, token %q, ok %v), want the mapping under the presented secret", up, key, storedTok, ok)
	}

	for _, c := range []struct {
		name, body string
		want       string
	}{
		{"bad json", `{`, "bad registration body"},
		{"no worker", `{"upstreams":{"openai":"https://u.example"}}`, "needs worker"},
		{"no upstreams", `{"worker":"w","upstreams":{}}`, "needs worker"},
		{"no token", `{"worker":"w","upstreams":{"openai":"https://u.example"}}`, "needs a restoration token"},
		{"unknown wire", `{"worker":"w","upstreams":{"grpc":"https://u.example"},"token":"t"}`, "unknown wire"},
		{"scheme-less upstream", `{"worker":"w","upstreams":{"openai":"u.example/v1"},"token":"t"}`, "no scheme/host"},
	} {
		if status, body := post(c.body); status != http.StatusBadRequest || !strings.Contains(body, c.want) {
			t.Errorf("%s: (%d) %s, want 400 naming %q", c.name, status, body, c.want)
		}
	}

	// A live registration holds the name: a collision presenting a
	// different secret is a 409 (re-presenting the stored one is the
	// heartbeat's refresh, pinned in TestRegistryTokenLifecycle).
	if status, body := post(`{"worker":"w","upstreams":{"openai":"https://evil.example/v1"},"token":"not-the-secret"}`); status != http.StatusConflict || !strings.Contains(body, "already registered") {
		t.Errorf("live collision = (%d) %s, want 409 naming the held name", status, body)
	}
	up, _, _, _, _, _ = registry.Resolve("w", "openai")
	if up.Host != "u.example" {
		t.Errorf("collision overwrote the upstream: %v", up)
	}

	// Keys and aliases on unstaged wires are dropped, not an error — and
	// the alias drop matches the key drop wire for wire: a label for a
	// wire this worker never staged is stored nowhere.
	status, _ = post(`{"worker":"w2","upstreams":{"anthropic":"https://a.example"},"keys":{"openai":"sk-oa"},"token":"t"}`)
	if status != http.StatusNoContent {
		t.Fatalf("unstaged-key registration = %d, want 204", status)
	}
	if _, _, _, key, _, ok := registry.Resolve("w2", "anthropic"); !ok || key != "" {
		t.Errorf("w2 = (key %q, ok %v), want the keyless anthropic wire", key, ok)
	}
	status, _ = post(`{"worker":"w3","upstreams":{"anthropic":"https://a.example"},"keys":{"anthropic":"sk-a"},"aliases":{"anthropic":"quota-a","openai":"ghost-label"},"token":"t"}`)
	if status != http.StatusNoContent {
		t.Fatalf("aliased registration = %d, want 204", status)
	}
	if _, _, alias, key, _, ok := registry.Resolve("w3", "anthropic"); !ok || alias != "quota-a" || key != "sk-a" {
		t.Errorf("w3 = (alias %q, key %q, ok %v), want the labeled keyed anthropic wire", alias, key, ok)
	}
	if got := registry.m["w3"].Aliases; len(got) != 1 || got["openai"] != "" {
		t.Errorf("w3 stored aliases = %v, want only the staged wire's label (the unstaged openai label dropped)", got)
	}

	// Wrong method or unknown endpoints fail loudly.
	if resp, err := http.Get(root + "/register"); err != nil {
		t.Fatalf("GET /register: %v", err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET /register = %d, want 404", resp.StatusCode)
		}
	}
	if resp, err := http.Get(root + "/nope"); err != nil {
		t.Fatalf("GET /nope: %v", err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET /nope = %d, want 404", resp.StatusCode)
		}
	}
}

// TestServeDeregister pins the endpoint's face end to end: a deregistration
// presenting the registered secret answers 204 with no body and the freed
// name re-registers on its first attempt — the restart orphan window shut
// on the wire; an unknown worker is a 204 no-op; a wrong secret is a 401
// that leaves the registration serving; a missing field or an unreadable
// body is a 400 naming the problem; and GET is not the method.
func TestServeDeregister(t *testing.T) {
	registry := NewRegistry(time.Minute)
	_, root := startRelay(t, &Ledger{}, registry)
	registerRelay(t, root, "w", map[string]string{"openai": "https://u.example/v1"}, nil, nil)

	post := func(body string) (int, string) {
		resp, err := http.Post(root+"/deregister", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST /deregister: %v", err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	if status, body := post(`{"worker":"w","token":"not-the-secret"}`); status != http.StatusUnauthorized || !strings.Contains(body, "refuses this token") {
		t.Errorf("wrong-secret dereg = (%d) %q, want 401 naming the refusal", status, body)
	}
	if _, _, _, _, _, ok := registry.Resolve("w", "openai"); !ok {
		t.Fatal("a refused deregistration removed the live registration")
	}

	for _, c := range []struct {
		name, body, want string
	}{
		{"bad json", `{`, "bad deregistration body"},
		{"no worker", `{"token":"t"}`, "needs worker and token"},
		{"no token", `{"worker":"w"}`, "needs worker and token"},
	} {
		if status, body := post(c.body); status != http.StatusBadRequest || !strings.Contains(body, c.want) {
			t.Errorf("%s: (%d) %q, want 400 naming %q", c.name, status, body, c.want)
		}
	}

	// A boot that never registered has nothing to clean: still a success.
	if status, body := post(`{"worker":"ghost","token":"whatever"}`); status != http.StatusNoContent || body != "" {
		t.Errorf("unknown-worker dereg = (%d) %q, want 204 with no body", status, body)
	}

	// The handoff itself, and the win it buys: 204, no body — then the
	// restart's next boot registers under a fresh secret on its first
	// attempt instead of the 409.
	if status, body := post(`{"worker":"w","token":"` + testToken + `"}`); status != http.StatusNoContent || body != "" {
		t.Fatalf("dereg with the registered secret = (%d) %q, want 204 with no body", status, body)
	}
	if _, _, _, _, _, ok := registry.Resolve("w", "openai"); ok {
		t.Fatal("the deregistered name still resolves")
	}
	body, err := json.Marshal(registerRequest{Worker: "w", Queue: "q7", Upstreams: map[string]string{"openai": "https://u.example/v1"}, Token: "fresh-secret"})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(root+"/register", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("successor boot's register: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("successor boot's register = %d, want 204 on the first attempt (no 409 delayed boot)", resp.StatusCode)
	}
	if _, _, _, _, got, ok := registry.Resolve("w", "openai"); !ok || got != "fresh-secret" {
		t.Errorf("Resolve after the successor boot = (ok %v, token %q), want the fresh secret stored", ok, got)
	}

	// Wrong method fails loudly, like every other endpoint.
	if resp, err := http.Get(root + "/deregister"); err != nil {
		t.Fatalf("GET /deregister: %v", err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET /deregister = %d, want 404", resp.StatusCode)
		}
	}
}

// TestStatusAndLedgerEndpoints pins the daemon-facing reads: /status
// carries the liveness fields, /ledger echoes the effective paging and
// serves the recorded entries newest first.
func TestStatusAndLedgerEndpoints(t *testing.T) {
	ledger := &Ledger{}
	ledger.Append(Entry{Worker: "w", Wire: "openai", InputTokens: 3})
	registry := NewRegistry(time.Minute)
	registry.Register("w", "q7", map[string]*url.URL{"openai": {Scheme: "https", Host: "u.example"}}, nil, nil, testToken)
	r, root := startRelay(t, ledger, registry)

	resp, err := http.Get(root + "/status")
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	defer resp.Body.Close()
	var st struct {
		Status            string `json:"status"`
		UptimeSeconds     int64  `json:"uptime_seconds"`
		Entries           int    `json:"entries"`
		LiveRegistrations int    `json:"live_registrations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode /status: %v", err)
	}
	if st.Status != "ok" || st.Entries != 1 || st.LiveRegistrations != 1 || st.UptimeSeconds < 0 {
		t.Errorf("status doc = %+v, want ok with the ledger and registry counts", st)
	}

	resp2, err := http.Get(root + "/ledger?worker=w&page=1&page_size=5")
	if err != nil {
		t.Fatalf("GET /ledger: %v", err)
	}
	defer resp2.Body.Close()
	var doc struct {
		Total    int     `json:"total"`
		Page     int     `json:"page"`
		PageSize int     `json:"page_size"`
		Entries  []Entry `json:"entries"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&doc); err != nil {
		t.Fatalf("decode /ledger: %v", err)
	}
	if doc.Total != 1 || doc.Page != 1 || doc.PageSize != 5 || len(doc.Entries) != 1 {
		t.Errorf("ledger doc = %+v, want the one entry with the paging echoed", doc)
	}
	_ = r
}

// TestParseAttribution pins the synthetic key's parse: the task-, -role-,
// -round- anchors split in order, the -rk- tail is split off last (a round
// part containing -rk- still parses), and anything not the shape — an
// OAuth bearer, a plain key, a truncated form, an empty part — is unkeyed.
func TestParseAttribution(t *testing.T) {
	for _, c := range []struct {
		name, key                string
		task, role, round, token string
		keyed                    bool
	}{
		{"full shape", "task-w1-role-dev-round-3", "w1", "dev", "3", "", true},
		{"with token tail", "task-w1-role-dev-round-3-rk-" + testToken, "w1", "dev", "3", testToken, true},
		// The slim terminal review's role carries a hyphen; the anchor
		// split must not mistake it for a delimiter.
		{"hyphenated role", "task-w1-role-final-review-round-2", "w1", "final-review", "2", "", true},
		{"role none", "task-w1-role-none-round-0", "w1", "none", "0", "", true},
		{"round part carrying -rk- still parses", "task-w1-role-dev-round-3-rk-inner-rk-tok", "w1", "dev", "3-rk-inner", "tok", true},
		{"not the shape", "sk-plain-key", "", "", "", "", false},
		{"oauth bearer", "eyJhbGciOi.eyJzdWIi.TOKEN", "", "", "", "", false},
		{"missing role", "task-w1-round-3", "", "", "", "", false},
		{"missing round", "task-w1-role-dev", "", "", "", "", false},
		{"empty task", "task--role-dev-round-3", "", "", "", "", false},
		{"empty role", "task-w1-role--round-3", "", "", "", "", false},
		{"empty round", "task-w1-role-dev-round-", "", "", "", "", false},
		{"bare prefix", "task-", "", "", "", "", false},
	} {
		task, role, round, token, keyed := parseAttribution(c.key)
		if task != c.task || role != c.role || round != c.round || token != c.token || keyed != c.keyed {
			t.Errorf("%s: parseAttribution(%q) = (%q,%q,%q,%q,%v), want (%q,%q,%q,%q,%v)",
				c.name, c.key, task, role, round, token, keyed, c.task, c.role, c.round, c.token, c.keyed)
		}
	}
}

// TestApplyUsage pins the usage extraction from a buffered body: either
// wire's plain JSON, either wire's SSE stream, the openai naming mapped
// onto the same slots, an absent field never zeroing one an earlier chunk
// set, and a body that is neither shape leaving the entry untouched.
func TestApplyUsage(t *testing.T) {
	t.Run("plain openai json maps prompt/completion naming", func(t *testing.T) {
		var e Entry
		applyUsage(&e, []byte(`{"model":"glm-4","usage":{"prompt_tokens":10,"completion_tokens":5,`+
			`"prompt_tokens_details":{"cached_tokens":4}}}`))
		if e.Model != "glm-4" || e.InputTokens != 10 || e.OutputTokens != 5 || e.CacheReadTokens != 4 {
			t.Errorf("entry = %+v, want 10 in / 5 out / 4 cached from the openai names", e)
		}
	})

	t.Run("plain anthropic json with both cache sides", func(t *testing.T) {
		var e Entry
		applyUsage(&e, []byte(`{"model":"claude-opus-5","usage":{"input_tokens":7,"output_tokens":3,`+
			`"cache_read_input_tokens":2,"cache_creation_input_tokens":1}}`))
		if e.Model != "claude-opus-5" || e.InputTokens != 7 || e.OutputTokens != 3 ||
			e.CacheReadTokens != 2 || e.CacheWriteTokens != 1 {
			t.Errorf("entry = %+v, want the anthropic usage block mapped", e)
		}
	})

	t.Run("body without a usage block leaves the entry untouched", func(t *testing.T) {
		var e Entry
		applyUsage(&e, []byte(`{"error":{"type":"overloaded","message":"try again"}}`))
		if e != (Entry{}) {
			t.Errorf("entry = %+v, want untouched by an error object", e)
		}
		applyUsage(&e, []byte("not json at all"))
		if e != (Entry{}) {
			t.Errorf("entry = %+v, want untouched by non-json", e)
		}
	})

	t.Run("anthropic sse merge keeps the input side", func(t *testing.T) {
		var e Entry
		applyUsage(&e, []byte(
			"event: message_start\n"+
				`data: {"type":"message_start","message":{"model":"claude-opus-5","usage":{"input_tokens":100}}}`+"\n\n"+
				"event: ping\n"+
				`data: {"type":"ping"}`+"\n\n"+
				"event: message_delta\n"+
				`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":20}}`+"\n\n"))
		if e.Model != "claude-opus-5" || e.InputTokens != 100 || e.OutputTokens != 20 {
			t.Errorf("entry = %+v, want input 100 from message_start kept through the output-20 delta", e)
		}
	})

	t.Run("openai sse chunks carry model and one usage block", func(t *testing.T) {
		var e Entry
		applyUsage(&e, []byte(
			`data: {"model":"glm-4","choices":[{"delta":{"content":"a"}}]}`+"\n\n"+
				`data: {"model":"glm-4","choices":[],"usage":{"prompt_tokens":9,"completion_tokens":4}}`+"\n\n"+
				"data: [DONE]\n\n"))
		if e.Model != "glm-4" || e.InputTokens != 9 || e.OutputTokens != 4 {
			t.Errorf("entry = %+v, want 9 in / 4 out off the usage chunk", e)
		}
	})

	t.Run("a zero-valued reported field stays zero, an absent one does not zero", func(t *testing.T) {
		// message_start reports input 100; the delta reports output 0
		// explicitly. The explicit zero is real usage data and must land;
		// it must not zero the input side.
		var e Entry
		applyUsage(&e, []byte(
			`data: {"message":{"model":"m","usage":{"input_tokens":100}}}`+"\n"+
				`data: {"usage":{"output_tokens":0}}`+"\n"))
		if e.InputTokens != 100 || e.OutputTokens != 0 {
			t.Errorf("entry = %+v, want 100 in kept and the explicit 0 out landed", e)
		}
	})

	t.Run("a payload-less chunk naming a model does not vouch it", func(t *testing.T) {
		// The captured live frame (the model host's SSE keepalive, streamed
		// while the real model loads): a role-only delta, no content, no
		// usage. A stream killed in that phase must record no model at all —
		// the entry lands in the Models tab's unknown bucket, never a
		// phantom row named by whatever the wire carried.
		var e Entry
		applyUsage(&e, []byte(`data: {"model":"keepalive","choices":[{"delta":{"role":"assistant","content":""},"finish_reason":null}]}`+"\n\n"))
		if e != (Entry{}) {
			t.Errorf("entry = %+v, want untouched by the payload-less frame", e)
		}
		// The mechanism, not the name: any model-named chunk without
		// payload or usage stays unstamped, on the SSE walk as in plain
		// JSON. The explicit-null frame is its own trap: RawMessage keeps
		// the literal bytes, so a null tool_calls is len 4 and must be
		// rejected as the null it names, not read as payload by length.
		applyUsage(&e, []byte(`data: {"model":"phantom-synthetic","choices":[]}`+"\n\n"))
		applyUsage(&e, []byte(`{"model":"phantom-synthetic"}`))
		applyUsage(&e, []byte(`data: {"model":"keepalive","choices":[{"index":0,"delta":{"role":"assistant","content":null,"tool_calls":null},"finish_reason":null}]}`+"\n\n"))
		if e != (Entry{}) {
			t.Errorf("entry = %+v, want untouched by every payload-less shape", e)
		}
	})

	t.Run("payload vouches the model it names", func(t *testing.T) {
		cases := map[string]string{
			"content":         `data: {"model":"m","choices":[{"delta":{"content":"a"}}]}`,
			"reasoning":       `data: {"model":"m","choices":[{"delta":{"reasoning_content":"think"}}]}`,
			"tool calls":      `data: {"model":"m","choices":[{"delta":{"tool_calls":[{"id":"t1"}]}}]}`,
			"finish reason":   `data: {"model":"m","choices":[{"delta":{},"finish_reason":"stop"}]}`,
			"usage alone":     `data: {"model":"m","choices":[],"usage":{"prompt_tokens":9}}`,
			"anthropic body":  `data: {"message":{"model":"m","usage":{"input_tokens":3}}}`,
			"plain openai":    `{"model":"m","usage":{"prompt_tokens":9}}`,
			"plain anthropic": `{"model":"m","usage":{"input_tokens":3}}`,
		}
		for name, body := range cases {
			var e Entry
			applyUsage(&e, []byte(body+"\n\n"))
			if e.Model != "m" {
				t.Errorf("%s: entry model = %q, want the payload-vouched name", name, e.Model)
			}
		}
	})

	t.Run("a late payload-less frame cannot overwrite a vouched model", func(t *testing.T) {
		// The killed-mid-generation shape: content chunks vouched the real
		// model, then the host's synthetic frame rode a generation stall.
		// The vouched name stands.
		var e Entry
		applyUsage(&e, []byte(
			`data: {"model":"real-m","choices":[{"delta":{"content":"a"}}]}`+"\n\n"+
				`data: {"model":"real-m","choices":[{"delta":{"content":"b"}}]}`+"\n\n"+
				`data: {"model":"keepalive","choices":[{"delta":{"role":"assistant","content":""},"finish_reason":null}]}`+"\n\n"))
		if e.Model != "real-m" {
			t.Errorf("entry model = %q, want the payload-vouched model kept against the late frame", e.Model)
		}
	})
}

// TestCloseIsQuiet pins the designed shutdown's face: Close goes through
// the server, so Serve exits on the http.ErrServerClosed path and logs
// nothing — with the listener closed underneath it, Serve would exit with a
// raw accept error and every daemon shutdown would log a bogus
// "metricsrelay: serve:" line.
func TestCloseIsQuiet(t *testing.T) {
	r, root := startRelay(t, &Ledger{}, NewRegistry(time.Minute))

	status, body := func() (int, string) {
		resp, err := http.Get(root + "/status")
		if err != nil {
			t.Fatalf("GET /status before Close: %v", err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}()
	if status != http.StatusOK {
		t.Fatalf("relay not serving before Close: status %d, body %s", status, body)
	}

	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := http.Get(root + "/status"); err == nil {
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

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u
}
