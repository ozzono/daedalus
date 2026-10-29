package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

// capture records one probe request.
type capture struct {
	Method  string
	Path    string
	Headers http.Header
	Body    map[string]any
}

// probeServer serves status for every request and records the last one.
func probeServer(t *testing.T, status int, body string) (*httptest.Server, *capture) {
	t.Helper()
	got := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Method = r.Method
		got.Path = r.URL.Path
		got.Headers = r.Header.Clone()
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		got.Body = b
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, got
}

// TestProbeAnthropicShape pins the gen-heartbeat's anthropic-style request:
// path, x-api-key auth, protocol version header, and the tiny "hi" message
// naming the heartbeat model (falling back to the main model). The probe
// only generates when the config names gen-heartbeat — an unset format
// resolves per Style first, and the non-gen formats then derive their
// requests from Format alone (TestProbeHeartbeatFormats).
func TestProbeAnthropicShape(t *testing.T) {
	srv, got := probeServer(t, 200, `{}`)
	res := Probe(context.Background(), Spec{
		URL: srv.URL, Key: "sk-test", Model: "glm-air", Style: StyleAnthropic, Format: HeartbeatGen,
	})
	if !res.OK {
		t.Fatalf("result = %+v, want ok", res)
	}
	if got.Path != "/v1/messages" {
		t.Errorf("path = %q, want /v1/messages", got.Path)
	}
	if key := got.Headers.Get("x-api-key"); key != "sk-test" {
		t.Errorf("x-api-key = %q, want sk-test", key)
	}
	if v := got.Headers.Get("anthropic-version"); v == "" {
		t.Error("anthropic-version header missing")
	}
	if m := got.Body["model"]; m != "glm-air" {
		t.Errorf("model = %v, want glm-air", m)
	}
	if msgs, _ := got.Body["messages"].([]any); len(msgs) != 1 {
		t.Errorf("messages = %v, want exactly one", got.Body["messages"])
	}
	if _, ok := got.Body["thinking"]; ok {
		t.Error("anthropic probe must not send a thinking override")
	}
}

// TestProbeOpenAIShape pins the gen-heartbeat's OpenAI-style request: path,
// Bearer auth, thinking disabled, and the max_tokens bound — without one
// the server applies its own generation ceiling and a landed probe holds a
// serving slot for hours past this client's timeout (the anthropic shape
// has always pinned one).
func TestProbeOpenAIShape(t *testing.T) {
	srv, got := probeServer(t, 200, `{}`)
	res := Probe(context.Background(), Spec{
		URL: srv.URL, Key: "sk-test", Model: "glm-4.7", Style: StyleOpenAI, Format: HeartbeatGen,
	})
	if !res.OK {
		t.Fatalf("result = %+v, want ok", res)
	}
	if got.Path != "/chat/completions" {
		t.Errorf("path = %q, want /chat/completions", got.Path)
	}
	if a := got.Headers.Get("Authorization"); a != "Bearer sk-test" {
		t.Errorf("Authorization = %q, want Bearer sk-test", a)
	}
	th, _ := got.Body["thinking"].(map[string]any)
	if th["type"] != "disabled" {
		t.Errorf("thinking = %v, want disabled", got.Body["thinking"])
	}
	if mt, _ := got.Body["max_tokens"].(float64); mt != 16 {
		t.Errorf("max_tokens = %v, want 16 — an unbounded probe becomes the server's longest legal generation", got.Body["max_tokens"])
	}
}

// TestProbeClassifications pins the status-column text for the outcomes an
// operator acts on: quota with a reset stamp, auth rejection, other HTTP
// statuses, and transport failures. The reset stamp parses only under the
// gen-heartbeat — its 429 is the one a round would hit, so it is the one
// carrying quota semantics; a non-gen format's 429 reports plain failure
// detail.
func TestProbeClassifications(t *testing.T) {
	cases := []struct {
		name   string
		format string
		status int
		body   string
		wantIn string
	}{
		{"quota with reset", HeartbeatGen, 429, `Usage limit reached for 5 hour. Your limit will reset at 2099-01-01 09:13:00`, "429 quota (resets "},
		{"quota without reset", HeartbeatGen, 429, `too many requests`, "429 rate limited"},
		{"auth", HeartbeatGen, 401, `{}`, "401 auth rejected"},
		{"forbidden", HeartbeatGen, 403, `{}`, "403 auth rejected"},
		{"server error", HeartbeatGen, 500, `{}`, "http 500"},
		// A z.ai 429 stamp only classifies under the gen-heartbeat: the 429
		// a round would hit is the generation probe's, so its reset stamp
		// is the one parsed.
		{"non-gen 429 keeps its stamp unclassified", "", 429, `Usage limit reached for 5 hour. Your limit will reset at 2099-01-01 09:13:00`, "http 429"},
		{"non-gen auth", "", 401, `{}`, "401 auth rejected"},
		{"non-gen server error", "", 500, `{}`, "http 500"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := probeServer(t, tc.status, tc.body)
			res := Probe(context.Background(), Spec{
				URL: srv.URL, Key: "k", Model: "m", Style: StyleAnthropic, Format: tc.format,
			})
			if res.OK {
				t.Fatalf("result = %+v, want not ok", res)
			}
			if !strings.Contains(res.Detail, tc.wantIn) {
				t.Errorf("detail = %q, want it to contain %q", res.Detail, tc.wantIn)
			}
		})
	}

	// Unreachable endpoint.
	res := Probe(context.Background(), Spec{
		URL: "http://127.0.0.1:1", Key: "k", Model: "m", Style: StyleAnthropic,
	})
	if res.OK || !strings.Contains(res.Detail, "unreachable") {
		t.Errorf("result = %+v, want unreachable", res)
	}

	// Malformed URL fails at request construction, reported unreachable.
	res = Probe(context.Background(), Spec{URL: "://bad", Key: "k", Model: "m", Style: StyleAnthropic})
	if res.OK || !strings.Contains(res.Detail, "unreachable") {
		t.Errorf("result = %+v, want unreachable", res)
	}

	// Half-configured spec is reported as such, not probed.
	res = Probe(context.Background(), Spec{URL: "", Key: "k", Model: "m"})
	if res.OK || !strings.Contains(res.Detail, "not fully configured") {
		t.Errorf("result = %+v, want not fully configured", res)
	}
}

// TestProbeHeartbeatFormats pins each non-generation heartbeat's request —
// what the config's heartbeat field names the probe to send: the path and
// method per format, the auth surface it presents, and the body it spends.
// An unset format resolves per Style first — the anthropic wire defaults
// to its token-free count_tokens check, the openai wire to the model list
// — the root-addressed formats (ollama, colibri) strip the openai-style
// /v1 prefix and trailing slash the URL carries, and each named format
// pairs with either Style — the wire the rounds speak never leaks into the
// format's request. gen-heartbeat's requests are the two shape tests
// above.
func TestProbeHeartbeatFormats(t *testing.T) {
	anthroBody := map[string]any{
		"model":    "hb-m",
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}
	omlxBody := map[string]any{"model": "hb-m", "prompt": "hi"}
	cases := []struct {
		name        string
		format      string
		style       Style
		urlSuffix   string // appended to the server URL, exercising the trim rules
		wantMethod  string
		wantPath    string
		wantAuthKey string
		wantAuthVal string
		wantCT      string // Content-Type, empty when the request must not set one
		wantVersion string // anthropic-version, empty when the request must not set one
		wantBody    map[string]any
	}{
		{"empty default on the anthropic wire is the count_tokens check", "", StyleAnthropic, "", "POST", "/v1/messages/count_tokens", "x-api-key", "k", "application/json", "2023-06-01", anthroBody},
		{"empty default on the openai wire is the model list", "", StyleOpenAI, "", "GET", "/models", "Authorization", "Bearer k", "", "", nil},
		{"openai lists the models", HeartbeatOpenAI, StyleOpenAI, "", "GET", "/models", "Authorization", "Bearer k", "", "", nil},
		{"anthropic counts tokens", HeartbeatAnthropic, StyleAnthropic, "/", "POST", "/v1/messages/count_tokens", "x-api-key", "k", "application/json", "2023-06-01", anthroBody},
		{"ollama reads the root's loaded models", HeartbeatOllama, StyleOpenAI, "/v1/", "GET", "/api/ps", "Authorization", "Bearer k", "", "", nil},
		{"colibri reads the root's status", HeartbeatColibri, StyleAnthropic, "/v1", "GET", "/status", "Authorization", "Bearer k", "", "", nil},
		{"omlx tokenizes one word", HeartbeatOMLX, StyleOpenAI, "/", "POST", "/tokenize", "Authorization", "Bearer k", "application/json", "", omlxBody},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, got := probeServer(t, 200, `{}`)
			res := Probe(context.Background(), Spec{
				URL: srv.URL + tc.urlSuffix, Key: "k", Model: "hb-m", Style: tc.style, Format: tc.format,
			})
			if !res.OK {
				t.Fatalf("result = %+v, want ok", res)
			}
			if got.Method != tc.wantMethod {
				t.Errorf("method = %q, want %q", got.Method, tc.wantMethod)
			}
			if got.Path != tc.wantPath {
				t.Errorf("path = %q, want %q", got.Path, tc.wantPath)
			}
			if k := got.Headers.Get(tc.wantAuthKey); k != tc.wantAuthVal {
				t.Errorf("%s = %q, want %q", tc.wantAuthKey, k, tc.wantAuthVal)
			}
			if ct := got.Headers.Get("Content-Type"); ct != tc.wantCT {
				t.Errorf("Content-Type = %q, want %q", ct, tc.wantCT)
			}
			if v := got.Headers.Get("anthropic-version"); v != tc.wantVersion {
				t.Errorf("anthropic-version = %q, want %q", v, tc.wantVersion)
			}
			if !reflect.DeepEqual(got.Body, tc.wantBody) {
				t.Errorf("body = %v, want %v", got.Body, tc.wantBody)
			}
		})
	}
}

// TestParseQuotaReset pins the parser against the real z.ai sample
// observed on 2026-09-17 (09:13:00 UTC+8 == 01:13:00Z) and the
// no-stamp / truncated / malformed fallbacks.
func TestParseQuotaReset(t *testing.T) {
	real := `API Error: Request rejected (429) · [1308][Usage limit reached for 5 hour. Your limit will reset at 2026-09-18 09:13:00][20260918090607e2638973a55e476a]`
	got := ParseQuotaReset(real)
	want := time.Date(2026, 9, 18, 1, 13, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("ParseQuotaReset(real) = %v, want %v", got.UTC(), want)
	}
	// Signature-less samples never get past the vendor gate, so they pin
	// the gate itself (the guards past it are pinned below).
	for name, s := range map[string]string{
		"gate: no marker":    "429 rate limited",
		"gate: truncated":    "limit will reset at 2026-09-18 09:1",
		"gate: malformed":    "limit will reset at not-a-date-0000",
		"gate: other vendor": "Error 1226: limit will reset at 2026-09-18 09:13:00",
	} {
		if got := ParseQuotaReset(s); !got.IsZero() {
			t.Errorf("ParseQuotaReset(%q) = %v, want zero", name, got)
		}
	}
	// Signed-but-bad samples carry the z.ai signature, so the gate passes
	// and a later guard must reject them: no reset marker after the
	// signature, a stamp truncated below the layout length, and a stamp
	// long enough but not a date.
	for name, s := range map[string]string{
		"signed, no reset marker": "Usage limit reached for 5 hour. Your limit will reset at",
		"signed, truncated stamp": "Usage limit reached for 5 hour. Your limit will reset at 2099-01-01 09:1",
		"signed, malformed stamp": "Usage limit reached for 5 hour. Your limit will reset at not-a-date-0000000000",
	} {
		if got := ParseQuotaReset(s); !got.IsZero() {
			t.Errorf("ParseQuotaReset(%q) = %v, want zero", name, got)
		}
	}
}
