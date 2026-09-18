package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// capture records one probe request.
type capture struct {
	Path    string
	Headers http.Header
	Body    map[string]any
}

// probeServer serves status for every request and records the last one.
func probeServer(t *testing.T, status int, body string) (*httptest.Server, *capture) {
	t.Helper()
	got := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

// TestProbeAnthropicShape pins the anthropic-style request: path, x-api-key
// auth, protocol version header, and the tiny "hi" message naming the
// heartbeat model (falling back to the main model).
func TestProbeAnthropicShape(t *testing.T) {
	srv, got := probeServer(t, 200, `{}`)
	res := Probe(context.Background(), Spec{
		URL: srv.URL, Key: "sk-test", Model: "glm-air", Style: StyleAnthropic,
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

// TestProbeOpenAIShape pins the OpenAI-style request: path, Bearer auth,
// and thinking disabled — the minimal chat-completions call.
func TestProbeOpenAIShape(t *testing.T) {
	srv, got := probeServer(t, 200, `{}`)
	res := Probe(context.Background(), Spec{
		URL: srv.URL, Key: "sk-test", Model: "glm-4.7", Style: StyleOpenAI,
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
}

// TestProbeClassifications pins the status-column text for the outcomes an
// operator acts on: quota with a reset stamp, auth rejection, other HTTP
// statuses, and transport failures.
func TestProbeClassifications(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		wantIn string
	}{
		{"quota with reset", 429, `Usage limit reached for 5 hour. Your limit will reset at 2099-01-01 09:13:00`, "429 quota (resets "},
		{"quota without reset", 429, `too many requests`, "429 rate limited"},
		{"auth", 401, `{}`, "401 auth rejected"},
		{"forbidden", 403, `{}`, "403 auth rejected"},
		{"server error", 500, `{}`, "http 500"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := probeServer(t, tc.status, tc.body)
			res := Probe(context.Background(), Spec{
				URL: srv.URL, Key: "k", Model: "m", Style: StyleAnthropic,
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
