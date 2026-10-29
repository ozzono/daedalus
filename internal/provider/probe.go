// Package provider probes agent API providers for liveness and quota
// state — the live check behind `worker status`'s API column, and the
// quota-reset parser the worker's provider failover shares.
package provider

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// ProbeTimeout bounds one probe request. A provider slower than this is
// effectively down for the status report's purposes.
const ProbeTimeout = 10 * time.Second

// quotaResetZone parses the timestamps z.ai embeds in its 429 quota
// messages ("Usage limit reached ... Your limit will reset at
// 2026-09-18 09:13:00"). Observed to be UTC+8: a stamp of 09:13:00
// corresponded to a real 01:13:00Z reset. The offset is z.ai-specific, so
// only messages carrying z.ai's signature are parsed; any other vendor
// embedding the same date format returns zero and lands in the callers'
// unparsed-escalation path rather than being mis-held in the wrong zone.
var quotaResetZone = time.FixedZone("UTC+8", 8*60*60)

// ParseQuotaReset extracts the provider's quota-window reset time from an
// error or round-output text. Zero when absent, unparseable, or from a
// vendor whose timestamp zone is unknown — callers then fall back to
// their own escalating hold schedule.
func ParseQuotaReset(s string) time.Time {
	if !strings.Contains(strings.ToLower(s), "usage limit reached") {
		return time.Time{}
	}
	_, rest, ok := strings.Cut(s, "limit will reset at ")
	if !ok {
		return time.Time{}
	}
	const layout = "2006-01-02 15:04:05"
	if len(rest) < len(layout) {
		return time.Time{}
	}
	t, err := time.ParseInLocation(layout, rest[:len(layout)], quotaResetZone)
	if err != nil {
		return time.Time{}
	}
	return t
}

// Style selects the wire the generation heartbeat speaks. The non-gen
// formats derive their whole request from Spec.Format alone.
type Style int

const (
	// StyleAnthropic POSTs {url}/v1/messages with the x-api-key header —
	// the same call the jailed agent's rounds make.
	StyleAnthropic Style = iota
	// StyleOpenAI POSTs {url}/chat/completions with a Bearer token and
	// thinking disabled — the minimal chat-completions request.
	StyleOpenAI
)

// Heartbeat formats — the wire shape one probe request takes, named by
// the config's provider.heartbeat / fallback.heartbeat. The format is a
// property of the backend behind the endpoint (which liveness surface it
// exposes), not of the wire rounds speak, so any format pairs with either
// Style. Every format but HeartbeatGen spends no tokens and holds no
// serving slot.
const (
	// HeartbeatOpenAI GETs {url}/models — the OpenAI-shaped model list:
	// the openai-wire default, a process-up check that costs nothing.
	HeartbeatOpenAI = "openai"
	// HeartbeatAnthropic POSTs {url}/v1/messages/count_tokens — creates no
	// message and bills nothing, but the key is validated (401 otherwise),
	// the hosted wire's reachability+auth check without a generation.
	HeartbeatAnthropic = "anthropic"
	// HeartbeatOllama GETs {root}/api/ps — ollama's currently-loaded model
	// list, the closest non-gen answer to "would a round succeed now".
	HeartbeatOllama = "ollama"
	// HeartbeatColibri GETs {root}/status — the control-plane endpoint that
	// stays instant while the serving path wedges: the one check that
	// separates "server alive, serving wedged" from "server dead".
	HeartbeatColibri = "colibri"
	// HeartbeatOMLX POSTs {url}/tokenize — tokenizes a one-word prompt with
	// the loaded model's tokenizer: no slot, no generation, and unlike a
	// process-up check the model itself is exercised.
	HeartbeatOMLX = "omlx"
	// HeartbeatGen is the generation probe — Spec.Style's minimal "hi"
	// request. Costs tokens and holds a serving slot, but it is the only
	// format proving an end-to-end completion and the only one parsing
	// z.ai's 429 quota-reset stamp.
	HeartbeatGen = "gen-heartbeat"
)

// HeartbeatFormats lists the accepted heartbeat values, for the config's
// load-time validation. Empty (the config field unset) is legal everywhere
// and resolves at the probe per the probed wire's Style: HeartbeatAnthropic
// on an anthropic-wire base, HeartbeatOpenAI on an openai-wire one.
var HeartbeatFormats = []string{
	HeartbeatOpenAI, HeartbeatAnthropic, HeartbeatOllama,
	HeartbeatColibri, HeartbeatOMLX, HeartbeatGen,
}

// Spec names one provider to probe. URL is the base (e.g.
// https://api.z.ai/api/anthropic or an OpenAI-style .../v4 base); the
// format-appropriate path is appended. Model is what the probe names —
// callers pass the heartbeat model when configured, else the main model.
type Spec struct {
	URL   string
	Key   string
	Model string
	// Style is consumed only by HeartbeatGen.
	Style Style
	// Format names the heartbeat's wire shape — one of the Heartbeat*
	// constants. Empty is the default, resolved per Style (the one backend
	// signal an unset format has): an anthropic-wire base gets
	// HeartbeatAnthropic's non-token count_tokens check, an openai-wire
	// base HeartbeatOpenAI's model list. The generation probe runs only
	// when the config names gen-heartbeat explicitly.
	Format string
}

// Result is one probe outcome. Detail is safe to print: it never carries
// the key. OK covers any 2xx.
type Result struct {
	OK     bool
	Detail string
}

// probeClient is the probe's HTTP client. Its root pool unions Go's system
// roots with the macOS OpenSSL bundle (/etc/ssl/cert.pem — the same file
// curl verifies against): on some macOS configurations Go's Security-
// framework extraction yields an empty pool and every HTTPS probe would
// die to a spurious x509 error while curl works fine.
var probeClient = &http.Client{
	Timeout:   ProbeTimeout,
	Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: rootPool()}},
}

// rootPool builds the TLS root pool: the system pool plus, when the file
// exists, the macOS OpenSSL CA bundle.
func rootPool() *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if pem, err := os.ReadFile("/etc/ssl/cert.pem"); err == nil {
		pool.AppendCertsFromPEM(pem)
	}
	return pool
}

// Probe sends one provider's configured heartbeat and classifies the
// response: by default a non-generation liveness check, the token-costing
// generation probe only when the config names gen-heartbeat.
func Probe(ctx context.Context, spec Spec) Result {
	if spec.URL == "" || spec.Key == "" || spec.Model == "" {
		return Result{Detail: "n/a (provider not fully configured)"}
	}
	if spec.Format == HeartbeatGen {
		return probeGen(ctx, spec)
	}
	// An unset format resolves per Style — the probed wire is the one
	// signal the default has, and an OpenAI-shaped request does not exist
	// on an anthropic wire.
	if spec.Format == "" && spec.Style == StyleAnthropic {
		spec.Format = HeartbeatAnthropic
	}
	var (
		url  string
		body []byte
		err  error
	)
	authKey, authVal := "Authorization", "Bearer "+spec.Key
	switch spec.Format {
	case HeartbeatAnthropic:
		url = strings.TrimSuffix(spec.URL, "/") + "/v1/messages/count_tokens"
		authKey, authVal = "x-api-key", spec.Key
		body, err = json.Marshal(map[string]any{
			"model":    spec.Model,
			"messages": []map[string]string{{"role": "user", "content": "hi"}},
		})
	case HeartbeatOllama:
		url = rootURL(spec.URL) + "/api/ps"
	case HeartbeatColibri:
		url = rootURL(spec.URL) + "/status"
	case HeartbeatOMLX:
		url = strings.TrimSuffix(spec.URL, "/") + "/tokenize"
		body, err = json.Marshal(map[string]any{"model": spec.Model, "prompt": "hi"})
	default: // HeartbeatOpenAI — named, or empty on an openai-wire base
		url = strings.TrimSuffix(spec.URL, "/") + "/models"
	}
	if err != nil {
		return Result{Detail: "build request: " + err.Error()}
	}

	method := http.MethodGet
	var reqBody io.Reader
	if body != nil {
		method = http.MethodPost
		reqBody = strings.NewReader(string(body))
	}
	ctx, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return Result{Detail: "unreachable: " + err.Error()}
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(authKey, authVal)
	if authKey == "x-api-key" {
		req.Header.Set("anthropic-version", "2023-06-01")
	}
	resp, err := probeClient.Do(req)
	if err != nil {
		return Result{Detail: "unreachable: " + err.Error()}
	}
	defer resp.Body.Close()
	return classify(resp.StatusCode, drain(resp), false)
}

// probeGen is HeartbeatGen's implementation — the request every config's
// probe made before formats existed, preserved verbatim: a real (tiny)
// generation through the Style-named wire.
func probeGen(ctx context.Context, spec Spec) Result {
	var (
		url     string
		authKey string
		authVal string
		body    []byte
		err     error
	)
	switch spec.Style {
	case StyleAnthropic:
		url = strings.TrimSuffix(spec.URL, "/") + "/v1/messages"
		authKey, authVal = "x-api-key", spec.Key
		body, err = json.Marshal(map[string]any{
			"model":      spec.Model,
			"max_tokens": 8,
			"messages":   []map[string]string{{"role": "user", "content": "hi"}},
		})
	case StyleOpenAI:
		url = strings.TrimSuffix(spec.URL, "/") + "/chat/completions"
		authKey, authVal = "Authorization", "Bearer "+spec.Key
		// max_tokens bounds the generation: without it the server applies
		// its own --ngen ceiling, and a probe that lands becomes a legal
		// hours-long generation holding a serving slot past this client's
		// timeout (the Anthropic shape below has always pinned one).
		body, err = json.Marshal(map[string]any{
			"model":      spec.Model,
			"max_tokens": 16,
			"thinking":   map[string]string{"type": "disabled"},
			"messages":   []map[string]string{{"role": "user", "content": "hi"}},
		})
	}
	if err != nil {
		return Result{Detail: "build request: " + err.Error()}
	}

	ctx, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return Result{Detail: "unreachable: " + err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(authKey, authVal)
	if spec.Style == StyleAnthropic {
		req.Header.Set("anthropic-version", "2023-06-01")
	}
	resp, err := probeClient.Do(req)
	if err != nil {
		return Result{Detail: "unreachable: " + err.Error()}
	}
	defer resp.Body.Close()
	return classify(resp.StatusCode, drain(resp), true)
}

// rootURL strips a trailing slash and /v1 suffix, giving the server root
// the control-plane paths hang off: the config's openai-side URLs carry
// the OpenAI-style version prefix, which ollama's and colibri's own
// addressing does not use.
func rootURL(u string) string {
	return strings.TrimSuffix(strings.TrimSuffix(u, "/"), "/v1")
}

// drain reads (bounded) the response body so quota messages with their
// reset stamps can be classified.
func drain(resp *http.Response) string {
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	return string(buf[:n])
}

// classify turns a probe response into the status column's text. The
// quota-reset parse is gen-only by decision: the generation probe is the
// check whose 429 is the one a round would hit, so its reset stamp is the
// one parsed into a hold; a non-gen format's 429 reports plain failure
// detail and leaves reset-stamp parsing to the round-failure path.
func classify(status int, body string, gen bool) Result {
	switch {
	case status >= 200 && status < 300:
		return Result{OK: true, Detail: fmt.Sprintf("ok (http %d)", status)}
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return Result{Detail: fmt.Sprintf("%d auth rejected", status)}
	case status == http.StatusTooManyRequests && gen:
		if reset := ParseQuotaReset(body); !reset.IsZero() {
			return Result{Detail: "429 quota (resets " + reset.Local().Format("15:04") + ")"}
		}
		return Result{Detail: "429 rate limited"}
	default:
		return Result{Detail: fmt.Sprintf("http %d", status)}
	}
}
