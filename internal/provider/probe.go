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

// Style selects the probe's wire shape.
type Style int

const (
	// StyleAnthropic POSTs {url}/v1/messages with the x-api-key header —
	// the same call the jailed agent's rounds make.
	StyleAnthropic Style = iota
	// StyleOpenAI POSTs {url}/chat/completions with a Bearer token and
	// thinking disabled — the minimal chat-completions request.
	StyleOpenAI
)

// Spec names one provider to probe. URL is the base (e.g.
// https://api.z.ai/api/anthropic or an OpenAI-style .../v4 base); the
// style-appropriate path is appended. Model is what the probe names —
// callers pass the heartbeat model when configured, else the main model.
type Spec struct {
	URL   string
	Key   string
	Model string
	Style Style
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

// Probe sends one minimal message ("hi") through the provider and
// classifies the response. It is deliberately cheap — a handful of tokens
// — and side-effect free.
func Probe(ctx context.Context, spec Spec) Result {
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
		body, err = json.Marshal(map[string]any{
			"model":    spec.Model,
			"thinking": map[string]string{"type": "disabled"},
			"messages": []map[string]string{{"role": "user", "content": "hi"}},
		})
	}
	if err != nil {
		return Result{Detail: "build request: " + err.Error()}
	}
	if spec.URL == "" || spec.Key == "" || spec.Model == "" {
		return Result{Detail: "n/a (provider not fully configured)"}
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
	return classify(resp.StatusCode, drain(resp))
}

// drain reads (bounded) the response body so quota messages with their
// reset stamps can be classified.
func drain(resp *http.Response) string {
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	return string(buf[:n])
}

// classify turns a probe response into the status column's text.
func classify(status int, body string) Result {
	switch {
	case status >= 200 && status < 300:
		return Result{OK: true, Detail: fmt.Sprintf("ok (http %d)", status)}
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return Result{Detail: fmt.Sprintf("%d auth rejected", status)}
	case status == http.StatusTooManyRequests:
		if reset := ParseQuotaReset(body); !reset.IsZero() {
			return Result{Detail: "429 quota (resets " + reset.Local().Format("15:04") + ")"}
		}
		return Result{Detail: "429 rate limited"}
	default:
		return Result{Detail: fmt.Sprintf("http %d", status)}
	}
}
