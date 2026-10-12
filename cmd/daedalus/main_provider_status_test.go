package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
)

// TestEnvWithoutProviderVars pins the daemon spawn scrub: every
// config-derived provider variable is dropped from the inherited
// environment — so a stale shell export can never win over a rotated
// config — while everything else passes through untouched.
func TestEnvWithoutProviderVars(t *testing.T) {
	in := []string{
		"PATH=/usr/bin:/bin",
		"ANTHROPIC_BASE_URL=https://stale.example",
		"ANTHROPIC_API_KEY=stale-key",
		"ANTHROPIC_MODEL=stale-model",
		"ANTHROPIC_DEFAULT_HAIKU_MODEL=stale-air",
		"API_TIMEOUT_MS=1",
		"OPENAI_BASE_URL=https://stale-oa.example",
		"OPENAI_API_KEY=stale-oa",
		"OPENAI_MODEL=stale-oa-model",
		"DAEDALUS_FALLBACK_BASE_URL=https://stale-backup.example",
		"DAEDALUS_FALLBACK_API_KEY=stale-backup",
		"DAEDALUS_FALLBACK_MODEL=stale-backup-model",
		"DAEDALUS_FALLBACK_HEARTBEAT_MODEL=stale-backup-air",
		"HOME=/Users/test",
	}
	out := envWithoutProviderVars(in)
	if len(out) != 2 || out[0] != "PATH=/usr/bin:/bin" || out[1] != "HOME=/Users/test" {
		t.Errorf("scrubbed env = %v, want only PATH and HOME", out)
	}
	// Every name the config can set must be scrubbed — a var added to
	// AgentEnv/ProviderEnvVars but not to the scrub list would leak again.
	scrubbed := map[string]bool{}
	for _, kv := range out {
		name, _, _ := strings.Cut(kv, "=")
		scrubbed[name] = true
	}
	for _, name := range config.ProviderEnvVars() {
		if scrubbed[name] {
			t.Errorf("provider var %s survived the scrub", name)
		}
	}
}

// providerServer stands in for a provider endpoint, answering status with
// body for every request.
func providerServer(t *testing.T, status int, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// writeProviderConfig writes a loadable config whose anthropic section
// points at url with key, plus the optional extra YAML (e.g. a fallback
// block), and returns its path.
func writeProviderConfig(t *testing.T, url, key, extra string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	doc := "agent: claude\ntemporal:\n  task_queue: q\nanthropic:\n  url: " + url + "\n  key: " + key + "\n  model: m\n" + extra
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// writeOpenAILegacyConfig writes a loadable config whose legacy openai
// section stands alone — no provider: section, no anthropic: section — the
// openai-only shape whose served channel the status probe must find.
func writeOpenAILegacyConfig(t *testing.T, url, key, extra string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	doc := "agent: claude\ntemporal:\n  task_queue: q\nopenai:\n  url: " + url + "\n  key: " + key + "\n  model: m\n" + extra
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// providerModelServer stands in for a healthy provider endpoint and
// records the model named in the probe request body.
func providerModelServer(t *testing.T) (url string, model *string) {
	t.Helper()
	m := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		m = b.Model
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &m
}

// TestProviderStatus pins the status column's provider reporting: a
// healthy main provider, the n/a and config-error placeholders, and the
// main→fallback handoff marking the fallback as active when the main is
// down — mirroring the worker's own failover choice.
func TestProviderStatus(t *testing.T) {
	t.Run("healthy main", func(t *testing.T) {
		cfg := writeProviderConfig(t, providerServer(t, 200, `{}`), "k", "")
		got := providerStatus(cfg)
		if !strings.Contains(got, "ok (http 200)") {
			t.Errorf("providerStatus = %q, want ok", got)
		}
	})

	t.Run("no provider configured", func(t *testing.T) {
		got := providerStatus(writeQueueConfig(t, "q"))
		if !strings.Contains(got, "n/a (provider not configured)") {
			t.Errorf("providerStatus = %q, want n/a", got)
		}
	})

	t.Run("no record", func(t *testing.T) {
		if got := providerStatus(""); !strings.Contains(got, "n/a (no config record)") {
			t.Errorf("providerStatus(\"\") = %q, want n/a", got)
		}
	})

	t.Run("unloadable config", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte("anthropic: [not a mapping"), 0o644); err != nil {
			t.Fatal(err)
		}
		if got := providerStatus(path); !strings.Contains(got, "config error") {
			t.Errorf("providerStatus(broken) = %q, want config error", got)
		}
	})

	t.Run("probe names the heartbeat model when configured", func(t *testing.T) {
		url, model := providerModelServer(t)
		cfg := writeProviderConfig(t, url, "k", "  heartbeat_model: hb-air\n")
		if got := providerStatus(cfg); !strings.Contains(got, "ok (http 200)") {
			t.Errorf("providerStatus = %q, want ok", got)
		}
		if *model != "hb-air" {
			t.Errorf("probe model = %q, want the configured heartbeat model", *model)
		}
	})

	t.Run("probe names the main model without a heartbeat model", func(t *testing.T) {
		url, model := providerModelServer(t)
		if got := providerStatus(writeProviderConfig(t, url, "k", "")); !strings.Contains(got, "ok (http 200)") {
			t.Errorf("providerStatus = %q, want ok", got)
		}
		if *model != "m" {
			t.Errorf("probe model = %q, want the main model", *model)
		}
	})

	t.Run("quota-exhausted main with live fallback reports the fallback active", func(t *testing.T) {
		main := providerServer(t, 429, `Usage limit reached for 5 hour. Your limit will reset at 2099-01-01 09:13:00`)
		fallback := providerServer(t, 200, `{}`)
		extra := "fallback:\n  enabled: true\n  url: " + fallback + "\n  key: fb\n  model: fbm\n"
		cfg := writeProviderConfig(t, main, "k", extra)
		got := providerStatus(cfg)
		for _, want := range []string{"main: 429 quota (resets", "fallback: ok (http 200)", "[active: fallback]"} {
			if !strings.Contains(got, want) {
				t.Errorf("providerStatus = %q, want it to contain %q", got, want)
			}
		}
	})

	t.Run("both down reports none active", func(t *testing.T) {
		main := providerServer(t, 429, `rate limited`)
		fallback := providerServer(t, 401, `{}`)
		extra := "fallback:\n  enabled: true\n  url: " + fallback + "\n  key: fb\n  model: fbm\n"
		cfg := writeProviderConfig(t, main, "k", extra)
		got := providerStatus(cfg)
		for _, want := range []string{"main: 429 rate limited", "fallback: 401 auth rejected", "[active: none]"} {
			if !strings.Contains(got, want) {
				t.Errorf("providerStatus = %q, want it to contain %q", got, want)
			}
		}
	})

	t.Run("openai-type fallback probes the chat-completions wire", func(t *testing.T) {
		main := providerServer(t, 429, `rate limited`)
		var path, auth string
		fb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path, auth = r.URL.Path, r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		t.Cleanup(fb.Close)
		extra := "fallback:\n  enabled: true\n  type: openai\n  url: " + fb.URL + "\n  key: fb\n  model: fbm\n"
		got := providerStatus(writeProviderConfig(t, main, "k", extra))
		if !strings.Contains(got, "fallback: ok (http 200)") || !strings.Contains(got, "[active: fallback]") {
			t.Errorf("providerStatus = %q, want the openai fallback reported active", got)
		}
		if path != "/chat/completions" {
			t.Errorf("fallback probe path = %q, want /chat/completions", path)
		}
		if auth != "Bearer fb" {
			t.Errorf("fallback probe auth = %q, want the Bearer scheme with the fallback key", auth)
		}
	})

	t.Run("default fallback probes the anthropic wire", func(t *testing.T) {
		main := providerServer(t, 429, `rate limited`)
		var path, key string
		fb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path, key = r.URL.Path, r.Header.Get("x-api-key")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		t.Cleanup(fb.Close)
		// No type field: the anthropic default must pick the probe's wire.
		extra := "fallback:\n  enabled: true\n  url: " + fb.URL + "\n  key: fb\n  model: fbm\n"
		if got := providerStatus(writeProviderConfig(t, main, "k", extra)); !strings.Contains(got, "fallback: ok (http 200)") {
			t.Errorf("providerStatus = %q, want the fallback reported ok", got)
		}
		if path != "/v1/messages" {
			t.Errorf("fallback probe path = %q, want /v1/messages", path)
		}
		if key != "fb" {
			t.Errorf("fallback probe x-api-key = %q, want the fallback key", key)
		}
	})

	t.Run("disabled fallback probes main only", func(t *testing.T) {
		main := providerServer(t, 429, `rate limited`)
		fallback := providerServer(t, 200, `{}`)
		extra := "fallback:\n  enabled: false\n  url: " + fallback + "\n  key: fb\n  model: fbm\n"
		cfg := writeProviderConfig(t, main, "k", extra)
		got := providerStatus(cfg)
		if !strings.Contains(got, "429 rate limited") || strings.Contains(got, "fallback") {
			t.Errorf("providerStatus = %q, want main's detail only", got)
		}
	})

	t.Run("openai-only config probes the openai wire", func(t *testing.T) {
		var path, auth, model string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path = r.URL.Path
			auth = r.Header.Get("Authorization")
			var b struct {
				Model string `json:"model"`
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			model = b.Model
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		t.Cleanup(srv.Close)
		// No anthropic section at all: the openai section is the served
		// channel, so reporting n/a here would call a working worker
		// unconfigured.
		if got := providerStatus(writeOpenAILegacyConfig(t, srv.URL, "k", "")); !strings.Contains(got, "ok (http 200)") {
			t.Errorf("providerStatus = %q, want ok", got)
		}
		if path != "/chat/completions" {
			t.Errorf("probe path = %q, want /chat/completions", path)
		}
		if auth != "Bearer k" {
			t.Errorf("probe auth = %q, want the Bearer scheme with the openai key", auth)
		}
		if model != "m" {
			t.Errorf("probe model = %q, want the openai section's model", model)
		}
	})

	t.Run("configured anthropic section is still the primary over the legacy openai section", func(t *testing.T) {
		var hits int
		open := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits++
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		t.Cleanup(open.Close)
		var path, key string
		anth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path, key = r.URL.Path, r.Header.Get("x-api-key")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		t.Cleanup(anth.Close)
		// Both legacy sections configured: the anthropic wire stays the
		// primary — the openai section is probed only when the anthropic
		// side is unset, never alongside it.
		extra := "openai:\n  url: " + open.URL + "\n  key: sk-oa\n  model: gpt-x\n"
		if got := providerStatus(writeProviderConfig(t, anth.URL, "k", extra)); !strings.Contains(got, "ok (http 200)") {
			t.Errorf("providerStatus = %q, want ok", got)
		}
		if path != "/v1/messages" || key != "k" {
			t.Errorf("probe = %q with x-api-key %q, want the anthropic wire with the anthropic key", path, key)
		}
		if hits != 0 {
			t.Errorf("openai endpoint hit %d times, want 0 (anthropic is the primary)", hits)
		}
	})

	t.Run("anthropic url without a key falls to the openai channel", func(t *testing.T) {
		var hits int
		anth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits++
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		t.Cleanup(anth.Close)
		open := providerServer(t, 200, `{}`)
		// A keyless anthropic endpoint cannot serve a round, so the openai
		// section is the probeable channel — mirroring the worker's own
		// unset-side choice.
		path := filepath.Join(t.TempDir(), "config.yaml")
		doc := "agent: claude\ntemporal:\n  task_queue: q\nanthropic:\n  url: " + anth.URL + "\n  model: claude-x\nopenai:\n  url: " + open + "\n  key: sk-oa\n  model: gpt-x\n"
		if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
			t.Fatal(err)
		}
		if got := providerStatus(path); !strings.Contains(got, "ok (http 200)") {
			t.Errorf("providerStatus = %q, want ok", got)
		}
		if hits != 0 {
			t.Errorf("keyless anthropic endpoint hit %d times, want 0 (the openai channel is the probeable one)", hits)
		}
	})

	t.Run("openai-only main with live fallback reports the fallback active", func(t *testing.T) {
		main := providerServer(t, 429, `rate limited`)
		fallback := providerServer(t, 200, `{}`)
		extra := "fallback:\n  enabled: true\n  url: " + fallback + "\n  key: fb\n  model: fbm\n"
		got := providerStatus(writeOpenAILegacyConfig(t, main, "k", extra))
		for _, want := range []string{"main: 429 rate limited", "fallback: ok (http 200)", "[active: fallback]"} {
			if !strings.Contains(got, want) {
				t.Errorf("providerStatus = %q, want it to contain %q", got, want)
			}
		}
	})
}

// writeProviderSectionConfig writes a loadable config whose provider:
// section — the wire-agnostic primary — is typed typ and points at url with
// key, plus the optional extra YAML (e.g. a fallback or legacy block), and
// returns its path.
func writeProviderSectionConfig(t *testing.T, typ, url, key, extra string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	doc := "agent: claude\ntemporal:\n  task_queue: q\nprovider:\n  type: " + typ + "\n  url: " + url + "\n  key: " + key + "\n  model: m\n" + extra
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestProviderStatusProviderSection pins the status probe's primary
// dispatch through the provider section: an anthropic-typed provider is
// probed on the anthropic wire (with its heartbeat model naming the probe),
// an openai-typed provider on the chat-completions wire — and the
// openai-typed provider is the primary even when a legacy anthropic section
// also stands, so an unconfigured provider section still reports n/a rather
// than probing the legacy channel. The fallback handoff keeps working with
// a provider-served primary.
func TestProviderStatusProviderSection(t *testing.T) {
	t.Run("anthropic-typed provider probes the anthropic wire", func(t *testing.T) {
		var path, key string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path, key = r.URL.Path, r.Header.Get("x-api-key")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		t.Cleanup(srv.Close)
		if got := providerStatus(writeProviderSectionConfig(t, "anthropic", srv.URL, "k", "")); !strings.Contains(got, "ok (http 200)") {
			t.Errorf("providerStatus = %q, want ok", got)
		}
		if path != "/v1/messages" {
			t.Errorf("probe path = %q, want /v1/messages", path)
		}
		if key != "k" {
			t.Errorf("probe x-api-key = %q, want the provider section's key", key)
		}
	})

	t.Run("provider heartbeat model names the probe", func(t *testing.T) {
		url, model := providerModelServer(t)
		cfg := writeProviderSectionConfig(t, "anthropic", url, "k", "  heartbeat_model: hb-air\n")
		if got := providerStatus(cfg); !strings.Contains(got, "ok (http 200)") {
			t.Errorf("providerStatus = %q, want ok", got)
		}
		if *model != "hb-air" {
			t.Errorf("probe model = %q, want the provider section's heartbeat model", *model)
		}
	})

	t.Run("openai-typed provider probes the chat-completions wire", func(t *testing.T) {
		var path, auth, model string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path = r.URL.Path
			auth = r.Header.Get("Authorization")
			var b struct {
				Model string `json:"model"`
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			model = b.Model
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		t.Cleanup(srv.Close)
		if got := providerStatus(writeProviderSectionConfig(t, "openai", srv.URL, "k", "")); !strings.Contains(got, "ok (http 200)") {
			t.Errorf("providerStatus = %q, want ok", got)
		}
		if path != "/chat/completions" {
			t.Errorf("probe path = %q, want /chat/completions", path)
		}
		if auth != "Bearer k" {
			t.Errorf("probe auth = %q, want the Bearer scheme with the provider section's key", auth)
		}
		if model != "m" {
			t.Errorf("probe model = %q, want the provider section's model (no heartbeat on the openai wire)", model)
		}
	})

	t.Run("openai-typed provider is the primary over a legacy anthropic section", func(t *testing.T) {
		var hits int
		legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits++
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		t.Cleanup(legacy.Close)
		open := providerServer(t, 200, `{}`)
		extra := "anthropic:\n  url: " + legacy.URL + "\n  key: sk\n  model: claude-x\n"
		if got := providerStatus(writeProviderSectionConfig(t, "openai", open, "k", extra)); !strings.Contains(got, "ok (http 200)") {
			t.Errorf("providerStatus = %q, want ok", got)
		}
		if hits != 0 {
			t.Errorf("legacy anthropic endpoint hit %d times, want 0 (the openai-typed provider is the primary)", hits)
		}
	})

	t.Run("provider section without url or key reports n/a", func(t *testing.T) {
		got := providerStatus(writeProviderSectionConfig(t, "anthropic", "", "k", ""))
		if !strings.Contains(got, "n/a (provider not configured)") {
			t.Errorf("providerStatus = %q, want n/a", got)
		}
	})

	t.Run("provider-served main with live fallback reports the fallback active", func(t *testing.T) {
		main := providerServer(t, 429, `rate limited`)
		fallback := providerServer(t, 200, `{}`)
		extra := "fallback:\n  enabled: true\n  url: " + fallback + "\n  key: fb\n  model: fbm\n"
		got := providerStatus(writeProviderSectionConfig(t, "openai", main, "k", extra))
		for _, want := range []string{"main: 429 rate limited", "fallback: ok (http 200)", "[active: fallback]"} {
			if !strings.Contains(got, want) {
				t.Errorf("providerStatus = %q, want it to contain %q", got, want)
			}
		}
	})
}

// TestWorkerStatusAllAPIColumn pins the table-level wiring: the header
// carries the API column and a recorded worker with a live provider shows
// its probe result in the row.
func TestWorkerStatusAllAPIColumn(t *testing.T) {
	dir := useDaemonDir(t)
	cfg := writeProviderConfig(t, providerServer(t, 200, `{}`), "k", "")
	writeRecord(t, "p", cfg)

	out := captureStdout(t, func() {
		if err := workerStatusAll(); err != nil {
			t.Errorf("workerStatusAll: %v", err)
		}
	})
	if !strings.Contains(out, "API") {
		t.Errorf("status output %q should carry the API column header", out)
	}
	if !strings.Contains(out, "ok (http 200)") {
		t.Errorf("status output %q should carry the probe result", out)
	}
	if !strings.Contains(out, filepath.Join(dir, "worker-p.log")) {
		t.Errorf("status output %q should still carry the log column", out)
	}
}
