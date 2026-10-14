package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
)

// useMetricsDaemonDir is useDaemonDir plus the metrics daemon's file
// seeding: the pid file names a live pid (this process) and statusDoc is
// published next to it, as runMetricsDaemon does — the cheapest way to
// fake a live metrics daemon for the record-driven reads.
func useMetricsDaemonDir(t *testing.T, statusDoc metricsStatusDoc) {
	t.Helper()
	useDaemonDir(t)
	pidFile, _, statusFile := metricsPaths()
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(statusDoc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statusFile, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// reapedChild runs a trivial child to completion and reaps it, returning
// its pid — a pid guaranteed to name no live process, without gambling on
// a numeric literal staying unallocated on the host.
func reapedChild(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	return cmd.Process.Pid
}

// TestIsMetricsRecordRead pins the record-driven invocation grammar: the
// three reads work from the daemon files alone (and tolerate a flag tail —
// list parses its own), while the actions that need a config do not
// qualify and neither does anything that is not the metrics subcommand.
// A -c-leading invocation is not the grammar either — flags.go applies
// this predicate to the positional tail after global flags are stripped.
func TestIsMetricsRecordRead(t *testing.T) {
	for _, c := range []struct {
		args []string
		want bool
	}{
		{[]string{"metrics", "status"}, true},
		{[]string{"metrics", "stop"}, true},
		{[]string{"metrics", "list"}, true},
		{[]string{"metrics", "list", "--worker", "w", "--page", "2"}, true},
		{[]string{"-c", "cfg.yaml", "metrics", "list"}, false},
		{[]string{"metrics", "start"}, false},
		{[]string{"metrics", "restart"}, false},
		{[]string{"metrics", "foreground"}, false},
		{[]string{"metrics"}, false},
		{[]string{"metrics", "frobnicate"}, false},
		{[]string{"worker", "status"}, false},
		{[]string{"report"}, false},
	} {
		if got := isMetricsRecordRead(c.args); got != c.want {
			t.Errorf("isMetricsRecordRead(%q) = %v, want %v", c.args, got, c.want)
		}
	}
}

// TestMetricsRelayEndpoint pins the live daemon's endpoint read: the relay
// address comes from the published status file and is trusted only when
// the pid file names a live process and the record agrees with it — a dead
// daemon's stale file, a pid mismatch, or a portless record is no endpoint.
func TestMetricsRelayEndpoint(t *testing.T) {
	t.Run("no daemon", func(t *testing.T) {
		useDaemonDir(t)
		if root, ok := metricsRelayEndpoint(); ok || root != "" {
			t.Errorf("metricsRelayEndpoint = (%q, %v), want none without a daemon", root, ok)
		}
	})

	t.Run("live daemon publishes its relay port", func(t *testing.T) {
		useMetricsDaemonDir(t, metricsStatusDoc{PID: os.Getpid(), Updated: time.Now().UTC(), RelayPort: 7778, UIPort: 7777})
		root, ok := metricsRelayEndpoint()
		if !ok || root != "http://127.0.0.1:7778" {
			t.Errorf("metricsRelayEndpoint = (%q, %v), want the published relay port", root, ok)
		}
	})

	t.Run("a record from another pid is not the daemon's", func(t *testing.T) {
		useMetricsDaemonDir(t, metricsStatusDoc{PID: os.Getpid() + 1_000_000, Updated: time.Now().UTC(), RelayPort: 7778})
		if _, ok := metricsRelayEndpoint(); ok {
			t.Error("endpoint accepted a status file written by another pid")
		}
	})

	t.Run("a portless record is no endpoint", func(t *testing.T) {
		useMetricsDaemonDir(t, metricsStatusDoc{PID: os.Getpid(), Updated: time.Now().UTC()})
		if _, ok := metricsRelayEndpoint(); ok {
			t.Error("endpoint accepted a record without a relay port")
		}
	})
}

// TestMetricsStatus pins the record-driven status: a live daemon prints its
// pid and, when its published record agrees, its two ports and version; a
// stale or missing pid file reads as "not running" and clears the stale
// record.
func TestMetricsStatus(t *testing.T) {
	t.Run("live daemon with its record", func(t *testing.T) {
		useMetricsDaemonDir(t, metricsStatusDoc{
			PID: os.Getpid(), Updated: time.Now().UTC(), Version: "test", UIPort: 7777, RelayPort: 7778,
		})
		out := captureStdout(t, func() {
			if err := metricsStatus(); err != nil {
				t.Errorf("metricsStatus: %v", err)
			}
		})
		for _, want := range []string{
			"metrics daemon running (pid " + strconv.Itoa(os.Getpid()) + ")",
			"relay:     127.0.0.1:7778",
			"dashboard: :7777",
			"version:   test",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("status output %q should contain %q", out, want)
			}
		}
	})

	t.Run("record from another pid is ignored", func(t *testing.T) {
		useMetricsDaemonDir(t, metricsStatusDoc{PID: os.Getpid() + 1_000_000, Updated: time.Now().UTC(), RelayPort: 7778})
		out := captureStdout(t, func() {
			if err := metricsStatus(); err != nil {
				t.Errorf("metricsStatus: %v", err)
			}
		})
		if !strings.Contains(out, "metrics daemon running") {
			t.Errorf("status output %q should name the running pid", out)
		}
		if strings.Contains(out, "127.0.0.1:7778") {
			t.Errorf("status output %q served another pid's record", out)
		}
	})

	t.Run("dead pid reads as not running and clears the file", func(t *testing.T) {
		useDaemonDir(t)
		pidFile, _, _ := metricsPaths()
		if err := os.WriteFile(pidFile, []byte(strconv.Itoa(reapedChild(t))), 0o644); err != nil {
			t.Fatal(err)
		}
		out := captureStdout(t, func() {
			if err := metricsStatus(); err != nil {
				t.Errorf("metricsStatus: %v", err)
			}
		})
		if !strings.Contains(out, "metrics daemon not running") {
			t.Errorf("status output %q, want the not-running line", out)
		}
		if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
			t.Errorf("stale pid file survived the status read: %v", err)
		}
	})
}

// TestMetricsStop pins the stop path: a live daemon is signaled and its pid
// file cleared; with nothing live the stale record is cleared and the
// not-running line printed — never an error.
func TestMetricsStop(t *testing.T) {
	t.Run("live daemon stops and clears its pid file", func(t *testing.T) {
		useDaemonDir(t)
		// A real, killable process stands in for the daemon: a child that
		// sleeps until the SIGTERM lands.
		cmd := exec.Command("sh", "-c", "sleep 30")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cmd.Wait() }) // reap, whatever the assertions did
		pidFile, _, _ := metricsPaths()
		if err := os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644); err != nil {
			t.Fatal(err)
		}

		out := captureStdout(t, func() {
			if err := metricsStop(); err != nil {
				t.Errorf("metricsStop: %v", err)
			}
		})
		if !strings.Contains(out, "metrics daemon stopped (pid "+strconv.Itoa(cmd.Process.Pid)+")") {
			t.Errorf("stop output %q, want the stopped line", out)
		}
		if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
			t.Errorf("pid file survived the stop: %v", err)
		}
	})

	t.Run("nothing running", func(t *testing.T) {
		useDaemonDir(t)
		pidFile, _, _ := metricsPaths()
		if err := os.WriteFile(pidFile, []byte(strconv.Itoa(reapedChild(t))), 0o644); err != nil {
			t.Fatal(err)
		}
		out := captureStdout(t, func() {
			if err := metricsStop(); err != nil {
				t.Errorf("metricsStop: %v", err)
			}
		})
		if !strings.Contains(out, "metrics daemon not running") {
			t.Errorf("stop output %q, want the not-running line", out)
		}
		if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
			t.Errorf("stale pid file survived the stop: %v", err)
		}
	})
}

// TestMetricsGateCheck pins the boot gate: a metrics-enabled worker
// requires the host daemon's relay listener, and an unreachable one fails
// the start naming the port and the remedy — while a listening relay
// passes.
func TestMetricsGateCheck(t *testing.T) {
	t.Run("relay reachable passes", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		cfg := config.Config{Metrics: config.MetricsConfig{RelayPort: ln.Addr().(*net.TCPAddr).Port}}
		if err := metricsGateCheck(cfg); err != nil {
			t.Errorf("metricsGateCheck against a listening relay: %v", err)
		}
	})

	t.Run("relay unreachable fails with the remedy", func(t *testing.T) {
		// Take a port and give it back — nothing listens there now.
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		ln.Close()

		cfg := config.Config{Metrics: config.MetricsConfig{RelayPort: port}}
		err = metricsGateCheck(cfg)
		if err == nil {
			t.Fatal("metricsGateCheck against a dead relay: nil, want the boot gate")
		}
		for _, want := range []string{
			"metrics enabled but the metrics daemon is not reachable on 127.0.0.1:" + strconv.Itoa(port),
			"daedalus metrics start",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("gate error %q should contain %q", err, want)
			}
		}
	})
}

// TestMetricsRegister pins the worker's registration with the daemon: the
// upstream mapping is read from this process's environment (the same
// values per-round staging reads, so what the relay restores is exactly
// what staging displaced), the returned token is exported onto the token
// env, a config with no provider base URL registers nothing and stays
// successful, and a refusal (or an empty token — a pre-token daemon) fails
// with the remedy named.
func TestMetricsRegister(t *testing.T) {
	for _, k := range []string{"ANTHROPIC_BASE_URL", "ANTHROPIC_API_KEY", "OPENAI_BASE_URL", "OPENAI_API_BASE", "OPENAI_API_KEY"} {
		t.Setenv(k, "") // restoreProcessEnv-style scrub: no ambient provider env on the host leaks in
	}
	type registerCase struct {
		name       string
		env        map[string]string
		wantWorker string
		wantQueue  string
		wantUp     map[string]string
		wantKeys   map[string]string
	}
	for _, c := range []registerCase{
		{
			name: "both wires from the environment",
			env: map[string]string{
				"ANTHROPIC_BASE_URL": "https://a.example",
				"ANTHROPIC_API_KEY":  "sk-a",
				"OPENAI_BASE_URL":    "https://o.example/v1",
				"OPENAI_API_KEY":     "sk-o",
			},
			wantWorker: "w1", wantQueue: "q7",
			wantUp:   map[string]string{"anthropic": "https://a.example", "openai": "https://o.example/v1"},
			wantKeys: map[string]string{"anthropic": "sk-a", "openai": "sk-o"},
		},
		{
			name: "legacy API_BASE names the openai upstream",
			env: map[string]string{
				"OPENAI_API_BASE": "https://legacy.example/v1",
				"OPENAI_API_KEY":  "sk-legacy",
			},
			wantWorker: "w1", wantQueue: "q7",
			wantUp:   map[string]string{"openai": "https://legacy.example/v1"},
			wantKeys: map[string]string{"openai": "sk-legacy"},
		},
		{
			name: "keyless wires register keyless",
			env: map[string]string{
				"ANTHROPIC_BASE_URL": "https://a.example",
			},
			wantWorker: "w1", wantQueue: "q7",
			wantUp:   map[string]string{"anthropic": "https://a.example"},
			wantKeys: map[string]string{},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			t.Setenv(config.MetricsRelayTokenEnv, "sentinel")

			var gotBody map[string]any
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
					t.Errorf("decode registration body: %v", err)
				}
				// 204, no body: the secret is the worker's own — presented,
				// stored, never echoed.
				w.WriteHeader(http.StatusNoContent)
			}))
			defer srv.Close()

			cfg := config.Config{WorkerID: c.wantWorker, Temporal: config.TemporalConfig{TaskQueue: c.wantQueue}}
			if err := metricsRegister(cfg, srv.URL, "worker-minted-tok"); err != nil {
				t.Fatalf("metricsRegister: %v", err)
			}
			if got := os.Getenv(config.MetricsRelayTokenEnv); got != "worker-minted-tok" {
				t.Errorf("%s = %q, want the worker's own mint exported", config.MetricsRelayTokenEnv, got)
			}
			if got, want := gotBody["worker"], c.wantWorker; got != want {
				t.Errorf("registration worker = %v, want %q", got, want)
			}
			if got, want := gotBody["queue"], c.wantQueue; got != want {
				t.Errorf("registration queue = %v, want %q", got, want)
			}
			if got, want := gotBody["token"], "worker-minted-tok"; got != want {
				t.Errorf("registration token = %v, want the presented secret carried in the body", got)
			}
			upstreams, _ := gotBody["upstreams"].(map[string]any)
			if len(upstreams) != len(c.wantUp) {
				t.Errorf("registration upstreams = %v, want %v", upstreams, c.wantUp)
			}
			for wire, raw := range c.wantUp {
				if got := upstreams[wire]; got != raw {
					t.Errorf("registration upstream %s = %v, want %q", wire, got, raw)
				}
			}
			keys, _ := gotBody["keys"].(map[string]any)
			if len(keys) != len(c.wantKeys) {
				t.Errorf("registration keys = %v, want %v", keys, c.wantKeys)
			}
			for wire, key := range c.wantKeys {
				if got := keys[wire]; got != key {
					t.Errorf("registration key %s = %v, want %q", wire, got, key)
				}
			}
		})
	}

	t.Run("no provider base URL registers nothing", func(t *testing.T) {
		t.Setenv(config.MetricsRelayTokenEnv, "sentinel")
		out := captureStdout(t, func() {
			if err := metricsRegister(config.Config{WorkerID: "w1"}, "http://127.0.0.1:1", "worker-minted-tok"); err != nil {
				t.Errorf("metricsRegister: %v", err)
			}
		})
		if !strings.Contains(out, "nothing registered") {
			t.Errorf("output %q, want the coverage-limit note", out)
		}
		if got := os.Getenv(config.MetricsRelayTokenEnv); got != "sentinel" {
			t.Errorf("%s = %q, want the sentinel untouched (nothing was registered)", config.MetricsRelayTokenEnv, got)
		}
	})

	t.Run("a refusal fails the registration", func(t *testing.T) {
		t.Setenv("ANTHROPIC_BASE_URL", "https://a.example")
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "metricsrelay: registration needs worker and at least one upstream", http.StatusBadRequest)
		}))
		defer srv.Close()
		err := metricsRegister(config.Config{WorkerID: "w1"}, srv.URL, "worker-minted-tok")
		if err == nil || !strings.Contains(err.Error(), "registration refused") {
			t.Errorf("metricsRegister = %v, want the refusal surfaced", err)
		}
	})

	t.Run("a server-minted token face names the version skew", func(t *testing.T) {
		t.Setenv("ANTHROPIC_BASE_URL", "https://a.example")
		// A daemon older than the token handshake mints server-side and
		// echoes the secret in a 200 body — refused: this worker's staged
		// rounds would present a token that daemon never stored.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"token":"server-minted"}`))
		}))
		defer srv.Close()
		err := metricsRegister(config.Config{WorkerID: "w1"}, srv.URL, "worker-minted-tok")
		if err == nil || !strings.Contains(err.Error(), "older than this worker") ||
			!strings.Contains(err.Error(), "daedalus metrics restart") {
			t.Errorf("metricsRegister = %v, want the server-minted daemon named with the restart remedy", err)
		}
		if got := os.Getenv(config.MetricsRelayTokenEnv); got != "" {
			t.Errorf("%s = %q after a refused registration, want nothing exported", config.MetricsRelayTokenEnv, got)
		}
	})

	t.Run("a live name collision names the TTL remedy", func(t *testing.T) {
		t.Setenv("ANTHROPIC_BASE_URL", "https://a.example")
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "metricsrelay: worker \"w1\" is already registered", http.StatusConflict)
		}))
		defer srv.Close()
		err := metricsRegister(config.Config{WorkerID: "w1"}, srv.URL, "worker-minted-tok")
		if err == nil || !strings.Contains(err.Error(), "registration refused") ||
			!strings.Contains(err.Error(), "registration TTL") {
			t.Errorf("metricsRegister = %v, want the collision surfaced with the TTL remedy", err)
		}
	})

	t.Run("an unreachable daemon fails", func(t *testing.T) {
		t.Setenv("ANTHROPIC_BASE_URL", "https://a.example")
		if err := metricsRegister(config.Config{WorkerID: "w1"}, "http://127.0.0.1:1", "worker-minted-tok"); err == nil {
			t.Error("metricsRegister against a dead daemon: nil, want the dial error")
		}
	})
}

// TestRunMetricsList pins the CLI's ledger read: the endpoint comes from
// the daemon's published record (no config involved), the flags travel
// into the query, --json prints the page as the daemon sent it, and the
// table is the default. Every malformed flag fails naming the fix, and no
// live daemon is a start-it-first error.
func TestRunMetricsList(t *testing.T) {
	ledgerDoc := func(entries ...metricsrelay.Entry) []byte {
		t.Helper()
		data, err := json.Marshal(map[string]any{
			"total": len(entries), "page": 1, "page_size": 50, "entries": entries,
		})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	entry := metricsrelay.Entry{
		At: time.Now(), Worker: "w1", Queue: "q7",
		Wire: "openai", Task: "task-1", Role: "dev", Round: "3", Model: "glm-4",
		InputTokens: 10, OutputTokens: 5, CacheReadTokens: 2, DurationMS: 4200, Status: 200,
	}

	newDaemon := func(t *testing.T, doc []byte) *string {
		t.Helper()
		gotQuery := new(string) // the handler writes through it; requests land after this returns
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			*gotQuery = r.URL.RawQuery
			w.Header().Set("Content-Type", "application/json")
			w.Write(doc)
		}))
		t.Cleanup(srv.Close)
		useMetricsDaemonDir(t, metricsStatusDoc{
			PID: os.Getpid(), Updated: time.Now().UTC(),
			RelayPort: srv.Listener.Addr().(*net.TCPAddr).Port,
		})
		return gotQuery
	}

	t.Run("json output echoes the daemon's page", func(t *testing.T) {
		newDaemon(t, ledgerDoc(entry))
		out := captureStdout(t, func() {
			if err := runMetricsList([]string{"--json"}); err != nil {
				t.Errorf("runMetricsList: %v", err)
			}
		})
		var doc struct {
			Total    int                  `json:"total"`
			Page     int                  `json:"page"`
			PageSize int                  `json:"page_size"`
			Entries  []metricsrelay.Entry `json:"entries"`
		}
		if err := json.Unmarshal([]byte(out), &doc); err != nil {
			t.Fatalf("json output %q: %v", out, err)
		}
		if doc.Total != 1 || len(doc.Entries) != 1 {
			t.Fatalf("json doc = %+v, want the one entry", doc)
		}
		e := doc.Entries[0]
		if e.Worker != "w1" || e.Task != "task-1" || e.Role != "dev" || e.Round != "3" ||
			e.Model != "glm-4" || e.InputTokens != 10 || e.OutputTokens != 5 ||
			e.CacheReadTokens != 2 || e.DurationMS != 4200 || e.Status != 200 {
			t.Errorf("entry = %+v, want the daemon's entry round-tripped", e)
		}
	})

	t.Run("table is the default and renders the row", func(t *testing.T) {
		newDaemon(t, ledgerDoc(entry))
		out := captureStdout(t, func() {
			if err := runMetricsList(nil); err != nil {
				t.Errorf("runMetricsList: %v", err)
			}
		})
		for _, want := range []string{
			"METRICS LEDGER — 1 entry, page 1 (page size 50)",
			"task-1", "dev", "glm-4", "4200", "200",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("table output %q should contain %q", out, want)
			}
		}
	})

	t.Run("flags travel into the query", func(t *testing.T) {
		for _, c := range []struct {
			args []string
			want string
		}{
			{[]string{"--worker", "w1", "--page", "2"}, "worker=w1&page=2"},
			{[]string{"--worker=w2"}, "worker=w2&page=1"},
			{[]string{"--page=3", "--table"}, "worker=&page=3"},
		} {
			gotQuery := newDaemon(t, ledgerDoc())
			if err := runMetricsList(c.args); err != nil {
				t.Errorf("runMetricsList(%v): %v", c.args, err)
				continue
			}
			if gotQuery == nil || *gotQuery != c.want {
				t.Errorf("runMetricsList(%v) query = %v, want %q", c.args, gotQuery, c.want)
			}
		}
	})

	t.Run("malformed flags fail naming the fix", func(t *testing.T) {
		for _, c := range []struct {
			args    []string
			wantErr string
		}{
			{[]string{"--worker"}, "--worker requires a value"},
			{[]string{"--page"}, "--page requires a value"},
			{[]string{"--page", "0"}, "wants a positive page number"},
			{[]string{"--page=abc"}, "wants a positive page number"},
			{[]string{"--csv"}, `unknown metrics list flag "--csv"`},
		} {
			if err := runMetricsList(c.args); err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("runMetricsList(%v) = %v, want it to contain %q", c.args, err, c.wantErr)
			}
		}
	})

	t.Run("no live daemon is a start-it-first error", func(t *testing.T) {
		useDaemonDir(t)
		err := runMetricsList(nil)
		if err == nil || !strings.Contains(err.Error(), "the metrics daemon is not running") ||
			!strings.Contains(err.Error(), "daedalus metrics start") {
			t.Errorf("runMetricsList without a daemon = %v, want the remedy named", err)
		}
	})

	t.Run("a failing ledger query surfaces the daemon's status", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}))
		defer srv.Close()
		port := srv.Listener.Addr().(*net.TCPAddr).Port
		useMetricsDaemonDir(t, metricsStatusDoc{PID: os.Getpid(), Updated: time.Now().UTC(), RelayPort: port})
		err := runMetricsList(nil)
		if err == nil || !strings.Contains(err.Error(), "ledger query failed") {
			t.Errorf("runMetricsList against a failing daemon = %v, want the status surfaced", err)
		}
	})
}

// TestFlagsRejectConfigOnMetricsRecordRead pins the -c gate: the
// record-driven metrics reads work from the daemon files alone, so an
// explicit config there is rejected rather than silently ignored — its own
// wording, since the worker-status message is pinned verbatim elsewhere.
func TestFlagsRejectConfigOnMetricsRecordRead(t *testing.T) {
	const rejection = "-c/--config does not apply to metrics status, stop, or list"
	for _, args := range [][]string{
		{"-c", "cfg.yaml", "metrics", "status"},
		{"metrics", "stop", "--config=cfg.yaml"},
		{"--config=cfg.yaml", "metrics", "list", "--json"},
	} {
		if _, _, err := parseFlags(args); err == nil || err.Error() != rejection {
			t.Errorf("parseFlags(%v) err = %v, want %q", args, err, rejection)
		}
	}
	// The actions that need a config keep taking one.
	if _, _, err := parseFlags([]string{"-c", "cfg.yaml", "metrics", "start"}); err != nil {
		t.Errorf("parseFlags(metrics start with -c): %v", err)
	}
}

// TestCompleteMetricsActions pins the metrics action completion: every
// action on the action position, nothing after one is typed.
func TestCompleteMetricsActions(t *testing.T) {
	if got := complete([]string{"metrics", ""}); !reflect.DeepEqual(got,
		[]string{"foreground", "list", "restart", "start", "status", "stop"}) {
		t.Errorf("complete(metrics \"\") = %q, want every action (sorted, like every completion)", got)
	}
	if got := complete([]string{"metrics", "st"}); !reflect.DeepEqual(got, []string{"start", "status", "stop"}) {
		t.Errorf("complete(metrics st) = %q, want the st-prefixed actions", got)
	}
	if got := complete([]string{"metrics", "list", ""}); got != nil {
		t.Errorf("complete(metrics list \"\") = %q, want nothing after an action", got)
	}
}

// TestMetricsStartReexecArgv pins the daemon spawn: `metrics start`
// re-executes `metrics foreground -c <path>` — the ports re-read from disk
// on every restart — writes the child's pid file, and reports the ports it
// will serve. The observable argv is the child's real one, printed into
// the daemon log by the reexecDumpArgsEnv branch of TestMain (the child is
// this test binary; its stdout is the daemon log).
func TestMetricsStartReexecArgv(t *testing.T) {
	useDaemonDir(t)
	t.Setenv(reexecDumpArgsEnv, "1")
	cfgPath := writeQueueConfig(t, "daedalus")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t, func() {
		if err := metricsStart(cfg, cfgPath); err != nil {
			t.Errorf("metricsStart: %v", err)
		}
	})
	if !strings.Contains(out, "metrics daemon started") {
		t.Fatalf("start output %q should report a started daemon", out)
	}
	for _, want := range []string{
		"relay:     127.0.0.1:" + strconv.Itoa(config.DefaultMetricsRelayPort),
		"dashboard: :" + strconv.Itoa(config.DefaultMetricsUIPort),
	} {
		if !strings.Contains(out, want) {
			t.Errorf("start output %q should contain %q", out, want)
		}
	}

	pidFile, logFile, _ := metricsPaths()
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("read pid file: %v", err)
	}
	if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err != nil || pid <= 0 {
		t.Errorf("pid file = %q, want a positive pid", data)
	}
	argv := daemonArgv(t, logFile)
	want := []string{"metrics", "foreground", "-c", cfgPath}
	if !reflect.DeepEqual(argv, want) {
		t.Errorf("daemon argv = %v, want %v", argv, want)
	}
}

// TestMainMetricsDispatch pins the dispatch-level contracts through the
// real main(): the record-driven reads dispatch with no config resolvable
// anywhere (the config lookup is bypassed for them — a regression there
// exits the test process on the config diagnostic, which fails the run),
// bare `metrics` is start with the spawned child defused, and the grammar
// failures exit 1 with their own diagnostics in a subprocess — each path
// exiting before any daemon file is touched, so the host's real daemon
// dir (/tmp/daedalus, what a child resolves) is never involved.
func TestMainMetricsDispatch(t *testing.T) {
	t.Run("status and stop need no config", func(t *testing.T) {
		useDaemonDir(t)
		t.Setenv("HOME", t.TempDir()) // no ~/.config/daedalus/config.yaml fallback
		t.Chdir(t.TempDir())          // no ./config.yaml

		for _, args := range [][]string{{"metrics", "status"}, {"metrics", "stop"}} {
			swap := os.Args
			os.Args = append([]string{"daedalus"}, args...)
			out := captureStdout(t, main)
			os.Args = swap
			if !strings.Contains(out, "metrics daemon not running") {
				t.Errorf("daedalus %v stdout = %q, want the not-running line", args, out)
			}
		}
	})

	t.Run("list dispatches record-driven off the daemon's published port", func(t *testing.T) {
		var gotQuery *string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.RawQuery
			gotQuery = &q
			w.Write([]byte(`{"total":0,"page":1,"page_size":50,"entries":[]}`))
		}))
		defer srv.Close()
		useMetricsDaemonDir(t, metricsStatusDoc{
			PID: os.Getpid(), Updated: time.Now().UTC(),
			RelayPort: srv.Listener.Addr().(*net.TCPAddr).Port,
		})
		t.Setenv("HOME", t.TempDir())
		t.Chdir(t.TempDir())

		swap := os.Args
		os.Args = []string{"daedalus", "metrics", "list", "--worker", "w1"}
		out := captureStdout(t, main)
		os.Args = swap

		if !strings.Contains(out, "METRICS LEDGER — 0 entries, page 1") {
			t.Errorf("dispatch output %q, want the ledger table (no config diagnostic — list is record-driven)", out)
		}
		if gotQuery == nil || *gotQuery != "worker=w1&page=1" {
			t.Errorf("dispatch query = %v, want worker=w1&page=1", gotQuery)
		}
	})

	t.Run("bare metrics is start, spawned child defused", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		t.Setenv("HOME", t.TempDir())
		cfgPath := writeQueueConfig(t, "daedalus")
		t.Chdir(filepath.Dir(cfgPath)) // ./config.yaml resolves

		swap := os.Args
		os.Args = []string{"daedalus", "metrics"}
		out := captureStdout(t, main)
		os.Args = swap

		if !strings.Contains(out, "metrics daemon started") {
			t.Errorf("bare metrics output %q, want the start default", out)
		}
		pidFile, _, _ := metricsPaths()
		data, err := os.ReadFile(pidFile)
		if err != nil {
			t.Fatalf("read pid file: %v", err)
		}
		if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err != nil || pid <= 0 {
			t.Errorf("pid file = %q, want a positive pid", data)
		}
	})

	t.Run("grammar failures exit 1 with their diagnostics", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		cfgPath := writeQueueConfig(t, "daedalus")
		dir := filepath.Dir(cfgPath) // the child's ./config.yaml
		for _, c := range []struct {
			args    []string
			wantErr string
		}{
			{[]string{"metrics", "frobnicate"}, "unknown metrics action"},
			{[]string{"metrics", "start", "extra"}, "metrics start takes no arguments"},
			{[]string{"metrics", "stop", "extra"}, "metrics stop takes no arguments"},
			{[]string{"metrics", "list", "--csv"}, "unknown metrics list flag"},
		} {
			_, stderr, code := runMainIn(t, dir, c.args...)
			if code != 1 {
				t.Errorf("daedalus %v exit = %d, want 1", c.args, code)
			}
			if !strings.Contains(stderr, c.wantErr) {
				t.Errorf("daedalus %v stderr = %q, want it to contain %q", c.args, stderr, c.wantErr)
			}
		}
	})

	t.Run("no config anywhere fails the config-needing default", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir()) // no ~/.config/daedalus/config.yaml fallback — the subtest's premise, established not assumed
		_, stderr, code := runMainIn(t, t.TempDir(), "metrics")
		if code != 1 {
			t.Errorf("daedalus metrics with no config exit = %d, want 1", code)
		}
		if !strings.Contains(stderr, "no config found") {
			t.Errorf("stderr = %q, want the config resolution failure (bare metrics is start, which needs one)", stderr)
		}
	})
}

// TestRunWorkerMetricsEnvExports pins the worker's metrics env channel:
// with metrics on, the relay roots are exported for per-round staging and
// the worker registers with the daemon (the token lands on the token env);
// with metrics off, a stale export is cleared — a worker restarted without
// metrics must not keep staging rounds onto a relay. The gate runs before
// anything is exported: an unreachable daemon fails the start with the
// remedy and leaves the env untouched. runWorker is driven to its first
// failure (an empty task queue fails worktree preflight before any
// Temporal dial) purely to observe the exports.
func TestRunWorkerMetricsEnvExports(t *testing.T) {
	restoreProcessEnv(t, "DAEDALUS_SLIM", "DAEDALUS_THINKING", "DAEDALUS_STREAM", "DAEDALUS_AGENT",
		"DAEDALUS_MAX_CONCURRENT_AGENT_RUNS", "DAEDALUS_MAX_CONCURRENT_TESTS", "DAEDALUS_WORKER_NAME",
		"API_TIMEOUT_MS", config.ToolRelayURLEnv, config.BugDirEnv,
		config.MetricsRelayAnthropicEnv, config.MetricsRelayOpenAIEnv, config.MetricsRelayTokenEnv,
		"ANTHROPIC_BASE_URL", "ANTHROPIC_API_KEY",
		"OPENAI_BASE_URL", "OPENAI_API_BASE", "OPENAI_API_KEY", "OPENAI_MODEL")

	// A stale export must not outlive a config that says off — the token
	// env included: it is scrubbed from the daemon spawn and unset with
	// the roots, so a worker restarted without metrics carries no secret
	// from a registration that no longer exists.
	t.Setenv(config.MetricsRelayAnthropicEnv, "http://127.0.0.1:1")
	t.Setenv(config.MetricsRelayOpenAIEnv, "http://127.0.0.1:1")
	t.Setenv(config.MetricsRelayTokenEnv, "stale-tok")
	if err := runWorker(config.Config{}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	for _, name := range []string{config.MetricsRelayAnthropicEnv, config.MetricsRelayOpenAIEnv, config.MetricsRelayTokenEnv} {
		if got := os.Getenv(name); got != "" {
			t.Errorf("%s = %q after a metrics-off config, want the stale export cleared", name, got)
		}
	}

	// An unreachable daemon fails the start with the remedy before any
	// export.
	err := runWorker(config.Config{Metrics: config.MetricsConfig{Enabled: true, RelayPort: 1}}, "")
	if err == nil || !strings.Contains(err.Error(), "daedalus metrics start") {
		t.Fatalf("runWorker with metrics on a dead daemon = %v, want the boot gate", err)
	}
	if got := os.Getenv(config.MetricsRelayAnthropicEnv); got != "" {
		t.Errorf("%s = %q after a failed gate, want nothing staged", config.MetricsRelayAnthropicEnv, got)
	}

	// With the daemon reachable, the roots are exported and the
	// registration's token lands on the token env.
	var registered map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&registered); err != nil {
			t.Errorf("decode registration: %v", err)
		}
		w.WriteHeader(http.StatusNoContent) // no body: the secret is the worker's own
	}))
	defer srv.Close()
	port := srv.Listener.Addr().(*net.TCPAddr).Port

	t.Setenv("ANTHROPIC_BASE_URL", "https://a.example")
	t.Setenv("ANTHROPIC_API_KEY", "sk-a")
	if err := runWorker(config.Config{
		WorkerID: "w-metrics",
		Metrics:  config.MetricsConfig{Enabled: true, RelayPort: port},
	}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}

	wantRoot := "http://127.0.0.1:" + strconv.Itoa(port)
	for _, name := range []string{config.MetricsRelayAnthropicEnv, config.MetricsRelayOpenAIEnv} {
		if got := os.Getenv(name); got != wantRoot {
			t.Errorf("%s = %q, want the live daemon's relay root %q", name, got, wantRoot)
		}
	}
	// The one-mint contract: what the worker registered with — its own
	// secret, carried in the body — is exactly what it exported for the
	// per-round staging to append to the attribution keys. The daemon
	// never hands a secret back, so any mismatch would stage rounds under
	// a token the relay never stored.
	if got, want := registered["token"], os.Getenv(config.MetricsRelayTokenEnv); got == nil || got != want || want == "" {
		t.Errorf("registered token %v vs exported %s, want the same non-empty worker mint on both channels", got, want)
	}
	if registered["worker"] != "w-metrics" {
		t.Errorf("registered worker = %v, want w-metrics (the worker's own name)", registered["worker"])
	}

	// The slim relay chains onto the metrics relay when both are on: its
	// staged URL carries the whole staged prefix, so pi → slim relay →
	// metrics relay → upstream.
	if err := runWorker(config.Config{
		WorkerID: "w-metrics",
		Metrics:  config.MetricsConfig{Enabled: true, RelayPort: port},
		Slim:     config.SlimConfig{Enabled: true, ParserModel: "qwen2.5-coder:3b-parser"},
		OpenAI:   config.OpenAIConfig{URL: "https://o.example/v1"},
	}, ""); err == nil {
		t.Fatal("runWorker with an empty task queue should fail worktree preflight, got nil")
	}
	staged, err := url.Parse(os.Getenv(config.ToolRelayURLEnv))
	if err != nil {
		t.Fatalf("parse staged %s: %v", config.ToolRelayURLEnv, err)
	}
	if staged.Path != "/relay/w-metrics/openai/v1" {
		t.Errorf("tool relay URL path = %q, want the metrics relay's staged prefix (pi → slim relay → metrics relay)", staged.Path)
	}
}
