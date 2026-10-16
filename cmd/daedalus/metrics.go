// The `daedalus metrics` daemon: the host-global host of the metrics relay
// (internal/metricsrelay) and the dashboard surface. Lifecycle mirrors the
// worker daemon's — pid/log/status files in the same daemon dir, keyed
// "metrics-*" so the worker roster's worker-* globs never match them. Its
// serving config (ports) is read from the host context at start time
// (Q15: global daemon, per-project switch). Two listeners (Q17): the relay
// bound to 127.0.0.1 on metrics.relay_port — physically unreachable
// off-host — and the UI mux on metrics.port bound to the LAN with no auth
// (an accepted posture: any LAN peer can read all run data and start
// runs). The dashboard views (ui.go — htmx-ui pt2) serve the decided
// route map (Q14) off that mux; /metrics is GoLens's mount, unbuilt here
// (the golens-metrics module is absent from this build), so it renders
// the daemon's own aggregates — the relay ledger's counters and the live
// session counts — where the embed's charts would, and says so.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
	"github.com/ozzono/daedalus/internal/version"
)

// metricsRegistrationTTL is how long a worker's registration serves staged
// paths past its last refresh; metricsHeartbeatInterval re-registers well
// inside it, so an expired entry means a dead worker.
const (
	metricsRegistrationTTL   = time.Minute
	metricsHeartbeatInterval = 15 * time.Second
)

// metricsHTTP bounds every CLI-side call into the daemon — the ledger read
// and the worker's registration/heartbeat, both on the loopback listener.
// The client default (no timeout) would hang the worker boot or the CLI
// indefinitely on a daemon that accepts but never answers.
var metricsHTTP = &http.Client{Timeout: 10 * time.Second}

// metricsActions lists the `metrics` subcommands, in the order the unknown-
// action diagnostic names them — shared by that diagnostic and the shell
// completion's candidates, like workerActions.
var metricsActions = []string{"start", "stop", "restart", "status", "list", "foreground"}

// metricsPaths returns the daemon's runtime file paths, keyed "metrics-"
// (the worker files' "worker-" prefix is glob-matched by the worker roster
// and the pid sweep, which must never see the metrics daemon).
func metricsPaths() (pidFile, logFile, statusFile string) {
	return filepath.Join(daemonDir, "metrics.pid"),
		filepath.Join(daemonDir, "metrics.log"),
		filepath.Join(daemonDir, "metrics.status")
}

// isMetricsRecordRead reports whether args spell one of the record-driven
// metrics invocations — `metrics status`, `metrics stop`, `metrics list`:
// they read the daemon's pid file alone and must not depend on whatever the
// invoking directory resolves to (isWorkerStatus's counterpart). list is
// among them because the ledger lives in the daemon process; its endpoint
// is the daemon's published port, never this invocation's config. Trailing
// flags are tolerated (`metrics list --worker w --page 2`) — flags.go uses
// this to exempt the form from -c; extra positionals for status and stop
// are rejected by the dispatch arity guard, unknown flags by their parsers.
func isMetricsRecordRead(args []string) bool {
	return len(args) >= 2 && args[0] == "metrics" &&
		(args[1] == "status" || args[1] == "stop" || args[1] == "list")
}

// metricsRelayEndpoint returns the live daemon's relay endpoint from its
// published status file — the ports a running daemon serves are its start
// config's, not necessarily this invocation's. The second result is false
// when no daemon is live or its record is unreadable; there is then no
// endpoint to dial and callers say so.
func metricsRelayEndpoint() (string, bool) {
	pidFile, _, statusFile := metricsPaths()
	pid, ok := readLivePid(pidFile)
	if !ok {
		return "", false
	}
	data, err := os.ReadFile(statusFile)
	if err != nil {
		return "", false
	}
	var st metricsStatusDoc
	if json.Unmarshal(data, &st) != nil || st.PID != pid || st.RelayPort <= 0 {
		return "", false
	}
	return fmt.Sprintf("http://127.0.0.1:%d", st.RelayPort), true
}

// metricsGateCheck is the Q12 boot gate: with metrics enabled, a worker
// requires the host metrics daemon. Probes the relay listener and fails
// fast naming the port and the remedy — a worker never runs with staged
// URLs pointing at a relay it never verified. Host-side reachability only,
// per the design: a future jail that unshares the network surfaces later
// as round-level dial errors (the accepted mid-flight behavior).
func metricsGateCheck(cfg config.Config) error {
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.Metrics.RelayPort))
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("metrics enabled but the metrics daemon is not reachable on %s — start it with `daedalus metrics start`", addr)
	}
	conn.Close()
	return nil
}

// metricsStart launches the metrics daemon as a detached process — the
// worker daemon's start shape: re-exec `metrics foreground`, redirect into
// the metrics log, write the pid file.
func metricsStart(cfg config.Config, configPath string) error {
	pidFile, logFile, _ := metricsPaths()
	if pid, ok := readLivePid(pidFile); ok {
		return fmt.Errorf("already running (pid %d) — use 'daedalus metrics restart' or 'stop'", pid)
	}
	if err := os.MkdirAll(daemonDir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", daemonDir, err)
	}
	pruneOldLogs()

	log, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open log %s: %w", logFile, err)
	}
	defer log.Close()

	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}
	cmd := exec.Command(self, "metrics", "foreground", "-c", configPath)
	cmd.Env = append(envWithoutWorkerVars(os.Environ()), daemonEnv+"=1")
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // detach from the terminal
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start daemon: %w", err)
	}
	pid := cmd.Process.Pid
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(pid)), 0o644); err != nil {
		return fmt.Errorf("write pid file %s: %w", pidFile, err)
	}
	cmd.Process.Release() // the daemon outlives this process

	fmt.Printf("metrics daemon started (pid %d)\n", pid)
	fmt.Printf("  relay:     127.0.0.1:%d\n", cfg.Metrics.RelayPort)
	fmt.Printf("  dashboard: :%d\n", cfg.Metrics.Port)
	fmt.Printf("  log:       %s\n", logFile)
	fmt.Printf("  stop:      daedalus metrics stop\n")
	return nil
}

// metricsStop terminates the daemon — the worker stop shape (SIGTERM, 30s
// poll, pid file cleared). Record-driven: no config involved.
func metricsStop() error {
	pidFile, logFile, _ := metricsPaths()
	pid, ok := readLivePid(pidFile)
	if !ok {
		os.Remove(pidFile)
		fmt.Println("metrics daemon not running")
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("signal pid %d: %w", pid, err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := readLivePid(pidFile); !ok {
			break
		}
		if !pidAlive(pid) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if pidAlive(pid) {
		os.Remove(pidFile)
		return fmt.Errorf("pid %d still alive after 30s — it may need SIGKILL; log: %s", pid, logFile)
	}
	os.Remove(pidFile)
	fmt.Printf("metrics daemon stopped (pid %d)\n", pid)
	return nil
}

// metricsRestart cycles the daemon with the invoking command's config —
// the serving ports re-read from disk on every restart, like the worker's.
func metricsRestart(cfg config.Config, configPath string) error {
	if err := metricsStop(); err != nil {
		return err
	}
	return metricsStart(cfg, configPath)
}

// metricsStatusDoc is the JSON document the daemon publishes to
// metrics.status — the worker status file's counterpart, rewritten on the
// same ticker so readers can spot a dead daemon's stale file.
type metricsStatusDoc struct {
	PID       int       `json:"pid"`
	Updated   time.Time `json:"updated"`
	Version   string    `json:"version,omitempty"`
	UIPort    int       `json:"ui_port"`
	RelayPort int       `json:"relay_port"`
}

// metricsStatus reports the daemon's liveness from the pid file plus the
// serving ports from its published status file. Record-driven: no config.
func metricsStatus() error {
	pidFile, logFile, statusFile := metricsPaths()
	pid, ok := readLivePid(pidFile)
	if !ok {
		os.Remove(pidFile)
		fmt.Println("metrics daemon not running")
		return nil
	}
	fmt.Printf("metrics daemon running (pid %d)\n", pid)
	if data, err := os.ReadFile(statusFile); err == nil {
		var st metricsStatusDoc
		if json.Unmarshal(data, &st) == nil && st.PID == pid {
			fmt.Printf("  relay:     127.0.0.1:%d\n", st.RelayPort)
			fmt.Printf("  dashboard: :%d\n", st.UIPort)
			fmt.Printf("  version:   %s (status updated %s ago)\n",
				st.Version, time.Since(st.Updated).Round(time.Second))
		}
	}
	fmt.Printf("  log:       %s\n", logFile)
	return nil
}

// runMetricsDaemon is the foreground process: the relay listener, the UI
// mux, the status publisher, and a signal-driven shutdown. In-flight
// requests are not drained — the lifecycle contract is the worker's.
func runMetricsDaemon(cfg config.Config) error {
	ledger := &metricsrelay.Ledger{} // zero value ready: mutex plus empty ring
	registry := metricsrelay.NewRegistry(metricsRegistrationTTL)
	relay, err := metricsrelay.Start(cfg.Metrics.RelayPort, ledger, registry)
	if err != nil {
		return fmt.Errorf("start relay: %w", err)
	}
	defer relay.Close()

	// The UI mux binds all interfaces — the LAN serving Q17 decided (no
	// auth; the relay stays loopback-only). The dashboard (ui.go, htmx-ui
	// pt2) owns the route map; every walk it renders reads the serving
	// config below.
	ui, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.Metrics.Port))
	if err != nil {
		return fmt.Errorf("listen on :%d: %w (the metrics.port knob names this port)", cfg.Metrics.Port, err)
	}
	srv := &http.Server{Handler: newUIServer(cfg, ledger).routes(), ReadHeaderTimeout: 30 * time.Second}
	go func() {
		if err := srv.Serve(ui); err != http.ErrServerClosed {
			fmt.Printf("metrics: dashboard serve: %v\n", err)
		}
	}()

	fmt.Printf("daedalus metrics %s starting %s (relay 127.0.0.1:%d, dashboard :%d)\n",
		version.String(), time.Now().Format(time.RFC3339), cfg.Metrics.RelayPort, cfg.Metrics.Port)
	stopPublish := make(chan struct{})
	defer close(stopPublish)
	go publishMetricsStatus(cfg, stopPublish)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Close()
	return nil
}

// publishMetricsStatus rewrites the daemon's status file on the worker's
// publish cadence — best-effort, write-then-rename, so a reader never sees
// a torn file.
func publishMetricsStatus(cfg config.Config, stop <-chan struct{}) {
	if err := os.MkdirAll(daemonDir, 0o755); err != nil {
		return
	}
	_, _, statusFile := metricsPaths()
	write := func() {
		data, err := json.Marshal(metricsStatusDoc{
			PID:       os.Getpid(),
			Updated:   time.Now().UTC(),
			Version:   version.String(),
			UIPort:    cfg.Metrics.Port,
			RelayPort: cfg.Metrics.RelayPort,
		})
		if err != nil {
			return
		}
		tmp := statusFile + ".tmp"
		if err := os.WriteFile(tmp, data, 0o644); err != nil {
			return
		}
		_ = os.Rename(tmp, statusFile)
	}
	write()
	ticker := time.NewTicker(statusPublishInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			write()
		}
	}
}

// runMetricsList is the CLI ledger read path: the live daemon's /ledger
// endpoint, newest first, one page at a time, optional worker filter,
// table or --json. The daemon is the ledger's only home (in-memory), so
// the endpoint comes from its published status file — no config involved.
func runMetricsList(args []string) error {
	worker, page, jsonOut := "", 1, false
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--json":
			jsonOut = true
		case a == "--table": // the default output, accepted for explicitness
		case a == "--worker":
			if i+1 >= len(args) {
				return fmt.Errorf("%s requires a value", a)
			}
			i++
			worker = args[i]
		case strings.HasPrefix(a, "--worker="):
			worker = strings.TrimPrefix(a, "--worker=")
		case a == "--page":
			if i+1 >= len(args) {
				return fmt.Errorf("%s requires a value", a)
			}
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil || n < 1 {
				return fmt.Errorf("--page wants a positive page number, got %q", args[i])
			}
			page = n
		case strings.HasPrefix(a, "--page="):
			v := strings.TrimPrefix(a, "--page=")
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				return fmt.Errorf("--page wants a positive page number, got %q", v)
			}
			page = n
		default:
			return fmt.Errorf("unknown metrics list flag %q (--worker, --page, --json, --table)", a)
		}
	}
	root, ok := metricsRelayEndpoint()
	if !ok {
		return fmt.Errorf("the metrics daemon is not running — start it with `daedalus metrics start`")
	}
	resp, err := metricsHTTP.Get(root + fmt.Sprintf("/ledger?worker=%s&page=%d", url.QueryEscape(worker), page))
	if err != nil {
		return fmt.Errorf("the metrics daemon is not reachable on %s (%v)", root, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("ledger query failed (%s): %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var doc struct {
		Total    int                  `json:"total"`
		Page     int                  `json:"page"`
		PageSize int                  `json:"page_size"`
		Entries  []metricsrelay.Entry `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return fmt.Errorf("read ledger response: %w", err)
	}
	if jsonOut {
		data, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "METRICS LEDGER — %d entr%s, page %d (page size %d)%s\n\n",
		doc.Total, pluralY(doc.Total), doc.Page, doc.PageSize, workerFilterNote(worker))
	fmt.Fprintln(w, "TIME\tWORKER\tTASK\tROLE\tROUND\tMODEL\tIN\tOUT\tCACHE R/W\tMS\tSTATUS")
	for _, e := range doc.Entries {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d/%d\t%d\t%d\n",
			e.At.Local().Format("15:04:05"), cellText(e.Worker), cellText(e.Task), cellText(e.Role), cellText(e.Round), cellText(e.Model),
			e.InputTokens, e.OutputTokens, e.CacheReadTokens, e.CacheWriteTokens,
			e.DurationMS, e.Status)
	}
	w.Flush()
	return nil
}

// cellText makes an upstream-controlled string safe for a table cell: a
// tab would forge columns and a newline forge rows in the operator's
// terminal, so control characters render as '?' (--json escapes
// correctly on its own). The numeric columns need none of this.
func cellText(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, s)
}

func pluralY(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

func workerFilterNote(worker string) string {
	if worker == "" {
		return ""
	}
	return ", worker " + worker
}

// metricsRegister posts this worker's upstream mapping to the daemon's
// relay — the registration half of the staged-path contract (Q16): the
// relay resolves staged paths against it and, when a keyed round presents
// the registration's secret token, restores the real credential the
// round's synthetic attribution key displaced. Token is the process's
// single mint (runWorker makes it once before the first registration and
// passes it here for both the body and the Setenv) — sent with the
// registration, answered with a bare 204, never echoed back, so no
// listener caller can harvest it by re-registering the name, and a name
// collision without it fails here at boot. Because every call presents
// the same secret, the heartbeat refreshes the live registration instead
// of colliding with it, and rounds staged at boot stay valid for the
// process's life. Upstreams and keys are read from this process's
// environment — the same values the per-round staging
// (activities.metricsRelayEnv) reads, so what the relay restores is
// exactly what staging displaced. Called once at boot (fail the start
// when the working listener refuses the endpoint) and refreshed on the
// heartbeat ticker.
func metricsRegister(cfg config.Config, root, token string) error {
	upstreams := map[string]string{}
	keys := map[string]string{}
	if v := os.Getenv("ANTHROPIC_BASE_URL"); v != "" {
		upstreams["anthropic"] = v
		if k := os.Getenv("ANTHROPIC_API_KEY"); k != "" {
			keys["anthropic"] = k
		}
	}
	if v := os.Getenv("OPENAI_BASE_URL"); v != "" {
		upstreams["openai"] = v
		if k := os.Getenv("OPENAI_API_KEY"); k != "" {
			keys["openai"] = k
		}
	} else if v := os.Getenv("OPENAI_API_BASE"); v != "" {
		upstreams["openai"] = v
		if k := os.Getenv("OPENAI_API_KEY"); k != "" {
			keys["openai"] = k
		}
	}
	if len(upstreams) == 0 {
		// No provider base URL configured: the per-round staging rewrites
		// nothing (rounds dial their CLI defaults) and the relay has
		// nothing to route — the documented per-CLI-wiring coverage limit.
		fmt.Printf("metrics relay: no provider base URL configured — nothing registered; rounds dial their CLI defaults and stay invisible to the ledger\n")
		return nil
	}
	body, err := json.Marshal(map[string]any{
		"worker":    cfg.WorkerName(),
		"queue":     cfg.Temporal.TaskQueue,
		"upstreams": upstreams,
		"keys":      keys,
		"token":     token,
	})
	if err != nil { // unreachable: map of marshalable values
		return err
	}
	resp, err := metricsHTTP.Post(root+"/register", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// A daemon older than the token handshake mints server-side and echoes
	// the secret in a 200 body — refused: this worker's staged rounds would
	// present a token that daemon never stored.
	if resp.StatusCode == http.StatusOK {
		return fmt.Errorf("the metrics daemon is older than this worker (server-minted registration token) — restart it (`daedalus metrics restart`)")
	}
	if resp.StatusCode == http.StatusConflict {
		// A live registration holds this name. Either this worker restarted
		// inside the registration TTL (the legitimate face — the dead
		// process's entry lingers up to a minute) or another process
		// squatted the name; neither can be told apart from here, and both
		// clear the same way, so name the remedy.
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("registration refused (%s): %s — a worker restarted inside the registration TTL hits this; wait for the old registration to lapse (up to a minute) and restart, or clear it now with `daedalus metrics restart`", resp.Status, strings.TrimSpace(string(b)))
	}
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("registration refused (%s): %s", resp.Status, strings.TrimSpace(string(b)))
	}
	if err := os.Setenv(config.MetricsRelayTokenEnv, token); err != nil {
		return fmt.Errorf("set %s: %w", config.MetricsRelayTokenEnv, err)
	}
	return nil
}
