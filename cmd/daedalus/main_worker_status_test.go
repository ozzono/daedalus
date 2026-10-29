package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// reapedChildPid returns the pid of an exited-and-reaped child — the
// cheapest process that is certainly gone (TestPidAlive's shape): pidAlive
// must answer false for it, zombie included.
func reapedChildPid(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("sh", "-c", "exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		t.Fatalf("reap the stand-in child: %v", err)
	}
	return pid
}

// writeBootMarker seeds name's boot marker naming pid with deadline.
func writeBootMarker(t *testing.T, name string, pid int, deadline time.Time) {
	t.Helper()
	data, err := json.Marshal(workerBootState{PID: pid, Deadline: deadline.UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bootMarkerPath(name), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestWorkerStatusAll pins the record-driven roster listing: every recorded
// queue — plus any live stray running without a record, marked "(no config
// record)" — appears sorted, with its running state, its config record, and
// its log path; an empty or missing roster prints the start-one-first hint
// instead of failing.
func TestWorkerStatusAll(t *testing.T) {
	t.Run("empty roster prints the hint, not an error", func(t *testing.T) {
		dir := useDaemonDir(t)
		out := captureStdout(t, func() {
			if err := workerStatusAll(); err != nil {
				t.Errorf("workerStatusAll(empty): %v", err)
			}
		})
		if want := "no workers on record in " + dir; !strings.Contains(out, want) {
			t.Errorf("status output %q should carry %q", out, want)
		}
	})

	t.Run("missing daemon dir reads as an empty roster", func(t *testing.T) {
		// A fresh machine: the daemon dir has never been created, and the
		// hint must still say "start one first", not report a read failure.
		gone := filepath.Join(t.TempDir(), "gone")
		reset := t.TempDir()
		t.Cleanup(func() { daemonDir = reset })
		daemonDir = gone

		out := captureStdout(t, func() {
			if err := workerStatusAll(); err != nil {
				t.Errorf("workerStatusAll(missing dir): %v", err)
			}
		})
		if want := "no workers on record in " + gone; !strings.Contains(out, want) {
			t.Errorf("status output %q should carry %q", out, want)
		}
	})

	t.Run("lists records and live strays with state, config, and log", func(t *testing.T) {
		dir := useDaemonDir(t)
		cfgB := writeQueueConfig(t, "b")
		writeLivePidFile(t, "a")  // live stray, no record
		writeRecord(t, "b", cfgB) // recorded, running
		writeLivePidFile(t, "b")
		writeRecord(t, "z", writeQueueConfig(t, "z")) // recorded, not running

		out := captureStdout(t, func() {
			if err := workerStatusAll(); err != nil {
				t.Errorf("workerStatusAll: %v", err)
			}
		})

		for _, want := range []string{"WORKER", "STATE", "VERSION", "API", "CONFIG", "CODE PATH", "LOG"} {
			if !strings.Contains(out, want) {
				t.Errorf("status output %q should carry the %s column header", out, want)
			}
		}
		// Neither live worker has published a status file here, so the
		// VERSION column cannot confirm a build and says "(unknown)"; the
		// stopped worker has nothing to report at all.
		if !strings.Contains(out, "(unknown)") {
			t.Errorf("status output %q should mark the live workers' unknown version", out)
		}
		if !strings.Contains(out, "n/a (not running)") {
			t.Errorf("status output %q should leave the stopped worker's version n/a", out)
		}
		// Row b: running under this process's pid, with its recorded config
		// and log path.
		for _, want := range []string{
			"running (pid " + strconv.Itoa(os.Getpid()) + ")",
			cfgB,
			filepath.Join(dir, "worker-b.log"),
		} {
			if !strings.Contains(out, want) {
				t.Errorf("status output %q should carry %q for the running worker", out, want)
			}
		}
		if !strings.Contains(out, "not running") {
			t.Errorf("status output %q should mark the stopped worker", out)
		}
		if !strings.Contains(out, "(no config record)") {
			t.Errorf("status output %q should mark the live stray's missing record", out)
		}
		// The stray's API column must read n/a, not "config error": the
		// placeholder is display text for the CONFIG column only, never a
		// config path for providerStatus to load.
		if strings.Contains(out, "config error") {
			t.Errorf("status output %q should report n/a for the missing record, not \"config error\"", out)
		}
		// The merged roster runs sorted. The stray deliberately sorts before
		// both records: recordedWorkers yields [b, z] and the stray appends
		// as [a], so without workerStatusAll's own merge sort the
		// concatenation would print b, z, a and this ordering assertion
		// fails.
		ia, ib, iz := strings.Index(out, "\na"), strings.Index(out, "\nb"), strings.Index(out, "\nz")
		if ia < 0 || ib < 0 || iz < 0 || !(ia < ib && ib < iz) {
			t.Errorf("status output %q should list workers a, b, z sorted (indexes %d, %d, %d)", out, ia, ib, iz)
		}
	})

	t.Run("shows the published version of a running worker, unknown when the file disagrees", func(t *testing.T) {
		useDaemonDir(t)
		// pub is live and its status file names this pid and a build;
		// mis is live but its file was left by a different pid, so the
		// version in it must not be shown.
		writeLivePidFile(t, "mis")
		writeStatusFile(t, "mis", workerStatus{PID: os.Getpid() + 1, Updated: time.Now().UTC(), Version: "v0.0-wrong"})
		writeLivePidFile(t, "pub")
		writeStatusFile(t, "pub", workerStatus{PID: os.Getpid(), Updated: time.Now().UTC(), Version: "v0.0-test"})

		out := captureStdout(t, func() {
			if err := workerStatusAll(); err != nil {
				t.Errorf("workerStatusAll: %v", err)
			}
		})

		row := func(name string) string {
			for _, line := range strings.Split(out, "\n") {
				if strings.HasPrefix(line, name) {
					return line
				}
			}
			t.Fatalf("status output %q has no row for %s", out, name)
			return ""
		}
		if r := row("pub"); !strings.Contains(r, "v0.0-test") {
			t.Errorf("row %q should carry the published version v0.0-test", r)
		}
		if r := row("mis"); !strings.Contains(r, "(unknown)") || strings.Contains(r, "v0.0-wrong") {
			t.Errorf("row %q should distrust the mismatched file's version", r)
		}
	})
}

// TestWorkerBootingPhase pins the boot marker's read side: a marker naming
// a live process with retry budget left reports the booting phase with the
// remaining budget; a lapsed budget or a dead process — the crash's
// leftover marker — reports nothing, so a dead worker never shows as
// booting; a corrupt marker reads as not booting too.
func TestWorkerBootingPhase(t *testing.T) {
	useDaemonDir(t)
	fresh := time.Now().Add(metricsRegistrationRetryWait)

	if _, _, ok := workerBootingPhase("ghost"); ok {
		t.Error("a worker with no boot marker reported a booting phase")
	}

	writeBootMarker(t, "booting", os.Getpid(), fresh)
	pid, left, ok := workerBootingPhase("booting")
	if !ok || pid != os.Getpid() || left <= 0 || left > metricsRegistrationRetryWait {
		t.Errorf("workerBootingPhase(live marker) = (%d, %s, %v), want this pid with budget left inside the retry wait", pid, left, ok)
	}

	// The budget lapsed while the process is still alive: expired.
	writeBootMarker(t, "lapsed", os.Getpid(), time.Now().Add(-time.Second))
	if _, _, ok := workerBootingPhase("lapsed"); ok {
		t.Error("a marker past its deadline still reported booting")
	}

	// The process died with budget left: a crash's leftover marker expires
	// itself on the read.
	writeBootMarker(t, "dead", reapedChildPid(t), fresh)
	if _, _, ok := workerBootingPhase("dead"); ok {
		t.Error("a marker naming a dead process still reported booting")
	}

	if err := os.WriteFile(bootMarkerPath("bad"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := workerBootingPhase("bad"); ok {
		t.Error("a corrupt boot marker reported a booting phase")
	}
}

// TestWorkerStatusBootPhase pins the roster row's booting state: a live
// boot marker outranks the pid file — the delayed boot shows as booting
// with its retry budget instead of a bare "running" (the spawned shape,
// whose pid file is written early) or a false "not running" (the
// supervised shape, whose pid file is written after the boot) — and the
// booting pid serves the version lookup, so the row never pairs "booting
// (pid N)" with "n/a (not running)". A marker that expired itself — lapsed
// budget or dead process — leaves the plain shapes standing.
func TestWorkerStatusBootPhase(t *testing.T) {
	fresh := time.Now().Add(metricsRegistrationRetryWait)

	t.Run("a live marker outranks the pid file and serves the version", func(t *testing.T) {
		useDaemonDir(t)
		writeLivePidFile(t, "spawned") // spawned shape: pid file already written, boot still inside the window
		writeBootMarker(t, "spawned", os.Getpid(), fresh)
		// The supervised shape reaches the roster through its config record:
		// `worker start` records it before the daemon's boot writes a pid file.
		writeRecord(t, "supervised", writeQueueConfig(t, "supervised"))
		writeBootMarker(t, "supervised", os.Getpid(), fresh)
		writeRecord(t, "ver", writeQueueConfig(t, "ver"))
		writeBootMarker(t, "ver", os.Getpid(), fresh)
		writeStatusFile(t, "ver", workerStatus{PID: os.Getpid(), Updated: time.Now().UTC(), Version: "v0.0-test"})

		out := captureStdout(t, func() {
			if err := workerStatusAll(); err != nil {
				t.Errorf("workerStatusAll: %v", err)
			}
		})
		row := func(name string) string {
			for _, line := range strings.Split(out, "\n") {
				if strings.HasPrefix(line, name) {
					return line
				}
			}
			t.Fatalf("status output %q has no row for %s", out, name)
			return ""
		}

		booting := "booting (pid " + strconv.Itoa(os.Getpid()) + ", metrics relay registration retry"
		for _, name := range []string{"spawned", "supervised", "ver"} {
			if r := row(name); !strings.Contains(r, booting) {
				t.Errorf("%s row %q should show the delayed boot", name, r)
			}
		}
		if r := row("spawned"); strings.Contains(r, "running (pid ") {
			t.Errorf("spawned row %q still reads as bare running inside the boot window", r)
		}
		if r := row("supervised"); strings.Contains(r, "not running") {
			t.Errorf("supervised row %q reads as not running inside the boot window", r)
		}
		if r := row("ver"); !strings.Contains(r, "v0.0-test") || strings.Contains(r, "n/a (not running)") {
			t.Errorf("ver row %q should carry the published version via the booting pid, never the not-running n/a", r)
		}
	})

	t.Run("an expired marker leaves the plain shapes", func(t *testing.T) {
		useDaemonDir(t)
		writeRecord(t, "lapsed", writeQueueConfig(t, "lapsed"))
		writeBootMarker(t, "lapsed", os.Getpid(), time.Now().Add(-time.Second)) // budget gone, no pid file: not running
		writeLivePidFile(t, "deadpid")                                          // live pid file outranked only by a live marker
		writeBootMarker(t, "deadpid", reapedChildPid(t), fresh)

		out := captureStdout(t, func() {
			if err := workerStatusAll(); err != nil {
				t.Errorf("workerStatusAll: %v", err)
			}
		})
		row := func(name string) string {
			for _, line := range strings.Split(out, "\n") {
				if strings.HasPrefix(line, name) {
					return line
				}
			}
			t.Fatalf("status output %q has no row for %s", out, name)
			return ""
		}
		if r := row("lapsed"); !strings.Contains(r, "not running") || strings.Contains(r, "booting") {
			t.Errorf("lapsed row %q should read as the plain not running", r)
		}
		if r := row("deadpid"); !strings.Contains(r, "running (pid ") || strings.Contains(r, "booting") {
			t.Errorf("dead-marker row %q should fall back to the pid file's running", r)
		}
	})
}

// TestWorkerCodePathLadder pins workerCodePath's display-text ladder for
// the cases decidable without a Temporal round trip: no config record, and
// an unloadable config. Everything past config.Load (dial, query, history)
// degrades to "temporal unreachable"/"idle" in the row and is exercised
// only live.
func TestWorkerCodePathLadder(t *testing.T) {
	if got := workerCodePath(""); got != "n/a (no config record)" {
		t.Errorf("workerCodePath(no record) = %q, want the n/a placeholder", got)
	}
	if got := workerCodePath(filepath.Join(t.TempDir(), "gone.yaml")); got != "config error" {
		t.Errorf("workerCodePath(missing config) = %q, want \"config error\"", got)
	}
}

// TestWorkerVersionLadder pins workerVersion's display-text ladder for the
// cases decidable without a live daemon: a worker that is not running has
// nothing to report, and a running worker's version is shown only when a
// readable status file confirms it with a matching pid and a non-empty
// version — no file, a pre-version file from an old build, a pid
// mismatch, or corrupt JSON all degrade to "(unknown)".
func TestWorkerVersionLadder(t *testing.T) {
	useDaemonDir(t)
	writeStatusFile(t, "ok", workerStatus{PID: os.Getpid(), Updated: time.Now().UTC(), Version: "v1.2.3"})
	// A file from a worker old enough to predate the version field.
	writeStatusFile(t, "old", workerStatus{PID: os.Getpid(), Updated: time.Now().UTC()})
	writeStatusFile(t, "pid", workerStatus{PID: os.Getpid() + 1, Updated: time.Now().UTC(), Version: "v1.2.3"})
	if err := os.WriteFile(filepath.Join(daemonDir, "worker-bad.status"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name string // worker name; also keys its worker-<name>.status file
		live bool
		want string
	}{
		{"stopped", false, "n/a (not running)"},
		{"nofile", true, "(unknown)"},
		{"ok", true, "v1.2.3"},
		{"old", true, "(unknown)"},
		{"pid", true, "(unknown)"},
		{"bad", true, "(unknown)"},
	} {
		if got := workerVersion(c.name, os.Getpid(), c.live); got != c.want {
			t.Errorf("workerVersion(%q, live=%v) = %q, want %q", c.name, c.live, got, c.want)
		}
	}
}

// TestMainWorkerStatusNeedsNoConfig pins the dispatch wiring in-process:
// `worker status` runs with no config resolvable anywhere (bare cwd, empty
// HOME) and still lists the records — without the isWorkerStatus bypass in
// main, resolveConfigPath would exit(1) with a "load config" diagnostic
// before any record is read. Status spawns nothing, so the real daemon dir
// is only replaced to keep the listing deterministic.
func TestMainWorkerStatusNeedsNoConfig(t *testing.T) {
	useDaemonDir(t)
	cfg := writeQueueConfig(t, "daedalus")
	writeRecord(t, "daedalus", cfg)

	t.Setenv("HOME", t.TempDir()) // no ~/.config/daedalus/config.yaml fallback
	t.Chdir(t.TempDir())          // no ./config.yaml

	args := os.Args
	os.Args = []string{"daedalus", "worker", "status"}
	defer func() { os.Args = args }()
	out := captureStdout(t, main)

	if !strings.Contains(out, "daedalus") || !strings.Contains(out, cfg) {
		t.Errorf("worker status output %q should list the recorded worker with its config", out)
	}
}

// TestProbeProviderNAPaths pins the not-probeable faces: a worker with no
// config record, one whose record no longer loads, and one whose config
// wires no provider at all — each renders its n/a text, never a
// fabricated probe outcome.
func TestProbeProviderNAPaths(t *testing.T) {
	if p := probeProvider(""); p.NA != "n/a (no config record)" || p.Main.OK {
		t.Errorf("probeProvider(no record) = %+v, want the no-record note", p)
	}
	if p := probeProvider(filepath.Join(t.TempDir(), "gone.yaml")); p.NA != "config error" {
		t.Errorf("probeProvider(unloadable) = %+v, want the config-error note", p)
	}
	conf := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(conf, []byte("agent: claude\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if p := probeProvider(conf); p.NA != "n/a (provider not configured)" {
		t.Errorf("probeProvider(unwired) = %+v, want the not-configured note", p)
	}
	if got := providerStatus(""); got != "n/a (no config record)" {
		t.Errorf("providerStatus(no record) = %q, want the note passed through", got)
	}
}

// TestProbeProviderLive pins the wired probes against httptest stand-ins:
// the probe's wire shape is the section's heartbeat format — a legacy-only
// config (whose sections carry no heartbeat field) rides the token-free
// check its wire resolves to (count_tokens on the anthropic side), while a
// section-named format names the request on the provider and the fallback
// alike, the gen-heartbeat returning to a real generation only when named.
// A failing primary with a configured fallback probes that fallback too
// and the row renders the failover arrow — active fallback when it
// answers, none when it does not.
func TestProbeProviderLive(t *testing.T) {
	// stand serves every path with code and records what was hit.
	stand := func(code int) (*httptest.Server, func() []string) {
		mu := &sync.Mutex{}
		var paths []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			paths = append(paths, r.URL.Path)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_, _ = w.Write([]byte("{}"))
		}))
		t.Cleanup(srv.Close)
		return srv, func() []string { mu.Lock(); defer mu.Unlock(); return paths }
	}
	writeCfg := func(content string) string {
		t.Helper()
		conf := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(conf, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		return conf
	}

	t.Run("a legacy-only primary probes its wire's token-free check", func(t *testing.T) {
		srv, hits := stand(http.StatusOK)
		conf := writeCfg("agent: claude\nanthropic:\n  url: " + srv.URL + "\n  key: k\n  model: m\n")
		p := probeProvider(conf)
		if !p.Main.OK || p.Main.Detail != "ok (http 200)" {
			t.Errorf("Main = %+v, want ok (http 200)", p.Main)
		}
		if p.FbArmed {
			t.Errorf("no fallback is configured, none should be probed")
		}
		n := 0
		for _, path := range hits() {
			if path == "/v1/messages/count_tokens" {
				n++
			}
		}
		if n != 1 {
			t.Errorf("the default probe hit %v, want exactly one /v1/messages/count_tokens", hits())
		}
		if got := providerStatus(conf); got != "ok (http 200)" {
			t.Errorf("providerStatus = %q, want the main detail alone", got)
		}
	})

	t.Run("a down primary arms the configured openai fallback", func(t *testing.T) {
		downSrv, downHits := stand(http.StatusInternalServerError)
		okSrv, okHits := stand(http.StatusOK)
		conf := writeCfg("agent: claude\nanthropic:\n  url: " + downSrv.URL + "\n  key: k\n  model: m\n" +
			"fallback:\n  enabled: true\n  type: openai\n  url: " + okSrv.URL + "\n  key: fk\n  model: fbm\n")
		p := probeProvider(conf)
		if p.Main.OK {
			t.Errorf("the 500-serving primary probed ok")
		}
		if !p.FbArmed || !p.Fb.OK {
			t.Errorf("Fb armed/ok = %v/%v, want the armed fallback probed ok", p.FbArmed, p.Fb.OK)
		}
		// The format-less main resolves per its anthropic wire; the
		// openai-typed fallback resolves per its own, to the model list.
		if !saw(downHits(), "/v1/messages/count_tokens") || !saw(okHits(), "/models") {
			t.Errorf("wire shapes wrong: main hit %v, fallback hit %v, want the token-free checks each wire resolves to", downHits(), okHits())
		}
		got := providerStatus(conf)
		if !strings.Contains(got, "main: http 500") || !strings.Contains(got, "→ fallback: ok (http 200) [active: fallback]") {
			t.Errorf("providerStatus = %q, want the failover arrow with the fallback active", got)
		}
	})

	t.Run("the sections' heartbeat fields name the probe's shape", func(t *testing.T) {
		downSrv, downHits := stand(http.StatusInternalServerError)
		okSrv, okHits := stand(http.StatusOK)
		conf := writeCfg("agent: claude\n" +
			"provider:\n  type: anthropic\n  url: " + downSrv.URL + "\n  key: k\n  model: m\n  heartbeat: gen-heartbeat\n" +
			"fallback:\n  enabled: true\n  type: openai\n  url: " + okSrv.URL + "\n  key: fk\n  model: fbm\n  heartbeat: anthropic\n")
		p := probeProvider(conf)
		if p.Main.OK {
			t.Errorf("the 500-serving primary probed ok")
		}
		if !p.FbArmed || !p.Fb.OK {
			t.Errorf("Fb armed/ok = %v/%v, want the armed fallback probed ok", p.FbArmed, p.Fb.OK)
		}
		// The provider section's gen-heartbeat generates over the wire its
		// type names (/v1/messages), while the fallback's named anthropic
		// format ignores its openai type and counts tokens instead: the
		// format is the backend's property, not the wire's.
		if !saw(downHits(), "/v1/messages") || !saw(okHits(), "/v1/messages/count_tokens") {
			t.Errorf("heartbeat shapes wrong: main hit %v, fallback hit %v", downHits(), okHits())
		}
	})

	t.Run("primary and fallback both down render active none", func(t *testing.T) {
		downSrv, _ := stand(http.StatusInternalServerError)
		conf := writeCfg("agent: claude\nanthropic:\n  url: " + downSrv.URL + "\n  key: k\n  model: m\n" +
			"fallback:\n  enabled: true\n  type: anthropic\n  url: " + downSrv.URL + "\n  key: fk\n  model: fbm\n")
		if got := providerStatus(conf); !strings.Contains(got, "[active: none]") {
			t.Errorf("providerStatus = %q, want [active: none]", got)
		}
	})
}

// saw reports whether paths contains exactly path.
func saw(paths []string, path string) bool {
	for _, p := range paths {
		if p == path {
			return true
		}
	}
	return false
}
