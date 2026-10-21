package main

import (
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ozzono/daedalus/internal/config"
)

// The pins for the drain-until-idle self-restart: a `worker restart` on a
// live daemon signals it (SIGHUP schedules the drain, --force swaps
// immediately via SIGTERM) instead of stopping and starting it, the config
// record gains an optional run-type line, the daemon's exit cleanup is
// pid-guarded, and the re-exec's argv is rebuilt from the record.

// sleepIgnoring spawns a sleeping process that ignores sig — SIG_IGN
// survives the shell's exec into sleep — and returns its pid once the
// disposition is installed: the child announces readiness on a pipe after
// its trap and before the exec, so the test never signals through the
// window where the default disposition still stands. The trap doubles as
// the signal discriminator: the ignored signal leaves the child alive
// where any other one default-terminates it, so SIGHUP delivery vs a
// SIGTERM (and the reverse) is provable by who survives.
func sleepIgnoring(t *testing.T, sig string) (pid int) {
	t.Helper()
	ready, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sh", "-c", "trap '' "+sig+"; echo ready; exec sleep 30")
	cmd.Stdout = w
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ready.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("ready\n"))
	if _, err := io.ReadFull(ready, buf); err != nil {
		t.Fatalf("sleep child never armed its %s trap: %v", sig, err)
	}
	_ = ready.Close()
	return cmd.Process.Pid
}

// writePidFile seeds name's pid file with pid — a sleeping child's pid
// fakes a live daemon for the restart paths (readLivePid answers it)
// without any of the machinery of a real one.
func writePidFile(t *testing.T, name string, pid int) {
	t.Helper()
	pidFile, _, _ := daemonPaths(name)
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(pid)), 0o644); err != nil {
		t.Fatal(err)
	}
}

// pidFileContent reads name's pid file back as an int.
func pidFileContent(t *testing.T, name string) int {
	t.Helper()
	pidFile, _, _ := daemonPaths(name)
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("read pid file: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("pid file %s holds %q: %v", pidFile, data, err)
	}
	return pid
}

// stillAlive gives a delivered signal a moment to take effect and reports
// whether the process lives on: a default-terminating signal fells the
// child within milliseconds, an ignored one never.
func stillAlive(t *testing.T, pid int) bool {
	t.Helper()
	time.Sleep(300 * time.Millisecond)
	return pidAlive(pid)
}

// deadRelayPort hands back a loopback port nothing listens on — the boot
// gate's failing shape.
func deadRelayPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// writeGateConfig writes a loadable, metrics-ENABLED config naming queue
// with the relay on the given port — for the dead-port shape this is a
// config the boot gate refuses to restart onto.
func writeGateConfig(t *testing.T, queue string, relayPort int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "agent: claude\nmetrics:\n  enabled: true\n  relay_port: " +
		strconv.Itoa(relayPort) + "\ntemporal:\n  task_queue: " + queue + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// loadTestConfig loads path, failing the test on an unloadable config.
func loadTestConfig(t *testing.T, path string) config.Config {
	t.Helper()
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// TestConfigRecordRoundTrip pins the record's two-line format: line 1 the
// absolute config path, the optional line 2 the run type — written by
// writeConfigRecord only when one is given, read back by
// readConfigRecord, and line-1-compatible in both directions with
// pre-type single-line records (recordedConfigPath keeps working on
// typed records; old binaries reading typed records keep finding the
// path).
func TestConfigRecordRoundTrip(t *testing.T) {
	t.Run("a typed start records the type on line 2", func(t *testing.T) {
		useDaemonDir(t)
		_, _, confFile := daemonPaths("w")
		if err := writeConfigRecord(confFile, "/cfgs/dev.yaml", "dev"); err != nil {
			t.Fatalf("writeConfigRecord: %v", err)
		}
		data, err := os.ReadFile(confFile)
		if err != nil {
			t.Fatal(err)
		}
		if want := "/cfgs/dev.yaml\ndev"; string(data) != want {
			t.Errorf("record = %q, want %q", data, want)
		}
		path, typ := readConfigRecord("w")
		if path != "/cfgs/dev.yaml" || typ != "dev" {
			t.Errorf("readConfigRecord = (%q, %q), want (/cfgs/dev.yaml, dev)", path, typ)
		}
	})

	t.Run("an untyped start keeps the single-line format", func(t *testing.T) {
		useDaemonDir(t)
		_, _, confFile := daemonPaths("w")
		if err := writeConfigRecord(confFile, "/cfgs/all.yaml", ""); err != nil {
			t.Fatalf("writeConfigRecord: %v", err)
		}
		data, err := os.ReadFile(confFile)
		if err != nil {
			t.Fatal(err)
		}
		if want := "/cfgs/all.yaml"; string(data) != want {
			t.Errorf("record = %q, want the path alone with no trailing newline (%q)", data, want)
		}
		if path, typ := readConfigRecord("w"); path != "/cfgs/all.yaml" || typ != "" {
			t.Errorf("readConfigRecord = (%q, %q), want (/cfgs/all.yaml, %q)", path, typ, "")
		}
	})

	t.Run("a pre-type single-line record reads back untyped", func(t *testing.T) {
		useDaemonDir(t)
		writeRecord(t, "w", "/cfgs/old.yaml")
		if path, typ := readConfigRecord("w"); path != "/cfgs/old.yaml" || typ != "" {
			t.Errorf("readConfigRecord(single-line) = (%q, %q), want (/cfgs/old.yaml, %q)", path, typ, "")
		}
	})

	t.Run("recordedConfigPath reads line 1 of a typed record", func(t *testing.T) {
		useDaemonDir(t)
		_, _, confFile := daemonPaths("w")
		if err := writeConfigRecord(confFile, "/cfgs/dev.yaml", "test"); err != nil {
			t.Fatalf("writeConfigRecord: %v", err)
		}
		if got := recordedConfigPath("w"); got != "/cfgs/dev.yaml" {
			t.Errorf("recordedConfigPath(typed record) = %q, want line 1 only", got)
		}
	})

	t.Run("missing and blank records are no record", func(t *testing.T) {
		useDaemonDir(t)
		if path, typ := readConfigRecord("w"); path != "" || typ != "" {
			t.Errorf("readConfigRecord(missing) = (%q, %q), want empty both", path, typ)
		}
		writeRecord(t, "w", "  \n")
		if path, typ := readConfigRecord("w"); path != "" || typ != "" {
			t.Errorf("readConfigRecord(blank) = (%q, %q), want empty both", path, typ)
		}
	})
}

// TestWorkerRestartSchedulesDrain pins the default restart on a live
// daemon: the config is re-recorded, one SIGHUP is delivered (proved by a
// HUP-immune child surviving where a SIGTERM would have killed it), and
// the command returns without stopping anything — pid file untouched, no
// replacement spawned. A failed boot gate schedules nothing at all.
func TestWorkerRestartSchedulesDrain(t *testing.T) {
	t.Run("a live daemon is signaled, not waited on", func(t *testing.T) {
		useDaemonDir(t)
		cliCfg := writeQueueConfig(t, "daedalus")
		cfg := loadTestConfig(t, cliCfg)
		child := sleepIgnoring(t, "HUP")
		writePidFile(t, "daedalus", child)

		out := captureStdout(t, func() {
			if err := workerRestart(cfg, cliCfg, ""); err != nil {
				t.Errorf("workerRestart: %v", err)
			}
		})
		if !strings.Contains(out, "restart scheduled (drains current tasks)") ||
			!strings.Contains(out, strconv.Itoa(child)) {
			t.Errorf("restart output %q should name the scheduled drain and the signaled pid %d", out, child)
		}
		// SIGHUP, not a stop: the HUP-immune child lives on, and a
		// stop-then-start would have emptied the pid file and reported a
		// started worker instead.
		if !stillAlive(t, child) {
			t.Error("the live pid died — the scheduled restart must deliver SIGHUP, never stop the daemon")
		}
		if got := pidFileContent(t, "daedalus"); got != child {
			t.Errorf("pid file rewritten to %d — a scheduled restart never touches it", got)
		}
		if got, _ := readConfigRecord("daedalus"); got != cliCfg {
			t.Errorf("record = %q, want the invoking config %q (the re-exec reads it back)", got, cliCfg)
		}
	})

	t.Run("the scheduling restart records the run type", func(t *testing.T) {
		useDaemonDir(t)
		cliCfg := writeQueueConfig(t, "daedalus")
		cfg := loadTestConfig(t, cliCfg)
		child := sleepIgnoring(t, "HUP")
		writePidFile(t, "daedalus", child)

		out := captureStdout(t, func() {
			if err := workerRestart(cfg, cliCfg, "dev"); err != nil {
				t.Errorf("workerRestart: %v", err)
			}
		})
		if !strings.Contains(out, "restart scheduled (drains current tasks)") {
			t.Errorf("restart output %q should schedule the drain", out)
		}
		if path, typ := readConfigRecord("daedalus"); path != cliCfg || typ != "dev" {
			t.Errorf("record = (%q, %q), want (%q, dev) — the re-exec must come back typed", path, typ, cliCfg)
		}
		if !stillAlive(t, child) {
			t.Error("the live pid died — the scheduled restart must deliver SIGHUP")
		}
	})

	t.Run("a failed boot gate schedules nothing", func(t *testing.T) {
		useDaemonDir(t)
		cliCfg := writeQueueConfig(t, "daedalus")
		gateCfg := writeGateConfig(t, "daedalus", deadRelayPort(t))
		cfg := loadTestConfig(t, gateCfg)
		child := sleepIgnoring(t, "HUP")
		writePidFile(t, "daedalus", child)
		writeRecord(t, "daedalus", cliCfg)

		err := workerRestart(cfg, gateCfg, "")
		if err == nil || !strings.Contains(err.Error(), "metrics enabled but the metrics daemon is not reachable") {
			t.Errorf("workerRestart(dead relay) err = %v, want the boot gate", err)
		}
		if !stillAlive(t, child) {
			t.Error("the live pid died — a failed gate must leave the running daemon alone")
		}
		if got := pidFileContent(t, "daedalus"); got != child {
			t.Errorf("pid file rewritten to %d behind a failed gate", got)
		}
		if got := recordedConfigPath("daedalus"); got != cliCfg {
			t.Errorf("record rewritten to %q behind a failed gate, want the untouched %q", got, cliCfg)
		}
	})
}

// TestWorkerRestartForceSwapsNow pins the --force restart on a live
// daemon: one SIGTERM to the old pid (a TERM-immune child survives where
// a SIGHUP would have killed it) and a replacement started on the spot —
// new pid written, record rewritten, never blocking on the old process.
// A failed boot gate swaps nothing: a force restart onto a config that
// cannot boot must not trade a working worker for a dead one.
func TestWorkerRestartForceSwapsNow(t *testing.T) {
	t.Run("a live daemon is TERM'd and replaced without waiting", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		oldCfg := writeQueueConfig(t, "daedalus")
		cliCfg := writeQueueConfig(t, "daedalus")
		cfg := loadTestConfig(t, cliCfg)
		writeRecord(t, "daedalus", oldCfg)
		child := sleepIgnoring(t, "TERM")
		writePidFile(t, "daedalus", child)

		out := captureStdout(t, func() {
			if err := workerRestart(cfg, cliCfg, "", true); err != nil {
				t.Errorf("workerRestart(force): %v", err)
			}
		})
		if !strings.Contains(out, "force restart: signaled the old daemon") ||
			!strings.Contains(out, strconv.Itoa(child)) {
			t.Errorf("force-restart output %q should name the signaled old pid %d", out, child)
		}
		if !strings.Contains(out, "worker started (pid") {
			t.Errorf("force-restart output %q should report the replacement's start", out)
		}
		if strings.Contains(out, "restart scheduled") {
			t.Errorf("force-restart output %q must not schedule a drain", out)
		}
		// SIGTERM, not SIGHUP: the TERM-immune child lives on — its drain
		// is its own, and nothing here waited on it.
		if !stillAlive(t, child) {
			t.Error("the old pid died — the force swap must deliver SIGTERM (a HUP-immune child surviving pins the signal)")
		}
		if got := pidFileContent(t, "daedalus"); got == child {
			t.Errorf("pid file still names the old daemon %d — the replacement's pid was never written", got)
		}
		if got := recordedConfigPath("daedalus"); got != cliCfg {
			t.Errorf("record = %q, want the replacement's config %q", got, cliCfg)
		}
	})

	t.Run("nothing live just starts", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		cliCfg := writeQueueConfig(t, "daedalus")
		cfg := loadTestConfig(t, cliCfg)

		out := captureStdout(t, func() {
			if err := workerRestart(cfg, cliCfg, "", true); err != nil {
				t.Errorf("workerRestart(force, not running): %v", err)
			}
		})
		if !strings.Contains(out, "worker started (pid") {
			t.Errorf("force-restart output %q should plain-start the worker", out)
		}
		if strings.Contains(out, "force restart") {
			t.Errorf("force-restart output %q has no old daemon to signal", out)
		}
	})

	t.Run("a failed boot gate swaps nothing", func(t *testing.T) {
		useDaemonDir(t)
		oldCfg := writeQueueConfig(t, "daedalus")
		gateCfg := writeGateConfig(t, "daedalus", deadRelayPort(t))
		cfg := loadTestConfig(t, gateCfg)
		writeRecord(t, "daedalus", oldCfg)
		child := sleepIgnoring(t, "TERM")
		writePidFile(t, "daedalus", child)

		err := workerRestart(cfg, gateCfg, "", true)
		if err == nil || !strings.Contains(err.Error(), "metrics enabled but the metrics daemon is not reachable") {
			t.Errorf("workerRestart(force, dead relay) err = %v, want the boot gate", err)
		}
		if !stillAlive(t, child) {
			t.Error("the old pid died behind a failed gate — the working worker was traded for a dead one")
		}
		if got := pidFileContent(t, "daedalus"); got != child {
			t.Errorf("pid file rewritten to %d behind a failed gate", got)
		}
		if got := recordedConfigPath("daedalus"); got != oldCfg {
			t.Errorf("record rewritten to %q behind a failed gate, want %q", got, oldCfg)
		}
	})
}

// TestWorkerRestartNamedForce pins force's pass-through on the
// record-driven named restart: forced it swaps immediately, default it
// schedules the drain — both from the recorded config alone.
func TestWorkerRestartNamedForce(t *testing.T) {
	t.Run("force swaps now", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		cfgPath := writeWorkerIDConfig(t, "alpha")
		writeRecord(t, "alpha", cfgPath)
		child := sleepIgnoring(t, "TERM")
		writePidFile(t, "alpha", child)

		out := captureStdout(t, func() {
			if err := workerRestartNamed("alpha", true); err != nil {
				t.Errorf("workerRestartNamed(force): %v", err)
			}
		})
		if !strings.Contains(out, "force restart: signaled the old daemon") {
			t.Errorf("named force-restart output %q should swap immediately", out)
		}
		if !stillAlive(t, child) {
			t.Error("the old pid died — the named force swap must deliver SIGTERM")
		}
	})

	t.Run("the default schedules the drain", func(t *testing.T) {
		useDaemonDir(t)
		cfgPath := writeWorkerIDConfig(t, "alpha")
		writeRecord(t, "alpha", cfgPath)
		child := sleepIgnoring(t, "HUP")
		writePidFile(t, "alpha", child)

		out := captureStdout(t, func() {
			if err := workerRestartNamed("alpha"); err != nil {
				t.Errorf("workerRestartNamed: %v", err)
			}
		})
		if !strings.Contains(out, "restart scheduled (drains current tasks)") {
			t.Errorf("named restart output %q should schedule the drain", out)
		}
		if strings.Contains(out, "force restart") {
			t.Errorf("named restart output %q must not swap without force", out)
		}
		if !stillAlive(t, child) {
			t.Error("the live pid died — the named default restart must deliver SIGHUP")
		}
		if got := pidFileContent(t, "alpha"); got != child {
			t.Errorf("pid file rewritten to %d — a scheduled restart never touches it", got)
		}
	})
}

// TestRemoveOwnPidFile pins the pid-guarded exit cleanup: a daemon unlinks
// its pid file only while the file still names itself, so a --force swap's
// exiting predecessor cannot delete the file that already names its
// replacement.
func TestRemoveOwnPidFile(t *testing.T) {
	t.Run("our own pid file is removed", func(t *testing.T) {
		useDaemonDir(t)
		pidFile, _, _ := daemonPaths("w")
		if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
			t.Fatal(err)
		}
		removeOwnPidFile(pidFile)
		if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
			t.Errorf("own pid file survived the exit cleanup: %v", err)
		}
	})

	t.Run("surrounding whitespace still counts as ours", func(t *testing.T) {
		useDaemonDir(t)
		pidFile, _, _ := daemonPaths("w")
		if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		removeOwnPidFile(pidFile)
		if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
			t.Errorf("own pid file with a trailing newline survived: %v", err)
		}
	})

	t.Run("a pid file naming another process survives", func(t *testing.T) {
		useDaemonDir(t)
		pidFile, _, _ := daemonPaths("w")
		if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid()+1)), 0o644); err != nil {
			t.Fatal(err)
		}
		removeOwnPidFile(pidFile)
		if got := pidFileContent(t, "w"); got != os.Getpid()+1 {
			t.Errorf("foreign pid file rewritten to %d — an exiting daemon must never unname its replacement", got)
		}
	})

	t.Run("a malformed record survives", func(t *testing.T) {
		useDaemonDir(t)
		pidFile, _, _ := daemonPaths("w")
		if err := os.WriteFile(pidFile, []byte("not-a-pid"), 0o644); err != nil {
			t.Fatal(err)
		}
		removeOwnPidFile(pidFile)
		if _, err := os.Stat(pidFile); err != nil {
			t.Errorf("malformed pid file removed by the guard: %v", err)
		}
	})

	t.Run("a missing pid file is not an error", func(t *testing.T) {
		useDaemonDir(t)
		pidFile, _, _ := daemonPaths("w")
		removeOwnPidFile(pidFile)
		if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
			t.Errorf("pid file appeared out of nowhere: %v", err)
		}
	})
}

// TestRestartArgv pins the re-exec argv construction: the daemon restarts
// from its config record — `worker foreground -c <recorded path>` plus
// `-t <recorded type>` when the record carries one — and a worker with no
// readable record (started before records existed) re-execs as what it
// is: its own invocation.
func TestRestartArgv(t *testing.T) {
	const self = "/bin/daedalus"

	t.Run("a typed record re-execs typed", func(t *testing.T) {
		useDaemonDir(t)
		_, _, confFile := daemonPaths("w")
		if err := writeConfigRecord(confFile, "/cfgs/dev.yaml", "test"); err != nil {
			t.Fatalf("writeConfigRecord: %v", err)
		}
		got := restartArgv(self, "w")
		want := []string{self, "worker", "foreground", "-c", "/cfgs/dev.yaml", "-t", "test"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("restartArgv = %v, want %v", got, want)
		}
	})

	t.Run("an untyped record re-execs untyped", func(t *testing.T) {
		useDaemonDir(t)
		writeRecord(t, "w", "/cfgs/all.yaml")
		got := restartArgv(self, "w")
		want := []string{self, "worker", "foreground", "-c", "/cfgs/all.yaml"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("restartArgv = %v, want %v (no -t)", got, want)
		}
	})

	t.Run("no record re-execs the original invocation", func(t *testing.T) {
		useDaemonDir(t)
		got := restartArgv(self, "w")
		want := append([]string{self}, os.Args[1:]...)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("restartArgv = %v, want the running invocation %v", got, want)
		}
	})

	t.Run("a blank record counts as no record", func(t *testing.T) {
		useDaemonDir(t)
		writeRecord(t, "w", "  \n")
		got := restartArgv(self, "w")
		want := append([]string{self}, os.Args[1:]...)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("restartArgv(blank record) = %v, want the running invocation %v", got, want)
		}
	})
}

// TestMainWorkerRestartForceDispatch pins the dispatch wiring in-process:
// the restart scope's -f parses into force and reaches the named restart
// as an immediate swap — from a bare directory with no config anywhere,
// exactly like the unforced named restart.
func TestMainWorkerRestartForceDispatch(t *testing.T) {
	useDaemonDir(t)
	noDaemonSpawn(t)
	cfgPath := writeWorkerIDConfig(t, "alpha")
	writeRecord(t, "alpha", cfgPath)
	child := sleepIgnoring(t, "TERM")
	writePidFile(t, "alpha", child)

	t.Setenv("HOME", t.TempDir()) // no ~/.config/daedalus/config.yaml fallback
	t.Chdir(t.TempDir())          // no ./config.yaml

	args := os.Args
	os.Args = []string{"daedalus", "worker", "restart", "alpha", "-f"}
	defer func() { os.Args = args }()
	out := captureStdout(t, main)

	if !strings.Contains(out, "force restart: signaled the old daemon") {
		t.Errorf("worker restart alpha -f output %q should swap immediately — the dispatch's force wiring dropped the flag", out)
	}
	if strings.Contains(out, "restart scheduled") {
		t.Errorf("worker restart alpha -f output %q must not schedule a drain", out)
	}
	if !stillAlive(t, child) {
		t.Error("the old pid died — the dispatched force swap must deliver SIGTERM")
	}
	if got := pidFileContent(t, "alpha"); got == child {
		t.Errorf("pid file still names the old daemon %d — the replacement never started", got)
	}
}

// TestMainWorkerRestartForceArity pins the argument budget with force
// flags on the line: the scope's -f does not count toward the worker
// action's arguments, and junk behind it is still junk — the guard strips
// the force flags before counting, so both shapes stay what they always
// were.
func TestMainWorkerRestartForceArity(t *testing.T) {
	_, stderr, code := runMainIn(t, "", "-c", validConfig(t), "worker", "restart", "alpha", "-f", "extra")
	if code != 1 {
		t.Errorf("daedalus worker restart alpha -f extra exit code = %d, want 1", code)
	}
	if !strings.HasPrefix(stderr, "worker takes at most one action\n\n") {
		t.Errorf("stderr should start with the too-many-actions rejection, got %q", stderr)
	}
}
