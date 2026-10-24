package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/version"
)

// reexecEnv hands the re-exec'd test binary the CLI arguments to dispatch to
// main: when set, TestMain runs main() with those arguments (unit-separated,
// since no test argument carries one) instead of the test suite. The env var
// is private to this package's tests and set only on the child's exec.Cmd.
const reexecEnv = "DAEDALUS_TEST_REEXEC_ARGS"

// reexecDumpArgsEnv makes the test binary print its real argv (unit-joined,
// without the program name) and exit, instead of running anything. Tests set
// it when a spawned child — the daemon workerStart re-executes — must report
// the arguments it was actually given; the child's stdout is redirected into
// the daemon log, which is where the test reads the line back.
const reexecDumpArgsEnv = "DAEDALUS_TEST_DUMP_OSARGS"

// reexecDumpEnvVarsEnv makes the test binary print the named environment
// variables (unit-separated names, each as a NAME=value line) and exit,
// instead of running anything. Tests set it when a spawned daemon must
// report the environment it actually inherited; the child's stdout is
// redirected into the daemon log, which is where the test reads the lines
// back.
const reexecDumpEnvVarsEnv = "DAEDALUS_TEST_DUMP_ENV_VARS"

// reexecSweepSleepEnv turns a re-exec'd test binary into a stand-in worker:
// it writes the env value's path as a ready marker and sleeps until
// signaled, running no tests. The sweep tests plant it on a copy of the test
// binary so a real, killable process exists with the argv shape the sweep
// identifies — without it the child would re-run the whole test suite.
const reexecSweepSleepEnv = "DAEDALUS_TEST_SWEEP_SLEEP"

// TestMain is the re-exec guard for the exit-code paths: exitf, fail,
// usageFail, and loadConfig all call os.Exit(1), so they cannot run inside a
// test process. Tests route them through runMainIn, which execs this same
// binary with reexecEnv set; here that arrival is detected before the test
// framework starts.
func TestMain(m *testing.M) {
	if marker := os.Getenv(reexecSweepSleepEnv); marker != "" {
		_ = os.WriteFile(marker, []byte("ready"), 0o644)
		for {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if os.Getenv(reexecDumpArgsEnv) == "1" {
		fmt.Println(strings.Join(os.Args[1:], "\x1f"))
		os.Exit(0)
	}
	if names := os.Getenv(reexecDumpEnvVarsEnv); names != "" {
		for _, name := range strings.Split(names, "\x1f") {
			fmt.Printf("%s=%s\n", name, os.Getenv(name))
		}
		os.Exit(0)
	}
	if args := os.Getenv(reexecEnv); args != "" {
		os.Args = append([]string{"daedalus"}, strings.Split(args, "\x1f")...)
		main()
		// Every runMainIn path routed here asserts exit 1: main() exits
		// nonzero on each of them, so this os.Exit(0) line is never reached
		// by a test's child. A child returning from main() would surface as
		// an exit-0 mismatch in that test, not a silently passing one.
		os.Exit(0)
	}
	// Scrub the ambient worker environment before the suite runs: a
	// metrics-enabled worker serving a jailed `go test` exports its relay
	// roots, registration token, and DAEDALUS_WORKER_NAME into its own
	// process env, and every var on config.AllWorkerEnvVars (the
	// config-derived provider set plus the worker's own exports) rides
	// the same shell when the config carries one — the metrics staging
	// would rewrite pinned endpoints and an ambient token would defeat
	// the "unset after a refused registration" assertions. The scrub
	// sits after the re-exec branches so re-exec'd children (runMainIn,
	// the argv dumps) keep the env their parent handed them.
	for _, name := range config.AllWorkerEnvVars() {
		os.Unsetenv(name)
	}
	os.Exit(m.Run())
}

// reexecTimeout bounds one re-exec'd child — a dispatch that exits takes
// well under a second, so a child still running at this cap is a regression
// that hung (a bypassed fail-fast dialing Temporal forever, chief among the
// classes), and the test fails on it in seconds instead of stalling the
// suite to the package timeout.
const reexecTimeout = 30 * time.Second

// runMainIn re-executes the test binary as the daedalus CLI with args (and
// dir as the working directory, "" inheriting this process's) and returns its
// stdout, stderr, and exit code. The child's arguments travel via reexecEnv,
// never os.Setenv, so nothing leaks into this process. A child still running
// at reexecTimeout fails the test — see runMainWithin.
func runMainIn(t *testing.T, dir string, args ...string) (stdout, stderr string, exitCode int) {
	out, errS, code, hung := runMainWithin(t, reexecTimeout, dir, args...)
	if hung {
		t.Fatalf("daedalus %v did not exit within %s — a regression hung the child (stdout so far %q, stderr so far %q)",
			args, reexecTimeout, out, errS)
	}
	return out, errS, code
}

// runMainWithin is runMainIn with an explicit deadline: a child still
// running after within is killed and hung comes back true (exitCode is then
// meaningless), so a hang surfaces as this test's failure in seconds — not
// as the package timeout panicking the binary around an orphaned
// grandchild. The kill takes the child's whole process group: the
// re-exec'd child leads it, so anything it spawned (a daemon re-exec, a
// wedged CLI) dies with it instead of outliving the killed test binary.
func runMainWithin(t *testing.T, within time.Duration, dir string, args ...string) (stdout, stderr string, exitCode int, hung bool) {
	t.Helper()
	// A zero-arg call would join to an empty payload, which TestMain's guard
	// reads as absent — the child would re-run the whole test suite.
	if len(args) == 0 {
		t.Fatal("runMainWithin needs at least one CLI argument")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test binary: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	// A descendant that escaped the group would hold the output pipes open;
	// Wait then returns anyway instead of blocking behind it.
	cmd.WaitDelay = 5 * time.Second
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), reexecEnv+"="+strings.Join(args, "\x1f"))
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	runErr := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return out.String(), errBuf.String(), -1, true
	}
	if runErr != nil {
		var exitErr *exec.ExitError
		if !errors.As(runErr, &exitErr) {
			t.Fatalf("run daedalus %v: %v", args, runErr)
		}
		return out.String(), errBuf.String(), exitErr.ExitCode(), false
	}
	return out.String(), errBuf.String(), 0, false
}

// TestRunMainWithinDeadline pins the re-exec harness's deadline: a child
// stuck past it is killed — process group whole — and the caller is told,
// instead of the hang stalling the suite to the package timeout while the
// killed test binary leaves a reparented grandchild running (the class a
// bypassed fail-fast exposed: the child dialed Temporal forever). A
// sleeping `opencode` stub is the deterministic stuck child: `log --status`
// consults the session list, so the child blocks in that read while the
// stub's sleep counts far past any cap.
func TestRunMainWithinDeadline(t *testing.T) {
	stubDir := t.TempDir()
	pidFile := filepath.Join(t.TempDir(), "stub.pid")
	script := "#!/bin/sh\necho $$ > " + pidFile + "\nexec sleep 60\n"
	if err := os.WriteFile(filepath.Join(stubDir, "opencode"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", stubDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	home := t.TempDir()
	t.Setenv("HOME", home)              // claude's and pi's dirs off the host — and the child's state home rides it
	t.Setenv("CODEX_HOME", t.TempDir()) // codex's rollout tree off the host

	// The brief only runs once the task log exists; the child resolves its
	// own state home from the HOME above, so the probe's log lands there
	// under an id no real run can hold, and is removed after.
	id := fmt.Sprintf("daedalus-hangprobe-%d", time.Now().UnixNano())
	logDir := filepath.Join(home, ".local", "state", "daedalus")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(logDir, id+".log")
	started := "=== 2026-10-07T09:00:00Z jailed claude round started: stage=dev worktree=/wt pgid=1 (run abcd1234) ===\n"
	if err := os.WriteFile(logPath, []byte(started), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(logPath) })

	begin := time.Now()
	stdout, stderr, _, hung := runMainWithin(t, 750*time.Millisecond, "", "log", id, "--status")
	elapsed := time.Since(begin)
	if !hung {
		t.Fatalf("child exited on its own within %v (stdout %q, stderr %q) — the deadline never fired for a child stuck on a 60s stub", elapsed, stdout, stderr)
	}
	if elapsed > 10*time.Second {
		t.Errorf("deadline fired after %v, want seconds — not the child's own timeout or the package's", elapsed)
	}

	// The kill took the group: the stub's sleep is gone (a zombie is fine —
	// it holds nothing and dials nothing), not an orphan counting down its
	// 60 seconds.
	pidBytes, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("read stub pid: %v", err)
	}
	pid := strings.TrimSpace(string(pidBytes))
	t.Cleanup(func() {
		if p, err := strconv.Atoi(pid); err == nil {
			syscall.Kill(p, syscall.SIGKILL)
		}
	})
	alive := func() bool {
		b, err := os.ReadFile("/proc/" + pid + "/stat")
		if err != nil {
			return false // gone
		}
		fields := strings.Fields(string(b))
		return len(fields) >= 3 && fields[2] != "Z"
	}
	deadline := time.Now().Add(5 * time.Second)
	for alive() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if alive() {
		t.Errorf("the stub process %s outlived the killed child — the deadline kill did not take the process group", pid)
	}
}

// validConfig writes a minimal loadable config in a fresh temp dir and
// returns its path, so subprocess cases control config resolution instead of
// depending on ./config.yaml or ~/.config/daedalus on the host.
func validConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeConfig(t, path)
	return path
}

// TestMainUsageFail pins usageFail's contract in a subprocess: the diagnostic
// on stderr, then a blank line, then the usage text, then exit 1 — for a bad
// `list` argument, an unknown subcommand, an unknown worker action, and a
// `worker wakeup` invocation whose workflow id count is not exactly one.
func TestMainUsageFail(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "list with a non-numeric max",
			args: []string{"list", "notanumber"},
			want: `list takes an optional maximum number of sessions (got "notanumber")`,
		},
		{
			name: "unknown subcommand",
			args: []string{"frobnicate"},
			want: `unknown subcommand "frobnicate"`,
		},
		{
			name: "unknown worker action",
			args: []string{"worker", "dance"},
			want: `unknown worker action "dance" (start, stop, status, restart, wakeup, report, foreground)`,
		},
		{
			name: "worker wakeup without an id",
			args: []string{"worker", "wakeup"},
			want: `wakeup takes <workflow-id>`,
		},
		{
			name: "worker wakeup with two ids",
			args: []string{"worker", "wakeup", "daedalus-issue-42", "daedalus-issue-43"},
			want: `wakeup takes <workflow-id>`,
		},
		{
			name: "wipe without an id",
			args: []string{"wipe"},
			want: `wipe takes <workflow-id>`,
		},
		{
			name: "wipe with two ids",
			args: []string{"wipe", "daedalus-issue-42", "daedalus-issue-43"},
			want: `wipe takes <workflow-id>`,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			args := append([]string{"-c", validConfig(t)}, c.args...)
			stdout, stderr, code := runMainIn(t, "", args...)
			if code != 1 {
				t.Errorf("daedalus %v exit code = %d, want 1", c.args, code)
			}
			if stdout != "" {
				t.Errorf("daedalus %v stdout = %q, want empty", c.args, stdout)
			}
			// The message, then a blank line, then the usage text — never the
			// message alone, the usage alone, or a single-newline join. (A
			// bare Contains "\n\nUsage:" would be vacuous: the usage constant
			// itself contains that sequence in its header.)
			if !strings.HasPrefix(stderr, c.want+"\n\n") {
				t.Errorf("stderr should start with %q followed by a blank line, got %q", c.want, stderr)
			}
			if !strings.HasSuffix(stderr, usage) {
				t.Errorf("stderr should end with the usage text, got %q", stderr)
			}
		})
	}
}

// TestMainExitfLoadConfig pins exitf's contract through loadConfig: a config
// that cannot be read produces a single diagnostic line — no usage text — and
// exit 1.
func TestMainExitfLoadConfig(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-config.yaml")
	stdout, stderr, code := runMainIn(t, "", "-c", missing, "list")
	if code != 1 {
		t.Errorf("daedalus list (missing config) exit code = %d, want 1", code)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
	if !strings.HasPrefix(stderr, "load config: ") {
		t.Errorf("stderr should start with the load config diagnostic, got %q", stderr)
	}
	if !strings.Contains(stderr, "no-such-config.yaml") {
		t.Errorf("stderr should name the config path, got %q", stderr)
	}
	if strings.Contains(stderr, "Usage:") {
		t.Errorf("exitf must not append the usage text, got %q", stderr)
	}
}

// TestMainFailInit pins fail's contract: an init that cannot write its file
// reports "init failed: <err>" — no usage text — and exits 1. A read-only
// working directory makes writeExampleConfig fail offline and deterministically.
func TestMainFailInit(t *testing.T) {
	// Root ignores the 0555 mode bits below, so the write would succeed and
	// this test would fail (straying a config-example.yaml into the dir).
	if os.Geteuid() == 0 {
		t.Skip("running as root: the read-only-cwd failure injection does not fail")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o0555); err != nil {
		t.Fatal(err)
	}
	// Restore writability before t.TempDir's cleanup tries to remove the
	// directory (cleanups run LIFO, so this one runs first).
	t.Cleanup(func() { os.Chmod(dir, 0o0755) })

	stdout, stderr, code := runMainIn(t, dir, "init")
	if code != 1 {
		t.Errorf("daedalus init (read-only cwd) exit code = %d, want 1", code)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
	if !strings.HasPrefix(stderr, "init failed: ") {
		t.Errorf("stderr should start with the init failure, got %q", stderr)
	}
	if !strings.Contains(stderr, "config-example.yaml") {
		t.Errorf("stderr should name the file init tried to write, got %q", stderr)
	}
	if strings.Contains(stderr, "Usage:") {
		t.Errorf("fail must not append the usage text, got %q", stderr)
	}
}

// TestMainRunLocalConfig pins the project-local config end to end, in a
// subprocess: a fresh `run <repo-path>` loads the repo's own
// .daedalus/config.yaml even when nothing else resolves (the load error
// names that file, not "no config found"), while an explicit -c still wins
// over it. Both cases fail deterministically at load time — before any
// temporal connection — via an unloadable config at the losing location.
func TestMainRunLocalConfig(t *testing.T) {
	// A fresh home, so neither case can lean on the host's
	// ~/.config/daedalus/config.yaml; the child inherits this env.
	t.Setenv("HOME", t.TempDir())

	t.Run("repo-local config replaces the default resolution", func(t *testing.T) {
		repo := t.TempDir()
		local := filepath.Join(repo, ".daedalus", "config.yaml")
		if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(local, []byte("\tnot: yaml"), 0o644); err != nil {
			t.Fatal(err)
		}

		stdout, stderr, code := runMainIn(t, t.TempDir(), "run", repo, "42", "do it")
		if code != 1 {
			t.Errorf("daedalus run (unloadable repo-local config) exit code = %d, want 1", code)
		}
		if stdout != "" {
			t.Errorf("stdout = %q, want empty", stdout)
		}
		if !strings.HasPrefix(stderr, "load config: parse config "+local) {
			t.Errorf("stderr should name the repo-local config, got %q", stderr)
		}
		if strings.Contains(stderr, "no config found") {
			t.Errorf("stderr should not fall back to the no-config diagnostic, got %q", stderr)
		}
	})

	t.Run("explicit -c wins over the repo-local config", func(t *testing.T) {
		given := validConfig(t) + ".bad"
		if err := os.WriteFile(given, []byte("\tnot: yaml"), 0o644); err != nil {
			t.Fatal(err)
		}
		repo := t.TempDir()
		local := filepath.Join(repo, ".daedalus", "config.yaml")
		if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
			t.Fatal(err)
		}
		writeConfig(t, local)

		stdout, stderr, code := runMainIn(t, t.TempDir(), "-c", given, "run", repo, "42", "do it")
		if code != 1 {
			t.Errorf("daedalus -c <bad> run (loadable repo-local config) exit code = %d, want 1", code)
		}
		if stdout != "" {
			t.Errorf("stdout = %q, want empty", stdout)
		}
		if !strings.HasPrefix(stderr, "load config: parse config "+given) {
			t.Errorf("stderr should name the -c config, got %q", stderr)
		}
		if strings.Contains(stderr, local) {
			t.Errorf("the repo-local config must not be loaded over an explicit -c, got %q", stderr)
		}
	})

	t.Run("repo-local config beats the cwd repo-local config", func(t *testing.T) {
		// A repo-local config in the invoking shell's cwd must not shadow the
		// target repo's own: `run <other-repo>` configures the run for the
		// target repo. The cwd's config is loadable, the repo's is not, so
		// whichever one wins makes the failure deterministic at load time.
		cwd := t.TempDir()
		cwdLocal := filepath.Join(cwd, ".daedalus", "config.yaml")
		if err := os.MkdirAll(filepath.Dir(cwdLocal), 0o755); err != nil {
			t.Fatal(err)
		}
		writeConfig(t, cwdLocal)

		repo := t.TempDir()
		local := filepath.Join(repo, ".daedalus", "config.yaml")
		if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(local, []byte("\tnot: yaml"), 0o644); err != nil {
			t.Fatal(err)
		}

		stdout, stderr, code := runMainIn(t, cwd, "run", repo, "42", "do it")
		if code != 1 {
			t.Errorf("daedalus run from a configured cwd (unloadable repo-local config) exit code = %d, want 1", code)
		}
		if stdout != "" {
			t.Errorf("stdout = %q, want empty", stdout)
		}
		if !strings.HasPrefix(stderr, "load config: parse config "+local) {
			t.Errorf("stderr should name the target repo's local config, got %q", stderr)
		}
		if strings.Contains(stderr, cwdLocal) {
			t.Errorf("the cwd repo-local config must not shadow the target repo's, got %q", stderr)
		}
	})
}

// TestMainRunAppendReadTaskFileError pins the append-mode task-file failure:
// an unreadable -f/--file is reported as "read task file: <err>" without the
// usage text, exit 1, once the config has loaded.
func TestMainRunAppendReadTaskFileError(t *testing.T) {
	cfg := validConfig(t)
	missing := filepath.Join(t.TempDir(), "no-such-task.md")
	stdout, stderr, code := runMainIn(t, "", "-c", cfg, "-f", missing, "-a", "wf-1", "run")
	if code != 1 {
		t.Errorf("daedalus run -a with missing task file exit code = %d, want 1", code)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
	if !strings.HasPrefix(stderr, "read task file: ") {
		t.Errorf("stderr should start with the read task file diagnostic, got %q", stderr)
	}
	if strings.Contains(stderr, "Usage:") {
		t.Errorf("the task-file exit must not append the usage text, got %q", stderr)
	}
}

// TestMainWorkerStatusRejectsConfig pins the -c rejection's exit contract in
// a subprocess for `worker status`, the other record-driven worker command:
// usageFail's shape (diagnostic, blank line, usage text) and exit 1 —
// asserted on a path that fails in parseFlags, before the real daemon dir is
// ever read, so the live workers in /tmp/daedalus are never touched. The
// command's success path (record-driven, no config) is covered in-process by
// TestMainWorkerStatusNeedsNoConfig.
func TestMainWorkerStatusRejectsConfig(t *testing.T) {
	cfg := validConfig(t)

	stdout, stderr, code := runMainIn(t, "", "-c", cfg, "worker", "status")
	if code != 1 {
		t.Errorf("daedalus -c <config> worker status exit code = %d, want 1", code)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
	if !strings.HasPrefix(stderr, "-c/--config does not apply to worker restart all, restart <worker>, or worker status\n\n") {
		t.Errorf("stderr should start with the rejection and a blank line, got %q", stderr)
	}
	if !strings.HasSuffix(stderr, usage) {
		t.Errorf("stderr should end with the usage text, got %q", stderr)
	}
}

// TestReadTaskFile pins readTaskFile's own contract in-process: the file's
// bytes verbatim on success, and a "read task file:" error wrapping the
// underlying path error on failure.
func TestReadTaskFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "task.md")
	if err := os.WriteFile(file, []byte("do the thing"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := readTaskFile(file)
	if err != nil {
		t.Fatalf("readTaskFile: %v", err)
	}
	if got != "do the thing" {
		t.Errorf("readTaskFile = %q, want the file verbatim", got)
	}

	_, err = readTaskFile(filepath.Join(t.TempDir(), "missing.md"))
	if err == nil || !strings.Contains(err.Error(), "read task file:") {
		t.Errorf("readTaskFile(missing) error = %v, want a read task file diagnostic", err)
	}
	if _, ok := errors.AsType[*fs.PathError](err); !ok {
		t.Errorf("readTaskFile(missing) should wrap the path error, got %T", err)
	}
}

// TestReadTaskFileGlob pins the glob expansion of -f/--file: every matching
// file concatenated in lexical order with a blank line between parts,
// directories among the matches skipped rather than failing the read, and a
// "read task file:" error when nothing matches — distinguishing a pattern
// with no matches from a plain path that does not exist (which surfaces the
// underlying path error, pinned by TestReadTaskFile).
func TestReadTaskFileGlob(t *testing.T) {
	dir := t.TempDir()
	for name, content := range map[string]string{
		"b.md": "second task",
		"a.md": "first task",
		"c.md": "third task",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "d.md"), 0o755); err != nil {
		t.Fatal(err)
	}

	got, err := readTaskFile(filepath.Join(dir, "*.md"))
	if err != nil {
		t.Fatalf("readTaskFile(%s/*.md): %v", dir, err)
	}
	if want := "first task\n\nsecond task\n\nthird task"; got != want {
		t.Errorf("readTaskFile(%s/*.md) = %q, want matches joined in lexical order: %q", dir, got, want)
	}

	// A pattern that matches only a directory reads nothing: no error from
	// the skipped directory, but no content either.
	if _, err := readTaskFile(filepath.Join(dir, "d.md")); err == nil || !strings.Contains(err.Error(), "no matching files") {
		t.Errorf("readTaskFile(dir only) error = %v, want a no matching files diagnostic", err)
	}
	if _, err := readTaskFile(filepath.Join(dir, "z*.md")); err == nil || !strings.Contains(err.Error(), "no matching files") {
		t.Errorf("readTaskFile(no glob matches) error = %v, want a no matching files diagnostic", err)
	}

	// A malformed pattern is a usage error, not a silent empty read.
	if _, err := readTaskFile(filepath.Join(dir, "[")); err == nil || !strings.Contains(err.Error(), "read task file:") {
		t.Errorf("readTaskFile(malformed pattern) error = %v, want a read task file diagnostic", err)
	}
}

// TestMainCommandHelp pins "daedalus <command> --help" (and its -h/help
// spellings): every command documented in the root usage prints its own
// detailed help screen on stdout, exit 0, before any config resolution —
// help must work where no config can be found. An unknown command gets no
// such screen: it falls through to the unknown-subcommand rejection.
func TestMainCommandHelp(t *testing.T) {
	for _, cmd := range []string{"run", "continue", "guide", "attach", "list", "log", "wipe", "worker", "dash", "init", "config", "version", "completion"} {
		for _, spelling := range []string{"--help", "-h", "help"} {
			stdout, stderr, code := runMainIn(t, "", cmd, spelling)
			if code != 0 {
				t.Errorf("daedalus %s %s exit code = %d, want 0", cmd, spelling, code)
			}
			if stderr != "" {
				t.Errorf("daedalus %s %s stderr = %q, want empty", cmd, spelling, stderr)
			}
			if want := commandHelp[cmd]; stdout != want {
				t.Errorf("daedalus %s %s stdout = %q, want the command's help screen (%q)", cmd, spelling, stdout, want)
			}
		}
	}

	stdout, stderr, code := runMainIn(t, "", "-c", validConfig(t), "frobnicate", "--help")
	if code != 1 {
		t.Errorf("daedalus frobnicate --help exit code = %d, want 1", code)
	}
	if stdout != "" || !strings.Contains(stderr, `unknown subcommand "frobnicate"`) {
		t.Errorf("daedalus frobnicate --help printed stdout %q stderr %q, want the unknown-subcommand rejection", stdout, stderr)
	}
}

// TestMainVersionSubcommand pins the version subcommand: the same output
// as -v/--version, exit 0, and config-exempt like the flags.
func TestMainVersionSubcommand(t *testing.T) {
	stdout, stderr, code := runMainIn(t, "", "version")
	if code != 0 {
		t.Errorf("daedalus version exit code = %d, want 0", code)
	}
	if stderr != "" {
		t.Errorf("daedalus version stderr = %q, want empty", stderr)
	}
	if want := version.String() + "\n"; stdout != want {
		t.Errorf("daedalus version printed %q, want %q", stdout, want)
	}
}

// TestVersionHelpMatchesDispatch pins the version help wording to the
// dispatch's actual contract: `daedalus version` prints exactly one token
// (version.String()), so neither the root usage nor the command help may
// promise anything the dispatch does not emit. The named regression is the
// "and build details" claim that no test guarded for five review rounds and
// that halted daedalus-issue-cli-option-help; if you change the dispatch's
// output, change the help here in the same commit.
func TestVersionHelpMatchesDispatch(t *testing.T) {
	for name, doc := range map[string]string{"root usage": usage, "version help": commandHelp["version"]} {
		if strings.Contains(doc, "build detail") {
			t.Errorf("the %s promises \"build details\", but the dispatch prints only version.String() — docs contradict behavior", name)
		}
	}
	if want := "print the CLI version"; !strings.Contains(commandHelp["version"], want) {
		t.Errorf("version help %q should describe the one-token contract (%q)", commandHelp["version"], want)
	}
}

// TestSpawnDaemonScrubsWorkerEnv pins the daemon-spawn contract at the one
// helper both spawn sites share: the child re-executes this binary in its
// own session on an environment scrubbed of every config.AllWorkerEnvVars
// member — a stale provider export or a leftover bridge config in the
// invoking shell must never win over the recorded config — with the daemon
// marker re-appended and every non-worker var untouched, stdout captured
// into the daemon log, and the recorded pid matching the returned one.
func TestSpawnDaemonScrubsWorkerEnv(t *testing.T) {
	// daemonDir is global; point it at a throwaway dir rather than the
	// real task-log dir (spawnDaemon mkdirs it and prunes old logs there).
	reset := daemonDir
	daemonDir = t.TempDir()
	t.Cleanup(func() { daemonDir = reset })

	dir := t.TempDir()
	pidFile := filepath.Join(dir, "spawn.pid")
	logFile := filepath.Join(dir, "spawn.log")

	t.Setenv("DAEDALUS_WORKER_NAME", "spawn-fixture")
	t.Setenv("ANTHROPIC_API_KEY", "sk-stale")
	t.Setenv("OPENCODE_CONFIG", "/leftover/project.yaml")
	t.Setenv("DAEDALUS_TEST_SPAWN_KEEPVAR", "kept")
	t.Setenv(reexecDumpEnvVarsEnv, strings.Join([]string{
		"DAEDALUS_WORKER_DAEMON",
		"DAEDALUS_WORKER_NAME",
		"ANTHROPIC_API_KEY",
		"OPENCODE_CONFIG",
		"DAEDALUS_TEST_SPAWN_KEEPVAR",
	}, "\x1f"))

	pid, err := spawnDaemon(pidFile, logFile)
	if err != nil {
		t.Fatalf("spawnDaemon: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })

	// The daemon is detached; its stdout lands in the log file, so the
	// dump's arrival is the liveness and log-capture proof in one.
	want := map[string]string{
		"DAEDALUS_WORKER_DAEMON":      "1",    // re-appended after the scrub
		"DAEDALUS_WORKER_NAME":        "",     // worker export → scrubbed
		"ANTHROPIC_API_KEY":           "",     // stale provider export → scrubbed
		"OPENCODE_CONFIG":             "",     // leftover bridge config → scrubbed
		"DAEDALUS_TEST_SPAWN_KEEPVAR": "kept", // the scrub is surgical, not a wipe
	}
	var last string
	deadline := time.Now().Add(10 * time.Second)
	for {
		if data, err := os.ReadFile(logFile); err == nil {
			last = string(data)
			for _, line := range strings.Split(strings.TrimRight(last, "\n"), "\n") {
				name, value, ok := strings.Cut(line, "=")
				if !ok {
					continue
				}
				wantValue, probed := want[name]
				if !probed {
					continue
				}
				// Report a mismatched var once: deleting on the first
				// sighting keeps the poll loop from re-erroring the same
				// line every tick until the deadline.
				delete(want, name)
				if value != wantValue {
					t.Errorf("daemon env %s = %q, want %q", name, value, wantValue)
				}
			}
			if len(want) == 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			for name := range want {
				t.Errorf("daemon env dump never reported %s (log tail %q)", name, last)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	recorded, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("read pid file: %v", err)
	}
	if got, err := strconv.Atoi(strings.TrimSpace(string(recorded))); err != nil || got != pid {
		t.Errorf("pid file = %q, want %d", recorded, pid)
	}
}
