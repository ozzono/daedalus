package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

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

// TestMain is the re-exec guard for the exit-code paths: exitf, fail,
// usageFail, and loadConfig all call os.Exit(1), so they cannot run inside a
// test process. Tests route them through runMainIn, which execs this same
// binary with reexecEnv set; here that arrival is detected before the test
// framework starts.
func TestMain(m *testing.M) {
	if os.Getenv(reexecDumpArgsEnv) == "1" {
		fmt.Println(strings.Join(os.Args[1:], "\x1f"))
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
	os.Exit(m.Run())
}

// runMainIn re-executes the test binary as the daedalus CLI with args (and
// dir as the working directory, "" inheriting this process's) and returns its
// stdout, stderr, and exit code. The child's arguments travel via reexecEnv,
// never os.Setenv, so nothing leaks into this process.
func runMainIn(t *testing.T, dir string, args ...string) (stdout, stderr string, exitCode int) {
	t.Helper()
	// A zero-arg call would join to an empty payload, which TestMain's guard
	// reads as absent — the child would re-run the whole test suite.
	if len(args) == 0 {
		t.Fatal("runMainIn needs at least one CLI argument")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test binary: %v", err)
	}
	cmd := exec.Command(exe)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), reexecEnv+"="+strings.Join(args, "\x1f"))
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	runErr := cmd.Run()
	if runErr != nil {
		var exitErr *exec.ExitError
		if !errors.As(runErr, &exitErr) {
			t.Fatalf("run daedalus %v: %v", args, runErr)
		}
		return out.String(), errBuf.String(), exitErr.ExitCode()
	}
	return out.String(), errBuf.String(), 0
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
			want: `unknown worker action "dance" (start, stop, status, restart, wakeup, foreground)`,
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
	for _, cmd := range []string{"run", "continue", "guide", "attach", "list", "log", "wipe", "worker", "init", "config", "version"} {
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
