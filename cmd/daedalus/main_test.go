package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/version"
	"github.com/ozzono/daedalus/internal/workflows"
)

// TestParseFlagsDetach covers both spellings and that -d never consumes a
// positional argument.
func TestParseFlagsDetach(t *testing.T) {
	for _, flag := range []string{"-d", "--detach"} {
		f, rest, err := parseFlags([]string{"run", flag, "/repo", "42", "do it"})
		if err != nil {
			t.Fatalf("parseFlags(%q): %v", flag, err)
		}
		if !f.detach {
			t.Errorf("parseFlags(%q) detach = false, want true", flag)
		}
		wantRest := []string{"run", "/repo", "42", "do it"}
		if !reflect.DeepEqual(rest, wantRest) {
			t.Errorf("parseFlags(%q) rest = %v, want %v", flag, rest, wantRest)
		}
	}

	f, rest, err := parseFlags([]string{"run", "/repo", "42", "do it"})
	if err != nil {
		t.Fatalf("parseFlags(no -d): %v", err)
	}
	if f.detach {
		t.Error("detach should default to false")
	}
	if len(rest) != 4 {
		t.Errorf("parseFlags(no -d) rest = %v", rest)
	}
}

// TestParseFlagsAppend covers both spellings and that -a consumes exactly
// one value while the prompt stays positional.
func TestParseFlagsAppend(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"run", "-a", "daedalus-issue-0", "steer it"}, "daedalus-issue-0"},
		{[]string{"run", "--append", "daedalus-issue-0", "steer it"}, "daedalus-issue-0"},
		{[]string{"run", "--append=daedalus-issue-1", "steer it"}, "daedalus-issue-1"},
	} {
		f, rest, err := parseFlags(c.args)
		if err != nil {
			t.Fatalf("parseFlags(%q): %v", c.args, err)
		}
		if f.appendID != c.want {
			t.Errorf("parseFlags(%q) appendID = %q, want %q", c.args, f.appendID, c.want)
		}
		wantRest := []string{"run", "steer it"}
		if !reflect.DeepEqual(rest, wantRest) {
			t.Errorf("parseFlags(%q) rest = %v, want %v", c.args, rest, wantRest)
		}
	}

	if _, _, err := parseFlags([]string{"run", "-a"}); err == nil {
		t.Error("-a without a value should error")
	}
}

// TestParseFlagsPrefix covers both spellings of the run prefix flag, that an
// unusable prefix is rejected at parse time, and that -p consumes exactly one
// value.
func TestParseFlagsPrefix(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"run", "-p", "team", "/repo", "42", "do it"}, "team"},
		{[]string{"run", "--prefix", "team", "/repo", "42", "do it"}, "team"},
		{[]string{"run", "--prefix=team/ship", "/repo", "42", "do it"}, "team/ship"},
	} {
		f, rest, err := parseFlags(c.args)
		if err != nil {
			t.Fatalf("parseFlags(%q): %v", c.args, err)
		}
		if f.branchPrefix != c.want {
			t.Errorf("parseFlags(%q) branchPrefix = %q, want %q", c.args, f.branchPrefix, c.want)
		}
		wantRest := []string{"run", "/repo", "42", "do it"}
		if !reflect.DeepEqual(rest, wantRest) {
			t.Errorf("parseFlags(%q) rest = %v, want %v", c.args, rest, wantRest)
		}
	}

	if _, _, err := parseFlags([]string{"run", "-p"}); err == nil {
		t.Error("-p without a value should error")
	}
	// A reserved namespace must fail here, not at finalize after the run.
	if _, _, err := parseFlags([]string{"run", "-p", "feat", "/repo", "42", "do it"}); err == nil ||
		!strings.Contains(err.Error(), "reserved") {
		t.Errorf("parseFlags(-p feat) err = %v, want a reserved-namespace rejection", err)
	}
	// -p names a fresh-run option; elsewhere — append mode included — it
	// would be silently ignored.
	if _, _, err := parseFlags([]string{"guide", "-p", "team", "wf-1", "hi"}); err == nil ||
		!strings.Contains(err.Error(), "only applies to run") {
		t.Errorf("parseFlags(guide -p) err = %v, want a -p-outside-run rejection", err)
	}
	if _, _, err := parseFlags([]string{"run", "-a", "wf-1", "-p", "team", "steer it"}); err == nil ||
		!strings.Contains(err.Error(), "only applies to run") {
		t.Errorf("parseFlags(run -a -p) err = %v, want a -p-in-append-mode rejection", err)
	}
}

// TestParseFlagsCLI covers both spellings of the run agent flag, that an
// unknown agent is rejected at parse time (with the same error Load would
// give), and that -cli is refused outside a fresh `run`: the worker no longer
// takes it (the selection travels with the run, not the worker), and append
// mode targets a pipeline whose agent is already fixed.
func TestParseFlagsCLI(t *testing.T) {
	for _, c := range []struct {
		args     []string
		want     string
		wantRest []string
	}{
		{[]string{"run", "-cli", "amp", "/repo", "42", "do it"}, "amp", []string{"run", "/repo", "42", "do it"}},
		{[]string{"run", "--cli", "opencode", "/repo", "42", "do it"}, "opencode", []string{"run", "/repo", "42", "do it"}},
		{[]string{"--cli=amp", "run", "/repo", "42", "do it"}, "amp", []string{"run", "/repo", "42", "do it"}},
	} {
		f, rest, err := parseFlags(c.args)
		if err != nil {
			t.Fatalf("parseFlags(%q): %v", c.args, err)
		}
		if f.agentCLI != c.want {
			t.Errorf("parseFlags(%q) agentCLI = %q, want %q", c.args, f.agentCLI, c.want)
		}
		wantRest := c.wantRest
		if !reflect.DeepEqual(rest, wantRest) {
			t.Errorf("parseFlags(%q) rest = %v, want %v", c.args, rest, wantRest)
		}
	}

	if _, _, err := parseFlags([]string{"run", "-cli"}); err == nil {
		t.Error("-cli without a value should error")
	}
	// The override must fail here, not at pipeline start.
	if _, _, err := parseFlags([]string{"run", "-cli", "cursor", "/repo", "42", "do it"}); err == nil ||
		!strings.Contains(err.Error(), "unknown agent") {
		t.Errorf("parseFlags(-cli cursor) err = %v, want an unknown-agent rejection", err)
	}
	// -cli names a run option; the worker would silently ignore it.
	if _, _, err := parseFlags([]string{"worker", "start", "-cli", "amp"}); err == nil ||
		!strings.Contains(err.Error(), "only applies to run") {
		t.Errorf("parseFlags(worker -cli) err = %v, want a -cli-outside-run rejection", err)
	}
	// Append mode targets a pipeline whose agent is already fixed.
	if _, _, err := parseFlags([]string{"run", "-a", "wf-1", "-cli", "amp", "steer it"}); err == nil ||
		!strings.Contains(err.Error(), "only applies to run") {
		t.Errorf("parseFlags(run -a -cli) err = %v, want a -cli-in-append-mode rejection", err)
	}
}

// TestResolveConfigPath pins the discovery order: an explicit -c is honored
// verbatim, ./config.yaml wins, ./.daedalus/config.yaml comes next (the
// repo-local config of the repo the shell sits in), and the home config
// (~/.config/daedalus/config.yaml) is found when neither working-directory
// location has one — the property that lets the CLI run from any directory.
func TestResolveConfigPath(t *testing.T) {
	for _, c := range []struct {
		name string
		// writeCwdConfig, writeRepoLocalConfig, and writeHomeConfig seed the
		// lookup locations for this case; each subtest gets a fresh home and
		// workdir, so cases cannot see each other's configs.
		writeCwdConfig       bool
		writeRepoLocalConfig bool
		writeHomeConfig      bool
		given                string
		// wantCwdConfig/wantRepoLocalConfig/wantHomeConfig name that
		// location's config.yaml as the expected result (its path is only
		// known inside the subtest).
		wantCwdConfig       bool
		wantRepoLocalConfig bool
		wantHomeConfig      bool
		want                string
		wantErrSubstring    string
	}{
		{
			name:  "explicit -c is verbatim",
			given: "/elsewhere/config.yaml",
			want:  "/elsewhere/config.yaml",
		},
		{
			name:            "cwd config wins over repo-local and home",
			writeCwdConfig:  true,
			writeHomeConfig: true,
			wantCwdConfig:   true,
		},
		{
			name:                 "cwd config wins over repo-local",
			writeCwdConfig:       true,
			writeRepoLocalConfig: true,
			wantCwdConfig:        true,
		},
		{
			name:                 "repo-local config wins over home",
			writeRepoLocalConfig: true,
			writeHomeConfig:      true,
			wantRepoLocalConfig:  true,
		},
		{
			name:                 "repo-local config found from a bare directory",
			writeRepoLocalConfig: true,
			wantRepoLocalConfig:  true,
		},
		{
			name:            "home config found from a bare directory",
			writeHomeConfig: true,
			wantHomeConfig:  true,
		},
		{
			name:             "no config anywhere",
			wantErrSubstring: "no config found — looked for ./config.yaml, ./.daedalus/config.yaml, and",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			homeCfg := filepath.Join(home, homeConfigDir, defaultConfigPath)
			if err := os.MkdirAll(filepath.Dir(homeCfg), 0o755); err != nil {
				t.Fatal(err)
			}
			workdir := t.TempDir()
			if c.writeCwdConfig {
				writeConfig(t, filepath.Join(workdir, defaultConfigPath))
			}
			if c.writeRepoLocalConfig {
				repoLocal := filepath.Join(workdir, ".daedalus", defaultConfigPath)
				if err := os.MkdirAll(filepath.Dir(repoLocal), 0o755); err != nil {
					t.Fatal(err)
				}
				writeConfig(t, repoLocal)
			}
			if c.writeHomeConfig {
				writeConfig(t, homeCfg)
			}
			t.Chdir(workdir)

			// Cases without an explicit given exercise the parseFlags
			// default, never an empty string (Abs("") would be the cwd).
			given := c.given
			if given == "" {
				given = defaultConfigPath
			}
			got, err := resolveConfigPath(given)
			if c.wantErrSubstring != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErrSubstring) {
					t.Errorf("resolveConfigPath error = %v, want it to contain %q", err, c.wantErrSubstring)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveConfigPath: %v", err)
			}
			var want string
			switch {
			case c.wantCwdConfig:
				want = filepath.Join(workdir, defaultConfigPath)
			case c.wantRepoLocalConfig:
				want = filepath.Join(workdir, ".daedalus", defaultConfigPath)
			case c.wantHomeConfig:
				want = homeCfg
			default:
				want = c.want
			}
			if got != want {
				t.Errorf("resolveConfigPath = %q, want %q", got, want)
			}
		})
	}
}

// TestRunLocalConfigPath pins the project-local config lookup for a fresh
// `daedalus run <repo-path>`: <repo-path>/.daedalus/config.yaml is selected
// (returned absolute) only for the run subcommand, only without an explicit
// -c, and only outside append mode — everything else falls back to the
// default resolution.
func TestRunLocalConfigPath(t *testing.T) {
	repoWithLocal := t.TempDir()
	local := filepath.Join(repoWithLocal, ".daedalus", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
		t.Fatal(err)
	}
	writeConfig(t, local)

	bareRepo := t.TempDir()

	defaults := flags{configPath: defaultConfigPath, workflow: defaultWorkflowName}

	t.Run("repo-local config is selected", func(t *testing.T) {
		got, ok := runLocalConfigPath([]string{"run", repoWithLocal, "42", "do it"}, defaults)
		if !ok {
			t.Fatal("runLocalConfigPath should select the repo-local config")
		}
		if got != local {
			t.Errorf("runLocalConfigPath = %q, want %q", got, local)
		}
	})

	t.Run("relative repo path resolves absolute", func(t *testing.T) {
		workdir := t.TempDir()
		rel := "repo"
		local := filepath.Join(workdir, rel, ".daedalus", "config.yaml")
		if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
			t.Fatal(err)
		}
		writeConfig(t, local)
		t.Chdir(workdir)

		got, ok := runLocalConfigPath([]string{"run", rel, "42", "do it"}, defaults)
		if !ok {
			t.Fatal("runLocalConfigPath should select the repo-local config")
		}
		if !filepath.IsAbs(got) {
			t.Errorf("runLocalConfigPath = %q, want an absolute path", got)
		}
		if got != local {
			t.Errorf("runLocalConfigPath = %q, want %q", got, local)
		}
	})

	t.Run("explicit -c wins", func(t *testing.T) {
		f := defaults
		f.configPath, f.configSet = "/elsewhere/config.yaml", true
		if _, ok := runLocalConfigPath([]string{"run", repoWithLocal, "42", "do it"}, f); ok {
			t.Error("runLocalConfigPath should defer to an explicit -c")
		}
	})

	t.Run("append mode has no repo path", func(t *testing.T) {
		// rest is ["run", <workflow-id>, <prompt>], so without the appendID
		// guard args[1] would be probed as a repo path.
		f := defaults
		f.appendID = "wf-1"
		if _, ok := runLocalConfigPath([]string{"run", "wf-1", "steer it"}, f); ok {
			t.Error("runLocalConfigPath should skip append mode")
		}
	})

	t.Run("other subcommands are exempt", func(t *testing.T) {
		if _, ok := runLocalConfigPath([]string{"worker", repoWithLocal, "start"}, defaults); ok {
			t.Error("runLocalConfigPath should only apply to run")
		}
	})

	t.Run("no repo-local config falls back", func(t *testing.T) {
		if _, ok := runLocalConfigPath([]string{"run", bareRepo, "42", "do it"}, defaults); ok {
			t.Error("runLocalConfigPath should fall back when the repo carries none")
		}
	})

	t.Run("bare run has no repo path", func(t *testing.T) {
		if _, ok := runLocalConfigPath([]string{"run"}, defaults); ok {
			t.Error("runLocalConfigPath should skip a run without a repo path")
		}
	})
}

// writeConfig drops a minimal readable config file at path.
func writeConfig(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("agent: claude\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestPruneOldLogs pins the retention rule: logs untouched for over a week
// are removed, recent ones and non-log files stay.
func TestPruneOldLogs(t *testing.T) {
	// daemonDir is global; restore it to a throwaway dir rather than its
	// original value, so no later test can touch the real daemon dir.
	reset := t.TempDir()
	t.Cleanup(func() { daemonDir = reset })
	daemonDir = t.TempDir()

	fresh := filepath.Join(daemonDir, "worker-daedalus.log")
	stale := filepath.Join(daemonDir, "worker-old.log")
	other := filepath.Join(daemonDir, "worker-daedalus.pid")
	for _, f := range []string{fresh, stale, other} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	lastWeek := time.Now().Add(-8 * 24 * time.Hour)
	if err := os.Chtimes(stale, lastWeek, lastWeek); err != nil {
		t.Fatal(err)
	}

	pruneOldLogs()

	for _, keep := range []string{fresh, other} {
		if _, err := os.Stat(keep); err != nil {
			t.Errorf("%s should survive the prune: %v", filepath.Base(keep), err)
		}
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale log should be pruned (stat err = %v)", err)
	}
}

// TestReadLivePid pins the pid-file contract: a live pid reads back, a stale
// file (dead pid or garbage) does not.
func TestReadLivePid(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "worker-daedalus.pid")

	if err := os.WriteFile(pidFile, fmt.Appendf(nil, "%d", os.Getpid()), 0o644); err != nil {
		t.Fatal(err)
	}
	if pid, ok := readLivePid(pidFile); !ok || pid != os.Getpid() {
		t.Errorf("readLivePid = (%d, %v), want (%d, true)", pid, ok, os.Getpid())
	}

	// A pid that cannot exist (pid 1 on this machine is alive, so use a
	// garbage file instead) must read as not-live.
	if err := os.WriteFile(pidFile, []byte("not a pid"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := readLivePid(pidFile); ok {
		t.Error("readLivePid should reject a malformed pid file")
	}
}

// TestVersionFlag covers both spellings of the version flag: the CLI prints
// the embedded version and exits cleanly, without needing a config.
func TestVersionFlag(t *testing.T) {
	for _, flag := range []string{"-v", "--version"} {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		stdout, args := os.Stdout, os.Args
		os.Stdout, os.Args = w, []string{"daedalus", flag}
		main()
		w.Close()
		os.Stdout, os.Args = stdout, args

		out, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		if want := version.String() + "\n"; string(out) != want {
			t.Errorf("daedalus %s printed %q, want %q", flag, out, want)
		}
	}
}

func TestRunPrompt(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "task.md")
	if err := os.WriteFile(file, []byte("add the feature"), 0o644); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name             string
		taskFile         string
		args             []string
		want             string
		wantErrSubstring string
	}{
		{name: "inline prompt", args: []string{"/repo", "42", "add the feature"}, want: "add the feature"},
		{name: "file instead of prompt", taskFile: file, args: []string{"/repo", "42"}, want: "add the feature"},
		{name: "file and prompt", taskFile: file, args: []string{"/repo", "42", "add the feature"}, wantErrSubstring: `not both`},
		{name: "neither", args: []string{"/repo", "42"}, wantErrSubstring: `<prompt>`},
		{name: "unreadable file", taskFile: filepath.Join(dir, "missing.md"), args: []string{"/repo", "42"}, wantErrSubstring: "read task file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runPrompt(tt.taskFile, tt.args)
			if tt.wantErrSubstring != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrSubstring) {
					t.Errorf("runPrompt error = %v, want it to contain %q", err, tt.wantErrSubstring)
				}
				return
			}
			if err != nil {
				t.Fatalf("runPrompt: %v", err)
			}
			if got != tt.want {
				t.Errorf("runPrompt = %q, want %q", got, tt.want)
			}
		})
	}
}

// initGitRepo turns dir into a minimal git repository, the way an operator's
// project root looks when `daedalus run` is invoked from inside it.
func initGitRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	out, err := exec.Command("git", "-C", dir, "init", "--quiet").CombinedOutput()
	if err != nil {
		t.Fatalf("git init %s: %v: %s", dir, err, out)
	}
	return dir
}

// TestResolveRepoPath pins the contract that lets a repo path cross the
// process boundary to the worker daemon: it must come back absolute and be
// verified as a git repository up front. A relative path that reached the
// workflow input verbatim resolved against the daemon's working directory —
// wherever the worker happened to be started from — and failed every
// pipeline inside CreateWorktreeActivity with "fatal: not a git repository".
func TestResolveRepoPath(t *testing.T) {
	repo := initGitRepo(t)

	t.Run("relative path resolves to the invoking directory", func(t *testing.T) {
		t.Chdir(repo)
		got, err := resolveRepoPath(".")
		if err != nil {
			t.Fatalf("resolveRepoPath(\".\"): %v", err)
		}
		if got != repo {
			t.Errorf("resolveRepoPath(\".\") = %q, want %q", got, repo)
		}
	})

	t.Run("relative subdirectory of a repo resolves", func(t *testing.T) {
		sub := filepath.Join(repo, "pkg")
		if err := os.Mkdir(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		t.Chdir(repo)
		got, err := resolveRepoPath("pkg")
		if err != nil {
			t.Fatalf("resolveRepoPath(\"pkg\"): %v", err)
		}
		if got != sub {
			t.Errorf("resolveRepoPath(\"pkg\") = %q, want %q", got, sub)
		}
	})

	t.Run("absolute path passes through", func(t *testing.T) {
		got, err := resolveRepoPath(repo)
		if err != nil {
			t.Fatalf("resolveRepoPath(%q): %v", repo, err)
		}
		if got != repo {
			t.Errorf("resolveRepoPath(%q) = %q", repo, got)
		}
	})

	t.Run("non-git directory is rejected up front", func(t *testing.T) {
		plain := t.TempDir()
		_, err := resolveRepoPath(plain)
		if err == nil {
			t.Fatal("resolveRepoPath on a plain directory should error")
		}
		if !strings.Contains(err.Error(), "not a git repository") {
			t.Errorf("error = %v, want it to mention the missing repository", err)
		}
	})

	t.Run("missing path is rejected", func(t *testing.T) {
		if _, err := resolveRepoPath(filepath.Join(t.TempDir(), "nope")); err == nil {
			t.Fatal("resolveRepoPath on a missing path should error")
		}
	})

	// Git discovery must judge the directory on disk, not the invoking
	// shell's environment: an exported GIT_DIR would validate an unrelated
	// repository and ship a non-repo path to the worker, while
	// GIT_CEILING_DIRECTORIES could reject a valid one.
	t.Run("exported GIT_DIR cannot bypass validation", func(t *testing.T) {
		t.Setenv("GIT_DIR", filepath.Join(repo, ".git"))
		plain := t.TempDir()
		if _, err := resolveRepoPath(plain); err == nil {
			t.Fatal("GIT_DIR let a plain directory validate as a repository")
		}
	})

	t.Run("exported GIT_CEILING_DIRECTORIES cannot reject a valid repo", func(t *testing.T) {
		t.Setenv("GIT_CEILING_DIRECTORIES", repo)
		sub := filepath.Join(repo, "ceiling-probe")
		if err := os.Mkdir(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		got, err := resolveRepoPath(sub)
		if err != nil {
			t.Fatalf("resolveRepoPath(%q): %v", sub, err)
		}
		if got != sub {
			t.Errorf("resolveRepoPath(%q) = %q", sub, got)
		}
	})
}

func TestParseFlagsFile(t *testing.T) {
	for _, flag := range []string{"-f", "--file", "--file="} {
		var args []string
		if flag == "--file=" {
			args = []string{"run", flag + "task.md", "/repo", "42"}
		} else {
			args = []string{"run", flag, "task.md", "/repo", "42"}
		}
		f, rest, err := parseFlags(args)
		if err != nil {
			t.Fatalf("parseFlags(%q): %v", flag, err)
		}
		if f.taskFile != "task.md" {
			t.Errorf("parseFlags(%q) taskFile = %q, want task.md", flag, f.taskFile)
		}
		wantRest := []string{"run", "/repo", "42"}
		if len(rest) != len(wantRest) {
			t.Fatalf("parseFlags(%q) rest = %v, want %v", flag, rest, wantRest)
		}
		for i := range wantRest {
			if rest[i] != wantRest[i] {
				t.Errorf("parseFlags(%q) rest = %v, want %v", flag, rest, wantRest)
				break
			}
		}
	}
}

// TestWriteExampleConfig pins the init behavior: writes the example, refuses
// to overwrite.
func TestWriteExampleConfig(t *testing.T) {
	dir := t.TempDir()

	if err := writeExampleConfig(dir); err != nil {
		t.Fatalf("writeExampleConfig: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "config-example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != config.ExampleYAML {
		t.Error("written file should carry config.ExampleYAML verbatim")
	}

	if err := writeExampleConfig(dir); err == nil {
		t.Error("second init should refuse to overwrite")
	}
}

// stubWorkflowRun is a client.WorkflowRun whose outcome is scripted — all
// awaitPipeline needs: the workflow's ID and its result or failure.
type stubWorkflowRun struct {
	id      string
	getErr  error
	getResp string
}

func (s stubWorkflowRun) GetID() string                  { return s.id }
func (s stubWorkflowRun) GetRunID() string               { return "run-1" }
func (s stubWorkflowRun) GetFirstExecutionRunID() string { return "run-1" }
func (s stubWorkflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	if s.getErr != nil {
		return s.getErr
	}
	*(valuePtr.(*string)) = s.getResp
	return nil
}
func (s stubWorkflowRun) GetWithOptions(ctx context.Context, valuePtr interface{}, options client.WorkflowRunGetOptions) error {
	return s.Get(ctx, valuePtr)
}

// captureStdout runs f with os.Stdout redirected to a pipe and returns
// whatever f printed. The restore, close, and drain all run deferred: f may
// call t.Fatal (runtime.Goexit runs defers, not the statements after f) or
// panic. Nothing drains the pipe until the close, so captured output must
// stay smaller than the OS pipe buffer or f blocks.
func captureStdout(t *testing.T, f func()) (out string) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = w
	defer func() {
		os.Stdout = stdout
		w.Close()
		if b, rerr := io.ReadAll(r); rerr == nil {
			out = string(b)
		}
		r.Close()
	}()
	f()
	return
}

// TestAwaitPipelineParked pins the parked-run reporting: a workflow failure
// carrying the ErrAwaitingMaintainer message (the sentinel survives the
// workflow→client boundary as text only) prints the resume hint and returns
// errRunParked, while any other failure stays a plain execution error and a
// success still reports the preserved branch.
func TestAwaitPipelineParked(t *testing.T) {
	parked := errors.New(workflows.ErrAwaitingMaintainer.Error() +
		`: code review halted the run — the task cannot be completed as stated: needs a secret`)

	out := captureStdout(t, func() {
		err := awaitPipeline(stubWorkflowRun{id: "daedalus-issue-42", getErr: parked})
		if !errors.Is(err, errRunParked) {
			t.Errorf("awaitPipeline(parked) = %v, want errRunParked", err)
		}
	})
	for _, want := range []string{
		"Workflow parked",
		"run parked awaiting maintainer restart",
		`daedalus continue daedalus-issue-42`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("parked-run output %q should contain %q", out, want)
		}
	}

	out = captureStdout(t, func() {
		err := awaitPipeline(stubWorkflowRun{id: "daedalus-issue-42", getErr: errors.New("activity timed out")})
		if err == nil || !strings.Contains(err.Error(), "workflow execution") {
			t.Errorf("awaitPipeline(failed) = %v, want a wrapped execution error", err)
		}
		if errors.Is(err, errRunParked) {
			t.Error("a plain failure must not be labeled as parked")
		}
	})
	if strings.Contains(out, "continue") {
		t.Errorf("failed-run output %q should not carry the resume hint", out)
	}

	out = captureStdout(t, func() {
		if err := awaitPipeline(stubWorkflowRun{id: "daedalus-issue-42", getResp: "daedalus/issue-42-1"}); err != nil {
			t.Errorf("awaitPipeline(success) = %v, want nil", err)
		}
	})
	if !strings.Contains(out, "daedalus/issue-42-1") {
		t.Errorf("success output %q should name the preserved branch", out)
	}
}
