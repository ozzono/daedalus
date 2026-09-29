package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
)

// boolPtr is a literal for the config's pointer toggles.
func boolPtr(b bool) *bool { return &b }

// TestVerifyBranch pins the ref check that gates resume paths on the
// preserved branch: an existing branch resolves silently, and a missing one
// fails with an error naming both the branch and the repository.
func TestVerifyBranch(t *testing.T) {
	repo := initGitRepo(t)
	if out, err := exec.Command("git", "-C", repo, "-c", "user.email=test@example.com", "-c", "user.name=test",
		"commit", "--allow-empty", "--quiet", "-m", "seed").CombinedOutput(); err != nil {
		t.Fatalf("seed commit in %s: %v: %s", repo, err, out)
	}
	out, err := exec.Command("git", "-C", repo, "rev-parse", "--abbrev-ref", "HEAD").Output()
	if err != nil {
		t.Fatalf("read current branch of %s: %v", repo, err)
	}
	branch := strings.TrimSpace(string(out))

	if err := verifyBranch(repo, branch); err != nil {
		t.Errorf("verifyBranch(%q) on an existing branch: %v", branch, err)
	}

	err = verifyBranch(repo, "feat/missing-branch")
	if err == nil {
		t.Fatal("verifyBranch on a missing branch should error")
	}
	if !strings.Contains(err.Error(), "feat/missing-branch") || !strings.Contains(err.Error(), repo) {
		t.Errorf("error = %v, want it to name the branch and the repository", err)
	}
}

// TestSharedTestQueueInput pins the config → PipelineInput resolution of
// the shared_test_queue toggle: it always yields a concrete boolean — a nil
// field would mean "a pre-field replay" to the run, so an absent key must
// resolve to an explicit true (the shared routing), never to nil.
func TestSharedTestQueueInput(t *testing.T) {
	falseVal := false
	for _, c := range []struct {
		name  string
		field *bool
		want  bool
	}{
		{"absent", nil, true},
		{"explicit true", boolPtr(true), true},
		{"explicit false", &falseVal, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := sharedTestQueueInput(config.Config{SharedTestQueue: c.field})
			if got == nil {
				t.Fatal("sharedTestQueueInput returned nil, want a concrete value the run can route by")
			}
			if *got != c.want {
				t.Errorf("sharedTestQueueInput = %v, want %v", *got, c.want)
			}
		})
	}
}

// dirtyGitRepo seeds a repo with one commit and then leaves the tree dirty
// in every way `git status --porcelain` reports — an unstaged modification,
// a staged addition, and an untracked file.
func dirtyGitRepo(t *testing.T) string {
	t.Helper()
	repo := initGitRepo(t)
	git := func(args ...string) {
		t.Helper()
		full := append([]string{"-C", repo, "-c", "user.email=test@example.com", "-c", "user.name=test"}, args...)
		if out, err := exec.Command("git", full...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(repo, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("tracked.txt", "v1\n")
	git("add", "tracked.txt")
	git("commit", "--quiet", "-m", "seed")
	write("tracked.txt", "v2\n") // unstaged modification
	write("staged.txt", "s\n")   // staged addition
	git("add", "staged.txt")
	write("untracked.txt", "u\n") // untracked file
	return repo
}

// TestConfirmCleanRepo pins run's repo preflight: a clean tree passes
// silently, a dirty one has its full state printed and needs an affirmative
// answer — only y/yes proceeds, so any other answer, or a closed stdin,
// cancels with nothing started — and a repo git cannot read is an error
// naming the path.
func TestConfirmCleanRepo(t *testing.T) {
	t.Run("a clean repo passes silently", func(t *testing.T) {
		repo := initGitRepo(t)
		out := captureStdout(t, func() {
			if err := confirmCleanRepo(repo); err != nil {
				t.Errorf("confirmCleanRepo(clean): %v", err)
			}
		})
		if out != "" {
			t.Errorf("clean repo printed %q, want no output at all", out)
		}
	})

	t.Run("a dirty repo prints its state and y/yes proceeds", func(t *testing.T) {
		for _, answer := range []string{"y\n", "YES\n", "  yes  \n"} {
			repo := dirtyGitRepo(t)
			swapStdin(t, answer)
			out := captureStdout(t, func() {
				if err := confirmCleanRepo(repo); err != nil {
					t.Errorf("confirmCleanRepo(answer %q): %v", answer, err)
				}
			})
			if !strings.Contains(out, `Type "y" to proceed`) {
				t.Errorf("dirty-repo output %q should carry the confirm prompt", out)
			}
			for _, want := range []string{"uncommitted changes", "tracked.txt", "staged.txt", "untracked.txt"} {
				if !strings.Contains(out, want) {
					t.Errorf("dirty-repo output %q should carry %q", out, want)
				}
			}
		}
	})

	t.Run("any other answer cancels with nothing started", func(t *testing.T) {
		for _, answer := range []string{"n\n", "\n"} {
			repo := dirtyGitRepo(t)
			swapStdin(t, answer)
			var err error
			captureStdout(t, func() { err = confirmCleanRepo(repo) })
			if err == nil || !strings.Contains(err.Error(), "nothing was started") {
				t.Errorf("confirmCleanRepo(answer %q) = %v, want the nothing-started cancel", answer, err)
			}
		}
	})

	t.Run("a closed stdin cancels", func(t *testing.T) {
		repo := dirtyGitRepo(t)
		swapStdin(t, "")
		var err error
		captureStdout(t, func() { err = confirmCleanRepo(repo) })
		if err == nil || !strings.Contains(err.Error(), "no confirmation given") {
			t.Errorf("confirmCleanRepo(EOF) = %v, want the no-confirmation cancel", err)
		}
	})

	t.Run("a repo git cannot read is an error naming the path", func(t *testing.T) {
		notRepo := t.TempDir()
		err := confirmCleanRepo(notRepo)
		if err == nil || !strings.Contains(err.Error(), notRepo) {
			t.Errorf("confirmCleanRepo(%s) = %v, want an error naming the path", notRepo, err)
		}
	})
}

// TestEnsureWorkerActive pins run's worker preflight: a live worker passes
// without starting anything, a failed start comes back wrapped as a
// preflight error, and a freshly spawned daemon that dies at boot is a
// clean startup error naming the log — with the spawn itself untyped (both
// pollers), since a run always needs the main-queue poller.
func TestEnsureWorkerActive(t *testing.T) {
	pfqConfig := func(t *testing.T) (config.Config, string) {
		t.Helper()
		path := writeQueueConfig(t, "pfq")
		cfg, err := config.Load(path)
		if err != nil {
			t.Fatal(err)
		}
		return cfg, path
	}

	t.Run("a live worker passes without starting one", func(t *testing.T) {
		useDaemonDir(t)
		noDaemonSpawn(t)
		writeLivePidFile(t, "pfq")
		cfg, _ := pfqConfig(t)
		out := captureStdout(t, func() {
			if err := ensureWorkerActive(cfg, "unused-config.yaml"); err != nil {
				t.Errorf("ensureWorkerActive(live worker): %v", err)
			}
		})
		if out != "" {
			t.Errorf("live worker printed %q, want nothing", out)
		}
	})

	t.Run("a failed start is wrapped as a preflight error", func(t *testing.T) {
		useDaemonDir(t)
		// The daemon's log path occupied by a directory: workerStart's log
		// open fails before anything is spawned.
		if err := os.Mkdir(filepath.Join(daemonDir, "worker-pfq.log"), 0o755); err != nil {
			t.Fatal(err)
		}
		cfg, _ := pfqConfig(t)
		var err error
		out := captureStdout(t, func() { err = ensureWorkerActive(cfg, "unused-config.yaml") })
		if !strings.Contains(out, "starting it") {
			t.Errorf("output %q should announce the attempted start", out)
		}
		if err == nil || !strings.HasPrefix(err.Error(), "worker preflight:") {
			t.Errorf("ensureWorkerActive(start failure) = %v, want a worker-preflight-wrapped error", err)
		}
	})

	t.Run("a daemon that dies at boot fails cleanly and comes up untyped", func(t *testing.T) {
		useDaemonDir(t)
		// The spawned child reports its argv into the daemon log and exits —
		// so the boot the preflight waits for never comes up, and the argv
		// proves the daemon a run starts carries no -t.
		t.Setenv(reexecDumpArgsEnv, "1")
		cfg, cfgPath := pfqConfig(t)
		var err error
		out := captureStdout(t, func() { err = ensureWorkerActive(cfg, cfgPath) })
		if !strings.Contains(out, "starting it") {
			t.Errorf("output %q should announce the start", out)
		}
		if err == nil {
			t.Fatal("ensureWorkerActive(child dies at boot) = nil, want an error")
		}
		_, logFile, _ := daemonPaths("pfq")
		if !strings.Contains(err.Error(), "exited during startup") || !strings.Contains(err.Error(), logFile) {
			t.Errorf("err = %v, want the exited-during-startup diagnosis naming the log %s", err, logFile)
		}
		if got := daemonArgv(t, logFile); !reflect.DeepEqual(got, []string{"worker", "foreground", "-c", cfgPath}) {
			t.Errorf("daemon argv = %v, want untyped worker foreground -c %s", got, cfgPath)
		}
	})
}

// TestStartPipelinePreflight pins the preflights' place in startPipeline's
// sequence: a dirty repo cancels before the worker check runs — no daemon
// file is ever touched — and a clean repo reaches the worker preflight with
// the config path the run loaded, which must travel to the daemon re-exec
// so the started worker serves that config.
func TestStartPipelinePreflight(t *testing.T) {
	cfgPath := writeQueueConfig(t, "pfq")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("a dirty repo cancels before the worker check", func(t *testing.T) {
		useDaemonDir(t)
		repo := dirtyGitRepo(t)
		swapStdin(t, "") // if the confirm were ever reached, EOF declines
		var err error
		captureStdout(t, func() {
			err = startPipeline(cfg, cfgPath, defaultWorkflowName, repo, "42", "prompt", false, "", "")
		})
		if err == nil || !strings.Contains(err.Error(), "nothing was started") {
			t.Errorf("startPipeline(dirty repo) = %v, want the confirm-cancel error", err)
		}
		if _, perr := os.Stat(filepath.Join(daemonDir, "worker-pfq.pid")); !os.IsNotExist(perr) {
			t.Errorf("worker pid file exists after a dirty-repo cancel (%v) — the worker check must not run before the repo confirm", perr)
		}
	})

	t.Run("a clean repo proceeds to the worker preflight with the loaded config", func(t *testing.T) {
		useDaemonDir(t)
		t.Setenv(reexecDumpArgsEnv, "1")
		repo := initGitRepo(t)
		var err error
		captureStdout(t, func() {
			err = startPipeline(cfg, cfgPath, defaultWorkflowName, repo, "42", "prompt", false, "", "")
		})
		if err == nil || !strings.Contains(err.Error(), "worker preflight:") {
			t.Errorf("startPipeline(clean repo, no worker) = %v, want the worker preflight error", err)
		}
		_, logFile, _ := daemonPaths("pfq")
		if got := daemonArgv(t, logFile); !reflect.DeepEqual(got, []string{"worker", "foreground", "-c", cfgPath}) {
			t.Errorf("daemon argv = %v, want worker foreground -c %s", got, cfgPath)
		}
	})
}

// TestResolveFolderGrants pins the submit-time folder-grant validation: the
// -folder grants plus the -f task file's containing folder resolve to
// cleaned absolute paths, keep argument order (flags first, the task
// folder last), and collapse duplicates (the same folder via -f and
// -folder included). Every unusable grant fails here rather than at the
// first round: a missing path, a plain file, the filesystem root, a colon
// (the path composes into ai-jail's --rw-map <source>:<dest> spec), two
// grants sharing a basename (the second would shadow the first inside
// .daedalus-folders), and a glob-metachar task directory — taken
// literally, it cannot be granted.
func TestResolveFolderGrants(t *testing.T) {
	host := t.TempDir()
	shared := filepath.Join(host, "shared")
	notes := filepath.Join(host, "notes")
	other := filepath.Join(host, "other")
	for _, dir := range []string{shared, notes, other, filepath.Join(other, "notes")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relShared, err := filepath.Rel(cwd, shared)
	if err != nil {
		t.Fatal(err)
	}

	// Flag grants plus the task file's folder, in order; a relative flag
	// grant resolves against the process working directory.
	got, err := resolveFolderGrants([]string{shared, relShared}, filepath.Join(notes, "task.md"))
	if err != nil {
		t.Fatalf("resolveFolderGrants: %v", err)
	}
	if want := []string{shared, notes}; !reflect.DeepEqual(got, want) {
		t.Errorf("resolveFolderGrants = %v, want %v", got, want)
	}

	// The task file's folder dedupes against an explicit grant of the same
	// folder.
	got, err = resolveFolderGrants([]string{notes}, filepath.Join(notes, "task.md"))
	if err != nil {
		t.Fatalf("resolveFolderGrants: %v", err)
	}
	if want := []string{notes}; !reflect.DeepEqual(got, want) {
		t.Errorf("resolveFolderGrants = %v, want the single collapsed grant %v", got, want)
	}

	// No grants and no task file — the no-grants run.
	if got, err := resolveFolderGrants(nil, ""); err != nil || got != nil {
		t.Errorf("resolveFolderGrants(nil, \"\") = %v, %v, want nil/nil", got, err)
	}

	const (
		basenameErr = "share the basename"
		rootErr     = "filesystem root"
		colonErr    = "--rw-map"
		notDirErr   = "is not a directory"
		metaErr     = "glob metachars"
	)
	for _, c := range []struct {
		name       string
		flagDirs   []string
		taskFile   string
		wantSubstr string
	}{
		{"two grants sharing a basename", []string{notes, filepath.Join(other, "notes")}, "", basenameErr},
		{"filesystem root", []string{"/"}, "", rootErr},
		{"missing folder", []string{filepath.Join(host, "gone")}, "", "no such file or directory"},
		{"plain file", nil, "", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			dirs := c.flagDirs
			if c.name == "plain file" {
				file := filepath.Join(host, "brief.md")
				if err := os.WriteFile(file, []byte("x\n"), 0o644); err != nil {
					t.Fatal(err)
				}
				dirs = []string{file}
				c.wantSubstr = notDirErr
			}
			_, err := resolveFolderGrants(dirs, c.taskFile)
			if err == nil || !strings.Contains(err.Error(), c.wantSubstr) {
				t.Errorf("resolveFolderGrants(%v, %q) err = %v, want it to name %q", dirs, c.taskFile, err, c.wantSubstr)
			}
		})
	}

	// A colon in a grant composes into the mount spec and is refused.
	coloned := filepath.Join(host, "co:lon")
	if err := os.MkdirAll(coloned, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveFolderGrants([]string{coloned}, ""); err == nil || !strings.Contains(err.Error(), colonErr) {
		t.Errorf("resolveFolderGrants(colon dir) err = %v, want it to name %q", err, colonErr)
	}

	// A glob-metachar task directory is taken literally and refused.
	if _, err := resolveFolderGrants(nil, filepath.Join(notes, "[x]/task.md")); err == nil || !strings.Contains(err.Error(), metaErr) {
		t.Errorf("resolveFolderGrants(metachar task dir) err = %v, want it to name %q", err, metaErr)
	}
}
