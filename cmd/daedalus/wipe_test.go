package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// wipeGitHelper runs a git command against repo and fails the test on any
// error, surfacing git's output — setup for wipeBranches tests.
func wipeGitHelper(t *testing.T, repo string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// wipeTestRepo lays out a repo with the branch families wipe is expected to
// distinguish: two in-flight branches for the issue, one preserved-looking
// branch under a different prefix, and the aborted/ snapshot — all on top
// of one commit so the branches exist.
func wipeTestRepo(t *testing.T) string {
	t.Helper()
	repo := initGitRepo(t)
	wipeGitHelper(t, repo, "config", "user.email", "test@example.com")
	wipeGitHelper(t, repo, "config", "user.name", "test")
	// The unborn initial branch follows the host's init.defaultBranch, and
	// the exact-surviving-list assertion below names it — pin it here (not
	// in the shared initGitRepo) so the fixture is identical on every host.
	wipeGitHelper(t, repo, "symbolic-ref", "HEAD", "refs/heads/master")
	wipeGitHelper(t, repo, "commit", "--allow-empty", "-m", "base")
	for _, b := range []string{
		"feat/issue-42-a",
		"feat/issue-42-stale",
		"team/issue-42-a",
		"aborted/issue-42",
	} {
		wipeGitHelper(t, repo, "branch", b)
	}
	return repo
}

// listBranches returns the repo's local branch names, one per line.
func listBranches(t *testing.T, repo string) []string {
	t.Helper()
	out := wipeGitHelper(t, repo, "branch", "--format=%(refname:short)")
	return strings.Fields(out)
}

// TestWipeBranches pins the deliberate-destruction contract behind the
// wipe confirmation's branch globs: exactly the branches matching the
// pattern are deleted (the count is what the per-artifact report prints),
// every other family — preserved deliverables, the aborted/ snapshot —
// survives, and a pattern with no matches is a clean zero, not an error
// (a run that never opened a round has no in-flight branches).
func TestWipeBranches(t *testing.T) {
	t.Run("deletes exactly the matching branches", func(t *testing.T) {
		repo := wipeTestRepo(t)

		n, err := wipeBranches(repo, "feat/issue-42-*")
		if err != nil {
			t.Fatalf("wipeBranches: %v", err)
		}
		if n != 2 {
			t.Errorf("wipeBranches deleted %d branches, want 2", n)
		}
		got := listBranches(t, repo)
		want := []string{"aborted/issue-42", "master", "team/issue-42-a"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("surviving branches = %v, want %v", got, want)
		}
	})

	t.Run("no matches deletes nothing", func(t *testing.T) {
		repo := wipeTestRepo(t)

		n, err := wipeBranches(repo, "feat/issue-99-*")
		if err != nil {
			t.Fatalf("wipeBranches: %v", err)
		}
		if n != 0 {
			t.Errorf("wipeBranches deleted %d branches, want 0", n)
		}
		if got := len(listBranches(t, repo)); got != 5 {
			t.Errorf("surviving branch count = %d, want 5", got)
		}
	})

	t.Run("a failed deletion is reported, not skipped", func(t *testing.T) {
		repo := wipeTestRepo(t)
		// A branch checked out in a linked worktree refuses deletion — the
		// one failure a glob listing cannot see coming, and the case the
		// wipe report must surface instead of silently leaving the branch.
		// The other matching branches are still deleted: the joined error
		// carries what failed, the count what actually went away.
		wt := filepath.Join(t.TempDir(), "linked")
		wipeGitHelper(t, repo, "worktree", "add", "-b", "feat/issue-42-wt", wt)

		n, err := wipeBranches(repo, "feat/issue-42-*")
		if err == nil || !strings.Contains(err.Error(), "delete feat/issue-42-wt") {
			t.Fatalf("wipeBranches err = %v, want a failed-deletion report naming the branch", err)
		}
		if n != 2 {
			t.Errorf("wipeBranches deleted %d branches, want 2 (the two deletable ones)", n)
		}
		got := strings.Join(listBranches(t, repo), ",")
		if !strings.Contains(got, "feat/issue-42-wt") {
			t.Errorf("feat/issue-42-wt missing from %v, want it to survive the failed deletion", got)
		}
		if strings.Contains(got, "feat/issue-42-a") || strings.Contains(got, "feat/issue-42-stale") {
			t.Errorf("deletable branches missing from the deletion pass: %v", got)
		}
	})

	t.Run("listing failure is returned", func(t *testing.T) {
		if _, err := wipeBranches(t.TempDir(), "feat/*"); err == nil ||
			!strings.Contains(err.Error(), "git branch") {
			t.Errorf("wipeBranches(non-repo) err = %v, want a git branch failure", err)
		}
	})
}

// TestRemoveFile pins removeFile's existence reporting: an existing file
// is removed and reported, an already-absent one is a clean false (a wipe
// of a run that never started rounds is full of those), and a real
// removal failure — a non-empty directory, here — is an error, not a
// silent skip.
func TestRemoveFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gone.md")
	if err := os.WriteFile(path, []byte("log"), 0o644); err != nil {
		t.Fatal(err)
	}
	existed, err := removeFile(path)
	if err != nil || !existed {
		t.Errorf("removeFile(existing) = %v, %v; want true, nil", existed, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file still statable after removeFile: %v", err)
	}

	existed, err = removeFile(path)
	if err != nil || existed {
		t.Errorf("removeFile(absent) = %v, %v; want false, nil", existed, err)
	}

	full := filepath.Join(dir, "fulldir")
	if err := os.Mkdir(full, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(full, "keep"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if existed, err := removeFile(full); err == nil || !existed {
		t.Errorf("removeFile(non-empty dir) = %v, %v; want true, an error", existed, err)
	}
}

// TestReadConfirmLine pins the confirmation read: the typed line comes
// back verbatim (case and trimming happen in the caller), and a closed
// stdin — EOF with nothing typed — is an error, so a piped or closed
// stdin can never confirm a wipe by accident.
func TestReadConfirmLine(t *testing.T) {
	swapStdin(t, "YES\n")
	line, err := readConfirmLine()
	if err != nil {
		t.Fatalf("readConfirmLine: %v", err)
	}
	if line != "YES\n" {
		t.Errorf("readConfirmLine = %q, want %q verbatim", line, "YES\n")
	}

	swapStdin(t, "")
	if line, err := readConfirmLine(); err == nil {
		t.Errorf("readConfirmLine(EOF) = %q, %v; want an error", line, err)
	}
}

// swapStdin feeds content to os.Stdin for the duration of the test —
// readConfirmLine reads the variable at call time, so the swap reaches it.
func swapStdin(t *testing.T, content string) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stdin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	old := os.Stdin
	os.Stdin = f
	t.Cleanup(func() { os.Stdin = old; f.Close() })
}
