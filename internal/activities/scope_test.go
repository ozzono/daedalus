package activities

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestMatchPathPolicy pins the glob semantics the write-scope policy and
// the test-path policy both lean on: a trailing slash matches the whole
// subtree, a pattern containing a slash matches the full path, and a bare
// pattern matches the file name at any depth.
func TestMatchPathPolicy(t *testing.T) {
	for _, c := range []struct {
		path     string
		patterns []string
		want     bool
	}{
		{"docs/design.md", []string{"docs/"}, true},
		{"docs/deep/design.md", []string{"docs/"}, true},
		{"docs/design.md", []string{"docs/*.md"}, true},
		{"docs/deep/design.md", []string{"docs/*.md"}, false},
		{"src/util/Makefile", []string{"Makefile"}, true},
		{"main.go", []string{"*.go"}, true},
		{"main.go", []string{"docs/"}, false},
		{"main.go", nil, false},
	} {
		if got := matchPathPolicy(c.path, c.patterns); got != c.want {
			t.Errorf("matchPathPolicy(%q, %v) = %v, want %v", c.path, c.patterns, got, c.want)
		}
	}
}

// TestFlowScopedNames pins the flow segment in every per-issue derived
// name: a flow-scoped run namespaces its worktree directory and aborted
// branch so concurrent flows on one issue can neither collide with nor
// clean up each other's state; the empty flow keeps the legacy unscoped
// names.
func TestFlowScopedNames(t *testing.T) {
	aborted, err := AbortedBranchNameFor("42", "investigate")
	if err != nil || aborted != "aborted/investigate-issue-42" {
		t.Errorf("AbortedBranchNameFor(42, investigate) = %q, %v, want aborted/investigate-issue-42", aborted, err)
	}
	legacy, err := AbortedBranchNameFor("42", "")
	if err != nil || legacy != "aborted/issue-42" {
		t.Errorf("AbortedBranchNameFor(42, \"\") = %q, %v, want the legacy aborted/issue-42", legacy, err)
	}

	home := fakeHome(t)
	p, err := worktreePathFor("daedalus", "42", "test-only")
	if err != nil {
		t.Fatalf("worktreePathFor: %v", err)
	}
	if want := filepath.Join(home, ".daedalus", "worktrees", "daedalus", "test-only-issue-42"); p != want {
		t.Errorf("worktreePathFor flow-scoped = %q, want %q", p, want)
	}
	if _, err := worktreePathFor("daedalus", "42", "bad/flow"); err == nil {
		t.Error("flow segment bad/flow should be rejected like any path component")
	}
}

// gitRepoWithCommit turns a temp dir into a git repository with one commit
// carrying the given files — the base tree the gates diff against.
func gitRepoWithCommit(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := gitRepo(t)
	for name, content := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	git("add", "-A")
	git("-c", "user.name=daedalus", "-c", "user.email=daedalus@local", "commit", "-q", "-m", "base")
	return dir
}

// registeredWorktrees counts the worktrees git still lists for a repo —
// a teardown leak would leave more than the one main entry.
func registeredWorktrees(t *testing.T, repo string) int {
	t.Helper()
	out, err := exec.Command("git", "-C", repo, "worktree", "list", "--porcelain").CombinedOutput()
	if err != nil {
		t.Fatalf("git worktree list: %v: %s", err, out)
	}
	n := 0
	for line := range strings.SplitSeq(string(out), "\n") {
		if strings.HasPrefix(line, "worktree ") {
			n++
		}
	}
	return n
}

// TestVerifyWriteScopeActivity pins the structural write-scope gate: the
// diff (tracked changes plus untracked files) is judged against the flow's
// frozen and allowed path sets, daedalus's own sandbox litter never counts,
// and an empty policy leaves any diff untouched.
func TestVerifyWriteScopeActivity(t *testing.T) {
	setup := func(t *testing.T) string {
		repo := gitRepoWithCommit(t, map[string]string{
			"main.go":        "package main\n",
			"docs/readme.md": "docs\n",
		})
		// A tracked change outside the allowed set, an untracked new test
		// file inside the frozen set, and daedalus's own litter.
		if err := os.WriteFile(filepath.Join(repo, "main.go"), []byte("package main // changed\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, "new_test.go"), []byte("package main\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(repo, ".ai-jail"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, ".ai-jail", "junk"), []byte("litter\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return repo
	}

	t.Run("violations", func(t *testing.T) {
		repo := setup(t)
		res, err := VerifyWriteScopeActivity(context.Background(), ScopeCheckInput{
			WorktreePath: repo,
			Allowed:      []string{"docs/"},
			Frozen:       []string{"*_test.go"},
		})
		if err != nil {
			t.Fatalf("VerifyWriteScopeActivity: %v", err)
		}
		want := []string{
			"main.go: outside this flow's allowed paths",
			"new_test.go: frozen path — this flow may not change it",
		}
		if len(res.Violations) != len(want) {
			t.Fatalf("violations = %v, want %v", res.Violations, want)
		}
		for i, w := range want {
			if res.Violations[i] != w {
				t.Errorf("violation %d = %q, want %q", i, res.Violations[i], w)
			}
		}
	})

	t.Run("unrestricted", func(t *testing.T) {
		repo := setup(t)
		res, err := VerifyWriteScopeActivity(context.Background(), ScopeCheckInput{WorktreePath: repo})
		if err != nil {
			t.Fatalf("VerifyWriteScopeActivity: %v", err)
		}
		if len(res.Violations) != 0 {
			t.Errorf("violations = %v, want none without a policy", res.Violations)
		}
	})
}
