package activities

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestIsTestPath pins the multi-language test-file policy: the same
// pattern set the test-only flow allows and the refactor flow freezes is
// what the repro-first gate recognizes as the diff's repro tests.
func TestIsTestPath(t *testing.T) {
	for _, c := range []struct {
		path string
		want bool
	}{
		{"foo_test.go", true},
		{"internal/pkg/bar_test.go", true},
		{"test_login.py", true},
		{"login_test.py", true},
		{"app.test.js", true},
		{"app.test.tsx", true},
		{"testdata/fixtures/input.json", true},
		{"main.go", false},
		{"util.js", false},
		{"cmd/main.py", false},
	} {
		if got := isTestPath(c.path); got != c.want {
			t.Errorf("isTestPath(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

// TestReproFirstGateActivity pins the bug-fix flow's repro-first gate
// against a real repository: the diff's new or changed test files (never
// deletions, never non-test files) are copied into a throwaway checkout of
// the pre-fix code and the suite command runs there — a failing run means
// the repro is real, a green one means the tests do not capture the bug,
// and a diff with no test files is refused outright. The throwaway checkout
// must be torn down in every case.
func TestReproFirstGateActivity(t *testing.T) {
	assertTeardown := func(t *testing.T, repo string) {
		t.Helper()
		if n := registeredWorktrees(t, repo); n != 1 {
			t.Errorf("git still lists %d worktrees after the gate, want 1 (the throwaway checkout must be removed)", n)
		}
	}

	t.Run("no test files in the diff", func(t *testing.T) {
		repo := gitRepoWithCommit(t, map[string]string{
			"main.go":     "package main\n",
			"old_test.go": "package main\n",
		})
		// A non-test edit plus a test deletion: the deletion has no content
		// to copy, so the diff still carries no repro test.
		if err := os.WriteFile(filepath.Join(repo, "main.go"), []byte("package main // changed\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(repo, "old_test.go")); err != nil {
			t.Fatal(err)
		}

		res, err := ReproFirstGateActivity(context.Background(), ReproGateInput{
			RepoPath:     repo,
			WorktreePath: repo,
			Command:      "true",
		})
		if err != nil {
			t.Fatalf("ReproFirstGateActivity: %v", err)
		}
		if res.Reproduced {
			t.Errorf("Reproduced = true, want a refusal for a diff with no test files")
		}
		if !strings.Contains(res.Logs, "REPRO-FIRST GATE FAILED") || !strings.Contains(res.Logs, "no test files") {
			t.Errorf("logs = %q, want the no-test-files refusal", res.Logs)
		}
	})

	// dirty adds an untracked repro test on top of the base commit — the
	// minimal "the agent wrote the repro" state.
	dirty := func(t *testing.T) string {
		repo := gitRepoWithCommit(t, map[string]string{"bug.txt": "buggy\n"})
		if err := os.WriteFile(filepath.Join(repo, "repro_test.go"), []byte("package main\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return repo
	}

	t.Run("reproduced", func(t *testing.T) {
		repo := dirty(t)
		// The command fails on the pre-fix code (the base tree has no
		// FIXED marker) — exactly what the gate exists to prove.
		res, err := ReproFirstGateActivity(context.Background(), ReproGateInput{
			RepoPath:     repo,
			WorktreePath: repo,
			Command:      "echo repro-ran; grep -q FIXED bug.txt",
		})
		if err != nil {
			t.Fatalf("ReproFirstGateActivity: %v", err)
		}
		if !res.Reproduced {
			t.Errorf("Reproduced = false, want true (the command failed on the pre-fix code)")
		}
		if !strings.Contains(res.Logs, "repro-ran") {
			t.Errorf("logs = %q, want the failing run's output", res.Logs)
		}
		assertTeardown(t, repo)
	})

	t.Run("modified test file is staged", func(t *testing.T) {
		repo := gitRepoWithCommit(t, map[string]string{
			"bug.txt":       "buggy\n",
			"repro_test.go": "exit 0\n", // the committed repro passes on base
		})
		// The worktree turns the tracked repro into its failing form and
		// drops an untracked non-test helper next to it.
		if err := os.WriteFile(filepath.Join(repo, "repro_test.go"), []byte("exit 1\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, "helper.go"), []byte("package main\n"), 0o644); err != nil {
			t.Fatal(err)
		}

		// The command runs the copied test file itself. The guard is
		// deliberate: if the gate copied helper.go along, `test -e`
		// succeeds and the command exits green — a failure the mirrored
		// `test ! -e ... &&` shape could not expose, since a wrongly
		// copied helper would also exit non-zero and masquerade as a
		// failing repro.
		res, err := ReproFirstGateActivity(context.Background(), ReproGateInput{
			RepoPath:     repo,
			WorktreePath: repo,
			Command:      "test -e helper.go || sh repro_test.go",
		})
		if err != nil {
			t.Fatalf("ReproFirstGateActivity: %v", err)
		}
		// Reproduced=true is only reachable if the gate staged the
		// modified content: the committed version would pass on base, and
		// a copied helper.go would end the command green.
		if !res.Reproduced {
			t.Errorf("Reproduced = false, want true (the modified test must be staged over the base version, and only test files)")
		}
		assertTeardown(t, repo)
	})

	t.Run("tests pass on base", func(t *testing.T) {
		repo := dirty(t)
		res, err := ReproFirstGateActivity(context.Background(), ReproGateInput{
			RepoPath:     repo,
			WorktreePath: repo,
			Command:      "grep -q buggy bug.txt",
		})
		if err != nil {
			t.Fatalf("ReproFirstGateActivity: %v", err)
		}
		if res.Reproduced {
			t.Errorf("Reproduced = true, want a red round (the tests pass on the pre-fix code)")
		}
		if !strings.Contains(res.Logs, "pass on the pre-fix code") || !strings.Contains(res.Logs, "repro_test.go") {
			t.Errorf("logs = %q, want the gate refusal naming the test files", res.Logs)
		}
		assertTeardown(t, repo)
	})
}
