package main

import (
	"os/exec"
	"strings"
	"testing"
)

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
