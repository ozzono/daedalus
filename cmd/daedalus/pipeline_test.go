package main

import (
	"os/exec"
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
