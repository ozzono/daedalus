package main

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/workflows"
)

// fakeDepClient answers only the two client calls preflightDependency
// makes — DescribeWorkflowExecution for the dependency's state and queue,
// GetWorkflow(...).Get for a completed workflow's result. The embedded nil
// interface keeps the rest of client.Client unreachable: a panic there is
// the test failing loudly, not a silent wrong verdict.
type fakeDepClient struct {
	client.Client
	resp       *workflowservice.DescribeWorkflowExecutionResponse
	describeEr error
	result     string
	resultErr  error
}

func (f *fakeDepClient) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return f.resp, f.describeEr
}

func (f *fakeDepClient) GetWorkflow(ctx context.Context, workflowID string, runID string) client.WorkflowRun {
	return preflightRun{f}
}

type preflightRun struct{ f *fakeDepClient }

func (r preflightRun) GetID() string                  { return "" }
func (r preflightRun) GetRunID() string               { return "" }
func (r preflightRun) GetFirstExecutionRunID() string { return "" }

func (r preflightRun) Get(ctx context.Context, value interface{}) error { return r.get(value) }

func (r preflightRun) GetWithOptions(ctx context.Context, value interface{}, _ client.WorkflowRunGetOptions) error {
	return r.get(value)
}

func (r preflightRun) get(value interface{}) error {
	if r.f.resultErr != nil {
		return r.f.resultErr
	}
	if s, ok := value.(*string); ok {
		*s = r.f.result
	}
	return nil
}

func preflightDescribe(status enums.WorkflowExecutionStatus, taskQueue string) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: status, TaskQueue: taskQueue},
	}
}

// TestPreflightDependency pins the submit-time half of the dependency
// gate: a self-dependency is refused before any server call, an in-flight
// dependency on this config's queue dispatches, an already-approved one
// passes straight through (idempotent re-submit), and a paused one
// dispatches (resumable, not broken). In the default refusal posture an
// already-broken shape — a foreign queue, a failed run, or a completion
// carrying the park marker — is refused here instead of pending forever;
// in release posture (the section enabled with a fallback branch) the
// failed and parked shapes dispatch, so the workflow gate applies the skip
// flags with the same resolved section instead of the submit gate
// pre-empting it.
func TestPreflightDependency(t *testing.T) {
	cfg := config.Config{Temporal: config.TemporalConfig{TaskQueue: "daedalus"}}

	t.Run("a run cannot depend on itself", func(t *testing.T) {
		// The self check fires before any client use — nil proves it.
		err := preflightDependency(nil, cfg, "wf-1", "wf-1")
		if err == nil || !strings.Contains(err.Error(), "cannot depend on itself") {
			t.Errorf("preflightDependency(self) = %v, want the self-dependency refusal", err)
		}
	})

	t.Run("an in-flight dependency on this queue dispatches", func(t *testing.T) {
		c := &fakeDepClient{resp: preflightDescribe(enums.WORKFLOW_EXECUTION_STATUS_RUNNING, "daedalus")}
		if err := preflightDependency(c, cfg, "wf-2", "wf-1"); err != nil {
			t.Errorf("preflightDependency(running) = %v, want dispatch", err)
		}
	})

	t.Run("an already-approved dependency passes straight through", func(t *testing.T) {
		c := &fakeDepClient{
			resp:   preflightDescribe(enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, "daedalus"),
			result: "daedalus/issue-41-1",
		}
		if err := preflightDependency(c, cfg, "wf-2", "wf-1"); err != nil {
			t.Errorf("preflightDependency(approved) = %v, want pass-through", err)
		}
	})

	t.Run("a foreign-queue dependency is refused", func(t *testing.T) {
		c := &fakeDepClient{resp: preflightDescribe(enums.WORKFLOW_EXECUTION_STATUS_RUNNING, "other-queue")}
		err := preflightDependency(c, cfg, "wf-2", "wf-1")
		if err == nil || !strings.Contains(err.Error(), "runs on task queue") {
			t.Errorf("preflightDependency(foreign queue) = %v, want the queue-ownership refusal", err)
		}
	})

	t.Run("an already-failed dependency is refused", func(t *testing.T) {
		c := &fakeDepClient{resp: preflightDescribe(enums.WORKFLOW_EXECUTION_STATUS_FAILED, "daedalus")}
		err := preflightDependency(c, cfg, "wf-2", "wf-1")
		if err == nil || !strings.Contains(err.Error(), "already broken") {
			t.Errorf("preflightDependency(failed) = %v, want the already-broken refusal", err)
		}
	})

	t.Run("an already-parked completion is refused", func(t *testing.T) {
		c := &fakeDepClient{
			resp:   preflightDescribe(enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, "daedalus"),
			result: workflows.ParkedResult("needs the maintainer"),
		}
		err := preflightDependency(c, cfg, "wf-2", "wf-1")
		if err == nil || !strings.Contains(err.Error(), "completed parked") {
			t.Errorf("preflightDependency(parked) = %v, want the parked-chain refusal", err)
		}
	})

	t.Run("a paused dependency dispatches", func(t *testing.T) {
		// Paused is resumable (`temporal workflow unpause`), not broken —
		// the old default case refused it, failing every chain behind a
		// dependency a maintainer had merely held.
		c := &fakeDepClient{resp: preflightDescribe(enums.WORKFLOW_EXECUTION_STATUS_PAUSED, "daedalus")}
		if err := preflightDependency(c, cfg, "wf-2", "wf-1"); err != nil {
			t.Errorf("preflightDependency(paused) = %v, want dispatch — the gate waits on a held dependency", err)
		}
	})

	t.Run("in release posture an already-failed dependency dispatches", func(t *testing.T) {
		// The submit-time half of the release: a broken chain the section
		// would release past is not refused here — dispatching lets the
		// workflow gate apply the skip flags with the same resolved section.
		release := cfg
		release.Dependency = config.DependencyConfig{FallbackBranch: "release-base"}
		c := &fakeDepClient{resp: preflightDescribe(enums.WORKFLOW_EXECUTION_STATUS_FAILED, "daedalus")}
		if err := preflightDependency(c, release, "wf-2", "wf-1"); err != nil {
			t.Errorf("preflightDependency(failed, release posture) = %v, want dispatch", err)
		}
	})

	t.Run("in release posture an already-parked completion dispatches", func(t *testing.T) {
		release := cfg
		release.Dependency = config.DependencyConfig{FallbackBranch: "release-base"}
		c := &fakeDepClient{
			resp:   preflightDescribe(enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, "daedalus"),
			result: workflows.ParkedResult("needs the maintainer"),
		}
		if err := preflightDependency(c, release, "wf-2", "wf-1"); err != nil {
			t.Errorf("preflightDependency(parked, release posture) = %v, want dispatch", err)
		}
	})

	t.Run("a non-NotFound describe failure returns plainly", func(t *testing.T) {
		c := &fakeDepClient{describeEr: serviceerror.NewUnavailable("server down")}
		err := preflightDependency(c, cfg, "wf-2", "wf-1")
		if err == nil || !strings.Contains(err.Error(), "describe dependency wf-1") {
			t.Errorf("preflightDependency(unreachable) = %v, want the wrapped describe error", err)
		}
	})
}

// TestInvocationBranch pins the default fallback_branch resolution: the
// branch the run is submitted from, by name. A detached HEAD names no
// branch and is refused with the config key as the fix — falling back to a
// raw commit reference would start work from a moving target's idea of a
// branch.
func TestInvocationBranch(t *testing.T) {
	repo := initGitRepo(t)
	if out, err := exec.Command("git", "-C", repo, "-c", "user.email=test@example.com", "-c", "user.name=test",
		"commit", "--allow-empty", "--quiet", "-m", "seed").CombinedOutput(); err != nil {
		t.Fatalf("seed commit in %s: %v: %s", repo, err, out)
	}
	if out, err := exec.Command("git", "-C", repo, "checkout", "-q", "-b", "chain-base").CombinedOutput(); err != nil {
		t.Fatalf("checkout -b chain-base: %v: %s", err, out)
	}

	branch, err := invocationBranch(repo)
	if err != nil {
		t.Fatalf("invocationBranch: %v", err)
	}
	if branch != "chain-base" {
		t.Errorf("invocationBranch = %q, want the branch the repo sits on", branch)
	}

	// Detached: the abbrev-ref spelling prints the literal "HEAD", and the
	// refusal names the config key that fixes it.
	if out, err := exec.Command("git", "-C", repo, "checkout", "-q", "--detach", "HEAD").CombinedOutput(); err != nil {
		t.Fatalf("checkout --detach: %v: %s", err, out)
	}
	_, err = invocationBranch(repo)
	if err == nil || !strings.Contains(err.Error(), "detached HEAD") || !strings.Contains(err.Error(), "dependency.fallback_branch") {
		t.Errorf("invocationBranch(detached) = %v, want a refusal naming detached HEAD and dependency.fallback_branch", err)
	}
}

// TestVerifyBranchExists pins the submit-time existence check on the
// explicitly named fallback branch: only refs/heads/<branch> counts, so a
// typo fails the run at submit (before the dependency is waited out) and a
// tag or SHA sharing the name is not a worktree base this check vouches
// for.
func TestVerifyBranchExists(t *testing.T) {
	repo := initGitRepo(t)
	if out, err := exec.Command("git", "-C", repo, "-c", "user.email=test@example.com", "-c", "user.name=test",
		"commit", "--allow-empty", "--quiet", "-m", "seed").CombinedOutput(); err != nil {
		t.Fatalf("seed commit in %s: %v: %s", repo, err, out)
	}
	if out, err := exec.Command("git", "-C", repo, "branch", "-q", "release-base").CombinedOutput(); err != nil {
		t.Fatalf("branch release-base: %v: %s", err, out)
	}
	if err := verifyBranchExists(repo, "release-base"); err != nil {
		t.Errorf("verifyBranchExists(existing branch) = %v, want nil", err)
	}

	err := verifyBranchExists(repo, "relese-base")
	if err == nil || !strings.Contains(err.Error(), "relese-base") || !strings.Contains(err.Error(), "dependency.fallback_branch") {
		t.Errorf("verifyBranchExists(typo) = %v, want a refusal naming the branch and dependency.fallback_branch", err)
	}

	// A tag is not a branch: the full refname pins the resolution.
	if out, err := exec.Command("git", "-C", repo, "tag", "impostor").CombinedOutput(); err != nil {
		t.Fatalf("tag impostor: %v: %s", err, out)
	}
	if err := verifyBranchExists(repo, "impostor"); err == nil {
		t.Error("verifyBranchExists(tag name) = nil, want a refusal — a tag is not a worktree base")
	}
}
