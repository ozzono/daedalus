package workflows

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"

	"daedalus/internal/activities"
	"daedalus/internal/template"
)

var (
	errAgentStub    = errors.New("agent exploded")
	errWorktreeStub = errors.New("git exploded")
	errTestsStub    = errors.New("go binary missing")
	errReviewStub   = errors.New("reviewer exploded")
)

// newTestEnv builds an in-process workflow environment with the pipeline's
// activities mocked out.
func newTestEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	s := &testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(FeatureDevWorkflow)
	env.RegisterActivity(activities.CreateWorktreeActivity)
	env.RegisterActivity(activities.RunJailedClaudeActivity)
	env.RegisterActivity(activities.RunJailedReviewerActivity)
	env.RegisterActivity(activities.RunNativeTestsActivity)
	env.RegisterActivity(activities.FinalizeWorktreeActivity)
	env.RegisterActivity(activities.CleanupWorktreeActivity)
	return env
}

// agentRecorder captures every RunJailedClaudeActivity input.
type agentRecorder struct {
	env     *testsuite.TestWorkflowEnvironment
	inputs  []activities.AgentRunInput
	stubErr error
}

func (r *agentRecorder) record() {
	r.env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.AgentRunInput); ok {
					r.inputs = append(r.inputs, in)
				}
			}
		}).
		Return(func(ctx context.Context, in activities.AgentRunInput) (activities.AgentRunResult, error) {
			return activities.AgentRunResult{Text: "stub agent output"}, r.stubErr
		})
}

// reviewerRecorder captures every RunJailedReviewerActivity input and replays
// the given verdicts in order (repeating the last one if more are needed).
type reviewerRecorder struct {
	env     *testsuite.TestWorkflowEnvironment
	inputs  []activities.ReviewInput
	stub    []activities.ReviewResult
	stubErr error
}

func (r *reviewerRecorder) record() {
	r.env.OnActivity(activities.RunJailedReviewerActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.ReviewInput); ok {
					r.inputs = append(r.inputs, in)
				}
			}
		}).
		Return(func(ctx context.Context, in activities.ReviewInput) (activities.ReviewResult, error) {
			if r.stubErr != nil {
				return activities.ReviewResult{}, r.stubErr
			}
			if len(r.stub) == 0 {
				return activities.ReviewResult{}, errors.New("no stubbed verdicts")
			}
			verdict := r.stub[0]
			if len(r.stub) > 1 {
				r.stub = r.stub[1:]
			}
			return verdict, nil
		})
}

func baseInput() PipelineInput {
	return PipelineInput{
		RepoPath:  "/repo",
		TaskQueue: "daedalus",
		IssueID:   "42",
		Prompt:    "implement the feature",
	}
}

func TestFeatureDevWorkflowHappyPath(t *testing.T) {
	env := newTestEnv(t)

	var createdQueue string
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.WorktreeInput); ok {
					createdQueue = in.TaskQueue
				}
			}
		}).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil).Once()

	rec := &agentRecorder{env: env}
	rec.record()

	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()

	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()

	var cleanupCount int
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { cleanupCount++ }).
		Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var branch string
	if err := env.GetWorkflowResult(&branch); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if branch != "daedalus/issue-42-1" {
		t.Errorf("workflow result = %q, want the preserved branch name", branch)
	}
	if createdQueue != "daedalus" {
		t.Errorf("create worktree queue = %q, want daedalus", createdQueue)
	}
	if len(rec.inputs) != 2 {
		t.Fatalf("agent ran %d times, want 2 (implement + tests)", len(rec.inputs))
	}
	want, err := template.Implement("implement the feature")
	if err != nil {
		t.Fatalf("build expected implement prompt: %v", err)
	}
	if rec.inputs[0].Prompt != want {
		t.Errorf("implement prompt = %q, want %q", rec.inputs[0].Prompt, want)
	}
	want, err = template.Tests()
	if err != nil {
		t.Fatalf("build expected test-phase prompt: %v", err)
	}
	if rec.inputs[1].Prompt != want {
		t.Errorf("test-phase prompt = %q, want %q", rec.inputs[1].Prompt, want)
	}
	if len(rev.inputs) != 2 {
		t.Fatalf("reviewer ran %d times, want 2 (code + tests)", len(rev.inputs))
	}
	if rev.inputs[0].Focus != "the implementation" || rev.inputs[0].TestLogs != "" {
		t.Errorf("code review input = %+v, want focus on the implementation with no test logs", rev.inputs[0])
	}
	if rev.inputs[1].Focus != "the test suite" || rev.inputs[1].TestLogs != "ok" {
		t.Errorf("test review input = %+v, want focus on the test suite with test logs", rev.inputs[1])
	}
	if cleanupCount != 1 {
		t.Errorf("cleanup ran %d times, want 1", cleanupCount)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowGuidanceSignal pins the guide path: a "guide" signal
// sent mid-run is folded into the next agent fix prompt, ahead of the review
// comments.
func TestFeatureDevWorkflowGuidanceSignal(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()

	// Code review rejects once — the operator steers mid-run right then —
	// and approves on the second look; test review approves.
	var reviewCalls int
	var revInputs []activities.ReviewInput
	env.OnActivity(activities.RunJailedReviewerActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			reviewCalls++
			for _, a := range args {
				if in, ok := a.(activities.ReviewInput); ok {
					revInputs = append(revInputs, in)
				}
			}
			if reviewCalls == 1 {
				// Mid-run guidance: buffered by the workflow's signal
				// channel until the next fix prompt drains it.
				env.SignalWorkflow("guide", "write the missing tests first")
			}
		}).
		Return(func(ctx context.Context, in activities.ReviewInput) (activities.ReviewResult, error) {
			if reviewCalls == 1 {
				return activities.ReviewResult{Approved: false, Comments: "rename foo to bar"}, nil
			}
			return activities.ReviewResult{Approved: true}, nil
		})

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-2", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(rec.inputs) != 3 {
		t.Fatalf("agent ran %d times, want 3 (implement, review-fix, tests)", len(rec.inputs))
	}
	fixPrompt := rec.inputs[1].Prompt
	if !strings.Contains(fixPrompt, "OPERATOR GUIDANCE") ||
		!strings.Contains(fixPrompt, "write the missing tests first") {
		t.Errorf("fix prompt lacks guidance: %q", fixPrompt)
	}
	if !strings.Contains(fixPrompt, "rename foo to bar") {
		t.Errorf("fix prompt lacks review comments: %q", fixPrompt)
	}
	// Guidance must not leak into rounds it was not sent for.
	if strings.Contains(rec.inputs[2].Prompt, "OPERATOR GUIDANCE") {
		t.Errorf("guidance leaked into the tests prompt: %q", rec.inputs[2].Prompt)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowContinued pins the resume plumbing: a run with
// BaseBranch hands it to CreateWorktreeActivity and opens on the continue
// prompt — continuation framing plus the prior review feedback.
func TestFeatureDevWorkflowContinued(t *testing.T) {
	env := newTestEnv(t)

	var created activities.WorktreeInput
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.WorktreeInput); ok {
					created = in
				}
			}
		}).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	env.OnActivity(activities.RunJailedReviewerActivity, mock.Anything, mock.Anything).
		Return(activities.ReviewResult{Approved: true}, nil)
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil)
	var finalized, cleanedUp activities.WorktreeInput
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.WorktreeInput); ok {
					finalized = in
				}
			}
		}).
		Return("team/ship/issue-42-2", nil)
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.WorktreeInput); ok {
					cleanedUp = in
				}
			}
		}).
		Return(nil)

	in := baseInput()
	in.BaseBranch = "aborted/issue-42"
	in.PriorFeedback = "finding 1"
	in.BranchPrefix = "team/ship"
	env.ExecuteWorkflow(FeatureDevWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if created.BaseBranch != "aborted/issue-42" {
		t.Errorf("CreateWorktreeActivity BaseBranch = %q, want aborted/issue-42", created.BaseBranch)
	}
	if created.BranchPrefix != "team/ship" {
		t.Errorf("CreateWorktreeActivity BranchPrefix = %q, want team/ship", created.BranchPrefix)
	}
	// The prefix must reach the activities whose behavior depends on it:
	// finalize names the deliverable branch under it, and cleanup's
	// finalized check globs under it.
	if finalized.BranchPrefix != "team/ship" {
		t.Errorf("FinalizeWorktreeActivity BranchPrefix = %q, want team/ship", finalized.BranchPrefix)
	}
	if cleanedUp.BranchPrefix != "team/ship" {
		t.Errorf("CleanupWorktreeActivity BranchPrefix = %q, want team/ship", cleanedUp.BranchPrefix)
	}
	if len(rec.inputs) == 0 {
		t.Fatal("agent never ran")
	}
	first := rec.inputs[0].Prompt
	if !strings.Contains(first, "continuing a previous attempt") ||
		!strings.Contains(first, "finding 1") ||
		!strings.Contains(first, "implement the feature") {
		t.Errorf("opening prompt lacks continuation context: %q", first)
	}
	env.AssertExpectations(t)
}

func TestFeatureDevWorkflowCodeReviewLoop(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()

	// Code review demands changes once, then approves; test review approves.
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: false, Comments: "rename foo to bar"},
		{Approved: true},
	}}
	rev.record()

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()

	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-2", nil).Once()

	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(rec.inputs) != 3 {
		t.Fatalf("agent ran %d times, want 3 (implement, review-fix, tests)", len(rec.inputs))
	}
	fixPrompt := rec.inputs[1].Prompt
	if !strings.Contains(fixPrompt, "Code review feedback") ||
		!strings.Contains(fixPrompt, "rename foo to bar") ||
		!strings.Contains(fixPrompt, "Address the review comments") {
		t.Errorf("review-fix prompt %q should carry the review comments", fixPrompt)
	}
	env.AssertExpectations(t)
}

func TestFeatureDevWorkflowTestLoopFeedback(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()

	// Code approves immediately; first test review fails on both axes
	// (failing tests AND requested changes), second approves.
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: true},
		{Approved: false, Comments: "cover the error path"},
		{Approved: true},
	}}
	rev.record()

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: false, Logs: "--- FAIL: TestBoom\nboom"}, nil).Once()
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()

	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-3", nil).Once()

	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(rec.inputs) != 3 {
		t.Fatalf("agent ran %d times, want 3 (implement, tests, tests-fix)", len(rec.inputs))
	}
	fixPrompt := rec.inputs[2].Prompt
	if !strings.Contains(fixPrompt, "Tests failed with output:") ||
		!strings.Contains(fixPrompt, "TestBoom") ||
		!strings.Contains(fixPrompt, "Fix the tests so they pass.") {
		t.Errorf("test-fix prompt %q should carry the failing test output", fixPrompt)
	}
	if !strings.Contains(fixPrompt, "Test review feedback") ||
		!strings.Contains(fixPrompt, "cover the error path") ||
		!strings.Contains(fixPrompt, "Address the review comments") {
		t.Errorf("test-fix prompt %q should carry the test review comments", fixPrompt)
	}
	// The reviewer's first phase-2 round must have seen the failing output.
	if rev.inputs[1].TestLogs != "--- FAIL: TestBoom\nboom" {
		t.Errorf("first test review logs = %q, want the failing output", rev.inputs[1].TestLogs)
	}
	env.AssertExpectations(t)
}

func TestFeatureDevWorkflowReviewerErrorFails(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()

	rev := &reviewerRecorder{env: env, stubErr: errReviewStub}
	rev.record()

	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), "code review") {
		t.Fatalf("want workflow error from reviewer failure, got %v", err)
	}
	env.AssertExpectations(t)
}

func TestFeatureDevWorkflowCleansUpOnAgentFailure(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env, stubErr: errAgentStub}
	rec.record()

	env.OnActivity(activities.RunJailedReviewerActivity, mock.Anything, mock.Anything).
		Return(activities.ReviewResult{}, nil).Maybe()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("want workflow error when the agent run fails")
	}
	env.AssertExpectations(t)
}

func TestFeatureDevWorkflowCreateFailure(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{}, errWorktreeStub).Once()

	// The agent must never run when the worktree could not be created.
	var agentCalls int
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { agentCalls++ }).
		Return(activities.AgentRunResult{}, nil).Maybe()
	env.OnActivity(activities.RunJailedReviewerActivity, mock.Anything, mock.Anything).
		Return(activities.ReviewResult{}, nil).Maybe()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error when worktree creation fails")
	}
	if !strings.Contains(err.Error(), "create worktree") {
		t.Errorf("error %q should mention worktree creation", err)
	}
	if agentCalls != 0 {
		t.Errorf("agent ran %d times, want 0", agentCalls)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowTestActivityError covers the system-error branch of
// the test loop: a failing RunNativeTestsActivity (e.g. missing go binary)
// must fail the workflow instead of being treated as failed tests.
func TestFeatureDevWorkflowTestActivityError(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()

	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(activities.TestResult{}, errTestsStub).Once()

	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error when the test activity errors")
	}
	if !strings.Contains(err.Error(), "run tests") {
		t.Errorf("error %q should mention the test run", err)
	}
	if len(rec.inputs) != 2 {
		t.Errorf("agent ran %d times, want 2 (no fix round for system errors)", len(rec.inputs))
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowCleanupOnCancellation cancels the workflow while its
// first activity is in flight and asserts the deferred cleanup still runs —
// it must execute on a disconnected context that survives cancellation.
func TestFeatureDevWorkflowCleanupOnCancellation(t *testing.T) {
	env := newTestEnv(t)

	unblock := make(chan struct{})
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { <-unblock }).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil).Once()

	// The workflow exits at the create step once cancelled, so the agent and
	// reviewer may never run — their mocks must not require calls.
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Return(activities.AgentRunResult{}, nil).Maybe()
	env.OnActivity(activities.RunJailedReviewerActivity, mock.Anything, mock.Anything).
		Return(activities.ReviewResult{}, nil).Maybe()

	var cleanupCount int
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { cleanupCount++ }).
		Return(nil).Once()

	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
		close(unblock)
	}, time.Second)

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("want workflow error after cancellation")
	}
	if cleanupCount != 1 {
		t.Fatalf("cleanup ran %d times after cancellation, want 1 — the deferred cleanup must use a disconnected context", cleanupCount)
	}
	env.AssertExpectations(t)
}
