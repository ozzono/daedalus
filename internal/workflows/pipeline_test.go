package workflows

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/template"
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

// errTimeoutStub is what a timed-out activity surfaces to workflow code
// (temporal.NewTimeoutError exists for exactly this unit-testing use).
var errTimeoutStub = temporal.NewTimeoutError(enums.TIMEOUT_TYPE_START_TO_CLOSE, nil)

// TestFeatureDevWorkflowAgentTimeoutRecovers pins the core recovery: a
// round that dies at the StartToClose ceiling re-runs as a continuation
// of its own partial work instead of failing the run.
func TestFeatureDevWorkflowAgentTimeoutRecovers(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	var inputs []activities.AgentRunInput
	var calls int
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.AgentRunInput); ok {
					inputs = append(inputs, in)
				}
			}
		}).
		Return(func(ctx context.Context, in activities.AgentRunInput) (activities.AgentRunResult, error) {
			calls++
			if calls == 1 {
				return activities.AgentRunResult{}, errTimeoutStub
			}
			return activities.AgentRunResult{Text: "done"}, nil
		})
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	// Three rounds: implement (timed out), its continuation, then the
	// phase-2 tests round a fresh approval leads into.
	if calls != 3 {
		t.Fatalf("agent calls = %d, want 3 (timed-out round, continuation, tests round)", calls)
	}
	if len(inputs) != 3 || inputs[1].Prompt == inputs[0].Prompt {
		t.Fatalf("retry must re-prompt, inputs recorded: %d", len(inputs))
	}
	if !strings.Contains(inputs[1].Prompt, "continuing a previous attempt") {
		t.Errorf("retry prompt is not a continuation: %q", inputs[1].Prompt)
	}
	if !strings.Contains(inputs[1].Prompt, "timeout ceiling") {
		t.Errorf("retry prompt does not explain the timeout: %q", inputs[1].Prompt)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowAgentTimeoutStreakFails pins the cap: a run whose
// every round dies at the ceiling fails loudly instead of looping forever.
func TestFeatureDevWorkflowAgentTimeoutStreakFails(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Return(activities.AgentRunResult{}, errTimeoutStub)
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error after a timeout streak")
	}
	if !strings.Contains(err.Error(), "timed out 3 rounds in a row") {
		t.Errorf("error does not name the streak: %v", err)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowTestsTimeoutRecovers pins that a timed-out native
// suite becomes a failing round: the fix loop gets synthetic timeout logs
// and the run still finishes.
func TestFeatureDevWorkflowTestsTimeoutRecovers(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	var inputs []activities.AgentRunInput
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.AgentRunInput); ok {
					inputs = append(inputs, in)
				}
			}
		}).
		Return(activities.AgentRunResult{Text: "done"}, nil)
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	var suiteCalls int
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, path string) (activities.TestResult, error) {
			suiteCalls++
			if suiteCalls == 1 {
				return activities.TestResult{}, errTimeoutStub
			}
			return activities.TestResult{Passed: true, Logs: "ok"}, nil
		})
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if suiteCalls != 2 {
		t.Fatalf("suite calls = %d, want 2 (timed-out round plus rerun)", suiteCalls)
	}
	// The last round is the fix: implement, tests round, then the fix
	// digesting the synthetic timeout logs.
	last := inputs[len(inputs)-1]
	if !strings.Contains(last.Prompt, "NATIVE TEST SUITE TIMED OUT") {
		t.Errorf("fix prompt does not carry the timeout logs: %q", last.Prompt)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowReviewerTimeoutRetries pins that a reviewer round
// lost to the ceiling is simply retried.
func TestFeatureDevWorkflowReviewerTimeoutRetries(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	rec := &agentRecorder{env: env}
	rec.record()
	var reviewCalls int
	env.OnActivity(activities.RunJailedReviewerActivity, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, in activities.ReviewInput) (activities.ReviewResult, error) {
			reviewCalls++
			if reviewCalls == 1 {
				return activities.ReviewResult{}, errTimeoutStub
			}
			return activities.ReviewResult{Approved: true}, nil
		})
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if reviewCalls < 2 {
		t.Fatalf("review calls = %d, want the timed-out round retried", reviewCalls)
	}
	env.AssertExpectations(t)
}

// errQuotaStub mirrors how a quota-exhausted activity round surfaces on the
// workflow side: the ErrAPIExhausted sentinel wrapped in richer text, matched
// by substring across the worker→workflow boundary.
var errQuotaStub = fmt.Errorf("run jailed: %w: usage limit reached", activities.ErrAPIExhausted)

// TestFeatureDevWorkflowQuotaHeartbeatRecovers pins the pause semantics: an
// exhausted round is retried unchanged after an hourly heartbeat, the streak
// budget resets on any completed round (a later exhaustion gets a full five
// heartbeats again), and the run still finishes.
func TestFeatureDevWorkflowQuotaHeartbeatRecovers(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	// Implement round: one exhaustion, then through. Tests round: five
	// exhaustions in a row — only survivable because the earlier completed
	// round reset the budget — then through.
	var calls int
	var inputs []activities.AgentRunInput
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.AgentRunInput); ok {
					inputs = append(inputs, in)
				}
			}
		}).
		Return(func(ctx context.Context, in activities.AgentRunInput) (activities.AgentRunResult, error) {
			calls++
			switch calls {
			case 1, 3, 4, 5, 6, 7:
				return activities.AgentRunResult{}, errQuotaStub
			}
			return activities.AgentRunResult{Text: "done"}, nil
		})

	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if calls != 8 {
		t.Fatalf("agent calls = %d, want 8 (exhausted+retry, then 5 exhausted+retry in the tests round)", calls)
	}
	// The retried round must be the same round, not a continuation.
	if inputs[1].Prompt != inputs[0].Prompt {
		t.Errorf("heartbeat retry changed the prompt:\n%q\n!=\n%q", inputs[1].Prompt, inputs[0].Prompt)
	}
	if inputs[7].Prompt != inputs[2].Prompt {
		t.Errorf("heartbeat retry changed the tests-round prompt")
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowQuotaHeartbeatParks pins the cap: still exhausted
// past five heartbeats, the run parks itself with ErrAwaitingMaintainer
// instead of failing with a raw error — and still cleans up.
func TestFeatureDevWorkflowQuotaHeartbeatParks(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	var calls int
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { calls++ }).
		Return(activities.AgentRunResult{}, errQuotaStub)

	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error when the API stays exhausted")
	}
	for _, want := range []string{
		ErrAwaitingMaintainer.Error(),
		"still exhausted after 5 hourly heartbeats",
		`stage "implement"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	// Five heartbeats retry the round five times: six exhausted attempts.
	if calls != 6 {
		t.Errorf("agent calls = %d, want 6 (initial attempt plus five heartbeat retries)", calls)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowQuotaHeartbeatCancelled pins the sleep-error branch
// of the heartbeat: a cancellation landing mid-sleep — a brand-new window,
// now that an exhausted run sits in an hourly Sleep instead of failing
// immediately — must fail the run promptly rather than retry past the error,
// and still clean up.
func TestFeatureDevWorkflowQuotaHeartbeatCancelled(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	var calls int
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { calls++ }).
		Return(activities.AgentRunResult{}, errQuotaStub)

	var cleanupCount int
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { cleanupCount++ }).
		Return(nil).Once()

	// The first agent call exhausts the API and the run enters its hourly
	// sleep; cancel 30 minutes in, mid-heartbeat.
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 30*time.Minute)

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error when cancelled during a heartbeat sleep")
	}
	if !strings.Contains(err.Error(), "quota heartbeat sleep") {
		t.Errorf("error %q should name the interrupted heartbeat sleep", err)
	}
	if calls != 1 {
		t.Errorf("agent calls = %d, want 1 (no retry past a cancelled sleep)", calls)
	}
	if cleanupCount != 1 {
		t.Fatalf("cleanup ran %d times after cancellation, want 1", cleanupCount)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowReviewerQuotaHeartbeatRecovers pins the review
// call site: an exhausted reviewer round heartbeats and retries, and the
// verdict then flows normally.
func TestFeatureDevWorkflowReviewerQuotaHeartbeatRecovers(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()

	var reviewCalls int
	env.OnActivity(activities.RunJailedReviewerActivity, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, in activities.ReviewInput) (activities.ReviewResult, error) {
			reviewCalls++
			if reviewCalls == 1 {
				return activities.ReviewResult{}, errQuotaStub
			}
			return activities.ReviewResult{Approved: true}, nil
		})

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if reviewCalls != 3 {
		t.Fatalf("review calls = %d, want 3 (exhausted code review, retry, test review)", reviewCalls)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowTestsQuotaHeartbeatRecovers pins the suite call
// site: test-command discovery can exhaust the API too, and the suite round
// is retried after a heartbeat rather than failing the run.
func TestFeatureDevWorkflowTestsQuotaHeartbeatRecovers(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	var suiteCalls int
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, path string) (activities.TestResult, error) {
			suiteCalls++
			if suiteCalls == 1 {
				return activities.TestResult{}, errQuotaStub
			}
			return activities.TestResult{Passed: true, Logs: "ok"}, nil
		})

	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if suiteCalls != 2 {
		t.Fatalf("suite calls = %d, want 2 (exhausted discovery round plus retry)", suiteCalls)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowQuotaHeartbeatResetsOnRedSuite pins the "even a red
// one" reset: a suite round that ran to completion but failed still resets
// the heartbeat streak. The suite exhausts once (streak = 1) before its red
// round, and the very next call — the test review — then exhausts: with the
// reset it gets a full five heartbeats (six attempts before parking); a
// stale streak would park one attempt earlier. Routing through the test
// review, not a fix round, matters: a successful review resets the streak
// itself and would mask the suite round's reset.
func TestFeatureDevWorkflowQuotaHeartbeatResetsOnRedSuite(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	agentRec := &agentRecorder{env: env}
	agentRec.record()

	// Code review approves; every test-review call is exhausted.
	var reviewCalls int
	env.OnActivity(activities.RunJailedReviewerActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { reviewCalls++ }).
		Return(func(ctx context.Context, in activities.ReviewInput) (activities.ReviewResult, error) {
			if reviewCalls == 1 {
				return activities.ReviewResult{Approved: true}, nil
			}
			return activities.ReviewResult{}, errQuotaStub
		})

	var suiteCalls int
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, path string) (activities.TestResult, error) {
			suiteCalls++
			if suiteCalls == 1 {
				return activities.TestResult{}, errQuotaStub
			}
			return activities.TestResult{Passed: false, Logs: "--- FAIL: TestBoom"}, nil
		})

	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error when the API stays exhausted at test review")
	}
	for _, want := range []string{
		"still exhausted after 5 hourly heartbeats",
		`stage "review"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	// One approving code review plus six exhausted test-review attempts
	// (initial plus five heartbeat retries): the full budget only a reset
	// streak grants.
	if reviewCalls != 7 {
		t.Errorf("review calls = %d, want 7 — a red-but-completed suite round must reset the heartbeat budget", reviewCalls)
	}
	if len(agentRec.inputs) != 2 || suiteCalls != 2 {
		t.Errorf("agent ran %d times and suite %d times, want 2 and 2 (implement, tests; exhausted retry, red round)",
			len(agentRec.inputs), suiteCalls)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowCodeReviewNeedsMaintainerParks pins the reviewer
// halt: a NEEDS_MAINTAINER verdict in phase 1 parks the run carrying
// ErrAwaitingMaintainer and the halting round's comments. The halt arrives
// after one changes-requested round — the realistic shape, a reviewer that
// tries to fix and then gives up — so prior rounds are proven not to steer
// the park path or leak their comments into the error.
func TestFeatureDevWorkflowCodeReviewNeedsMaintainerParks(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: false, Comments: "rename foo to bar"},
		{NeedsMaintainer: true, Comments: "the fix needs a secret only the maintainer can provide"},
	}}
	rev.record()

	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error from a NEEDS_MAINTAINER verdict")
	}
	for _, want := range []string{
		ErrAwaitingMaintainer.Error(),
		"code review halted the run",
		"the fix needs a secret only the maintainer can provide",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "rename foo to bar") {
		t.Errorf("park error %q should carry the halting round's comments, not an earlier round's", err)
	}
	if len(rec.inputs) != 2 {
		t.Errorf("agent ran %d times, want 2 (implement plus one fix round; no fix round after a halt)", len(rec.inputs))
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowTestReviewNeedsMaintainerParks pins the phase-2
// halt: a NEEDS_MAINTAINER verdict after the suite ran parks the run the
// same way, labeled as a test-review halt.
func TestFeatureDevWorkflowTestReviewNeedsMaintainerParks(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: true},
		{NeedsMaintainer: true, Comments: "the suite requires a license key"},
	}}
	rev.record()

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error from a NEEDS_MAINTAINER verdict")
	}
	for _, want := range []string{
		ErrAwaitingMaintainer.Error(),
		"test review halted the run",
		"the suite requires a license key",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	env.AssertExpectations(t)
}

// TestTruncateParkComments pins the park-error excerpt rule: short comments
// pass through untouched, long ones are cut at the byte cap on a whole-rune
// boundary and marked as truncated.
func TestTruncateParkComments(t *testing.T) {
	const marker = "\n[... review comments truncated ...]"

	exact := strings.Repeat("a", maxParkCommentBytes)
	if got := truncateParkComments(exact); got != exact {
		t.Errorf("input at the cap should pass through, got %q-ish (len %d)", got[:32], len(got))
	}

	long := strings.Repeat("a", maxParkCommentBytes+50)
	got := truncateParkComments(long)
	if want := strings.Repeat("a", maxParkCommentBytes) + marker; got != want {
		t.Errorf("ASCII truncation: got %d bytes, want cap+marker (%d bytes)", len(got), len(want))
	}

	// The byte cap lands mid-rune (each € is three bytes): the cut must
	// slide forward to a whole rune, never emitting half of one.
	runes := strings.Repeat("€", 342) + strings.Repeat("a", 10) // 1026 + 10 bytes
	got = truncateParkComments(runes)
	if want := strings.Repeat("€", 342) + marker; got != want {
		t.Errorf("multibyte truncation: got %d bytes (valid=%v), want 342 whole runes + marker",
			len(got), utf8.ValidString(got))
	}

	// The cap lands exactly on a rune boundary: no slide, the multibyte
	// tail is simply dropped.
	boundary := strings.Repeat("a", maxParkCommentBytes) + "€€"
	got = truncateParkComments(boundary)
	if want := strings.Repeat("a", maxParkCommentBytes) + marker; got != want {
		t.Errorf("rune-boundary truncation: got %d bytes, want cap+marker (%d bytes)",
			len(got), len(want))
	}
}
