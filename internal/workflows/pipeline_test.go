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
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
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

// agentStep is one scripted agent-round outcome: the recorder plays the
// script in order, one entry per call, then falls back to its default.
type agentStep struct {
	result activities.AgentRunResult
	err    error
}

// agentRecorder captures every RunJailedClaudeActivity input.
type agentRecorder struct {
	env    *testsuite.TestWorkflowEnvironment
	inputs []activities.AgentRunInput
	// result is the default outcome once script (if any) is exhausted.
	result  activities.AgentRunResult
	script  []agentStep
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
			if len(r.script) > 0 {
				step := r.script[0]
				r.script = r.script[1:]
				return step.result, step.err
			}
			if r.result == (activities.AgentRunResult{}) {
				r.result = activities.AgentRunResult{Text: "stub agent output"}
			}
			return r.result, r.stubErr
		})
}

// reviewStep is one scripted reviewer-round outcome: the recorder plays the
// script in order, one entry per call, then falls back to its verdict stub.
type reviewStep struct {
	result activities.ReviewResult
	err    error
}

// reviewerRecorder captures every RunJailedReviewerActivity input and replays
// the given verdicts in order (repeating the last one if more are needed).
type reviewerRecorder struct {
	env    *testsuite.TestWorkflowEnvironment
	inputs []activities.ReviewInput
	// script, when non-empty, is played one entry per call before the stub
	// verdicts take over.
	script  []reviewStep
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
			if len(r.script) > 0 {
				step := r.script[0]
				r.script = r.script[1:]
				return step.result, step.err
			}
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

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
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

// TestFeatureDevWorkflowAgentTravelsToEveryRound pins the -cli plumbing: a
// run started with Agent set carries it into every jailed round — the agent
// rounds, both review phases, and the native-suite activity (whose discovery
// fallback runs a jailed agent too) — so the override applies on whichever
// worker serves the queue, and the worker's own DAEDALUS_AGENT is never
// consulted for the run.
func TestFeatureDevWorkflowAgentTravelsToEveryRound(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	var suiteAgents []string
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			// argv shape: (ctx, worktreePath, agent) — the trailing string.
			if n := len(args); n > 0 {
				if agent, ok := args.Get(n - 1).(string); ok {
					suiteAgents = append(suiteAgents, agent)
				}
			}
		}).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	in := baseInput()
	in.Agent = "amp"
	env.ExecuteWorkflow(FeatureDevWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	for i, input := range rec.inputs {
		if input.Agent != "amp" {
			t.Errorf("agent round %d Agent = %q, want amp", i, input.Agent)
		}
	}
	if len(rec.inputs) != 2 {
		t.Fatalf("agent ran %d times, want 2 (implement + tests)", len(rec.inputs))
	}
	for i, input := range rev.inputs {
		if input.Agent != "amp" {
			t.Errorf("review round %d Agent = %q, want amp", i, input.Agent)
		}
	}
	if len(suiteAgents) != 1 || suiteAgents[0] != "amp" {
		t.Errorf("suite activity agent args = %v, want one amp", suiteAgents)
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

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
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

// TestFeatureDevWorkflowSessionChaining pins the four role-separated
// sessions: each role's first round starts cold, later rounds of the same
// role resume its session, and no round ever carries another role's session.
func TestFeatureDevWorkflowSessionChaining(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env, result: activities.AgentRunResult{Text: "ok", SessionID: "agent-s1"}}
	rec.record()

	// Code review rejects once, then approves; test review approves. The
	// code-review rounds report one reviewer session; the test review would
	// report its own (empty here — it is the last review, nothing resumes
	// it).
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: false, Comments: "rename foo to bar", SessionID: "review-s1"},
		{Approved: true, SessionID: "review-s1"},
	}}
	rev.record()

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	if len(rec.inputs) != 3 {
		t.Fatalf("agent ran %d times, want 3 (implement, review-fix, tests)", len(rec.inputs))
	}
	if got := rec.inputs[0].SessionID; got != "" {
		t.Errorf("implement round SessionID = %q, want empty (dev agent starts cold)", got)
	}
	if got := rec.inputs[1].SessionID; got != "agent-s1" {
		t.Errorf("fix round SessionID = %q, want the dev agent session", got)
	}
	if got := rec.inputs[2].SessionID; got != "" {
		t.Errorf("tests round SessionID = %q, want empty (test agent is its own session, not the dev agent's)", got)
	}

	if len(rev.inputs) != 3 {
		t.Fatalf("reviewer ran %d times, want 3 (code review x2, test review)", len(rev.inputs))
	}
	if got := rev.inputs[0].SessionID; got != "" {
		t.Errorf("first code review SessionID = %q, want empty (dev reviewer starts cold)", got)
	}
	if got := rev.inputs[1].SessionID; got != "review-s1" {
		t.Errorf("second code review SessionID = %q, want the dev reviewer session", got)
	}
	if got := rev.inputs[2].SessionID; got != "" {
		t.Errorf("test review SessionID = %q, want empty (test reviewer is separate from the dev reviewer)", got)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowSessionFallback pins the lost-session fallback: a
// resumed round that fails outright (its session no longer exists under the
// jail's state dir, say) is retried once with a fresh session instead of
// failing the whole run.
func TestFeatureDevWorkflowSessionFallback(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env, script: []agentStep{
		// implement: establishes the dev agent session
		{result: activities.AgentRunResult{Text: "ok", SessionID: "sess-1"}},
		// fix: the resume fails — session gone
		{err: errors.New("no conversation found with session ID: sess-1")},
		// fix retried fresh: succeeds under a new session
		{result: activities.AgentRunResult{Text: "ok", SessionID: "sess-2"}},
	}, result: activities.AgentRunResult{Text: "ok"}}
	rec.record()

	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: false, Comments: "rename foo to bar"},
		{Approved: true},
	}}
	rev.record()

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	if len(rec.inputs) != 4 {
		t.Fatalf("agent ran %d times, want 4 (implement, failed fix, fresh fix, tests)", len(rec.inputs))
	}
	if got := rec.inputs[1].SessionID; got != "sess-1" {
		t.Errorf("failed fix round SessionID = %q, want sess-1 (the resumed session)", got)
	}
	if got := rec.inputs[2].SessionID; got != "" {
		t.Errorf("fallback fix round SessionID = %q, want empty (fresh session)", got)
	}
	if got := rec.inputs[3].SessionID; got != "" {
		t.Errorf("tests round SessionID = %q, want empty (test agent starts cold regardless of the dev session)", got)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowReviewerSessionFallback pins the reviewer's
// lost-session fallback: a resumed review that fails outright is retried
// once with a fresh session, same as the agent rounds.
func TestFeatureDevWorkflowReviewerSessionFallback(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env, result: activities.AgentRunResult{Text: "ok"}}
	rec.record()

	// Code review 1 rejects (establishing the reviewer session); code
	// review 2's resume fails; code review 2 retried fresh approves. Test
	// review approves.
	rev := &reviewerRecorder{env: env, script: []reviewStep{
		{result: activities.ReviewResult{Approved: false, Comments: "rename foo to bar", SessionID: "rev-1"}},
		{err: errors.New("no conversation found with session ID: rev-1")},
		{result: activities.ReviewResult{Approved: true}},
	}, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	if len(rev.inputs) != 4 {
		t.Fatalf("reviewer ran %d times, want 4 (code review, failed resume, fresh retry, test review)", len(rev.inputs))
	}
	if got := rev.inputs[1].SessionID; got != "rev-1" {
		t.Errorf("failed resume review SessionID = %q, want rev-1", got)
	}
	if got := rev.inputs[2].SessionID; got != "" {
		t.Errorf("fallback review SessionID = %q, want empty (fresh session)", got)
	}
	if got := rev.inputs[3].SessionID; got != "" {
		t.Errorf("test review SessionID = %q, want empty (own session, unaffected by the dev reviewer's fallback)", got)
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
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
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

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
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

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: false, Logs: "--- FAIL: TestBoom\nboom"}, nil).Once()
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
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

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
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
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
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
	if !strings.Contains(err.Error(), "cut off 3 rounds in a row") {
		t.Errorf("error does not name the streak: %v", err)
	}
	env.AssertExpectations(t)
}

// errKilledStub is an abruptly killed agent round as the workflow sees it:
// the ErrAgentKilled sentinel wrapped the way runJailed wraps it in
// production — wait-status-derived, with no agent output embedded.
var errKilledStub = fmt.Errorf("%w: signal: killed", activities.ErrAgentKilled)

// TestFeatureDevWorkflowAgentKilledRecovers pins that an externally killed
// round — not a timeout, none of daedalus's own kill paths — is recovered
// like one: the run continues from the round's partial work instead of
// failing, and the continuation prompt says the round was cut off abruptly.
func TestFeatureDevWorkflowAgentKilledRecovers(t *testing.T) {
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
				return activities.AgentRunResult{}, errKilledStub
			}
			return activities.AgentRunResult{Text: "done"}, nil
		})
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("agent calls = %d, want 3 (killed round, continuation, tests round)", calls)
	}
	if len(inputs) != 3 || inputs[1].Prompt == inputs[0].Prompt {
		t.Fatalf("retry must re-prompt, inputs recorded: %d", len(inputs))
	}
	if !strings.Contains(inputs[1].Prompt, "killed mid-round") {
		t.Errorf("retry prompt does not explain the abrupt cutoff: %q", inputs[1].Prompt)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowAgentKilledStreakFails pins that kills count against
// the same streak as timeouts: a run killed every round fails loudly instead
// of looping forever.
func TestFeatureDevWorkflowAgentKilledStreakFails(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Return(activities.AgentRunResult{}, errKilledStub)
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error after a kill streak")
	}
	if !strings.Contains(err.Error(), "cut off 3 rounds in a row") {
		t.Errorf("error does not name the streak: %v", err)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowAgentKilledWrappedErrorClassifies pins the
// workflow side of the kill sentinel: across the Temporal activity
// boundary the sentinel arrives as error text inside the worker's wrapper,
// so classification is a substring match — the sentinel embedded in
// surrounding wrapper text still routes the round to kill recovery.
func TestFeatureDevWorkflowAgentKilledWrappedErrorClassifies(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	var prompts []string
	var calls int
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.AgentRunInput); ok {
					prompts = append(prompts, in.Prompt)
				}
			}
		}).
		Return(func(ctx context.Context, in activities.AgentRunInput) (activities.AgentRunResult, error) {
			calls++
			if calls == 1 {
				return activities.AgentRunResult{}, fmt.Errorf(
					"activity RunJailedClaudeActivity (type ...) failed: %w", errKilledStub)
			}
			return activities.AgentRunResult{Text: "done"}, nil
		})
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("agent calls = %d, want 3 (killed round, continuation, tests round)", calls)
	}
	if len(prompts) != 3 || !strings.Contains(prompts[1], "killed mid-round") {
		t.Errorf("sentinel inside wrapper text did not route to kill recovery, prompts: %d", len(prompts))
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowKillWordsWithoutSentinelFailNormally is the flip
// side of the substring surface: a generic failure whose embedded output
// tail merely says something similar — signal-death words without the
// sentinel itself — must not be diverted into kill recovery.
func TestFeatureDevWorkflowKillWordsWithoutSentinelFailNormally(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Return(activities.AgentRunResult{}, errors.New(
			"agent failed; output tail: ... terminating on signal: killed ... exit status 1"))
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error from a normal agent failure")
	}
	if strings.Contains(err.Error(), "cut off") {
		t.Errorf("near-miss text diverted into kill recovery: %v", err)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowKillOutranksExhaustion pins the classification
// order on the workflow side: an error carrying both the kill sentinel and
// exhaustion text routes to kill recovery — a continuation prompt that
// resumes from partial work — instead of parking the round in the quota
// heartbeat, which would re-queue the same prompt unchanged.
func TestFeatureDevWorkflowKillOutranksExhaustion(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	var prompts []string
	var calls int
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.AgentRunInput); ok {
					prompts = append(prompts, in.Prompt)
				}
			}
		}).
		Return(func(ctx context.Context, in activities.AgentRunInput) (activities.AgentRunResult, error) {
			calls++
			if calls == 1 {
				return activities.AgentRunResult{}, fmt.Errorf(
					"%w: %w", errKilledStub, activities.ErrAPIExhausted)
			}
			return activities.AgentRunResult{Text: "done"}, nil
		})
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(prompts) < 2 || prompts[1] == prompts[0] {
		t.Fatalf("round must be re-prompted as a continuation, prompts recorded: %d", len(prompts))
	}
	if !strings.Contains(prompts[1], "killed mid-round") {
		t.Errorf("kill+exhaustion error did not route to kill recovery; retry prompt: %q", prompts[1])
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowCleanupTimeoutWiring pins that the deferred cleanup
// runs under its own ceiling: the configured CleanupTimeout reaches the
// activity as its StartToCloseTimeout, and a zero (a run started by an
// older worker, replayed without the field) falls back to
// config.DefaultCleanupTimeout.
func TestFeatureDevWorkflowCleanupTimeoutWiring(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"configured", 7 * time.Minute, 7 * time.Minute},
		{"zero falls back to default", 0, config.DefaultCleanupTimeout},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newTestEnv(t)
			var got time.Duration
			env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
				Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
			rec := &agentRecorder{env: env}
			rec.record()
			rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
			rev.record()
			env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
				Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
			env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
				Return("daedalus/issue-42-1", nil).Once()
			env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) {
					got = activity.GetInfo(args.Get(0).(context.Context)).StartToCloseTimeout
				}).
				Return(nil).Once()
			in := baseInput()
			in.CleanupTimeout = c.in
			env.ExecuteWorkflow(FeatureDevWorkflow, in)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow error: %v", err)
			}
			if got != c.want {
				t.Errorf("cleanup StartToCloseTimeout = %v, want %v", got, c.want)
			}
			env.AssertExpectations(t)
		})
	}
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
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, path, agent string) (activities.TestResult, error) {
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
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
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

// TestFeatureDevWorkflowReviewerKilledRetries pins that a reviewer round
// lost to an abrupt kill (not a timeout) is retried like a timed-out one.
func TestFeatureDevWorkflowReviewerKilledRetries(t *testing.T) {
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
				return activities.ReviewResult{}, errKilledStub
			}
			return activities.ReviewResult{Approved: true}, nil
		})
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if reviewCalls < 2 {
		t.Fatalf("review calls = %d, want the killed round retried", reviewCalls)
	}
	env.AssertExpectations(t)
}

// errQuotaStub mirrors how a quota-exhausted activity round surfaces on the
// workflow side: the ErrAPIExhausted sentinel wrapped in richer text, matched
// by substring across the worker→workflow boundary.
var errQuotaStub = fmt.Errorf("run jailed: %w: usage limit reached", activities.ErrAPIExhausted)

// errSlotStub is a round that gave up waiting for a concurrency slot, as
// the workflow sees it: the sentinel wrapped the way the activities wrap
// it in production (runJailed's own %w here, "review run:"/"discover test
// command:" prefixes at the other call sites).
var errSlotStub = fmt.Errorf("%w: no slot freed within 5m0s", activities.ErrAgentSlotsBusy)

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
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
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

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
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
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, path, agent string) (activities.TestResult, error) {
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

// TestFeatureDevWorkflowSlotWaitRetries pins the agent call site: a round
// that gave up waiting for a concurrency slot is re-queued unchanged after
// the backoff — the prompt must not become a timeout continuation, whose
// "here's what you produced so far" framing would fabricate partial work
// for a round that never launched.
func TestFeatureDevWorkflowSlotWaitRetries(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

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
			if calls == 1 {
				return activities.AgentRunResult{}, errSlotStub
			}
			return activities.AgentRunResult{Text: "done"}, nil
		})

	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("agent calls = %d, want 3 (slot-busy implement round, re-queue, tests round)", calls)
	}
	if inputs[1].Prompt != inputs[0].Prompt {
		t.Errorf("slot re-queue changed the prompt:\n%q\n!=\n%q", inputs[1].Prompt, inputs[0].Prompt)
	}
	if strings.Contains(inputs[1].Prompt, "timeout ceiling") {
		t.Errorf("slot re-queue was answered as a timeout continuation: %q", inputs[1].Prompt)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowReviewerSlotWaitRetries pins the review call site:
// a reviewer round that never got a slot is retried with the same focus,
// and the verdict then flows normally.
func TestFeatureDevWorkflowReviewerSlotWaitRetries(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()

	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	rev.script = []reviewStep{{err: errSlotStub}}

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(activities.TestResult{Passed: true, Logs: "ok"}, nil).Once()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(rev.inputs) != 3 {
		t.Fatalf("review calls = %d, want 3 (slot-busy code review, re-queue, test review)", len(rev.inputs))
	}
	if rev.inputs[1].Focus != rev.inputs[0].Focus {
		t.Errorf("slot re-queue changed the review focus: %q != %q", rev.inputs[1].Focus, rev.inputs[0].Focus)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowTestsSlotWaitRetries pins the suite call site:
// test-command discovery queues on the same semaphore as every other
// jailed round, so the suite round can come back slot-busy too — and is
// re-queued rather than failing the run.
func TestFeatureDevWorkflowTestsSlotWaitRetries(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	var suiteCalls int
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, path, agent string) (activities.TestResult, error) {
			suiteCalls++
			if suiteCalls == 1 {
				return activities.TestResult{}, errSlotStub
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
		t.Fatalf("suite calls = %d, want 2 (slot-busy discovery round plus re-queue)", suiteCalls)
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
	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, path, agent string) (activities.TestResult, error) {
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

	env.OnActivity(activities.RunNativeTestsActivity, mock.Anything, mock.Anything, mock.Anything).
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
