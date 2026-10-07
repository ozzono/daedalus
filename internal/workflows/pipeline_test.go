package workflows

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

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

// registerPipelineActivities registers the pipeline's activities with a
// test workflow environment (the mocks below stub their outcomes).
func registerPipelineActivities(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterActivity(activities.CreateWorktreeActivity)
	env.RegisterActivity(activities.RunJailedClaudeActivity)
	env.RegisterActivity(activities.RunJailedReviewerActivity)
	env.RegisterActivity(activities.ResolveTestCommandActivity)
	env.RegisterActivity(activities.RunTestSuiteActivity)
	env.RegisterActivity(activities.ReproFirstGateActivity)
	env.RegisterActivity(activities.VerifyWriteScopeActivity)
	env.RegisterActivity(activities.FinalizeWorktreeActivity)
	env.RegisterActivity(activities.CleanupWorktreeActivity)
}

// newTestEnv builds an in-process workflow environment with the pipeline's
// activities mocked out.
func newTestEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	s := &testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(FeatureDevWorkflow)
	env.RegisterWorkflow(TestOnlyWorkflow)
	env.RegisterWorkflow(BugFixWorkflow)
	env.RegisterWorkflow(InvestigateWorkflow)
	env.RegisterWorkflow(RefactorWorkflow)
	env.RegisterWorkflow(DevOnlyWorkflow)
	env.RegisterWorkflow(SlimWorkflow)
	registerPipelineActivities(env)
	return env
}

// newCompleteGreenEnv builds an environment with feature-dev registered the
// way the worker registers it — wrapped in CompleteGreen under the
// unwrapped flow's type name — and nothing else under that name. The SDK
// registry rejects re-registering a name, so the wrapped-registration tests
// cannot reuse newTestEnv.
func newCompleteGreenEnv(t *testing.T) (*testsuite.TestWorkflowEnvironment, string) {
	t.Helper()
	s := &testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	registerPipelineActivities(env)
	name := WorkflowTypeName(FeatureDevWorkflow)
	env.RegisterWorkflowWithOptions(CompleteGreen(FeatureDevWorkflow), workflow.RegisterOptions{Name: name})
	return env, name
}

// discoveryStep is one scripted test-command-discovery outcome: the
// recorder plays the script in order, one entry per call, then falls back
// to its default command.
type discoveryStep struct {
	command string
	err     error
}

// discoveryRecorder captures every ResolveTestCommandActivity call — the
// jailed round that resolves the suite's command — and plays its script
// in order before falling back to a green `go test ./...`.
type discoveryRecorder struct {
	env    *testsuite.TestWorkflowEnvironment
	agents []string
	script []discoveryStep
}

func (r *discoveryRecorder) record() {
	// Maybe: discovery errors legitimately leave the suite (and, in the
	// case tests, either activity) uncalled; round-count assertions in the
	// tests pin how often each actually runs.
	r.env.OnActivity(activities.ResolveTestCommandActivity, mock.Anything, mock.Anything, mock.Anything).Maybe().
		Run(func(args mock.Arguments) {
			// argv shape: (ctx, worktreePath, agent) — the trailing string.
			if n := len(args); n > 0 {
				if agent, ok := args.Get(n - 1).(string); ok {
					r.agents = append(r.agents, agent)
				}
			}
		}).
		Return(func(ctx context.Context, path, agent string) (string, error) {
			if len(r.script) > 0 {
				step := r.script[0]
				r.script = r.script[1:]
				return step.command, step.err
			}
			return "go test ./...", nil
		})
}

// suiteStep is one scripted native-suite execution outcome.
type suiteStep struct {
	result activities.TestResult
	err    error
}

// suiteRecorder captures every RunTestSuiteActivity input — the suite
// executions on the test task queue — and plays its script in order before
// falling back to a green run.
type suiteRecorder struct {
	env    *testsuite.TestWorkflowEnvironment
	runs   []activities.TestRunInput
	script []suiteStep
}

func (r *suiteRecorder) record() {
	r.env.OnActivity(activities.RunTestSuiteActivity, mock.Anything, mock.Anything).Maybe().
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.TestRunInput); ok {
					r.runs = append(r.runs, in)
				}
			}
		}).
		Return(func(ctx context.Context, in activities.TestRunInput) (activities.TestResult, error) {
			if len(r.script) > 0 {
				step := r.script[0]
				r.script = r.script[1:]
				return step.result, step.err
			}
			return activities.TestResult{Passed: true, Logs: "ok"}, nil
		})
}

// stubTestPhase wires green defaults for both test-phase activities —
// command discovery on the main queue and the suite run on the test queue.
// Tests needing scripted outcomes or call records reach into the returned
// recorders.
func stubTestPhase(env *testsuite.TestWorkflowEnvironment) (*discoveryRecorder, *suiteRecorder) {
	disc := &discoveryRecorder{env: env}
	disc.record()
	suite := &suiteRecorder{env: env}
	suite.record()
	return disc, suite
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
	env *testsuite.TestWorkflowEnvironment
	// inputs and timeouts are indexed together: inputs[i] ran under the
	// StartToCloseTimeout timeouts[i].
	inputs   []activities.ReviewInput
	timeouts []time.Duration
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
				if ctx, ok := a.(context.Context); ok {
					r.timeouts = append(r.timeouts, activity.GetInfo(ctx).StartToCloseTimeout)
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
	// The expected prompts below pin the no-bug-dir rendering; a worker host
	// with bug_filing enabled exports DAEDALUS_BUG_DIR into this process.
	t.Setenv(config.BugDirEnv, "")
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

	_, suiteRec := stubTestPhase(env)

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
	// The suite ran twice — the preflight baseline plus the phase-2 round —
	// both executing the command discovery resolved in the worktree the run
	// created — the discover→suite handoff.
	if len(suiteRec.runs) != 2 {
		t.Fatalf("suite ran %d times, want 2 (preflight baseline plus phase 2)", len(suiteRec.runs))
	}
	for i, run := range suiteRec.runs {
		if run.Command != "go test ./..." || run.WorktreePath != "/wt/issue-42" {
			t.Errorf("suite run %d = %+v, want the resolved command in the run's worktree", i, run)
		}
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
	want, err := template.Implement("implement the feature", "")
	if err != nil {
		t.Fatalf("build expected implement prompt: %v", err)
	}
	if rec.inputs[0].Prompt != want {
		t.Errorf("implement prompt = %q, want %q", rec.inputs[0].Prompt, want)
	}
	want, err = template.Tests("")
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

// TestFeatureDevWorkflowBugDirPrompt pins the env channel the bug-filing
// toggle travels on: the workflow reads DAEDALUS_BUG_DIR from the worker
// process (exported at startup iff bug_filing is enabled — never workflow
// history or activity inputs) and hands it to the round-prompt builders,
// so the opener and the tests prompt carry the configured bug dir.
func TestFeatureDevWorkflowBugDirPrompt(t *testing.T) {
	t.Setenv(config.BugDirEnv, "docs/known-bugs")
	env := newTestEnv(t)

	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil).Once()

	rec := &agentRecorder{env: env}
	rec.record()

	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	stubTestPhase(env)

	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
		Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	if len(rec.inputs) != 2 {
		t.Fatalf("agent ran %d times, want 2 (implement + tests)", len(rec.inputs))
	}
	want, err := template.Implement("implement the feature", "docs/known-bugs")
	if err != nil {
		t.Fatalf("build expected implement prompt: %v", err)
	}
	if rec.inputs[0].Prompt != want {
		t.Errorf("implement prompt = %q, want the bug-dir variant", rec.inputs[0].Prompt)
	}
	want, err = template.Tests("docs/known-bugs")
	if err != nil {
		t.Fatalf("build expected tests prompt: %v", err)
	}
	if rec.inputs[1].Prompt != want {
		t.Errorf("test-phase prompt = %q, want the bug-dir variant", rec.inputs[1].Prompt)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowAgentTravelsToEveryRound pins the -cli plumbing: a
// run started with Agent set carries it into every jailed round — the agent
// rounds, both review phases, and test-command discovery (whose AI fallback
// runs a jailed agent too) — so the override applies on whichever worker
// serves the queue, and the worker's own DAEDALUS_AGENT is never consulted
// for the run. The suite execution itself is agent-free: it carries only
// the resolved command.
func TestFeatureDevWorkflowAgentTravelsToEveryRound(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	disc, suiteRec := stubTestPhase(env)
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
	if len(disc.agents) != 2 || disc.agents[0] != "amp" || disc.agents[1] != "amp" {
		t.Errorf("discovery agent args = %v, want amp twice (preflight discovery, phase-2 discovery)", disc.agents)
	}
	if len(suiteRec.runs) != 2 || suiteRec.runs[0].Command != "go test ./..." {
		t.Errorf("suite runs = %+v, want the preflight baseline plus phase 2 executing the resolved command", suiteRec.runs)
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

	stubTestPhase(env)
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

	stubTestPhase(env)
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

	stubTestPhase(env)
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

	stubTestPhase(env)
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
	stubTestPhase(env)
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

// TestFeatureDevWorkflowAuthorshipWiring pins that the authorship flag
// reaches every worktree activity that commits on daedalus's behalf: the
// preserve (cleanup) and finalize commits only carry the forced
// "daedalus <daedalus@local>" identity when PipelineInput.Authorship is
// set, and the create path receives the flag untouched.
func TestFeatureDevWorkflowAuthorshipWiring(t *testing.T) {
	env := newTestEnv(t)

	var created, cleanedUp activities.WorktreeInput
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			created = args.Get(1).(activities.WorktreeInput)
		}).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	stubTestPhase(env)
	var finalized activities.WorktreeInput
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			finalized = args.Get(1).(activities.WorktreeInput)
		}).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			cleanedUp = args.Get(1).(activities.WorktreeInput)
		}).
		Return(nil).Once()

	in := baseInput()
	in.Authorship = true
	env.ExecuteWorkflow(FeatureDevWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if !created.Authorship {
		t.Errorf("CreateWorktreeActivity Authorship = false, want true")
	}
	if !finalized.Authorship {
		t.Errorf("FinalizeWorktreeActivity Authorship = false, want true")
	}
	if !cleanedUp.Authorship {
		t.Errorf("CleanupWorktreeActivity Authorship = false, want true")
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

	stubTestPhase(env)

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

	_, suiteRec := stubTestPhase(env)
	// The green first entry is the preflight gate's baseline; the test
	// loop's red round follows it.
	suiteRec.script = []suiteStep{
		{result: activities.TestResult{Passed: true, Logs: "ok"}},
		{result: activities.TestResult{Passed: false, Logs: "--- FAIL: TestBoom\nboom"}},
	}

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

// TestFeatureDevWorkflowTestOutputDumpRelay pins the dump relay end to end:
// the config-resolved dir rides the pipeline input into every suite
// activity call (never worker env), a dumped round's Logs gain one line
// naming the file wherever they travel — the fix prompt and the test
// review — and a round with no dump (off, or the write failed) travels
// byte-identical to before.
func TestFeatureDevWorkflowTestOutputDumpRelay(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()

	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: true},
		{Approved: true},
	}}
	rev.record()

	_, suiteRec := stubTestPhase(env)
	// Green preflight baseline (no dump recorded), red phase-2 round with
	// a dump, then the green fix round (no dump — as if the write failed).
	const dumpPath = ".daedalus/test-output/20260927-101112.log"
	suiteRec.script = []suiteStep{
		{result: activities.TestResult{Passed: true, Logs: "ok"}},
		{result: activities.TestResult{Passed: false, Logs: "--- FAIL: TestBoom\nboom", DumpPath: dumpPath}},
		{result: activities.TestResult{Passed: true, Logs: "ok"}},
	}

	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-3", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	input := baseInput()
	input.TestOutputDir = ".daedalus/test-output"
	env.ExecuteWorkflow(FeatureDevWorkflow, input)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	// The dir rides the activity input, so a shared test queue landing the
	// suite on a foreign deployment's worker still dumps into this run's
	// worktree-relative dir.
	if len(suiteRec.runs) != 3 {
		t.Fatalf("suite ran %d times, want 3 (baseline, red round, fix round)", len(suiteRec.runs))
	}
	for i, run := range suiteRec.runs {
		if run.OutputDir != ".daedalus/test-output" {
			t.Errorf("suite run %d OutputDir = %q, want the pipeline input's dir", i, run.OutputDir)
		}
	}
	// The red round's dump line rides Logs into the fix prompt...
	if len(rec.inputs) != 3 {
		t.Fatalf("agent ran %d times, want 3 (implement, tests, tests-fix)", len(rec.inputs))
	}
	fixPrompt := rec.inputs[2].Prompt
	if !strings.Contains(fixPrompt, "full suite output: "+dumpPath) {
		t.Errorf("test-fix prompt should name the dumped record %s: %q", dumpPath, fixPrompt)
	}
	// ...and into the reviewer round that saw the failing output.
	if !strings.Contains(rev.inputs[1].TestLogs, "full suite output: "+dumpPath) {
		t.Errorf("test review logs should name the dumped record: %q", rev.inputs[1].TestLogs)
	}
	// A round with no dump adds nothing: the fix round's green Logs reach
	// the second test review byte-identical to the pre-toggle shape.
	if len(rev.inputs) != 3 {
		t.Fatalf("reviewer ran %d times, want 3 (code, red test review, green test review)", len(rev.inputs))
	}
	if rev.inputs[2].TestLogs != "ok" {
		t.Errorf("second test review logs = %q, want the undumped round's logs with no relay line", rev.inputs[2].TestLogs)
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

	stubTestPhase(env)
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
	stubTestPhase(env)
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

// TestFeatureDevWorkflowDiscoveryErrorFails covers the discovery side of the
// test-phase split: any ResolveTestCommandActivity error other than slot
// wait or API exhaustion — a missing go binary, the timeout ceiling — is
// not suite output a reviewer can act on, so the run fails instead of
// feeding the fix loop a synthetic red round, and that discovery's suite
// never runs. The green first discovery step passes the preflight gate;
// the failing one is the phase-2 round.
func TestFeatureDevWorkflowDiscoveryErrorFails(t *testing.T) {
	for _, errStub := range []error{errTestsStub, errTimeoutStub} {
		env := newTestEnv(t)
		env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
			Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

		rec := &agentRecorder{env: env}
		rec.record()

		rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
		rev.record()

		disc, suiteRec := stubTestPhase(env)
		disc.script = []discoveryStep{
			{command: "go test ./..."},
			{err: errStub},
		}

		env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

		env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

		err := env.GetWorkflowError()
		if err == nil {
			t.Fatalf("want workflow error when discovery fails with %v", errStub)
		}
		if !strings.Contains(err.Error(), "resolve test command") {
			t.Errorf("error %q should name the discovery step", err)
		}
		if len(suiteRec.runs) != 1 {
			t.Errorf("suite ran %d times, want 1 (only the preflight baseline — the failed discovery ran no suite)", len(suiteRec.runs))
		}
		if len(rec.inputs) != 2 {
			t.Errorf("agent ran %d times, want 2 (no fix round for a discovery error)", len(rec.inputs))
		}
		env.AssertExpectations(t)
	}
}

// TestFeatureDevWorkflowNoSuitePreflightPassesVacuously pins the NONE side
// of the preflight gate: a discovery round concluding the repository has no
// test suite at all (activities.ErrNoSuite) is not a red baseline — the
// gate passes vacuously and the run proceeds, the sentinel's classification
// surviving the worker→workflow boundary as message text (isNoSuite's
// contract). No baseline suite runs; the only suite execution is the
// phase-2 round on discovery's green fallback.
func TestFeatureDevWorkflowNoSuitePreflightPassesVacuously(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()

	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	disc, suiteRec := stubTestPhase(env)
	disc.script = []discoveryStep{{err: activities.ErrNoSuite}}

	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(disc.agents) != 2 {
		t.Errorf("discovery ran %d times, want 2 (no-suite preflight, phase 2)", len(disc.agents))
	}
	if len(suiteRec.runs) != 1 {
		t.Errorf("suite ran %d times, want 1 (the vacuous gate pass runs no baseline suite)", len(suiteRec.runs))
	}
	var branch string
	if err := env.GetWorkflowResult(&branch); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if branch != "daedalus/issue-42-1" {
		t.Errorf("workflow result = %q, want the preserved branch name", branch)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowTestActivityError covers the system-error branch of
// the suite execution itself: a failing RunTestSuiteActivity (e.g. the shell
// missing on the test worker) must fail the workflow instead of being
// treated as failed tests.
func TestFeatureDevWorkflowTestActivityError(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()

	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	disc, suiteRec := stubTestPhase(env)
	// The green first entry passes the preflight gate; the failing one is
	// the phase-2 suite run.
	suiteRec.script = []suiteStep{
		{result: activities.TestResult{Passed: true, Logs: "ok"}},
		{err: errTestsStub},
	}

	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error when the test activity errors")
	}
	if !strings.Contains(err.Error(), "run tests") {
		t.Errorf("error %q should mention the test run", err)
	}
	if len(disc.agents) != 2 {
		t.Errorf("discovery ran %d times, want 2 (preflight, phase 2)", len(disc.agents))
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
	stubTestPhase(env)
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
	stubTestPhase(env)
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
	stubTestPhase(env)
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
	stubTestPhase(env)
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
	stubTestPhase(env)
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
	stubTestPhase(env)
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
	stubTestPhase(env)
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
			stubTestPhase(env)
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

// TestFeatureDevWorkflowReviewTimeoutWiring pins that the reviewer rounds
// run under their own ceiling: the configured ReviewTimeout reaches the
// review activity as its StartToCloseTimeout, and a zero (a run started by
// an older worker, replayed without the field) falls back to
// config.DefaultReviewTimeout.
func TestFeatureDevWorkflowReviewTimeoutWiring(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"configured", 20 * time.Minute, 20 * time.Minute},
		{"zero falls back to default", 0, config.DefaultReviewTimeout},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newTestEnv(t)
			env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
				Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
			rec := &agentRecorder{env: env}
			rec.record()
			rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
			rev.record()
			stubTestPhase(env)
			env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
				Return("daedalus/issue-42-1", nil).Once()
			env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
			in := baseInput()
			in.ReviewTimeout = c.in
			env.ExecuteWorkflow(FeatureDevWorkflow, in)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow error: %v", err)
			}
			if len(rev.timeouts) != 2 {
				t.Fatalf("reviewer ran %d times, want 2 (code + tests)", len(rev.timeouts))
			}
			for i, got := range rev.timeouts {
				if got != c.want {
					t.Errorf("review %d StartToCloseTimeout = %v, want %v", i, got, c.want)
				}
			}
			env.AssertExpectations(t)
		})
	}
}

// TestFeatureDevWorkflowRolesScopeEveryRound pins that every jailed round
// carries its conversation role: the activities' recorded-session fallback
// must be able to tell the run's four conversations apart, so the workflow
// stamps the role into each round's input.
func TestFeatureDevWorkflowRolesScopeEveryRound(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	stubTestPhase(env)
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	if len(rec.inputs) != 2 {
		t.Fatalf("agent ran %d times, want 2 (implement + tests)", len(rec.inputs))
	}
	if got := rec.inputs[0].Role; got != activities.RoleDev {
		t.Errorf("implement round Role = %q, want %q", got, activities.RoleDev)
	}
	if got := rec.inputs[1].Role; got != activities.RoleTest {
		t.Errorf("tests round Role = %q, want %q (the test agent is its own conversation)", got, activities.RoleTest)
	}
	if len(rev.inputs) != 2 {
		t.Fatalf("reviewer ran %d times, want 2 (code + tests)", len(rev.inputs))
	}
	if got := rev.inputs[0].Role; got != activities.RoleDevReview {
		t.Errorf("code review Role = %q, want %q", got, activities.RoleDevReview)
	}
	if got := rev.inputs[1].Role; got != activities.RoleTestReview {
		t.Errorf("test review Role = %q, want %q (the test reviewer is its own conversation)", got, activities.RoleTestReview)
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
	_, suiteRec := stubTestPhase(env)
	// The green first entry passes the preflight gate; the timed-out round
	// and its rerun follow.
	suiteRec.script = []suiteStep{
		{result: activities.TestResult{Passed: true, Logs: "ok"}},
		{err: errTimeoutStub},
	}
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(suiteRec.runs) != 3 {
		t.Fatalf("suite runs = %d, want 3 (preflight baseline, timed-out round, rerun)", len(suiteRec.runs))
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
	stubTestPhase(env)
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
	stubTestPhase(env)
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
	stubTestPhase(env)
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

	stubTestPhase(env)
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

// TestFeatureDevWorkflowQuotaHeartbeatCancelled pins that a cancellation
// landing mid-heartbeat — a brand-new window, now that an exhausted run
// sits in an hourly sleep instead of failing immediately — must fail the
// run promptly rather than retry past the error, and still clean up. The
// heartbeat's sleep is a selector over its timer and the "wakeup" signal,
// so the cancellation surfaces as the pending agent activity's canceled
// error, not a sleep-specific wrapper.
func TestFeatureDevWorkflowQuotaHeartbeatCancelled(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	var calls int
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { calls++ }).
		Return(activities.AgentRunResult{}, errQuotaStub)

	stubTestPhase(env)
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
	if !strings.Contains(err.Error(), "canceled") {
		t.Errorf("error %q should be the agent activity's cancellation", err)
	}
	if calls != 1 {
		t.Errorf("agent calls = %d, want 1 (no retry past a cancelled sleep)", calls)
	}
	if cleanupCount != 1 {
		t.Fatalf("cleanup ran %d times after cancellation, want 1", cleanupCount)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowWakeupInterruptsQuotaHeartbeat pins the "wakeup"
// signal (`daedalus worker wakeup`): a wakeup landing mid-heartbeat ends
// the sleep immediately, so the round retries — and the run finishes —
// long before the hour is out. The cancel 45 minutes in is the
// discriminator: if the signal branch were dead, the hour-long timer would
// still be pending then and the cancel would kill the run before its
// second agent call.
func TestFeatureDevWorkflowWakeupInterruptsQuotaHeartbeat(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	var calls int
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, in activities.AgentRunInput) (activities.AgentRunResult, error) {
			calls++
			if calls == 1 {
				return activities.AgentRunResult{}, errQuotaStub
			}
			return activities.AgentRunResult{Text: "done"}, nil
		})

	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()
	stubTestPhase(env)
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	// The first agent call exhausts the API and the run enters its hourly
	// sleep; wake it 30 minutes in.
	env.RegisterDelayedCallback(func() { env.SignalWorkflow("wakeup", "") }, 30*time.Minute)
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 45*time.Minute)

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	// (the same mock serves both rounds: the wakeup-resumed implement
	// retry, then the tests round's agent call — all before the cancel).
	if calls != 3 {
		t.Errorf("agent calls = %d, want 3 (the wakeup-resumed retry must land before the 45-minute cancel)", calls)
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

	stubTestPhase(env)
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

// TestFeatureDevWorkflowTestsQuotaHeartbeatRecovers pins the discovery call
// site: test-command discovery is a jailed round, so it can exhaust the API
// too — and is retried after a heartbeat, before the suite ever runs,
// rather than failing the run.
func TestFeatureDevWorkflowTestsQuotaHeartbeatRecovers(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	disc, suiteRec := stubTestPhase(env)
	disc.script = []discoveryStep{{err: errQuotaStub}}

	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(disc.agents) != 3 {
		t.Fatalf("discovery ran %d times, want 3 (exhausted gate discovery, its retry, then phase 2)", len(disc.agents))
	}
	if len(suiteRec.runs) != 2 {
		t.Fatalf("suite ran %d times, want 2 (the gate's baseline after the heartbeat retry, then phase 2)", len(suiteRec.runs))
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
	stubTestPhase(env)
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

	stubTestPhase(env)
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

// TestFeatureDevWorkflowTestsSlotWaitRetries pins the discovery call site:
// test-command discovery queues on the same semaphore as every other
// jailed round, so it can come back slot-busy too — and is re-queued
// rather than failing the run, before the suite ever runs.
func TestFeatureDevWorkflowTestsSlotWaitRetries(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	disc, suiteRec := stubTestPhase(env)
	disc.script = []discoveryStep{{err: errSlotStub}}

	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(disc.agents) != 3 {
		t.Fatalf("discovery ran %d times, want 3 (slot-busy gate discovery, its re-queue, then phase 2)", len(disc.agents))
	}
	if len(suiteRec.runs) != 2 {
		t.Fatalf("suite ran %d times, want 2 (the gate's baseline after the re-queue, then phase 2)", len(suiteRec.runs))
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowQuotaHeartbeatResetsOnRedSuite pins the "even a red
// one" reset: a suite round that ran to completion but failed still resets
// the heartbeat streak. The gate's discovery stays green; the phase-2
// discovery exhausts once (streak = 1 — its re-queue is not a completed
// round and does not reset), and the red suite round — the next completed
// round — is then what clears the streak. The very next call, the test
// review, exhausts: with the reset it gets a full five heartbeats (six
// attempts before parking); a stale streak would park one attempt earlier.
// Routing through the test review, not a fix round, matters: a successful
// review resets the streak itself and would mask the suite round's reset.
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

	disc, suiteRec := stubTestPhase(env)
	// The gate's discovery stays green; the phase-2 discovery exhausts once
	// before its re-queue, seeding the streak the red suite round must reset.
	disc.script = []discoveryStep{
		{command: "go test ./..."},
		{err: errQuotaStub},
	}
	// The green first entry passes the preflight gate; the red round is
	// the phase-2 suite run whose reset is under test.
	suiteRec.script = []suiteStep{
		{result: activities.TestResult{Passed: true, Logs: "ok"}},
		{result: activities.TestResult{Passed: false, Logs: "--- FAIL: TestBoom"}},
	}

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
	if len(agentRec.inputs) != 2 || len(disc.agents) != 3 || len(suiteRec.runs) != 2 {
		t.Errorf("agent ran %d times, discovery %d, suite %d — want 2, 3, 2 (implement, tests; gate, exhausted phase-2 discovery, its retry; green baseline, red round)",
			len(agentRec.inputs), len(disc.agents), len(suiteRec.runs))
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

	stubTestPhase(env)
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

	stubTestPhase(env)
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

// TestFeatureDevWorkflowVerdictlessReviewsPark pins the three-strike budget:
// a marker-less review round still counts as changes requested — its
// comments drive a fix round like any other — but three in a row park the
// run instead of looping forever: a reviewer that never emits any verdict
// line is failing infrastructure (e.g. pi folding every provider request at
// its request timeout), not requesting changes. The park error names the
// strike count and the round's last line, the only trace a fold leaves here.
func TestFeatureDevWorkflowVerdictlessReviewsPark(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	verdictless := activities.ReviewResult{
		Approved: false, NoVerdict: true,
		Comments: "the request folded at the provider timeout\nError: stream ended before completion",
	}
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{verdictless}}
	rev.record()

	stubTestPhase(env)
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	// If the strike budget regresses, the repeating stub would feed this
	// loop forever — bound the test so the regression fails fast instead
	// of hanging until the go-test timeout.
	env.SetTestTimeout(30 * time.Second)

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error from three verdictless review rounds in a row")
	}
	for _, want := range []string{
		ErrAwaitingMaintainer.Error(),
		"without any verdict 3 rounds in a row",
		"Error: stream ended before completion",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	// Two verdictless rounds each drove a fix round before the third parked
	// the run — the changes-requested equivalence holds until the budget.
	if len(rec.inputs) != 3 {
		t.Errorf("agent ran %d times, want 3 (implement plus two fix rounds; none after the park)", len(rec.inputs))
	}
	if len(rev.inputs) != 3 {
		t.Errorf("reviewer ran %d times, want 3", len(rev.inputs))
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowVerdictResetsVerdictlessStreak pins the reset: only
// a review carrying a real verdict marker restarts the strike budget. Two
// marker-less rounds followed by a markered changes-requested round must
// absorb two more marker-less rounds after it — the run completes instead
// of parking at what would be strike three without the reset.
func TestFeatureDevWorkflowVerdictResetsVerdictlessStreak(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	verdictless := activities.ReviewResult{
		Approved: false, NoVerdict: true,
		Comments: "connection reset by peer",
	}
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		verdictless,
		verdictless,
		{Approved: false, Comments: "rename foo to bar"},
		verdictless,
		{Approved: true},
	}}
	rev.record()

	stubTestPhase(env)
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-5", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	// implement, one fix per non-approving round (NV, NV, CHANGES_REQUESTED,
	// NV), then the test phase — the run survived streaks of two around the
	// markered round instead of parking at three consecutive.
	if len(rec.inputs) != 6 {
		t.Errorf("agent ran %d times, want 6 (implement, four fix rounds, tests)", len(rec.inputs))
	}
	if len(rev.inputs) != 6 {
		t.Errorf("reviewer ran %d times, want 6 (five code rounds, test review)", len(rev.inputs))
	}
	// A marker-less round is still a changes-requested round: its comments
	// reach the implementing agent like any other review's.
	if !strings.Contains(rec.inputs[1].Prompt, "connection reset by peer") {
		t.Errorf("fix prompt after a verdictless round = %q, want the round's comments relayed", rec.inputs[1].Prompt)
	}
	var branch string
	if err := env.GetWorkflowResult(&branch); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if branch != "daedalus/issue-42-5" {
		t.Errorf("workflow result = %q, want the preserved branch name", branch)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowIdenticalVerdictsPark pins the identical-verdict
// cap: three review rounds in a row returning a whitespace-identical
// non-approved comments body park the run — a well-formed reviewer that
// never changes its findings is failing to converge, not requesting
// changes, and the fix loop cannot act on what never differs. The third
// verdict differs from the first only in whitespace: the cap compares
// normalized bodies, not bytes.
func TestFeatureDevWorkflowIdenticalVerdictsPark(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	identical := activities.ReviewResult{
		Approved: false,
		Comments: "the handler drops the error path\nreturn err without wrapping",
	}
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		identical,
		identical,
		// Same verdict, reflowed — still strike three.
		{Approved: false, Comments: "the handler drops the error path  return err without wrapping\n"},
	}}
	rev.record()

	stubTestPhase(env)
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	// If the cap regresses, the repeating stub would feed this loop
	// forever — bound the test so the regression fails fast instead of
	// hanging until the go-test timeout.
	env.SetTestTimeout(30 * time.Second)

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error from three identical review verdicts in a row")
	}
	for _, want := range []string{
		ErrAwaitingMaintainer.Error(),
		"consecutive identical review verdicts",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	// Two identical verdicts each drove a fix round before the third
	// parked the run.
	if len(rec.inputs) != 3 {
		t.Errorf("agent ran %d times, want 3 (implement plus two fix rounds; none after the park)", len(rec.inputs))
	}
	if len(rev.inputs) != 3 {
		t.Errorf("reviewer ran %d times, want 3", len(rev.inputs))
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowDifferentVerdictResetsIdenticalStreak pins the
// reset: only a changed comments body restarts the identical-verdict
// budget. Two identical verdicts, a different one, then two more identical
// to the first pair must complete — the run survives the second streak of
// two instead of parking at what would be strike three without the reset.
func TestFeatureDevWorkflowDifferentVerdictResetsIdenticalStreak(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: false, Comments: "rename foo to bar"},
		{Approved: false, Comments: "rename foo to bar"},
		{Approved: false, Comments: "now rename baz to quux"},
		{Approved: false, Comments: "rename foo to bar"},
		{Approved: true},
	}}
	rev.record()

	stubTestPhase(env)
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-5", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	// implement, one fix per non-approving round, then the test phase —
	// the run survived the second streak of two around the changed verdict.
	if len(rec.inputs) != 6 {
		t.Errorf("agent ran %d times, want 6 (implement, four fix rounds, tests)", len(rec.inputs))
	}
	if len(rev.inputs) != 6 {
		t.Errorf("reviewer ran %d times, want 6 (five code rounds, test review)", len(rev.inputs))
	}
	var branch string
	if err := env.GetWorkflowResult(&branch); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if branch != "daedalus/issue-42-5" {
		t.Errorf("workflow result = %q, want the preserved branch name", branch)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowIdenticalVerdictsCappedPerRole pins the per-role
// streaks: the REBUILD cycle interleaves a code-review approval between
// every repeated test-review finding, and that other conversation's
// approval must not reset the test reviewer's count — three identical
// REBUILD bodies park the run even though each was followed by an
// approval. With the streaks shared instead of per-role, this loop would
// never park and the workflow would churn until the test timeout.
func TestFeatureDevWorkflowIdenticalVerdictsCappedPerRole(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rebuild := activities.ReviewResult{Rebuild: true, Comments: "the suite asserts the wrong invariant"}
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: true}, // phase 1 code review
		rebuild,          // test review, strike 1
		{Approved: true}, // code review on the rebuild
		rebuild,          // test review, strike 2
		{Approved: true}, // code review on the rebuild
		rebuild,          // test review, strike 3 — parks
	}}
	rev.record()

	stubTestPhase(env)
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.SetTestTimeout(30 * time.Second)

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error from three identical test-review verdicts across the rebuild cycle")
	}
	for _, want := range []string{
		ErrAwaitingMaintainer.Error(),
		"consecutive identical review verdicts",
		"the test suite",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	// implement, tests, one rebuild round per non-parking REBUILD verdict.
	if len(rec.inputs) != 4 {
		t.Errorf("agent ran %d times, want 4 (implement, tests, two rebuilds)", len(rec.inputs))
	}
	if len(rev.inputs) != 6 {
		t.Errorf("reviewer ran %d times, want 6 (three code rounds, three test rounds)", len(rev.inputs))
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowTestReviewRebuildLoopsThroughDevCycle pins the
// REBUILD loop-back: a test-review REBUILD verdict routes the finding back
// through the dev cycle — a tight, finding-only prompt into the dev
// session, then code review — and the test loop resumes with its reviewer
// session intact, re-running the suite before approval is possible.
func TestFeatureDevWorkflowTestReviewRebuildLoopsThroughDevCycle(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env, result: activities.AgentRunResult{Text: "stub agent output", SessionID: "dev-sess"}}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: true, SessionID: "code-sess"},                                          // phase 1 code review
		{Rebuild: true, Comments: "handler drops the error path", SessionID: "test-sess"}, // rebuild verdict
		{Approved: true, SessionID: "code-sess"},                                          // code review after the rebuild
		{Approved: true, SessionID: "test-sess"},                                          // test review after the rebuild
	}}
	rev.record()

	stubTestPhase(env)
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var branch string
	if err := env.GetWorkflowResult(&branch); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if branch != "daedalus/issue-42" {
		t.Errorf("workflow result = %q, want the preserved branch name", branch)
	}

	// Agent rounds: implement, tests, rebuild — no tests-fix round, the
	// resumed test review approves the passing suite directly.
	if len(rec.inputs) != 3 {
		t.Fatalf("agent ran %d times, want 3 (implement + tests + rebuild)", len(rec.inputs))
	}
	rebuildPrompt := rec.inputs[2].Prompt
	if !strings.Contains(rebuildPrompt, "handler drops the error path") {
		t.Errorf("rebuild prompt %q should carry the finding", rebuildPrompt)
	}
	if implPrompt, err := template.Implement("implement the feature", ""); err != nil || strings.Contains(rebuildPrompt, implPrompt) {
		t.Errorf("rebuild prompt %q should be tight-context, not replay the implement prompt (%v)", rebuildPrompt, err)
	}
	if rec.inputs[2].SessionID != "dev-sess" {
		t.Errorf("rebuild round SessionID = %q, want the dev session resumed", rec.inputs[2].SessionID)
	}

	// Reviewer rounds: code review, test review (rebuild), code review on
	// the rebuild, test review on the resumed loop — each role resuming its
	// own session, and the tester's reply relayed to the test reviewer only.
	if len(rev.inputs) != 4 {
		t.Fatalf("reviewer ran %d times, want 4 (code + test + code + test)", len(rev.inputs))
	}
	if rev.inputs[1].AgentReply == "" {
		t.Error("test review should carry the tester's latest reply")
	}
	if rev.inputs[2].AgentReply != "" || rev.inputs[2].TestsInScope {
		t.Error("the rebuild's code review must not carry the tester relay")
	}
	if rev.inputs[2].SessionID != "code-sess" {
		t.Errorf("post-rebuild code review SessionID = %q, want the code-review session resumed", rev.inputs[2].SessionID)
	}
	if rev.inputs[3].SessionID != "test-sess" {
		t.Errorf("resumed test review SessionID = %q, want the test-review session resumed", rev.inputs[3].SessionID)
	}
	env.AssertExpectations(t)
}

// rebuildScript builds the reviewer verdicts for a run that absorbs n
// REBUILD cycles: the phase-1 code review approves, and every rebuild
// cycle's inner code review approves immediately so the loop returns to
// the test review. Each cycle's finding body is suffixed with its round
// number — a real non-converging loop rewords its findings, and a constant
// body would trip the identical-verdict runaway guard (3 consecutive
// whitespace-identical verdicts park the run) long before the rebuild cap
// this script exists to pin. Call order is exactly the returned slice's
// order.
func rebuildScript(n int, finding string) []activities.ReviewResult {
	script := []activities.ReviewResult{{Approved: true}} // phase 1 code review
	for i := 0; i < n; i++ {
		script = append(script,
			activities.ReviewResult{Rebuild: true, Comments: fmt.Sprintf("%s (rebuild %d)", finding, i+1)}, // test review
			activities.ReviewResult{Approved: true})                                                        // rebuild's code review
	}
	return script
}

// TestFeatureDevWorkflowRebuildCapParks pins the green stage's one bound:
// the 9th REBUILD verdict parks the run for a maintainer restart instead of
// cycling on the provider budget forever. Eight rebuild rounds are absorbed
// first; the park carries the round count and the last finding's comments,
// and the attempt's work is preserved (cleanup, no finalize).
func TestFeatureDevWorkflowRebuildCapParks(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: append(rebuildScript(8, "handler drops the error path"),
		activities.ReviewResult{Rebuild: true, Comments: "the handler still drops the error path on the ninth pass"})}
	rev.record()

	stubTestPhase(env)
	var cleanupCount int
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { cleanupCount++ }).
		Return(nil).Once()

	// If the cap regresses, the repeating stub would feed this loop forever
	// — bound the test so the regression fails fast instead of hanging
	// until the go-test timeout.
	env.SetTestTimeout(30 * time.Second)

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error from the rebuild cap parking the run")
	}
	for _, want := range []string{
		ErrAwaitingMaintainer.Error(),
		"green stage failed to converge",
		"issued rebuild 9",
		"the handler still drops the error path on the ninth pass",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	// Eight rebuild rounds ran (implement + tests + 8 rebuilds); the park
	// precedes a ninth dev round.
	if len(rec.inputs) != 10 {
		t.Errorf("agent ran %d times, want 10 (implement + tests + 8 rebuilds; none after the park)", len(rec.inputs))
	}
	// Phase 1 code review, then per rebuild cycle a test review and its
	// code review, then the ninth (parking) test review: 1 + 2*8 + 1.
	if len(rev.inputs) != 18 {
		t.Errorf("reviewer ran %d times, want 18 (1 code + 8×[test + code] + 1 parking test review)", len(rev.inputs))
	}
	if cleanupCount != 1 {
		t.Errorf("cleanup ran %d times, want 1 (the attempt's work is preserved for continue)", cleanupCount)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowRebuildsUnderCapComplete pins the cap's boundary:
// eight absorbed REBUILD rounds still converge — the ninth verdict is the
// parker, not the eighth, so a healthy loop at the cap's edge is never
// parked.
func TestFeatureDevWorkflowRebuildsUnderCapComplete(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: append(rebuildScript(8, "handler drops the error path"),
		activities.ReviewResult{Approved: true})}
	rev.record()

	stubTestPhase(env)
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
		Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var branch string
	if err := env.GetWorkflowResult(&branch); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if branch != "daedalus/issue-42" {
		t.Errorf("workflow result = %q, want the preserved branch name", branch)
	}
	// implement + tests + 8 rebuilds — no park, no tests-fix round (the
	// resumed review approves the passing suite directly).
	if len(rec.inputs) != 10 {
		t.Errorf("agent ran %d times, want 10 (implement + tests + 8 rebuilds)", len(rec.inputs))
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowRebuildCountIgnoresFixRounds pins the cap's
// monotonicity: a tests-fix round (changes requested, no REBUILD) does not
// reset the rebuild counter — 8 rebuilds, one fix round, and one more
// rebuild is already past the cap and parks. Without the monotonic count
// the loop would keep cycling; the round-count assertions catch that
// without waiting out the test timeout.
func TestFeatureDevWorkflowRebuildCountIgnoresFixRounds(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: append(rebuildScript(8, "rebuild finding"),
		activities.ReviewResult{Approved: false, Comments: "tests still fail"},     // a tests-fix round
		activities.ReviewResult{Rebuild: true, Comments: "rebuild finding again"})} // rebuild 9 parks
	rev.record()

	stubTestPhase(env)
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
		Return(nil).Once()

	env.SetTestTimeout(30 * time.Second)

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error from the rebuild cap parking the run")
	}
	for _, want := range []string{
		ErrAwaitingMaintainer.Error(),
		"issued rebuild 9",
		"rebuild finding again",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	// implement + tests + 8 rebuilds + 1 tests-fix; the park precedes a
	// ninth rebuild round.
	if len(rec.inputs) != 11 {
		t.Errorf("agent ran %d times, want 11 (implement + tests + 8 rebuilds + 1 tests-fix)", len(rec.inputs))
	}
	// Phase 1 code review, 8 full rebuild cycles, the fix round's test
	// review, and the parking test review: 1 + 2*8 + 1 + 1.
	if len(rev.inputs) != 19 {
		t.Errorf("reviewer ran %d times, want 19 (1 code + 8×[test + code] + fix-round test + parking test)", len(rev.inputs))
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowTesterReplyRelayedToReview pins the tester→reviewer
// leg of the rebuild path: the test reviewer's input carries the tester's
// latest reply across both fix rounds, so the reviewer — not the tester —
// decides on a REBUILD; the code review's input never does.
func TestFeatureDevWorkflowTesterReplyRelayedToReview(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env, result: activities.AgentRunResult{Text: "the handler fix is outside my test-only scope"}}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: true}, // phase 1 code review
		{Approved: false, Comments: "name the helper"}, // test review requests changes
		{Approved: true}, // test review approves
	}}
	rev.record()

	stubTestPhase(env)
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(rev.inputs) != 3 {
		t.Fatalf("reviewer ran %d times, want 3 (code + test + test)", len(rev.inputs))
	}
	if rev.inputs[0].AgentReply != "" {
		t.Errorf("code review AgentReply = %q, want empty outside the test phase", rev.inputs[0].AgentReply)
	}
	for i := 1; i < len(rev.inputs); i++ {
		if rev.inputs[i].AgentReply != "the handler fix is outside my test-only scope" {
			t.Errorf("test review %d AgentReply = %q, want the tester's latest reply relayed", i, rev.inputs[i].AgentReply)
		}
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowParkErrorCarriesFullComments pins the untruncated
// park error: reviewer comments reach the workflow failure message whole,
// however long — the maintainer reads the halt reason in full from the
// failure, so no excerpt stands in for the text and no truncation marker
// may appear.
func TestFeatureDevWorkflowParkErrorCarriesFullComments(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	// Well past the byte cap the park message once enforced, so this fails
	// exclusively if truncation sneaks back in.
	comments := strings.Repeat("the layout must stay columnar. ", 64)
	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{NeedsMaintainer: true, Comments: comments},
	}}
	rev.record()

	stubTestPhase(env)
	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error from a NEEDS_MAINTAINER verdict")
	}
	if !strings.Contains(err.Error(), comments) {
		t.Errorf("park error should carry the reviewer comments in full, got %q", err)
	}
	const marker = "[... review comments truncated ...]"
	if strings.Contains(err.Error(), marker) {
		t.Errorf("park error should not truncate the reviewer comments, got %q", err)
	}
	env.AssertExpectations(t)
}

// boolPtr is a literal for the PipelineInput pointer toggles.
func boolPtr(b bool) *bool { return &b }

// TestSuiteQueueRouting pins the suite-execution queue selection: a run
// sharing its suites (nil input — a pre-field replay — or explicit true)
// schedules them on the fleet-shared ReservedTestTaskQueue, while an
// opted-out run derives "<task_queue>-test" from its own task queue. The
// queue rides the activity's scheduling options, so it is read back from
// the executing activity's info rather than any input struct.
func TestSuiteQueueRouting(t *testing.T) {
	for _, c := range []struct {
		name   string
		shared *bool
		want   string
	}{
		{"nil input, pre-field replay", nil, config.ReservedTestTaskQueue},
		{"shared", boolPtr(true), config.ReservedTestTaskQueue},
		{"opted out", boolPtr(false), config.TestQueueFor("q9")},
	} {
		t.Run(c.name, func(t *testing.T) {
			env := newTestEnv(t)
			env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
				Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil).Once()

			rec := &agentRecorder{env: env}
			rec.record()
			rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
			rev.record()

			// Discovery stays on the workflow's queue; only the suite
			// routing is under test, and its stub reads the queue each
			// execution was actually scheduled on — the queue rides the
			// activity's scheduling options, not any input struct.
			env.OnActivity(activities.ResolveTestCommandActivity, mock.Anything, mock.Anything, mock.Anything).
				Return("go test ./...", nil)
			var queues []string
			env.OnActivity(activities.RunTestSuiteActivity, mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) {
					if ctx, ok := args.Get(0).(context.Context); ok {
						queues = append(queues, activity.GetInfo(ctx).TaskQueue)
					}
				}).
				Return(activities.TestResult{Passed: true, Logs: "ok"}, nil)

			scope := &scopeRecorder{env: env}
			scope.record()
			env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
				Return("daedalus/issue-42-1", nil).Once()
			env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
				Return(nil).Once()

			in := baseInput()
			// A non-default task queue, so the derived suite queue provably
			// comes from the run's own queue rather than any constant.
			in.TaskQueue = "q9"
			in.Flow = "test-only"
			in.AllowedPaths = activities.TestPathPatterns
			in.SharedTestQueue = c.shared
			env.ExecuteWorkflow(TestOnlyWorkflow, in)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow error: %v", err)
			}

			// Both suite executions — the preflight baseline and the loop
			// round — must ride the expected queue; the count check keeps
			// the queue assertion from passing vacuously.
			if len(queues) != 2 {
				t.Fatalf("suite ran %d times, want 2 (preflight baseline plus loop round)", len(queues))
			}
			for i, q := range queues {
				if q != c.want {
					t.Errorf("suite run %d executed on queue %q, want %q", i, q, c.want)
				}
			}
		})
	}
}

// registerCompleteGreen re-registers fn the way runWorker registers every
// flow: wrapped in CompleteGreen, under the unwrapped flow's own type name
// (WorkflowTypeName) — the registration contract the parked-green behavior
// lives or dies by.
// TestCompleteGreenParkedRunCompletesGreen pins the registration contract
// end to end: with the flow registered the way the worker registers it
// (CompleteGreen under WorkflowTypeName), a NEEDS_MAINTAINER park does not
// fail the workflow — it completes successfully with a result carrying the
// parked marker plus the full reason (halting round's comments included),
// while the deferred cleanup still preserves the attempt's work.
func TestCompleteGreenParkedRunCompletesGreen(t *testing.T) {
	t.Setenv(config.BugDirEnv, "")
	env, name := newCompleteGreenEnv(t)

	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: false, Comments: "rename foo to bar"},
		{NeedsMaintainer: true, Comments: "the fix needs a secret only the maintainer can provide"},
	}}
	rev.record()

	stubTestPhase(env)
	var cleanups int
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { cleanups++ }).
		Return(nil).Once()

	env.ExecuteWorkflow(name, baseInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("parked run failed the workflow: %v", err)
	}
	var result string
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if !IsParkedResult(result) {
		t.Fatalf("result = %q, want the parked marker prefix %q", result, ParkedResultPrefix)
	}
	for _, want := range []string{
		ErrAwaitingMaintainer.Error(),
		"code review halted the run",
		"the fix needs a secret only the maintainer can provide",
	} {
		if !strings.Contains(result, want) {
			t.Errorf("parked result %q should contain %q", result, want)
		}
	}
	if strings.Contains(result, "rename foo to bar") {
		t.Errorf("parked result %q should carry the halting round's comments, not an earlier round's", result)
	}
	if cleanups != 1 {
		t.Errorf("cleanup ran %d times, want 1 (the attempt's work is preserved either way)", cleanups)
	}
	env.AssertExpectations(t)
}

// TestCompleteGreenFailureStillFails pins the wrapper's restraint: only an
// ErrAwaitingMaintainer park completes green — every other error fails the
// workflow exactly as the unwrapped registration would, so the failure
// signal is not blanket-suppressed.
func TestCompleteGreenFailureStillFails(t *testing.T) {
	t.Setenv(config.BugDirEnv, "")
	env, name := newCompleteGreenEnv(t)

	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{}, errWorktreeStub).Once()

	env.ExecuteWorkflow(name, baseInput())

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("a genuine failure completed green; want the workflow error")
	}
	if strings.Contains(err.Error(), ParkedResultPrefix) {
		t.Errorf("failure error %q masqueraded as a park", err)
	}
	env.AssertExpectations(t)
}

// TestWorkflowTypeName pins the registration/start agreement: the derived
// name is the flow's bare function name — no package path, no closure or
// method suffix — so a start and a wrapped registration name the same
// workflow type.
func TestWorkflowTypeName(t *testing.T) {
	for name, fn := range map[string]func(workflow.Context, PipelineInput) (string, error){
		"FeatureDevWorkflow":  FeatureDevWorkflow,
		"TestOnlyWorkflow":    TestOnlyWorkflow,
		"InvestigateWorkflow": InvestigateWorkflow,
		"RefactorWorkflow":    RefactorWorkflow,
		"BugFixWorkflow":      BugFixWorkflow,
		"DevOnlyWorkflow":     DevOnlyWorkflow,
	} {
		got := WorkflowTypeName(fn)
		if got != name {
			t.Errorf("WorkflowTypeName = %q, want %q", got, name)
		}
	}
}

// TestParkedResultMarkers pins the park marker the CLI gates on
// (awaitPipeline, wipe): the prefix marks exactly the results CompleteGreen
// produces — any other completion payload, a preserved branch name above
// all — must never read as a park.
func TestParkedResultMarkers(t *testing.T) {
	reason := ErrAwaitingMaintainer.Error() + ": code review halted the run"
	result := ParkedResult(reason)
	if !IsParkedResult(result) {
		t.Errorf("IsParkedResult(%q) = false, want true", result)
	}
	if got := strings.TrimPrefix(result, ParkedResultPrefix); got != reason {
		t.Errorf("parked result stripped = %q, want the full reason %q", got, reason)
	}
	for _, notPark := range []string{"", "daedalus/issue-42-1", "feat/dev-only-issue-42-abc1234", ParkedResultPrefix[:len(ParkedResultPrefix)-1]} {
		if IsParkedResult(notPark) {
			t.Errorf("IsParkedResult(%q) = true, want false", notPark)
		}
	}
}

// errOverflowStub mirrors how a static prompt-overflow failure surfaces at
// the activity boundary: message text only (claude's result JSON prints the
// marker; the OpenAI-compatible wire its two siblings).
var errOverflowStub = errors.New("run jailed: Prompt is too long: 250000 tokens > 200000 limit")

// TestFeatureDevWorkflowReviewerPromptOverflowNotRetried pins the
// overflow backstop: a reviewer round whose prompt cannot fit the serving
// model's context is terminal — the run fails naming the static size, and
// neither the fresh-session fallback nor the quota heartbeat is spent on a
// prompt every retry re-sends at the same size (the stub would approve any
// wrongful retry, so the single-call count is the pin).
func TestFeatureDevWorkflowReviewerPromptOverflowNotRetried(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()

	rev := &reviewerRecorder{env: env, script: []reviewStep{{err: errOverflowStub}},
		stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	stubTestPhase(env)
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(FeatureDevWorkflow, baseInput())

	err := env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), "cannot fit the serving model's context window") {
		t.Fatalf("want the terminal prompt-overflow failure, got %v", err)
	}
	if len(rev.inputs) != 1 {
		t.Fatalf("reviewer ran %d times, want 1 (the size is static, so no retry)", len(rev.inputs))
	}
	env.AssertExpectations(t)
}

// TestIsPromptOverflow pins the classification: each provider's overflow
// marker matches (wrapped or bare), while the recoverable failure shapes —
// quota exhaustion, a killed round, an ordinary error, no error — do not.
func TestIsPromptOverflow(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"claude result marker", errors.New(`run jailed: result: {"subtype":"error_during_execution"} Prompt is too long: 250000 tokens > 200000 limit`), true},
		{"openai wire marker", errors.New(`error: {"error":{"code":"context_length_exceeded"}}`), true},
		{"anthropic wire marker", errors.New("invalid_request_error: prompt exceeds the maximum context length"), true},
		{"wrapped", fmt.Errorf("review %q: %w", "the implementation", errors.New("Prompt is too long")), true},
		{"quota exhaustion", errQuotaStub, false},
		{"killed round", errors.New("agent killed by SIGKILL"), false},
		{"ordinary failure", errReviewStub, false},
		{"no error", nil, false},
	} {
		if got := isPromptOverflow(c.err); got != c.want {
			t.Errorf("isPromptOverflow(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}
