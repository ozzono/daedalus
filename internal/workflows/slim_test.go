package workflows

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"

	"github.com/ozzono/daedalus/internal/activities"
)

// twoSubtaskPlan is a well-formed planner reply: two atomic sub-tasks, the
// second building on the first.
const twoSubtaskPlan = `[{"id":1,"type":"implement","target_files":["adder.go"],"description":"add the adder","acceptance_criteria":["adder.go defines Add","Add(2,2) returns 4"]},{"id":2,"type":"implement","target_files":["mul.go"],"description":"multiply the adder","acceptance_criteria":["mul.go defines Mul","Mul(3,2) returns 6"]}]`

// stubSlimRun wires the shared happy-path defaults for a slim run: a
// worktree, green-discovered suite, finalize, and cleanup. Tests script the
// recorders they care about.
func stubSlimRun(t *testing.T) (*testsuite.TestWorkflowEnvironment, *agentRecorder, *reviewerRecorder, *suiteRecorder) {
	t.Helper()
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	agents := &agentRecorder{env: env}
	agents.record()
	reviews := &reviewerRecorder{env: env}
	reviews.record()
	_, suites := stubTestPhase(env)
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	return env, agents, reviews, suites
}

// TestParseSlimPlan pins the planner reply's parse contract: the outermost
// JSON array of an otherwise chatty reply is extracted and validated — an
// empty array, a reply without an array, malformed JSON, and a sub-task
// missing its description or acceptance criteria are all rejected, because
// the atomic loop cannot run without them.
func TestParseSlimPlan(t *testing.T) {
	valid := twoSubtaskPlan
	for _, c := range []struct {
		name    string
		text    string
		wantN   int
		wantErr string
	}{{
		name:  "raw array",
		text:  valid,
		wantN: 2,
	}, {
		name:  "array wrapped in prose",
		text:  "Here is the plan:\n" + valid + "\nLet me know if this works!",
		wantN: 2,
	}, {
		name:    "no array",
		text:    "I cannot plan this task, sorry.",
		wantErr: "no JSON array found",
	}, {
		name:    "empty array",
		text:    "[]",
		wantErr: "empty",
	}, {
		name:    "malformed JSON",
		text:    `[{"id": 1, "description": }]`,
		wantErr: "invalid character",
	}, {
		name:    "subtask without description",
		text:    `[{"id":1,"acceptance_criteria":["c1"]}]`,
		wantErr: "carries no description",
	}, {
		name:    "subtask without acceptance criteria",
		text:    `[{"id":1,"description":"do it"}]`,
		wantErr: "carries no acceptance criteria",
	}} {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseSlimPlan(c.text)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("parseSlimPlan(%q) err = %v, want it to contain %q", c.text, err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSlimPlan(%q): %v", c.text, err)
			}
			if len(got) != c.wantN {
				t.Fatalf("parsed %d subtasks, want %d", len(got), c.wantN)
			}
		})
	}
	if got, err := parseSlimPlan(valid); err != nil || got[1].Description != "multiply the adder" ||
		len(got[1].AcceptanceCriteria) != 2 || got[1].TargetFiles[0] != "mul.go" {
		t.Errorf("parseSlimPlan fields = %+v (err %v), want the wire schema's named fields carried through", got, err)
	}
}

// TestSlimWorkflowHappyPath pins the slim flow's shape end to end: the
// planner round opens the dev conversation, each sub-task implements in the
// chained worker conversation while every review is a fresh session judged
// against exactly that sub-task's acceptance criteria, a red suite blocks
// approval even under an approving review (its logs ride the fix prompt,
// green logs stay out of the review prompt), and an accepted queue
// finalizes.
func TestSlimWorkflowHappyPath(t *testing.T) {
	env, agents, reviews, suites := stubSlimRun(t)

	agents.script = []agentStep{
		{result: activities.AgentRunResult{Text: twoSubtaskPlan, SessionID: "dev-1"}},
		{result: activities.AgentRunResult{Text: "step one done", SessionID: "dev-2"}},
		{result: activities.AgentRunResult{Text: "fixed the failure", SessionID: "dev-3"}},
		{result: activities.AgentRunResult{Text: "fixed the shape", SessionID: "dev-4"}},
		{result: activities.AgentRunResult{Text: "step two done", SessionID: "dev-5"}},
	}
	// Round 1 approves while the suite is red (and must still loop);
	// round 2 rejects a green suite (comments-only fix prompt); round 3
	// and sub-task 2's review approve.
	reviews.script = []reviewStep{
		{result: activities.ReviewResult{Approved: true, Comments: "verdict yes despite the red suite"}},
		{result: activities.ReviewResult{Approved: false, Comments: "the shape is wrong"}},
		{result: activities.ReviewResult{Approved: true}},
	}
	// Sub-task 2's review repeats the last verdict.
	reviews.stub = []activities.ReviewResult{{Approved: true}}
	suites.script = []suiteStep{
		// The preflight baseline runs green; sub-task 1 then needs two
		// rounds (red, green, green), sub-task 2 one (green).
		{result: activities.TestResult{Passed: true, Logs: "ok"}},
		{result: activities.TestResult{Passed: false, Logs: "--- FAIL: TestAdd"}},
		{result: activities.TestResult{Passed: true, Logs: "ok"}},
		{result: activities.TestResult{Passed: true, Logs: "ok"}},
		{result: activities.TestResult{Passed: true, Logs: "ok"}},
	}
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()

	branch, err := runSlim(t, env)
	if err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if branch != "daedalus/issue-42-1" {
		t.Errorf("branch = %q, want the finalize deliverable", branch)
	}

	// Worker rounds: plan, step 1, fix 1, fix 2, step 2 — one chained dev
	// conversation across the whole run.
	if len(agents.inputs) != 5 {
		t.Fatalf("agent ran %d times, want 5 (plan, step, fix, fix, step)", len(agents.inputs))
	}
	if got := agents.inputs[0].SessionID; got != "" {
		t.Errorf("planner SessionID = %q, want empty (the dev conversation starts cold)", got)
	}
	if p := agents.inputs[0].Prompt; !strings.Contains(p, "implement the feature") || !strings.Contains(p, "PLANNER") {
		t.Errorf("planner prompt should carry the task through the slim plan template, got:\n%s", p)
	}
	for i, wantSession := range []string{"dev-1", "dev-2", "dev-3", "dev-4"} {
		if got := agents.inputs[i+1].SessionID; got != wantSession {
			t.Errorf("worker round %d SessionID = %q, want %q (the conversation chains)", i+1, got, wantSession)
		}
	}
	if p := agents.inputs[1].Prompt; !strings.Contains(p, "sub-tasks 1 of 2") || !strings.Contains(p, "add the adder") ||
		!strings.Contains(p, "Add(2,2) returns 4") {
		t.Errorf("step prompt should carry sub-task 1 of 2 with its own description and criteria, got:\n%s", p)
	}
	if p := agents.inputs[2].Prompt; !strings.Contains(p, "--- FAIL: TestAdd") ||
		strings.Contains(p, "Reviewer comments") {
		t.Errorf("fix prompt for a red-suite round should carry the failing logs and no reviewer comments (the round approved), got:\n%s", p)
	}
	if p := agents.inputs[3].Prompt; !strings.Contains(p, "the shape is wrong") || strings.Contains(p, "--- FAIL: TestAdd") {
		t.Errorf("fix prompt for a green-suite round should carry only the reviewer comments, got:\n%s", p)
	}
	if p := agents.inputs[4].Prompt; !strings.Contains(p, "sub-tasks 2 of 2") || !strings.Contains(p, "multiply the adder") {
		t.Errorf("step 2 prompt should carry sub-task 2 of 2, got:\n%s", p)
	}

	// Reviews: one fresh session per round, judged against exactly the
	// current sub-task's criteria; red logs travel, green logs stay out.
	if len(reviews.inputs) != 4 {
		t.Fatalf("reviewer ran %d times, want 4 (three rounds for sub-task 1, one for sub-task 2)", len(reviews.inputs))
	}
	for i, in := range reviews.inputs {
		if !in.FreshReview {
			t.Errorf("review %d FreshReview = false, want true (no review resumes another's conversation)", i)
		}
		if in.SessionID != "" {
			t.Errorf("review %d SessionID = %q, want empty (fresh review mode never resumes)", i, in.SessionID)
		}
		if in.Role != activities.RoleDevReview {
			t.Errorf("review %d Role = %v, want RoleDevReview", i, in.Role)
		}
	}
	for i, want := range []string{
		"subtask 1 (add the adder)", "subtask 1 (add the adder)",
		"subtask 1 (add the adder)", "subtask 2 (multiply the adder)",
	} {
		if got := reviews.inputs[i].Focus; got != want {
			t.Errorf("review %d Focus = %q, want %q", i, got, want)
		}
	}
	for i, want := range [][]string{
		{"adder.go defines Add", "Add(2,2) returns 4"},
		{"adder.go defines Add", "Add(2,2) returns 4"},
		{"adder.go defines Add", "Add(2,2) returns 4"},
		{"mul.go defines Mul", "Mul(3,2) returns 6"},
	} {
		got := reviews.inputs[i].AcceptanceCriteria
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Errorf("review %d criteria = %v, want %v (the reviewer judges exactly this sub-task)", i, got, want)
		}
	}
	if logs := reviews.inputs[0].TestLogs; !strings.Contains(logs, "--- FAIL: TestAdd") {
		t.Errorf("red round's review TestLogs = %q, want the failing suite output relayed", logs)
	}
	for i := 1; i < len(reviews.inputs); i++ {
		if logs := reviews.inputs[i].TestLogs; logs != "" {
			t.Errorf("green round's review %d TestLogs = %q, want empty (the reviewer runs the suite itself)", i, logs)
		}
	}

	if len(suites.runs) != 5 {
		t.Fatalf("suite ran %d times, want 5 (preflight + one per sub-task round)", len(suites.runs))
	}
	if got := suites.runs[0].Command; got != "go test ./..." {
		t.Errorf("suite command = %q, want the discovered command re-run per round", got)
	}
	env.AssertExpectations(t)
}

// TestSlimWorkflowPlannerRetry pins the planner's one strict re-ask: a
// malformed first reply is re-asked inside the planner's own conversation
// (the model sees and corrects its previous reply); a parseable second
// reply proceeds, an unparseable one parks the run.
func TestSlimWorkflowPlannerRetry(t *testing.T) {
	t.Run("second reply parses", func(t *testing.T) {
		env, agents, reviews, _ := stubSlimRun(t)
		agents.script = []agentStep{
			{result: activities.AgentRunResult{Text: "I would start by exploring the repository."}},
			{result: activities.AgentRunResult{Text: `[{"id":1,"type":"implement","target_files":["a.go"],"description":"do it","acceptance_criteria":["a.go exists"]}]`}},
		}
		reviews.stub = []activities.ReviewResult{{Approved: true}}
		env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
			Return("daedalus/issue-42-1", nil).Once()
		if _, err := runSlim(t, env); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if len(agents.inputs) != 3 {
			t.Fatalf("agent ran %d times, want 3 (plan, re-ask, step)", len(agents.inputs))
		}
		retry := agents.inputs[1].Prompt
		if !strings.Contains(retry, "could not be parsed as the required JSON array") ||
			!strings.Contains(retry, "implement the feature") {
			t.Errorf("re-ask prompt should carry the parse failure and the original task, got:\n%s", retry)
		}
	})
	t.Run("second reply parks", func(t *testing.T) {
		env, agents, reviews, suites := stubSlimRun(t)
		garbage := activities.AgentRunResult{Text: "still just chatting."}
		agents.script = []agentStep{{result: garbage}, {result: garbage}}
		err := runSlimErr(t, env)
		for _, want := range []string{ErrAwaitingMaintainer.Error(), "planner could not produce a parseable subtask queue"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("park error %q should contain %q", err, want)
			}
		}
		if len(agents.inputs) != 2 {
			t.Errorf("agent ran %d times, want 2 (plan + one re-ask)", len(agents.inputs))
		}
		if len(reviews.inputs) != 0 {
			t.Errorf("reviewer ran %d times, want 0 (nothing to review without a plan)", len(reviews.inputs))
		}
		if len(suites.runs) != 1 {
			t.Errorf("suite ran %d times, want 1 (preflight only)", len(suites.runs))
		}
	})
}

// TestSlimWorkflowNoSuiteParks pins the gate: slim's every-round ground
// truth is the native suite, so a repository with no discoverable suite
// parks before any agent budget is spent.
func TestSlimWorkflowNoSuiteParks(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	var agentCalls int
	env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { agentCalls++ }).
		Return(activities.AgentRunResult{}, nil).Maybe()
	env.OnActivity(activities.ResolveTestCommandActivity, mock.Anything, mock.Anything, mock.Anything).
		Return("", activities.ErrNoSuite)
	var cleanupCount int
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { cleanupCount++ }).
		Return(nil).Once()

	in := baseInput()
	in.Flow = "slim"
	env.ExecuteWorkflow(SlimWorkflow, in)

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want a park error for a suite-less repository")
	}
	for _, want := range []string{ErrAwaitingMaintainer.Error(), "slim requires a discoverable native test suite"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	if agentCalls != 0 {
		t.Errorf("agent ran %d times, want 0 (park before any agent budget is spent)", agentCalls)
	}
	if cleanupCount != 1 {
		t.Errorf("cleanup ran %d times, want 1 (the untouched worktree is preserved for continue)", cleanupCount)
	}
	env.AssertExpectations(t)
}

// TestSlimWorkflowReviewHaltsParks pins the maintainer-halt path: a review
// that declares the sub-task impossible as stated parks the run with the
// sub-task's identity and the reviewer's reason, instead of looping.
func TestSlimWorkflowReviewHaltsParks(t *testing.T) {
	env, agents, reviews, _ := stubSlimRun(t)
	agents.script = []agentStep{
		{result: activities.AgentRunResult{Text: `[{"id":1,"type":"implement","target_files":["a.go"],"description":"do it","acceptance_criteria":["a.go exists"]}]`}},
	}
	reviews.script = []reviewStep{{result: activities.ReviewResult{NeedsMaintainer: true, Comments: "the criteria contradict each other"}}}

	err := runSlimErr(t, env)
	for _, want := range []string{ErrAwaitingMaintainer.Error(), "slim subtask 1 review halted the run", "the criteria contradict each other"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	if len(agents.inputs) != 2 {
		t.Errorf("agent ran %d times, want 2 (plan + step, no fix loop after the halt)", len(agents.inputs))
	}
	env.AssertExpectations(t)
}

// TestSlimWorkflowSubtaskRoundCapParks pins the per-sub-task convergence
// cap: a sub-task that cannot reach a green suite plus an approving review
// within maxSlimStepRounds ground-truth rounds parks the run — the suite
// and the reviewer both stay live through every round, and the counter
// counts rounds, not agent calls.
func TestSlimWorkflowSubtaskRoundCapParks(t *testing.T) {
	env, agents, reviews, suites := stubSlimRun(t)
	agents.script = []agentStep{
		{result: activities.AgentRunResult{Text: `[{"id":7,"type":"fix","target_files":["a.go"],"description":"the stubborn one","acceptance_criteria":["a.go compiles"]}]`}},
	}
	// Every round after the green preflight baseline: red suite, approving
	// review — approval must never land while the suite is red, until the
	// cap parks the run.
	reviews.stub = []activities.ReviewResult{{Approved: true}}
	script := make([]suiteStep, maxSlimStepRounds+1)
	script[0] = suiteStep{result: activities.TestResult{Passed: true, Logs: "ok"}}
	for i := 1; i < len(script); i++ {
		script[i] = suiteStep{result: activities.TestResult{Passed: false, Logs: "--- FAIL: round"}}
	}
	suites.script = script

	err := runSlimErr(t, env)
	for _, want := range []string{ErrAwaitingMaintainer.Error(), "slim subtask 7 failed to converge", "8 ground-truth rounds", "the stubborn one"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	if len(suites.runs) != maxSlimStepRounds+1 {
		t.Errorf("suite ran %d times, want %d (green preflight + the capped rounds)", len(suites.runs), maxSlimStepRounds+1)
	}
	if len(reviews.inputs) != maxSlimStepRounds {
		t.Errorf("reviewer ran %d times, want %d", len(reviews.inputs), maxSlimStepRounds)
	}
	// plan + step + one fix per capped round.
	if len(agents.inputs) != 2+maxSlimStepRounds {
		t.Errorf("agent ran %d times, want %d (plan, step, one fix per round)", len(agents.inputs), 2+maxSlimStepRounds)
	}
	env.AssertExpectations(t)
}

// runSlim executes SlimWorkflow on the env with the base input's flow set
// and returns the finalized branch.
func runSlim(t *testing.T, env *testsuite.TestWorkflowEnvironment) (string, error) {
	t.Helper()
	in := baseInput()
	in.Flow = "slim"
	var branch string
	env.ExecuteWorkflow(SlimWorkflow, in)
	if err := env.GetWorkflowResult(&branch); err != nil {
		return "", err
	}
	return branch, env.GetWorkflowError()
}

// runSlimErr executes SlimWorkflow and returns the workflow error (for
// park-path tests).
func runSlimErr(t *testing.T, env *testsuite.TestWorkflowEnvironment) error {
	t.Helper()
	in := baseInput()
	in.Flow = "slim"
	env.ExecuteWorkflow(SlimWorkflow, in)
	return env.GetWorkflowError()
}
