package workflows

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/template"
)

// TestFlowScope pins the per-issue scope segment every flow-derived name
// carries: feature-dev (and pre-field inputs) keep the legacy unscoped
// names so existing history, continue, and recapture stay untouched; every
// other flow is namespaced by its name.
func TestFlowScope(t *testing.T) {
	for _, c := range []struct{ flow, want string }{
		{"", ""},
		{"feature-dev", ""},
		{"investigate", "investigate"},
		{"bug-fix", "bug-fix"},
	} {
		if got := FlowScope(c.flow); got != c.want {
			t.Errorf("FlowScope(%q) = %q, want %q", c.flow, got, c.want)
		}
	}
}

// scopeRecorder captures every VerifyWriteScopeActivity call — the run's
// structural write-scope gate — and answers with its preset result.
type scopeRecorder struct {
	env    *testsuite.TestWorkflowEnvironment
	res    activities.ScopeResult
	inputs []activities.ScopeCheckInput
}

func (r *scopeRecorder) record() {
	r.env.OnActivity(activities.VerifyWriteScopeActivity, mock.Anything, mock.Anything).Maybe().
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.ScopeCheckInput); ok {
					r.inputs = append(r.inputs, in)
				}
			}
		}).
		Return(r.res, nil)
}

// reproGateRecorder captures every ReproFirstGateActivity call and plays
// its script in order before falling back to a reproduced verdict.
type reproGateRecorder struct {
	env    *testsuite.TestWorkflowEnvironment
	inputs []activities.ReproGateInput
	script []activities.ReproResult
}

func (r *reproGateRecorder) record() {
	r.env.OnActivity(activities.ReproFirstGateActivity, mock.Anything, mock.Anything).Maybe().
		Run(func(args mock.Arguments) {
			for _, a := range args {
				if in, ok := a.(activities.ReproGateInput); ok {
					r.inputs = append(r.inputs, in)
				}
			}
		}).
		Return(func(ctx context.Context, in activities.ReproGateInput) (activities.ReproResult, error) {
			if len(r.script) > 0 {
				step := r.script[0]
				r.script = r.script[1:]
				return step, nil
			}
			return activities.ReproResult{Reproduced: true, Logs: "gate ok"}, nil
		})
}

// TestInvestigateWorkflowHappyPath pins the docs-only flow's shape: the
// investigate prompt opens the run, a single approving docs review leads
// through the write-scope gate to finalize, and the flow-scoped input
// namespaces every derived name (branch, worktree path) under the flow.
func TestInvestigateWorkflowHappyPath(t *testing.T) {
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
		Return(activities.WorktreeOutput{WorktreePath: "/wt/investigate-issue-42"}, nil).Once()

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	scope := &scopeRecorder{env: env}
	scope.record()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/investigate-issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	in := baseInput()
	in.Flow = "investigate"
	in.AllowedPaths = []string{"*.md", "docs/"}
	env.ExecuteWorkflow(InvestigateWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if created.Flow != "investigate" {
		t.Errorf("WorktreeInput.Flow = %q, want investigate", created.Flow)
	}
	if !strings.HasPrefix(created.BranchName, "feat/investigate-issue-42-") {
		t.Errorf("BranchName = %q, want the flow-scoped in-flight branch", created.BranchName)
	}
	if len(rec.inputs) != 1 {
		t.Fatalf("agent ran %d times, want 1 (investigate)", len(rec.inputs))
	}
	want, err := template.Investigate("implement the feature")
	if err != nil {
		t.Fatalf("build expected investigate prompt: %v", err)
	}
	if rec.inputs[0].Prompt != want {
		t.Errorf("investigate prompt = %q, want %q", rec.inputs[0].Prompt, want)
	}
	if got := rec.inputs[0].Role; got != activities.RoleDev {
		t.Errorf("investigate round Role = %q, want %q", got, activities.RoleDev)
	}
	if len(rev.inputs) != 1 {
		t.Fatalf("reviewer ran %d times, want 1 (docs review)", len(rev.inputs))
	}
	ri := rev.inputs[0]
	if ri.Focus != "the documentation" || ri.TestLogs != "" || ri.TestsInScope || ri.ReproInScope || ri.AgentReply != "" {
		t.Errorf("docs review input = %+v, want the documentation focus with no test machinery", ri)
	}
	if got := ri.Role; got != activities.RoleDevReview {
		t.Errorf("docs review Role = %q, want %q", got, activities.RoleDevReview)
	}
	if len(scope.inputs) != 1 {
		t.Fatalf("write-scope gate ran %d times, want 1", len(scope.inputs))
	}
	if !reflect.DeepEqual(scope.inputs[0].Allowed, in.AllowedPaths) || len(scope.inputs[0].Frozen) != 0 {
		t.Errorf("scope check input = %+v, want the flow's allowed paths", scope.inputs[0])
	}
	var branch string
	if err := env.GetWorkflowResult(&branch); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if branch != "daedalus/investigate-issue-42-1" {
		t.Errorf("workflow result = %q, want the preserved branch name", branch)
	}
	env.AssertExpectations(t)
}

// TestInvestigateWorkflowDocsFixLoop pins the review loop: a docs review
// requesting changes feeds its comments back through the investigate-fix
// prompt inside the same dev session, docs-only fence intact.
func TestInvestigateWorkflowDocsFixLoop(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/investigate-issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: false, Comments: "cite the files backing each claim"},
		{Approved: true},
	}}
	rev.record()

	scope := &scopeRecorder{env: env}
	scope.record()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/investigate-issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	in := baseInput()
	in.Flow = "investigate"
	in.AllowedPaths = []string{"*.md", "docs/"}
	env.ExecuteWorkflow(InvestigateWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(rec.inputs) != 2 {
		t.Fatalf("agent ran %d times, want 2 (investigate, docs-fix)", len(rec.inputs))
	}
	fixPrompt := rec.inputs[1].Prompt
	if !strings.Contains(fixPrompt, "Documentation review feedback") ||
		!strings.Contains(fixPrompt, "cite the files backing each claim") {
		t.Errorf("docs-fix prompt %q should carry the review comments", fixPrompt)
	}
	if !strings.Contains(fixPrompt, "production code and tests stay untouched") {
		t.Errorf("docs-fix prompt %q should keep the docs-only fence", fixPrompt)
	}
	if got := rec.inputs[1].Role; got != activities.RoleDev {
		t.Errorf("docs-fix round Role = %q, want %q (one dev conversation)", got, activities.RoleDev)
	}
	env.AssertExpectations(t)
}

// TestInvestigateWorkflowScopeViolationParks pins the structural half of
// docs-only enforcement: an approved diff that left the docs scope parks
// the run for the maintainer — naming the offending paths — instead of
// finalizing or silently stripping files.
func TestInvestigateWorkflowScopeViolationParks(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/investigate-issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	scope := &scopeRecorder{env: env, res: activities.ScopeResult{Violations: []string{
		"cmd/main.go: outside this flow's allowed paths",
	}}}
	scope.record()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	in := baseInput()
	in.Flow = "investigate"
	in.AllowedPaths = []string{"*.md", "docs/"}
	env.ExecuteWorkflow(InvestigateWorkflow, in)

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error from a write-scope violation")
	}
	for _, want := range []string{
		ErrAwaitingMaintainer.Error(),
		"write-scope violation",
		"cmd/main.go: outside this flow's allowed paths",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	if len(rec.inputs) != 1 {
		t.Errorf("agent ran %d times, want 1 (no fix round after a structural park)", len(rec.inputs))
	}
	env.AssertExpectations(t)
}

// TestTestOnlyWorkflowSuiteLoop pins the test-only flow's shape: the run
// opens directly in the test loop (no implementation phase), suite
// executions record coverage, the reviewer sees each round's coverage with
// the delta against the previous one, the tester's reply is relayed, the
// suite runs with the cover flag, and approval leads through the
// write-scope gate (tests allowed, everything else frozen by omission) to
// finalize.
func TestTestOnlyWorkflowSuiteLoop(t *testing.T) {
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
		Return(activities.WorktreeOutput{WorktreePath: "/wt/test-only-issue-42"}, nil).Once()

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: false, Comments: "cover the error path"},
		{Approved: true},
	}}
	rev.record()

	_, suiteRec := stubTestPhase(env)
	// The first (green) entry is the preflight gate's baseline; the loop's
	// two suite rounds follow it.
	suiteRec.script = []suiteStep{
		{result: activities.TestResult{Passed: true, Logs: "ok"}},
		{result: activities.TestResult{Passed: true, Logs: "ok", Coverage: "40.0"}},
		{result: activities.TestResult{Passed: true, Logs: "ok", Coverage: "50.0"}},
	}

	scope := &scopeRecorder{env: env}
	scope.record()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/test-only-issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	in := baseInput()
	in.Flow = "test-only"
	in.AllowedPaths = activities.TestPathPatterns
	env.ExecuteWorkflow(TestOnlyWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if created.Flow != "test-only" || !strings.HasPrefix(created.BranchName, "feat/test-only-issue-42-") {
		t.Errorf("WorktreeInput = %+v, want the flow-scoped names", created)
	}
	if len(rec.inputs) != 2 {
		t.Fatalf("agent ran %d times, want 2 (tests, tests-fix — no implementation round)", len(rec.inputs))
	}
	want, err := template.Tests()
	if err != nil {
		t.Fatalf("build expected tests prompt: %v", err)
	}
	if rec.inputs[0].Prompt != want {
		t.Errorf("opening prompt = %q, want the tests prompt (the flow starts in the test loop)", rec.inputs[0].Prompt)
	}
	if got := rec.inputs[0].Role; got != activities.RoleTest {
		t.Errorf("tests round Role = %q, want %q", got, activities.RoleTest)
	}
	fixPrompt := rec.inputs[1].Prompt
	if !strings.Contains(fixPrompt, "Test review feedback") ||
		!strings.Contains(fixPrompt, "cover the error path") {
		t.Errorf("test-fix prompt %q should carry the review comments", fixPrompt)
	}
	if got := rec.inputs[1].Role; got != activities.RoleTest {
		t.Errorf("test-fix round Role = %q, want %q", got, activities.RoleTest)
	}
	for i, run := range suiteRec.runs {
		if !run.Cover {
			t.Errorf("suite run %d Cover = false, want the test-only flow to record coverage", i)
		}
	}
	if len(rev.inputs) != 2 {
		t.Fatalf("reviewer ran %d times, want 2", len(rev.inputs))
	}
	if rev.inputs[0].TestLogs != "GO STATEMENT COVERAGE THIS ROUND: 40.0%\n\nok" {
		t.Errorf("first test review logs = %q, want the coverage note ahead of the suite output", rev.inputs[0].TestLogs)
	}
	if !strings.Contains(rev.inputs[1].TestLogs,
		"GO STATEMENT COVERAGE THIS ROUND: 50.0% (previous round: 40.0%)") {
		t.Errorf("second test review logs = %q, want this round's coverage with the delta", rev.inputs[1].TestLogs)
	}
	for i, ri := range rev.inputs {
		if ri.Focus != "the test suite" || !ri.TestsInScope || ri.ReproInScope || ri.AgentReply != "stub agent output" {
			t.Errorf("test review %d input = %+v, want the suite focus, tests in scope, and the tester's relay", i, ri)
		}
		if got := ri.Role; got != activities.RoleTestReview {
			t.Errorf("test review %d Role = %q, want %q", i, got, activities.RoleTestReview)
		}
	}
	if len(scope.inputs) != 1 || !reflect.DeepEqual(scope.inputs[0].Allowed, activities.TestPathPatterns) {
		t.Errorf("scope check inputs = %+v, want one check against the test-path policy", scope.inputs)
	}
	var branch string
	if err := env.GetWorkflowResult(&branch); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if branch != "daedalus/test-only-issue-42-1" {
		t.Errorf("workflow result = %q, want the preserved branch name", branch)
	}
	env.AssertExpectations(t)
}

// TestTestOnlyWorkflowRebuildParks pins the flow's defining constraint:
// there is no implementation loop to rebuild into, so a test-review REBUILD
// parks the run for the maintainer — with the finding in the message —
// keeping "merged work is approved work" true.
func TestTestOnlyWorkflowRebuildParks(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/test-only-issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Rebuild: true, Comments: "handler drops the error path"},
	}}
	rev.record()

	stubTestPhase(env)

	var cleanupCount int
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { cleanupCount++ }).
		Return(nil).Once()

	in := baseInput()
	in.Flow = "test-only"
	in.AllowedPaths = activities.TestPathPatterns
	env.ExecuteWorkflow(TestOnlyWorkflow, in)

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("want workflow error from a REBUILD verdict")
	}
	for _, want := range []string{
		ErrAwaitingMaintainer.Error(),
		"requested a rebuild",
		"outside this flow's test-only scope",
		"handler drops the error path",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	if len(rec.inputs) != 1 {
		t.Errorf("agent ran %d times, want 1 (no dev rebuild round exists in this flow)", len(rec.inputs))
	}
	if cleanupCount != 1 {
		t.Errorf("cleanup ran %d times, want 1 (the attempt's work is preserved for continue)", cleanupCount)
	}
	env.AssertExpectations(t)
}

// TestRefactorWorkflowLoop pins the behavior-frozen flow: the refactor
// prompt opens the run, a red suite still goes to the reviewer (who judges
// structure), both the red logs and the comments travel in the fix prompt,
// and approval leads through the frozen-path gate to finalize.
func TestRefactorWorkflowLoop(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: false, Comments: "extract the parser"},
		{Approved: true},
	}}
	rev.record()

	_, suiteRec := stubTestPhase(env)
	// The green first entry is the preflight gate's baseline; the loop's
	// red round follows it.
	suiteRec.script = []suiteStep{
		{result: activities.TestResult{Passed: true, Logs: "ok"}},
		{result: activities.TestResult{Passed: false, Logs: "--- FAIL: TestParse"}},
	}

	scope := &scopeRecorder{env: env}
	scope.record()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	in := baseInput()
	in.Flow = "refactor"
	in.FrozenPaths = activities.TestPathPatterns
	env.ExecuteWorkflow(RefactorWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(rec.inputs) != 2 {
		t.Fatalf("agent ran %d times, want 2 (refactor, refactor-fix)", len(rec.inputs))
	}
	want, err := template.Refactor("implement the feature")
	if err != nil {
		t.Fatalf("build expected refactor prompt: %v", err)
	}
	if rec.inputs[0].Prompt != want {
		t.Errorf("refactor prompt = %q, want %q", rec.inputs[0].Prompt, want)
	}
	fixPrompt := rec.inputs[1].Prompt
	for _, want := range []string{
		"The frozen test suite failed under the refactoring:",
		"--- FAIL: TestParse",
		"Code review feedback on the refactoring:",
		"extract the parser",
	} {
		if !strings.Contains(fixPrompt, want) {
			t.Errorf("refactor-fix prompt %q should contain %q", fixPrompt, want)
		}
	}
	for i, run := range suiteRec.runs {
		if run.Cover {
			t.Errorf("suite run %d Cover = true, want coverage only in the test-only flow", i)
		}
	}
	if len(rev.inputs) != 2 {
		t.Fatalf("reviewer ran %d times, want 2", len(rev.inputs))
	}
	if rev.inputs[0].Focus != "the refactoring" || rev.inputs[0].TestLogs != "--- FAIL: TestParse" ||
		rev.inputs[0].TestsInScope || rev.inputs[0].ReproInScope {
		t.Errorf("refactor review input = %+v, want the refactoring focus with the red logs", rev.inputs[0])
	}
	if len(scope.inputs) != 1 || !reflect.DeepEqual(scope.inputs[0].Frozen, activities.TestPathPatterns) ||
		len(scope.inputs[0].Allowed) != 0 {
		t.Errorf("scope check inputs = %+v, want one check against the frozen test paths", scope.inputs)
	}
	env.AssertExpectations(t)
}

// TestBugFixWorkflowReproGateLoops pins the repro-first gate: a green suite
// whose diff-tests pass on the pre-fix code is not a reviewable diff — the
// gate's reason feeds back as a red round with no review — and only a real
// repro (gate reproduced) reaches the reviewer in the repro framing, then
// finalize without a write-scope gate (the flow has no path policy).
func TestBugFixWorkflowReproGateLoops(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{{Approved: true}}}
	rev.record()

	_, suiteRec := stubTestPhase(env)
	gate := &reproGateRecorder{env: env, script: []activities.ReproResult{
		{Reproduced: false, Logs: "REPRO-FIRST GATE FAILED: the diff's test files (repro_test.go) pass on the pre-fix code"},
	}}
	gate.record()

	scope := &scopeRecorder{env: env}
	scope.record()
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	in := baseInput()
	in.Flow = "bug-fix"
	env.ExecuteWorkflow(BugFixWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(rec.inputs) != 2 {
		t.Fatalf("agent ran %d times, want 2 (bug-fix, fix after the gate refusal)", len(rec.inputs))
	}
	want, err := template.BugFix("implement the feature")
	if err != nil {
		t.Fatalf("build expected bug-fix prompt: %v", err)
	}
	if rec.inputs[0].Prompt != want {
		t.Errorf("bug-fix prompt = %q, want %q", rec.inputs[0].Prompt, want)
	}
	fixPrompt := rec.inputs[1].Prompt
	if !strings.Contains(fixPrompt,
		"REPRO-FIRST GATE FAILED: the diff's test files (repro_test.go) pass on the pre-fix code") {
		t.Errorf("fix prompt %q should carry the gate's refusal", fixPrompt)
	}
	if strings.Contains(fixPrompt, "Code review feedback") {
		t.Errorf("fix prompt %q should carry no review comments (the gate round never reached a reviewer)", fixPrompt)
	}
	if len(gate.inputs) != 2 {
		t.Fatalf("repro gate ran %d times, want 2 (refused round, reproduced round)", len(gate.inputs))
	}
	if len(suiteRec.runs) != 3 {
		t.Fatalf("suite ran %d times, want 3 (preflight baseline plus one per loop round)", len(suiteRec.runs))
	}
	if gate.inputs[0].Command != "go test ./..." || gate.inputs[0].RepoPath != "/repo" ||
		gate.inputs[0].WorktreePath != "/wt/issue-42" {
		t.Errorf("gate input = %+v, want the resolved command in the run's worktree", gate.inputs[0])
	}
	if len(rev.inputs) != 1 {
		t.Fatalf("reviewer ran %d times, want 1 (only the reproduced round)", len(rev.inputs))
	}
	ri := rev.inputs[0]
	if ri.Focus != "the bug fix and its reproducing test" || !ri.ReproInScope || ri.TestsInScope || ri.AgentReply != "" {
		t.Errorf("bug-fix review input = %+v, want the repro framing with no tester relay", ri)
	}
	if !strings.HasPrefix(ri.TestLogs, "REPRO-FIRST GATE: passed — ") {
		t.Errorf("bug-fix review logs = %q, want the gate's pass note prefixed", ri.TestLogs)
	}
	if len(scope.inputs) != 0 {
		t.Errorf("write-scope gate ran %d times, want 0 (bug-fix has no path policy)", len(scope.inputs))
	}
	env.AssertExpectations(t)
}

// TestBugFixWorkflowRedSuiteSkipsGate pins the gate's precondition: a red
// suite is its own finding and goes straight to the reviewer with its logs —
// the repro gate only means something on a green suite, so it never runs.
func TestBugFixWorkflowRedSuiteSkipsGate(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env}
	rec.record()
	rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
		{Approved: false, Comments: "the fix is too wide"},
		{Approved: true},
	}}
	rev.record()

	_, suiteRec := stubTestPhase(env)
	// The green first entry is the preflight gate's baseline; the loop's
	// red round follows it.
	suiteRec.script = []suiteStep{
		{result: activities.TestResult{Passed: true, Logs: "ok"}},
		{result: activities.TestResult{Passed: false, Logs: "--- FAIL: TestFix"}},
	}

	gate := &reproGateRecorder{env: env}
	gate.record()

	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	in := baseInput()
	in.Flow = "bug-fix"
	env.ExecuteWorkflow(BugFixWorkflow, in)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	// The gate only runs on a green suite: the red round skips it, the
	// second (green) round runs it and comes back reproduced.
	if len(gate.inputs) != 1 {
		t.Errorf("repro gate ran %d times, want 1 (only on the green round)", len(gate.inputs))
	}
	if len(rec.inputs) != 2 {
		t.Fatalf("agent ran %d times, want 2 (bug-fix, fix after review)", len(rec.inputs))
	}
	fixPrompt := rec.inputs[1].Prompt
	for _, want := range []string{
		"The test suite failed:",
		"--- FAIL: TestFix",
		"Code review feedback on the fix:",
		"the fix is too wide",
	} {
		if !strings.Contains(fixPrompt, want) {
			t.Errorf("fix prompt %q should contain %q", fixPrompt, want)
		}
	}
	if len(rev.inputs) != 2 {
		t.Fatalf("reviewer ran %d times, want 2 (red round, green+reproduced round)", len(rev.inputs))
	}
	if rev.inputs[0].TestLogs != "--- FAIL: TestFix" {
		t.Errorf("red-round review logs = %q, want the failing output without a gate prefix", rev.inputs[0].TestLogs)
	}
	if !strings.HasPrefix(rev.inputs[1].TestLogs, "REPRO-FIRST GATE: passed — ") {
		t.Errorf("green-round review logs = %q, want the gate's pass note prefixed", rev.inputs[1].TestLogs)
	}
	env.AssertExpectations(t)
}

// TestFlowsParkOnNeedsMaintainer pins the halt path in the flows whose
// label differs from feature-dev's: a NEEDS_MAINTAINER verdict parks the
// run with the flow's own halt label and the reviewer's comments.
func TestFlowsParkOnNeedsMaintainer(t *testing.T) {
	flows := []struct {
		name  string
		fn    func(workflowContext, PipelineInput) (string, error)
		flow  string
		label string
	}{
		// feature-dev's labels are pinned in its own tests; the docs and
		// test loops carry differently-worded halts.
		{"investigate", InvestigateWorkflow, "investigate", "docs review halted the run"},
		{"test-only", TestOnlyWorkflow, "test-only", "test review halted the run"},
		{"refactor", RefactorWorkflow, "refactor", "code review halted the run"},
		{"bug-fix", BugFixWorkflow, "bug-fix", "code review halted the run"},
	}
	for _, f := range flows {
		t.Run(f.name, func(t *testing.T) {
			env := newTestEnv(t)
			env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
				Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
			rec := &agentRecorder{env: env}
			rec.record()
			rev := &reviewerRecorder{env: env, stub: []activities.ReviewResult{
				{NeedsMaintainer: true, Comments: "needs a secret only the maintainer holds"},
			}}
			rev.record()
			stubTestPhase(env)
			gate := &reproGateRecorder{env: env}
			gate.record()
			env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

			in := baseInput()
			in.Flow = f.flow
			env.ExecuteWorkflow(f.fn, in)

			err := env.GetWorkflowError()
			if err == nil {
				t.Fatal("want workflow error from a NEEDS_MAINTAINER verdict")
			}
			for _, want := range []string{
				ErrAwaitingMaintainer.Error(),
				f.label,
				"needs a secret only the maintainer holds",
			} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("park error %q should contain %q", err, want)
				}
			}
			env.AssertExpectations(t)
		})
	}
}

// TestFlowsPreflightGateParksOnRedBaseline pins the gate itself: a suite
// that is red on the untouched worktree parks the run before any session
// starts — no agent or reviewer round runs, the baseline ran once in the
// run's own worktree, and the park names the command whose baseline failed.
// (Investigate's exemption is pinned by its own tests: they complete with
// no test-phase mocks at all, which a gate there would fail on.)
func TestFlowsPreflightGateParksOnRedBaseline(t *testing.T) {
	flows := []struct {
		name string
		fn   func(workflowContext, PipelineInput) (string, error)
		flow string
	}{
		{"feature-dev", FeatureDevWorkflow, ""},
		{"test-only", TestOnlyWorkflow, "test-only"},
		{"refactor", RefactorWorkflow, "refactor"},
		{"bug-fix", BugFixWorkflow, "bug-fix"},
	}
	for _, f := range flows {
		t.Run(f.name, func(t *testing.T) {
			env := newTestEnv(t)
			env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
				Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
			var agentCalls, reviewCalls int
			env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) { agentCalls++ }).
				Return(activities.AgentRunResult{}, nil).Maybe()
			env.OnActivity(activities.RunJailedReviewerActivity, mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) { reviewCalls++ }).
				Return(activities.ReviewResult{}, nil).Maybe()
			_, suiteRec := stubTestPhase(env)
			suiteRec.script = []suiteStep{
				{result: activities.TestResult{Passed: false, Logs: "--- FAIL: TestBase"}},
			}
			var cleanupCount int
			env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) { cleanupCount++ }).
				Return(nil).Once()

			in := baseInput()
			in.Flow = f.flow
			env.ExecuteWorkflow(f.fn, in)

			err := env.GetWorkflowError()
			if err == nil {
				t.Fatal("want workflow error from a red preflight baseline")
			}
			for _, want := range []string{
				ErrAwaitingMaintainer.Error(),
				"preflight gate failed — the suite must be green before a session starts:",
				"go test ./...",
			} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("park error %q should contain %q", err, want)
				}
			}
			if agentCalls != 0 || reviewCalls != 0 {
				t.Errorf("agent ran %d times, reviewer %d — want 0 and 0 (no session may start on a red baseline)",
					agentCalls, reviewCalls)
			}
			if len(suiteRec.runs) != 1 || suiteRec.runs[0].WorktreePath != "/wt/issue-42" {
				t.Errorf("suite runs = %+v, want one baseline run in the run's own worktree", suiteRec.runs)
			}
			if cleanupCount != 1 {
				t.Errorf("cleanup ran %d times, want 1 (the untouched worktree is preserved for continue)", cleanupCount)
			}
			env.AssertExpectations(t)
		})
	}
}

// workflowContext aliases the workflow context type for the flow tables
// above, keeping the struct literals readable.
type workflowContext = workflow.Context

// TestFlowsAgentErrorFails pins the shared failure path: an agent-round
// system error (not a timeout, kill, slot wait, or exhaustion) fails the
// flow instead of looping, in every flow — the flows share runAgent, but
// each loop body wraps the error with its own stage label.
func TestFlowsAgentErrorFails(t *testing.T) {
	flows := []struct {
		name  string
		fn    func(workflowContext, PipelineInput) (string, error)
		flow  string
		stage string
	}{
		{"investigate", InvestigateWorkflow, "investigate", "initial agent run"},
		{"test-only", TestOnlyWorkflow, "test-only", "initial agent run"},
		{"refactor", RefactorWorkflow, "refactor", "initial agent run"},
		{"bug-fix", BugFixWorkflow, "bug-fix", "initial agent run"},
	}
	for _, f := range flows {
		t.Run(f.name, func(t *testing.T) {
			env := newTestEnv(t)
			env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
				Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
			env.OnActivity(activities.RunJailedClaudeActivity, mock.Anything, mock.Anything).
				Return(activities.AgentRunResult{}, errors.New("agent exploded"))
			// The gated flows run the preflight suite before the first
			// agent round; investigate never touches these activities.
			stubTestPhase(env)
			env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

			in := baseInput()
			in.Flow = f.flow
			env.ExecuteWorkflow(f.fn, in)

			err := env.GetWorkflowError()
			if err == nil || !strings.Contains(err.Error(), f.stage) || !strings.Contains(err.Error(), "agent exploded") {
				t.Errorf("error = %v, want the %s stage wrapping the agent failure", err, f.stage)
			}
			env.AssertExpectations(t)
		})
	}
}
