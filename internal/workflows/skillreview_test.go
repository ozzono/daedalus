package workflows

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/ozzono/daedalus/internal/activities"
)

// TestSkillLabel pins the log/park labeling of a review_skill_list entry: a
// /name entry by its (trimmed) name, a hand-written entry by its position —
// the text itself can be a paragraph, no place for a log line.
func TestSkillLabel(t *testing.T) {
	for _, tt := range []struct {
		i     int
		entry string
		want  string
	}{
		{i: 0, entry: "/style", want: "style"},
		{i: 0, entry: "/ spaced ", want: "spaced"},
		{i: 2, entry: "audit the error handling", want: "#3"},
		{i: 2, entry: "/   ", want: "#3"},
	} {
		if got := skillLabel(tt.i, tt.entry); got != tt.want {
			t.Errorf("skillLabel(%d, %q) = %q, want %q", tt.i, tt.entry, got, tt.want)
		}
	}
}

// TestFeatureDevWorkflowSkillReviewsChain pins the whole chain on a
// two-entry run: after the default reviewer approves the code, each entry
// gets its own reviewer conversation and ping-pong in order, its feedback
// riding the dev session; the test phase then runs the same chain on the
// suite. Empty SkillEntry marks the default reviews; the entries travel
// verbatim (resolution is the reviewer activity's job).
func TestFeatureDevWorkflowSkillReviewsChain(t *testing.T) {
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)

	rec := &agentRecorder{env: env, result: activities.AgentRunResult{Text: "ok", SessionID: "dev-s1"}}
	rec.record()

	rev := &reviewerRecorder{env: env, script: []reviewStep{
		{result: activities.ReviewResult{Approved: true}}, // default code review
		// /style's code review rejects once — its fix rides the dev session —
		// then approves.
		{result: activities.ReviewResult{Approved: false, Comments: "no passive voice", SessionID: "style-s1"}},
		{result: activities.ReviewResult{Approved: true, SessionID: "style-s1"}},
		{result: activities.ReviewResult{Approved: true}}, // the hand-written entry's code review
		{result: activities.ReviewResult{Approved: true}}, // default test review
		{result: activities.ReviewResult{Approved: true, SessionID: "tstyle-s1"}}, // /style's suite review
		{result: activities.ReviewResult{Approved: true}}, // the hand-written entry's suite review
	}}
	rev.record()

	stubTestPhase(env)
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

	in := baseInput()
	in.ReviewSkillList = []string{"/style", "audit the error handling in prose"}
	env.ExecuteWorkflow(FeatureDevWorkflow, in)
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

	if len(rev.inputs) != 7 {
		t.Fatalf("reviewer ran %d times, want 7 (code + 2 skill code + test + 2 skill test, with one skill fix round)", len(rev.inputs))
	}
	wantEntries := []string{"", "/style", "/style", "audit the error handling in prose", "", "/style", "audit the error handling in prose"}
	for i, want := range wantEntries {
		if got := rev.inputs[i].SkillEntry; got != want {
			t.Errorf("review %d SkillEntry = %q, want %q (default rounds empty, entries verbatim, in order)", i, got, want)
		}
	}
	// Phase 1 stays an implementation review for every skill; phase 2 the
	// test review.
	for i := 0; i < 4; i++ {
		if rev.inputs[i].Focus != "the implementation" || rev.inputs[i].TestsInScope ||
			rev.inputs[i].Role != activities.RoleDevReview {
			t.Errorf("review %d = %+v, want an implementation review on the dev-review role", i, rev.inputs[i])
		}
	}
	for i := 4; i < 7; i++ {
		if rev.inputs[i].Focus != "the test suite" || !rev.inputs[i].TestsInScope ||
			rev.inputs[i].Role != activities.RoleTestReview {
			t.Errorf("review %d = %+v, want a test-suite review on the test-review role", i, rev.inputs[i])
		}
	}
	// Each skill's conversation is its own: /style's second code round
	// resumes its session; the other entries and the test-phase reviewers
	// all start cold even though /style just ran.
	if got := rev.inputs[1].SessionID; got != "" {
		t.Errorf("first /style code review SessionID = %q, want empty (own conversation starts cold)", got)
	}
	if got := rev.inputs[2].SessionID; got != "style-s1" {
		t.Errorf("second /style code review SessionID = %q, want its own session resumed", got)
	}
	if got := rev.inputs[3].SessionID; got != "" {
		t.Errorf("second entry's code review SessionID = %q, want empty (a different conversation, not /style's)", got)
	}
	if got := rev.inputs[5].SessionID; got != "" {
		t.Errorf("/style's suite review SessionID = %q, want empty (phase 2 gives each skill its own reviewer too)", got)
	}
	if got := rev.inputs[6].SessionID; got != "" {
		t.Errorf("second entry's suite review SessionID = %q, want empty (not /style's test-phase conversation)", got)
	}

	// The skill's feedback rode the dev session: one fix round carrying its
	// comments, resuming the implementing conversation.
	if len(rec.inputs) != 3 {
		t.Fatalf("agent ran %d times, want 3 (implement, one skill fix round, tests)", len(rec.inputs))
	}
	if !strings.Contains(rec.inputs[1].Prompt, "no passive voice") {
		t.Errorf("fix prompt %q should carry the skill's comments", rec.inputs[1].Prompt)
	}
	if got := rec.inputs[1].SessionID; got != "dev-s1" {
		t.Errorf("fix round SessionID = %q, want the dev session", got)
	}
	env.AssertExpectations(t)
}

// TestFeatureDevWorkflowSkillReviewHaltParks pins both NEEDS_MAINTAINER
// sites: a phase-1 skill halt parks labeled by the entry's name, a phase-2
// skill halt by a hand-written entry's position — and neither drives a fix
// round.
func TestFeatureDevWorkflowSkillReviewHaltParks(t *testing.T) {
	t.Run("code phase, named skill", func(t *testing.T) {
		env := newTestEnv(t)
		env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
			Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
		rec := &agentRecorder{env: env}
		rec.record()
		rev := &reviewerRecorder{env: env, script: []reviewStep{
			{result: activities.ReviewResult{Approved: true}},
			{result: activities.ReviewResult{NeedsMaintainer: true, Comments: "the skill demands a style guide no agent can write"}},
		}}
		rev.record()
		stubTestPhase(env)
		env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

		in := baseInput()
		in.ReviewSkillList = []string{"/style"}
		env.ExecuteWorkflow(FeatureDevWorkflow, in)

		err := env.GetWorkflowError()
		if err == nil {
			t.Fatal("want workflow error from a skill's NEEDS_MAINTAINER verdict")
		}
		for _, want := range []string{
			ErrAwaitingMaintainer.Error(),
			"skill review (style) halted the run",
			"the skill demands a style guide no agent can write",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("park error %q should contain %q", err, want)
			}
		}
		if len(rec.inputs) != 1 {
			t.Errorf("agent ran %d times, want 1 (implement only; no fix round after a halt)", len(rec.inputs))
		}
		env.AssertExpectations(t)
	})

	t.Run("test phase, hand-written entry is labeled by position", func(t *testing.T) {
		env := newTestEnv(t)
		env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
			Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
		rec := &agentRecorder{env: env}
		rec.record()
		rev := &reviewerRecorder{env: env, script: []reviewStep{
			{result: activities.ReviewResult{Approved: true}}, // default code review
			{result: activities.ReviewResult{Approved: true}}, // the entry's code review
			{result: activities.ReviewResult{Approved: true}}, // default test review
			{result: activities.ReviewResult{NeedsMaintainer: true, Comments: "the suite needs a human-owned fixture"}},
		}}
		rev.record()
		stubTestPhase(env)
		env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

		in := baseInput()
		in.ReviewSkillList = []string{"demand a changelog entry"}
		env.ExecuteWorkflow(FeatureDevWorkflow, in)

		err := env.GetWorkflowError()
		if err == nil {
			t.Fatal("want workflow error from a skill's NEEDS_MAINTAINER verdict")
		}
		for _, want := range []string{
			ErrAwaitingMaintainer.Error(),
			"skill test review (#1) halted the run",
			"the suite needs a human-owned fixture",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("park error %q should contain %q", err, want)
			}
		}
		if len(rec.inputs) != 2 {
			t.Errorf("agent ran %d times, want 2 (implement + tests; no fix round after a halt)", len(rec.inputs))
		}
		env.AssertExpectations(t)
	})
}

// TestFeatureDevWorkflowTestPhaseSkillReviews pins the phase-2 skill gate's
// two behaviors the split into suiteRound/reviewSuite exists for: a skill's
// first round reviews the suite result already collected (no agent round
// ran since — a re-run is pure spend and could show a flaky failure the
// diff did not cause), and only a fix round, which moves the tree, triggers
// a fresh execution; and a skill REBUILD re-enters the whole phase at the
// top — fresh suite, default reviewer first, phase-1 skill reviews too.
func TestFeatureDevWorkflowTestPhaseSkillReviews(t *testing.T) {
	t.Run("first round reuses the collected suite; a fix round runs a fresh one", func(t *testing.T) {
		env := newTestEnv(t)
		env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
			Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
		rec := &agentRecorder{env: env, result: activities.AgentRunResult{Text: "tester reply", SessionID: "test-s1"}}
		rec.record()
		rev := &reviewerRecorder{env: env, script: []reviewStep{
			{result: activities.ReviewResult{Approved: true}}, // default code review
			{result: activities.ReviewResult{Approved: true}}, // the entry's code review
			{result: activities.ReviewResult{Approved: true}}, // default test review
			// The skill rejects the collected result, its fix rides the tester
			// session, and its next look approves the fresh suite.
			{result: activities.ReviewResult{Approved: false, Comments: "tighten the assertions", SessionID: "suite-s1"}},
			{result: activities.ReviewResult{Approved: true, SessionID: "suite-s1"}},
		}}
		rev.record()
		_, suiteRec := stubTestPhase(env)
		suiteRec.script = []suiteStep{
			{result: activities.TestResult{Passed: true, Logs: "baseline"}},
			{result: activities.TestResult{Passed: true, Logs: "phase2"}},
			// Script exhausted: the post-fix round falls back to green "ok".
		}
		env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
			Return("daedalus/issue-42-1", nil).Once()
		env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

		in := baseInput()
		in.ReviewSkillList = []string{"/suite"}
		env.ExecuteWorkflow(FeatureDevWorkflow, in)
		if err := env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow error: %v", err)
		}

		// Three executions: preflight baseline, the phase-2 round, and the
		// one after the fix — the skill's first look re-ran nothing.
		if len(suiteRec.runs) != 3 {
			t.Fatalf("suite ran %d times, want 3 (baseline, phase 2, post-fix)", len(suiteRec.runs))
		}
		if len(rev.inputs) != 5 {
			t.Fatalf("reviewer ran %d times, want 5 (code + entry code + test + entry test x2)", len(rev.inputs))
		}
		wantEntries := []string{"", "/suite", "", "/suite", "/suite"}
		for i, want := range wantEntries {
			if got := rev.inputs[i].SkillEntry; got != want {
				t.Errorf("review %d SkillEntry = %q, want %q", i, got, want)
			}
		}
		if got := rev.inputs[3].TestLogs; got != "phase2" {
			t.Errorf("skill's first suite review logs = %q, want the already-collected result, not a fresh execution", got)
		}
		if got := rev.inputs[4].TestLogs; got != "ok" {
			t.Errorf("skill's second suite review logs = %q, want the post-fix fresh execution", got)
		}
		// The skill's rounds are ordinary test reviews: tester relay, test
		// framing, and their own conversation across rounds.
		if !rev.inputs[3].TestsInScope || rev.inputs[3].Role != activities.RoleTestReview {
			t.Errorf("skill suite review = %+v, want the test-review framing", rev.inputs[3])
		}
		if rev.inputs[3].AgentReply == "" {
			t.Error("skill suite review should carry the tester's latest reply")
		}
		if got := rev.inputs[3].SessionID; got != "" {
			t.Errorf("skill suite review SessionID = %q, want empty (own conversation starts cold)", got)
		}
		if got := rev.inputs[4].SessionID; got != "suite-s1" {
			t.Errorf("second skill suite review SessionID = %q, want its own session resumed", got)
		}
		// The fix rode the tester conversation, not the dev session.
		if len(rec.inputs) != 3 {
			t.Fatalf("agent ran %d times, want 3 (implement, tests, tests-fix)", len(rec.inputs))
		}
		if rec.inputs[2].Role != activities.RoleTest {
			t.Errorf("fix round Role = %q, want %q", rec.inputs[2].Role, activities.RoleTest)
		}
		if !strings.Contains(rec.inputs[2].Prompt, "tighten the assertions") {
			t.Errorf("fix prompt %q should carry the skill's comments", rec.inputs[2].Prompt)
		}
		if got := rec.inputs[2].SessionID; got != "test-s1" {
			t.Errorf("fix round SessionID = %q, want the tester session", got)
		}
		env.AssertExpectations(t)
	})

	t.Run("a skill's REBUILD re-enters the whole phase", func(t *testing.T) {
		env := newTestEnv(t)
		env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
			Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
		rec := &agentRecorder{env: env}
		rec.record()
		rev := &reviewerRecorder{env: env, script: []reviewStep{
			{result: activities.ReviewResult{Approved: true}}, // default code review
			{result: activities.ReviewResult{Approved: true}}, // the entry's code review
			{result: activities.ReviewResult{Approved: true}}, // default test review
			// The skill verdicts REBUILD on the implementation...
			{result: activities.ReviewResult{Rebuild: true, Comments: "the handler drops the error path"}},
			// ...and the re-entered phase runs the whole gate again: default
			// code review, the entry's code review, a fresh suite, the
			// default test review, and the skill's approval of the collected
			// result.
			{result: activities.ReviewResult{Approved: true}},
			{result: activities.ReviewResult{Approved: true}},
			{result: activities.ReviewResult{Approved: true}},
			{result: activities.ReviewResult{Approved: true}},
		}}
		rev.record()
		_, suiteRec := stubTestPhase(env)
		suiteRec.script = []suiteStep{
			{result: activities.TestResult{Passed: true, Logs: "baseline"}},
			{result: activities.TestResult{Passed: true, Logs: "phase2"}},
			// Script exhausted: the post-rebuild round falls back to "ok".
		}
		env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
			Return("daedalus/issue-42-1", nil).Once()
		env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()

		in := baseInput()
		in.ReviewSkillList = []string{"/suite"}
		env.ExecuteWorkflow(FeatureDevWorkflow, in)
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

		// Three executions: baseline, the first phase-2 round, the
		// post-rebuild one the re-entry ran before any review.
		if len(suiteRec.runs) != 3 {
			t.Fatalf("suite ran %d times, want 3 (baseline, phase 2, post-rebuild)", len(suiteRec.runs))
		}
		if len(rev.inputs) != 8 {
			t.Fatalf("reviewer ran %d times, want 8 (2 phase-1 default+skill, default test, skill rebuild, then 2+1+1 on the re-entry)", len(rev.inputs))
		}
		wantEntries := []string{"", "/suite", "", "/suite", "", "/suite", "", "/suite"}
		for i, want := range wantEntries {
			if got := rev.inputs[i].SkillEntry; got != want {
				t.Errorf("review %d SkillEntry = %q, want %q (the re-entry reruns the default first)", i, got, want)
			}
		}
		if got := rev.inputs[6].TestLogs; got != "ok" {
			t.Errorf("re-entry default test review logs = %q, want the post-rebuild execution", got)
		}
		if got := rev.inputs[7].TestLogs; got != "ok" {
			t.Errorf("re-entry skill review logs = %q, want the same collected result, not another execution", got)
		}
		// The rebuild finding went back through the dev conversation.
		if len(rec.inputs) != 3 {
			t.Fatalf("agent ran %d times, want 3 (implement, tests, rebuild)", len(rec.inputs))
		}
		if !strings.Contains(rec.inputs[2].Prompt, "the handler drops the error path") {
			t.Errorf("rebuild prompt %q should carry the skill's finding", rec.inputs[2].Prompt)
		}
		env.AssertExpectations(t)
	})
}
