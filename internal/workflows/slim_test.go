package workflows

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
)

// twoSubtaskProsePlan is a well-formed plan round reply: the prose plan the
// planner's pure-generation round emits, which the parse round transcribes.
const twoSubtaskProsePlan = "Sub-task 1 (implement, adder.go): add the adder — Add(2,2) returns 4.\nSub-task 2 (implement, mul.go): multiply the adder — Mul(3,2) returns 6."

// twoSubtaskPlan is a well-formed parse round reply: two atomic sub-tasks
// transcribed from that plan, the second building on the first.
const twoSubtaskPlan = `[{"id":1,"type":"implement","target_files":["adder.go"],"description":"add the adder","acceptance_criteria":["adder.go defines Add","Add(2,2) returns 4"]},{"id":2,"type":"implement","target_files":["mul.go"],"description":"multiply the adder","acceptance_criteria":["mul.go defines Mul","Mul(3,2) returns 6"]}]`

// stubSlimRun wires the shared happy-path defaults for a slim run: a
// worktree, green-discovered suite, an in-scope write-scope gate, finalize,
// and cleanup. Tests script the recorders they care about — a violating
// scope result is `scope.res` set after the stub.
func stubSlimRun(t *testing.T) (*testsuite.TestWorkflowEnvironment, *agentRecorder, *reviewerRecorder, *suiteRecorder, *scopeRecorder) {
	t.Helper()
	env := newTestEnv(t)
	env.OnActivity(activities.CreateWorktreeActivity, mock.Anything, mock.Anything).
		Return(activities.WorktreeOutput{WorktreePath: "/wt/issue-42"}, nil)
	agents := &agentRecorder{env: env}
	agents.record()
	reviews := &reviewerRecorder{env: env}
	reviews.record()
	_, suites := stubTestPhase(env)
	scope := &scopeRecorder{env: env}
	scope.record()
	env.OnActivity(activities.CleanupWorktreeActivity, mock.Anything, mock.Anything).Return(nil).Once()
	return env, agents, reviews, suites, scope
}

// TestParseSlimPlan pins the parse round's reply contract: strict — the
// whole reply, whitespace trimmed and one wrapping markdown code fence
// dropped, must unmarshal as the raw SlimSubtask JSON array. There is no
// bracket slicing, so an array wrapped in prose is rejected (the parse
// round owns the wire format outright); an empty array, a reply without an
// array, malformed JSON, and a sub-task missing its description or
// acceptance criteria are all rejected too, because the atomic loop cannot
// run without them. Two syntax classes are repaired deterministically
// before the reply costs its re-ask: a comma directly before a closing
// bracket is stripped, and a reply whose every element was already
// complete but whose final closers were dropped is closed — accepted only
// when the closed text re-parses in full and passes the per-field
// validation, and reported through the repaired flag so the caller can
// stamp the run (the comma strip is never reported; only the closure rides
// the run memo). String content is untouched by both, a reply cut off
// mid-value is never closed (inventing the quote would fabricate content),
// an element-less reply is never closed either (a bracket-only fragment
// keeps its parse error instead of closing into a fabricated empty queue),
// the two repairs are never composed, and a failure after repair still
// reports the error for the reply as written. The flag's gate side is
// pinned in TestParseSlimPlanRepairGate.
func TestParseSlimPlan(t *testing.T) {
	valid := twoSubtaskPlan
	for _, c := range []struct {
		name     string
		text     string
		wantN    int
		wantErr  string
		wantDesc string
		repaired bool
	}{{
		name:  "raw array",
		text:  valid,
		wantN: 2,
	}, {
		name:  "array in a wrapping code fence",
		text:  "```json\n" + valid + "\n```\n",
		wantN: 2,
	}, {
		// A fence is the one formatting wrapper tolerated; a missing closer
		// still strips (the leading fence line is enough).
		name:  "array under an unclosed fence",
		text:  "```json\n" + valid,
		wantN: 2,
	}, {
		// The old extractor took the text from the first "[" through the
		// last "]", so prose around the array parsed; the strict parse
		// rejects it.
		name:    "array wrapped in prose",
		text:    "Here is the plan:\n" + valid + "\nLet me know if this works!",
		wantErr: "reply is not the required raw JSON array",
	}, {
		// The deterministic trailing-comma repair: the one syntax class
		// small models emit routinely is stripped and the parse retried
		// before the reply spends its re-ask — after the last subtask, the
		// last field, inside a nested array, across a newline, and inside
		// the code fence the reply may wrap.
		name:  "trailing comma after the last subtask",
		text:  valid[:len(valid)-1] + ",]",
		wantN: 2,
	}, {
		name:  "trailing comma after the last field",
		text:  `[{"id":1,"type":"implement","target_files":["a.go"],"description":"do it","acceptance_criteria":["a.go exists"],}]`,
		wantN: 1,
	}, {
		name:  "trailing comma in a nested criteria array",
		text:  `[{"id":1,"type":"implement","target_files":["a.go"],"description":"do it","acceptance_criteria":["a.go exists","it compiles",]}]`,
		wantN: 1,
	}, {
		name: "trailing comma across a newline",
		text: `[
  {"id":1,"type":"implement","target_files":["a.go"],"description":"do it","acceptance_criteria":["a.go exists"]},
]`,
		wantN: 1,
	}, {
		name:  "trailing comma inside a code fence",
		text:  "```json\n" + `[{"id":1,"type":"implement","target_files":["a.go"],"description":"do it","acceptance_criteria":["a.go exists"]},]` + "\n```",
		wantN: 1,
	}, {
		// A comma inside a string literal is content, never a repair
		// candidate: the value must survive the strip byte-exact — a naive
		// byte sweep would silently rewrite it to "fix a] bug".
		name:     "comma inside a string survives the repair",
		text:     `[{"id":1,"type":"implement","target_files":["a.go"],"description":"fix a,] bug","acceptance_criteria":["a.go exists"],}]`,
		wantN:    1,
		wantDesc: "fix a,] bug",
	}, {
		// Escaped quotes keep the scanner inside the string: the in-string
		// comma after one stays and the value round-trips exactly — a
		// quote-toggle scanner would leave the string at the escape and
		// strip that comma, silently rewriting the description.
		name:     "escaped quotes keep string state during the repair",
		text:     `[{"id":1,"type":"implement","target_files":["a.go"],"description":"say \"a,] ok","acceptance_criteria":["a.go exists"],}]`,
		wantN:    1,
		wantDesc: `say "a,] ok`,
	}, {
		// The deterministic bracket-closure repair: a reply whose every
		// element was complete but whose final closers were dropped is
		// closed and accepted on the full re-parse, with repaired
		// reporting the rescue (the caller warns and stamps the run
		// memo). The motivating shape: the whole queue present, only the
		// array's final "]" missing.
		name:     "missing final bracket closes",
		text:     valid[:len(valid)-1],
		wantN:    2,
		repaired: true,
	}, {
		// Closers append in reverse opening order: an element cut right
		// after its last field's array still needs "}" and then "]".
		name:     "missing closers after the last field close",
		text:     `[{"id":1,"type":"implement","target_files":["a.go"],"description":"do it","acceptance_criteria":["a.go exists"]`,
		wantN:    1,
		repaired: true,
	}, {
		// Brackets inside string literals are content and are never
		// balanced against — the walk is string-aware exactly like the
		// comma strip, so a description full of brackets closes to a
		// queue carrying it byte-exact.
		name:     "brackets inside a string never balance the walk",
		text:     `[{"id":1,"type":"implement","target_files":["a.go"],"description":"fix {the} [bug]","acceptance_criteria":["a.go exists"]`,
		wantN:    1,
		wantDesc: "fix {the} [bug]",
		repaired: true,
	}, {
		// A walk that ends inside an unterminated string closes nothing:
		// a reply cut off mid-value is genuine truncation, and inventing
		// the quote that ends it would fabricate content — the one thing
		// the closure may not do.
		name:    "unterminated string closes nothing",
		text:    `[{"id":1,"type":"implement","target_files":["a.go"],"description":"cut off`,
		wantErr: "reply is not the required raw JSON array",
	}, {
		// The element gate on the closure: a reply carrying no subtask
		// object — the degenerate one-token "[" a starved round emits —
		// is never closed into a fabricated "[]", whose "empty array"
		// failure would misdescribe a queue the model never wrote. The
		// original parse error stands and the re-ask is spent instead.
		name:    "bracket-only fragment closes nothing",
		text:    "[",
		wantErr: "reply is not the required raw JSON array",
	}, {
		// The repairs are never composed: this element was cut off right
		// after a dangling field comma, so its closure only parses
		// together with the comma strip — closing AND stripping one text
		// would silently accept a complete-looking queue, the
		// truncation-masking outcome the strict parse exists to prevent.
		// The reply keeps failing and spends the re-ask.
		name:    "closure and comma strip are never composed",
		text:    `[{"id":1,"type":"implement","target_files":["a.go"],"description":"do it","acceptance_criteria":["a.go exists"],`,
		wantErr: "reply is not the required raw JSON array",
	}, {
		// The closure never bypasses validation: closed text is held to
		// the same per-field contract, so an element whose criteria
		// array closes empty is rejected like any other.
		name:    "closed subtask still validated",
		text:    `[{"id":1,"type":"implement","target_files":["a.go"],"description":"do it","acceptance_criteria":[`,
		wantErr: "carries no acceptance criteria",
	}, {
		// Deliberately not a truncation repair, from either direction:
		// this reply was cut off right after a complete element plus a
		// comma. The closure walk finds the array still open and appends
		// its "]", but the closed text (",]") fails the full re-parse —
		// and the repairs never compose — so the reply still fails and
		// spends the re-ask instead of unmarshaling a shortened queue as
		// complete.
		name:    "truncated reply is not auto-closed",
		text:    `[{"id":1,"type":"implement","target_files":["a.go"],"description":"do it","acceptance_criteria":["a.go exists"]},`,
		wantErr: "reply is not the required raw JSON array",
	}, {
		// The error the re-ask quotes must describe the reply as written:
		// a failure after the strip keeps the original unmarshal error
		// (']' after the comma), not the repaired text's ('H' in the prose).
		name:    "repair failure keeps the original error",
		text:    valid[:len(valid)-1] + ",]\nHope this helps!",
		wantErr: "invalid character ']'",
	}, {
		// The repair never skips validation: a stripped-down empty object
		// still needs the fields the atomic loop cannot run without.
		name:    "repaired subtask still validated",
		text:    `[{"id":1,}]`,
		wantErr: "carries no description",
	}, {
		// ponytail ceiling of the line-based fence strip (slim.go): a
		// single-line fence-plus-array is invisible to it and spends the
		// parse round's re-ask.
		name:    "single-line fence and array",
		text:    "```json " + valid,
		wantErr: "reply is not the required raw JSON array",
	}, {
		name:    "no array",
		text:    "I cannot plan this task, sorry.",
		wantErr: "reply is not the required raw JSON array",
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
	}, {
		// The acceptance gate enforces the declared scope, so a subtask
		// with nothing declared for it cannot run: the missing field and
		// the explicit empty array both reject into the parse round's
		// re-ask (the gate side is pinned in TestParseSlimPlanTargetGate).
		name:    "subtask without target files",
		text:    `[{"id":1,"type":"implement","description":"do it","acceptance_criteria":["a.go exists"]}]`,
		wantErr: "carries no target files",
	}, {
		name:    "subtask with empty target files",
		text:    `[{"id":1,"type":"implement","target_files":[],"description":"do it","acceptance_criteria":["a.go exists"]}]`,
		wantErr: "carries no target files",
	}} {
		t.Run(c.name, func(t *testing.T) {
			got, repaired, err := parseSlimPlan(c.text, true, true)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("parseSlimPlan(%q) err = %v, want it to contain %q", c.text, err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSlimPlan(%q): %v", c.text, err)
			}
			if repaired != c.repaired {
				t.Fatalf("parsed with repaired = %v, want %v (only the bracket closure reports a repair, never the comma strip)", repaired, c.repaired)
			}
			if len(got) != c.wantN {
				t.Fatalf("parsed %d subtasks, want %d", len(got), c.wantN)
			}
			if c.wantDesc != "" && got[0].Description != c.wantDesc {
				t.Fatalf("parsed description = %q, want %q (the repair must not touch string content)", got[0].Description, c.wantDesc)
			}
		})
	}
	if got, _, err := parseSlimPlan(valid, true, true); err != nil || got[1].Description != "multiply the adder" ||
		len(got[1].AcceptanceCriteria) != 2 || got[1].TargetFiles[0] != "mul.go" {
		t.Errorf("parseSlimPlan fields = %+v (err %v), want the wire schema's named fields carried through", got, err)
	}
}

// TestParseSlimPlanRepairGate pins the replay gate around the closure
// repair: the workflow passes its GetVersion marker as parseSlimPlan's
// repair flag, so the false side must decide exactly as the code that
// recorded pre-marker history did — the closure is off (a missing-closer
// reply fails and spends the re-ask), while the comma strip predates the
// marker and stays on, repairing without reporting a repair (only the
// closure is ever reported; the run memo rides that report alone). Both
// markers off: targets=false beside repair=false is the full pre-marker
// parse.
func TestParseSlimPlanRepairGate(t *testing.T) {
	closable := twoSubtaskPlan[:len(twoSubtaskPlan)-1] // every element complete, the final "]" dropped
	if _, repaired, err := parseSlimPlan(closable, false, false); err == nil || repaired {
		t.Errorf("parseSlimPlan(closable reply, repair=false) = repaired %v, err %v, want the gated-off closure to reject the reply", repaired, err)
	}
	got, repaired, err := parseSlimPlan(closable+",]", false, false)
	if err != nil || repaired || len(got) != 2 {
		t.Errorf("parseSlimPlan(trailing-comma reply, repair=false) = (%d subtasks, repaired %v, err %v), want the ungated comma strip to repair without reporting", len(got), repaired, err)
	}
}

// TestParseSlimPlanTargetGate pins the replay gate around the target-files
// contract: parseSlimPlan's targets flag carries the slim scope marker, so
// the false side must decide exactly as the code that recorded pre-marker
// history did — a subtask without target files was a valid queue then and
// parses there still — while the gated-on side rejects it into the parse
// round's re-ask (the acceptance gate has nothing to enforce without it).
func TestParseSlimPlanTargetGate(t *testing.T) {
	noTargets := `[{"id":1,"type":"implement","description":"do it","acceptance_criteria":["a.go exists"]}]`
	if got, _, err := parseSlimPlan(noTargets, false, false); err != nil || len(got) != 1 {
		t.Errorf("parseSlimPlan(no-targets reply, targets=false) = (%d subtasks, err %v), want the gated-off contract to accept the queue as pre-marker history recorded it", len(got), err)
	}
	if _, _, err := parseSlimPlan(noTargets, false, true); err == nil || !strings.Contains(err.Error(), "carries no target files") {
		t.Errorf("parseSlimPlan(no-targets reply, targets=true) err = %v, want the target-files rejection that spends the re-ask", err)
	}
}

// TestSlimWorkflowHappyPath pins the slim flow's shape end to end: the plan
// round opens the dev conversation, the parse round opens a fresh session of
// its own (the plan text rides its prompt verbatim, so the planner's
// exploration never inflates the parse prompt) and stays out of that
// conversation for good, each sub-task implements in the chained worker
// conversation while every review is a
// fresh session judged against exactly that sub-task's acceptance criteria,
// a red suite blocks approval even under an approving review (its logs ride
// the fix prompt, green logs stay out of the review prompt), each acceptance
// clears the structural write-scope gate on the sub-task's targets stacked
// on every accepted sub-task's (checked only at the acceptance point, so
// in-scope work costs no new rounds or prompts), and an accepted queue
// finalizes.
func TestSlimWorkflowHappyPath(t *testing.T) {
	env, agents, reviews, suites, scope := stubSlimRun(t)

	agents.script = []agentStep{
		{result: activities.AgentRunResult{Text: twoSubtaskProsePlan, SessionID: "dev-1"}},
		{result: activities.AgentRunResult{Text: twoSubtaskPlan, SessionID: "dev-2"}},
		{result: activities.AgentRunResult{Text: "step one done", SessionID: "dev-3"}},
		{result: activities.AgentRunResult{Text: "fixed the failure", SessionID: "dev-4"}},
		{result: activities.AgentRunResult{Text: "fixed the shape", SessionID: "dev-5"}},
		{result: activities.AgentRunResult{Text: "step two done", SessionID: "dev-6"}},
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

	// Worker rounds: plan, parse, step 1, fix 1, fix 2, step 2 — the
	// implementation rounds chain the plan's dev conversation, while the
	// parse round's fresh session stays out of the chain entirely.
	if len(agents.inputs) != 6 {
		t.Fatalf("agent ran %d times, want 6 (plan, parse, step, fix, fix, step)", len(agents.inputs))
	}
	if got := agents.inputs[0].SessionID; got != "" {
		t.Errorf("planner SessionID = %q, want empty (the dev conversation starts cold)", got)
	}
	if p := agents.inputs[0].Prompt; !strings.Contains(p, "implement the feature") || !strings.Contains(p, "PLANNER") {
		t.Errorf("planner prompt should carry the task through the slim plan template, got:\n%s", p)
	}
	if got := agents.inputs[1].SessionID; got != "" {
		t.Errorf("parse round SessionID = %q, want empty (the parse round opens a fresh session — the planner's exploration must not inflate the parse prompt)", got)
	}
	if p := agents.inputs[1].Prompt; !strings.Contains(p, twoSubtaskProsePlan) ||
		!strings.Contains(p, "raw JSON array") || !strings.Contains(p, `"acceptance_criteria"`) {
		t.Errorf("parse prompt should carry the plan it transcribes and the JSON array contract, got:\n%s", p)
	}
	for i, wantSession := range []string{"dev-1", "dev-3", "dev-4", "dev-5"} {
		if got := agents.inputs[i+2].SessionID; got != wantSession {
			t.Errorf("worker round %d SessionID = %q, want %q (the implementation rounds chain the plan's dev conversation)", i+2, got, wantSession)
		}
	}
	if p := agents.inputs[2].Prompt; !strings.Contains(p, "sub-tasks 1 of 2") || !strings.Contains(p, "add the adder") ||
		!strings.Contains(p, "Add(2,2) returns 4") {
		t.Errorf("step prompt should carry sub-task 1 of 2 with its own description and criteria, got:\n%s", p)
	}
	if p := agents.inputs[3].Prompt; !strings.Contains(p, "--- FAIL: TestAdd") ||
		strings.Contains(p, "Reviewer comments") {
		t.Errorf("fix prompt for a red-suite round should carry the failing logs and no reviewer comments (the round approved), got:\n%s", p)
	}
	if p := agents.inputs[4].Prompt; !strings.Contains(p, "the shape is wrong") || strings.Contains(p, "--- FAIL: TestAdd") {
		t.Errorf("fix prompt for a green-suite round should carry only the reviewer comments, got:\n%s", p)
	}
	if p := agents.inputs[5].Prompt; !strings.Contains(p, "sub-tasks 2 of 2") || !strings.Contains(p, "multiply the adder") {
		t.Errorf("step 2 prompt should carry sub-task 2 of 2, got:\n%s", p)
	}

	// Reviews: one fresh session per round, judged against exactly the
	// current sub-task's criteria; red logs travel, green logs stay out.
	// Role: every sub-task but the last reviews as the plain reviewer;
	// the last sub-task's reviews are the run's terminal review (its
	// worktree diff carries the whole plan's accepted work) and resolve
	// as the final-review role.
	if len(reviews.inputs) != 4 {
		t.Fatalf("reviewer ran %d times, want 4 (three rounds for sub-task 1, one for sub-task 2)", len(reviews.inputs))
	}
	for i, in := range reviews.inputs {
		wantRole := activities.RoleDevReview
		if i == 3 {
			wantRole = activities.RoleFinalReview
		}
		if !in.FreshReview {
			t.Errorf("review %d FreshReview = false, want true (no review resumes another's conversation)", i)
		}
		if in.SessionID != "" {
			t.Errorf("review %d SessionID = %q, want empty (fresh review mode never resumes)", i, in.SessionID)
		}
		if in.Role != wantRole {
			t.Errorf("review %d Role = %v, want %v", i, in.Role, wantRole)
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

	// The write-scope gate runs exactly at each acceptance point — two
	// calls for two sub-tasks, never on the red or comment-rejected
	// rounds between them — each against the diff's cumulative scope: the
	// sub-task's own targets plus every earlier accepted sub-task's
	// (nothing commits before finalize, so the final check carries the
	// whole plan's union). In-scope work pays no new rounds or prompts:
	// the agent, review, and suite counts above are unchanged by the gate.
	if len(scope.inputs) != 2 {
		t.Fatalf("write-scope gate ran %d times, want 2 (one per acceptance point — never on a red or comment-rejected round)", len(scope.inputs))
	}
	for i, want := range [][]string{{"adder.go"}, {"adder.go", "mul.go"}} {
		if !reflect.DeepEqual(scope.inputs[i].Allowed, want) {
			t.Errorf("scope check %d Allowed = %v, want %v (the sub-task's targets on top of every accepted sub-task's)", i, scope.inputs[i].Allowed, want)
		}
		if len(scope.inputs[i].Frozen) != 0 {
			t.Errorf("scope check %d Frozen = %v, want empty (the slim flow declares no frozen paths)", i, scope.inputs[i].Frozen)
		}
	}
	env.AssertExpectations(t)
}

// TestSlimWorkflowParseRetry pins the parse round's one strict re-ask: a
// malformed first reply is re-asked inside the parse round's own
// conversation — the re-ask carries the parse prompt (and so the plan) with
// the failure appended, never a re-run of the planner — and a parseable
// second reply proceeds while an unparseable one parks the run.
func TestSlimWorkflowParseRetry(t *testing.T) {
	oneSubtaskQueue := `[{"id":1,"type":"implement","target_files":["a.go"],"description":"do it","acceptance_criteria":["a.go exists"]}]`
	t.Run("second reply parses", func(t *testing.T) {
		env, agents, reviews, _, _ := stubSlimRun(t)
		agents.script = []agentStep{
			{result: activities.AgentRunResult{Text: twoSubtaskProsePlan, SessionID: "dev-1"}},
			{result: activities.AgentRunResult{Text: "a chatty reply, no array in sight.", SessionID: "dev-2"}},
			{result: activities.AgentRunResult{Text: oneSubtaskQueue, SessionID: "dev-3"}},
		}
		reviews.stub = []activities.ReviewResult{{Approved: true}}
		env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
			Return("daedalus/issue-42-1", nil).Once()
		if _, err := runSlim(t, env); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if len(agents.inputs) != 4 {
			t.Fatalf("agent ran %d times, want 4 (plan, parse, re-ask, step)", len(agents.inputs))
		}
		if got := agents.inputs[2].SessionID; got != "dev-2" {
			t.Errorf("re-ask SessionID = %q, want %q (resumed in the parse round's own conversation)", got, "dev-2")
		}
		retry := agents.inputs[2].Prompt
		if !strings.Contains(retry, "could not be parsed as the required JSON array") ||
			!strings.Contains(retry, twoSubtaskProsePlan) {
			t.Errorf("re-ask prompt should carry the parse failure under the parse prompt's plan, got:\n%s", retry)
		}
		if strings.Contains(retry, "PLANNER") {
			t.Errorf("re-ask prompt = the parse round's conversation, never a planner re-run, got:\n%s", retry)
		}
		if got := agents.inputs[3].SessionID; got != "dev-1" {
			t.Errorf("worker step SessionID = %q, want %q (the step resumes the plan's dev conversation — the parse round's session is abandoned once the queue parses)", got, "dev-1")
		}
	})
	t.Run("second reply parks", func(t *testing.T) {
		env, agents, reviews, suites, _ := stubSlimRun(t)
		garbage := activities.AgentRunResult{Text: "still just chatting."}
		agents.script = []agentStep{
			{result: activities.AgentRunResult{Text: twoSubtaskProsePlan}},
			{result: garbage}, {result: garbage},
		}
		err := runSlimErr(t, env)
		for _, want := range []string{ErrAwaitingMaintainer.Error(), "parse round could not produce a parseable subtask queue"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("park error %q should contain %q", err, want)
			}
		}
		if len(agents.inputs) != 3 {
			t.Errorf("agent ran %d times, want 3 (plan, parse, one re-ask)", len(agents.inputs))
		}
		if len(reviews.inputs) != 0 {
			t.Errorf("reviewer ran %d times, want 0 (nothing to review without a queue)", len(reviews.inputs))
		}
		if len(suites.runs) != 1 {
			t.Errorf("suite ran %d times, want 1 (preflight only)", len(suites.runs))
		}
	})
	t.Run("a targetless reply re-asks, and a second one parks", func(t *testing.T) {
		// The scope gate's parse contract routes through the same re-ask:
		// a reply that unmarshals fine but declares no target files spends
		// the re-ask with the specific failure quoted (the model can fix
		// exactly that), and a second one parks the run. The re-asked
		// parseSlimPlan call must carry the flag too — a retry that forgot
		// it would accept a targetless queue, whose acceptance then runs
		// the gate on an empty allowed set: a check that short-circuits
		// without an activity, silently fencing nothing.
		env, agents, reviews, suites, _ := stubSlimRun(t)
		targetless := `[{"id":1,"type":"implement","description":"do it","acceptance_criteria":["a.go exists"]}]`
		agents.script = []agentStep{
			{result: activities.AgentRunResult{Text: twoSubtaskProsePlan}},
			{result: activities.AgentRunResult{Text: targetless}},
			{result: activities.AgentRunResult{Text: targetless}},
		}
		err := runSlimErr(t, env)
		for _, want := range []string{
			ErrAwaitingMaintainer.Error(),
			"parse round could not produce a parseable subtask queue",
			"carries no target files",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("park error %q should contain %q", err, want)
			}
		}
		if len(agents.inputs) != 3 {
			t.Fatalf("agent ran %d times, want 3 (plan, parse, one re-ask — the targetless queue never ran)", len(agents.inputs))
		}
		if retry := agents.inputs[2].Prompt; !strings.Contains(retry, "carries no target files") || strings.Contains(retry, "PLANNER") {
			t.Errorf("re-ask prompt should carry the target-files failure inside the parse round's own conversation, got:\n%s", retry)
		}
		if len(reviews.inputs) != 0 {
			t.Errorf("reviewer ran %d times, want 0 (nothing to review without a queue)", len(reviews.inputs))
		}
		if len(suites.runs) != 1 {
			t.Errorf("suite ran %d times, want 1 (preflight only)", len(suites.runs))
		}
		// No AssertExpectations: the reviewer recorder's minimum-call
		// expectation can never be met on a park path that runs no review
		// (the "second reply parks" subtest above omits it for the same
		// reason).
	})
}

// TestSlimWorkflowParseRepair pins the closure repair's run-level surface:
// a parse reply whose every element was complete but whose final closers
// were dropped is closed, the queue runs to finalize, and the rescue is
// stamped on the run memo (ParseQueueRepaired) — durable and run-visible,
// not just logged. The routine comma strip is not a repair and stamps
// nothing, and a pre-marker replay (GetVersion pinned to the default)
// parses with the strip alone: the same closable reply fails there and
// spends the parse round's re-ask, exactly as the history that recorded it
// did.
func TestSlimWorkflowParseRepair(t *testing.T) {
	// oneClosableQueue is the complete one-subtask queue missing only its
	// final closers — the shape the closure repair exists for.
	oneClosableQueue := `[{"id":1,"type":"implement","target_files":["a.go"],"description":"do it","acceptance_criteria":["a.go exists"]`

	t.Run("closed queue finalizes and stamps the run memo", func(t *testing.T) {
		env, agents, reviews, _, _ := stubSlimRun(t)
		env.OnUpsertMemo(map[string]interface{}{"ParseQueueRepaired": true}).Return(nil).Once()
		agents.script = []agentStep{
			{result: activities.AgentRunResult{Text: twoSubtaskProsePlan, SessionID: "dev-1"}},
			{result: activities.AgentRunResult{Text: oneClosableQueue, SessionID: "dev-2"}},
			{result: activities.AgentRunResult{Text: "step one done", SessionID: "dev-3"}},
		}
		reviews.stub = []activities.ReviewResult{{Approved: true}}
		env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
			Return("daedalus/issue-42-1", nil).Once()

		branch, err := runSlim(t, env)
		if err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if branch != "daedalus/issue-42-1" {
			t.Errorf("branch = %q, want the finalize deliverable", branch)
		}
		if len(agents.inputs) != 3 {
			t.Fatalf("agent ran %d times, want 3 (plan, parse, step — the closed queue needed no re-ask)", len(agents.inputs))
		}
		// The memo assertion rides AssertExpectations: exactly one stamp,
		// exactly the ParseQueueRepaired map.
		env.AssertExpectations(t)
	})

	t.Run("the comma strip is not a repair", func(t *testing.T) {
		env, agents, reviews, _, _ := stubSlimRun(t)
		env.OnUpsertMemo(map[string]interface{}{"ParseQueueRepaired": true}).Never()
		agents.script = []agentStep{
			{result: activities.AgentRunResult{Text: twoSubtaskProsePlan, SessionID: "dev-1"}},
			{result: activities.AgentRunResult{Text: oneClosableQueue + "},]", SessionID: "dev-2"}},
			{result: activities.AgentRunResult{Text: "step one done", SessionID: "dev-3"}},
		}
		reviews.stub = []activities.ReviewResult{{Approved: true}}
		env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
			Return("daedalus/issue-42-1", nil).Once()

		if _, err := runSlim(t, env); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if len(agents.inputs) != 3 {
			t.Fatalf("agent ran %d times, want 3 (plan, parse, step — the comma strip absorbed the reply without a re-ask)", len(agents.inputs))
		}
		env.AssertExpectations(t)
	})

	t.Run("a pre-marker replay spends the re-ask", func(t *testing.T) {
		env, agents, reviews, _, scope := stubSlimRun(t)
		env.OnUpsertMemo(map[string]interface{}{"ParseQueueRepaired": true}).Never()
		env.OnGetVersion(slimParseRepairChangeID, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
		agents.script = []agentStep{
			{result: activities.AgentRunResult{Text: twoSubtaskProsePlan, SessionID: "dev-1"}},
			// The closable reply fails the gated-off parse...
			{result: activities.AgentRunResult{Text: oneClosableQueue, SessionID: "dev-2"}},
			// ...and the correction carries the closers.
			{result: activities.AgentRunResult{Text: oneClosableQueue + "}]", SessionID: "dev-3"}},
			{result: activities.AgentRunResult{Text: "step one done", SessionID: "dev-4"}},
		}
		reviews.stub = []activities.ReviewResult{{Approved: true}}
		env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
			Return("daedalus/issue-42-1", nil).Once()

		if _, err := runSlim(t, env); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if len(agents.inputs) != 4 {
			t.Fatalf("agent ran %d times, want 4 (plan, parse, re-ask, step — the closure must not fire on a pre-marker replay)", len(agents.inputs))
		}
		if got := agents.inputs[2].SessionID; got != "dev-2" {
			t.Errorf("re-ask SessionID = %q, want %q (resumed in the parse round's own conversation)", got, "dev-2")
		}
		if retry := agents.inputs[2].Prompt; !strings.Contains(retry, "could not be parsed as the required JSON array") {
			t.Errorf("re-ask prompt should carry the parse failure, got:\n%s", retry)
		}
		if got := agents.inputs[3].SessionID; got != "dev-1" {
			t.Errorf("worker step SessionID = %q, want %q (the step resumes the plan's dev conversation — the parse round's session is abandoned once the queue parses)", got, "dev-1")
		}
		// The two markers are independent: this replay pins only the parse
		// repair off, so the scope gate still fires at the acceptance of
		// the corrected queue.
		if len(scope.inputs) != 1 {
			t.Errorf("write-scope gate ran %d times, want 1 (the scope marker is independent of the parse-repair marker)", len(scope.inputs))
		}
		env.AssertExpectations(t)
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
	env.OnActivity(activities.RunJailedAgentActivity, mock.Anything, mock.Anything).
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

// TestSlimWorkflowContinuedPreflightGate pins the gate skip through the one
// flow that consumes the gate's return value: on a validated continue slim
// executes no baseline suite, and the resolved command still rides out of
// the skip — the run reaches its planner (never the suite-less park an
// empty command would trigger) and keeps the per-round ground truth the
// flow exists for. The feature-dev continue pins discard the return value,
// so a skip that stopped returning the command leaves them green while
// every continued slim run parks; the count here also catches a skip that
// regressed into executing the baseline.
func TestSlimWorkflowContinuedPreflightGate(t *testing.T) {
	env, agents, reviews, suites, _ := stubSlimRun(t)

	agents.script = []agentStep{
		{result: activities.AgentRunResult{Text: twoSubtaskProsePlan, SessionID: "dev-1"}},
		// The queue carries one sub-task: the loop converges after its
		// first green round and finalizes.
		{result: activities.AgentRunResult{Text: `[{"id":1,"type":"implement","target_files":["adder.go"],"description":"add the adder","acceptance_criteria":["adder.go defines Add","Add(2,2) returns 4"]}]`, SessionID: "dev-2"}},
		{result: activities.AgentRunResult{Text: "step one done", SessionID: "dev-3"}},
	}
	reviews.stub = []activities.ReviewResult{{Approved: true}}
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()

	in := baseInput()
	in.Flow = "slim"
	in.BaseBranch = "aborted/issue-42"
	in.BaselineValidated = true
	env.ExecuteWorkflow(SlimWorkflow, in)

	var branch string
	if err := env.GetWorkflowResult(&branch); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if branch != "daedalus/issue-42-1" {
		t.Errorf("branch = %q, want the finalize deliverable", branch)
	}
	if len(agents.inputs) == 0 {
		t.Fatalf("no agent rounds ran on a validated continue — the skip must open the planner session, not park")
	}
	if p := agents.inputs[0].Prompt; !strings.Contains(p, "PLANNER") {
		t.Errorf("first agent round prompt = %q, want the planner — the skip opens the session, it does not park the run", p)
	}
	if len(suites.runs) != 1 {
		t.Errorf("suite ran %d times, want 1 (the sub-task round's ground truth alone — no baseline execution on a validated continue)", len(suites.runs))
	}
	env.AssertExpectations(t)
}

// TestSlimWorkflowReviewHaltsParks pins the maintainer-halt path: a review
// that declares the sub-task impossible as stated parks the run with the
// sub-task's identity and the reviewer's reason, instead of looping.
func TestSlimWorkflowReviewHaltsParks(t *testing.T) {
	env, agents, reviews, _, _ := stubSlimRun(t)
	agents.script = []agentStep{
		{result: activities.AgentRunResult{Text: twoSubtaskProsePlan}},
		{result: activities.AgentRunResult{Text: `[{"id":1,"type":"implement","target_files":["a.go"],"description":"do it","acceptance_criteria":["a.go exists"]}]`}},
	}
	reviews.script = []reviewStep{{result: activities.ReviewResult{NeedsMaintainer: true, Comments: "the criteria contradict each other"}}}

	err := runSlimErr(t, env)
	for _, want := range []string{ErrAwaitingMaintainer.Error(), "slim subtask 1 review halted the run", "the criteria contradict each other"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("park error %q should contain %q", err, want)
		}
	}
	if len(agents.inputs) != 3 {
		t.Errorf("agent ran %d times, want 3 (plan, parse, step, no fix loop after the halt)", len(agents.inputs))
	}
	env.AssertExpectations(t)
}

// TestSlimWorkflowSubtaskRoundCapParks pins the per-sub-task convergence
// cap: a sub-task that cannot reach a green suite plus an approving review
// within maxSlimStepRounds ground-truth rounds parks the run — the suite
// and the reviewer both stay live through every round, and the counter
// counts rounds, not agent calls.
func TestSlimWorkflowSubtaskRoundCapParks(t *testing.T) {
	env, agents, reviews, suites, _ := stubSlimRun(t)
	agents.script = []agentStep{
		{result: activities.AgentRunResult{Text: twoSubtaskProsePlan}},
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
	// plan + parse + step + one fix per capped round.
	if len(agents.inputs) != 3+maxSlimStepRounds {
		t.Errorf("agent ran %d times, want %d (plan, parse, step, one fix per round)", len(agents.inputs), 3+maxSlimStepRounds)
	}
	env.AssertExpectations(t)
}

// TestSlimWorkflowScopeGateParks pins the hard write-scope gate's park: a
// worktree diff that leaves the sub-task's declared write scope parks the
// run at the acceptance point — the same point the suite and the reviewer
// just cleared — for the maintainer, with the offending paths named, and
// never spends a fix round on work the gate has already rejected. The suite
// stays in charge of ordering: a red round is never consulted (its fix loop
// runs first), so the gate only ever judges a diff that is already green
// and approved.
func TestSlimWorkflowScopeGateParks(t *testing.T) {
	violating := activities.ScopeResult{Violations: []string{"extra.go: outside this flow's allowed paths"}}
	t.Run("out-of-scope diff parks at the acceptance gate", func(t *testing.T) {
		env, agents, reviews, suites, scope := stubSlimRun(t)
		scope.res = violating
		agents.script = []agentStep{
			{result: activities.AgentRunResult{Text: twoSubtaskProsePlan}},
			{result: activities.AgentRunResult{Text: twoSubtaskPlan}},
			{result: activities.AgentRunResult{Text: "step one done"}},
		}
		reviews.script = []reviewStep{{result: activities.ReviewResult{Approved: true}}}
		suites.script = []suiteStep{
			{result: activities.TestResult{Passed: true, Logs: "ok"}},
			{result: activities.TestResult{Passed: true, Logs: "ok"}},
		}

		err := runSlimErr(t, env)
		for _, want := range []string{
			ErrAwaitingMaintainer.Error(),
			"slim subtask 1 left its write scope",
			"extra.go: outside this flow's allowed paths",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("park error %q should contain %q", err, want)
			}
		}
		if len(agents.inputs) != 3 {
			t.Errorf("agent ran %d times, want 3 (plan, parse, step — no fix round for work the gate has rejected)", len(agents.inputs))
		}
		if len(scope.inputs) != 1 {
			t.Errorf("write-scope gate ran %d times, want 1 (the first sub-task's acceptance)", len(scope.inputs))
		}
		if len(reviews.inputs) != 1 {
			t.Errorf("reviewer ran %d times, want 1 (the gate judges only after the reviewer approves)", len(reviews.inputs))
		}
		if len(suites.runs) != 2 {
			t.Errorf("suite ran %d times, want 2 (preflight + the green acceptance round)", len(suites.runs))
		}
		env.AssertExpectations(t)
	})

	t.Run("a red suite blocks ahead of the scope check", func(t *testing.T) {
		env, agents, reviews, suites, scope := stubSlimRun(t)
		scope.res = violating
		agents.script = []agentStep{
			{result: activities.AgentRunResult{Text: twoSubtaskProsePlan}},
			{result: activities.AgentRunResult{Text: twoSubtaskPlan}},
			{result: activities.AgentRunResult{Text: "step one done"}},
			{result: activities.AgentRunResult{Text: "fixed the failure"}},
		}
		reviews.script = []reviewStep{
			{result: activities.ReviewResult{Approved: true}},
			{result: activities.ReviewResult{Approved: true}},
		}
		suites.script = []suiteStep{
			{result: activities.TestResult{Passed: true, Logs: "ok"}},
			{result: activities.TestResult{Passed: false, Logs: "--- FAIL: TestAdd"}},
			{result: activities.TestResult{Passed: true, Logs: "ok"}},
		}

		err := runSlimErr(t, env)
		for _, want := range []string{ErrAwaitingMaintainer.Error(), "slim subtask 1 left its write scope", "extra.go"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("park error %q should contain %q", err, want)
			}
		}
		if len(agents.inputs) != 4 {
			t.Errorf("agent ran %d times, want 4 (plan, parse, step, fix — the red round ran its fix loop before the gate judged)", len(agents.inputs))
		}
		if len(scope.inputs) != 1 {
			t.Errorf("write-scope gate ran %d times, want 1 (only after the green round — the red round was never consulted)", len(scope.inputs))
		}
		env.AssertExpectations(t)
	})
}

// TestSlimWorkflowScopeGateRunPolicy pins the run-level policy
// interaction: the flow's own AllowedPaths ride the final sub-task's check
// — the one whose diff carries the whole run's accepted work — as an
// additive widening of the accepted union, never a second restriction on
// the declared targets. A policy narrower than the union cannot reject a
// target, and an overlapping one is simply absorbed into it. The frozen
// policy is the other half: it rides every check unchanged, mid-plan and
// final — the gate stacks target files onto the allowed set, it never
// lifts the run's frozen paths. The recorder answers clean on both runs,
// so the pin is the exact allowed and frozen sets the activity receives:
// the matching itself is VerifyWriteScopeActivity's own tested contract.
func TestSlimWorkflowScopeGateRunPolicy(t *testing.T) {
	run := func(t *testing.T, allowedPaths []string) *scopeRecorder {
		t.Helper()
		env, agents, reviews, _, scope := stubSlimRun(t)
		agents.script = []agentStep{
			{result: activities.AgentRunResult{Text: twoSubtaskProsePlan}},
			{result: activities.AgentRunResult{Text: twoSubtaskPlan}},
			{result: activities.AgentRunResult{Text: "step one done"}},
			{result: activities.AgentRunResult{Text: "step two done"}},
		}
		reviews.stub = []activities.ReviewResult{{Approved: true}}
		env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
			Return("daedalus/issue-42-1", nil).Once()
		in := baseInput()
		in.Flow = "slim"
		in.AllowedPaths = allowedPaths
		in.FrozenPaths = []string{".ai-jail/"}
		env.ExecuteWorkflow(SlimWorkflow, in)
		if err := env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		var branch string
		if err := env.GetWorkflowResult(&branch); err != nil {
			t.Fatalf("workflow result: %v", err)
		}
		return scope
	}

	t.Run("a policy narrower than the declared targets cannot reject them", func(t *testing.T) {
		scope := run(t, []string{"adder.go"})
		if len(scope.inputs) != 2 {
			t.Fatalf("write-scope gate ran %d times, want 2", len(scope.inputs))
		}
		want := []string{"adder.go", "mul.go", "adder.go"}
		if !reflect.DeepEqual(scope.inputs[1].Allowed, want) {
			t.Errorf("final scope check Allowed = %v, want %v (accepted union plus the run's policy, appended as declared)", scope.inputs[1].Allowed, want)
		}
		for i := range scope.inputs {
			if !reflect.DeepEqual(scope.inputs[i].Frozen, []string{".ai-jail/"}) {
				t.Errorf("scope check %d Frozen = %v, want the run's frozen policy riding every check, mid-plan and final", i, scope.inputs[i].Frozen)
			}
		}
	})

	t.Run("an overlapping policy widens the final union", func(t *testing.T) {
		scope := run(t, []string{"mul.go", "docs/extra.md"})
		if len(scope.inputs) != 2 {
			t.Fatalf("write-scope gate ran %d times, want 2", len(scope.inputs))
		}
		want := []string{"adder.go", "mul.go", "mul.go", "docs/extra.md"}
		if !reflect.DeepEqual(scope.inputs[1].Allowed, want) {
			t.Errorf("final scope check Allowed = %v, want %v (the run-level paths ride only the final check)", scope.inputs[1].Allowed, want)
		}
		if !reflect.DeepEqual(scope.inputs[0].Allowed, []string{"adder.go"}) {
			t.Errorf("first scope check Allowed = %v, want %v (the run-level policy never widens a mid-plan check)", scope.inputs[0].Allowed, []string{"adder.go"})
		}
	})
}

// TestSlimWorkflowScopeGateReplay pins the scope marker's replay side: a
// history recorded before the marker replays with the old parse contract —
// a queue without target files parses and runs without spending the re-ask
// — and without the acceptance gate's activity dispatch or its park. Both
// halves switch together (slimScopeChangeID), so the one pinned marker
// covers both.
func TestSlimWorkflowScopeGateReplay(t *testing.T) {
	env, agents, reviews, suites, scope := stubSlimRun(t)
	env.OnGetVersion(slimScopeChangeID, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
	// The queue the old contract accepted: no target_files at all.
	preMarkerQueue := `[{"id":1,"type":"implement","description":"do it","acceptance_criteria":["a.go exists"]}]`
	agents.script = []agentStep{
		{result: activities.AgentRunResult{Text: twoSubtaskProsePlan}},
		{result: activities.AgentRunResult{Text: preMarkerQueue}},
		{result: activities.AgentRunResult{Text: "step one done"}},
	}
	reviews.stub = []activities.ReviewResult{{Approved: true}}
	env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
		Return("daedalus/issue-42-1", nil).Once()

	branch, err := runSlim(t, env)
	if err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if branch != "daedalus/issue-42-1" {
		t.Errorf("branch = %q, want the finalize deliverable", branch)
	}
	if len(agents.inputs) != 3 {
		t.Errorf("agent ran %d times, want 3 (plan, parse, step — the pre-marker parse contract accepted the queue without a re-ask)", len(agents.inputs))
	}
	if len(scope.inputs) != 0 {
		t.Errorf("write-scope gate ran %d times, want 0 (a pre-marker replay never dispatches the gate's activity)", len(scope.inputs))
	}
	if len(suites.runs) != 2 {
		t.Errorf("suite ran %d times, want 2 (preflight + the accepted round — the suite gate predates this marker and stays on)", len(suites.runs))
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

// slimSalvageQueue is the parse round's reply transcribing the salvaged
// prose plan: one sub-task, so the loop converges after its first green
// round.
const slimSalvageQueue = `[{"id":1,"type":"implement","target_files":["adder.go"],"description":"add the adder","acceptance_criteria":["adder.go defines Add","Add(2,2) returns 4"]}]`

// TestSlimWorkflowNoDeliverySalvage pins the salvage seat: the planner round
// is the one stage armed with the salvage pointer, so a deliver-contract
// kill whose envelope carries the stranded plan feeds the prose to the parse
// round instead of parking — a plan round's deliverable IS the prose, and
// the kill did not damage it. The kill is never re-run, the salvage is
// stamped on the run memo (run-visible, where a maintainer answers "why did
// the parse round read salvaged prose?"), and the implementation rounds
// open the dev conversation cold — the killed round never returned a
// session to chain. A kill without a capture parks even under the seat:
// salvaging "" would only move the park one call frame.
func TestSlimWorkflowNoDeliverySalvage(t *testing.T) {
	t.Run("a captured kill feeds the parse round", func(t *testing.T) {
		env, agents, reviews, suites, _ := stubSlimRun(t)
		env.OnUpsertMemo(map[string]interface{}{"PlanProseSalvaged": true}).Return(nil).Once()
		agents.script = []agentStep{
			{err: errNoDeliveryStub},
			{result: activities.AgentRunResult{Text: slimSalvageQueue, SessionID: "dev-2"}},
			{result: activities.AgentRunResult{Text: "step one done", SessionID: "dev-3"}},
		}
		reviews.stub = []activities.ReviewResult{{Approved: true}}
		env.OnActivity(activities.FinalizeWorktreeActivity, mock.Anything, mock.Anything).
			Return("daedalus/issue-42-1", nil).Once()

		branch, err := runSlim(t, env)
		if err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if branch != "daedalus/issue-42-1" {
			t.Errorf("branch = %q, want the finalize deliverable", branch)
		}
		if len(agents.inputs) != 3 {
			t.Fatalf("agent ran %d times, want 3 (killed plan, parse, step — the kill is never re-run)", len(agents.inputs))
		}
		parse := agents.inputs[1]
		if got := parse.SessionID; got != "" {
			t.Errorf("parse SessionID = %q, want empty (the parse round always opens fresh)", got)
		}
		if p := parse.Prompt; !strings.Contains(p, slimSalvagePlanText) || strings.Contains(p, "PLANNER") {
			t.Errorf("parse prompt should carry the salvaged prose under the parse template, never a planner re-run, got:\n%s", p)
		}
		if got := agents.inputs[2].SessionID; got != "" {
			t.Errorf("step SessionID = %q, want empty — the killed round never returned a session, so the dev conversation opens cold", got)
		}
		if len(suites.runs) != 2 {
			t.Errorf("suite ran %d times, want 2 (preflight + the accepted round)", len(suites.runs))
		}
		// The memo assertion rides AssertExpectations: exactly one stamp,
		// exactly the PlanProseSalvaged map.
		env.AssertExpectations(t)
	})

	t.Run("a kill without a capture parks", func(t *testing.T) {
		env, agents, reviews, _, _ := stubSlimRun(t)
		env.OnUpsertMemo(map[string]interface{}{"PlanProseSalvaged": true}).Never()
		agents.script = []agentStep{
			{err: errNoDeliveryBareStub},
		}

		err := runSlimErr(t, env)
		for _, want := range []string{
			ErrAwaitingMaintainer.Error(),
			"planner round:",
			errNoDeliveryBareStub.Error(),
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("park error %q should contain %q", err, want)
			}
		}
		if len(agents.inputs) != 1 {
			t.Errorf("agent ran %d times, want 1 — the kill parks without spending a parse round on an empty salvage", len(agents.inputs))
		}
		if len(reviews.inputs) != 0 {
			t.Errorf("reviewer ran %d times, want 0 (nothing to review without a queue)", len(reviews.inputs))
		}
		// No AssertExpectations: the reviewer recorder's minimum-call
		// expectation can never be met on a park path that runs no review
		// (the slim parse-retry park tests omit it for the same reason).
	})
}
