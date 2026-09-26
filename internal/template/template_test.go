package template

import (
	"strings"
	"testing"
)

// TestContinue pins the resumed-run opener: continuation framing and the
// prior feedback section only when feedback exists. The template is
// phase-neutral — a continuation may resume the dev or the test loop, so it
// carries no test-scoping rule of its own.
func TestContinue(t *testing.T) {
	got, err := Continue("finish the feature", "finding 1\nfinding 2")
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	for _, want := range []string{
		"finish the feature",
		"continuing a previous attempt",
		"finding 1\nfinding 2",
		// The continuation opener carries the same impossibility escape
		// hatch as the fresh implement prompt: say what cannot be done so
		// the reviewer can halt for maintainer input.
		"Never silently loop over an impossible goal",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Continue = %q, want it to contain %q", got, want)
		}
	}
	if strings.Contains(got, "test code") {
		t.Errorf("Continue = %q, should not carry a test-scoping rule", got)
	}

	got, err = Continue("finish the feature", "")
	if err != nil {
		t.Fatalf("Continue (no feedback): %v", err)
	}
	if strings.Contains(got, "last review feedback") {
		t.Errorf("Continue = %q, feedback section should be omitted when empty", got)
	}
}

func TestImplement(t *testing.T) {
	got, err := Implement("add the feature")
	if err != nil {
		t.Fatalf("Implement: %v", err)
	}
	want := "add the feature\n\n" +
		"Implement the change. Do not write, modify, or delete test code; leave every existing test file untouched and pay it no attention.\n\n" +
		"If the task as stated cannot be completed — contradictory requirements, something missing from the worktree or outside your reach — say exactly what is impossible and why in your reply, then do the best sound partial work you can. Never silently loop over an impossible goal; your reply is what lets the reviewer halt the run for maintainer input instead of requesting changes forever.\n\n" +
		"You run sandboxed and git writes are forbidden to every agent: never stage, commit, branch, or restore. Edit files and leave the changes in the working tree — the pipeline commits your work for you once it is approved. Stay scoped: touch only what the task requires, and ignore anything already differing in the worktree that the task did not ask for — sandbox or tooling artifacts such as .ai-jail, environment files, unrelated noise. They are not yours; leave them untouched.\n\n" +
		"Work style — the laziest solution that actually works:\n\n" +
		"- Question whether each piece needs to exist (YAGNI). Skip speculative generality, flags, and future-proofing.\n" +
		"- Reuse what already exists in this codebase; prefer the standard library over new dependencies — never add one for what a few lines of stdlib can do.\n" +
		"- Shortest working diff wins: one line before fifty, fewest files, no single-caller abstractions or pass-through wrappers.\n" +
		"- Fix the root cause, not the symptom — one guard where all callers route through beats a guard per caller.\n" +
		"- Never lazy about correctness: keep validation, error handling, edge cases, and cleanup intact. Mark a deliberate corner-cut with a `ponytail:` comment naming its ceiling.\n\n" +
		"Bug policy — every bug you find, in your diff or anywhere you looked, is recorded twice: a file under backlog/bugs/ (trigger, impact, where it lives) and a note in Arete Memory. If it is in scope for this task, fix it now as part of the change. If it is out of scope, leave the code untouched — record it and add an alert about it in the docs."
	if got != want {
		t.Errorf("Implement = %q, want %q", got, want)
	}
}

func TestImplementFix(t *testing.T) {
	got, err := ImplementFix("rename foo to bar")
	if err != nil {
		t.Fatalf("ImplementFix: %v", err)
	}
	want := "Code review feedback on your implementation:\n\nrename foo to bar\n\n" +
		"Address the review comments. Do not write, modify, or delete test code; leave every existing test file untouched and pay it no attention.\n\n" +
		"Git writes are forbidden in this sandbox: never stage, commit, branch, or restore — edit files and leave the changes in the working tree; the pipeline commits approved work. Stay scoped: address only the comments about this task's changes; anything else already differing in the worktree — sandbox or tooling artifacts such as .ai-jail, environment files — is unrelated, leave it untouched and ignore it. A comment you cannot satisfy by editing files in this worktree is one to explain in your reply, not to force.\n\n" +
		"Keep it lazy and minimal: shortest working diff, reuse existing helpers, no new abstractions or dependencies unless the comments require them — and never simplify away validation, error handling, or edge cases."
	if got != want {
		t.Errorf("ImplementFix = %q, want %q", got, want)
	}
}

func TestTests(t *testing.T) {
	got, err := Tests()
	if err != nil {
		t.Fatalf("Tests: %v", err)
	}
	want := "Write, fix, or improve the test suite covering the change in this repository.\n\n" +
		"Changes in this round are test-scoped: write only test code and leave the implementation as-is — its current behavior is the contract the tests verify. If a test exposes an implementation bug, say so in your reply rather than changing the implementation.\n\n" +
		"You run sandboxed and git writes are forbidden to every agent: never stage, commit, branch, or restore. Edit files and leave the changes in the working tree — the pipeline commits your work for you once it is approved. Stay scoped: touch only test code, and ignore anything already differing in the worktree that this round did not ask for — sandbox or tooling artifacts such as .ai-jail, environment files, unrelated noise. They are not yours; leave them untouched.\n\n" +
		"While iterating, run only the tests covering the change — the test packages or files the change touched. Finish every round with the full suite: once the covering tests pass, run the whole suite, and the round is not done until it runs green — a known-red suite is not acceptable. Report the suite and its outcome in your reply: the pipeline runs the full suite again on the test worker and feeds that output to your reviewer, so your report is cross-checked, not taken on faith.\n\n" +
		"Never buy a green suite by gaming the tests: removing, skipping, obfuscating, or tweaking tests so they pass is invalid. Test changes follow code changes — a test may change only because the behavior it verifies legitimately changed, never to force a pass. When a test is right and the code is wrong, leave both alone and say so in your reply; the reviewer routes the fix back to the implementation.\n\n" +
		"Bug policy — every bug you find, in the tests or anywhere you looked, is recorded twice: a file under backlog/bugs/ (trigger, impact, where it lives) and a note in Arete Memory. A bug in the tests you are writing is in scope: fix it. Everything else — implementation bugs the tests expose included — is documented only, never fixed here; report what you found in your reply.\n\n" +
		"Keep it lazy and minimal: test observable behavior, not implementation details. Cover the change's behavior, edge cases, and error paths with the fewest tests that genuinely verify them — no redundant happy-path duplicates, no speculative tests, no over-mocking."
	if got != want {
		t.Errorf("Tests = %q, want %q", got, want)
	}
}

func TestTestsFix(t *testing.T) {
	const failed = "Tests failed with output:\n\n--- FAIL: TestBoom\n\n" +
		"Fix the tests so they pass. Find the root cause first: if the tests assert implementation details or are otherwise wrong, fix the tests; if the code is wrong, that fix is an implementation change this test-scoped round cannot make — say so in your reply instead of forcing the test green, and the reviewer can trigger a rebuild. Never make a test pass by removing, skipping, obfuscating, or tweaking it: test changes follow code changes — a test changes only because the behavior it verifies legitimately changed, never to force a pass. Full-suite testing belongs to the pipeline's gate and the maintainer: verify only that the tests in the packages or files this round's change touched pass; failures elsewhere in the suite are not yours — report them in your reply instead of chasing them. Git writes are forbidden in this sandbox — edit files and leave the changes in the working tree; the pipeline commits approved work. Stay scoped: change only what the failure demands; anything else already differing in the worktree — sandbox or tooling artifacts such as .ai-jail — is unrelated, leave it untouched and ignore it."
	const review = "Test review feedback:\n\ncover the error path\n\n" +
		"Address the review comments. Changes stay test-scoped: touch only test code; if a comment seems to require an implementation change, say so in your reply instead of making it. Never make a test pass by removing, skipping, obfuscating, or tweaking it: test changes follow code changes — a test changes only because the behavior it verifies legitimately changed, never to force a pass. Git writes are forbidden in this sandbox — edit files and leave the changes in the working tree; the pipeline commits approved work. Stay scoped: address only the comments about the tests; anything else already differing in the worktree — sandbox or tooling artifacts such as .ai-jail — is unrelated, leave it untouched and ignore it. Keep it lazy and minimal: the fewest tests that genuinely verify the behavior the comments name — no redundant or speculative tests, no over-mocking. Full-suite testing belongs to the pipeline's gate and the maintainer: judge and verify only the test packages and files the change touched."

	tests := []struct {
		name               string
		testLogs, comments string
		want               string
	}{
		{name: "both", testLogs: "--- FAIL: TestBoom", comments: "cover the error path", want: failed + "\n\n" + review},
		{name: "tests only", testLogs: "--- FAIL: TestBoom", want: failed},
		{name: "review only", comments: "cover the error path", want: review},
		{name: "neither", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TestsFix(tt.testLogs, tt.comments)
			if err != nil {
				t.Fatalf("TestsFix: %v", err)
			}
			if got != tt.want {
				t.Errorf("TestsFix = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReview(t *testing.T) {
	got, err := Review("the implementation", "M foo.go", "", false, "")
	if err != nil {
		t.Fatalf("Review: %v", err)
	}
	want := "You are a maximally rigorous reviewer. Review the implementation in this repository and let nothing pass.\n\n" +
		"The full diff of the change under review:\n\nM foo.go\n\n" +
		"Review with zero tolerance: read every changed line plus the surrounding code it touches, and verify every claim against the actual code — never trust comments, names, or the author's intent. " +
		"Hunt for: incorrect edge cases (empty, nil, zero, off-by-one, overflow); error paths and cleanup skipped on failure branches; docs or names that contradict behavior; mishandled cancellation, timeouts, and concurrency; " +
		"trust-boundary breaches (path traversal, injection, secrets leaking into logs, argv, or history); " +
		"and bloat — speculative abstractions, dead code, needless dependencies, diffs wider than the task. " +
		"When uncertain, request changes and state exactly what must be verified; approve only what you have checked in full.\n\n" +
		"Everything here — your review included — runs inside the same sandbox, and git writes are forbidden to every agent: never request a git operation (stage, commit, branch, restore) or a change to anything beyond the implementing agent's reach. Your scope is the diff above and the code it touches, nothing else: changes outside it — sandbox or tooling artifacts such as .ai-jail, environment files, unrelated worktree noise — are not part of this work; ignore them and never flag them, no matter how wrong they look. Every finding must be fixable by editing files in this worktree alone; anything that is not, is not a finding — unless it makes the task itself impossible to complete as stated, which is the one case where you halt instead (see NEEDS_MAINTAINER below).\n\n" +
		"Tests are out of scope for this review. The test suite is written and reviewed in a separate phase after this one: missing, absent, or thin tests are not findings — do not request changes over test coverage. Judge only the implementation. (You may still build and run the existing suite to verify the change is sound.)\n\n" +
		"Bug policy — every bug you find, in the diff or anywhere you looked, is recorded twice: a file under backlog/bugs/ (trigger, impact, where it lives) and a note in Arete Memory. If it is in scope for this review, make it a finding and request changes. If it is out of scope, do not block approval over it — record it and add an alert about it in the docs.\n\n" +
		"End your response with a final line containing exactly APPROVED if it is acceptable as-is, " +
		"CHANGES_REQUESTED if changes are required, " +
		"or NEEDS_MAINTAINER if the task as stated cannot be completed by editing files in this worktree alone — " +
		"contradictory or impossible requirements, a missing dependency or resource outside the worktree, " +
		"a constraint only a human can lift. NEEDS_MAINTAINER stops the pipeline and waits for the maintainer: " +
		"use it whenever you have good reason to believe the implementing agent can never satisfy what you would " +
		"otherwise request — an endless fix loop is a worse outcome than a halt — but never as an escape from " +
		"ordinary hard work that is merely difficult. Above the verdict line, state exactly what the maintainer " +
		"must decide, provide, or relax. Put all review comments above that final line."
	if got != want {
		t.Errorf("Review = %q, want %q", got, want)
	}
	if strings.Contains(got, "Latest test run output") {
		t.Error("Review should omit the test-output section when no logs are given")
	}
	if strings.Contains(got, "tests that restate") {
		t.Error("code review should not carry the test-quality hunt criterion")
	}
}

func TestReviewWithTestLogs(t *testing.T) {
	got, err := Review("the test suite", "A foo_test.go", "--- FAIL: TestBoom", true, "")
	if err != nil {
		t.Fatalf("Review: %v", err)
	}
	if !strings.Contains(got, "Latest test run output:\n\n--- FAIL: TestBoom") {
		t.Errorf("Review = %q, want the test-output section after the diff", got)
	}
	if !strings.Contains(got, "tests that restate the code instead of exercising their failure modes") {
		t.Error("test review should carry the test-quality hunt criterion")
	}
	if strings.Contains(got, "Tests are out of scope") {
		t.Error("test review must not declare tests out of scope")
	}
	if !strings.Contains(got, "Only a bug in the tests under review is a finding") {
		t.Error("test review should carry the test-scoped bug policy")
	}
	if !strings.Contains(got, "Your scope is the test packages and files the change touched") {
		t.Error("test review should scope the reviewer to the changed test packages and files")
	}
	if strings.Contains(got, "do not block approval") {
		t.Error("test review must not carry the dev-review bug policy")
	}
	if !strings.Contains(got, "A fourth verdict exists in this phase only") {
		t.Error("test review should document the REBUILD verdict")
	}
	// The full-suite gate: red output is never approvable, gaming the tests
	// is a finding, and test changes must follow code changes.
	for _, want := range []string{
		"test failures cannot be accepted",
		"a suite that is not green is never APPROVED",
		"removed, skipped, renamed out of run, obfuscated, or tweaked",
		"Test changes follow code changes",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Review = %q, want it to contain %q", got, want)
		}
	}
}

// TestReviewWithAgentReply pins the tester-relay section: only the test
// review carries the test agent's latest reply, and it frames the relay as
// a request the reviewer verifies rather than obeys.
func TestReviewWithAgentReply(t *testing.T) {
	got, err := Review("the test suite", "A foo_test.go", "", true, "the handler fix is outside my test-only scope — please rebuild")
	if err != nil {
		t.Fatalf("Review: %v", err)
	}
	if !strings.Contains(got, "The test agent's latest reply:\n\nthe handler fix is outside my test-only scope — please rebuild") {
		t.Errorf("Review = %q, want the quoted agent reply section", got)
	}
	if !strings.Contains(got, "verdict REBUILD (below)") {
		t.Errorf("Review = %q, want the relay framing that lets the reviewer trigger the rebuild", got)
	}

	phase1, err := Review("the implementation", "M foo.go", "", false, "please rebuild")
	if err != nil {
		t.Fatalf("Review: %v", err)
	}
	if strings.Contains(phase1, "The test agent's latest reply") {
		t.Error("code review must not carry the agent-relay section")
	}
	if strings.Contains(phase1, "REBUILD") {
		t.Error("code review must not offer the REBUILD verdict")
	}
}

// TestRebuild pins the tight-context rebuild prompt: the finding is the
// payload, and the prompt fences the agent against rework — approved
// decisions stay untouched and test files stay out of bounds.
func TestRebuild(t *testing.T) {
	got, err := Rebuild("handler drops the error path")
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if !strings.Contains(got, "REBUILD — the test phase's review found that the work needs an implementation change:\n\nhandler drops the error path") {
		t.Errorf("Rebuild = %q, want the finding as the payload", got)
	}
	if !strings.Contains(got, "Change only what the finding demands") {
		t.Error("Rebuild should fence against rework of approved decisions")
	}
	if !strings.Contains(got, "never touch the test files") {
		t.Error("Rebuild should keep test files out of the rebuild scope")
	}
}

// TestInvestigateAndFix pins the docs-only flow's prompt pair: the opener
// scopes the run to documentation with a structural enforcement backstop
// and an impossibility escape hatch, and the fix prompt carries the review
// comments under the same docs-only fence.
func TestInvestigateAndFix(t *testing.T) {
	got, err := Investigate("map the worker restart paths")
	if err != nil {
		t.Fatalf("Investigate: %v", err)
	}
	for _, want := range []string{
		"map the worker restart paths",
		"Only documentation may be created or changed",
		"the pipeline verifies this structurally and refuses a diff that leaves the docs scope",
		"If the deliverable is not expressible as documentation, say so in your reply",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Investigate = %q, want it to contain %q", got, want)
		}
	}
	if strings.Contains(got, "test code") {
		t.Errorf("Investigate = %q, the docs scope already bars tests; no test-scoping rule expected", got)
	}

	got, err = InvestigateFix("cite the files backing each claim")
	if err != nil {
		t.Fatalf("InvestigateFix: %v", err)
	}
	for _, want := range []string{
		"Documentation review feedback:",
		"cite the files backing each claim",
		"production code and tests stay untouched",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("InvestigateFix = %q, want it to contain %q", got, want)
		}
	}
}

// TestRefactorAndFix pins the behavior-frozen flow's prompt pair: the
// opener freezes behavior and tests with a full-suite gate, and the fix
// prompt carries whichever of the red suite output and review comments
// failed the round — omitted when empty.
func TestRefactorAndFix(t *testing.T) {
	got, err := Refactor("extract the parser")
	if err != nil {
		t.Fatalf("Refactor: %v", err)
	}
	for _, want := range []string{
		"extract the parser",
		"the existing test suite is the contract and must stay green, untouched",
		"Do not write, modify, or delete test code or testdata",
		"If you find a bug, record it in your reply instead of fixing it",
		"the round is not done until it runs green with the tests exactly as you found them",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Refactor = %q, want it to contain %q", got, want)
		}
	}

	const logsHeader = "The frozen test suite failed under the refactoring:"
	const commentsHeader = "Code review feedback on the refactoring:"
	const tail = "Changes stay structure-only — behavior frozen, tests and testdata untouched"
	for _, tt := range []struct {
		name               string
		testLogs, comments string
		want               []string
		notWant            []string
	}{
		{name: "both", testLogs: "--- FAIL: TestParse", comments: "flatten the nesting",
			want: []string{logsHeader, "--- FAIL: TestParse", commentsHeader, "flatten the nesting", tail,
				"find the behavior the refactoring changed and fix the code, never the test"}},
		{name: "logs only", testLogs: "--- FAIL: TestParse",
			want:    []string{logsHeader, "--- FAIL: TestParse", tail},
			notWant: []string{commentsHeader}},
		{name: "comments only", comments: "flatten the nesting",
			want:    []string{commentsHeader, "flatten the nesting", tail},
			notWant: []string{logsHeader}},
		{name: "neither", want: []string{tail}, notWant: []string{logsHeader, commentsHeader}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RefactorFix(tt.testLogs, tt.comments)
			if err != nil {
				t.Fatalf("RefactorFix: %v", err)
			}
			for _, want := range tt.want {
				if !strings.Contains(got, want) {
					t.Errorf("RefactorFix = %q, want it to contain %q", got, want)
				}
			}
			for _, notWant := range tt.notWant {
				if strings.Contains(got, notWant) {
					t.Errorf("RefactorFix = %q, want %q omitted", got, notWant)
				}
			}
		})
	}
}

// TestBugFixAndFix pins the test-first flow's prompt pair: the opener
// makes the repro test part of the deliverable and names the repro-first
// gate, and the fix prompt carries the red output (or the gate's refusal)
// and review comments, omitted when empty.
func TestBugFixAndFix(t *testing.T) {
	got, err := BugFix("login panics on an empty email")
	if err != nil {
		t.Fatalf("BugFix: %v", err)
	}
	for _, want := range []string{
		"login panics on an empty email",
		"write the test that reproduces it",
		"The repro test is part of the deliverable",
		"the pipeline runs the diff's tests against the pre-fix code",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("BugFix = %q, want it to contain %q", got, want)
		}
	}

	const logsHeader = "The test suite failed:"
	const commentsHeader = "Code review feedback on the fix:"
	const tail = "a repro test that fails on the pre-fix code, the minimal fix, and a green full suite"
	for _, tt := range []struct {
		name               string
		testLogs, comments string
		want               []string
		notWant            []string
	}{
		{name: "both", testLogs: "--- FAIL: TestLogin", comments: "shrink the diff",
			want: []string{logsHeader, "--- FAIL: TestLogin", commentsHeader, "shrink the diff", tail}},
		{name: "gate refusal only", testLogs: "REPRO-FIRST GATE FAILED: no test files",
			want:    []string{logsHeader, "REPRO-FIRST GATE FAILED: no test files", tail},
			notWant: []string{commentsHeader}},
		{name: "comments only", comments: "shrink the diff",
			want:    []string{commentsHeader, "shrink the diff", tail},
			notWant: []string{logsHeader}},
		{name: "neither", want: []string{tail}, notWant: []string{logsHeader, commentsHeader}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BugFixFix(tt.testLogs, tt.comments)
			if err != nil {
				t.Fatalf("BugFixFix: %v", err)
			}
			for _, want := range tt.want {
				if !strings.Contains(got, want) {
					t.Errorf("BugFixFix = %q, want it to contain %q", got, want)
				}
			}
			for _, notWant := range tt.notWant {
				if strings.Contains(got, notWant) {
					t.Errorf("BugFixFix = %q, want %q omitted", got, notWant)
				}
			}
		})
	}
}

// TestReviewRepro pins the bug-fix review framing: the diff's own tests are
// part of the deliverable, the reviewer alone judges whether the repro
// captures the reported bug, both the implementation and its test are in
// scope for findings, and the framing carries no REBUILD verdict and no
// tests-out-of-scope clause.
func TestReviewRepro(t *testing.T) {
	got, err := ReviewRepro("the bug fix and its reproducing test", "M login.go", "--- FAIL: TestLogin", "")
	if err != nil {
		t.Fatalf("ReviewRepro: %v", err)
	}
	for _, want := range []string{
		"Review the bug fix and its reproducing test",
		"Latest test run output:\n\n--- FAIL: TestLogin",
		"the diff must carry a test that reproduces the reported bug",
		"you alone judge whether it really does",
		"Here both the implementation and its test are in scope",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("ReviewRepro = %q, want it to contain %q", got, want)
		}
	}
	if strings.Contains(got, "REBUILD") {
		t.Error("ReviewRepro must not offer the REBUILD verdict")
	}
	if strings.Contains(got, "Tests are out of scope") {
		t.Error("ReviewRepro must not declare tests out of scope")
	}
	if strings.Contains(got, "The test agent's latest reply") {
		t.Error("ReviewRepro must not carry the tester-relay section when no reply is given")
	}
}
