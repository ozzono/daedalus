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
		"While iterating you may run only the tests you are working on, but know that the pipeline's final gate runs the COMPLETE test suite, not a subset — do not consider the round done until the full suite passes.\n\n" +
		"Bug policy — every bug you find, in the tests or anywhere you looked, is recorded twice: a file under backlog/bugs/ (trigger, impact, where it lives) and a note in Arete Memory. A bug in the tests you are writing is in scope: fix it. Everything else — implementation bugs the tests expose included — is documented only, never fixed here; report what you found in your reply.\n\n" +
		"Keep it lazy and minimal: test observable behavior, not implementation details. Cover the change's behavior, edge cases, and error paths with the fewest tests that genuinely verify them — no redundant happy-path duplicates, no speculative tests, no over-mocking."
	if got != want {
		t.Errorf("Tests = %q, want %q", got, want)
	}
}

func TestTestsFix(t *testing.T) {
	const failed = "Tests failed with output:\n\n--- FAIL: TestBoom\n\n" +
		"Fix the tests so they pass. Find the root cause first: if the tests assert implementation details, fix the tests; if the code is wrong, fix the code — shortest working change wins, and nothing else. Remember the final gate runs the COMPLETE test suite: verify the whole suite passes, not just the cases that were failing. Git writes are forbidden in this sandbox — edit files and leave the changes in the working tree; the pipeline commits approved work. Stay scoped: change only what the failure demands; anything else already differing in the worktree — sandbox or tooling artifacts such as .ai-jail — is unrelated, leave it untouched and ignore it."
	const review = "Test review feedback:\n\ncover the error path\n\n" +
		"Address the review comments. Changes stay test-scoped: touch only test code; if a comment seems to require an implementation change, say so in your reply instead of making it. Git writes are forbidden in this sandbox — edit files and leave the changes in the working tree; the pipeline commits approved work. Stay scoped: address only the comments about the tests; anything else already differing in the worktree — sandbox or tooling artifacts such as .ai-jail — is unrelated, leave it untouched and ignore it. Keep it lazy and minimal: the fewest tests that genuinely verify the behavior the comments name — no redundant or speculative tests, no over-mocking. Remember the final gate runs the COMPLETE test suite, not just the tests under discussion."

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
	got, err := Review("the implementation", "M foo.go", "", false)
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
		"Everything here — your review included — runs inside the same sandbox, and git writes are forbidden to every agent: never request a git operation (stage, commit, branch, restore) or a change to anything beyond the implementing agent's reach. Your scope is the diff above and the code it touches, nothing else: changes outside it — sandbox or tooling artifacts such as .ai-jail, environment files, unrelated worktree noise — are not part of this work; ignore them and never flag them, no matter how wrong they look. Every finding must be fixable by editing files in this worktree alone; anything that is not, is not a finding.\n\n" +
		"Tests are out of scope for this review. The test suite is written and reviewed in a separate phase after this one: missing, absent, or thin tests are not findings — do not request changes over test coverage. Judge only the implementation. (You may still build and run the existing suite to verify the change is sound.)\n\n" +
		"Bug policy — every bug you find, in the diff or anywhere you looked, is recorded twice: a file under backlog/bugs/ (trigger, impact, where it lives) and a note in Arete Memory. If it is in scope for this review, make it a finding and request changes. If it is out of scope, do not block approval over it — record it and add an alert about it in the docs.\n\n" +
		"End your response with a final line containing exactly APPROVED if it is acceptable as-is, " +
		"or CHANGES_REQUESTED if changes are required. Put all review comments above that final line."
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
	got, err := Review("the test suite", "A foo_test.go", "--- FAIL: TestBoom", true)
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
	if strings.Contains(got, "do not block approval") {
		t.Error("test review must not carry the dev-review bug policy")
	}
}
