package template

import (
	"strings"
	"testing"
)

func TestImplement(t *testing.T) {
	got, err := Implement("add the feature")
	if err != nil {
		t.Fatalf("Implement: %v", err)
	}
	want := "add the feature\n\n" +
		"Implement the change. Do not write, modify, or delete test code — the test suite is handled in a separate phase; leave every existing test file untouched.\n\n" +
		"Work style — the laziest solution that actually works:\n\n" +
		"- Question whether each piece needs to exist (YAGNI). Skip speculative generality, flags, and future-proofing.\n" +
		"- Reuse what already exists in this codebase; prefer the standard library over new dependencies — never add one for what a few lines of stdlib can do.\n" +
		"- Shortest working diff wins: one line before fifty, fewest files, no single-caller abstractions or pass-through wrappers.\n" +
		"- Fix the root cause, not the symptom — one guard where all callers route through beats a guard per caller.\n" +
		"- Never lazy about correctness: keep validation, error handling, edge cases, and cleanup intact. Mark a deliberate corner-cut with a `ponytail:` comment naming its ceiling."
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
		"Address the review comments. Do not write, modify, or delete test code — the test suite is handled in a separate phase; leave every existing test file untouched.\n\n" +
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
	want := "Write, fix, or improve the test suite covering the change in this repository. Run the tests and make sure they pass.\n\n" +
		"Keep it lazy and minimal: test observable behavior, not implementation details. Cover the change's behavior, edge cases, and error paths with the fewest tests that genuinely verify them — no redundant happy-path duplicates, no speculative tests, no over-mocking."
	if got != want {
		t.Errorf("Tests = %q, want %q", got, want)
	}
}

func TestTestsFix(t *testing.T) {
	const failed = "Tests failed with output:\n\n--- FAIL: TestBoom\n\n" +
		"Fix the tests so they pass. Find the root cause first: if the tests assert implementation details, fix the tests; if the code is wrong, fix the code. Shortest working change wins."
	const review = "Test review feedback:\n\ncover the error path\n\n" +
		"Address the review comments. Keep it lazy and minimal: the fewest tests that genuinely verify the behavior the comments name — no redundant or speculative tests, no over-mocking."

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
		"Tests are out of scope for this review. The test suite is written and reviewed in a separate phase after this one: missing, absent, or thin tests are not findings — do not request changes over test coverage. Judge only the implementation. (You may still build and run the existing suite to verify the change is sound.)\n\n" +
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
}
