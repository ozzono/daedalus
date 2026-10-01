// Package template holds the Markdown prompt templates that drive the
// pipeline's two review-gated loops — implementation ↔ code review, then
// tests ↔ test review. The templates are embedded at compile time, so the
// worker binary is self-contained while the prompts stay editable as plain
// Markdown under prompts/.
package template

import (
	"embed"
	"fmt"
	"strings"
	"text/template"
)

//go:embed prompts/*.md
var promptFiles embed.FS

// parsed holds every prompt, parsed once at startup. A malformed template is
// a packaging bug that must fail fast, not surface mid-workflow.
var parsed = template.Must(template.ParseFS(promptFiles, "prompts/*.md"))

// render executes the named template (without its .md suffix) and returns its
// output with surrounding whitespace trimmed.
func render(name string, data any) (string, error) {
	var b strings.Builder
	if err := parsed.ExecuteTemplate(&b, name+".md", data); err != nil {
		return "", fmt.Errorf("render prompt %q: %w", name, err)
	}
	return strings.TrimSpace(b.String()), nil
}

// TaskTouchesJail reports whether the maintainer-authored task text
// mentions .ai-jail — a deliberately coarse substring test, and the only
// input the jail carve-out may key on (a diff-keyed carve-out would let an
// out-of-scope round-1 edit to .ai-jail legitimize itself and unlock it for
// every later round — the agent deciding its own permissions). A mention is
// not a command: the carve-out branches are worded so a task that merely
// names .ai-jail (or forbids touching it) never orders edits — they only
// stop barring changes the task or the review comments actually ask for.
func TaskTouchesJail(task string) bool {
	return strings.Contains(task, ".ai-jail")
}

// Jail carries the reviewer-side jail carve-out: Touches marks a task whose
// own text names .ai-jail (switching the reviewer's scope clause from
// blanket-ignoring .ai-jail to auditing it), and Spec is the worktree's
// current .ai-jail content, relayed as its own labeled section — git never
// shows it (the repo ignores the file; the jail drops it untracked), so the
// audit mandate would have no data without it. The zero value keeps the
// default exclusion, byte-identical.
type Jail struct {
	Touches bool
	Spec    string
}

// Implement builds the phase-1 opener: the issue task, framed so the
// implementation phase excludes tests — the test suite gets its own reviewed
// phase. bugDir is the configured out-of-scope-bug filing folder; empty
// drops the file-filing instruction from the bug policy (the Arete Memory
// note and reply-reporting duty stay). A task that itself names .ai-jail
// gets the carve-out branch: editing .ai-jail is in scope instead of
// barred.
func Implement(task, bugDir string) (string, error) {
	return render("implement", struct {
		Task, BugDir string
		TouchesJail  bool
	}{task, bugDir, TaskTouchesJail(task)})
}

// Continue builds the phase-1 opener for a resumed run: the new task, the
// framing that the worktree already contains an aborted attempt's work, and
// that attempt's last review feedback when available.
func Continue(task, priorFeedback string) (string, error) {
	return render("continue", struct {
		Task          string
		PriorFeedback string
	}{task, priorFeedback})
}

// ImplementFix feeds code-review comments back to the implementing agent
// inside the phase-1 (implementation ↔ code review) loop. jail.Touches,
// when set, marks a task whose own text names .ai-jail: the fix prompt's
// scope clause then allows acting on .ai-jail comments. It trails as
// variadic so existing single-argument callers stay valid.
func ImplementFix(comments string, jail ...Jail) (string, error) {
	j := Jail{}
	if len(jail) > 0 {
		j = jail[0]
	}
	return render("implement_fix", struct {
		Comments    string
		TouchesJail bool
	}{comments, j.Touches})
}

// Tests opens phase 2: the agent writes or improves the test suite covering
// the change. bugDir is the configured out-of-scope-bug filing folder; empty
// drops the file-filing instruction from the bug policy.
func Tests(bugDir string) (string, error) {
	return render("tests", struct{ BugDir string }{bugDir})
}

// TestsFix feeds phase-2 (tests ↔ test review) failures back to the agent:
// the failing test output, the test-review comments, or both. Empty
// arguments are omitted.
func TestsFix(testLogs, comments string) (string, error) {
	var parts []string
	if testLogs != "" {
		p, err := render("tests_failed", struct{ Logs string }{testLogs})
		if err != nil {
			return "", err
		}
		parts = append(parts, p)
	}
	if comments != "" {
		p, err := render("tests_review", struct{ Comments string }{comments})
		if err != nil {
			return "", err
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, "\n\n"), nil
}

// Review builds the reviewer prompt for either loop. The verdict protocol at
// the end of the template is the contract parseReviewVerdict in
// internal/activities relies on: the reviewer's final non-empty line must be
// exactly APPROVED or CHANGES_REQUESTED (or, in the test review, REBUILD).
// testLogs is optional; when empty the test-output section is left out
// entirely. testsInScope switches the template's phase: the code reviewer
// (phase 1) is told test coverage is out of scope — tests get their own
// reviewed phase — while the test reviewer (phase 2) judges the suite itself
// and alone carries the REBUILD verdict. agentReply, when set, quotes the
// test agent's latest reply for the test reviewer to weigh. bugDir is the
// configured out-of-scope-bug filing folder; empty drops the file-filing
// instruction from the bug policy. For the bug-fix
// framing — the diff's own repro test in the deliverable — see ReviewRepro.
// jail, when set, marks a task whose own text names .ai-jail: the reviewer's
// scope clause then audits .ai-jail changes (with the spec content relayed)
// instead of blanket-ignoring them. It trails as variadic so existing
// callers stay valid.
func Review(focus, diff, testLogs string, testsInScope bool, agentReply, bugDir string, jail ...Jail) (string, error) {
	return review(focus, diff, testLogs, testsInScope, false, agentReply, bugDir, jail...)
}

// ReviewRepro is Review's bug-fix framing: the diff's own tests are part of
// its deliverable, and the reviewer must judge whether the repro actually
// captures the reported bug — something the repro-first gate cannot. No
// REBUILD verdict exists in this framing.
func ReviewRepro(focus, diff, testLogs, agentReply, bugDir string, jail ...Jail) (string, error) {
	return review(focus, diff, testLogs, false, true, agentReply, bugDir, jail...)
}

// review renders the shared reviewer template for all three framings.
func review(focus, diff, testLogs string, testsInScope, reproInScope bool, agentReply, bugDir string, jail ...Jail) (string, error) {
	j := Jail{}
	if len(jail) > 0 {
		j = jail[0]
	}
	return render("review", struct {
		Focus, Diff, TestLogs, AgentReply, BugDir, JailSpec string
		TestsInScope, ReproInScope, TouchesJail             bool
	}{
		Focus:        focus,
		Diff:         diff,
		TestLogs:     testLogs,
		TestsInScope: testsInScope,
		ReproInScope: reproInScope,
		AgentReply:   agentReply,
		BugDir:       bugDir,
		TouchesJail:  j.Touches,
		JailSpec:     j.Spec,
	})
}

// Rebuild feeds a test-review REBUILD finding back to the implementing
// agent: a tight, finding-only prompt — the change was already code- and
// test-reviewed once, so the template fences the agent against reworking
// anything the finding does not demand.
func Rebuild(finding string) (string, error) {
	return render("rebuild", struct{ Finding string }{finding})
}

// Investigate opens a docs-only investigation run: analysis and written
// deliverables, no code changes.
func Investigate(task string) (string, error) {
	return render("investigate", struct{ Task string }{task})
}

// InvestigateFix feeds docs-review comments back to the agent inside the
// investigation loop.
func InvestigateFix(comments string) (string, error) {
	return render("investigate_fix", struct{ Comments string }{comments})
}

// Refactor opens a behavior-frozen refactoring run: structure only, the
// existing suite green and untouched.
func Refactor(task string) (string, error) {
	return render("refactor", struct{ Task string }{task})
}

// RefactorFix feeds a refactoring round's failure back to the agent: the
// frozen suite's red output, the review comments, or both. Empty arguments
// are omitted.
func RefactorFix(testLogs, comments string) (string, error) {
	return render("refactor_fix", struct{ Logs, Comments string }{testLogs, comments})
}

// BugFix opens a test-first bug-fix run: the repro test and the minimal fix
// in one deliverable.
func BugFix(task string) (string, error) {
	return render("bugfix", struct{ Task string }{task})
}

// BugFixFix feeds a bug-fix round's failure back to the agent: the suite's
// red output (or the repro-first gate's refusal), the review comments, or
// both. Empty arguments are omitted.
func BugFixFix(testLogs, comments string) (string, error) {
	return render("bugfix_fix", struct{ Logs, Comments string }{testLogs, comments})
}
