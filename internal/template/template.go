// Package template holds the Markdown prompt templates that drive the
// pipeline's two review-gated loops — implementation ↔ code review, then
// tests ↔ test review. The templates are embedded at compile time, so the
// worker binary is self-contained while the prompts stay editable as plain
// Markdown under prompts/.
package template

import (
	"embed"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"text/template/parse"
)

//go:embed prompts/*.md
var promptFiles embed.FS

// parsed holds every prompt, parsed once at startup. A malformed template is
// a packaging bug that must fail fast, not surface mid-workflow.
var parsed = template.Must(template.ParseFS(promptFiles, "prompts/*.md"))

// installed holds the configured prompt overrides, keyed by prompt name.
// LoadOverrides writes it once at worker startup, before any poller can
// render, and everything afterwards only reads it — no lock. Nil (the
// default deployment) leaves every render byte-identical to the embedded
// template.
var installed map[string]*template.Template

// Prompts lists the overridable prompt names — the embedded prompts/*.md
// file stems — sorted. This is the exact vocabulary a config's prompt
// section keys on.
func Prompts() []string {
	var names []string
	for _, t := range parsed.Templates() {
		if n, ok := strings.CutSuffix(t.Name(), ".md"); ok {
			names = append(names, n)
		}
	}
	slices.Sort(names)
	return names
}

// PromptSpec describes one overridable prompt: its name (the override
// file's stem), its embedded source (the canonical sample), and the data
// fields its template references — the exact field vocabulary an override
// may use, since checkOverride rejects anything else at worker startup.
type PromptSpec struct {
	Name   string
	Source string
	Fields []string
}

// PromptSpecs returns every overridable prompt, sorted by name. Built for
// `daedalus init prompt <dir>`: the scaffold emits each Source as
// <Name>.md and lists Fields per prompt in its guide.
func PromptSpecs() ([]PromptSpec, error) {
	names := Prompts()
	specs := make([]PromptSpec, 0, len(names))
	for _, name := range names {
		src, err := promptFiles.ReadFile("prompts/" + name + ".md")
		if err != nil {
			return nil, fmt.Errorf("prompt overrides: embedded source for %s: %w", name, err)
		}
		allowed, err := dataFields(parsed.Lookup(name + ".md").Tree.Root)
		if err != nil {
			return nil, fmt.Errorf("prompt overrides: embedded prompt %s: %w", name, err)
		}
		specs = append(specs, PromptSpec{
			Name:   name,
			Source: string(src),
			Fields: slices.Sorted(maps.Keys(allowed)),
		})
	}
	return specs, nil
}

// LoadOverrides installs the per-deployment prompt replacements: dir holds
// one <prompt-name>.md file per replaced prompt, every stem naming a prompt
// (see Prompts). The path is absolute, ~/…, or relative to baseDir (the
// config file's directory, so a deployment's prompts travel with its
// config). Every failure — an unknown stem, a missing directory, an
// unreadable file, a template that does not parse, a data-field reference
// the prompt's data does not carry, a {{template}} action, a
// {{define}}/{{block}} block, a review override that dropped the verdict
// protocol — errors here, at worker startup, instead of surfacing as a
// broken prompt mid-round. Overrides
// resolve once per process like the embedded set itself: rendered prompts
// are recorded in workflow history, so a mid-run edit plus worker restart
// is the same divergence class as upgrading the binary mid-run, which the
// pipeline already accepts.
func LoadOverrides(dir, baseDir string) error {
	if dir == "" {
		return nil
	}
	if dir == "~" || strings.HasPrefix(dir, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("prompt overrides: resolve home directory: %w", err)
		}
		dir = filepath.Join(home, strings.TrimPrefix(dir, "~"))
	} else if !filepath.IsAbs(dir) {
		dir = filepath.Join(baseDir, dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("prompt overrides: read %s: %w", dir, err)
	}
	// An empty directory is the unset case — no overrides, and no wiping of
	// anything previously installed in this process.
	if len(entries) == 0 {
		return nil
	}
	loaded := make(map[string]*template.Template, len(entries))
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".md")
		if !ok {
			continue
		}
		emb := parsed.Lookup(name + ".md")
		if emb == nil {
			return fmt.Errorf("prompt overrides: %s does not name a prompt (available: %s)", e.Name(), strings.Join(Prompts(), ", "))
		}
		path := filepath.Join(dir, e.Name())
		src, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("prompt overrides: read %s: %w", path, err)
		}
		// An empty replacement would install an empty prompt — every round
		// of that kind would launch the jailed agent with nothing (the
		// review override is the only one the verdict-word check happens to
		// catch). An empty or whitespace-only source parses cleanly with no
		// field references, so the check is on the source text.
		if strings.TrimSpace(string(src)) == "" {
			return fmt.Errorf("prompt overrides: %s is empty — a replacement prompt must have content", path)
		}
		ov, err := template.New(name + ".md").Parse(string(src))
		if err != nil {
			return fmt.Errorf("prompt overrides: parse %s: %w", path, err)
		}
		// A {{define}} body is dead content in an override: the parser
		// hoists it out of the root tree, where it would dodge the field
		// validation below and render nothing, and the only action that
		// could invoke it ({{template}}) is rejected. A root-level {{block}}
		// desugars to the same shape (a defined template plus a
		// {{template}} invocation), so the same check catches it — the
		// diagnostic names both spellings, since the source may carry
		// neither the word "define". A define whose name equals the file's
		// own template name is the one form that renders: the parser makes
		// it the root tree, so the set stays size 1 and every check below
		// sees the real content.
		if len(ov.Templates()) > 1 {
			return fmt.Errorf("prompt overrides: %s contains a {{define}}/{{block}} block — a define body can never render in an override; put the content in the file body", path)
		}
		if err := checkOverride(name, emb, ov); err != nil {
			return err
		}
		// Startup render against the prompt's representative data: a
		// reference that parses and names a real first-level field can
		// still fail at render time — a nested access (.Handoff.Path) or
		// one a guard no longer shields (the override stripped the
		// {{if}}) errors only here, never again mid-round. review renders
		// three shapes: the zero value (the framing every non-handoff
		// round actually renders), a non-nil Handoff (the guarded branch
		// of the embedded shape — an override with a broken access behind
		// a surviving {{if .Handoff}} fails here instead of on the first
		// oversized diff), and the test-review framing the verdict check
		// below reads.
		if _, err := renderOverride(ov, representativeData[name]); err != nil {
			return fmt.Errorf("prompt overrides: %s: %w", path, err)
		}
		if name == "review" {
			if _, err := renderOverride(ov, reviewData{Handoff: &DiffHandoff{}}); err != nil {
				return fmt.Errorf("prompt overrides: %s (with a diff handoff attached): %w", path, err)
			}
			// The wholesale-replacement guard runs against the test-review
			// framing, the one shape that renders all four words: REBUILD
			// exists only inside {{if .TestsInScope}}, so the zero value
			// carries three of the four and a byte-verbatim copy of the
			// embedded prompt — exactly what "daedalus init prompt"
			// scaffolds — would fail a zero-value check
			// (backlog/bugs/review-override-verdict-guard-rejects-verbatim-copy.md).
			// A dropped word — or one carried only where it never renders —
			// would silently break every review loop, and each round would
			// burn a NoVerdict strike. Whole-word matching, so DISAPPROVED
			// cannot vouch for APPROVED.
			framed, err := renderOverride(ov, reviewData{TestsInScope: true})
			if err != nil {
				return fmt.Errorf("prompt overrides: %s (in the test-review framing): %w", path, err)
			}
			for i, v := range reviewVerdicts {
				if !reviewVerdictRes[i].MatchString(framed) {
					return fmt.Errorf("prompt overrides: review.md is missing %q — the verdict protocol is the machine contract the review loop parses (the reviewer's final line must be exactly one of %s); keep all four verdict words in the rendered text", v, strings.Join(reviewVerdicts, " / "))
				}
			}
		}
		loaded[name] = ov
	}
	installed = loaded
	return nil
}

// reviewVerdicts is the verdict protocol of review.md — the machine
// contract parseReviewVerdict in internal/activities reads (the reviewer's
// final non-empty line must be exactly one of these). REBUILD is offered
// to the test reviewer only, but the same template serves both review
// loops, so a replacement must carry all four even in a deployment that
// believes it will never run a test phase.
var reviewVerdicts = []string{"APPROVED", "CHANGES_REQUESTED", "NEEDS_MAINTAINER", "REBUILD"}

// reviewVerdictRes mirrors reviewVerdicts with whole-word matchers for the
// rendered-text guard: \b treats the tail of DISAPPROVED as inside another
// word, so a substring hit cannot vouch for a verdict the prompt never
// states.
var reviewVerdictRes = func() []*regexp.Regexp {
	res := make([]*regexp.Regexp, len(reviewVerdicts))
	for i, w := range reviewVerdicts {
		res[i] = regexp.MustCompile(`\b` + w + `\b`)
	}
	return res
}()

// renderOverride executes an override against data — the startup
// validation's render. It is render's execute-without-install twin: the
// installed set is not written yet, and the output is checked, not kept.
func renderOverride(ov *template.Template, data any) (string, error) {
	var b strings.Builder
	if err := ov.Execute(&b, data); err != nil {
		return "", fmt.Errorf("does not render against the prompt's data: %w", err)
	}
	return b.String(), nil
}

// representativeData maps each prompt to the data its startup render runs
// against — the zero value of the prompt's data struct, defined right next
// to every struct so a new prompt cannot be added without being on the
// list (a missing key renders against nil and misses field errors).
var representativeData = map[string]any{
	"bugfix":           bugfixData{},
	"bugfix_fix":       bugfixFixData{},
	"continue":         continueData{},
	"implement":        implementData{},
	"implement_fix":    implementFixData{},
	"investigate":      investigateData{},
	"investigate_fix":  investigateFixData{},
	"rebuild":          rebuildData{},
	"refactor":         refactorData{},
	"refactor_fix":     refactorFixData{},
	"review":           reviewData{},
	"slim_fix":         slimFixData{},
	"slim_parse":       slimParseData{},
	"slim_parse_reask": slimParseReaskData{},
	"slim_plan":        slimPlanData{},
	"slim_step":        slimStepData{},
	"tests":            testsData{},
	"tests_failed":     testsFailedData{},
	"tests_review":     testsReviewData{},
}

// checkOverride is the cheap structural pre-filter of a parsed override
// against the embedded prompt it replaces: its data-field references must
// stay within the embedded template's, and it must not use {{template}} —
// an override parses alone, so a reference could only fail (or recurse
// into itself) at render time. LoadOverrides' startup render is the real
// gate; this check stays first because its error names the offending field
// without needing any data.
func checkOverride(name string, emb, ov *template.Template) error {
	allowed, err := dataFields(emb.Tree.Root)
	if err != nil {
		return err
	}
	referenced, err := dataFields(ov.Tree.Root)
	if err != nil {
		return fmt.Errorf("prompt override %q: %w", name, err)
	}
	var unknown []string
	for _, f := range slices.Sorted(maps.Keys(referenced)) {
		if !allowed[f] {
			unknown = append(unknown, f)
		}
	}
	if len(unknown) > 0 {
		return fmt.Errorf("prompt override %q: references data field(s) %s, which the prompt's data does not carry (its fields: %s)",
			name, quotedList(unknown), quotedList(slices.Sorted(maps.Keys(allowed))))
	}
	return nil
}

// dataFields walks a parsed template collecting every root-level data-field
// name it references — the X of each .X and .X.Y, and of each $.X and
// $.X.Y (the $ root is the render data, so those are data lookups exactly
// like .X; the parser folds them into a VariableNode, not a ChainNode).
// Other variables are skipped: $x and $x.Y carry values, not data lookups.
// {{template}} actions are rejected — overrides run alone. The walk is
// scope-flat by design: a field read off a range or with variable
// ({{range .Xs}}{{.Typo}}{{end}}) is collected as a root field and checked
// against the root's data — ponytail: that can reject a legitimate
// override, and the error names the fix (write it as .Xs.Typo). Tracking
// dot's real type through range/with is a typechecker's job; LoadOverrides'
// startup render exercises references at their real scope instead, so this
// walk stays first-level-only on purpose.
func dataFields(n parse.Node) (map[string]bool, error) {
	fields := map[string]bool{}
	var walk func(parse.Node) error
	walkBranch := func(b parse.BranchNode) error {
		if err := walk(b.Pipe); err != nil {
			return err
		}
		return errors.Join(walk(b.List), walk(b.ElseList))
	}
	walk = func(n parse.Node) error {
		switch n := n.(type) {
		case *parse.ListNode:
			if n == nil {
				return nil
			}
			for _, c := range n.Nodes {
				if err := walk(c); err != nil {
					return err
				}
			}
		case *parse.ActionNode:
			return walk(n.Pipe)
		case *parse.IfNode:
			return walkBranch(n.BranchNode)
		case *parse.RangeNode:
			return walkBranch(n.BranchNode)
		case *parse.WithNode:
			return walkBranch(n.BranchNode)
		case *parse.PipeNode:
			for _, c := range n.Cmds {
				if err := walk(c); err != nil {
					return err
				}
			}
		case *parse.CommandNode:
			for _, a := range n.Args {
				if err := walk(a); err != nil {
					return err
				}
			}
		case *parse.FieldNode:
			fields[n.Ident[0]] = true
		case *parse.ChainNode:
			return walk(n.Node)
		case *parse.VariableNode:
			// Only the $ root names the render data: $.X validates like
			// .X, while $x and $x.Y reference a declared variable, not the
			// data. ({{$.X}} parses to Ident ["$" "X"]; {{$x.Y}} to
			// ["$x" "Y"].)
			if len(n.Ident) > 1 && n.Ident[0] == "$" {
				fields[n.Ident[1]] = true
			}
		case *parse.TemplateNode:
			return fmt.Errorf("{{template %q}}: overrides run alone and may not reference other templates", n.Name)
		}
		return nil
	}
	return fields, walk(n)
}

// quotedList renders names as a comma-separated quoted list for error text.
func quotedList(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = strconv.Quote(n)
	}
	return strings.Join(q, ", ")
}

// render executes the named template (without its .md suffix) — the
// installed override when one was loaded, else the embedded prompt — and
// returns its output with surrounding whitespace trimmed.
func render(name string, data any) (string, error) {
	t := parsed.Lookup(name + ".md")
	if o, ok := installed[name]; ok {
		t = o
	}
	var b strings.Builder
	if err := t.Execute(&b, data); err != nil {
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

// implementData is implement.md's data. Every prompt's data is a named
// type for one reason: LoadOverrides startup-renders each override against
// the prompt's zero value, so a reference that only fails at render time
// (a nested access, a guard an edit stripped) fails the worker's start
// instead of the round.
type implementData struct {
	Task, BugDir string
	TouchesJail  bool
}

// Implement builds the phase-1 opener: the issue task, framed so the
// implementation phase excludes tests — the test suite gets its own reviewed
// phase. bugDir is the configured out-of-scope-bug filing folder; empty
// drops the file-filing instruction from the bug policy (the Arete Memory
// note and reply-reporting duty stay). A task that itself names .ai-jail
// gets the carve-out branch: editing .ai-jail is in scope instead of
// barred.
func Implement(task, bugDir string) (string, error) {
	return render("implement", implementData{task, bugDir, TaskTouchesJail(task)})
}

// continueData is continue.md's data.
type continueData struct {
	Task          string
	PriorFeedback string
}

// Continue builds the phase-1 opener for a resumed run: the new task, the
// framing that the worktree already contains an aborted attempt's work, and
// that attempt's last review feedback when available.
func Continue(task, priorFeedback string) (string, error) {
	return render("continue", continueData{task, priorFeedback})
}

// implementFixData is implement_fix.md's data.
type implementFixData struct {
	Comments    string
	TouchesJail bool
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
	return render("implement_fix", implementFixData{comments, j.Touches})
}

// testsData is tests.md's data.
type testsData struct{ BugDir string }

// Tests opens phase 2: the agent writes or improves the test suite covering
// the change. bugDir is the configured out-of-scope-bug filing folder; empty
// drops the file-filing instruction from the bug policy.
func Tests(bugDir string) (string, error) {
	return render("tests", testsData{bugDir})
}

// TestsFix feeds phase-2 (tests ↔ test review) failures back to the agent:
// the failing test output, the test-review comments, or both. Empty
// arguments are omitted.
func TestsFix(testLogs, comments string) (string, error) {
	var parts []string
	if testLogs != "" {
		p, err := render("tests_failed", testsFailedData{testLogs})
		if err != nil {
			return "", err
		}
		parts = append(parts, p)
	}
	if comments != "" {
		p, err := render("tests_review", testsReviewData{comments})
		if err != nil {
			return "", err
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, "\n\n"), nil
}

// testsFailedData is tests_failed.md's data.
type testsFailedData struct{ Logs string }

// testsReviewData is tests_review.md's data.
type testsReviewData struct{ Comments string }

// DiffHandoff routes an oversized review diff to a worktree file: the
// prompt then carries the change digest, the per-file table of contents,
// and reading instructions instead of the diff itself — nothing is
// cropped, the reviewer reads the file in slices. Nil (the zero case)
// embeds the diff inline, byte-identical to the original prompt shape.
type DiffHandoff struct {
	// Path is the diff file's worktree-relative path — the reviewer's
	// round runs with the worktree as its directory, so this is the path
	// its reads address.
	Path  string
	Lines int
	// Stat is `git diff --compact-summary` over the same change.
	Stat string
	// Files is the table of contents, one precomputed entry per file's
	// section: "lines a-b: path", the address a slice read (sed -n
	// 'a,bp') takes. Precomputing the lines keeps element fields out of
	// the prompt's template — the override field vocabulary stays exact
	// (the range renders each entry as dot only).
	Files []string
}

// ReviewExtra is one optional attachment to a reviewer prompt. The
// interface is sealed by its unexported method to this package's two
// concrete kinds: Jail (the jail carve-out) and *DiffHandoff (the
// oversized-diff file handoff).
type ReviewExtra interface{ reviewExtra() }

func (Jail) reviewExtra() {}

func (*DiffHandoff) reviewExtra() {}

// reviewExtras splits a review builder's optional attachments into their
// parts. At most one of each kind is meaningful; the last one wins.
func reviewExtras(extra []ReviewExtra) (Jail, *DiffHandoff) {
	var j Jail
	var h *DiffHandoff
	for _, e := range extra {
		switch v := e.(type) {
		case Jail:
			j = v
		case *DiffHandoff:
			h = v
		}
	}
	return j, h
}

// reviewData is the shared data of review.md. AcceptanceCriteria, when
// non-empty, switches the template's framing to the slim flow's atomic
// sub-task review (see SlimReview); the other framings leave it nil.
// Handoff, when set, replaces the embedded diff with the file handoff.
type reviewData struct {
	Focus, Diff, TestLogs, AgentReply, BugDir, JailSpec string
	TestsInScope, ReproInScope, TouchesJail             bool
	AcceptanceCriteria                                  []string
	Handoff                                             *DiffHandoff
}

// review renders the shared reviewer template for all framings.
func review(d reviewData) (string, error) {
	return render("review", d)
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
// instead of blanket-ignoring them. Further extra attachments trail in the
// same variadic — a *DiffHandoff swaps the embedded diff for the file
// handoff (see DiffHandoff). They trail as variadic so existing callers
// stay valid.
func Review(focus, diff, testLogs string, testsInScope bool, agentReply, bugDir string, extra ...ReviewExtra) (string, error) {
	jail, handoff := reviewExtras(extra)
	return review(reviewData{
		Focus:        focus,
		Diff:         diff,
		TestLogs:     testLogs,
		TestsInScope: testsInScope,
		AgentReply:   agentReply,
		BugDir:       bugDir,
		TouchesJail:  jail.Touches,
		JailSpec:     jail.Spec,
		Handoff:      handoff,
	})
}

// ReviewRepro is Review's bug-fix framing: the diff's own tests are part of
// its deliverable, and the reviewer must judge whether the repro actually
// captures the reported bug — something the repro-first gate cannot. No
// REBUILD verdict exists in this framing. The trailing extras are Review's
// (jail carve-out, diff file handoff).
func ReviewRepro(focus, diff, testLogs, agentReply, bugDir string, extra ...ReviewExtra) (string, error) {
	jail, handoff := reviewExtras(extra)
	return review(reviewData{
		Focus:        focus,
		Diff:         diff,
		TestLogs:     testLogs,
		ReproInScope: true,
		AgentReply:   agentReply,
		BugDir:       bugDir,
		TouchesJail:  jail.Touches,
		JailSpec:     jail.Spec,
		Handoff:      handoff,
	})
}

// SlimReview renders the reviewer prompt for the slim flow's atomic
// sub-task review: the diff is one sub-task of a larger plan, and the
// reviewer judges it against exactly the sub-task's acceptance criteria —
// criteria belonging to later sub-tasks are not findings. No test-phase
// framing exists (the slim loop runs the suite itself and relays it via
// testLogs), and no REBUILD verdict. The trailing extras are Review's
// (jail carve-out, diff file handoff).
func SlimReview(focus, diff, testLogs, bugDir string, criteria []string, extra ...ReviewExtra) (string, error) {
	jail, handoff := reviewExtras(extra)
	return review(reviewData{
		Focus:              focus,
		Diff:               diff,
		TestLogs:           testLogs,
		BugDir:             bugDir,
		AcceptanceCriteria: criteria,
		Handoff:            handoff,
		TouchesJail:        jail.Touches,
		JailSpec:           jail.Spec,
	})
}

// rebuildData is rebuild.md's data.
type rebuildData struct{ Finding string }

// Rebuild feeds a test-review REBUILD finding back to the implementing
// agent: a tight, finding-only prompt — the change was already code- and
// test-reviewed once, so the template fences the agent against reworking
// anything the finding does not demand.
func Rebuild(finding string) (string, error) {
	return render("rebuild", rebuildData{finding})
}

// investigateData is investigate.md's data.
type investigateData struct{ Task string }

// Investigate opens a docs-only investigation run: analysis and written
// deliverables, no code changes.
func Investigate(task string) (string, error) {
	return render("investigate", investigateData{task})
}

// investigateFixData is investigate_fix.md's data.
type investigateFixData struct{ Comments string }

// InvestigateFix feeds docs-review comments back to the agent inside the
// investigation loop.
func InvestigateFix(comments string) (string, error) {
	return render("investigate_fix", investigateFixData{comments})
}

// refactorData is refactor.md's data.
type refactorData struct{ Task string }

// Refactor opens a behavior-frozen refactoring run: structure only, the
// existing suite green and untouched.
func Refactor(task string) (string, error) {
	return render("refactor", refactorData{task})
}

// refactorFixData is refactor_fix.md's data.
type refactorFixData struct{ Logs, Comments string }

// RefactorFix feeds a refactoring round's failure back to the agent: the
// frozen suite's red output, the review comments, or both. Empty arguments
// are omitted.
func RefactorFix(testLogs, comments string) (string, error) {
	return render("refactor_fix", refactorFixData{testLogs, comments})
}

// bugfixData is bugfix.md's data.
type bugfixData struct{ Task string }

// BugFix opens a test-first bug-fix run: the repro test and the minimal fix
// in one deliverable.
func BugFix(task string) (string, error) {
	return render("bugfix", bugfixData{task})
}

// bugfixFixData is bugfix_fix.md's data.
type bugfixFixData struct{ Logs, Comments string }

// BugFixFix feeds a bug-fix round's failure back to the agent: the suite's
// red output (or the repro-first gate's refusal), the review comments, or
// both. Empty arguments are omitted.
func BugFixFix(testLogs, comments string) (string, error) {
	return render("bugfix_fix", bugfixFixData{testLogs, comments})
}

// SlimSubtask is one atomized unit of the slim flow's plan — the JSON
// schema the parse round is told to emit and SlimStep renders into the
// worker prompt. The struct doubles as the parse target for the parse
// round's raw JSON array reply (workflows.parseSlimPlan), so the wire
// schema and the prompt schema can never drift.
type SlimSubtask struct {
	ID                 int      `json:"id"`
	Type               string   `json:"type"`
	TargetFiles        []string `json:"target_files"`
	Description        string   `json:"description"`
	AcceptanceCriteria []string `json:"acceptance_criteria"`
}

// slimPlanData is slim_plan.md's data.
type slimPlanData struct{ Task string }

// SlimPlan builds the slim flow's planner opener: the task, deconstructed
// in prose into a strictly ordered queue of atomic sub-tasks. Pure
// generation — no JSON contract; transcription into the machine queue is
// the parse round's job (SlimParse).
func SlimPlan(task string) (string, error) {
	return render("slim_plan", slimPlanData{task})
}

// slimParseData is slim_parse.md's data.
type slimParseData struct{ Plan string }

// SlimParse builds the slim flow's parse round: the planner's prose plan,
// transcribed into the raw SlimSubtask JSON array the loop executes. The
// round runs in the planner's conversation, so its reply joins the plan in
// the worker's progressive context.
func SlimParse(plan string) (string, error) {
	return render("slim_parse", slimParseData{plan})
}

// slimParseReaskData is slim_parse_reask.md's data.
type slimParseReaskData struct{ Error string }

// SlimParseReask builds the parse round's one strict re-ask: the parse
// failure, delivered into the parse round's own conversation — the model
// sees its previous reply and corrects it.
func SlimParseReask(parseErr string) (string, error) {
	return render("slim_parse_reask", slimParseReaskData{parseErr})
}

// slimStepData is slim_step.md's data.
type slimStepData struct {
	Index, Total int
	Subtask      SlimSubtask
}

// SlimStep builds the slim worker prompt for one sub-task (1-based index
// of total): implement only this sub-task, in the dev conversation that
// already carries the plan and every completed sub-task before it.
func SlimStep(index, total int, st SlimSubtask) (string, error) {
	return render("slim_step", slimStepData{index, total, st})
}

// slimFixData is slim_fix.md's data.
type slimFixData struct {
	Description, Logs, Comments string
}

// SlimFix feeds a slim sub-task round's failure back to the worker: the
// suite's red output, the reviewer's comments, or both. Empty arguments
// are omitted.
func SlimFix(description, testLogs, comments string) (string, error) {
	return render("slim_fix", slimFixData{description, testLogs, comments})
}
