package template

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// resetInstalled scopes the package-global installed set to the test: every
// render in this package reads it, so an override left behind would break
// every embedded-output pin that runs afterwards.
func resetInstalled(t *testing.T) {
	t.Helper()
	prev := installed
	t.Cleanup(func() { installed = prev })
}

// writeFiles drops files (name → content) into dir, creating it.
func writeFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, src := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// TestPrompts pins the overridable vocabulary: exactly the embedded
// prompts/*.md stems, sorted — the list the README and the config docs
// spell out, so a renamed or added prompt must update them in the same
// change.
func TestPrompts(t *testing.T) {
	want := []string{
		"bugfix", "bugfix_fix", "continue", "implement", "implement_fix",
		"investigate", "investigate_fix", "rebuild", "refactor", "refactor_fix",
		"review", "slim_fix", "slim_parse", "slim_parse_reask", "slim_plan",
		"slim_step", "tests", "tests_failed", "tests_review",
	}
	got := Prompts()
	if !slices.Equal(got, want) {
		t.Errorf("Prompts() = %v, want %v", got, want)
	}
}

// TestLoadOverridesRejects pins the documented fail-fast set: every malformed
// override directory errors from LoadOverrides — never installs anything —
// so a deployment's mistake fails the worker's start instead of surfacing as
// a broken prompt mid-round.
func TestLoadOverridesRejects(t *testing.T) {
	cases := []struct {
		name  string
		dir   string
		files map[string]string
		want  string
	}{
		{
			name: "missing directory",
			dir:  filepath.Join(t.TempDir(), "absent"),
			want: "prompt overrides: read",
		},
		{
			name:  "stray stem",
			files: map[string]string{"dev-session.md": "there is no dev-session prompt"},
			want:  "dev-session.md does not name a prompt",
		},
		{
			name:  "unparsable",
			files: map[string]string{"implement.md": "{{.Task"},
			want:  "parse",
		},
		{
			name:  "empty file",
			files: map[string]string{"continue.md": ""},
			want:  "is empty",
		},
		{
			name:  "whitespace-only file",
			files: map[string]string{"continue.md": "  \n\t\n"},
			want:  "is empty",
		},
		{
			name:  "unknown data field",
			files: map[string]string{"implement.md": "{{.Task}} {{.Nope}}"},
			want:  `references data field(s) "Nope"`,
		},
		{
			// The $ root is the render data: $.Nope validates exactly like
			// .Nope, not like a declared variable.
			name:  "unknown data field via $ root",
			files: map[string]string{"implement.md": "{{$.Nope}}"},
			want:  `references data field(s) "Nope"`,
		},
		{
			// Scope-flat walk, the documented caveat: a field read off a
			// range variable is still checked against the root data.
			name:  "range-scoped field read off the root data",
			files: map[string]string{"review.md": "{{range .AcceptanceCriteria}}{{.Description}}{{end}}"},
			want:  `references data field(s) "Description"`,
		},
		{
			name:  "template action",
			files: map[string]string{"rebuild.md": `{{template "continue.md"}}`},
			want:  "may not reference other templates",
		},
		{
			// Dead content: the parser hoists a define body out of the root
			// tree, where it would render nothing.
			name:  "define-only override",
			files: map[string]string{"rebuild.md": `{{define "x"}}never shown{{end}}`},
			want:  "contains a {{define}}/{{block}} block",
		},
		{
			// The define gate closes the dead-content route before field
			// validation: an unknown field cannot hide in a define body.
			name:  "unknown field hidden in a define body",
			files: map[string]string{"rebuild.md": `ROOT TEXT {{define "x"}}{{.Nope}}{{end}}`},
			want:  "contains a {{define}}/{{block}} block",
		},
		{
			// A root-level block desugars to the same dead shape (a defined
			// template plus a template invocation), so the same gate catches
			// it — the diagnostic names both spellings, since the source may
			// carry neither the word "define".
			name:  "root-level block override",
			files: map[string]string{"rebuild.md": `{{block "x" .}}never shown{{end}}`},
			want:  "contains a {{define}}/{{block}} block",
		},
		{
			// The one legal define form — naming the file's own template —
			// becomes the root tree, so field validation still sees it.
			name:  "unknown field in an own-name define body",
			files: map[string]string{"rebuild.md": `{{define "rebuild.md"}}{{.Nope}}{{end}}`},
			want:  `references data field(s) "Nope"`,
		},
		{
			// A first-level field carrying a second-level read the data type
			// does not have: checkOverride's first-level walk passes it, and
			// only the startup render can catch it.
			name:  "nested access fails the startup render",
			files: map[string]string{"tests.md": "{{.BugDir}} {{.BugDir.Nope}}"},
			want:  "does not render against the prompt's data",
		},
		{
			// The same shape with the guard stripped: the embedded review
			// reads Handoff only under {{if .Handoff}}, so a replacement that
			// keeps the reference but drops the guard would have failed on
			// every round's render, not just the oversized-diff ones.
			name:  "stripped handoff guard fails the startup render",
			files: map[string]string{"review.md": "verdicts: APPROVED CHANGES_REQUESTED NEEDS_MAINTAINER REBUILD {{.Handoff.Nope}}"},
			want:  "does not render against the prompt's data",
		},
		{
			// A broken access behind a surviving guard: the zero-value render
			// skips it, so the handoff-attached render is what catches it —
			// failing the start instead of the first oversized diff.
			name: "broken access behind the handoff guard",
			files: map[string]string{"review.md": "{{if .Handoff}}{{.Handoff.Nope}}{{end}} " +
				"APPROVED CHANGES_REQUESTED NEEDS_MAINTAINER REBUILD"},
			want: "(with a diff handoff attached)",
		},
		{
			// Whole-word matching: DISAPPROVED contains APPROVED as a
			// substring, and the source-level contains-check this guard once
			// used took it as vouching for the verdict.
			name:  "DISAPPROVED cannot vouch for APPROVED",
			files: map[string]string{"review.md": "say DISAPPROVED CHANGES_REQUESTED NEEDS_MAINTAINER REBUILD"},
			want:  `missing "APPROVED"`,
		},
		{
			// A word present in the source but rendered nowhere: REBUILD
			// hidden behind a condition that never fires fails the rendered-
			// text check the same way a deletion would — the rendered text is
			// the contract the review loop parses.
			name:  "verdict word carried only where it never renders",
			files: map[string]string{"review.md": `APPROVED CHANGES_REQUESTED NEEDS_MAINTAINER {{if eq .Focus "never"}}REBUILD{{end}}`},
			want:  `missing "REBUILD"`,
		},
		{
			name:  "review override dropped a verdict word",
			files: map[string]string{"review.md": "End with exactly APPROVED, always."},
			want:  `missing "CHANGES_REQUESTED"`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resetInstalled(t)
			dir := c.dir
			if dir == "" {
				dir = t.TempDir()
				writeFiles(t, dir, c.files)
			}
			err := LoadOverrides(dir, t.TempDir())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("LoadOverrides(%q) error = %v, want it to contain %q", dir, err, c.want)
			}
			// A rejected directory installs nothing: no partial set, no wipe
			// of what was there before.
			if installed != nil {
				t.Errorf("LoadOverrides failure installed %d override(s), want none", len(installed))
			}
		})
	}

	// The unparsable wrap chains (the %w wrap): the diagnostic carries
	// text/template's own message, so callers see why the template does not
	// parse, not just that it does not.
	resetInstalled(t)
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{"implement.md": "{{.Task"})
	err := LoadOverrides(dir, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "unclosed action") {
		t.Errorf("LoadOverrides parse error = %v, want text/template's own diagnostic in the chain", err)
	}
}

// TestLoadOverridesPathForms pins the three documented spellings: relative
// resolves against the config file's directory (baseDir), ~/… against the
// home directory, absolute as-is — never the process's working directory.
func TestLoadOverridesPathForms(t *testing.T) {
	resetInstalled(t)
	base := t.TempDir()

	// Relative to baseDir. The working directory (this package's dir) holds
	// no "overrides" folder, so success alone proves the baseDir join.
	writeFiles(t, filepath.Join(base, "overrides"), map[string]string{
		"rebuild.md": "RELATIVE REBUILD {{.Finding}}",
	})
	if err := LoadOverrides("overrides", base); err != nil {
		t.Fatalf("LoadOverrides(relative): %v", err)
	}
	if got, err := Rebuild("x"); err != nil || !strings.Contains(got, "RELATIVE REBUILD x") {
		t.Errorf("Rebuild after relative load = %q, %v; want the override", got, err)
	}

	// A missing relative directory errors with the joined path, not the bare
	// spelling — the diagnostic points where the worker actually looked.
	err := LoadOverrides("absent", base)
	if err == nil || !strings.Contains(err.Error(), filepath.Join(base, "absent")) {
		t.Errorf("LoadOverrides(missing relative) error = %v, want it to name %q", err, filepath.Join(base, "absent"))
	}

	// ~/… against the home directory, whatever baseDir says.
	t.Setenv("HOME", base)
	writeFiles(t, filepath.Join(base, "homeprompts"), map[string]string{
		"tests.md": "HOME TESTS {{$.BugDir}}",
	})
	if err := LoadOverrides("~/homeprompts", filepath.Dir(base)); err != nil {
		t.Fatalf("LoadOverrides(~/…): %v", err)
	}
	if got, err := Tests("docs/bugs"); err != nil || !strings.Contains(got, "HOME TESTS docs/bugs") {
		t.Errorf("Tests after ~/ load = %q, %v; want the override rendering the bug dir", got, err)
	}
}

// TestLoadOverridesInstalls pins the happy path: a valid directory replaces
// exactly the prompts it names, control flow and $-root references in the
// replacement execute for real, entries that are not .md files are ignored,
// and every prompt the directory does not name keeps rendering the embedded
// template. The review override must carry all four verdict words to pass
// the guard — the accepted side of the machine contract.
func TestLoadOverridesInstalls(t *testing.T) {
	resetInstalled(t)
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"implement.md": "CUSTOM IMPLEMENT: {{.Task}}",
		"tests.md":     "CUSTOM TESTS: {{$.BugDir}}",
		"review.md":    "CUSTOM REVIEW {{.Focus}} APPROVED CHANGES_REQUESTED NEEDS_MAINTAINER REBUILD",
		// A define naming the file's own template is the one legal form: the
		// parser makes its body the root tree, so it renders and validates.
		"rebuild.md": `{{define "rebuild.md"}}CUSTOM DEFINE {{.Finding}}{{end}}`,
	})
	// Entries that are not replacement prompts are not overrides: a readme
	// and a directory ride along unexamined.
	if err := os.WriteFile(filepath.Join(dir, "README.txt"), []byte("notes"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := LoadOverrides(dir, t.TempDir()); err != nil {
		t.Fatalf("LoadOverrides: %v", err)
	}

	got, err := Implement("ship it", "")
	if err != nil || !strings.Contains(got, "CUSTOM IMPLEMENT: ship it") {
		t.Errorf("Implement after override = %q, %v; want the replacement", got, err)
	}
	got, err = Tests("docs/bugs")
	if err != nil || !strings.Contains(got, "CUSTOM TESTS: docs/bugs") {
		t.Errorf("Tests after override = %q, %v; want the $-root reference rendering the bug dir", got, err)
	}
	got, err = Review("the change", "M x.go", "", false, "", "")
	if err != nil || !strings.Contains(got, "CUSTOM REVIEW the change") {
		t.Errorf("Review after override = %q, %v; want the replacement (all four verdict words passed the guard)", got, err)
	}
	got, err = Rebuild("x")
	if err != nil || !strings.Contains(got, "CUSTOM DEFINE x") {
		t.Errorf("Rebuild after override = %q, %v; want the own-name define body rendered as the root tree", got, err)
	}

	// A prompt the directory does not name stays embedded.
	got, err = Continue("t", "fb")
	if err != nil || !strings.Contains(got, "continuing a previous attempt") {
		t.Errorf("Continue after unrelated overrides = %q, %v; want the embedded prompt", got, err)
	}
}

// TestLoadOverridesVerbatimReviewCopy pins the scaffold round trip: a
// byte-verbatim copy of the embedded review prompt — exactly what
// "daedalus init prompt review" scaffolds — loads and renders identically
// to the embedded one. The rendered-text verdict check must read the
// test-review framing, the one shape where REBUILD renders (it lives inside
// {{if .TestsInScope}}), or this innocent copy fails the worker's start.
func TestLoadOverridesVerbatimReviewCopy(t *testing.T) {
	resetInstalled(t)
	src, err := promptFiles.ReadFile("prompts/review.md")
	if err != nil {
		t.Fatalf("read embedded review.md: %v", err)
	}
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{"review.md": string(src)})

	// The embedded render, captured before the override is installed.
	want, err := Review("the change", "M x.go", "", true, "reply", "docs/bugs")
	if err != nil {
		t.Fatalf("embedded Review: %v", err)
	}
	if err := LoadOverrides(dir, t.TempDir()); err != nil {
		t.Fatalf("LoadOverrides(verbatim review.md): %v — a byte-verbatim copy of the embedded prompt must load", err)
	}
	got, err := Review("the change", "M x.go", "", true, "reply", "docs/bugs")
	if err != nil {
		t.Fatalf("Review after override: %v", err)
	}
	if got != want {
		t.Errorf("verbatim override render differs from the embedded render (%d vs %d bytes)", len(got), len(want))
	}

	// The guard's positive catch on the same faithful shape: every REBUILD
	// occurrence stripped from the verbatim copy — an edit an override
	// author could actually make — fails the load naming the word.
	resetInstalled(t)
	stripped := strings.ReplaceAll(string(src), "REBUILD", "")
	if stripped == string(src) {
		t.Fatal("the embedded review.md carries no REBUILD — the strip fixture is vacuous")
	}
	writeFiles(t, dir, map[string]string{"review.md": stripped})
	err = LoadOverrides(dir, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), `missing "REBUILD"`) {
		t.Errorf("LoadOverrides(REBUILD-stripped verbatim copy) = %v, want the missing-REBUILD refusal", err)
	}
	// The rejection swaps in nothing: the verbatim set the earlier load
	// installed stays standing (the atomicity TestLoadOverridesAtomicity
	// pins, here over the failed re-load).
	if len(installed) != 1 {
		t.Errorf("installed set after the failed load = %d override(s), want the previous verbatim set kept", len(installed))
	}
}

// TestRepresentativeDataCoversPrompts pins the startup-render data map
// against the prompt vocabulary: every prompt has an entry (a missing key
// would render an override against nil and miss field errors), no entry is
// left over from a renamed prompt, and every embedded prompt actually
// renders against its entry — a mistyped zero value fails here instead of
// on the first deployment that overrides the prompt.
func TestRepresentativeDataCoversPrompts(t *testing.T) {
	names := Prompts()
	if len(representativeData) != len(names) {
		t.Errorf("representativeData holds %d entries, want one per prompt (%d)", len(representativeData), len(names))
	}
	for _, name := range names {
		data, ok := representativeData[name]
		if !ok {
			t.Errorf("representativeData has no entry for prompt %q", name)
			continue
		}
		emb := parsed.Lookup(name + ".md")
		if emb == nil {
			t.Fatalf("no embedded template named %s.md", name)
		}
		if _, err := renderOverride(emb, data); err != nil {
			t.Errorf("prompt %s does not render against its representative data: %v", name, err)
		}
	}
}

// TestSlimParseOverrideFields pins the two new prompts' render contracts at
// the override boundary: a slim_parse replacement reads the plan (.Plan),
// and slim_parse_reask's data is the parse error alone (.Error) — a re-ask
// override reaching for the plan is rejected at load, so the deployment's
// mistake fails the worker's start instead of surfacing mid-round. (The
// workflow composes the plan into the re-ask round itself: parsePrompt +
// re-ask, so the model still sees its reply to correct.)
func TestSlimParseOverrideFields(t *testing.T) {
	resetInstalled(t)
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"slim_parse.md":       "TRANSCRIBE THE PLAN: {{.Plan}}",
		"slim_parse_reask.md": "PARSE FAILED: {{.Error}} — try again.",
	})
	if err := LoadOverrides(dir, t.TempDir()); err != nil {
		t.Fatalf("LoadOverrides: %v", err)
	}
	got, err := SlimParse("add the adder, then multiply")
	if err != nil || !strings.Contains(got, "TRANSCRIBE THE PLAN: add the adder, then multiply") {
		t.Errorf("SlimParse after override = %q, %v; want the replacement rendering the plan", got, err)
	}
	got, err = SlimParseReask("the JSON array is empty")
	if err != nil || !strings.Contains(got, "PARSE FAILED: the JSON array is empty") {
		t.Errorf("SlimParseReask after override = %q, %v; want the replacement rendering the error", got, err)
	}

	// The re-ask's data is the error alone: a plan reference in its
	// replacement names a field the embedded prompt does not carry.
	resetInstalled(t)
	bad := t.TempDir()
	writeFiles(t, bad, map[string]string{"slim_parse_reask.md": "the plan says {{.Plan}}"})
	if err := LoadOverrides(bad, t.TempDir()); err == nil || !strings.Contains(err.Error(), `references data field(s) "Plan"`) {
		t.Errorf("LoadOverrides(reask override with .Plan) = %v, want the unknown-field rejection", err)
	}
}

// TestLoadOverridesAtomicity pins the install's all-or-nothing shape: a
// directory where one file fails leaves the previously installed set (or the
// embedded set) untouched, an empty directory is the unset case that wipes
// nothing, and an empty dir argument is a plain no-op.
func TestLoadOverridesAtomicity(t *testing.T) {
	resetInstalled(t)

	// From a clean slate, a mixed directory installs nothing: the surviving
	// valid file must not partially apply over the embedded prompt.
	mixed := t.TempDir()
	writeFiles(t, mixed, map[string]string{
		"rebuild.md":   "CUSTOM REBUILD {{.Finding}}",
		"implement.md": "{{.Nope}}",
	})
	if err := LoadOverrides(mixed, t.TempDir()); err == nil {
		t.Fatal("LoadOverrides(mixed directory) = nil, want the implement.md rejection")
	}
	got, err := Rebuild("x")
	if err != nil || !strings.Contains(got, "REBUILD — the test phase's review") {
		t.Errorf("Rebuild after failed load = %q, %v; want the embedded prompt", got, err)
	}

	// A good directory installs; a later failing load and an empty directory
	// both leave that set standing.
	good := t.TempDir()
	writeFiles(t, good, map[string]string{"rebuild.md": "CUSTOM ONE {{.Finding}}"})
	if err := LoadOverrides(good, t.TempDir()); err != nil {
		t.Fatalf("LoadOverrides(good): %v", err)
	}
	if err := LoadOverrides(mixed, t.TempDir()); err == nil {
		t.Fatal("LoadOverrides(mixed) = nil, want rejection")
	}
	if err := LoadOverrides(t.TempDir(), t.TempDir()); err != nil {
		t.Fatalf("LoadOverrides(empty directory): %v", err)
	}
	if err := LoadOverrides("", t.TempDir()); err != nil {
		t.Fatalf("LoadOverrides(empty dir argument): %v", err)
	}
	got, err = Rebuild("x")
	if err != nil || !strings.Contains(got, "CUSTOM ONE x") {
		t.Errorf("Rebuild after failed re-loads = %q, %v; want the previously installed set kept", got, err)
	}
}

// TestPromptSpecs pins the scaffold's feed: one spec per overridable prompt
// (same sorted names, sources byte-identical to the embedded prompts,
// sorted non-empty field lists), and review's field list is the exact
// vocabulary of the shared review data — a handoff element's fields
// (First/Last/Path) must never reappear in it, because the range renders
// dot-only and an override reaching for them would pass or fail on fiction.
func TestPromptSpecs(t *testing.T) {
	specs, err := PromptSpecs()
	if err != nil {
		t.Fatalf("PromptSpecs: %v", err)
	}
	names := make([]string, len(specs))
	byName := make(map[string]PromptSpec, len(specs))
	for i, s := range specs {
		names[i] = s.Name
		byName[s.Name] = s
	}
	if want := Prompts(); !slices.Equal(names, want) {
		t.Errorf("PromptSpecs names = %v, want Prompts() %v", names, want)
	}
	for _, s := range specs {
		src, err := promptFiles.ReadFile("prompts/" + s.Name + ".md")
		if err != nil {
			t.Fatalf("read embedded %s: %v", s.Name, err)
		}
		if s.Source != string(src) {
			t.Errorf("spec %s source drifts from the embedded prompt (%d vs %d bytes)", s.Name, len(s.Source), len(src))
		}
		if len(s.Fields) == 0 {
			t.Errorf("spec %s lists no data fields", s.Name)
		}
		if !slices.IsSorted(s.Fields) {
			t.Errorf("spec %s fields are not sorted: %v", s.Name, s.Fields)
		}
	}
	wantReview := []string{
		"AcceptanceCriteria", "AgentReply", "BugDir", "Diff", "Focus", "Handoff",
		"JailSpec", "ReproInScope", "SkillInstructions", "TestLogs", "TestsInScope",
		"TouchesJail",
	}
	if !slices.Equal(byName["review"].Fields, wantReview) {
		t.Errorf("review fields = %v, want the exact review-data vocabulary %v", byName["review"].Fields, wantReview)
	}

	// The vocabulary is what startup enforces: a review override reaching
	// for a handoff element's field is rejected at load, not mid-round.
	resetInstalled(t)
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{"review.md": "{{range .Handoff.Files}}- {{.First}}{{end}}"})
	if err := LoadOverrides(dir, t.TempDir()); err == nil || !strings.Contains(err.Error(), `"First"`) {
		t.Errorf("LoadOverrides(review override with .First) = %v, want the unknown-field rejection", err)
	}
}
