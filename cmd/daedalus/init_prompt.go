// The `daedalus init prompt <dir>` scaffold: a prompt-override directory
// seeded with every embedded prompt's current source plus a plain-named
// guide. The samples are the worker's own embedded prompts (template
// sources, not hand-maintained copies), so editing one overrides exactly
// what it documents.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ozzono/daedalus/internal/template"
)

// scaffoldPromptOverrides creates the prompt-override directory dir:
// one <prompt>.md per embedded prompt (byte-identical samples) plus the
// README guide. The plain README name is load-bearing — LoadOverrides
// stops the worker on any .md stem that does not name a prompt, so the
// guide must not be README.md. Refuses a non-empty target, matching bare
// init's refuse-to-overwrite shape; the samples themselves overwrite
// nothing that exists only because the dir was empty. Afterwards it
// regenerates config-example.yaml as the prompt profile — the same end
// `daedalus init prompt` reaches, so the scaffold carries the block the
// activation step copies into config.yaml.
func scaffoldPromptOverrides(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		fail("init", err)
	}
	if err == nil && len(entries) > 0 {
		fail("init", fmt.Errorf("%s already exists and is not empty — scaffold into a new (or empty) directory", dir))
	}
	specs, err := template.PromptSpecs()
	if err != nil {
		fail("init", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fail("init", err)
	}
	for _, s := range specs {
		if err := os.WriteFile(filepath.Join(dir, s.Name+".md"), []byte(s.Source), 0o644); err != nil {
			fail("init", err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "README"), []byte(promptScaffoldGuide(specs)), 0o644); err != nil {
		fail("init", err)
	}
	fmt.Printf("scaffolded %s: %d prompt samples plus the README guide\n", dir, len(specs))
	writeProfiledExample([]string{"prompt"})
}

// promptScaffoldGroups is the guide's phase grouping: what the phase's
// loop needs from its prompts, and which prompts serve it. A prompt added
// to the template package later lands in the guide's fallback tail even
// though no group names it yet.
var promptScaffoldGroups = []struct {
	title string
	blurb string
	names []string
}{
	{
		"implementation ↔ code review loop",
		"The dev opener assigns the task; the code reviewer judges the diff and its comments are fed back verbatim into the dev conversation until approved. Reviews run with tests out of scope — the suite gets its own phase.",
		[]string{"implement", "implement_fix", "continue", "review"},
	},
	{
		"test phase",
		"The test agent writes or improves the suite; the test reviewer judges it (the only reviewer carrying the REBUILD verdict) and its comments plus failing output are fed back until the suite is green and approved.",
		[]string{"tests", "tests_failed", "tests_review", "rebuild"},
	},
	{
		"bug-fix flow",
		"A test-first run: the opener demands a repro test plus the minimal fix in one deliverable; failures and review comments feed back into the same conversation.",
		[]string{"bugfix", "bugfix_fix"},
	},
	{
		"investigation flow",
		"A docs-only run: written deliverables, no code changes; docs-review comments feed back until approved.",
		[]string{"investigate", "investigate_fix"},
	},
	{
		"refactor flow",
		"A behavior-frozen run: structure only, the existing suite green and untouched; its red output or review comments feed back.",
		[]string{"refactor", "refactor_fix"},
	},
	{
		"slim flow (small self-hosted models)",
		"The planner deconstructs the task into ordered atomic sub-tasks, a parse round transcribes the plan into the machine queue, and each sub-task runs implement-and-review against exactly its own acceptance criteria.",
		[]string{"slim_plan", "slim_parse", "slim_parse_reask", "slim_step", "slim_fix"},
	},
}

// promptScaffoldRoles is the guide's one-line role per prompt — what the
// round it opens (or feeds) does.
var promptScaffoldRoles = map[string]string{
	"implement":        "the phase-1 opener: the task, framed so the test suite stays out of scope",
	"implement_fix":    "review comments fed back into the dev conversation",
	"continue":         "the dev opener for a resumed run: the new task plus the aborted attempt's last review feedback",
	"review":           "the reviewer's prompt, every framing (ordinary, bug-fix repro, slim sub-task, test review)",
	"tests":            "the phase-2 opener: write or improve the suite",
	"tests_failed":     "failing suite output fed back into the test conversation",
	"tests_review":     "test-review comments fed back into the test conversation",
	"rebuild":          "a REBUILD finding sent back to the implementation cycle",
	"bugfix":           "the bug-fix opener: repro test plus minimal fix in one deliverable",
	"bugfix_fix":       "the suite's red output (or the repro-first gate's refusal) or review comments fed back",
	"investigate":      "the docs-only investigation opener",
	"investigate_fix":  "docs-review comments fed back into the investigation conversation",
	"refactor":         "the behavior-frozen refactor opener",
	"refactor_fix":     "the frozen suite's red output or review comments fed back",
	"slim_plan":        "the slim planner's opener: the task deconstructed in prose into a strictly ordered sub-task plan",
	"slim_parse":       "the parse round transcribing the plan into the raw sub-task JSON the loop executes",
	"slim_parse_reask": "the parse round's one strict re-ask: the parse failure, delivered into the parse conversation",
	"slim_step":        "one sub-task's worker prompt, in the dev conversation that already carries the plan",
	"slim_fix":         "a sub-task round's failing suite output or review comments fed back",
}

// promptScaffoldGuide renders the README: how activation works, the
// startup-enforced writing rules, what each phase needs, and every
// prompt's data fields (computed from the embedded templates, so the
// field lists cannot drift from what the worker actually validates).
func promptScaffoldGuide(specs []template.PromptSpec) string {
	byName := make(map[string]template.PromptSpec, len(specs))
	for _, s := range specs {
		byName[s.Name] = s
	}
	var b strings.Builder
	// The entry columns align under the longest prompt name, so the role
	// line's indent matches the fields column for every entry (a fixed
	// indent would misalign under any longer name).
	width := 0
	for _, s := range specs {
		if len(s.Name) > width {
			width = len(s.Name)
		}
	}
	b.WriteString(`Prompt overrides for daedalus
=============================

Scaffolded by ` + "`daedalus init prompt <dir>`" + `. Every .md file here is a
byte for byte copy of the prompt the worker carries embedded, and a copy
overrides nothing: renders are identical until you edit a file. Edit the
prompt whose phase you want to change and leave the rest; deleting a file
falls back to the embedded default.

Activate
--------
Point config.yaml at this directory:

  prompt: <dir>

(the scaffold also wrote config-example.yaml carrying the prompt: block —
copy it into your config.yaml). A relative path resolves against the
config file's directory; ~/… and absolute paths work too.

The worker reads and validates the whole directory at startup and refuses
to start on any problem, so a broken override never reaches a round.

Writing rules (all enforced at startup)
---------------------------------------
- A file's name is the prompt it replaces — <name>.md from the list
  below. Any other .md stem stops the worker, which is why this guide is
  named README and not README.md.
- Every {{.Field}} a text references must be in that prompt's field list
  below; an unknown field stops the worker.
- No {{template}} action, and no {{define}} or {{block}} block. The one
  exception: a define named exactly after the file's own prompt renders
  as the file body — don't rely on it; put the content in the file body
  plain.
- review.md must keep all four verdict words (APPROVED, CHANGES_REQUESTED,
  NEEDS_MAINTAINER, REBUILD) in its text: the final verdict line is the
  machine contract the review loop parses, and an override that dropped
  one would silently break every review.

How to write one
----------------
A prompt is the whole opening message of one jailed agent round: rendered
with the data listed below, it is all the agent sees of the pipeline.
Write it as instructions to a rigorous engineer working alone in the
worktree — state the deliverable, the constraints, what is out of scope,
and how to end (the exact verdict protocol for the review prompts, plain
text elsewhere). Keep every machine-parsed shape you find in a sample
exactly as it is.

`)
	for _, g := range promptScaffoldGroups {
		fmt.Fprintf(&b, "%s\n%s\n\n", g.title, g.blurb)
		for _, name := range g.names {
			writePromptEntry(&b, byName, name, width)
		}
		b.WriteString("\n")
	}
	var rest []string
	for _, s := range specs {
		grouped := false
		for _, g := range promptScaffoldGroups {
			grouped = grouped || strings.Contains(strings.Join(g.names, " "), s.Name)
		}
		if !grouped {
			rest = append(rest, s.Name)
		}
	}
	if len(rest) > 0 {
		b.WriteString("Other prompts\n\n")
		for _, name := range rest {
			writePromptEntry(&b, byName, name, width)
		}
		b.WriteString("\n")
	}
	return b.String()
}

// writePromptEntry writes one prompt's guide entry: name, data fields, and
// role (a fallback note for a prompt this scaffold's tables predate). The
// role line indents to the fields column, aligned under the padded name.
func writePromptEntry(b *strings.Builder, byName map[string]template.PromptSpec, name string, width int) {
	s, ok := byName[name]
	if !ok {
		return
	}
	fmt.Fprintf(b, "  %-*s  fields: %s\n", width, s.Name, strings.Join(s.Fields, ", "))
	role, ok := promptScaffoldRoles[s.Name]
	if !ok {
		role = "(a prompt this scaffold's guide predates — read the file itself)"
	}
	fmt.Fprintf(b, "%s%s\n\n", strings.Repeat(" ", width+4), role)
}
