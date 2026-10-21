package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/activities"
)

// TestResolveBranchPrefix pins the precedence for a new run's preserved
// branch: an explicit -p/--prefix wins for that run, otherwise the config's
// (already validated and defaulted) branch_prefix applies.
func TestResolveBranchPrefix(t *testing.T) {
	for _, c := range []struct{ flag, cfg, want string }{
		{"team", "feat", "team"},
		{"", "feat", "feat"},
		{"", "", ""},
	} {
		if got := resolveBranchPrefix(c.flag, c.cfg); got != c.want {
			t.Errorf("resolveBranchPrefix(%q, %q) = %q, want %q", c.flag, c.cfg, got, c.want)
		}
	}
}

// TestParseFlagsWorkflowRegistry pins the -w/--workflow dispatch surface:
// the registered default is accepted and travels with the run's flags, and
// an unknown name is rejected at parse time with the available workflows
// named (so a typo fails before any client dials Temporal).
func TestParseFlagsWorkflowRegistry(t *testing.T) {
	f, rest, err := parseFlags([]string{"run", "-w", "feature-dev", "/repo", "42", "do it"})
	if err != nil {
		t.Fatalf("parseFlags(-w feature-dev): %v", err)
	}
	if f.workflow != "feature-dev" {
		t.Errorf("workflow = %q, want feature-dev", f.workflow)
	}
	wantRest := []string{"run", "/repo", "42", "do it"}
	if !reflect.DeepEqual(rest, wantRest) {
		t.Errorf("rest = %v, want %v", rest, wantRest)
	}

	if _, _, err := parseFlags([]string{"run", "-w", "nope"}); err == nil ||
		!strings.Contains(err.Error(), `unknown workflow "nope"`) ||
		!strings.Contains(err.Error(), "feature-dev") {
		t.Errorf("parseFlags(-w nope) err = %v, want an unknown-workflow rejection naming the registry", err)
	}

	// The other registered entries dispatch too — dev-only is selection
	// only: no config key picks a flow.
	if _, _, err := parseFlags([]string{"run", "-w", "dev-only", "/repo", "42", "do it"}); err != nil {
		t.Errorf("parseFlags(-w dev-only): %v", err)
	}

	// slim dispatches like any flow, and workflowSet separates an explicit
	// -w from the default: only a defaulted -w is subject to the config's
	// slim gate, an explicit one always wins.
	f, _, err = parseFlags([]string{"run", "-w", "slim", "/repo", "42", "do it"})
	if err != nil {
		t.Fatalf("parseFlags(-w slim): %v", err)
	}
	if f.workflow != "slim" || !f.workflowSet {
		t.Errorf("parseFlags(-w slim) = (%q, %v), want (slim, true)", f.workflow, f.workflowSet)
	}
	f, _, err = parseFlags([]string{"run", "/repo", "42", "do it"})
	if err != nil {
		t.Fatalf("parseFlags(default): %v", err)
	}
	if f.workflow != "feature-dev" || f.workflowSet {
		t.Errorf("parseFlags(default) = (%q, %v), want (feature-dev, false)", f.workflow, f.workflowSet)
	}
}

// TestParseFlagsWorkflowFreshRunOnly pins -w/--workflow's rejection sweep:
// the flow is decided at start and travels in the run's workflow input, so
// only a fresh `run` can honor it — append mode steers a pipeline whose flow
// is already fixed, a session continue resumes keeps the flow it started
// with, and no other command reads one. Each of those parses the flag,
// validates the name against the registry, and would silently drop it; all
// of them are rejected at parse time instead.
func TestParseFlagsWorkflowFreshRunOnly(t *testing.T) {
	// The honoring site the sweep must keep working: a fresh run, the flag
	// before the command included.
	f, _, err := parseFlags([]string{"-w", "bug-fix", "run", "/repo", "42", "do it"})
	if err != nil || f.workflow != "bug-fix" || !f.workflowSet {
		t.Errorf("parseFlags(-w bug-fix run …) = (%q, %v, %v), want (bug-fix, true, nil)", f.workflow, f.workflowSet, err)
	}

	const rejection = "-w/--workflow only applies to a fresh run"
	for _, args := range [][]string{
		{"run", "-a", "wf-1", "-w", "slim", "steer it"},
		{"-w", "slim", "run", "-a", "wf-1", "steer it"},
		{"continue", "-w", "bug-fix", "wf-1", "resume it"},
		{"-w", "bug-fix", "continue", "wf-1", "resume it"},
		{"guide", "-w", "slim", "wf-1", "hello"},
		{"-w", "slim", "list"},
		{"wipe", "-w", "slim", "wf-1"},
	} {
		if _, _, err := parseFlags(args); err == nil || err.Error() != rejection {
			t.Errorf("parseFlags(%v) err = %v, want %q", args, err, rejection)
		}
	}
}

// TestWorkflowRegistryFlowPolicies pins the flow registry as the CLI's
// dispatch surface: every entry carries a workflow function, and the
// per-flow write-scope policies are exactly what the workflows verify
// structurally before finalize — docs-only for investigate, the shared
// test-path policy allowed for test-only and frozen for refactor, and no
// policy at all for feature-dev, bug-fix, and dev-only.
func TestWorkflowRegistryFlowPolicies(t *testing.T) {
	for name, spec := range workflowRegistry {
		if spec.Fn == nil {
			t.Errorf("flow %q carries no workflow function", name)
		}
	}
	inv := workflowRegistry["investigate"]
	if len(inv.AllowedPaths) != 2 || inv.AllowedPaths[0] != "*.md" || inv.AllowedPaths[1] != "docs/" || len(inv.FrozenPaths) != 0 {
		t.Errorf("investigate policy = %+v, want docs-only allowed paths", inv)
	}
	to := workflowRegistry["test-only"]
	if !reflect.DeepEqual(to.AllowedPaths, activities.TestPathPatterns) || len(to.FrozenPaths) != 0 {
		t.Errorf("test-only policy = %+v, want the shared test-path patterns allowed", to)
	}
	rf := workflowRegistry["refactor"]
	if len(rf.AllowedPaths) != 0 || !reflect.DeepEqual(rf.FrozenPaths, activities.TestPathPatterns) {
		t.Errorf("refactor policy = %+v, want the shared test-path patterns frozen", rf)
	}
	for _, name := range []string{"feature-dev", "bug-fix", "dev-only", "slim"} {
		if spec := workflowRegistry[name]; len(spec.AllowedPaths) != 0 || len(spec.FrozenPaths) != 0 {
			t.Errorf("%s policy = %+v, want unrestricted", name, spec)
		}
	}
}

// TestParseFlagsWorkerType pins the -t/--type run config: both spellings
// parse into workerType, a value outside {dev, test} is rejected at parse
// time (naming the valid ones), and the record-driven worker commands —
// which revive the daemon untyped, since a run config is never persisted —
// refuse the flag outright, while start, bare restart, and foreground
// accept it.
func TestParseFlagsWorkerType(t *testing.T) {
	t.Run("accepted spellings set workerType", func(t *testing.T) {
		for _, c := range []struct {
			args []string
			want string
		}{
			{[]string{"worker", "start", "-t", "dev"}, workerTypeDev},
			{[]string{"worker", "start", "--type", "test"}, workerTypeTest},
			{[]string{"worker", "start", "--type=dev"}, workerTypeDev},
			{[]string{"worker", "restart", "-t", "test"}, workerTypeTest},
			{[]string{"worker", "foreground", "-t", "dev"}, workerTypeDev},
			// Bare `worker` is start's spelling: a typed-worker line too.
			{[]string{"worker", "-t", "dev"}, workerTypeDev},
			{[]string{"-t", "dev", "worker", "start"}, workerTypeDev},
		} {
			f, _, err := parseFlags(c.args)
			if err != nil {
				t.Errorf("parseFlags(%v): %v", c.args, err)
				continue
			}
			if f.workerType != c.want {
				t.Errorf("parseFlags(%v) workerType = %q, want %q", c.args, f.workerType, c.want)
			}
		}
	})

	t.Run("an unknown value is rejected with the valid ones named", func(t *testing.T) {
		_, _, err := parseFlags([]string{"worker", "start", "-t", "both"})
		if err == nil || !strings.Contains(err.Error(), `unknown worker type "both"`) ||
			!strings.Contains(err.Error(), "dev") || !strings.Contains(err.Error(), "test") {
			t.Errorf("parseFlags(-t both) err = %v, want an unknown-worker-type rejection naming dev and test", err)
		}
	})

	t.Run("a missing value is rejected", func(t *testing.T) {
		_, _, err := parseFlags([]string{"worker", "start", "-t"})
		if err == nil || !strings.Contains(err.Error(), "requires a value") {
			t.Errorf("parseFlags(-t) err = %v, want a requires-a-value rejection", err)
		}
	})

	const rejection = "-t/--type does not apply to worker restart all, restart <worker>, or worker status"
	t.Run("rejected on the record-driven worker commands", func(t *testing.T) {
		for _, args := range [][]string{
			{"worker", "restart", "all", "-t", "dev"},
			{"-t", "dev", "worker", "restart", "all"},
			{"worker", "restart", "alpha", "-t", "dev"},
			{"-t", "dev", "worker", "restart", "alpha"},
			{"worker", "status", "-t", "dev"},
			{"worker", "status", "--type=test"},
		} {
			if _, _, err := parseFlags(args); err == nil || err.Error() != rejection {
				t.Errorf("parseFlags(%v) err = %v, want %q", args, err, rejection)
			}
		}
	})

	// A typed daemon serves only its own poller, but a run always needs the
	// main-queue one — the worker preflight starts its daemon untyped, so
	// the flag has nothing to do on run and is rejected at parse time.
	const runRejection = "-t/--type does not apply to daedalus run — the worker it starts comes untyped (both pollers); type one with `daedalus worker start -t <type>`"
	t.Run("rejected on run", func(t *testing.T) {
		for _, args := range [][]string{
			{"run", "-t", "dev"},
			{"-t", "dev", "run"},
			{"run", "--type=test"},
		} {
			if _, _, err := parseFlags(args); err == nil || err.Error() != runRejection {
				t.Errorf("parseFlags(%v) err = %v, want %q", args, err, runRejection)
			}
		}
	})

	// The sweep's tail: stop drains whatever daemon is running, wakeup
	// interrupts one session's heartbeat, and no non-worker command reads a
	// daemon's type — a -t there is rejected like every other misplaced
	// spelling instead of parsing into a value nothing reads.
	const tailRejection = "-t/--type only applies to worker start, bare worker restart, and worker foreground"
	t.Run("rejected on stop, wakeup, and every non-worker command", func(t *testing.T) {
		for _, args := range [][]string{
			{"worker", "stop", "-t", "dev"},
			{"-t", "dev", "worker", "stop"},
			{"worker", "stop", "--type=test"},
			{"worker", "wakeup", "-t", "dev", "wf-1"},
			{"worker", "wakeup", "wf-1", "--type=test"},
			{"list", "-t", "dev"},
			{"wipe", "-t", "dev", "wf-1"},
			{"log", "-t", "dev", "wf-1"},
		} {
			if _, _, err := parseFlags(args); err == nil || err.Error() != tailRejection {
				t.Errorf("parseFlags(%v) err = %v, want %q", args, err, tailRejection)
			}
		}
	})
}

// TestParseFlagsYes pins the --yes surface: it belongs to `wipe` alone —
// where it parses in any argument position — and any other command
// carrying it is rejected at parse time, before a config is resolved or a
// client dialed. A destructive-erase switch that silently attached to, say,
// `run` or `list` would skip a confirmation that was never meant to exist
// there.
func TestParseFlagsYes(t *testing.T) {
	for _, args := range [][]string{
		{"wipe", "--yes", "daedalus-issue-42"},
		{"wipe", "daedalus-issue-42", "--yes"},
		{"--yes", "wipe", "daedalus-issue-42"},
	} {
		f, rest, err := parseFlags(args)
		if err != nil {
			t.Errorf("parseFlags(%v): %v", args, err)
			continue
		}
		if !f.yes {
			t.Errorf("parseFlags(%v) yes = false, want true", args)
		}
		if !reflect.DeepEqual(rest, []string{"wipe", "daedalus-issue-42"}) {
			t.Errorf("parseFlags(%v) rest = %v, want [wipe daedalus-issue-42]", args, rest)
		}
	}

	f, _, err := parseFlags([]string{"wipe", "daedalus-issue-42"})
	if err != nil || f.yes {
		t.Errorf("parseFlags(wipe without --yes): yes = %v, err = %v, want false/nil", f.yes, err)
	}

	const rejection = "--yes only applies to daedalus wipe <workflow-id>"
	for _, args := range [][]string{
		{"run", "--yes", "/repo", "42", "do it"},
		{"--yes", "list"},
		{"log", "--status", "--yes", "daedalus-issue-42"},
	} {
		if _, _, err := parseFlags(args); err == nil || err.Error() != rejection {
			t.Errorf("parseFlags(%v) err = %v, want %q", args, err, rejection)
		}
	}
}

// TestParseFlagsSkipPreflight pins the preflight opt-out's flag surface:
// both the flag-before-command and trailing spellings parse into
// skipPreflight on a fresh run, it stays off by default, and every other
// command shape — append mode (the baseline was validated or skipped
// before the pipeline existed), a continue, and the non-run commands — is
// rejected at parse time instead of silently dropping the flag.
func TestParseFlagsSkipPreflight(t *testing.T) {
	for _, args := range [][]string{
		{"--skip-preflight", "run", "/repo", "42", "do it"},
		{"run", "--skip-preflight", "/repo", "42", "do it"},
	} {
		f, rest, err := parseFlags(args)
		if err != nil {
			t.Errorf("parseFlags(%v): %v", args, err)
			continue
		}
		if !f.skipPreflight {
			t.Errorf("parseFlags(%v) skipPreflight = false, want true", args)
		}
		if !reflect.DeepEqual(rest, []string{"run", "/repo", "42", "do it"}) {
			t.Errorf("parseFlags(%v) rest = %v, want [run /repo 42 do it]", args, rest)
		}
	}

	f, _, err := parseFlags([]string{"run", "/repo", "42", "do it"})
	if err != nil || f.skipPreflight {
		t.Errorf("parseFlags(run without --skip-preflight): skipPreflight = %v, err = %v, want false/nil", f.skipPreflight, err)
	}

	const rejection = "--skip-preflight only applies to a fresh run"
	for _, args := range [][]string{
		{"run", "-a", "wf-1", "--skip-preflight", "steer it"},
		{"--skip-preflight", "run", "-a", "wf-1", "steer it"},
		{"continue", "--skip-preflight", "wf-1", "resume it"},
		{"--skip-preflight", "list"},
		{"log", "--status", "--skip-preflight", "daedalus-issue-42"},
	} {
		if _, _, err := parseFlags(args); err == nil || err.Error() != rejection {
			t.Errorf("parseFlags(%v) err = %v, want %q", args, err, rejection)
		}
	}
}

// TestParseFlagsFolder pins the folder-grant flag surface: -folder/--folder
// is repeatable and accumulates in argument order (the "=" spelling
// included), a missing value is a parse error, and the grants are refused
// outside a fresh `run` — the worker no longer takes them (they travel with
// the run's workflow input), and append mode targets a pipeline whose
// write scope is already fixed.
func TestParseFlagsFolder(t *testing.T) {
	for _, c := range []struct {
		args     []string
		want     []string
		wantRest []string
	}{
		{[]string{"run", "-folder", "/a", "/repo", "42", "do it"}, []string{"/a"}, []string{"run", "/repo", "42", "do it"}},
		{[]string{"run", "--folder", "/a", "--folder", "/b", "/repo", "42", "do it"}, []string{"/a", "/b"}, []string{"run", "/repo", "42", "do it"}},
		{[]string{"run", "-folder", "/b", "--folder=/a", "/repo", "42", "do it"}, []string{"/b", "/a"}, []string{"run", "/repo", "42", "do it"}},
		{[]string{"--folder=/a", "run", "/repo", "42", "do it"}, []string{"/a"}, []string{"run", "/repo", "42", "do it"}},
	} {
		f, rest, err := parseFlags(c.args)
		if err != nil {
			t.Errorf("parseFlags(%v): %v", c.args, err)
			continue
		}
		if !reflect.DeepEqual(f.folders, c.want) {
			t.Errorf("parseFlags(%v) folders = %v, want %v", c.args, f.folders, c.want)
		}
		if !reflect.DeepEqual(rest, c.wantRest) {
			t.Errorf("parseFlags(%v) rest = %v, want %v", c.args, rest, c.wantRest)
		}
	}

	if _, _, err := parseFlags([]string{"run", "-folder"}); err == nil {
		t.Error("-folder without a value should error")
	}
	// Grants name a fresh-run option; the worker would silently ignore them.
	if _, _, err := parseFlags([]string{"worker", "start", "-folder", "/a"}); err == nil ||
		!strings.Contains(err.Error(), "only applies to a fresh run") {
		t.Errorf("parseFlags(worker -folder) err = %v, want a fresh-run-only rejection", err)
	}
	// Append mode targets a pipeline whose grants are already fixed.
	if _, _, err := parseFlags([]string{"run", "-a", "wf-1", "-folder", "/a", "steer it"}); err == nil ||
		!strings.Contains(err.Error(), "only applies to a fresh run") {
		t.Errorf("parseFlags(run -a -folder) err = %v, want a fresh-run-only rejection", err)
	}
}

// TestParseFlagsDepends pins the -dep/--depends surface: it parses in any
// argument position, in both spelling forms (the = form included), and is
// a fresh-run option like -folder — the chain is decided at submit, so
// append mode and the worker commands reject it at parse time.
func TestParseFlagsDepends(t *testing.T) {
	for _, c := range []struct {
		args     []string
		want     string
		wantRest []string
	}{
		{[]string{"run", "-dep", "wf-1", "/repo", "42", "do it"}, "wf-1", []string{"run", "/repo", "42", "do it"}},
		{[]string{"run", "--depends", "wf-1", "/repo", "42", "do it"}, "wf-1", []string{"run", "/repo", "42", "do it"}},
		{[]string{"run", "--depends=wf-1", "/repo", "42", "do it"}, "wf-1", []string{"run", "/repo", "42", "do it"}},
		{[]string{"--depends=wf-1", "run", "/repo", "42", "do it"}, "wf-1", []string{"run", "/repo", "42", "do it"}},
	} {
		f, rest, err := parseFlags(c.args)
		if err != nil {
			t.Errorf("parseFlags(%v): %v", c.args, err)
			continue
		}
		if f.depends != c.want {
			t.Errorf("parseFlags(%v) depends = %q, want %q", c.args, f.depends, c.want)
		}
		if !reflect.DeepEqual(rest, c.wantRest) {
			t.Errorf("parseFlags(%v) rest = %v, want %v", c.args, rest, c.wantRest)
		}
	}

	if _, _, err := parseFlags([]string{"run", "-dep"}); err == nil {
		t.Error("-dep without a value should error")
	}
	// The chain is decided at submit; a worker command can never honor it.
	if _, _, err := parseFlags([]string{"worker", "start", "-dep", "wf-1"}); err == nil ||
		!strings.Contains(err.Error(), "only applies to a fresh run") {
		t.Errorf("parseFlags(worker -dep) err = %v, want a fresh-run-only rejection", err)
	}
	// Append mode steers a pipeline whose dependency was already resolved.
	if _, _, err := parseFlags([]string{"run", "-a", "wf-1", "--depends", "wf-2", "steer it"}); err == nil ||
		!strings.Contains(err.Error(), "only applies to a fresh run") {
		t.Errorf("parseFlags(run -a -dep) err = %v, want a fresh-run-only rejection", err)
	}
}

// TestRestartForceScoping pins the restart scope's force flag surface:
// -f/--force parse as force — in either spelling, anywhere after the
// `worker restart` prefix, consuming no value — while everywhere else -f
// keeps its global --file meaning and --force is not a flag at all. The
// record-driven rejections (-c, -t) still fire with force flags on the
// line, since those flags never reach rest.
func TestRestartForceScoping(t *testing.T) {
	t.Run("accepted in the restart scope in either spelling", func(t *testing.T) {
		for _, c := range []struct {
			args     []string
			wantRest []string
		}{
			{[]string{"worker", "restart", "-f"}, []string{"worker", "restart"}},
			{[]string{"worker", "restart", "--force"}, []string{"worker", "restart"}},
			{[]string{"worker", "restart", "alpha", "--force"}, []string{"worker", "restart", "alpha"}},
			{[]string{"worker", "restart", "all", "-f"}, []string{"worker", "restart", "all"}},
			// Force rides alongside the bare restart's -t, and the flag
			// may precede the command like any global one.
			{[]string{"worker", "restart", "-f", "-t", "dev"}, []string{"worker", "restart"}},
			{[]string{"-c", "cfg.yaml", "worker", "restart", "-f"}, []string{"worker", "restart"}},
		} {
			f, rest, err := parseFlags(c.args)
			if err != nil {
				t.Errorf("parseFlags(%v): %v", c.args, err)
				continue
			}
			if !f.force {
				t.Errorf("parseFlags(%v) force = false, want true", c.args)
			}
			if f.taskFile != "" {
				t.Errorf("parseFlags(%v) taskFile = %q — -f must not keep its file meaning in the restart scope", c.args, f.taskFile)
			}
			if !reflect.DeepEqual(rest, c.wantRest) {
				t.Errorf("parseFlags(%v) rest = %v, want %v", c.args, rest, c.wantRest)
			}
		}
	})

	// -f at the line's end must not demand a value there: in the restart
	// scope it is the boolean force, not run's value-taking --file.
	if _, _, err := parseFlags([]string{"worker", "restart", "-f"}); err != nil {
		t.Errorf("parseFlags(worker restart -f): %v, want the boolean force parsed", err)
	}

	t.Run("outside the scope -f stays the global file", func(t *testing.T) {
		f, _, err := parseFlags([]string{"-f", "notes.md", "run", "/repo", "42", "do it"})
		if err != nil {
			t.Fatalf("parseFlags(-f before run): %v", err)
		}
		if f.taskFile != "notes.md" || f.force {
			t.Errorf("parseFlags(-f notes.md run …) = (taskFile %q, force %v), want (notes.md, false)", f.taskFile, f.force)
		}
		// A value-taking -f elsewhere is its old self, rejection included.
		if _, _, err := parseFlags([]string{"worker", "start", "--file", "x"}); err == nil ||
			!strings.Contains(err.Error(), "-f/--file only applies to run") {
			t.Errorf("parseFlags(worker start --file) err = %v, want the run-only rejection", err)
		}
	})

	t.Run("--force is unknown outside the restart scope", func(t *testing.T) {
		f, rest, err := parseFlags([]string{"run", "--force"})
		if err != nil {
			t.Fatalf("parseFlags(run --force): %v", err)
		}
		if f.force {
			t.Error("parseFlags(run --force) force = true, want false — --force lives outside flagTable")
		}
		if len(rest) != 2 || rest[1] != "--force" {
			t.Errorf("parseFlags(run --force) rest = %v, want --force left as an unparsed positional", rest)
		}
	})

	t.Run("the record-driven rejections still fire with force present", func(t *testing.T) {
		const configRejection = "-c/--config does not apply to worker restart all, restart <worker>, or worker status"
		if _, _, err := parseFlags([]string{"worker", "restart", "all", "-f", "-c", "cfg.yaml"}); err == nil ||
			err.Error() != configRejection {
			t.Errorf("parseFlags(restart all -f -c) err = %v, want %q", err, configRejection)
		}
		const typeRejection = "-t/--type does not apply to worker restart all, restart <worker>, or worker status"
		if _, _, err := parseFlags([]string{"worker", "restart", "all", "-f", "-t", "dev"}); err == nil ||
			err.Error() != typeRejection {
			t.Errorf("parseFlags(restart all -f -t) err = %v, want %q", err, typeRejection)
		}
	})
}

// TestRestartForceHelpers pins the positional scoping machinery the force
// flag, its matcher bypass, and the arity guard share: the command
// position behind the global-flag grammar, the `worker restart` prefix
// from which force re-scopes, and the strip that leaves the positional
// shape the matchers compare.
func TestRestartForceHelpers(t *testing.T) {
	for _, c := range []struct {
		args []string
		want int
	}{
		{nil, -1},
		{[]string{"run"}, 0},
		{[]string{"-d", "run"}, 1},
		{[]string{"-c", "cfg.yaml", "worker"}, 2},
		{[]string{"--config=cfg.yaml", "worker"}, 1},
		// A trailing value flag consumes past the end: no command word.
		{[]string{"-c", "-t"}, -1},
	} {
		if got := commandPos(c.args); got != c.want {
			t.Errorf("commandPos(%v) = %d, want %d", c.args, got, c.want)
		}
	}

	for _, c := range []struct {
		args []string
		want int
	}{
		{[]string{"worker", "restart"}, 2},
		{[]string{"worker", "restart", "all"}, 2},
		{[]string{"-c", "cfg.yaml", "worker", "restart", "alpha"}, 4},
		// Anything else leaves no force scope: the length is the sentinel.
		{[]string{"worker", "start"}, 2},
		{[]string{"worker"}, 1},
		{[]string{"run"}, 1},
		{nil, 0},
	} {
		if got := restartForceFrom(c.args); got != c.want {
			t.Errorf("restartForceFrom(%v) = %d, want %d", c.args, got, c.want)
		}
	}

	for _, c := range []struct {
		args []string
		want []string
	}{
		{[]string{"worker", "restart", "-f", "all"}, []string{"worker", "restart", "all"}},
		{[]string{"worker", "restart", "--force", "alpha", "-f"}, []string{"worker", "restart", "alpha"}},
		// Before the prefix -f is run's --file: untouched.
		{[]string{"-f", "x", "worker", "restart"}, []string{"-f", "x", "worker", "restart"}},
		{[]string{"worker", "restart"}, []string{"worker", "restart"}},
	} {
		if got := stripRestartForce(c.args); !reflect.DeepEqual(got, c.want) {
			t.Errorf("stripRestartForce(%v) = %v, want %v", c.args, got, c.want)
		}
	}

	// The matchers see through the force flags wherever they sit among the
	// restart arguments.
	if !isRestartAll([]string{"worker", "restart", "all", "-f"}) ||
		!isRestartAll([]string{"worker", "restart", "-f", "all"}) ||
		!isRestartAll([]string{"worker", "restart", "all"}) {
		t.Error("isRestartAll should match the all shape with the force flags anywhere in the scope")
	}
	if name, ok := isRestartNamed([]string{"worker", "restart", "alpha", "--force"}); !ok || name != "alpha" {
		t.Errorf("isRestartNamed(trailing --force) = (%q, %v), want (alpha, true)", name, ok)
	}
	if name, ok := isRestartNamed([]string{"worker", "restart", "--force", "alpha"}); !ok || name != "alpha" {
		t.Errorf("isRestartNamed(leading --force) = (%q, %v), want (alpha, true)", name, ok)
	}
	if _, ok := isRestartNamed([]string{"worker", "restart", "-f"}); ok {
		t.Error("isRestartNamed(force flags alone) should not match — there is no worker name")
	}
}
