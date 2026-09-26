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
}

// TestWorkflowRegistryFlowPolicies pins the flow registry as the CLI's
// dispatch surface: every entry carries a workflow function, and the
// per-flow write-scope policies are exactly what the workflows verify
// structurally before finalize — docs-only for investigate, the shared
// test-path policy allowed for test-only and frozen for refactor, and no
// policy at all for feature-dev and bug-fix.
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
	for _, name := range []string{"feature-dev", "bug-fix"} {
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
			{"worker", "restart", "arete", "-t", "dev"},
			{"-t", "dev", "worker", "restart", "arete"},
			{"worker", "status", "-t", "dev"},
			{"worker", "status", "--type=test"},
		} {
			if _, _, err := parseFlags(args); err == nil || err.Error() != rejection {
				t.Errorf("parseFlags(%v) err = %v, want %q", args, err, rejection)
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
