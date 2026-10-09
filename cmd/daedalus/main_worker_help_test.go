package main

import (
	"strings"
	"testing"
)

// TestMainWorkerActionHelp pins "daedalus worker <action> --help" (and its
// -h/help spellings): every dispatched action prints its own sub-action
// screen on stdout, exit 0, answered before any config resolution — the
// screen must work where no config can be found ("worker start -h" used to
// die at the config lookup before its own dispatch). A help word after a
// word naming no action is not a screen: it fails right there with the usual
// unknown-action diagnostic, also configless — a misspelled probe never dies
// at the config lookup either. And the help word rides the action position:
// the four-token "worker restart all -h" is no screen, staying the
// too-many-actions usage failure it has always been.
func TestMainWorkerActionHelp(t *testing.T) {
	t.Setenv("HOME", t.TempDir()) // no ~/.config/daedalus/config.yaml fallback
	cwd := t.TempDir()            // no ./config.yaml or ./.daedalus/config.yaml

	for _, action := range workerActions {
		for _, spelling := range []string{"--help", "-h", "help"} {
			stdout, stderr, code := runMainIn(t, cwd, "worker", action, spelling)
			if code != 0 {
				t.Errorf("daedalus worker %s %s exit code = %d, want 0", action, spelling, code)
			}
			if stderr != "" {
				t.Errorf("daedalus worker %s %s stderr = %q, want empty", action, spelling, stderr)
			}
			if want := workerActionHelp[action]; stdout != want {
				t.Errorf("daedalus worker %s %s stdout = %q, want the action's help screen (%q)", action, spelling, stdout, want)
			}
		}
	}

	for _, spelling := range []string{"--help", "-h", "help"} {
		stdout, stderr, code := runMainIn(t, cwd, "worker", "dance", spelling)
		if code != 1 {
			t.Errorf("daedalus worker dance %s exit code = %d, want 1", spelling, code)
		}
		if stdout != "" {
			t.Errorf("daedalus worker dance %s stdout = %q, want empty", spelling, stdout)
		}
		if !strings.HasPrefix(stderr, `unknown worker action "dance" (start, stop, status, restart, wakeup, foreground)`+"\n\n") {
			t.Errorf("daedalus worker dance %s stderr should start with the unknown-action diagnostic and a blank line, got %q", spelling, stderr)
		}
		if !strings.HasSuffix(stderr, usage) {
			t.Errorf("daedalus worker dance %s stderr should end with the usage text, got %q", spelling, stderr)
		}
	}

	// No screen answers four tokens — neither "all" sitting where the help
	// word would have to be (the plain worker-dispatch arity rejection) nor
	// a help word mid-line with trailing junk after it: a screen only ever
	// answers a three-token worker line.
	for _, c := range []struct {
		args []string
	}{
		{[]string{"worker", "restart", "all", "-h"}},
		{[]string{"worker", "start", "-h", "extra"}},
	} {
		stdout, stderr, code := runMainIn(t, cwd, append([]string{"-c", validConfig(t)}, c.args...)...)
		if code != 1 {
			t.Errorf("daedalus %v exit code = %d, want 1", c.args, code)
		}
		if stdout != "" {
			t.Errorf("daedalus %v stdout = %q, want empty", c.args, stdout)
		}
		if !strings.HasPrefix(stderr, "worker takes at most one action\n\n") {
			t.Errorf("daedalus %v stderr should start with the too-many-actions rejection, got %q", c.args, stderr)
		}
		if !strings.HasSuffix(stderr, usage) {
			t.Errorf("daedalus %v stderr should end with the usage text, got %q", c.args, stderr)
		}
	}
}

// TestWorkerActionHelpCoversActions pins the invariant the dispatch relies
// on: every action in workerActions carries a non-empty screen — the claim
// that makes the intercept's unknown-action failure unreachable for a real
// action — and the screens name nothing but dispatched actions (the counts
// matching plus every action present pins the key sets equal).
func TestWorkerActionHelpCoversActions(t *testing.T) {
	if len(workerActionHelp) != len(workerActions) {
		t.Errorf("workerActionHelp has %d screens for %d dispatched actions", len(workerActionHelp), len(workerActions))
	}
	for _, action := range workerActions {
		if workerActionHelp[action] == "" {
			t.Errorf("worker action %q has no help screen — \"worker %s --help\" would fail as an unknown action", action, action)
		}
	}
}

// TestWorkerActionHelpFlagClaims pins the sub-action screens' claims against
// the behavior pinned elsewhere (TestParseFlagsWorkerType), the way
// TestRootHelpDispatch pins the root usage's claims: the stop and wakeup
// screens must say -t/--type is rejected there — parseFlags rejects it — and
// never regress to the stale "parses here but has no effect" inertness
// wording that misdocumented the flag for exactly those actions; and the
// status screen's output enumeration must name every column the listing
// prints, the CODE PATH column included.
func TestWorkerActionHelpFlagClaims(t *testing.T) {
	for _, action := range []string{"stop", "wakeup"} {
		screen := workerActionHelp[action]
		if !strings.Contains(screen, "-t/--type is rejected here") {
			t.Errorf("worker %s --help should say -t/--type is rejected there (parseFlags rejects it), got:\n%s", action, screen)
		}
		if strings.Contains(screen, "no effect") {
			t.Errorf("worker %s --help still carries the stale inertness wording:\n%s", action, screen)
		}
	}

	status := workerActionHelp["status"]
	for _, column := range []string{"name", "pid", "version", "API", "config", "code path", "log"} {
		if !strings.Contains(status, column) {
			t.Errorf("worker status --help output enumeration omits the %q column the listing prints:\n%s", column, status)
		}
	}
}

// TestRootHelpDispatch pins the root screen's dispatch — -h/--help/help
// print exactly the usage text on stdout, exit 0, configless — plus the
// changed surface's own promises: the GLOBAL FLAGS -c entry names the
// commands that reject it, and the worker sub-action path the root screen
// advertises is the one TestMainWorkerActionHelp proves works.
func TestRootHelpDispatch(t *testing.T) {
	t.Setenv("HOME", t.TempDir()) // no ~/.config/daedalus/config.yaml fallback
	cwd := t.TempDir()            // no ./config.yaml or ./.daedalus/config.yaml

	for _, spelling := range []string{"-h", "--help", "help"} {
		stdout, stderr, code := runMainIn(t, cwd, spelling)
		if code != 0 {
			t.Errorf("daedalus %s exit code = %d, want 0", spelling, code)
		}
		if stderr != "" {
			t.Errorf("daedalus %s stderr = %q, want empty", spelling, stderr)
		}
		if stdout != usage {
			t.Errorf("daedalus %s stdout = %q, want the root usage text (%q)", spelling, stdout, usage)
		}
	}

	for _, claim := range []string{
		"rejected by log and the record-driven worker commands",
		`"daedalus worker <action> --help"`,
	} {
		if !strings.Contains(usage, claim) {
			t.Errorf("the root usage should carry the claim %q — the behavior it documents is pinned elsewhere", claim)
		}
	}
}
