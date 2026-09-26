package main

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// TestCompleteTopLevelCommands pins the empty-line and prefix behavior of the
// probe at the command position: with nothing typed it offers every
// help-screened command exactly once, sorted — commandHelp is the single
// source, so a command added once is help-screened and completed together —
// and a partial word narrows the listing. Global flags before the cursor are
// skipped wherever they sit (the "--name=value" form included), never
// mistaken for the command.
func TestCompleteTopLevelCommands(t *testing.T) {
	var want []string
	for name := range commandHelp {
		want = append(want, name)
	}
	sort.Strings(want)

	for _, c := range []struct {
		words []string
		want  []string
	}{
		{nil, want},
		{[]string{""}, want},
		{[]string{"wo"}, []string{"worker"}},
		{[]string{"ver"}, []string{"version"}},
		{[]string{"zzz"}, nil},
		// A global flag and its value, in either spelling form, precede the
		// command and are consumed by the scan.
		{[]string{"-c", "cfg.yaml", ""}, want},
		{[]string{"--config", "cfg.yaml", ""}, want},
		{[]string{"--config=cfg.yaml", ""}, want},
		{[]string{"-d", "wo"}, []string{"worker"}},
	} {
		if got := complete(c.words); !reflect.DeepEqual(got, c.want) {
			t.Errorf("complete(%q) = %q, want %q", c.words, got, c.want)
		}
	}
}

// TestCompleteNoSecondPositional pins that the probe answers nothing once a
// command is on the line and the next position is not one it completes:
// run/continue take paths and prompts the shell's file completion serves,
// and an unknown first word is not a completion context at all.
func TestCompleteNoSecondPositional(t *testing.T) {
	for _, words := range [][]string{
		{"run", ""},
		{"run", "/repo", ""},
		{"continue", "wf-1", ""},
		{"frobnicate", ""},
		{"worker", "status", ""},
		{"worker", "restart", "arete", ""},
	} {
		if got := complete(words); got != nil {
			t.Errorf("complete(%q) = %q, want nil", words, got)
		}
	}
}

// TestCompleteWorkerActions pins the worker subcommand position: the probe
// offers exactly the dispatched workerActions (sorted) — the same table the
// unknown-action diagnostic names — filtered by the partial word, and
// nothing for a word that dispatch would reject.
func TestCompleteWorkerActions(t *testing.T) {
	sorted := append([]string(nil), workerActions...)
	sort.Strings(sorted)

	for _, c := range []struct {
		words []string
		want  []string
	}{
		{[]string{"worker", ""}, sorted},
		{[]string{"-c", "cfg.yaml", "worker", ""}, sorted},
		{[]string{"worker", "re"}, []string{"restart"}},
		{[]string{"worker", "sta"}, []string{"start", "status"}},
		{[]string{"worker", "dance"}, nil},
	} {
		if got := complete(c.words); !reflect.DeepEqual(got, c.want) {
			t.Errorf("complete(%q) = %q, want %q", c.words, got, c.want)
		}
	}
}

// TestCompleteRestartTargets pins the record-driven restart-target position:
// the record-spelling "all" plus every worker with a config record on file,
// running or not — read from daemonDir at completion time, so a worker
// started later is offered without reinstalling anything. A missing daemon
// directory is nothing on record, not an error: "all" alone is offered.
func TestCompleteRestartTargets(t *testing.T) {
	reset := daemonDir
	t.Cleanup(func() { daemonDir = reset })
	daemonDir = t.TempDir()
	for _, name := range []string{"arete", "forge"} {
		if err := os.WriteFile(filepath.Join(daemonDir, "worker-"+name+".conf"), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Non-record files in the daemon directory are not worker names.
	if err := os.WriteFile(filepath.Join(daemonDir, "worker-arete.pid"), []byte("123\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		words []string
		want  []string
	}{
		{[]string{"worker", "restart", ""}, []string{"all", "arete", "forge"}},
		{[]string{"worker", "restart", "al"}, []string{"all"}},
		{[]string{"worker", "restart", "ar"}, []string{"arete"}},
		{[]string{"worker", "restart", "zz"}, nil},
	} {
		if got := complete(c.words); !reflect.DeepEqual(got, c.want) {
			t.Errorf("complete(%q) = %q, want %q", c.words, got, c.want)
		}
	}

	daemonDir = filepath.Join(t.TempDir(), "gone")
	if got := complete([]string{"worker", "restart", ""}); !reflect.DeepEqual(got, []string{"all"}) {
		t.Errorf("complete(restart, missing daemon dir) = %q, want [all]", got)
	}
}

// TestCompleteFlags pins the flag position: every unused spelling matching
// the partial word is offered, sorted; a flag already used — by either of
// its spellings — is suppressed, so the same option is never offered twice;
// and the word after a value-taking flag is that flag's value, which the
// probe answers with nothing (the shell's file completion serves it).
func TestCompleteFlags(t *testing.T) {
	allFlags := make([]string, 0, len(flagTable))
	for spelling := range flagTable {
		allFlags = append(allFlags, spelling)
	}
	sort.Strings(allFlags)

	for _, c := range []struct {
		words []string
		want  []string
	}{
		{[]string{"-"}, allFlags},
		{[]string{"--co"}, []string{"--config", "--cot"}},
		{[]string{"run", "--det"}, []string{"--detach"}},
		// --yes already used: neither spelling is offered again.
		{[]string{"wipe", "wf-1", "--yes", "-"}, withoutNames(allFlags, "yes")},
		// -c already used: neither spelling is offered again, every other
		// long spelling is.
		{[]string{"wipe", "wf-1", "-c", "cfg.yaml", "--"}, []string{"--append", "--cli", "--cot", "--detach", "--file", "--prefix", "--status", "--type", "--workflow", "--yes"}},
		// The word after a value-taking flag is being completed as that
		// flag's value — no candidates.
		{[]string{"-f", ""}, nil},
		{[]string{"--file", ""}, nil},
		{[]string{"run", "-cli", ""}, nil},
	} {
		if got := complete(c.words); !reflect.DeepEqual(got, c.want) {
			t.Errorf("complete(%q) = %q, want %q", c.words, got, c.want)
		}
	}
}

// TestCompleteRestartTargetFlags pins that the only flag offered in the
// restart-target position is --all: the record-driven restart invocations
// reject the global flags outright, so offering them would complete a line
// parseFlags hard-errors on.
func TestCompleteRestartTargetFlags(t *testing.T) {
	for _, c := range []struct {
		words []string
		want  []string
	}{
		{[]string{"worker", "restart", "--a"}, []string{"--all"}},
		{[]string{"worker", "restart", "-"}, []string{"--all"}},
		{[]string{"worker", "restart", "--config"}, nil},
	} {
		if got := complete(c.words); !reflect.DeepEqual(got, c.want) {
			t.Errorf("complete(%q) = %q, want %q", c.words, got, c.want)
		}
	}
}

// withoutNames returns spellings whose semantic flag name is not in names.
func withoutNames(spellings []string, names ...string) []string {
	drop := map[string]bool{}
	for _, n := range names {
		drop[n] = true
	}
	var out []string
	for _, s := range spellings {
		if !drop[flagTable[s].name] {
			out = append(out, s)
		}
	}
	return out
}

// TestCompletionScript pins the installer surface: bash and zsh each get a
// script that feeds the shell's tab press to the __complete probe (so an
// installed script self-updates with the binary), and an unknown shell
// yields nothing — the dispatch, not this function, is what rejects it.
func TestCompletionScript(t *testing.T) {
	bash := completionScript("bash")
	if !strings.Contains(bash, "__complete") || !strings.Contains(bash, "complete -o default") {
		t.Errorf("bash script should wire tab to __complete via complete -o default, got %q", bash)
	}
	zsh := completionScript("zsh")
	if !strings.Contains(zsh, "__complete") || !strings.Contains(zsh, "compadd") {
		t.Errorf("zsh script should wire tab to __complete via compadd, got %q", zsh)
	}
	if bash == zsh {
		t.Error("bash and zsh scripts should differ")
	}
	if got := completionScript("tcsh"); got != "" {
		t.Errorf("completionScript(tcsh) = %q, want empty", got)
	}
}

// TestMainCompleteProbe pins the hidden probe end to end in a subprocess:
// one candidate per line and nothing else, exit 0, from a directory with no
// config — the probe answers before flags are parsed or a config is
// resolved, because its words are candidates, not arguments.
func TestMainCompleteProbe(t *testing.T) {
	var want []string
	for name := range commandHelp {
		want = append(want, name)
	}
	sort.Strings(want)
	wantOut := strings.Join(want, "\n") + "\n"

	dir := t.TempDir()
	stdout, stderr, code := runMainIn(t, dir, "__complete", "")
	if code != 0 {
		t.Errorf("daedalus __complete exit code = %d, want 0", code)
	}
	if stderr != "" {
		t.Errorf("daedalus __complete stderr = %q, want empty", stderr)
	}
	if stdout != wantOut {
		t.Errorf("daedalus __complete stdout = %q, want every command one per line (%q)", stdout, wantOut)
	}

	stdout, _, code = runMainIn(t, dir, "__complete", "worker", "")
	if code != 0 || stdout != strings.Join(sortedWorkerActions(), "\n")+"\n" {
		t.Errorf("daedalus __complete worker = (exit %d) %q, want the worker actions one per line", code, stdout)
	}

	stdout, _, code = runMainIn(t, dir, "__complete", "--config=cfg.yaml", "wo")
	if code != 0 || stdout != "worker\n" {
		t.Errorf("daedalus __complete --config=cfg.yaml wo = (exit %d) %q, want worker", code, stdout)
	}
}

// TestMainCompletionCommand pins the completion subcommand end to end:
// bash/zsh print exactly completionScript's bytes with exit 0 and no
// config needed; a missing or unknown shell, or an extra argument, is a
// usage failure.
func TestMainCompletionCommand(t *testing.T) {
	stdout, stderr, code := runMainIn(t, "", "completion", "bash")
	if code != 0 || stderr != "" || stdout != completionScript("bash") {
		t.Errorf("daedalus completion bash = (exit %d) stdout %q stderr %q, want the bash script", code, stdout, stderr)
	}
	// Config-exempt like version and init: a missing -c config must not
	// stand between an rc file and its completion script.
	stdout, _, code = runMainIn(t, "", "-c", filepath.Join(t.TempDir(), "no-such.yaml"), "completion", "zsh")
	if code != 0 || stdout != completionScript("zsh") {
		t.Errorf("daedalus completion zsh (missing config) = (exit %d) %q, want the zsh script", code, stdout)
	}

	const diagnostic = "completion takes bash or zsh"
	for _, args := range [][]string{
		{"completion"},
		{"completion", "tcsh"},
		{"completion", "bash", "zsh"},
	} {
		full := append([]string{"-c", validConfig(t)}, args...)
		stdout, stderr, code := runMainIn(t, "", full...)
		if code != 1 {
			t.Errorf("daedalus %v exit code = %d, want 1", args, code)
		}
		if stdout != "" {
			t.Errorf("daedalus %v stdout = %q, want empty", args, stdout)
		}
		if !strings.HasPrefix(stderr, diagnostic+"\n\n") || !strings.HasSuffix(stderr, usage) {
			t.Errorf("daedalus %v stderr = %q, want the diagnostic then the usage text", args, stderr)
		}
	}
}

// TestMainCompletionHelp pins that completion's help screen answers the
// same --help/-h/help spellings every other dispatched command does, with
// no config of its own.
func TestMainCompletionHelp(t *testing.T) {
	for _, spelling := range []string{"--help", "-h", "help"} {
		stdout, stderr, code := runMainIn(t, "", "completion", spelling)
		if code != 0 {
			t.Errorf("daedalus completion %s exit code = %d, want 0", spelling, code)
		}
		if stderr != "" {
			t.Errorf("daedalus completion %s stderr = %q, want empty", spelling, stderr)
		}
		if stdout != commandHelp["completion"] {
			t.Errorf("daedalus completion %s stdout = %q, want the completion help screen", spelling, stdout)
		}
	}
}

// sortedWorkerActions is workerActions as the probe lists them: sorted.
func sortedWorkerActions() []string {
	sorted := append([]string(nil), workerActions...)
	sort.Strings(sorted)
	return sorted
}
