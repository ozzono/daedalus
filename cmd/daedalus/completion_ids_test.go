package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// The pins for the live workflow-id completion: the positions that take a
// daedalus workflow id offer ids from one bounded Temporal visibility
// query — running ids where only a running run can be addressed (wakeup,
// attach, guide, run's -a/-dep values), recent ones where a closed session
// still means something (continue, log, wipe) — degrading to an empty
// reply on any failure. The id sources are package vars (the daemonDir
// pattern), so every probe here runs against stubs: the suite never dials
// a real Temporal.
//
// The flag-value candidates ride flagTable entries, which capture the
// source functions at package init — stubbing the package vars does not
// reach them, so the entries themselves carry the stub.

// idSourceSpy stands in for a completion id source, handing out fixed ids
// and recording the config path each call got — the offered ids must
// answer the queue the typed line selects.
type idSourceSpy struct {
	ids   []string
	calls []string
}

func (s *idSourceSpy) source(configPath string) []string {
	s.calls = append(s.calls, configPath)
	return s.ids
}

// stubIDSources swaps the positional id sources for the given spies for
// the duration of the test; a nil spy leaves that source alone.
func stubIDSources(t *testing.T, live, recent *idSourceSpy) {
	t.Helper()
	origLive, origRecent := liveWorkflowIDs, recentWorkflowIDs
	t.Cleanup(func() { liveWorkflowIDs, recentWorkflowIDs = origLive, origRecent })
	if live != nil {
		liveWorkflowIDs = live.source
	}
	if recent != nil {
		recentWorkflowIDs = recent.source
	}
}

// stubFlagValueIDs swaps the candidates source behind run's -a/--append
// and -dep/--depends value positions, restoring the entries afterwards.
func stubFlagValueIDs(t *testing.T, spy *idSourceSpy) {
	t.Helper()
	names := []string{"-a", "--append", "-dep", "--depends"}
	saved := make(map[string]flagSpec, len(names))
	for _, n := range names {
		saved[n] = flagTable[n]
		s := flagTable[n]
		s.candidates = spy.source
		flagTable[n] = s
	}
	t.Cleanup(func() {
		for _, n := range names {
			flagTable[n] = saved[n]
		}
	})
}

// writeCompletionConfig writes a loadable config whose Temporal is
// 127.0.0.1:1 — a guaranteed-refused dial — so a listCompletionIDs path
// that reaches the client fails deterministically instead of probing
// whatever the host happens to run.
func writeCompletionConfig(t *testing.T, queue string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "agent: claude\nmetrics:\n  enabled: false\ntemporal:\n  host: 127.0.0.1:1\n  task_queue: " + queue + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestCompleteWorkflowIDPositions pins the id-taking positionals: wakeup,
// attach, and guide offer running ids; continue, log, and wipe recent
// ones; each prefix-filtered; a saturated position answers nothing; and
// the positions that take no id never consult a source at all — the
// restart targets and the command listing stay record- and table-driven.
func TestCompleteWorkflowIDPositions(t *testing.T) {
	live := &idSourceSpy{ids: []string{"wf-live-2", "wf-live-1"}}
	recent := &idSourceSpy{ids: []string{"wf-old-2", "wf-live-1", "wf-old-1"}}
	stubIDSources(t, live, recent)

	for _, c := range []struct {
		words []string
		want  []string
	}{
		{[]string{"attach", ""}, []string{"wf-live-1", "wf-live-2"}},
		{[]string{"guide", "wf-l"}, []string{"wf-live-1", "wf-live-2"}},
		{[]string{"worker", "wakeup", ""}, []string{"wf-live-1", "wf-live-2"}},
		{[]string{"continue", ""}, []string{"wf-live-1", "wf-old-1", "wf-old-2"}},
		{[]string{"log", "wf-o"}, []string{"wf-old-1", "wf-old-2"}},
		{[]string{"wipe", "zz"}, nil},
	} {
		if got := complete(c.words); !reflect.DeepEqual(got, c.want) {
			t.Errorf("complete(%q) = %q, want %q", c.words, got, c.want)
		}
	}

	// Once the id position is filled the probe answers nothing — and asks
	// no source for it.
	for _, words := range [][]string{
		{"continue", "wf-1", ""},
		{"attach", "wf-1", ""},
		{"log", "wf-1", "extra", ""},
		{"worker", "wakeup", "wf-1", ""},
	} {
		if got := complete(words); got != nil {
			t.Errorf("complete(%q) = %q, want nil past the id", words, got)
		}
	}

	// The non-id positions never reach a source: the restart targets come
	// from the daemon dir, the command list from commandHelp.
	reset := daemonDir
	t.Cleanup(func() { daemonDir = reset })
	daemonDir = t.TempDir()
	before := len(live.calls) + len(recent.calls)
	if got := complete([]string{"worker", "restart", ""}); !reflect.DeepEqual(got, []string{"all"}) {
		t.Errorf("complete(worker restart) = %q, want the record-driven targets", got)
	}
	if got := complete([]string{"run", ""}); got != nil {
		t.Errorf("complete(run) = %q, want nil — run takes paths, not ids", got)
	}
	if got := complete([]string{""}); len(got) == 0 {
		t.Error("complete(empty line) should still list the commands")
	}
	if after := len(live.calls) + len(recent.calls); after != before {
		t.Errorf("non-id positions consulted an id source (%d extra calls)", after-before)
	}
}

// TestCompleteIDSourcesGetTypedConfigPath pins that the sources resolve
// the config the typed line itself selects — -c/--config wherever it
// sits, the "=" form included — so the offered ids answer the queue the
// invocation would act on; with no -c the source gets the empty path (the
// default resolution is the source's own business).
func TestCompleteIDSourcesGetTypedConfigPath(t *testing.T) {
	recent := &idSourceSpy{ids: []string{"wf-1"}}
	stubIDSources(t, nil, recent)

	complete([]string{"-c", "other.yaml", "continue", ""})
	complete([]string{"continue", "--config=other.yaml", ""})
	complete([]string{"log", "-c", "third.yaml", ""})
	if want := []string{"other.yaml", "other.yaml", "third.yaml"}; !reflect.DeepEqual(recent.calls, want) {
		t.Errorf("id sources saw configs %v, want %v", recent.calls, want)
	}

	recent.calls = nil
	complete([]string{"continue", ""})
	if len(recent.calls) != 1 || recent.calls[0] != "" {
		t.Errorf("id source without -c saw %v, want one call with the empty path", recent.calls)
	}
}

// TestCompleteFlagValueIDs pins the value positions of run's -a/--append
// and -dep/--depends: their candidates are the live ids, prefix-filtered,
// resolved against the config typed on the line — and only run's lines
// offer them there, since run is the only command that parses those
// values.
func TestCompleteFlagValueIDs(t *testing.T) {
	spy := &idSourceSpy{ids: []string{"wf-1", "other"}}
	stubFlagValueIDs(t, spy)

	for _, c := range []struct {
		words []string
		want  []string
	}{
		{[]string{"run", "-dep", ""}, []string{"other", "wf-1"}},
		{[]string{"run", "-dep", "wf"}, []string{"wf-1"}},
		{[]string{"run", "--append", ""}, []string{"other", "wf-1"}},
		{[]string{"run", "-a", "zz"}, nil},
	} {
		if got := complete(c.words); !reflect.DeepEqual(got, c.want) {
			t.Errorf("complete(%q) = %q, want %q", c.words, got, c.want)
		}
	}

	// The value resolves against the line's own config.
	if got := complete([]string{"run", "-c", "other.yaml", "-a", ""}); !reflect.DeepEqual(got, []string{"other", "wf-1"}) {
		t.Errorf("complete(run -c other.yaml -a) = %q, want the ids", got)
	}
	if n := len(spy.calls); n == 0 || spy.calls[n-1] != "other.yaml" {
		t.Errorf("flag-value source saw configs %v, want the typed -c last", spy.calls)
	}

	// Another command's -a value position answers nothing and consults no
	// source — -a parses under run alone.
	before := len(spy.calls)
	if got := complete([]string{"wipe", "-a", ""}); got != nil {
		t.Errorf("complete(wipe -a) = %q, want nil", got)
	}
	if after := len(spy.calls); after != before {
		t.Errorf("wipe's -a value consulted an id source (%d extra calls)", after-before)
	}
}

// TestCompleteRestartForceKeepsTargets pins the restart-target position
// through a force flag: the scope's -f/--force are boolean words
// parseFlags consumes as force — they neither saturate the target
// position nor trigger the value-position machinery, so both force
// spellings leave the targets reachable, and no id source is consulted.
func TestCompleteRestartForceKeepsTargets(t *testing.T) {
	reset := daemonDir
	t.Cleanup(func() { daemonDir = reset })
	daemonDir = t.TempDir()
	for _, name := range []string{"alpha", "forge"} {
		if err := os.WriteFile(filepath.Join(daemonDir, "worker-"+name+".conf"), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	live := &idSourceSpy{}
	recent := &idSourceSpy{}
	stubIDSources(t, live, recent)

	for _, c := range []struct {
		words []string
		want  []string
	}{
		{[]string{"worker", "restart", "-f", ""}, []string{"all", "alpha", "forge"}},
		// "alp" discriminates the strip: a --force left in the tail would
		// saturate the position and answer nil, not [alpha].
		{[]string{"worker", "restart", "--force", "alp"}, []string{"alpha"}},
		{[]string{"worker", "restart", "-f", "forge"}, []string{"forge"}},
	} {
		if got := complete(c.words); !reflect.DeepEqual(got, c.want) {
			t.Errorf("complete(%q) = %q, want %q", c.words, got, c.want)
		}
	}
	if len(live.calls)+len(recent.calls) != 0 {
		t.Error("the restart-target position consulted an id source")
	}
}

// TestListCompletionIDFailurePaths pins the probe's fail-open contract:
// a config that cannot be resolved or loaded, a queue that cannot ride a
// visibility query, and a dead Temporal all answer empty — never an
// error, and never a hang past the query's budget.
func TestListCompletionIDFailurePaths(t *testing.T) {
	if got := listCompletionIDs(true, filepath.Join(t.TempDir(), "gone.yaml")); got != nil {
		t.Errorf("listCompletionIDs(missing config) = %q, want empty", got)
	}

	broken := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(broken, []byte("agent: \"unclosed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := listCompletionIDs(true, broken); got != nil {
		t.Errorf("listCompletionIDs(unloadable config) = %q, want empty", got)
	}

	// A queue that could not ride the single-quoted visibility filter is
	// never probed.
	if got := listCompletionIDs(true, writeCompletionConfig(t, "q'uote")); got != nil {
		t.Errorf("listCompletionIDs(hostile queue) = %q, want empty", got)
	}

	// A dead Temporal fails the dial (refused, fast) and answers empty —
	// the freshness-bounded recent variant included.
	start := time.Now()
	if got := listCompletionIDs(true, writeCompletionConfig(t, "daedalus")); got != nil {
		t.Errorf("listCompletionIDs(dead temporal) = %q, want empty", got)
	}
	if got := listCompletionIDs(false, writeCompletionConfig(t, "daedalus")); got != nil {
		t.Errorf("listCompletionIDs(recent, dead temporal) = %q, want empty", got)
	}
	if elapsed := time.Since(start); elapsed.Seconds() > 10 {
		t.Errorf("two dead-Temporal probes took %s — completion must never hang on Temporal", elapsed)
	}
}
