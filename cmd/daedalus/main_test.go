package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/version"
)

// TestParseFlagsDetach covers both spellings and that -d never consumes a
// positional argument.
func TestParseFlagsDetach(t *testing.T) {
	for _, flag := range []string{"-d", "--detach"} {
		f, rest, err := parseFlags([]string{"run", flag, "/repo", "42", "do it"})
		if err != nil {
			t.Fatalf("parseFlags(%q): %v", flag, err)
		}
		if !f.detach {
			t.Errorf("parseFlags(%q) detach = false, want true", flag)
		}
		wantRest := []string{"run", "/repo", "42", "do it"}
		if !reflect.DeepEqual(rest, wantRest) {
			t.Errorf("parseFlags(%q) rest = %v, want %v", flag, rest, wantRest)
		}
	}

	f, rest, err := parseFlags([]string{"run", "/repo", "42", "do it"})
	if err != nil {
		t.Fatalf("parseFlags(no -d): %v", err)
	}
	if f.detach {
		t.Error("detach should default to false")
	}
	if len(rest) != 4 {
		t.Errorf("parseFlags(no -d) rest = %v", rest)
	}
}

// TestParseFlagsAppend covers both spellings and that -a consumes exactly
// one value while the prompt stays positional.
func TestParseFlagsAppend(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"run", "-a", "daedalus-issue-0", "steer it"}, "daedalus-issue-0"},
		{[]string{"run", "--append", "daedalus-issue-0", "steer it"}, "daedalus-issue-0"},
		{[]string{"run", "--append=daedalus-issue-1", "steer it"}, "daedalus-issue-1"},
	} {
		f, rest, err := parseFlags(c.args)
		if err != nil {
			t.Fatalf("parseFlags(%q): %v", c.args, err)
		}
		if f.appendID != c.want {
			t.Errorf("parseFlags(%q) appendID = %q, want %q", c.args, f.appendID, c.want)
		}
		wantRest := []string{"run", "steer it"}
		if !reflect.DeepEqual(rest, wantRest) {
			t.Errorf("parseFlags(%q) rest = %v, want %v", c.args, rest, wantRest)
		}
	}

	if _, _, err := parseFlags([]string{"run", "-a"}); err == nil {
		t.Error("-a without a value should error")
	}
}

// TestParseFlagsPrefix covers both spellings of the run prefix flag, that an
// unusable prefix is rejected at parse time, and that -p consumes exactly one
// value.
func TestParseFlagsPrefix(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"run", "-p", "team", "/repo", "42", "do it"}, "team"},
		{[]string{"run", "--prefix", "team", "/repo", "42", "do it"}, "team"},
		{[]string{"run", "--prefix=team/ship", "/repo", "42", "do it"}, "team/ship"},
	} {
		f, rest, err := parseFlags(c.args)
		if err != nil {
			t.Fatalf("parseFlags(%q): %v", c.args, err)
		}
		if f.branchPrefix != c.want {
			t.Errorf("parseFlags(%q) branchPrefix = %q, want %q", c.args, f.branchPrefix, c.want)
		}
		wantRest := []string{"run", "/repo", "42", "do it"}
		if !reflect.DeepEqual(rest, wantRest) {
			t.Errorf("parseFlags(%q) rest = %v, want %v", c.args, rest, wantRest)
		}
	}

	if _, _, err := parseFlags([]string{"run", "-p"}); err == nil {
		t.Error("-p without a value should error")
	}
	// A reserved namespace must fail here, not at finalize after the run.
	if _, _, err := parseFlags([]string{"run", "-p", "feat", "/repo", "42", "do it"}); err == nil ||
		!strings.Contains(err.Error(), "reserved") {
		t.Errorf("parseFlags(-p feat) err = %v, want a reserved-namespace rejection", err)
	}
	// -p names a fresh-run option; elsewhere — append mode included — it
	// would be silently ignored.
	if _, _, err := parseFlags([]string{"guide", "-p", "team", "wf-1", "hi"}); err == nil ||
		!strings.Contains(err.Error(), "only applies to run") {
		t.Errorf("parseFlags(guide -p) err = %v, want a -p-outside-run rejection", err)
	}
	if _, _, err := parseFlags([]string{"run", "-a", "wf-1", "-p", "team", "steer it"}); err == nil ||
		!strings.Contains(err.Error(), "only applies to run") {
		t.Errorf("parseFlags(run -a -p) err = %v, want a -p-in-append-mode rejection", err)
	}
}

// TestResolveConfigPath pins the discovery order: an explicit -c is honored
// verbatim, ./config.yaml wins over the home fallback, and the home config
// (~/.config/daedalus/config.yaml) is found when the working directory has
// none — the property that lets the CLI run from any directory.
func TestResolveConfigPath(t *testing.T) {
	for _, c := range []struct {
		name string
		// writeCwdConfig and writeHomeConfig seed the two lookup locations
		// for this case; each subtest gets a fresh home and workdir, so
		// cases cannot see each other's configs.
		writeCwdConfig  bool
		writeHomeConfig bool
		given           string
		// wantCwdConfig/wantHomeConfig name that location's config.yaml as
		// the expected result (its path is only known inside the subtest).
		wantCwdConfig    bool
		wantHomeConfig   bool
		want             string
		wantErrSubstring string
	}{
		{
			name:  "explicit -c is verbatim",
			given: "/elsewhere/config.yaml",
			want:  "/elsewhere/config.yaml",
		},
		{
			name:            "cwd config wins over home",
			writeCwdConfig:  true,
			writeHomeConfig: true,
			wantCwdConfig:   true,
		},
		{
			name:            "home config found from a bare directory",
			writeHomeConfig: true,
			wantHomeConfig:  true,
		},
		{
			name:             "no config anywhere",
			wantErrSubstring: "no config found",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			homeCfg := filepath.Join(home, homeConfigDir, defaultConfigPath)
			if err := os.MkdirAll(filepath.Dir(homeCfg), 0o755); err != nil {
				t.Fatal(err)
			}
			workdir := t.TempDir()
			if c.writeCwdConfig {
				writeConfig(t, filepath.Join(workdir, defaultConfigPath))
			}
			if c.writeHomeConfig {
				writeConfig(t, homeCfg)
			}
			t.Chdir(workdir)

			// Cases without an explicit given exercise the parseFlags
			// default, never an empty string (Abs("") would be the cwd).
			given := c.given
			if given == "" {
				given = defaultConfigPath
			}
			got, err := resolveConfigPath(given)
			if c.wantErrSubstring != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErrSubstring) {
					t.Errorf("resolveConfigPath error = %v, want it to contain %q", err, c.wantErrSubstring)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveConfigPath: %v", err)
			}
			var want string
			switch {
			case c.wantCwdConfig:
				want = filepath.Join(workdir, defaultConfigPath)
			case c.wantHomeConfig:
				want = homeCfg
			default:
				want = c.want
			}
			if got != want {
				t.Errorf("resolveConfigPath = %q, want %q", got, want)
			}
		})
	}
}

// writeConfig drops a minimal readable config file at path.
func writeConfig(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("agent: claude\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestPruneOldLogs pins the retention rule: logs untouched for over a week
// are removed, recent ones and non-log files stay.
func TestPruneOldLogs(t *testing.T) {
	// daemonDir is global; restore it to a throwaway dir rather than its
	// original value, so no later test can touch the real daemon dir.
	reset := t.TempDir()
	t.Cleanup(func() { daemonDir = reset })
	daemonDir = t.TempDir()

	fresh := filepath.Join(daemonDir, "worker-daedalus.log")
	stale := filepath.Join(daemonDir, "worker-old.log")
	other := filepath.Join(daemonDir, "worker-daedalus.pid")
	for _, f := range []string{fresh, stale, other} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	lastWeek := time.Now().Add(-8 * 24 * time.Hour)
	if err := os.Chtimes(stale, lastWeek, lastWeek); err != nil {
		t.Fatal(err)
	}

	pruneOldLogs()

	for _, keep := range []string{fresh, other} {
		if _, err := os.Stat(keep); err != nil {
			t.Errorf("%s should survive the prune: %v", filepath.Base(keep), err)
		}
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale log should be pruned (stat err = %v)", err)
	}
}

// TestReadLivePid pins the pid-file contract: a live pid reads back, a stale
// file (dead pid or garbage) does not.
func TestReadLivePid(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "worker-daedalus.pid")

	if err := os.WriteFile(pidFile, fmt.Appendf(nil, "%d", os.Getpid()), 0o644); err != nil {
		t.Fatal(err)
	}
	if pid, ok := readLivePid(pidFile); !ok || pid != os.Getpid() {
		t.Errorf("readLivePid = (%d, %v), want (%d, true)", pid, ok, os.Getpid())
	}

	// A pid that cannot exist (pid 1 on this machine is alive, so use a
	// garbage file instead) must read as not-live.
	if err := os.WriteFile(pidFile, []byte("not a pid"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := readLivePid(pidFile); ok {
		t.Error("readLivePid should reject a malformed pid file")
	}
}

// TestVersionFlag covers both spellings of the version flag: the CLI prints
// the embedded version and exits cleanly, without needing a config.
func TestVersionFlag(t *testing.T) {
	for _, flag := range []string{"-v", "--version"} {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		stdout, args := os.Stdout, os.Args
		os.Stdout, os.Args = w, []string{"daedalus", flag}
		main()
		w.Close()
		os.Stdout, os.Args = stdout, args

		out, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		if want := version.String() + "\n"; string(out) != want {
			t.Errorf("daedalus %s printed %q, want %q", flag, out, want)
		}
	}
}

func TestRunPrompt(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "task.md")
	if err := os.WriteFile(file, []byte("add the feature"), 0o644); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name             string
		taskFile         string
		args             []string
		want             string
		wantErrSubstring string
	}{
		{name: "inline prompt", args: []string{"/repo", "42", "add the feature"}, want: "add the feature"},
		{name: "file instead of prompt", taskFile: file, args: []string{"/repo", "42"}, want: "add the feature"},
		{name: "file and prompt", taskFile: file, args: []string{"/repo", "42", "add the feature"}, wantErrSubstring: `not both`},
		{name: "neither", args: []string{"/repo", "42"}, wantErrSubstring: `<prompt>`},
		{name: "unreadable file", taskFile: filepath.Join(dir, "missing.md"), args: []string{"/repo", "42"}, wantErrSubstring: "read task file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runPrompt(tt.taskFile, tt.args)
			if tt.wantErrSubstring != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrSubstring) {
					t.Errorf("runPrompt error = %v, want it to contain %q", err, tt.wantErrSubstring)
				}
				return
			}
			if err != nil {
				t.Fatalf("runPrompt: %v", err)
			}
			if got != tt.want {
				t.Errorf("runPrompt = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseFlagsFile(t *testing.T) {
	for _, flag := range []string{"-f", "--file", "--file="} {
		var args []string
		if flag == "--file=" {
			args = []string{"run", flag + "task.md", "/repo", "42"}
		} else {
			args = []string{"run", flag, "task.md", "/repo", "42"}
		}
		f, rest, err := parseFlags(args)
		if err != nil {
			t.Fatalf("parseFlags(%q): %v", flag, err)
		}
		if f.taskFile != "task.md" {
			t.Errorf("parseFlags(%q) taskFile = %q, want task.md", flag, f.taskFile)
		}
		wantRest := []string{"run", "/repo", "42"}
		if len(rest) != len(wantRest) {
			t.Fatalf("parseFlags(%q) rest = %v, want %v", flag, rest, wantRest)
		}
		for i := range wantRest {
			if rest[i] != wantRest[i] {
				t.Errorf("parseFlags(%q) rest = %v, want %v", flag, rest, wantRest)
				break
			}
		}
	}
}

// TestWriteExampleConfig pins the init behavior: writes the example, refuses
// to overwrite.
func TestWriteExampleConfig(t *testing.T) {
	dir := t.TempDir()

	if err := writeExampleConfig(dir); err != nil {
		t.Fatalf("writeExampleConfig: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "config-example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != config.ExampleYAML {
		t.Error("written file should carry config.ExampleYAML verbatim")
	}

	if err := writeExampleConfig(dir); err == nil {
		t.Error("second init should refuse to overwrite")
	}
}
