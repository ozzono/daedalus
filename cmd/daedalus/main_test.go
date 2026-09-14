package main

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"daedalus/internal/config"
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

// TestPruneOldLogs pins the retention rule: logs untouched for over a week
// are removed, recent ones and non-log files stay.
func TestPruneOldLogs(t *testing.T) {
	old := t.TempDir()
	t.Cleanup(func() { daemonDir = old })
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
