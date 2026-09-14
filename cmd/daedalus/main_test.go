package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
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
