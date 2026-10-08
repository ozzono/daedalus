package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/template"
)

// TestMainInitPromptScaffold pins the `init prompt <dir>` dispatch end to
// end in a subprocess: every embedded prompt lands in the target as a
// byte-identical sample (a copy overrides nothing until edited), the guide
// is named README — not README.md, a stray .md stem stops the worker — and
// names every prompt with its data fields, the receipt goes to stdout, and
// the scaffold ends where `init prompt` does: the prompt-profile config
// example in the cwd. An existing-but-empty target is accepted (the
// missing-dir and empty-dir cases are one path).
func TestMainInitPromptScaffold(t *testing.T) {
	dir := t.TempDir()
	// An empty target dir already in place must be accepted, not refused.
	if err := os.MkdirAll(filepath.Join(dir, "prompts-override"), 0o755); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runMainIn(t, dir, "init", "prompt", "prompts-override")
	if code != 0 || stderr != "" {
		t.Fatalf("daedalus init prompt = (exit %d) stderr %q, want a clean run", code, stderr)
	}
	specs, err := template.PromptSpecs()
	if err != nil {
		t.Fatalf("PromptSpecs: %v", err)
	}
	// Two receipts: the scaffold's own, then the profiled init's write.
	wantStdout := fmt.Sprintf("scaffolded prompts-override: %d prompt samples plus the README guide\nwrote config-example.yaml — copy to config.yaml and edit\n", len(specs))
	if stdout != wantStdout {
		t.Errorf("daedalus init prompt stdout = %q, want %q", stdout, wantStdout)
	}

	target := filepath.Join(dir, "prompts-override")
	var review template.PromptSpec
	for _, s := range specs {
		data, err := os.ReadFile(filepath.Join(target, s.Name+".md"))
		if err != nil {
			t.Fatalf("read scaffolded %s.md: %v", s.Name, err)
		}
		if string(data) != s.Source {
			t.Errorf("scaffolded %s.md = %d bytes, want the embedded prompt's %d bytes byte-identical", s.Name, len(data), len(s.Source))
		}
		if s.Name == "review" {
			review = s
		}
	}
	readme, err := os.ReadFile(filepath.Join(target, "README"))
	if err != nil {
		t.Fatalf("read scaffolded README: %v", err)
	}
	for _, s := range specs {
		if !strings.Contains(string(readme), s.Name) {
			t.Errorf("scaffold README does not name prompt %q", s.Name)
		}
	}
	if !strings.Contains(string(readme), strings.Join(review.Fields, ", ")) {
		t.Errorf("scaffold README does not list review's field vocabulary %v", review.Fields)
	}
	if _, err := os.Stat(filepath.Join(target, "README.md")); !os.IsNotExist(err) {
		t.Errorf("scaffold wrote README.md (stat err %v) — a stray .md stem stops the worker", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "config-example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if want := config.ExampleYAMLFor(false, true); string(data) != want {
		t.Errorf("scaffold wrote %d bytes of config-example.yaml, want the %d of the prompt profile", len(data), len(want))
	}
}

// TestPromptScaffoldGuideSubstringCollision pins the fallback tail's
// classification as exact membership: a prompt whose name is a substring of
// a grouped name (a future short "parse" inside the grouped "slim_parse")
// must land in the guide's "Other prompts" tail — the doc promises a prompt
// no group names still lands there — not be classified grouped by a
// substring match and silently render nowhere.
func TestPromptScaffoldGuideSubstringCollision(t *testing.T) {
	specs := []template.PromptSpec{
		{Name: "slim_parse", Fields: []string{"Plan"}},
		{Name: "parse", Fields: []string{"Plan"}},
	}
	guide := promptScaffoldGuide(specs)

	_, tail, found := strings.Cut(guide, "Other prompts")
	if !found {
		t.Fatalf("guide has no Other prompts section — %q rendered nowhere:\n%s", "parse", guide)
	}
	if !strings.Contains(tail, "\n  parse") {
		t.Errorf("the Other prompts section does not list parse at a line start:\n%s", tail)
	}
	if strings.Contains(tail, "slim_parse") {
		t.Errorf("the Other prompts section lists slim_parse — a grouped prompt stays in its group:\n%s", tail)
	}
	if !strings.Contains(guide, "\n  slim_parse") {
		t.Error("the guide lost the slim flow's slim_parse entry")
	}
}

// TestMainInitPromptScaffoldRefusesNonEmpty pins the refuse-to-overwrite
// shape: a non-empty target fails the init without writing anything — no
// samples, no README, no config example — and the target's own content is
// left untouched.
func TestMainInitPromptScaffoldRefusesNonEmpty(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "busy")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(target, "keep.txt")
	if err := os.WriteFile(keep, []byte("keep\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runMainIn(t, dir, "init", "prompt", "busy")
	if code != 1 {
		t.Errorf("daedalus init prompt over a non-empty dir exit code = %d, want 1", code)
	}
	if stdout != "" {
		t.Errorf("daedalus init prompt stdout = %q, want empty", stdout)
	}
	if !strings.Contains(stderr, "already exists and is not empty") {
		t.Errorf("daedalus init prompt stderr = %q, want the non-empty-target refusal", stderr)
	}
	if _, err := os.Stat(filepath.Join(target, "README")); !os.IsNotExist(err) {
		t.Errorf("scaffold wrote into the refused dir (README stat err %v)", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "config-example.yaml")); !os.IsNotExist(err) {
		t.Errorf("refused scaffold still wrote config-example.yaml (stat err %v)", err)
	}
	data, err := os.ReadFile(keep)
	if err != nil || string(data) != "keep\n" {
		t.Errorf("the refused scaffold disturbed the target's content: %q, %v", data, err)
	}
}
