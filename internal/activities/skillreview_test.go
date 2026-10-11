package activities

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeSkill plants one skill directory with its SKILL.md under a skills
// root (a worktree's .claude/skills or a fake home's), the layout
// resolveSkillInstructions searches.
func writeSkill(t *testing.T, root, name, body string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", filepath.Join(dir, "SKILL.md"), err)
	}
}

// TestResolveSkillInstructions pins one review_skill_list entry's resolution:
// a "/name" entry reads <name>/SKILL.md with the worktree's project skills
// winning over the worker's home ones, frontmatter stripped; any other entry
// is hand-written prompt text passed through verbatim; a skill that resolves
// nowhere — or an entry that is empty either way — fails loudly instead of
// degrading to a default review.
func TestResolveSkillInstructions(t *testing.T) {
	t.Run("hand-written text passes through verbatim", func(t *testing.T) {
		got, err := resolveSkillInstructions(t.TempDir(), "check every error path by hand")
		if err != nil {
			t.Fatalf("resolveSkillInstructions: %v", err)
		}
		if got != "check every error path by hand" {
			t.Errorf("resolveSkillInstructions = %q, want the text verbatim", got)
		}
	})

	t.Run("the worktree's skill wins over the home one", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		wt := t.TempDir()
		writeSkill(t, filepath.Join(home, ".claude", "skills"), "style", "home body")
		writeSkill(t, filepath.Join(wt, ".claude", "skills"), "style", "worktree body")
		got, err := resolveSkillInstructions(wt, "/style")
		if err != nil {
			t.Fatalf("resolveSkillInstructions: %v", err)
		}
		if got != "worktree body" {
			t.Errorf("resolveSkillInstructions = %q, want the project's copy to win", got)
		}
	})

	t.Run("the home skill resolves when the worktree has none", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		wt := t.TempDir()
		writeSkill(t, filepath.Join(home, ".claude", "skills"), "global-skill", "home body")
		got, err := resolveSkillInstructions(wt, "/global-skill")
		if err != nil {
			t.Fatalf("resolveSkillInstructions: %v", err)
		}
		if got != "home body" {
			t.Errorf("resolveSkillInstructions = %q, want the home skill", got)
		}
	})

	t.Run("frontmatter is stripped from the body", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		wt := t.TempDir()
		writeSkill(t, filepath.Join(wt, ".claude", "skills"), "style",
			"---\nname: style\ndescription: the registries' metadata\n---\nKeep comments short.")
		got, err := resolveSkillInstructions(wt, "/style")
		if err != nil {
			t.Fatalf("resolveSkillInstructions: %v", err)
		}
		if got != "Keep comments short." {
			t.Errorf("resolveSkillInstructions = %q, want the body with the frontmatter header gone", got)
		}
	})

	t.Run("a named skill found nowhere fails loudly", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		wt := t.TempDir()
		_, err := resolveSkillInstructions(wt, "/absent")
		if err == nil {
			t.Fatal("resolveSkillInstructions = nil error, want the not-found rejection")
		}
		for _, want := range []string{
			`review skill "/absent"`,
			"no SKILL.md found",
			filepath.Join(wt, ".claude", "skills", "absent", "SKILL.md"),
			filepath.Join(home, ".claude", "skills", "absent", "SKILL.md"),
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q should contain %q", err, want)
			}
		}
	})

	t.Run("a bare slash names no skill", func(t *testing.T) {
		_, err := resolveSkillInstructions(t.TempDir(), "/   ")
		if err == nil || !strings.Contains(err.Error(), "empty skill name") {
			t.Errorf("resolveSkillInstructions(\"/   \") = %v, want the empty-name rejection", err)
		}
	})

	t.Run("a blank hand-written entry fails", func(t *testing.T) {
		_, err := resolveSkillInstructions(t.TempDir(), "   ")
		if err == nil || !strings.Contains(err.Error(), "empty") {
			t.Errorf("resolveSkillInstructions(\"   \") = %v, want the blank-entry rejection", err)
		}
	})

	t.Run("an unresolvable home fails a named entry", func(t *testing.T) {
		t.Setenv("HOME", "")
		_, err := resolveSkillInstructions(t.TempDir(), "/style")
		if err == nil || !strings.Contains(err.Error(), "resolve home directory") {
			t.Errorf("resolveSkillInstructions = %v, want the home-resolution error", err)
		}
	})

	t.Run("a skill file that is nothing but frontmatter fails resolution", func(t *testing.T) {
		wt := t.TempDir()
		writeSkill(t, filepath.Join(wt, ".claude", "skills"), "empty",
			"---\nname: empty\ndescription: metadata only\n---\n")
		_, err := resolveSkillInstructions(wt, "/empty")
		if err == nil || !strings.Contains(err.Error(), "no instruction content") {
			t.Errorf("resolveSkillInstructions = %v, want the instruction-less rejection", err)
		}
	})
}

// TestResolveSkillInstructionsSymlinks pins the worktree skills lookup's
// symlink refusal: the jailed agent can plant a symlink under .claude/skills
// mid-run, and the worker-side read would follow it out of the sandbox — so
// a symlinked component anywhere below the worktree root refuses the round
// loudly even when the skill behind it resolves. The worktree root itself
// (base is excluded — the operator's own affair) and the home leg (the
// worker's own home, no round can write it) stay unchecked.
func TestResolveSkillInstructionsSymlinks(t *testing.T) {
	t.Run("a symlinked .claude refuses even a resolvable skill", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		wt := t.TempDir()
		outside := t.TempDir()
		writeSkill(t, filepath.Join(outside, ".claude", "skills"), "style", "host body")
		if err := os.Symlink(filepath.Join(outside, ".claude"), filepath.Join(wt, ".claude")); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		_, err := resolveSkillInstructions(wt, "/style")
		if err == nil {
			t.Fatal("resolveSkillInstructions = nil error, want the symlink refusal")
		}
		for _, want := range []string{
			`review skill "/style"`,
			"is a symlink",
			filepath.Join(wt, ".claude", "skills", "style", "SKILL.md"),
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q should contain %q", err, want)
			}
		}
	})

	t.Run("a symlinked skill directory refuses", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		wt := t.TempDir()
		if err := os.MkdirAll(filepath.Join(wt, ".claude", "skills"), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		outside := t.TempDir()
		writeSkill(t, outside, "style", "host body")
		if err := os.Symlink(filepath.Join(outside, "style"), filepath.Join(wt, ".claude", "skills", "style")); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		_, err := resolveSkillInstructions(wt, "/style")
		if err == nil || !strings.Contains(err.Error(), "is a symlink") {
			t.Errorf("resolveSkillInstructions = %v, want the symlink refusal", err)
		}
	})

	t.Run("a symlinked worktree root still resolves", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		realWt := t.TempDir()
		writeSkill(t, filepath.Join(realWt, ".claude", "skills"), "style", "worktree body")
		link := filepath.Join(t.TempDir(), "wt-link")
		if err := os.Symlink(realWt, link); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		got, err := resolveSkillInstructions(link, "/style")
		if err != nil {
			t.Fatalf("resolveSkillInstructions: %v", err)
		}
		if got != "worktree body" {
			t.Errorf("resolveSkillInstructions = %q, want the skill (a symlinked worktree location is the operator's own affair)", got)
		}
	})

	t.Run("a symlinked home skill resolves", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		outside := t.TempDir()
		writeSkill(t, outside, "style", "home body")
		if err := os.MkdirAll(filepath.Join(home, ".claude", "skills"), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.Symlink(filepath.Join(outside, "style"), filepath.Join(home, ".claude", "skills", "style")); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		got, err := resolveSkillInstructions(t.TempDir(), "/style")
		if err != nil {
			t.Fatalf("resolveSkillInstructions: %v", err)
		}
		if got != "home body" {
			t.Errorf("resolveSkillInstructions = %q, want the home skill (the global leg is not the agent-writable one)", got)
		}
	})
}

// TestStripFrontmatter pins the skill-file body extraction: a leading ---
// fence (with or without CRLF endings, its closing line allowed at EOF
// without a trailing newline) is removed down to a trimmed body; a file
// with no leading fence passes through whole; an unterminated fence or a
// body-less file fails — the metadata must never pose as instructions.
func TestStripFrontmatter(t *testing.T) {
	for name, tt := range map[string]struct {
		in      string
		want    string
		wantErr string
	}{
		"no fence passes through whole": {in: "plain body\nline two", want: "plain body\nline two"},
		"only a leading fence opens frontmatter": {
			in:   "body text\n---\nname: s\n---\ntail",
			want: "body text\n---\nname: s\n---\ntail",
		},
		"frontmatter stripped": {
			in:   "---\nname: style\ndescription: d\n---\nbody line\nmore body",
			want: "body line\nmore body",
		},
		"CRLF fences recognized, body endings kept": {
			in:   "---\r\nname: s\r\n---\r\nbody\r\nlines",
			want: "body\r\nlines",
		},
		"closing fence at EOF without a trailing newline": {
			in:      "---\nname: s\n---",
			wantErr: "no instruction content",
		},
		"frontmatter-only file": {
			in:      "---\nname: s\n---\n",
			wantErr: "no instruction content",
		},
		"unterminated fence": {
			in:      "---\nname: s\nnever closed",
			wantErr: "unterminated",
		},
		"blank body without a fence": {
			in:      "   \n",
			wantErr: "no instruction content",
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := stripFrontmatter(tt.in)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("stripFrontmatter(%q) = %q, %v; want an error containing %q", tt.in, got, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("stripFrontmatter(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("stripFrontmatter(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestRunJailedReviewerActivitySkillEntry pins the reviewer activity's
// skill-entry wiring end to end: the entry resolves at round time and its
// instructions render into the jailed reviewer's prompt under the fixed
// preamble; hand-written text rides verbatim; a skill that resolves nowhere
// fails the round before the reviewer ever runs — never a silent default
// review.
func TestRunJailedReviewerActivitySkillEntry(t *testing.T) {
	scrubBugFilingEnv(t)

	t.Run("a named skill's body renders under the fixed preamble", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		wt := t.TempDir()
		writeSkill(t, filepath.Join(wt, ".claude", "skills"), "style",
			"---\nname: style\ndescription: metadata\n---\nKeep every comment under three lines.")
		log := newStubLog(t)
		stubBin(t, "git", `if [ "$3" = "diff" ]; then printf 'M foo.go\n'; fi
exit 0`)
		stubBin(t, "ai-jail", `printf 'Looks good.\nAPPROVED\n'; exit 0`)

		res, err := RunJailedReviewerActivity(context.Background(), ReviewInput{
			WorktreePath: wt,
			Focus:        "the implementation",
			SkillEntry:   "/style",
		})
		if err != nil {
			t.Fatalf("RunJailedReviewerActivity: %v", err)
		}
		if !res.Approved {
			t.Error("Approved = false, want true")
		}
		calls := readCalls(t, log)
		if len(calls) != 4 {
			t.Fatalf("%d subprocess calls, want 4 (git add, git diff HEAD, git reset, ai-jail)", len(calls))
		}
		prompt := calls[3].Stdin
		for _, want := range []string{
			"This round is also a skill review",
			"Keep every comment under three lines.",
			"your final line must still be exactly one of the verdict words this prompt names",
		} {
			if !strings.Contains(prompt, want) {
				t.Errorf("reviewer prompt should mention %q", want)
			}
		}
		for _, notWant := range []string{"name: style", "description: metadata"} {
			if strings.Contains(prompt, notWant) {
				t.Errorf("reviewer prompt leaked the frontmatter line %q", notWant)
			}
		}
	})

	t.Run("hand-written instructions pass through verbatim", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		log := newStubLog(t)
		stubBin(t, "git", "exit 0")
		stubBin(t, "ai-jail", `printf 'Fine.\nAPPROVED\n'; exit 0`)

		if _, err := RunJailedReviewerActivity(context.Background(), ReviewInput{
			WorktreePath: t.TempDir(),
			Focus:        "the implementation",
			SkillEntry:   "check the error paths by hand",
		}); err != nil {
			t.Fatalf("RunJailedReviewerActivity: %v", err)
		}
		calls := readCalls(t, log)
		if len(calls) != 4 {
			t.Fatalf("%d subprocess calls, want 4 (git add, git diff HEAD, git reset, ai-jail)", len(calls))
		}
		if !strings.Contains(calls[3].Stdin, "check the error paths by hand") {
			t.Errorf("reviewer prompt should carry the hand-written text verbatim: %q", calls[3].Stdin)
		}
	})

	t.Run("a skill that resolves nowhere fails the round loudly", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		log := newStubLog(t)
		stubBin(t, "git", "exit 0")
		stubBin(t, "ai-jail", `printf 'degraded default review APPROVED'; exit 0`)

		_, err := RunJailedReviewerActivity(context.Background(), ReviewInput{
			WorktreePath: t.TempDir(),
			Focus:        "the implementation",
			SkillEntry:   "/absent",
		})
		if err == nil || !strings.Contains(err.Error(), "no SKILL.md found") {
			t.Fatalf("RunJailedReviewerActivity = %v, want the not-found rejection", err)
		}
		// The jail never ran: the round failed at resolution, not after a
		// review the configured discipline never gave.
		calls := readCalls(t, log)
		if len(calls) != 3 {
			t.Errorf("%d subprocess calls, want 3 (git add, git diff HEAD, git reset — no reviewer run)", len(calls))
		}
	})
}
