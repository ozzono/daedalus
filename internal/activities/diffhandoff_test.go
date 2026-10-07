package activities

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
)

// TestStagedDiffInlineBelowBudget pins the small-diff contract: at or under
// the byte budget the diff embeds inline byte-identical to `git diff`, no
// handoff is returned, and no review scratch dir is left in the worktree.
func TestStagedDiffInlineBelowBudget(t *testing.T) {
	t.Setenv(config.ContextTokensEnv, "100000")
	dir := gitRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "foo.go"), []byte("package foo\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	diff, handoff, err := stagedDiff(context.Background(), dir)
	if err != nil {
		t.Fatalf("stagedDiff: %v", err)
	}
	if handoff != nil {
		t.Fatalf("a small diff produced a handoff: %+v", handoff)
	}
	want, err := runGit(context.Background(), "-C", dir, "diff")
	if err != nil {
		t.Fatalf("git diff: %v", err)
	}
	if diff != want {
		t.Errorf("inline diff = %q, want the plain `git diff` output %q", diff, want)
	}
	if !strings.Contains(diff, "package foo") {
		t.Errorf("inline diff = %q, want the intent-to-add file's content", diff)
	}
	if _, err := os.Stat(filepath.Join(dir, reviewDiffDir)); !os.IsNotExist(err) {
		t.Errorf("an inline round left %s behind (stat err %v)", reviewDiffDir, err)
	}
}

// TestStagedDiffBudgetBoundary pins the budget branch on both sides of the
// exact byte: a diff of exactly the budget embeds inline (the `<=`), one
// byte over hands off to the file. The budget is driven through
// ContextTokensEnv — the worker's context window is the knob, not a
// constant.
func TestStagedDiffBudgetBoundary(t *testing.T) {
	t.Setenv(config.ContextTokensEnv, "1000")
	if budget := reviewDiffBudget(); budget != 1500 {
		t.Fatalf("reviewDiffBudget with %s=1000 = %d, want 1500 (half the window at 3 bytes/token)", config.ContextTokensEnv, budget)
	}
	dir := gitRepo(t)
	path := filepath.Join(dir, "big.txt")
	write := func(n int) {
		t.Helper()
		if err := os.WriteFile(path, []byte(strings.Repeat("x", n)+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// The diff of a one-line file grows one byte per content byte, so the
	// exact boundary is constructible: measure the diff of "x\n", then pad
	// the line until the diff is exactly the budget.
	write(1)
	if _, err := runGit(context.Background(), "-C", dir, "add", "-N", "-A"); err != nil {
		t.Fatalf("stage intent-to-add: %v", err)
	}
	base, err := runGit(context.Background(), "-C", dir, "diff")
	if err != nil {
		t.Fatalf("measure base diff: %v", err)
	}
	budget := reviewDiffBudget()
	write(1 + budget - len(base))

	diff, handoff, err := stagedDiff(context.Background(), dir)
	if err != nil {
		t.Fatalf("stagedDiff at the budget: %v", err)
	}
	if handoff != nil || len(diff) != budget {
		t.Fatalf("a diff of exactly the budget = (%d bytes, handoff %v), want the inline embed of %d bytes", len(diff), handoff, budget)
	}

	write(2 + budget - len(base))
	diff, handoff, err = stagedDiff(context.Background(), dir)
	if err != nil {
		t.Fatalf("stagedDiff over the budget: %v", err)
	}
	if diff != "" || handoff == nil {
		t.Fatalf("a diff one byte over the budget = (%d bytes, handoff %v), want the empty diff and a handoff", len(diff), handoff)
	}
}

// TestStagedDiffHandsOffOversizedDiff pins the oversized-diff contract end
// to end on a real repository: the complete uncropped diff is written to
// the handoff file (identical to what `git diff` prints for the same
// change), the prompt's handoff carries the digest and a per-file table of
// contents whose ranges tile the file 1..Lines with every section opening
// at its `diff --git` header — the address a slice read takes — and the
// scratch dir is invisible to git (status and diff), while the work itself
// stays visible.
func TestStagedDiffHandsOffOversizedDiff(t *testing.T) {
	t.Setenv(config.ContextTokensEnv, "200")
	ctx := context.Background()
	dir := gitRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "a.go"), []byte("one\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"config", "user.email", "t@example.com"},
		{"config", "user.name", "t"},
		{"add", "-A"},
		{"commit", "--no-gpg-sign", "-m", "base"},
	} {
		if _, err := runGit(ctx, append([]string{"-C", dir}, args...)...); err != nil {
			t.Fatalf("git %v: %v", args, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "a.go"), []byte("one\ntwo\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.go"), []byte(strings.Repeat("line of the new file\n", 30)), 0o644); err != nil {
		t.Fatal(err)
	}

	diff, handoff, err := stagedDiff(ctx, dir)
	if err != nil {
		t.Fatalf("stagedDiff: %v", err)
	}
	if diff != "" || handoff == nil {
		t.Fatalf("an oversized diff = (%d bytes inline, handoff %v), want the empty diff and a handoff", len(diff), handoff)
	}
	if handoff.Path != diffHandoffPath {
		t.Errorf("handoff Path = %q, want %q", handoff.Path, diffHandoffPath)
	}

	file, err := os.ReadFile(filepath.Join(dir, diffHandoffPath))
	if err != nil {
		t.Fatalf("read handoff file: %v", err)
	}
	wantDiff, err := runGit(ctx, "-C", dir, "diff")
	if err != nil {
		t.Fatalf("git diff: %v", err)
	}
	if string(file) != wantDiff {
		t.Errorf("handoff file = %d bytes, want the complete uncropped `git diff` (%d bytes)", len(file), len(wantDiff))
	}
	if handoff.Lines != strings.Count(string(file), "\n") {
		t.Errorf("handoff Lines = %d, want the diff file's %d lines", handoff.Lines, strings.Count(string(file), "\n"))
	}
	wantStat, err := runGit(ctx, "-C", dir, "diff", "--compact-summary")
	if err != nil {
		t.Fatalf("git diff --compact-summary: %v", err)
	}
	if handoff.Stat != wantStat || !strings.Contains(wantStat, "a.go") || !strings.Contains(wantStat, "b.go") {
		t.Errorf("handoff Stat = %q, want the compact summary %q", handoff.Stat, wantStat)
	}

	// The table of contents is load-bearing: its ranges must tile the file
	// exactly, each opening at a `diff --git` header — the slices a
	// `sed -n 'a,bp'` read addresses.
	lines := strings.Split(strings.TrimSuffix(string(file), "\n"), "\n")
	paths := map[string]bool{}
	prevEnd := 0
	for _, entry := range handoff.Files {
		rest, ok := strings.CutPrefix(entry, "lines ")
		if !ok {
			t.Fatalf("TOC entry %q is not a \"lines a-b: path\" line", entry)
		}
		dash := strings.Index(rest, "-")
		colon := strings.Index(rest, ": ")
		if dash <= 0 || colon < dash {
			t.Fatalf("TOC entry %q is not a \"lines a-b: path\" line", entry)
		}
		a, err1 := strconv.Atoi(rest[:dash])
		b, err2 := strconv.Atoi(rest[dash+1 : colon])
		if err1 != nil || err2 != nil {
			t.Fatalf("TOC entry %q carries unparseable range bounds", entry)
		}
		if a != prevEnd+1 {
			t.Errorf("TOC entry %q opens at line %d, want %d (the ranges must tile the file)", entry, a, prevEnd+1)
		}
		if !strings.HasPrefix(lines[a-1], "diff --git ") {
			t.Errorf("TOC entry %q opens at %q, want its section's `diff --git` header", entry, lines[a-1])
		}
		prevEnd = b
		paths[rest[colon+2:]] = true
	}
	if prevEnd != handoff.Lines {
		t.Errorf("TOC ranges end at line %d, want the diff file's last line %d", prevEnd, handoff.Lines)
	}
	// The modification and the intent-to-add addition each carry a section
	// named after their file.
	if !paths["a.go"] || !paths["b.go"] {
		t.Errorf("TOC paths = %v, want a.go and b.go", paths)
	}

	// The scratch dir is in git's blind spot; the work is not.
	status, err := runGit(ctx, "-C", dir, "status", "--porcelain")
	if err != nil {
		t.Fatalf("git status: %v", err)
	}
	if strings.Contains(status, ".daedalus-review") {
		t.Errorf("git status sees the handoff scratch dir: %q", status)
	}
	if !strings.Contains(status, "a.go") || !strings.Contains(status, "b.go") {
		t.Errorf("git status lost the work under the scratch-dir exclusion: %q", status)
	}
	exclude, err := os.ReadFile(filepath.Join(dir, ".git", "info", "exclude"))
	if err != nil {
		t.Fatalf("read info/exclude: %v", err)
	}
	if !strings.Contains(string(exclude), reviewDiffDir+"/") {
		t.Errorf("info/exclude = %q, want the %s/ pattern", exclude, reviewDiffDir)
	}
}

// TestStagedDiffRemovesStaleHandoff pins the back-transition: a round whose
// diff embeds inline again must not leave a superseded diff file for a
// later reviewer to read — the stale handoff (and its empty scratch dir)
// is removed. A dir holding anything else is left alone.
func TestStagedDiffRemovesStaleHandoff(t *testing.T) {
	t.Setenv(config.ContextTokensEnv, "100000")
	dir := gitRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "foo.go"), []byte("package foo\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	staleDir := filepath.Join(dir, reviewDiffDir)
	staleFile := filepath.Join(staleDir, "diff.patch")
	if err := os.MkdirAll(staleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(staleFile, []byte("a superseded diff\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, handoff, err := stagedDiff(context.Background(), dir); err != nil || handoff != nil {
		t.Fatalf("stagedDiff over a stale handoff = (handoff %v, err %v), want the inline embed", handoff, err)
	}
	if _, err := os.Stat(staleFile); !os.IsNotExist(err) {
		t.Errorf("stale handoff survived an inline round (stat err %v)", err)
	}
	if _, err := os.Stat(staleDir); !os.IsNotExist(err) {
		t.Errorf("the emptied scratch dir survived an inline round (stat err %v)", err)
	}

	// A scratch dir with other content keeps the dir; only the handoff
	// file goes.
	if err := os.MkdirAll(staleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(staleFile, []byte("stale\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(staleDir, "other.txt")
	if err := os.WriteFile(other, []byte("not ours\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, handoff, err := stagedDiff(context.Background(), dir); err != nil || handoff != nil {
		t.Fatalf("stagedDiff over a busy scratch dir = (handoff %v, err %v), want the inline embed", handoff, err)
	}
	if _, err := os.Stat(staleFile); !os.IsNotExist(err) {
		t.Errorf("stale handoff survived an inline round (stat err %v)", err)
	}
	if _, err := os.Stat(other); err != nil {
		t.Errorf("the scratch dir's other content was removed: %v", err)
	}
}

// TestDiffSections pins the table-of-contents builder over an adversarial
// diff: a hunk body line starting `+++ b/` must not retitle its section
// (only a section's header block may), a deletion (no `+++ b/` line) falls
// back to its header's b-side, a pure rename is named by its header, and a
// space-carrying path's trailing tab is trimmed. The ranges must tile the
// diff 1..diffLineCount.
func TestDiffSections(t *testing.T) {
	const diff = "" +
		"diff --git a/first.go b/first.go\n" +
		"index 1111111..2222222 100644\n" +
		"--- a/first.go\n" +
		"+++ b/first.go\n" +
		"@@ -1,3 +1,4 @@\n" +
		" alpha\n" +
		"+++ b/sneaky.go\n" +
		" beta\n" +
		"+gamma\n" +
		"diff --git a/gone.txt b/gone.txt\n" +
		"deleted file mode 100644\n" +
		"index 3333333..4444444\n" +
		"--- a/gone.txt\n" +
		"+++ /dev/null\n" +
		"@@ -1 +0,0 @@\n" +
		"-goodbye\n" +
		"diff --git a/old.txt b/renamed.txt\n" +
		"similarity index 100%\n" +
		"rename from old.txt\n" +
		"rename to renamed.txt\n"
	got := diffSections(diff)
	want := []string{
		"lines 1-9: first.go",
		"lines 10-16: gone.txt",
		"lines 17-20: renamed.txt",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("diffSections = %q, want %q", got, want)
	}
	if got := diffSections(""); len(got) != 0 {
		t.Errorf("diffSections(\"\") = %q, want no entries", got)
	}
}

// TestDiffHeaderPath pins the header's b-side extraction — the fallback for
// sections without a `+++ b/` line: the tail after the last " b/" wins, so
// a rename names the new side. A header git had to quote carries no plain
// " b/" and yields the whole tail — documented cosmetic, the line range is
// the load-bearing half of a TOC entry.
func TestDiffHeaderPath(t *testing.T) {
	for _, c := range []struct{ header, want string }{
		{"diff --git a/x.go b/x.go", "x.go"},
		{"diff --git a/old.txt b/new.txt", "new.txt"},
		{"diff --git a/dir/file b/dir/file", "dir/file"},
		{"diff --git \"a/sp ace.txt\" \"b/sp ace.txt\"", `"a/sp ace.txt" "b/sp ace.txt"`},
	} {
		if got := diffHeaderPath(c.header); got != c.want {
			t.Errorf("diffHeaderPath(%q) = %q, want %q", c.header, got, c.want)
		}
	}
}

// TestDiffLineCount pins the count a slice read addresses against: one per
// newline, plus a final unterminated line.
func TestDiffLineCount(t *testing.T) {
	for _, c := range []struct {
		diff string
		want int
	}{
		{"a\nb\n", 2},
		{"a\nb", 2},
		{"\n", 1},
		{"", 0},
	} {
		if got := diffLineCount(c.diff); got != c.want {
			t.Errorf("diffLineCount(%q) = %d, want %d", c.diff, got, c.want)
		}
	}
}
