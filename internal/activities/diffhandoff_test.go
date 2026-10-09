package activities

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
)

// TestStagedDiffInlineBelowBudget pins the small-diff contract on an unborn
// HEAD — a fresh `git init` with no commit, the shape a zero-commit repo's
// worktree lands in: `git diff HEAD` has no revision to name, so the plain
// index diff is the whole change. At or under the byte budget the diff
// embeds inline carrying the intent-to-add file's content, no handoff is
// returned, the index is restored afterwards (nothing staged, the new file
// back to plain untracked), and no review scratch dir is left behind.
func TestStagedDiffInlineBelowBudget(t *testing.T) {
	t.Setenv(config.ContextTokensEnv, "100000")
	ctx := context.Background()
	dir := gitRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "foo.go"), []byte("package foo\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	diff, handoff, err := stagedDiff(ctx, dir)
	if err != nil {
		t.Fatalf("stagedDiff: %v", err)
	}
	if handoff != nil {
		t.Fatalf("a small diff produced a handoff: %+v", handoff)
	}
	if !strings.Contains(diff, "new file mode") || !strings.Contains(diff, "package foo") {
		t.Errorf("inline diff = %q, want the intent-to-add file's new-file diff", diff)
	}
	// The index is restored after collection: the intent-to-add entries are
	// dropped, so the new file is plain untracked again — the shape the next
	// agent round's own `git diff`/`git status` must see (while the entries
	// stood, plain `git diff` printed the new file and status showed ` A`).
	remaining, err := runGit(ctx, "-C", dir, "diff")
	if err != nil {
		t.Fatalf("git diff after stagedDiff: %v", err)
	}
	if remaining != "" {
		t.Errorf("post-collection `git diff` = %q, want empty — the intent-to-add entries must be reset", remaining)
	}
	status, err := runGit(ctx, "-C", dir, "status", "--porcelain")
	if err != nil {
		t.Fatalf("git status: %v", err)
	}
	if strings.Contains(status, "foo.go") && !strings.Contains(status, "?? foo.go") {
		t.Errorf("git status = %q, want foo.go plain untracked (??), not intent-to-add", status)
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
// the handoff file — the worktree+index-vs-HEAD diff, so a tracked-file
// deletion and a rename ride along beside the modification and the
// intent-to-add addition (an index-only diff stages deletions and renames
// away, and the reviewer once never heard of a whole-module removal) — the
// prompt's handoff carries the digest gathered before the index reset (a
// post-reset digest loses every new file, which only the intent-to-add
// entries make visible) and a per-file table of contents whose ranges tile
// the file 1..Lines with every section opening at its `diff --git` header —
// the address a slice read takes — and the scratch dir is invisible to git
// (status and diff), while the work itself stays visible and the index is
// handed back unstaged.
func TestStagedDiffHandsOffOversizedDiff(t *testing.T) {
	t.Setenv(config.ContextTokensEnv, "200")
	ctx := context.Background()
	dir := gitRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "a.go"), []byte("one\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "gone.txt"), []byte("goodbye\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "old.txt"), []byte("stays the same\n"), 0o644); err != nil {
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
	if err := os.Remove(filepath.Join(dir, "gone.txt")); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("git", "-C", dir, "mv", "old.txt", "moved.txt").CombinedOutput(); err != nil {
		t.Fatalf("git mv old.txt moved.txt: %v: %s", err, out)
	}
	// The diff the reviewer reads is collected over the intent-to-add index;
	// stagedDiff resets that index before returning, so the reference cannot
	// be re-derived afterwards — capture the same window first.
	if _, err := runGit(ctx, "-C", dir, "add", "-N", "-A"); err != nil {
		t.Fatalf("stage intent-to-add: %v", err)
	}
	wantDiff, err := runGit(ctx, "-C", dir, "diff", "HEAD")
	if err != nil {
		t.Fatalf("git diff HEAD: %v", err)
	}
	if !strings.Contains(wantDiff, "deleted file mode") || !strings.Contains(wantDiff, "-goodbye") {
		t.Fatalf("git diff HEAD = %q, want the tracked-file deletion covered", wantDiff)
	}
	if !strings.Contains(wantDiff, "rename from old.txt") || !strings.Contains(wantDiff, "rename to moved.txt") {
		t.Fatalf("git diff HEAD = %q, want the rename covered", wantDiff)
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
	if string(file) != wantDiff {
		t.Errorf("handoff file = %d bytes, want the complete uncropped `git diff HEAD` (%d bytes)", len(file), len(wantDiff))
	}
	if handoff.Lines != strings.Count(string(file), "\n") {
		t.Errorf("handoff Lines = %d, want the diff file's %d lines", handoff.Lines, strings.Count(string(file), "\n"))
	}
	// The digest is gathered before the index reset: it must still see the
	// new file (b.go (new)) and the rename — after the reset the new file is
	// plain untracked and no re-runnable digest names either — beside the
	// modification and the deletion.
	for _, want := range []string{"a.go", "b.go (new)", "gone.txt (gone)", "old.txt => moved.txt", "4 files changed"} {
		if !strings.Contains(handoff.Stat, want) {
			t.Errorf("handoff Stat = %q, want it to carry %q", handoff.Stat, want)
		}
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
	// The modification, the intent-to-add addition, the deletion, and the
	// rename's new side each carry a section named after their file.
	for _, want := range []string{"a.go", "b.go", "gone.txt", "moved.txt"} {
		if !paths[want] {
			t.Errorf("TOC paths = %v, want %q among them", paths, want)
		}
	}

	// The scratch dir is in git's blind spot; the work is not — the deletion
	// and rename included, which the reset leaves unstaged — and the index
	// is handed back with nothing staged in it.
	status, err := runGit(ctx, "-C", dir, "status", "--porcelain")
	if err != nil {
		t.Fatalf("git status: %v", err)
	}
	if strings.Contains(status, ".daedalus-review") {
		t.Errorf("git status sees the handoff scratch dir: %q", status)
	}
	for _, want := range []string{"a.go", "b.go", "gone.txt", "moved.txt", "old.txt"} {
		if !strings.Contains(status, want) {
			t.Errorf("git status lost %q under the scratch-dir exclusion: %q", want, status)
		}
	}
	staged, err := runGit(ctx, "-C", dir, "diff", "--cached")
	if err != nil {
		t.Fatalf("git diff --cached: %v", err)
	}
	if staged != "" {
		t.Errorf("the index kept staged entries after stagedDiff: %q — the reset must restore it", staged)
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
