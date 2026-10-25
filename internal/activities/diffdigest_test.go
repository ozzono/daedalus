package activities

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// emptyDiffDigest is the digest of an empty diff — sha256 of the empty
// string — the value a clean worktree's probe returns.
const emptyDiffDigest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// TestDiffDigest pins the digest collection the workflow's swift review and
// strike reforms stand on: it hashes the same diff the reviewer reads —
// intent-to-add staging, `git diff HEAD`, index restored — so untracked
// files and staged edits count, a clean tree digests empty, and the
// collection leaves the index exactly as it found it.
func TestDiffDigest(t *testing.T) {
	ctx := context.Background()

	t.Run("a clean tree digests the empty diff", func(t *testing.T) {
		dir := gitRepoWithCommit(t, map[string]string{"README.md": "x\n"})
		got, err := diffDigest(ctx, dir)
		if err != nil {
			t.Fatalf("diffDigest: %v", err)
		}
		if got != emptyDiffDigest {
			t.Errorf("diffDigest(clean tree) = %q, want the empty-diff digest", got)
		}
	})

	t.Run("unchanged tree digests identically, any worktree edit changes it", func(t *testing.T) {
		dir := gitRepoWithCommit(t, map[string]string{"README.md": "x\n"})
		first, err := diffDigest(ctx, dir)
		if err != nil {
			t.Fatalf("diffDigest: %v", err)
		}
		second, err := diffDigest(ctx, dir)
		if err != nil {
			t.Fatalf("diffDigest again: %v", err)
		}
		if first != second {
			t.Errorf("two digest calls on one state = %q then %q, want them equal", first, second)
		}
		if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("edited\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		edited, err := diffDigest(ctx, dir)
		if err != nil {
			t.Fatalf("diffDigest after edit: %v", err)
		}
		if edited == first {
			t.Error("digest unchanged after a worktree edit — the digest would carry a stale verdict across changed diffs")
		}
	})

	t.Run("untracked files count through the intent-to-add staging", func(t *testing.T) {
		dir := gitRepoWithCommit(t, map[string]string{"README.md": "x\n"})
		// A plain `git diff HEAD` cannot see an untracked file; the
		// collection's add -N staging must make its content part of the
		// diff it hashes. If the staging regressed, both rewrites would
		// digest the empty diff and these two would come out equal.
		digests := map[string]string{}
		for _, content := range []string{"one\n", "two\n"} {
			if err := os.WriteFile(filepath.Join(dir, "new.go"), []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := diffDigest(ctx, dir)
			if err != nil {
				t.Fatalf("diffDigest with untracked %q: %v", content, err)
			}
			digests[content] = got
			if got == emptyDiffDigest {
				t.Errorf("diffDigest ignored the untracked file's content %q — intent-to-add staging is missing", content)
			}
		}
		if digests["one\n"] == digests["two\n"] {
			t.Error("two different untracked contents digested identically — the staging is not feeding the diff")
		}
	})

	t.Run("staged edits count", func(t *testing.T) {
		dir := gitRepoWithCommit(t, map[string]string{"README.md": "x\n"})
		clean, err := diffDigest(ctx, dir)
		if err != nil {
			t.Fatalf("diffDigest: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("staged\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("git", "-C", dir, "add", "README.md")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git add: %v: %s", err, out)
		}
		staged, err := diffDigest(ctx, dir)
		if err != nil {
			t.Fatalf("diffDigest with staged edit: %v", err)
		}
		if staged == clean {
			t.Error("digest unchanged after a staged edit — `git diff HEAD` must cover the index")
		}
	})

	t.Run("the index is restored after collection", func(t *testing.T) {
		dir := gitRepoWithCommit(t, map[string]string{"README.md": "x\n"})
		if err := os.WriteFile(filepath.Join(dir, "new.go"), []byte("package new\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := diffDigest(ctx, dir); err != nil {
			t.Fatalf("diffDigest: %v", err)
		}
		// The intent-to-add entries must be dropped again: the next
		// round's own `git diff` and `git status` see a plain untracked
		// file, not a staged one.
		remaining, err := runGit(ctx, "-C", dir, "diff")
		if err != nil {
			t.Fatalf("git diff after collection: %v", err)
		}
		if remaining != "" {
			t.Errorf("post-collection `git diff` = %q, want empty — the intent-to-add entries must be reset", remaining)
		}
		status, err := runGit(ctx, "-C", dir, "status", "--porcelain")
		if err != nil {
			t.Fatalf("git status: %v", err)
		}
		for _, line := range strings.Split(status, "\n") {
			if strings.TrimSpace(line) == "?? new.go" {
				return
			}
		}
		t.Errorf("git status = %q, want new.go plain untracked (??), not intent-to-add", status)
	})

	t.Run("an unborn HEAD digests the plain diff", func(t *testing.T) {
		// A fresh `git init` with no commit — `git diff HEAD` has no
		// revision to name, so the collection falls back to the plain
		// diff. The untracked file must still reach the digest, and the
		// value must stay stable across calls.
		dir := gitRepo(t)
		if err := os.WriteFile(filepath.Join(dir, "foo.go"), []byte("package foo\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		first, err := diffDigest(ctx, dir)
		if err != nil {
			t.Fatalf("diffDigest: %v", err)
		}
		second, err := diffDigest(ctx, dir)
		if err != nil {
			t.Fatalf("diffDigest again: %v", err)
		}
		if first == "" || first != second {
			t.Errorf("diffDigest on an unborn HEAD = %q then %q, want one stable non-empty digest", first, second)
		}
		if first == emptyDiffDigest {
			t.Error("diffDigest on an unborn HEAD digested empty — the fallback lost the only change there is")
		}
	})

	t.Run("DiffDigestActivity is diffDigest", func(t *testing.T) {
		dir := gitRepoWithCommit(t, map[string]string{"README.md": "x\n"})
		if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("edited\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		viaActivity, err := DiffDigestActivity(ctx, dir)
		if err != nil {
			t.Fatalf("DiffDigestActivity: %v", err)
		}
		direct, err := diffDigest(ctx, dir)
		if err != nil {
			t.Fatalf("diffDigest: %v", err)
		}
		if viaActivity != direct {
			t.Errorf("DiffDigestActivity = %q, diffDigest = %q — the workflow's probe must hash what the reviewer hashes", viaActivity, direct)
		}
	})
}
