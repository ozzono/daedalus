package activities

import (
	"context"
	"fmt"
	"path"
	"strings"
)

// ScopeCheckInput names a worktree and the flow's write-scope policy — the
// typed path sets the workflow carries in its input (workflows.Flow).
type ScopeCheckInput struct {
	WorktreePath string
	// Allowed, when non-empty: the diff may touch nothing outside these
	// patterns.
	Allowed []string
	// Frozen: the diff may touch nothing in these patterns.
	Frozen []string
}

// ScopeResult lists the diff's write-scope violations, one entry per
// offending path ("<path>: <why>"). Empty means the diff satisfies the
// policy.
type ScopeResult struct {
	Violations []string
}

// VerifyWriteScopeActivity checks the run's diff against its flow's
// write-scope policy — the structural half of scope enforcement (the
// prompt is the primary half; this one does not depend on obedience). It
// lists, and never strips: a violating diff parks the run for the
// maintainer, who decides whether the files belong.
//
// The diff is taken against the worktree's HEAD: the run's branch points at
// its creation commit until finalize renames it — the jail bars every agent
// from git writes, so HEAD is the base the run started from. ponytail: a
// bypass that committed mid-run would hide those commits from this gate.
func VerifyWriteScopeActivity(ctx context.Context, input ScopeCheckInput) (ScopeResult, error) {
	// Tracked changes against the base (staged and unstaged — the finalize
	// commit comes later) plus untracked new files, which `git diff` never
	// lists. The untracked listing excludes daedalus's own sandbox
	// artifacts (the same trio the repo template's .gitignore carries):
	// the jail drops .ai-jail into every worktree it runs in, and a target
	// repo that does not ignore it must not fail its flow's scope gate
	// over daedalus's own litter.
	out, err := runGit(ctx, "-C", input.WorktreePath, "diff", "--name-only", "HEAD")
	if err != nil {
		return ScopeResult{}, fmt.Errorf("write-scope check: diff the run's changes: %w", err)
	}
	others, err := runGit(ctx, "-C", input.WorktreePath,
		"ls-files", "--others", "--exclude-standard",
		"--exclude=.ai-jail", "--exclude=.aider*", "--exclude=.daedalus-aider/", "--exclude=coverage.out")
	if err != nil {
		return ScopeResult{}, fmt.Errorf("write-scope check: list untracked files: %w", err)
	}
	var res ScopeResult
	for _, listing := range []string{out, others} {
		for line := range strings.SplitSeq(listing, "\n") {
			p := strings.TrimSpace(line)
			if p == "" {
				continue
			}
			switch {
			case matchPathPolicy(p, input.Frozen):
				res.Violations = append(res.Violations, p+": frozen path — this flow may not change it")
			case len(input.Allowed) > 0 && !matchPathPolicy(p, input.Allowed):
				res.Violations = append(res.Violations, p+": outside this flow's allowed paths")
			}
		}
	}
	return res, nil
}

// matchPathPolicy reports whether a repo-relative path matches any pattern:
// a pattern ending in "/" matches everything under that directory ("docs/");
// a pattern containing a slash matches the full path as a glob ("docs/*.md");
// a bare pattern matches the file name at any depth ("*.md", "Makefile").
func matchPathPolicy(p string, patterns []string) bool {
	for _, pat := range patterns {
		switch {
		case strings.HasSuffix(pat, "/"):
			if strings.HasPrefix(p, pat) {
				return true
			}
		case strings.Contains(pat, "/"):
			if ok, _ := path.Match(pat, p); ok {
				return true
			}
		default:
			if ok, _ := path.Match(pat, path.Base(p)); ok {
				return true
			}
		}
	}
	return false
}
