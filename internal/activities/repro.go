package activities

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// ReproGateInput tells the repro-first gate where the run's diff lives and
// the suite command to run. The base tree ("the pre-fix code") is the run
// worktree's own HEAD — the commit its branch was created from, since
// nothing commits on the branch until finalize.
type ReproGateInput struct {
	// RepoPath is the repository the throwaway base checkout is created
	// from; WorktreePath is the run's worktree the changed test files are
	// copied out of (its HEAD is the base the checkout is taken from).
	RepoPath     string
	WorktreePath string
	// Command is the run's resolved suite command, executed verbatim in
	// the base checkout with the diff's test files copied in.
	Command string
}

// ReproResult reports the gate's outcome. Reproduced means at least one of
// the diff's test files failed on the base tree — the test captures the
// bug. When it is false, Logs carries the reason (formatted as a red-round
// message the agent's fix loop digests); when true, Logs carries the
// failing pre-fix output for the history.
type ReproResult struct {
	Reproduced bool
	Logs       string
}

// reproGateTimeout bounds the deferred teardown of the throwaway base
// checkout — it must finish even when the activity's ctx is already
// cancelled (worker shutdown), or a git worktree registration would linger.
const reproGateTimeout = 10 * time.Second

// TestPathPatterns is the multi-language test-file path policy, as glob
// patterns for matchPathPolicy: Go test files, pytest files, JS/TS test
// files, and testdata trees. It is the single source for every flow that
// names test paths — the test-only flow's allowed set and refactor's
// frozen set both carry it, and the repro gate recognizes exactly it.
var TestPathPatterns = []string{
	"*_test.go", "testdata/",
	"test_*.py", "*_test.py",
	"*.test.js", "*.test.jsx", "*.test.ts", "*.test.tsx",
}

// isTestPath reports whether a repo-relative path is a test file per
// TestPathPatterns. ponytail: a pattern list, not a parser — a repo whose
// tests live under other conventions gets a gate failure naming the
// missing test file, which the fix loop can answer.
func isTestPath(p string) bool {
	return matchPathPolicy(p, TestPathPatterns)
}

// ReproFirstGateActivity runs the bug-fix flow's repro-first gate: the
// diff's new or changed test files are copied into a throwaway checkout of
// the run's base tree and the suite command is run there. The contract is
// "new tests must fail pre-fix" — a suite that comes back green means the
// tests do not capture the bug, and the gate reports that as a failed round
// (Reproduced=false), not a system error. ponytail: the gate judges the
// whole suite's exit code in the base checkout, so its success message
// overstates — two false "reproduced" shapes exist: a base tree that is
// already red for unrelated reasons, and copied test files whose new
// helpers or testdata were not copied along (they fail to compile there
// instead of running). Either way the reviewer still polices whether the
// repro actually captures the reported bug.
func ReproFirstGateActivity(ctx context.Context, input ReproGateInput) (ReproResult, error) {
	testFiles, err := changedTestFiles(ctx, input.WorktreePath)
	if err != nil {
		return ReproResult{}, err
	}
	if len(testFiles) == 0 {
		return ReproResult{Logs: "REPRO-FIRST GATE FAILED: the diff contains no test files — this flow cannot leave the loop until it carries a test that fails on the pre-fix code and passes on the fixed code. Add one."}, nil
	}

	tmp, err := os.MkdirTemp("", "daedalus-repro-")
	if err != nil {
		return ReproResult{}, fmt.Errorf("repro-first gate: create base checkout directory: %w", err)
	}
	// The throwaway checkout is taken from the run worktree's HEAD — the
	// branch point (nothing commits on the branch until finalize), i.e.
	// exactly the pre-fix code. Adding the worktree from inside the run
	// worktree still registers it in the shared repository.
	if out, err := runGit(ctx, "-C", input.WorktreePath, "worktree", "add", "--detach", tmp, "HEAD"); err != nil {
		os.RemoveAll(tmp)
		return ReproResult{}, fmt.Errorf("repro-first gate: checkout the pre-fix code: %w: %s", err, out)
	}
	defer func() {
		// Teardown on a fresh context: a cancelled activity ctx must not
		// leave the throwaway worktree registered in the repository.
		tctx, cancel := context.WithTimeout(context.Background(), reproGateTimeout)
		defer cancel()
		_, _ = runGit(tctx, "-C", input.RepoPath, "worktree", "remove", tmp, "--force")
		_, _ = runGit(tctx, "-C", input.RepoPath, "worktree", "prune")
		_ = os.RemoveAll(tmp)
	}()

	for _, f := range testFiles {
		data, err := os.ReadFile(filepath.Join(input.WorktreePath, f))
		if err != nil {
			return ReproResult{}, fmt.Errorf("repro-first gate: read %s: %w", f, err)
		}
		dst := filepath.Join(tmp, f)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return ReproResult{}, fmt.Errorf("repro-first gate: stage %s: %w", f, err)
		}
		if err := os.WriteFile(dst, data, 0o644); err != nil {
			return ReproResult{}, fmt.Errorf("repro-first gate: stage %s: %w", f, err)
		}
	}

	passed, out, err := runSuiteCommand(ctx, tmp, input.Command)
	if err != nil {
		return ReproResult{}, err
	}
	logs := out
	if passed {
		return ReproResult{Logs: fmt.Sprintf(
			"REPRO-FIRST GATE FAILED: the diff's test files (%s) pass on the pre-fix code — they do not capture the bug. Strengthen the repro test so it fails without the fix, then re-apply the fix.",
			strings.Join(testFiles, ", "))}, nil
	}
	return ReproResult{Reproduced: true, Logs: logs}, nil
}

// changedTestFiles lists the run's diff paths that are test files: tracked
// changes against the worktree's HEAD (the branch point) plus untracked new
// files. Deletions are excluded (--diff-filter=d) — a deleted test file has
// no content to copy into the base checkout.
func changedTestFiles(ctx context.Context, worktreePath string) ([]string, error) {
	out, err := runGit(ctx, "-C", worktreePath, "diff", "--name-only", "--diff-filter=d", "HEAD")
	if err != nil {
		return nil, fmt.Errorf("repro-first gate: diff the run's changes: %w", err)
	}
	others, err := runGit(ctx, "-C", worktreePath,
		"ls-files", "--others", "--exclude-standard",
		// Same sandbox-artifact excludes as the write-scope gate: daedalus's
		// own litter (.ai-jail and friends) is never a repro test.
		"--exclude=.ai-jail", "--exclude=.aider*", "--exclude=.daedalus-aider/", "--exclude=coverage.out")
	if err != nil {
		return nil, fmt.Errorf("repro-first gate: list untracked files: %w", err)
	}
	var files []string
	for _, listing := range []string{out, others} {
		for line := range strings.SplitSeq(listing, "\n") {
			p := strings.TrimSpace(line)
			if p != "" && isTestPath(p) {
				files = append(files, p)
			}
		}
	}
	return files, nil
}

// runSuiteCommand executes a resolved suite command in dir, mirroring
// RunTestSuiteActivity's `cd <dir> && <command>` shape: a non-zero exit is
// test output (returned as passed=false), anything else is a system error.
func runSuiteCommand(ctx context.Context, dir, command string) (bool, string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return false, "", fmt.Errorf("repro-first gate: resolve home directory: %w", err)
	}
	cmd := exec.CommandContext(ctx, "sh", "-c",
		"cd "+shellQuote(dir)+" && "+command)
	cmd.Dir = home
	setProcessGroup(cmd)
	defer killGroup(cmd)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		return false, "", fmt.Errorf("repro-first gate: run %s: %w", command, err)
	}
	err = waitCommand(ctx, cmd)
	if err != nil && !isWaitDelay(err) {
		if _, ok := errors.AsType[*exec.ExitError](err); !ok {
			return false, "", fmt.Errorf("repro-first gate: run %s: %w: %s",
				command, err, out.String())
		}
		return false, out.String(), nil
	}
	return true, out.String(), nil
}
