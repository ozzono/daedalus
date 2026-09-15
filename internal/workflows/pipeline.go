// Package workflows defines the Temporal workflows that orchestrate a
// Daedalus feature-development pipeline.
package workflows

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/template"
)

// PipelineInput is the sole input to FeatureDevWorkflow. It deliberately
// carries no credentials: the worker reads provider settings from its own
// environment (exported from config.yaml), keeping secrets out of workflow
// history.
type PipelineInput struct {
	RepoPath  string
	TaskQueue string
	IssueID   string
	Prompt    string
	// BranchPrefix names the preserved branch that carries the run's
	// approved work (config branch_prefix, `run --prefix`). Empty means
	// the activities' default.
	BranchPrefix string
	// BaseBranch, when set, starts the worktree from an aborted attempt's
	// preserved branch instead of HEAD — a continued run (`daedalus
	// continue`). PriorFeedback is that attempt's last review feedback,
	// folded into the opening prompt.
	BaseBranch    string
	PriorFeedback string
	// TestTimeout bounds one execution of the native test suite (config
	// tests_timeout). Zero — a run whose input predates the field, replayed
	// by a newer worker — falls back to config.DefaultTestsTimeout.
	TestTimeout time.Duration
}

// FeatureDevWorkflow drives a full issue-development cycle in two
// review-gated phases: (1) implementation ↔ code review until the reviewer
// approves, then (2) tests ↔ test review until the reviewer approves AND the
// native test suite passes. On success the approved work is committed and
// the run's branch renamed to its preserved prefix; the workflow
// returns that branch name. Both loops are intentionally unbounded — they
// run until approval, with no attempt cap; each round is durable, auditable,
// and individually timed-out via activity options.
func FeatureDevWorkflow(ctx workflow.Context, input PipelineInput) (string, error) {
	logger := workflow.GetLogger(ctx)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	branchName := fmt.Sprintf("feat/issue-%s-%d", input.IssueID, workflow.Now(ctx).Unix())
	worktreeInput := activities.WorktreeInput{
		RepoPath:     input.RepoPath,
		TaskQueue:    input.TaskQueue,
		IssueID:      input.IssueID,
		BranchName:   branchName,
		BranchPrefix: input.BranchPrefix,
		BaseBranch:   input.BaseBranch,
	}

	// Guarantee the workspace is cleaned up on every exit path. The defer is
	// registered before the create activity so that a cancellation during
	// create still cleans up — a Get on a cancelled context fails the
	// workflow before any later defer could be installed. The cleanup runs
	// on a disconnected context so that cancelling the workflow does not
	// cancel the cleanup itself, and tolerates state that never came to
	// exist. It deletes only the in-flight feat/ branch; approved work was
	// renamed to the preserved prefix by FinalizeWorktreeActivity and
	// survives.
	defer func() {
		// NewDisconnectedContext returns (Context, CancelFunc) — the second
		// value is not an error. Detach the cancel from this deferred func's
		// lifetime only after the cleanup activity has completed.
		dctx, cancel := workflow.NewDisconnectedContext(ctx)
		defer cancel()
		if err := workflow.ExecuteActivity(dctx, activities.CleanupWorktreeActivity, worktreeInput).Get(dctx, nil); err != nil {
			logger.Error("Failed to clean up worktree", "Error", err)
		}
	}()

	var worktree activities.WorktreeOutput
	if err := workflow.ExecuteActivity(ctx, activities.CreateWorktreeActivity, worktreeInput).Get(ctx, &worktree); err != nil {
		return "", fmt.Errorf("create worktree: %w", err)
	}

	// runAgent executes one autonomous implementation round; review asks the
	// reviewer for a verdict on the worktree's current state. The agent's
	// text and chain of thought come back tail-bounded in the activity
	// result — serialized into Temporal history — while the worker log keeps
	// the full stream.
	//
	// guideCh carries operator guidance sent while the pipeline runs (see
	// `daedalus guide`): each fix prompt is prefixed with whatever arrived
	// since the last round, so a human can steer a stuck review loop without
	// restarting the run. Draining a signal channel is not a workflow
	// command — adding it mid-run is replay-safe.
	guideCh := workflow.GetSignalChannel(ctx, "guide")
	drainGuidance := func() string {
		var parts []string
		for {
			var g string
			if !guideCh.ReceiveAsync(&g) {
				break
			}
			if g != "" {
				parts = append(parts, g)
			}
		}
		if len(parts) == 0 {
			return ""
		}
		return "OPERATOR GUIDANCE (sent while this pipeline was running — treat as direct instructions from the operator, taking precedence over earlier plan assumptions):\n- " +
			strings.Join(parts, "\n- ")
	}
	runAgent := func(prompt string, stage string) error {
		var result activities.AgentRunResult
		err := workflow.ExecuteActivity(ctx, activities.RunJailedClaudeActivity, activities.AgentRunInput{
			WorktreePath: worktree.WorktreePath,
			Prompt:       prompt,
		}).Get(ctx, &result)
		if err == nil {
			logger.Info("Agent run completed", "Stage", stage,
				"TextChars", len(result.Text), "ThinkingChars", len(result.Thinking))
		}
		return err
	}
	review := func(focus, testLogs string, testsInScope bool) (activities.ReviewResult, error) {
		var result activities.ReviewResult
		err := workflow.ExecuteActivity(ctx, activities.RunJailedReviewerActivity, activities.ReviewInput{
			WorktreePath: worktree.WorktreePath,
			Focus:        focus,
			TestLogs:     testLogs,
			TestsInScope: testsInScope,
		}).Get(ctx, &result)
		return result, err
	}

	// Phase 1: implementation ↔ code review, until the reviewer approves.
	// A continued run opens on the aborted attempt's preserved work.
	var initialPrompt string
	var err error
	if input.BaseBranch != "" {
		initialPrompt, err = template.Continue(input.Prompt, input.PriorFeedback)
	} else {
		initialPrompt, err = template.Implement(input.Prompt)
	}
	if err != nil {
		return "", fmt.Errorf("build implement prompt: %w", err)
	}
	if err := runAgent(initialPrompt, "implement"); err != nil {
		return "", fmt.Errorf("initial agent run: %w", err)
	}
	for {
		verdict, err := review("the implementation", "", false)
		if err != nil {
			return "", fmt.Errorf("code review: %w", err)
		}
		if verdict.Approved {
			logger.Info("Code review approved")
			break
		}
		logger.Info("Code review requested changes")
		fixPrompt, err := template.ImplementFix(verdict.Comments)
		if err != nil {
			return "", fmt.Errorf("build implement-fix prompt: %w", err)
		}
		if g := drainGuidance(); g != "" {
			logger.Info("Operator guidance received, folding into fix prompt")
			fixPrompt = g + "\n\n" + fixPrompt
		}
		if err := runAgent(fixPrompt, "implement-fix"); err != nil {
			return "", fmt.Errorf("agent run after code review: %w", err)
		}
	}

	// Phase 2: tests ↔ test review, until the reviewer approves AND the
	// native suite passes.
	testsPrompt, err := template.Tests()
	if err != nil {
		return "", fmt.Errorf("build tests prompt: %w", err)
	}
	if err := runAgent(testsPrompt, "tests"); err != nil {
		return "", fmt.Errorf("test-phase agent run: %w", err)
	}
	// The native suite gets a wider ceiling than the shared 15 minutes:
	// command discovery (itself an agent round when nothing statically
	// detects) plus a cold build can legitimately overrun it. The Get still
	// uses ctx so cancellation propagates normally.
	testTimeout := input.TestTimeout
	if testTimeout <= 0 {
		testTimeout = config.DefaultTestsTimeout
	}
	testsCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: testTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	for {
		var result activities.TestResult
		if err := workflow.ExecuteActivity(testsCtx, activities.RunNativeTestsActivity, worktree.WorktreePath).Get(ctx, &result); err != nil {
			return "", fmt.Errorf("run tests: %w", err)
		}
		verdict, err := review("the test suite", result.Logs, true)
		if err != nil {
			return "", fmt.Errorf("test review: %w", err)
		}
		if result.Passed && verdict.Approved {
			logger.Info("Tests pass and test review approved")
			var preservedBranch string
			if err := workflow.ExecuteActivity(ctx, activities.FinalizeWorktreeActivity, worktreeInput).Get(ctx, &preservedBranch); err != nil {
				return "", fmt.Errorf("finalize worktree: %w", err)
			}
			logger.Info("Approved work committed", "Branch", preservedBranch)
			return preservedBranch, nil
		}
		testFix := testFixPrompt(result, verdict)
		if g := drainGuidance(); g != "" {
			logger.Info("Operator guidance received, folding into fix prompt")
			testFix = g + "\n\n" + testFix
		}
		if err := runAgent(testFix, "tests-fix"); err != nil {
			return "", fmt.Errorf("test-fix agent run: %w", err)
		}
	}
}

// testFixPrompt builds the phase-2 fix prompt from whatever failed: test
// output, review comments, or both. Template rendering is deterministic, so
// it is safe to call from workflow code.
func testFixPrompt(result activities.TestResult, verdict activities.ReviewResult) string {
	logs := ""
	if !result.Passed {
		logs = result.Logs
	}
	comments := ""
	if !verdict.Approved {
		comments = verdict.Comments
	}
	prompt, err := template.TestsFix(logs, comments)
	if err != nil {
		// The templates are compile-time-validated (template.Must); an
		// execution failure is a packaging bug. Fail the workflow loudly
		// rather than looping without feedback.
		return fmt.Sprintf("Internal error building the fix prompt: %v", err)
	}
	return prompt
}
