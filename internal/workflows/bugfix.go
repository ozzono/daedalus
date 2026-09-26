package workflows

import (
	"fmt"

	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/template"
)

// BugFixWorkflow drives a test-first bug fix: the agent writes the test
// that reproduces the bug and the minimal fix that turns it green. The
// repro-first gate runs the diff's new or changed test files against the
// run's base tree every round — a test that passes pre-fix does not capture
// the bug, and the gate feeds that back as a red round until the repro is
// real. The reviewer verifies the repro captures the reported bug and the
// fix is minimal.
func BugFixWorkflow(ctx workflow.Context, input PipelineInput) (string, error) {
	run, cleanup := startRun(ctx, input)
	defer cleanup()
	if err := run.createWorktree(); err != nil {
		return "", err
	}
	if err := run.preFlightGate(); err != nil {
		return "", err
	}

	var initialPrompt string
	var err error
	if input.BaseBranch != "" {
		initialPrompt, err = template.Continue(input.Prompt, input.PriorFeedback)
	} else {
		initialPrompt, err = template.BugFix(input.Prompt)
	}
	if err != nil {
		return "", fmt.Errorf("build bug-fix prompt: %w", err)
	}
	if _, err := run.runAgent(initialPrompt, "bug-fix", activities.RoleDev, &run.devSession); err != nil {
		return "", fmt.Errorf("initial agent run: %w", err)
	}

	for {
		command, err := run.resolveTestCommand()
		if err != nil {
			return "", err
		}
		result, err := run.runSuite(command)
		if err != nil {
			return "", err
		}
		verdict := activities.ReviewResult{}
		reviewRound := true
		if result.Passed {
			// The gate only means something on a green suite — a red one
			// goes to the reviewer with its own logs, as in feature-dev.
			gate, err := run.reproGate(command)
			if err != nil {
				return "", err
			}
			if !gate.Reproduced {
				// Not a reviewable diff yet: the gate's reason travels to
				// the agent as a red round, and the loop repeats.
				reviewRound = false
				result = activities.TestResult{Passed: false, Logs: gate.Logs}
			} else {
				result.Logs = "REPRO-FIRST GATE: passed — the diff's test files fail on the pre-fix code as required.\n\n" + result.Logs
			}
		}
		if reviewRound {
			verdict, err = run.review("the bug fix and its reproducing test", result.Logs, false, true, "", activities.RoleDevReview, &run.devReviewSession)
			if err != nil {
				return "", fmt.Errorf("code review: %w", err)
			}
			if verdict.NeedsMaintainer {
				run.logger.Info("Code review halted the run for maintainer input")
				return "", run.park(fmt.Sprintf("code review halted the run — the task cannot be completed as stated: %s",
					verdict.Comments))
			}
			if result.Passed && verdict.Approved {
				run.logger.Info("Tests pass and code review approved")
				return run.finalize()
			}
		}
		fixPrompt, err := template.BugFixFix(failureLogs(result), verdict.Comments)
		if err != nil {
			return "", fmt.Errorf("build bug-fix prompt: %w", err)
		}
		if g := run.drainGuidance(); g != "" {
			run.logger.Info("Operator guidance received, folding into fix prompt")
			fixPrompt = g + "\n\n" + fixPrompt
		}
		if _, err := run.runAgent(fixPrompt, "bug-fix", activities.RoleDev, &run.devSession); err != nil {
			return "", fmt.Errorf("agent run after bug-fix review: %w", err)
		}
	}
}
