package workflows

import (
	"fmt"

	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/template"
)

// RefactorWorkflow drives a behavior-frozen refactoring run: the agent
// restructures production code while the existing test suite — the
// correctness argument — stays untouched and green. The freeze is
// structural: the flow's frozen-path set (test files, testdata) is verified
// against the diff before finalize, and a diff touching it parks the run.
// The unedited suite gating every round carries the semantics; the reviewer
// judges structure.
func RefactorWorkflow(ctx workflow.Context, input PipelineInput) (string, error) {
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
		initialPrompt, err = template.Refactor(input.Prompt)
	}
	if err != nil {
		return "", fmt.Errorf("build refactor prompt: %w", err)
	}
	if _, err := run.runAgent(initialPrompt, "refactor", activities.RoleDev, &run.devSession); err != nil {
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
		verdict, err := run.review("the refactoring", result.Logs, false, false, "", activities.RoleDevReview, &run.devReviewSession)
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
			if err := run.checkWriteScope(); err != nil {
				return "", err
			}
			return run.finalize()
		}
		fixPrompt, err := template.RefactorFix(failureLogs(result), verdict.Comments)
		if err != nil {
			return "", fmt.Errorf("build refactor-fix prompt: %w", err)
		}
		if g := run.drainGuidance(); g != "" {
			run.logger.Info("Operator guidance received, folding into fix prompt")
			fixPrompt = g + "\n\n" + fixPrompt
		}
		if _, err := run.runAgent(fixPrompt, "refactor-fix", activities.RoleDev, &run.devSession); err != nil {
			return "", fmt.Errorf("agent run after refactor review: %w", err)
		}
	}
}

// failureLogs passes a red round's logs to a fix prompt; a green round
// carries none (only the reviewer's comments).
func failureLogs(result activities.TestResult) string {
	if result.Passed {
		return ""
	}
	return result.Logs
}
