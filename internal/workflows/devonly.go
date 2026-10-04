package workflows

import (
	"fmt"

	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/template"
)

// DevOnlyWorkflow drives feature-dev's phase 1 alone: implementation ↔
// code review until the reviewer approves, then the approved work is
// committed and the branch preserved. There is no test phase — no test
// session, no suite executions, no test reviewer — and deliberately no
// preflight gate either: that gate exists to guarantee a green baseline for
// the suite gating other flows apply, which this flow never does (a red
// baseline would only block test-free work a maintainer explicitly chose to
// run without tests). Selection is CLI-only (`daedalus run -w dev-only`);
// no config key picks a flow.
func DevOnlyWorkflow(ctx workflow.Context, input PipelineInput) (string, error) {
	run, cleanup, err := startRun(ctx, input)
	if err != nil {
		return "", err
	}
	defer cleanup()
	// Submit-time jail carve-out, same as feature-dev phase 1 — including
	// the finalize force-stage opt-in.
	touchesJail := template.TaskTouchesJail(input.Prompt)
	run.worktreeInput.TaskTouchesJail = touchesJail
	if err := run.createWorktree(); err != nil {
		return "", err
	}

	// A continued run opens on the aborted attempt's preserved work — the
	// same Continue framing feature-dev phase 1 uses.
	var initialPrompt string
	if input.BaseBranch != "" {
		initialPrompt, err = template.Continue(input.Prompt, input.PriorFeedback)
	} else {
		initialPrompt, err = template.Implement(input.Prompt, bugDir())
	}
	if err != nil {
		return "", fmt.Errorf("build implement prompt: %w", err)
	}
	if _, err := run.runAgent(initialPrompt, "implement", activities.RoleDev, &run.devSession); err != nil {
		return "", fmt.Errorf("initial agent run: %w", err)
	}

	for {
		verdict, err := run.review("the implementation", "", false, false, "", activities.RoleDevReview, &run.devReviewSession, touchesJail)
		if err != nil {
			return "", fmt.Errorf("code review: %w", err)
		}
		if verdict.NeedsMaintainer {
			run.logger.Info("Code review halted the run for maintainer input")
			return "", run.park(fmt.Sprintf("code review halted the run — the task cannot be completed as stated: %s",
				verdict.Comments))
		}
		if verdict.Approved {
			run.logger.Info("Code review approved")
			return run.finalize()
		}
		run.logger.Info("Code review requested changes")
		fixPrompt, err := template.ImplementFix(verdict.Comments, template.Jail{Touches: touchesJail})
		if err != nil {
			return "", fmt.Errorf("build implement-fix prompt: %w", err)
		}
		if g := run.drainGuidance(); g != "" {
			run.logger.Info("Operator guidance received, folding into fix prompt")
			fixPrompt = g + "\n\n" + fixPrompt
		}
		if _, err := run.runAgent(fixPrompt, "implement-fix", activities.RoleDev, &run.devSession); err != nil {
			return "", fmt.Errorf("agent run after code review: %w", err)
		}
	}
}
