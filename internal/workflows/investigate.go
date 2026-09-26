package workflows

import (
	"fmt"

	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/template"
)

// InvestigateWorkflow drives a docs-only investigation: the agent analyzes
// the repository and writes the deliverable as documentation (design notes,
// findings, ADRs, an architecture review), then a reviewer reads the docs
// and the diff until it approves. No production code changes, no test
// phase, no REBUILD path — nothing to rebuild into. The flow's write scope
// (docs paths only) travels in the input's path policy and is verified
// structurally before finalize: a diff that leaves it parks the run.
func InvestigateWorkflow(ctx workflow.Context, input PipelineInput) (string, error) {
	run, cleanup := startRun(ctx, input)
	defer cleanup()
	if err := run.createWorktree(); err != nil {
		return "", err
	}

	// A continued run opens on the aborted attempt's preserved work — the
	// same Continue framing feature-dev phase 1 uses.
	var initialPrompt string
	var err error
	if input.BaseBranch != "" {
		initialPrompt, err = template.Continue(input.Prompt, input.PriorFeedback)
	} else {
		initialPrompt, err = template.Investigate(input.Prompt)
	}
	if err != nil {
		return "", fmt.Errorf("build investigate prompt: %w", err)
	}
	if _, err := run.runAgent(initialPrompt, "investigate", activities.RoleDev, &run.devSession); err != nil {
		return "", fmt.Errorf("initial agent run: %w", err)
	}

	for {
		verdict, err := run.review("the documentation", "", false, false, "", activities.RoleDevReview, &run.devReviewSession)
		if err != nil {
			return "", fmt.Errorf("docs review: %w", err)
		}
		if verdict.NeedsMaintainer {
			run.logger.Info("Docs review halted the run for maintainer input")
			return "", run.park(fmt.Sprintf("docs review halted the run — the task cannot be completed as stated: %s",
				verdict.Comments))
		}
		if verdict.Approved {
			run.logger.Info("Docs review approved")
			break
		}
		run.logger.Info("Docs review requested changes")
		fixPrompt, err := template.InvestigateFix(verdict.Comments)
		if err != nil {
			return "", fmt.Errorf("build investigate-fix prompt: %w", err)
		}
		if g := run.drainGuidance(); g != "" {
			run.logger.Info("Operator guidance received, folding into fix prompt")
			fixPrompt = g + "\n\n" + fixPrompt
		}
		if _, err := run.runAgent(fixPrompt, "investigate-fix", activities.RoleDev, &run.devSession); err != nil {
			return "", fmt.Errorf("agent run after docs review: %w", err)
		}
	}

	if err := run.checkWriteScope(); err != nil {
		return "", err
	}
	return run.finalize()
}
