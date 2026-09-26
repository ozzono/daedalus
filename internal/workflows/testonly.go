package workflows

import (
	"fmt"

	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/template"
)

// TestOnlyWorkflow drives a test-improvement run: the agent modifies test
// files only, the native suite gates every round, and a test reviewer
// approves the suite. There is no implementation loop: a reviewer REBUILD —
// a finding the test-only agent cannot apply because the fix is an
// implementation change — parks the run for the maintainer instead, so a
// known-buggy diff can never finalize as approved work. Suite executions
// record Go statement coverage, and the reviewer sees the delta across
// rounds.
func TestOnlyWorkflow(ctx workflow.Context, input PipelineInput) (string, error) {
	run, cleanup := startRun(ctx, input)
	defer cleanup()
	run.cover = true
	if err := run.createWorktree(); err != nil {
		return "", err
	}
	if err := run.preFlightGate(); err != nil {
		return "", err
	}

	// The run starts directly in the test loop — the same
	// tests ↔ test-review cycle as feature-dev phase 2, lifted out as a
	// whole workflow. A continued run opens on the aborted attempt's
	// preserved work.
	var initialPrompt string
	var err error
	if input.BaseBranch != "" {
		initialPrompt, err = template.Continue(input.Prompt, input.PriorFeedback)
	} else {
		initialPrompt, err = template.Tests()
	}
	if err != nil {
		return "", fmt.Errorf("build tests prompt: %w", err)
	}
	testerReply, err := run.runAgent(initialPrompt, "tests", activities.RoleTest, &run.testSession)
	if err != nil {
		return "", fmt.Errorf("initial agent run: %w", err)
	}

	// lastCoverage is the previous round's recorded coverage, so the
	// reviewer sees the delta, not just the level.
	lastCoverage := ""
	for {
		command, err := run.resolveTestCommand()
		if err != nil {
			return "", err
		}
		result, err := run.runSuite(command)
		if err != nil {
			return "", err
		}
		logs := coverageLogs(result.Coverage, lastCoverage) + result.Logs
		if result.Coverage != "" {
			lastCoverage = result.Coverage
		}
		verdict, err := run.review("the test suite", logs, true, false, testerReply.Text, activities.RoleTestReview, &run.testReviewSession)
		if err != nil {
			return "", fmt.Errorf("test review: %w", err)
		}
		if verdict.NeedsMaintainer {
			run.logger.Info("Test review halted the run for maintainer input")
			return "", run.park(fmt.Sprintf("test review halted the run — the task cannot be completed as stated: %s",
				verdict.Comments))
		}
		if verdict.Rebuild {
			// No implementation loop exists in this flow to rebuild into:
			// the finding parks for the maintainer with the reviewer's
			// reasoning, keeping "merged work is approved work" true.
			run.logger.Info("Test review requested a rebuild; parking for the maintainer")
			return "", run.park(fmt.Sprintf("test review requested a rebuild — the fix is an implementation change, outside this flow's test-only scope: %s",
				verdict.Comments))
		}
		if result.Passed && verdict.Approved {
			run.logger.Info("Tests pass and test review approved")
			if err := run.checkWriteScope(); err != nil {
				return "", err
			}
			return run.finalize()
		}
		testFix := testFixPrompt(result, verdict)
		if g := run.drainGuidance(); g != "" {
			run.logger.Info("Operator guidance received, folding into fix prompt")
			testFix = g + "\n\n" + testFix
		}
		fixResult, err := run.runAgent(testFix, "tests-fix", activities.RoleTest, &run.testSession)
		if err != nil {
			return "", fmt.Errorf("test-fix agent run: %w", err)
		}
		testerReply = fixResult
	}
}

// coverageLogs prefixes the suite logs with this round's recorded coverage
// and the delta against the previous round; empty when no coverage was
// computable.
func coverageLogs(coverage, lastCoverage string) string {
	if coverage == "" {
		return ""
	}
	note := "GO STATEMENT COVERAGE THIS ROUND: " + coverage + "%"
	if lastCoverage != "" {
		note += " (previous round: " + lastCoverage + "%)"
	}
	return note + "\n\n"
}
