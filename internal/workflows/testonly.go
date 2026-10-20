package workflows

import (
	"fmt"

	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/template"
)

// TestOnlyWorkflow drives a test-improvement run: the agent modifies test
// files only, the native suite gates every round, and a test reviewer
// approves the suite. A reviewer REBUILD — a finding the test-only agent
// cannot apply because the fix is an implementation change — spends a
// capped rebuild round: a dev-role agent run on the finding, the flow's
// one dev-code path, its result gated by the code reviewer before the
// test loop resumes with the widened write scope the rebuilt state needs.
// Only a spent cap parks the run for the maintainer. Suite executions
// record Go statement coverage, and the reviewer sees the delta across
// rounds.
func TestOnlyWorkflow(ctx workflow.Context, input PipelineInput) (string, error) {
	run, cleanup, err := startRun(ctx, input)
	if err != nil {
		return "", err
	}
	defer cleanup()
	run.cover = true
	if err := run.createWorktree(); err != nil {
		return "", err
	}
	if _, err := run.preFlightGate(); err != nil {
		return "", err
	}

	// The run starts directly in the test loop — the same
	// tests ↔ test-review cycle as feature-dev phase 2, lifted out as a
	// whole workflow. A continued run opens on the aborted attempt's
	// preserved work.
	var initialPrompt string
	if input.BaseBranch != "" {
		initialPrompt, err = template.Continue(input.Prompt, input.PriorFeedback)
	} else {
		initialPrompt, err = template.TestsForTask(input.Prompt, bugDir())
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
	rebuilds := 0
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
		verdict, err := run.review("the test suite", logs, true, false, testerReply.Text, activities.RoleTestReview, &run.testReviewSession, false, false)
		if err != nil {
			return "", fmt.Errorf("test review: %w", err)
		}
		if verdict.NeedsMaintainer {
			run.logger.Info("Test review halted the run for maintainer input")
			return "", run.park(fmt.Sprintf("test review halted the run — the task cannot be completed as stated: %s",
				verdict.Comments))
		}
		if verdict.Rebuild {
			// The rebuild round is the flow's one dev-code path, budgeted
			// like the feature-dev green stage: a dev-role agent run on the
			// finding, then the test loop resumes. Widening the run's write
			// scope here is what lets the rebuilt state — implementation
			// fix included — pass the finalize gate; the mutation rides the
			// verdict, so replay is deterministic.
			rebuilds++
			if rebuilds > maxTestPhaseRebuilds {
				run.logger.Info("Test phase exceeded the rebuild cap; parking the run")
				return "", run.park(fmt.Sprintf("test phase failed to converge — the test review issued rebuild %d and its approval never coincided with a green suite; last finding: %s",
					rebuilds, verdict.Comments))
			}
			run.logger.Info("Test review requested a rebuild; running a dev round on the finding",
				"Rebuilds", rebuilds, "Of", maxTestPhaseRebuilds)
			run.input.AllowedPaths = nil
			prompt, err := template.Rebuild(verdict.Comments)
			if err != nil {
				return "", fmt.Errorf("build rebuild prompt: %w", err)
			}
			rebuildReply, err := run.runAgent(prompt, "rebuild", activities.RoleDev, &run.devSession)
			if err != nil {
				return "", fmt.Errorf("rebuild agent run: %w", err)
			}
			// The rebuild round's reply is what the test reviewer next sees
			// as the latest reply: quoting the pre-rebuild tester here would
			// describe a change the reviewer is not looking at.
			testerReply = rebuildReply
			// The rebuild's implementation change is unreviewed code until
			// the code reviewer approves it — the same gate feature-dev's
			// REBUILD path passes through, keeping "merged work is approved
			// work" true across the flow's one dev-code path.
			if err := run.codeReviewLoop(false); err != nil {
				return "", fmt.Errorf("post-rebuild code review: %w", err)
			}
			continue
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
