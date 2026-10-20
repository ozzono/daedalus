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
// test loop resumes. AllowedPaths is solely the finalize scope check's
// policy — nothing reads it mid-round, so the tester's fencing is
// prompt-level — and one rebuild lifts the check for the rest of the
// run: the approved implementation change rides the working tree, and
// that wholesale lift is the price of the rebuild path. Only a spent
// cap parks the run for the maintainer. Suite executions
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
		verdict, err := run.review(reviewOpts{focus: "the test suite", testLogs: logs, testsInScope: true, agentReply: testerReply.Text, role: activities.RoleTestReview, session: &run.testReviewSession})
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
			// like the feature-dev green stage (pipelineRun.rebuildRound):
			// a dev-role agent run on the finding, its change gated by the
			// full code review loop, then the test loop resumes. The dev
			// round's write fencing is prompt-level only: AllowedPaths has
			// no mid-run consumer — its one read is the finalize scope
			// check. The rebuild rides the verdict, so replay is
			// deterministic.
			reply, restart, rerr := run.rebuildRound(verdict.Comments, false)
			if !restart {
				return "", rerr
			}
			// The rebuild round's reply is what the test reviewer next sees
			// as the latest reply: quoting the pre-rebuild tester here would
			// describe a change the reviewer is not looking at.
			testerReply = reply
			continue
		}
		if result.Passed && verdict.Approved {
			run.logger.Info("Tests pass and test review approved")
			if run.testRebuilds > 0 {
				// A rebuild's approved implementation change is still
				// working-tree state at finalize — rounds never commit —
				// and the test-path-only scope check would park the run
				// on it. The lift is per-run, not per-change: from the
				// first rebuild on, the structural check is skipped for
				// the whole tree (an empty scope short-circuits it), so
				// every later edit — the code-reviewed change included —
				// finalizes unchecked. That is the price of letting the
				// rebuild's implementation change through; with no
				// rebuild the check runs as always.
				run.input.AllowedPaths = nil
			}
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
