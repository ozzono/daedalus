package workflows

import (
	"encoding/json"
	"fmt"
	"strings"

	"go.temporal.io/sdk/workflow"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/template"
)

// maxSlimStepRounds caps one sub-task's ground-truth fix loop (suite run +
// review per round) before the run parks: each round costs a suite
// execution and a fresh reviewer round, and a sub-task that cannot
// converge within this many — the same budget the green-stage rebuild cap
// grants a whole implementation, which one atomic sub-task should never
// need — is looping on the provider budget. The counter resets per
// sub-task: each atomic step starts with a clean budget.
const maxSlimStepRounds = 8

// SlimWorkflow drives the slim flow — the Micro-Stepped Atomic Loop for
// context-limited, self-hosted models (target agent: pi). Config-gated:
// with slim: true a default `daedalus run` starts this flow (an explicit
// -w always wins). Three phases, all on the machinery the other flows
// share, so every verdict, cap, park, and the finalize contract are
// inherited unchanged:
//
//  1. Planner: one round atomizes the task into a strictly ordered queue
//     of sub-tasks — each at most 1–2 target files, each verifiable
//     against its own acceptance criteria — emitted as a raw JSON array
//     (template.SlimSubtask). One strict re-ask absorbs a malformed
//     reply; an unparseable second reply parks the run.
//
//  2. Atomic loops: each sub-task in order runs implement ↔ review on
//     the shared pipelineRun machinery, with the native suite executed
//     every round as terminal ground truth (never self-confirmation:
//     compiler and test output decide, and a red suite blocks approval
//     even when the reviewer says yes). The worker conversation chains
//     across the whole run — plan and completed sub-tasks ride along as
//     progressive context — while every review round is a completely
//     fresh reviewer session (SlimWorkflow sets freshReviews: the REVIEW
//     role inherits nothing from the WORKER role; each review is a fresh
//     read of the current diff against that sub-task's criteria). A
//     sub-task's loop parks after maxSlimStepRounds non-converging
//     rounds, and the shared identical-verdict, verdictless, timeout,
//     and quota-heartbeat caps all stay live.
//
//  3. Finalize: the inherited deliverable contract — approved work is
//     committed and the branch preserved.
func SlimWorkflow(ctx workflow.Context, input PipelineInput) (string, error) {
	run, cleanup := startRun(ctx, input)
	defer cleanup()
	run.freshReviews = true
	if err := run.createWorktree(); err != nil {
		return "", err
	}
	command, err := run.preFlightGate()
	if err != nil {
		return "", err
	}
	// The suite is slim's every-round terminal ground truth, so a
	// repository without a discoverable suite cannot run slim at all —
	// park at the gate, before any agent budget is spent, instead of
	// failing opaquely mid-run on the first empty suite command.
	if command == "" {
		return "", run.park("slim requires a discoverable native test suite — every sub-task round is gated on its output, and this repository has none")
	}

	// Phase 1 — planner. The round opens the run's dev conversation, so
	// the plan itself is the first block of the worker's progressive
	// context.
	planPrompt, err := template.SlimPlan(input.Prompt)
	if err != nil {
		return "", fmt.Errorf("build slim plan prompt: %w", err)
	}
	planReply, err := run.runAgent(planPrompt, "slim-plan", activities.RoleDev, &run.devSession)
	if err != nil {
		return "", fmt.Errorf("planner round: %w", err)
	}
	subtasks, perr := parseSlimPlan(planReply.Text)
	if perr != nil {
		// One strict re-ask, resumed in the planner's own conversation:
		// the model sees its previous reply and corrects it.
		run.logger.Warn("Planner reply was not a parseable subtask queue; re-asking", "Error", perr)
		retry := planPrompt + "\n\nYour previous reply could not be parsed as the required JSON array (" + perr.Error() + "). Reply again with ONLY the raw JSON array — no prose, no markdown code fences."
		planReply, err = run.runAgent(retry, "slim-plan", activities.RoleDev, &run.devSession)
		if err != nil {
			return "", fmt.Errorf("planner retry round: %w", err)
		}
		if subtasks, perr = parseSlimPlan(planReply.Text); perr != nil {
			return "", run.park(fmt.Sprintf("planner could not produce a parseable subtask queue: %v", perr))
		}
	}
	run.logger.Info("Plan atomized", "Subtasks", len(subtasks))

	// Phase 2 — one atomic loop per sub-task, in the plan's order.
	for i, st := range subtasks {
		if err := run.slimStep(st, i+1, len(subtasks), command); err != nil {
			return "", err
		}
	}
	return run.finalize()
}

// slimStep runs one sub-task's atomic loop: an implement round in the
// chained dev conversation, then fix rounds until the suite is green AND
// a fresh reviewer approves against the sub-task's acceptance criteria.
// index/total are 1-based, for the prompts and the log.
func (r *pipelineRun) slimStep(st template.SlimSubtask, index, total int, command string) error {
	r.logger.Info("Slim subtask starting", "ID", st.ID, "Index", index, "Of", total, "Description", st.Description)
	prompt, err := template.SlimStep(index, total, st)
	if err != nil {
		return fmt.Errorf("build slim step prompt: %w", err)
	}
	if _, err := r.runAgent(prompt, fmt.Sprintf("slim-step-%d", st.ID), activities.RoleDev, &r.devSession); err != nil {
		return fmt.Errorf("slim step agent run: %w", err)
	}
	// The reviewer judges this sub-task against exactly its criteria.
	r.reviewCriteria = st.AcceptanceCriteria
	defer func() { r.reviewCriteria = nil }()
	focus := fmt.Sprintf("subtask %d (%s)", st.ID, st.Description)
	for round := 0; ; round++ {
		if round >= maxSlimStepRounds {
			return r.park(fmt.Sprintf("slim subtask %d failed to converge — %d ground-truth rounds without a green suite and an approving review; last fix prompt scope: %s",
				st.ID, maxSlimStepRounds, st.Description))
		}
		result, err := r.runSuite(command)
		if err != nil {
			return err
		}
		// Terminal ground truth gates approval: a red suite is never
		// accepted, whatever the reviewer says — the logs travel to the
		// fix prompt below. Green logs stay out of the review prompt:
		// the reviewer can run the suite itself, and a context-limited
		// model should not carry a full green log it does not need.
		testLogs := ""
		if !result.Passed {
			testLogs = result.Logs
		}
		var session string // freshReviews: never read, never written
		verdict, err := r.review(focus, testLogs, false, false, "", activities.RoleDevReview, &session, false)
		if err != nil {
			return fmt.Errorf("slim subtask review: %w", err)
		}
		if verdict.NeedsMaintainer {
			r.logger.Info("Slim subtask review halted the run for maintainer input")
			return r.park(fmt.Sprintf("slim subtask %d review halted the run — the sub-task cannot be completed as stated: %s",
				st.ID, verdict.Comments))
		}
		if result.Passed && verdict.Approved {
			r.logger.Info("Slim subtask accepted", "ID", st.ID, "Index", index, "Of", total)
			return nil
		}
		logs, comments := "", ""
		if !result.Passed {
			logs = result.Logs
		}
		if !verdict.Approved {
			comments = verdict.Comments
		}
		fixPrompt, err := template.SlimFix(st.Description, logs, comments)
		if err != nil {
			return fmt.Errorf("build slim fix prompt: %w", err)
		}
		if g := r.drainGuidance(); g != "" {
			r.logger.Info("Operator guidance received, folding into fix prompt")
			fixPrompt = g + "\n\n" + fixPrompt
		}
		if _, err := r.runAgent(fixPrompt, fmt.Sprintf("slim-step-%d", st.ID), activities.RoleDev, &r.devSession); err != nil {
			return fmt.Errorf("slim fix agent run: %w", err)
		}
	}
}

// parseSlimPlan extracts the planner's subtask queue from its reply: the
// outermost JSON array of the text (a strict model emits only the array;
// a chatty one may wrap it in prose), validated for the fields the loop
// cannot run without. Pure string/JSON work — deterministic, so it is
// safe to call from workflow code.
func parseSlimPlan(text string) ([]template.SlimSubtask, error) {
	start := strings.Index(text, "[")
	end := strings.LastIndex(text, "]")
	if start < 0 || end <= start {
		return nil, fmt.Errorf("no JSON array found in a %d-character reply", len(text))
	}
	var out []template.SlimSubtask
	if err := json.Unmarshal([]byte(text[start:end+1]), &out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("the JSON array is empty")
	}
	for i, st := range out {
		if st.Description == "" {
			return nil, fmt.Errorf("subtask %d (id %d) carries no description", i+1, st.ID)
		}
		if len(st.AcceptanceCriteria) == 0 {
			return nil, fmt.Errorf("subtask %d (id %d) carries no acceptance criteria", i+1, st.ID)
		}
	}
	return out, nil
}
