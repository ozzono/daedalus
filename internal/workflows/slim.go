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
//  1. Planner: two rounds on the run's dev conversation. A
//     pure-generation round writes the plan in prose — no format
//     pressure, so the planning model spends its budget on the plan
//     rather than on the wire shape — and a parse round transcribes that
//     plan into a strictly ordered queue of sub-tasks, each at most 1–2
//     target files, each verifiable against its own acceptance criteria,
//     emitted as a raw JSON array (template.SlimSubtask). A deterministic
//     trailing-comma strip absorbs that one purely decorative syntax class
//     before anything costs budget, and one strict re-ask of the parse
//     round absorbs any other malformed reply (resumed in the parse
//     round's own conversation, so the model sees and corrects its
//     previous reply); an unparseable second reply parks the run.
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
	run, cleanup, err := startRun(ctx, input)
	if err != nil {
		return "", err
	}
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
	// context. Pure generation: the plan is prose, and the wire format is
	// nobody's job in this round.
	planPrompt, err := template.SlimPlan(input.Prompt)
	if err != nil {
		return "", fmt.Errorf("build slim plan prompt: %w", err)
	}
	planReply, err := run.runAgent(planPrompt, "slim-plan", activities.RoleDev, &run.devSession)
	if err != nil {
		return "", fmt.Errorf("planner round: %w", err)
	}

	// Phase 1b — parse. The plan is transcribed into the machine queue in
	// the same dev conversation: the model sees the plan it is
	// transcribing, and the queue joins it in the worker's progressive
	// context.
	parsePrompt, err := template.SlimParse(planReply.Text)
	if err != nil {
		return "", fmt.Errorf("build slim parse prompt: %w", err)
	}
	parseReply, err := run.runAgent(parsePrompt, "slim-parse", activities.RoleDev, &run.devSession)
	if err != nil {
		return "", fmt.Errorf("parse round: %w", err)
	}
	subtasks, perr := parseSlimPlan(parseReply.Text)
	if perr != nil {
		// One strict re-ask, resumed in the parse round's own
		// conversation: the model sees its previous reply and corrects it
		// (never a re-run of the planner — the plan is not the thing that
		// failed). The re-ask text is an overridable template; the parse
		// prompt rides above it so the contract stays adjacent to the
		// correction.
		run.logger.Warn("Parse round reply was not a parseable subtask queue; re-asking", "Error", perr)
		reask, err := template.SlimParseReask(perr.Error())
		if err != nil {
			return "", fmt.Errorf("build slim parse re-ask prompt: %w", err)
		}
		parseReply, err = run.runAgent(parsePrompt+"\n\n"+reask, "slim-parse", activities.RoleDev, &run.devSession)
		if err != nil {
			return "", fmt.Errorf("parse retry round: %w", err)
		}
		if subtasks, perr = parseSlimPlan(parseReply.Text); perr != nil {
			return "", run.park(fmt.Sprintf("parse round could not produce a parseable subtask queue: %v", perr))
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

// parseSlimPlan extracts the parse round's subtask queue from its reply.
// The parse round's output contract is the raw JSON array and nothing
// else, so the parse is strict: the whole reply — whitespace trimmed, one
// wrapping markdown code fence tolerated — must unmarshal as the array.
// There is no bracket slicing: the extractor this replaces took the text
// from the first "[" through the last "]", so a stray bracketed phrase
// anywhere in surrounding prose (a "[Draft]" note) widened the slice and
// broke an otherwise valid queue; under the two-phase planner the parse
// round owns the wire format outright, so a full-string unmarshal is both
// available and stricter. Validated for the fields the loop cannot run
// without. On a failed unmarshal, one deterministic repair is attempted
// before the error is returned: trailing commas are stripped
// (stripTrailingCommas) and the parse retried — the syntax class small
// self-hosted models emit routinely, and repeat on correction, so it is
// absorbed for free and the parse round's single re-ask stays available
// for damage that actually needs the model (prose, truncation, wrong
// shape). Pure string/JSON work — deterministic, so it is safe to call
// from workflow code.
func parseSlimPlan(text string) ([]template.SlimSubtask, error) {
	if s := stripCodeFence(strings.TrimSpace(text)); s != "" {
		text = s
	}
	var out []template.SlimSubtask
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		// Only a full parse of the stripped text is accepted; anything
		// still broken keeps the original error — the re-ask quotes it,
		// so it must describe the reply the model actually wrote.
		if repaired := stripTrailingCommas([]byte(text)); json.Unmarshal(repaired, &out) != nil {
			return nil, fmt.Errorf("reply is not the required raw JSON array: %w", err)
		}
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

// stripTrailingCommas removes commas directly preceding a closing brace
// or bracket — JSON's one purely decorative syntax error — walking the
// bytes with string awareness: a comma inside a string literal (escapes
// included) is content and is never touched, so the repair cannot corrupt
// a value on its way to fixing the syntax. Multi-byte UTF-8 never carries
// ASCII bytes, so the byte walk is encoding-safe. Deliberately not a
// truncation repair: a reply cut off mid-array has no closing bracket for
// the strip to act on, and auto-closing one would unmarshal a truncated
// plan as a complete queue with subtasks silently missing — truncated
// replies keep failing and spend the re-ask. Single pass, in place (w
// never passes i), no parse: deterministic, so it is safe to call from
// workflow code.
func stripTrailingCommas(b []byte) []byte {
	w, inString := 0, false
	for i := 0; i < len(b); i++ {
		c := b[i]
		if inString {
			b[w] = c
			w++
			switch {
			case c == '\\' && i+1 < len(b): // escape: the next byte is content too
				i++
				b[w] = b[i]
				w++
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
			b[w] = c
			w++
		case ',':
			j := i + 1
			for j < len(b) && (b[j] == ' ' || b[j] == '\t' || b[j] == '\n' || b[j] == '\r') {
				j++
			}
			if j < len(b) && (b[j] == '}' || b[j] == ']') {
				continue // drop the comma; the closing bracket copies on its own pass
			}
			b[w] = c
			w++
		default:
			b[w] = c
			w++
		}
	}
	return b[:w]
}

// stripCodeFence drops one wrapping markdown code fence from a reply: a
// leading fence line (three or more backticks, optionally followed by an
// info string such as "json") and a trailing fence line when present. The
// parse round is told to reply with the bare array, but a fence is the one
// formatting wrapper models emit anyway and it carries no information —
// trimming it keeps the strict parse from spending the round's only
// re-ask on decoration. Any other prose stays and fails the parse. A
// fenced reply always spans lines, so a single-line reply is returned
// unchanged — ponytail: a single-line fence-plus-array ("```json [...]"
// with no newline) is the one shape this line-based strip cannot see, so
// such a reply spends the parse round's re-ask on its fence.
func stripCodeFence(s string) string {
	first, rest, found := strings.Cut(s, "\n")
	if !found || !strings.HasPrefix(strings.TrimLeft(first, " \t"), "```") {
		return s
	}
	lines := strings.Split(strings.TrimRight(rest, "\n"), "\n")
	if last := len(lines) - 1; strings.HasPrefix(strings.TrimLeft(lines[last], " \t"), "```") {
		lines = lines[:last]
	}
	return strings.Join(lines, "\n")
}
