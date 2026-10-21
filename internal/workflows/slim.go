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
//     emitted as a raw JSON array (template.SlimSubtask). Two
//     deterministic repairs — a trailing-comma strip and a bracket
//     closure of a reply whose every element was complete but whose final
//     closers were dropped (the run memo is stamped ParseQueueRepaired
//     when it fires) — absorb those routine syntax classes before
//     anything costs budget, and one strict re-ask of the parse round
//     absorbs any other malformed reply (resumed in the parse round's own
//     conversation, so the model sees and corrects its previous reply);
//     an unparseable second reply parks the run.
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
	repairOK := slimParseRepairEnabled(ctx)
	subtasks, repaired, perr := parseSlimPlan(parseReply.Text, repairOK)
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
		if subtasks, repaired, perr = parseSlimPlan(parseReply.Text, repairOK); perr != nil {
			return "", run.park(fmt.Sprintf("parse round could not produce a parseable subtask queue: %v", perr))
		}
	}
	if repaired {
		// The repair's firing is durable and run-visible, not just
		// logged: a short-looking queue that went green must answer "did
		// the parser close this?" where a maintainer actually looks. The
		// memo renders with the run; no search-attribute registration.
		run.logger.Warn("Parse reply closed by repair", "Subtasks", len(subtasks))
		workflow.UpsertMemo(run.ctx, map[string]interface{}{"ParseQueueRepaired": true})
	}
	run.logger.Info("Plan atomized", "Subtasks", len(subtasks))

	// Phase 2 — one atomic loop per sub-task, in the plan's order.
	for i, st := range subtasks {
		if err := run.slimStep(st, i+1, len(subtasks), command); err != nil {
			return "", err
		}
	}
	// Terminal freshness gate (approve side), slim's half of the
	// invariant: the loop above leaves lastSuite no older than the last
	// edit by construction — every accepted sub-task's suite runs after
	// its last fix round — so this fires only if that loop shape ever
	// changes. Then the close is re-grounded on a fresh execution of the
	// already-resolved suite command instead of approving unvalidated:
	// green finalizes; red, verdict-less (a timeout), or unexecutable
	// parks — there is no loop left to fix any of them, and the park
	// record says which. The parks go through parkRaw, not park: the
	// freshness outcome is decided and written right here, and park
	// re-checking suiteStale would re-fire the force-run a second time
	// at this terminal transition when the first attempt never executed.
	// Re-entering a sub-task loop is not an option either way: its first
	// act is another implement round, a fresh edit, not a re-validation.
	if run.suiteGate && run.suiteStale() {
		run.logger.Info("Suite stale at approval (agent rounds ran after the last suite execution); running the suite once before finalizing")
		result, ran := run.forceTerminalSuite("approve")
		if !ran {
			return "", run.parkRaw("terminal suite freshness gate: the suite state was stale at approval and the forced suite run did not execute — the final state is unvalidated")
		}
		if result.TimedOut {
			return "", run.parkRaw("terminal suite freshness gate: the forced suite run timed out without a verdict — the final state is unvalidated")
		}
		if !result.Passed {
			return "", run.parkRaw("terminal suite freshness gate: the forced suite run came out red — the final state never passed the suite")
		}
	}
	return run.finalize()
}

// slimStep runs one sub-task's atomic loop: an implement round in the
// chained dev conversation, then fix rounds until the suite is green AND
// a fresh reviewer approves against the sub-task's acceptance criteria.
// index/total are 1-based, for the prompts and the log. The last
// sub-task's reviews are the run's terminal review — its worktree diff
// carries every earlier sub-task's accepted work, so it alone sees the
// cumulative diff of the whole plan — and they resolve as RoleFinalReview:
// the final_reviewer provider group when configured, falling back to the
// reviewer chain (see activities.reviewerEnv). Every earlier sub-task's
// reviews resolve as the plain reviewer.
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
	reviewRole := activities.RoleDevReview
	if index == total {
		reviewRole = activities.RoleFinalReview
	}
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
		verdict, err := r.review(reviewOpts{focus: focus, testLogs: testLogs, role: reviewRole, session: &session})
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
			comments = verdict.Comments + verdictNote(verdict)
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

// slimParseRepairChangeID gates the parse round's bracket-closure repair
// behind its own version marker, for the same replay reason as the
// pipeline.go markers (visibilityChangeID and friends): the repair changes
// whether the parse round's re-ask activity is scheduled, so a run whose
// history was recorded before the repair and replayed by an upgraded
// worker would issue commands that history does not hold and wedge at the
// next worker upgrade. Pre-marker replays parse with the comma strip only,
// exactly as their history recorded. The memo stamp (ParseQueueRepaired)
// needs no gate of its own — repaired can only fire when the gated repair
// did.
const slimParseRepairChangeID = "slim-parse-closer-repair"

func slimParseRepairEnabled(ctx workflow.Context) bool {
	return workflow.GetVersion(ctx, slimParseRepairChangeID, workflow.DefaultVersion, 1) == 1
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
// without. On a failed unmarshal, two deterministic repairs are attempted
// before the error is returned — each absorbs a syntax class small
// self-hosted models emit routinely and repeat on correction, so it is
// absorbed for free and the parse round's single re-ask stays available
// for damage that actually needs the model (prose, truncation, wrong
// shape): appendImpliedClosers completes a reply whose every element was
// already fully written but whose final closing brackets were dropped,
// accepted only when the closed text re-parses in full and passes the
// validation below, and never silently — repaired reports the rescue so
// the caller warns and stamps the run memo; then stripTrailingCommas. The
// two are never composed: each runs on the original text alone, so a reply
// cut off right after a complete element plus a comma gains neither repair
// and keeps spending the re-ask (appendImpliedClosers carries the full
// rationale). repair gates the closure for replay — a history predating
// slimParseRepairChangeID must decide the parse exactly as the code that
// recorded it did. Pure string/JSON work — deterministic, so it is safe to
// call from workflow code.
func parseSlimPlan(text string, repair bool) ([]template.SlimSubtask, bool, error) {
	if s := stripCodeFence(strings.TrimSpace(text)); s != "" {
		text = s
	}
	var out []template.SlimSubtask
	closed := false
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		// Only a full parse of the repaired text is accepted; anything
		// still broken keeps the original error — the re-ask quotes it,
		// so it must describe the reply the model actually wrote.
		if repair {
			if fixed := appendImpliedClosers([]byte(text)); fixed != nil && json.Unmarshal(fixed, &out) == nil {
				closed = true
			}
		}
		if !closed {
			out = nil
			if fixed := stripTrailingCommas([]byte(text)); json.Unmarshal(fixed, &out) != nil {
				return nil, false, fmt.Errorf("reply is not the required raw JSON array: %w", err)
			}
		}
	}
	if len(out) == 0 {
		return nil, false, fmt.Errorf("the JSON array is empty")
	}
	for i, st := range out {
		if st.Description == "" {
			return nil, false, fmt.Errorf("subtask %d (id %d) carries no description", i+1, st.ID)
		}
		if len(st.AcceptanceCriteria) == 0 {
			return nil, false, fmt.Errorf("subtask %d (id %d) carries no acceptance criteria", i+1, st.ID)
		}
	}
	return out, closed, nil
}

// appendImpliedClosers returns b with the closing brackets its open-bracket
// depth still implies appended — "]" and "}" in reverse opening order — or
// nil when nothing may be closed. The walk is string-aware exactly like
// stripTrailingCommas: a bracket inside a string literal (escapes
// included) is content and is never balanced against, and a walk that ends
// inside an unterminated string closes nothing — a reply cut off mid-value
// is a genuine truncation, and inventing the quote that ends it would
// fabricate content, the one thing this repair may not do. Mismatched
// brackets pop whatever is innermost on purpose: the closed text is
// accepted only on a full re-parse plus the caller's per-field validation,
// so wrongly nested text has no route through this repair. Deliberately
// not composed with the comma strip: the strip runs its own path on the
// original text, so a reply cut off right after a complete element plus a
// comma gains neither repair and keeps spending the re-ask — closing AND
// stripping one text would silently accept a shortened queue, the
// truncation-masking outcome the strict parse exists to prevent.
// Deterministic string/JSON work — safe to call from workflow code.
func appendImpliedClosers(b []byte) []byte {
	var open []byte
	inString := false
	for i := 0; i < len(b); i++ {
		if inString {
			switch b[i] {
			case '\\': // escape: the next byte is content too
				i++
			case '"':
				inString = false
			}
			continue
		}
		switch b[i] {
		case '"':
			inString = true
		case '[', '{':
			open = append(open, b[i])
		case ']', '}':
			if len(open) > 0 {
				open = open[:len(open)-1]
			}
		}
	}
	if inString || len(open) == 0 {
		return nil
	}
	closed := make([]byte, len(b), len(b)+len(open))
	copy(closed, b)
	for i := len(open) - 1; i >= 0; i-- {
		c := byte('}')
		if open[i] == '[' {
			c = ']'
		}
		closed = append(closed, c)
	}
	return closed
}

// stripTrailingCommas removes commas directly preceding a closing brace
// or bracket — JSON's one purely decorative syntax error — walking the
// bytes with string awareness: a comma inside a string literal (escapes
// included) is content and is never touched, so the repair cannot corrupt
// a value on its way to fixing the syntax. Multi-byte UTF-8 never carries
// ASCII bytes, so the byte walk is encoding-safe. Deliberately not a
// truncation repair, and the truncation-masking hole (a truncated plan
// unmarshaled as a complete queue with subtasks silently missing) stays
// closed two ways: this strip and the bracket closure (appendImpliedClosers)
// are never composed — each runs on the original text alone, so a reply
// cut off right after a complete element plus a comma ends in a comma the
// strip cannot drop (no closing bracket follows it) and gains neither
// repair — and the closure is accepted only on a full re-parse of the
// closed text plus the per-field validation, with the run memo stamped
// when it fires. Single pass, in place (w never passes i), no parse:
// deterministic, so it is safe to call from workflow code.
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
