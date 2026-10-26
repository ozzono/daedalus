package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/ozzono/daedalus/internal/activities"
	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
)

// claudeProjectsDir returns claude's transcript directory for a worktree.
// A package var so tests can point it at a fake tree.
var claudeProjectsDir = activities.ClaudeProjectDir

// piSessionsDir returns pi's per-worktree session directory for a worktree.
// A package var so tests can point it at a fake tree.
var piSessionsDir = activities.PiSessionsDir

// codexNewestRollout returns the newest rollout file codex holds for a
// worktree. A package var so tests can point it at a fake tree.
var codexNewestRollout = activities.CodexNewestRollout

// opencodeNewestUpdate returns the newest updated stamp opencode lists for
// a worktree. A package var so tests can point it at a fake list.
var opencodeNewestUpdate = activities.OpencodeNewestUpdate

// runTaskLog implements `daedalus log <workflow-id>`: print the task's
// captured log file — with a log-tail block appended naming when the last
// log arrived and the run's state (see logTail) — or, with --status, a
// short status brief instead (the brief already is the freshness summary,
// so it carries no tail). File- and session-state-derived only: no
// temporal connection, no config (the one subprocess is opencode's session
// list, for opencode freshness).
func runTaskLog(workflowID string, status bool) {
	path, err := activities.TaskLogPath(workflowID)
	if err != nil {
		exitf("%v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			exitf("no task log for %s at %s — the run has not started any rounds yet", workflowID, path)
		}
		exitf("%v", err)
	}
	if status {
		taskStatusBrief(string(data))
		return
	}
	os.Stdout.Write(data)
	fmt.Print(logTail(workflowID, plainTailState(string(data))))
}

// plainTailState renders the plain log's run-state clause for the tail
// block, derived from the task log alone — the command never dials
// Temporal, so a finished run and a crashed one are both "no round in
// flight", and the clause says so rather than guessing an outcome.
func plainTailState(log string) string {
	_, stage, _, running := lastRoundState(log)
	if !running {
		return "run not running (no round in flight per task log): this dump is final as far as the file shows — completed vs crashed needs the temporal view (-cot)"
	}
	if stage == "" {
		stage = "unknown"
	}
	return fmt.Sprintf("round in flight (stage=%s): run still going — this dump is not final", stage)
}

// runTaskLogCot implements `daedalus log <workflow-id> -cot`: print the
// run's chain-of-thought logs instead of the task
// log — one complete section per jailed round (implementation and review),
// never truncated. Each section opens with a `=====` divider and stamps
// the round's finish time in its header — the completion event's, or the
// terminal event's for a round that died — so consecutive rounds read as
// separate blocks. cotN tails the finished sections (completed or dead)
// to the last N rounds (-1 means every round); a live in-flight section,
// when one exists, always follows — it is the newest material there is.
// Rounds with a native thinking channel (claude, amp, pi)
// show their captured Thinking — implementing and reviewer rounds alike;
// openai-wire rounds whose reasoning arrived inline show the
// <think>-fenced part; rounds with no CoT at all say so rather than
// rendering an empty section. Every section header carries the run-id
// attribution (the task-log blocks' 8-char-prefix convention), so a
// continued session's identically-headed retry sections attribute to the
// execution that produced them. The Temporal address comes from
// the resolved config when one is found — an unloadable one fails like
// every other client command — else the default, so the command works
// wherever the Temporal service is reachable. When a round is in flight
// right now, a live section rendered from the agent's host-side transcript
// follows the finished ones (see liveCotSectionWith). The dump ends with a
// log-tail block naming when the last log arrived and the workflow's real
// Temporal state, so a finished run's output never reads as live (see
// logTail).
func runTaskLogCot(workflowID string, cotN int) {
	host := config.DefaultTemporalHost
	if path, err := resolveConfigPath(defaultConfigPath); err == nil {
		host = loadConfig(path).Temporal.Host
	}
	c, err := newClient(config.Config{Temporal: config.TemporalConfig{Host: host}})
	if err != nil {
		exitf("%v", err)
	}
	defer c.Close()

	// A restarted or continued session keeps its workflow id across
	// temporal run ids — the task log's round blocks span all of them — so
	// the CoT view walks every execution of the id, oldest first, not just
	// the latest run that an empty run id reads.
	runs := listWorkflowRuns(c, workflowID)
	if len(runs) == 0 {
		// Visibility saw no executions (a very fresh run, or a visibility
		// backend lagging behind): fall back to the latest run's history.
		runs = []string{""}
	}
	// The live daemon is the dump's primary source (the relay capture's
	// content, the otel ledger's usage stamps); an unreachable one
	// degrades the whole dump to the Temporal walk, unstamped, never an
	// error.
	src := ""
	if root, ok := metricsRelayEndpoint(); ok {
		src = root
	}
	sections, err := cotSectionsWith(c, workflowID, runs, src)
	if err != nil {
		exitf("%v", err)
	}
	// cotN tails the finished sections (completed or dead) to the last N;
	// whole sections only — the no-truncation rule is per section, never
	// per byte.
	shown := sections
	if cotN >= 0 && len(sections) > cotN {
		shown = sections[len(sections)-cotN:]
	}
	for _, s := range shown {
		fmt.Print("=====================================\n")
		fmt.Print(s)
		fmt.Print("\n")
	}
	// The in-flight section replaces, never accompanies, the no-rounds
	// message: a round running with zero finished rounds is exactly the
	// case the live view exists for. It rides outside the tail: the round
	// it renders is newer than every finished one.
	live := liveCotSectionWith(workflowID, c)
	if live != "" {
		fmt.Print("=====================================\n")
		fmt.Print(live)
	}
	if len(sections) == 0 && live == "" {
		exitf("no jailed agent rounds in %s's Temporal history — the run has not started any rounds yet, or predates structured round capture", workflowID)
	}
	// The tail rides only on the dump path: the zero-round error above
	// already names that state ("not started"), and an error line plus a
	// tail would say it twice.
	fmt.Print(logTail(workflowID, cotTailState(c, workflowID)))
}

// cotSectionsWith walks each run's history like the usage report does: pair
// every jailed round's terminal event with its scheduled input, which
// carries the round's role and (when the run pinned one) its agent, and
// render one section per finished round — completed, or dead by failure,
// cancellation, or timeout (the dead ones render their degraded section;
// see the terminal branch below) — oldest first. Each section is its header line then its
// body — the round's CoT, or the no-CoT note — without the ===== divider;
// the printers add their own. Shared by the CLI's -cot dump and the run
// detail's last-round box.
//
// src names the live metrics daemon's relay root and turns on the source
// precedence: a keyed round's relay capture (content, stamped relay), else
// its otel ledger entries (usage stamp over the Temporal CoT, stamped
// otel), else the Temporal walk itself (stamped temporal). An empty src —
// no daemon — is the plain walk, unstamped: a capture-absent dump and a
// daemon-less one render byte-identically to the pre-capture shape.
func cotSectionsWith(c client.Client, workflowID string, runs []string, src string) ([]string, error) {
	dc := converter.GetDefaultDataConverter()
	type scheduledRound struct {
		typ        string
		role       activities.SessionRole
		agent      string
		worktree   string
		activityID string
	}
	otel, live := cotOtelUsage(src)
	// deadSection renders a failed, cancelled, or timed-out round's
	// degraded section: the shared header stamped with the terminal
	// event's time, the terminal face, then the usual source-precedence
	// body — the relay capture still matches by roundKey because the
	// activity id rode the scheduled event, so a killed round renders
	// whatever wire traffic it managed to stream. temporal is empty: no
	// result payload exists to walk.
	deadSection := func(n int, sr scheduledRound, runID, face string, at time.Time) string {
		roleName := string(sr.role)
		if roleName == "" {
			roleName = "none"
		}
		return cotHeader(n, string(sr.role), sr.agent, runID, at) + "\n" +
			face + "\n" +
			cotRoundBody(src, &live, otel, ledgerTaskID(workflowID), roleName, sr.activityID, "")
	}
	var sections []string
	for _, runID := range runs {
		scheduled := map[int64]scheduledRound{}
		iter := c.GetWorkflowHistory(context.Background(), workflowID, runID,
			false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			ev, err := iter.Next()
			if err != nil {
				return nil, fmt.Errorf("read history of %s: %w", workflowID, err)
			}
			if s := ev.GetActivityTaskScheduledEventAttributes(); s != nil {
				typ := s.GetActivityType().GetName()
				if !agentActivityTypes[typ] {
					continue
				}
				sr := scheduledRound{typ: typ, activityID: s.GetActivityId()}
				if ps := s.GetInput().GetPayloads(); len(ps) > 0 {
					// Same shape-vs-type dispatch as the result decode below:
					// each activity type carries its own input struct.
					if typ == "RunJailedClaudeActivity" {
						var in activities.AgentRunInput
						if dc.FromPayload(ps[0], &in) == nil {
							sr.role, sr.agent, sr.worktree = in.Role, in.Agent, in.WorktreePath
						}
					} else {
						var in activities.ReviewInput
						if dc.FromPayload(ps[0], &in) == nil {
							sr.role, sr.agent, sr.worktree = in.Role, in.Agent, in.WorktreePath
						}
					}
				}
				scheduled[ev.GetEventId()] = sr
				continue
			}
			a := ev.GetActivityTaskCompletedEventAttributes()
			if a == nil {
				// A round that died before completing — a failure or a
				// cancellation (a kill, an abort) — still owns a section:
				// rendering only completed rounds left every view tailing
				// the walk pointing at a hours-stale predecessor, with the
				// dead round's own relay capture sitting unrendered. Only a
				// terminal event emits: a scheduled event with no terminal
				// yet is the live round, and the live path
				// (liveCotSectionWith) owns that face — emitting here too
				// would double-render it.
				if f := ev.GetActivityTaskFailedEventAttributes(); f != nil {
					if sr := scheduled[f.GetScheduledEventId()]; agentActivityTypes[sr.typ] {
						face := "round failed"
						if msg := singleLine(f.GetFailure().GetMessage()); msg != "" {
							face += ": " + msg
						}
						sections = append(sections, deadSection(len(sections)+1, sr, runID,
							face, ev.GetEventTime().AsTime()))
					}
					continue
				}
				// A cancelled round carries no result payload to name a
				// reason with — the plain face says all that is known.
				if cx := ev.GetActivityTaskCanceledEventAttributes(); cx != nil {
					if sr := scheduled[cx.GetScheduledEventId()]; agentActivityTypes[sr.typ] {
						sections = append(sections, deadSection(len(sections)+1, sr, runID,
							"round cancelled", ev.GetEventTime().AsTime()))
					}
					continue
				}
				// The third terminal a round can die by: its StartToClose
				// burned before the activity could honor its own deadline —
				// rare (the round's wait paths watch ctx), but reachable,
				// and the walk renders it like any other killed round.
				if tx := ev.GetActivityTaskTimedOutEventAttributes(); tx != nil {
					if sr := scheduled[tx.GetScheduledEventId()]; agentActivityTypes[sr.typ] {
						sections = append(sections, deadSection(len(sections)+1, sr, runID,
							"round timed out", ev.GetEventTime().AsTime()))
					}
				}
				continue
			}
			sr := scheduled[a.GetScheduledEventId()]
			ps := a.GetResult().GetPayloads()
			if len(ps) == 0 {
				continue
			}
			var cot string
			switch sr.typ {
			case "RunJailedClaudeActivity":
				var r activities.AgentRunResult
				if dc.FromPayload(ps[0], &r) != nil {
					continue
				}
				cot = r.Thinking
				if cot == "" {
					cot = inlineCoT(r.Text)
				}
				if cot == "" {
					// json-mode claude stdout carries only the terminal
					// result event, so Thinking is structurally empty for
					// every json-mode claude round; the host-side
					// transcript holds the round's full assistant stream,
					// and the round's own SessionID pins the file
					// (claudeTranscriptCoT). Empty when that transcript is
					// absent — a round that ran on another host, or a
					// pruned ~/.claude — and the no-CoT note stands.
					cot = claudeTranscriptCoT(sr.worktree, r.SessionID)
				}
			case "RunJailedReviewerActivity":
				var r activities.ReviewResult
				if dc.FromPayload(ps[0], &r) != nil {
					continue
				}
				// Native thinking first (the activity captures it since the
				// reviewer-thinking fix), the inline fenced CoT second —
				// rounds whose history predates the Thinking field, and
				// plain-text reviewers, still surface what they have.
				cot = r.Thinking
				if cot == "" {
					cot = inlineCoT(r.Comments)
				}
			default:
				continue
			}
			var b strings.Builder
			b.WriteString(cotHeader(len(sections)+1, string(sr.role), sr.agent, runID,
				ev.GetEventTime().AsTime()) + "\n")
			roleName := string(sr.role)
			if roleName == "" {
				roleName = "none"
			}
			b.WriteString(cotRoundBody(src, &live, otel, ledgerTaskID(workflowID), roleName, sr.activityID, cot))
			sections = append(sections, b.String())
		}
	}
	return sections, nil
}

// cotHeader renders a section's header line: the round ordinal, the role,
// the pinned agent, the run-id attribution, and the stamp. Shared by the
// completed and the degraded (failed/cancelled) section builders, so the
// shapes cannot drift.
//
// Run-id attribution, the task-log blocks' own convention (an 8-char
// prefix): a continued session's identically-headed retry sections
// attribute to the execution that produced them. The visibility-lag
// fallback walks the latest run without an id — there is nothing to stamp,
// and the section says so by omission.
func cotHeader(n int, role, agent, runID string, at time.Time) string {
	header := fmt.Sprintf("=== round %d: %s", n, role)
	if agent != "" {
		header += fmt.Sprintf(" (%s)", agent)
	}
	if runID != "" {
		short := runID
		if len(short) > 8 {
			short = short[:8]
		}
		header += " (run " + short + ")"
	}
	// The stamp is when this round's material became final — a completed
	// event's time (the CoT became final) or a terminal event's (the round
	// died).
	return header + fmt.Sprintf(" @ %s ===", at.Format("2006-01-02 15:04:05 MST"))
}

// singleLine flattens s onto one line — a failure message with newlines
// inside a section would carry header-shaped material past every parser —
// without truncating it: the no-truncation rule holds for the terminal
// face the way it holds for a section's body.
func singleLine(s string) string {
	return strings.TrimSpace(strings.NewReplacer("\r", " ", "\n", " ", "\t", " ").Replace(s))
}

// otelUsage is one round's folded otel ledger counters — the usage stamp
// -cot renders when the relay never saw the round's wire traffic (otel
// carries no content, only these counts and the attribution).
type otelUsage struct {
	in, out, cacheRead, cacheWrite int64
}

func roundKey(task, role, round string) string {
	return task + "\x00" + role + "\x00" + round
}

// cotOtelUsage folds the daemon ledger's otel entries into per-round usage
// stamps for cotRoundBody's precedence. live is false when root is empty
// or the daemon is unreachable — the caller renders every round from the
// unstamped Temporal walk, never an error.
func cotOtelUsage(root string) (map[string]otelUsage, bool) {
	if root == "" {
		return nil, false
	}
	resp, err := metricsHTTP.Get(root + "/ledger?page=1&page_size=1000000")
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var page struct {
		Entries []metricsrelay.Entry `json:"entries"`
	}
	if json.NewDecoder(resp.Body).Decode(&page) != nil {
		return nil, false
	}
	otel := map[string]otelUsage{}
	for _, e := range page.Entries {
		if e.Source != metricsrelay.SourceOtel || e.Task == "" {
			continue
		}
		k := roundKey(e.Task, e.Role, e.Round)
		u := otel[k]
		u.in += e.InputTokens
		u.out += e.OutputTokens
		u.cacheRead += e.CacheReadTokens
		u.cacheWrite += e.CacheWriteTokens
		otel[k] = u
	}
	return otel, true
}

// fetchCotCapture fetches one round's relay-captured content from the
// daemon's /capture endpoint. found is false for a capture the daemon
// does not hold (404 — a round from before metrics was enabled, content
// aged out of the ring, an otel-staged round) and for any other
// answered-but-not-served status, which degrades this round to the next
// source rather than failing the dump; up is false only when the daemon
// is unreachable, which flips the whole dump's liveness off.
func fetchCotCapture(root, task, role, round string) (cr metricsrelay.CapturedRound, found, up bool) {
	resp, err := metricsHTTP.Get(root + "/capture?task=" + url.QueryEscape(task) +
		"&role=" + url.QueryEscape(role) + "&round=" + url.QueryEscape(round))
	if err != nil {
		return cr, false, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return cr, false, true
	}
	if json.NewDecoder(resp.Body).Decode(&cr) != nil {
		return cr, false, true
	}
	return cr, true, true
}

// cotRoundBody renders one finished round's body (completed, or dead by
// failure, cancellation, or timeout) from the source precedence: the
// relay capture (the round's real prompt and response, stamped relay),
// else the otel ledger's usage stamp over the Temporal walk's CoT
// (stamped otel — otel carries the counts, Temporal the text),
// else the Temporal walk itself (stamped temporal). The unstamped shape —
// !*live, or a daemon that died mid-dump, which flips *live off — is the
// pre-capture body byte-for-byte, so a daemon-less dump renders exactly
// today's walk.
func cotRoundBody(src string, live *bool, otel map[string]otelUsage, task, role, round, temporal string) string {
	unstamped := func() string {
		if temporal == "" {
			return "no CoT captured for this round\n"
		}
		return temporal + "\n"
	}
	if !*live {
		return unstamped()
	}
	cr, found, up := fetchCotCapture(src, task, role, round)
	if !up {
		*live = false
		return unstamped()
	}
	if found {
		var b strings.Builder
		b.WriteString("source: relay\n")
		if cr.Prompt != "" {
			b.WriteString("--- prompt ---\n" + cr.Prompt + "\n")
		}
		if cr.Response != "" {
			b.WriteString("--- response ---\n" + cr.Response + "\n")
		}
		return b.String()
	}
	if u, ok := otel[roundKey(task, role, round)]; ok {
		b := fmt.Sprintf("source: otel\nusage: in %d, out %d, cache read %d/write %d\n",
			u.in, u.out, u.cacheRead, u.cacheWrite)
		// The otel stamp is metadata, not a replacement: a completed
		// round's Temporal result still carries its real CoT (a dead
		// round walked no result, so only the counts render), and otel
		// staging must never downgrade the diagnostic surface to four
		// numbers.
		if temporal != "" {
			b += temporal + "\n"
		}
		return b
	}
	if temporal == "" {
		return "source: temporal\nno CoT captured for this round\n"
	}
	return "source: temporal\n" + temporal + "\n"
}

// lastRoundExit parses the newest "jailed … round exited" header timestamp
// from a task log — the runs-feed Last-CoT column's anchor, the moment a
// round's log material last landed. Zero when the log carries no exited
// round (nothing started, or a round in flight with no completed one).
func lastRoundExit(log string) time.Time {
	var newest time.Time
	for line := range strings.SplitSeq(log, "\n") {
		header, ok := strings.CutPrefix(line, "=== ")
		if !ok {
			continue
		}
		header, ok = strings.CutSuffix(header, " ===")
		if !ok {
			continue
		}
		stamp, rest, ok := strings.Cut(header, " ")
		if !ok {
			continue
		}
		event, _, _ := strings.Cut(rest, " (run ")
		if !strings.HasPrefix(event, "jailed ") || !strings.Contains(event, " round exited ") {
			continue
		}
		t, err := time.Parse(time.RFC3339, stamp)
		if err == nil && t.After(newest) {
			newest = t
		}
	}
	return newest
}

// lastRoundStart parses the newest "jailed … round started" header
// timestamp from a task log — the in-flight round's age anchor for the
// restart alert (dash-hardening Q6). Zero when the log carries no started
// round.
func lastRoundStart(log string) time.Time {
	var newest time.Time
	for line := range strings.SplitSeq(log, "\n") {
		header, ok := strings.CutPrefix(line, "=== ")
		if !ok {
			continue
		}
		header, ok = strings.CutSuffix(header, " ===")
		if !ok {
			continue
		}
		stamp, rest, ok := strings.Cut(header, " ")
		if !ok {
			continue
		}
		event, _, _ := strings.Cut(rest, " (run ")
		if !strings.HasPrefix(event, "jailed ") || !strings.Contains(event, " round started: ") {
			continue
		}
		t, err := time.Parse(time.RFC3339, stamp)
		if err == nil && t.After(newest) {
			newest = t
		}
	}
	return newest
}

// claudeTranscriptCoT renders a completed claude round's CoT from its
// host-side session transcript: <claude-projects>/<worktree-slug>/<session
// id>.jsonl. Empty when the round ran with no worktree or session id on
// record, or the transcript is unreadable — the caller's no-CoT note
// stands, never a fabricated section.
func claudeTranscriptCoT(worktree, sessionID string) string {
	if worktree == "" || sessionID == "" {
		return ""
	}
	dir, err := claudeProjectsDir(worktree)
	if err != nil {
		return ""
	}
	cot, found := transcriptCoT(filepath.Join(dir, sessionID+".jsonl"), "claude")
	if !found {
		return ""
	}
	return cot
}

// cotTailState renders -cot's run-state clause from the workflow's live
// Temporal status — the same empty-run-id Describe the dependency gate and
// wakeup use, which for a continued session means the latest execution,
// exactly what "is it still going" asks. Terminal statuses end the clause
// with "this dump is final", so a finished run's -cot output never reads
// as live; a describe failure degrades the clause rather than failing a
// dump that already succeeded.
func cotTailState(c client.Client, workflowID string) string {
	resp, err := c.DescribeWorkflowExecution(context.Background(), workflowID, "")
	if err != nil {
		return fmt.Sprintf("temporal state unknown (describe failed: %v)", err)
	}
	s := resp.GetWorkflowExecutionInfo().GetStatus()
	switch s {
	case enums.WORKFLOW_EXECUTION_STATUS_RUNNING, enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return fmt.Sprintf("temporal state %s: run still going — this dump is not final", s)
	case enums.WORKFLOW_EXECUTION_STATUS_PAUSED:
		// Paused is not terminal — an unpause resumes the run and more
		// rounds arrive — so it must never read final. daedalus itself
		// never pauses a workflow; only a manual temporal-CLI pause lands
		// here (see backlog/bugs/log-tail-paused-state-final-clause.md).
		return fmt.Sprintf("temporal state %s: run paused — this dump is not final; the run can resume", s)
	default:
		return fmt.Sprintf("temporal state %s: run not running — this dump is final", s)
	}
}

// logTail renders the tail block appended to every log dump (plain and
// -cot): one more === === line in the task log's own header style, naming
// when the last log material arrived and the run's state. The arrival
// stamp is the task log file's mtime — the writer appends each block in a
// single O_APPEND write, so mtime is the last arrival — and degrades to
// "unknown" when the file is absent or unreadable: -cot works on hosts the
// worker never touched, and a missing file must not fail a dump that
// already rendered. A read/stat race with a concurrent block write shifts
// the stamp by at most one block — this is an observability aid, not a
// transaction. The line never enters the log file itself, so the file
// parsers (lastRoundState and friends) never see it; even if one did, a
// "log tail" event matches no state-bearing grammar.
func logTail(workflowID, statePhrase string) string {
	last := "unknown (task log not readable on this host)"
	if path, err := activities.TaskLogPath(workflowID); err == nil {
		if info, serr := os.Stat(path); serr == nil {
			last = info.ModTime().UTC().Format(time.RFC3339)
		}
	}
	return fmt.Sprintf("=== log tail: last log %s — workflow %s — %s ===\n", last, workflowID, statePhrase)
}

// listWorkflowRuns returns the run ids of every execution recorded for
// workflowID, oldest first — a restarted session's earlier runs included,
// since its task log spans them. A visibility failure returns nil (the
// caller falls back to the latest run alone). An id that could not ride
// safely inside the visibility query never gets there: the guard below
// exits with a diagnostic first.
func listWorkflowRuns(c client.Client, workflowID string) []string {
	if strings.ContainsAny(workflowID, "'\\") {
		exitf("log -cot wants a plain workflow id, got %q", workflowID)
	}
	resp, err := c.ListWorkflow(context.Background(), &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: "default",
		// ponytail: first page only — the 100 most recent executions of the
		// id; a session restarted past that is beyond the view's scope.
		PageSize: 100,
		Query:    fmt.Sprintf("WorkflowId = '%s'", workflowID),
	})
	if err != nil {
		return nil
	}
	type runStart struct {
		id    string
		start time.Time
	}
	runs := make([]runStart, 0, len(resp.GetExecutions()))
	for _, e := range resp.GetExecutions() {
		runs = append(runs, runStart{e.GetExecution().GetRunId(), e.GetStartTime().AsTime()})
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].start.Before(runs[j].start) })
	ids := make([]string, len(runs))
	for i, r := range runs {
		ids[i] = r.id
	}
	return ids
}

// inlineCoT extracts chain-of-thought fenced in <think>...</think> tags
// from a round's visible text — the one inline-CoT shape that can be told
// apart reliably at render time. openai-wire models that reason inline
// without tags (lite-omlx among them) cannot be split from their answer,
// so their rounds render as no CoT rather than a guess; the tagless split
// is a parked maintainer decision. An unclosed fence still holds the
// reasoning — a round cut off mid-thought shows what it thought.
func inlineCoT(text string) string {
	open := strings.Index(text, "<think>")
	if open < 0 {
		return ""
	}
	rest := text[open+len("<think>"):]
	if end := strings.Index(rest, "</think>"); end >= 0 {
		rest = rest[:end]
	}
	return strings.TrimSpace(rest)
}

// liveCotSectionWith renders the round in flight as a live section for the
// -cot view: the agent's reasoning and assistant text, read from its
// host-side transcript as the agent writes it. The section's inputs are
// file-derived — the task log for the round state, the transcript for the
// content — though -cot itself only reaches it while Temporal answers,
// since the command walks history first. Returns ""
// when no round is in flight: between rounds the newest transcript is the
// just-finished round's, and rendering it live would duplicate that
// round's completed section and mislabel it.
//
// The header carries the section's own freshness stamp, so a mid-round
// dump no longer reads as stale: `last write` is the transcript's mtime
// — the liveness signal, what the agent last produced — and `activity`
// the run's LastActivityAt search attribute off the live describe, the
// same decode the runs table uses. touch() fires at round boundaries
// only, so mid-round the activity value reads as round-start: it answers
// "when did the workflow last change state", not "is anything moving
// right now" — the two labels keep the halves apart. A describe failure,
// an old run that never backfilled the attribute, or a nil client
// degrades the activity half to "unknown", never a fabricated stamp and
// never a failed dump. The early returns before a transcript is in hand
// (codex's note, the no-host-transcript note, no transcript yet) have no
// mtime to show and keep the bare header.
func liveCotSectionWith(workflowID string, c client.Client) string {
	path, err := activities.TaskLogPath(workflowID)
	if err != nil {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "" // no log, no round in flight
	}
	name, stage, worktree, running := lastRoundState(string(data))
	if !running {
		return ""
	}
	if stage == "" {
		stage = "unknown"
	}
	var b strings.Builder
	bare := fmt.Sprintf("=== in-flight round (stage=%s, live) ===\n", stage)
	if name == "codex" {
		// codex keeps a rollout transcript the live view does not follow
		// yet — say so rather than reuse the "keeps no host transcript"
		// note below, which is false for codex.
		b.WriteString(bare)
		fmt.Fprintf(&b, "%s keeps a host transcript the live view does not read; CoT appears here when the round completes\n", name)
		return b.String() + "\n"
	}
	if name != "claude" && name != "pi" {
		// aider, opencode, and amp keep no host transcript the live view
		// follows — opencode's session database exists, the live view just
		// does not read it; never silence, never a guess.
		b.WriteString(bare)
		fmt.Fprintf(&b, "%s keeps no host transcript; CoT appears here when the round completes\n", name)
		return b.String() + "\n"
	}
	tpath, _, tmod, ok, err := newestTranscript(worktree)
	if err != nil || !ok {
		b.WriteString(bare)
		b.WriteString("no transcript yet — no assistant output has landed\n")
		return b.String() + "\n"
	}
	activity := "unknown"
	if c != nil {
		if resp, derr := c.DescribeWorkflowExecution(context.Background(), workflowID, ""); derr == nil {
			// Same decode the runs table uses; the describe is the
			// empty-run-id latest-execution one cotTailState already makes.
			if vis := decodeRunVisibility(resp.GetWorkflowExecutionInfo().GetSearchAttributes(), converter.GetDefaultDataConverter()); !vis.LastAt.IsZero() {
				activity = vis.LastAt.UTC().Format(time.RFC3339)
			}
		}
	}
	fmt.Fprintf(&b, "=== in-flight round (stage=%s, live, last write %s|activity %s) ===\n",
		stage, tmod.UTC().Format(time.RFC3339), activity)
	// ponytail: the transcript is picked by newest-wins freshness alone —
	// the newest source is the live writer's except in the window before
	// the in-flight round creates its own transcript, where the
	// just-finished round's file (or another agent's just-finished
	// session) can briefly read as live. Per-round session attribution
	// is a maintainer decision, out of scope here.
	cot, found := transcriptCoT(tpath, name)
	if !found {
		b.WriteString("no assistant output in the transcript yet\n")
		return b.String() + "\n"
	}
	b.WriteString(cot)
	return b.String() + "\n"
}

// transcriptCoT renders the live chain of thought from one transcript
// file: the assistant's thinking and text blocks, interleaved in
// transcript order, never truncated — the same material a completed
// round's captured Thinking carries, plus the visible answer. Tool calls
// and results are omitted (the CoT-only focus the completed sections
// already have). Unparsable lines are skipped — a trailing partial line is
// an entry the live writer is still mid-way through, the same tolerance
// digestLastEntry exercises. found is false when no assistant output has
// landed in the file yet.
func transcriptCoT(path, agent string) (cot string, found bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	var pieces []string
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var e struct {
			Type    string `json:"type"`
			Message struct {
				Role    string `json:"role"`
				Content []struct {
					Type     string `json:"type"`
					Thinking string `json:"thinking"`
					Text     string `json:"text"`
				} `json:"content"`
			} `json:"message"`
		}
		if json.Unmarshal([]byte(line), &e) != nil {
			continue
		}
		switch agent {
		case "claude":
			// claude's transcript entries are typed at the top level;
			// only assistant entries carry reasoning or the answer.
			if e.Type != "assistant" {
				continue
			}
		case "pi":
			// pi's session entries carry the role inside the message (per
			// pi.dev's session-format/message-types docs; pi is not
			// probe-verified on any host daedalus runs on) — the
			// assistant's are the reasoning.
			if e.Message.Role != "assistant" {
				continue
			}
		}
		for _, block := range e.Message.Content {
			switch block.Type {
			case "thinking":
				if block.Thinking != "" {
					pieces = append(pieces, block.Thinking)
				}
			case "text":
				if block.Text != "" {
					pieces = append(pieces, block.Text)
				}
			}
		}
	}
	if len(pieces) == 0 {
		return "", false
	}
	return strings.Join(pieces, "\n\n") + "\n", true
}

// taskStatusBrief prints the maintainer-facing brief derived from the task
// log alone: which agent round is running right now, when its transcript
// was last touched, and a one-line digest of the transcript's latest entry
// — the "looks stuck" check, computed from files so it works while the
// worker or Temporal are down.
func taskStatusBrief(log string) {
	agent, worktree := runningRound(log)
	fmt.Printf("current agent: %s\n", agent)
	if worktree == "" {
		// An idle run with no worktree has either never started or is a
		// dependent held in pending by its dependency gate — the submit
		// note is the only line its log carries, and the only "why" the
		// brief can give.
		if line, ok := lastSubmitNote(log); ok {
			fmt.Println(line)
		}
		return
	}
	path, agent, mtime, ok, err := newestTranscript(worktree)
	if err != nil {
		fmt.Println("last update: unknown")
		return
	}
	if !ok {
		fmt.Println("last update: no transcripts yet")
		return
	}
	if agent == "opencode" {
		// opencode keeps no transcript files: freshness is its session
		// database's updated stamp, and the brief has no body to digest.
		fmt.Printf("last update: %s\n", mtime.UTC().Format(time.RFC3339))
		fmt.Println("last update text: opencode session database — the brief reads no message bodies")
		return
	}
	digest := "<unreadable transcript>"
	if data, rerr := os.ReadFile(path); rerr == nil {
		digest = digestLastEntry(string(data))
	}
	fmt.Printf("last update: %s\n", mtime.UTC().Format(time.RFC3339))
	fmt.Printf("last update text: %s\n", digest)
}

// lastSubmitNote returns the run's submit-time dependency note, if any —
// the one line a dependent's task log carries while the gate holds it in
// pending, before any round exists. Plain-prefix match: the line is
// writer-produced (AppendSubmitNote), and the brief only displays it.
func lastSubmitNote(log string) (string, bool) {
	var found string
	for line := range strings.SplitSeq(log, "\n") {
		if strings.HasPrefix(line, "waiting on dependency: ") {
			found = line
		}
	}
	return found, found != ""
}

// runningRound derives the current agent from the task log: the last
// "round started" block with no matching "round exited" block is the round
// in flight, and its stage (the round's session role) is the label. The
// worktree path comes from the same block — the transcripts live under it.
// ponytail: no workflow-terminal block is ever written (workflow-level
// events are out of the task log's scope), so the "done" state is
// unreachable — a finished run's log reads "idle".
func runningRound(log string) (agent, worktree string) {
	_, stage, worktree, running := lastRoundState(log)
	if !running {
		return "idle", worktree
	}
	if stage == "" {
		stage = "unknown"
	}
	return stage, worktree
}

// lastRoundState is runningRound's walk over the task log, keeping more of
// the last round block: the jailed agent's name (claude, pi, aider, …),
// the stage label, the worktree path, and whether that round is still in
// flight. Same grammar discipline as before: only the writer's own
// "jailed … round started/exited" headers count.
func lastRoundState(log string) (name, stage, worktree string, running bool) {
	for line := range strings.SplitSeq(log, "\n") {
		if !strings.HasPrefix(line, "=== ") {
			continue
		}
		header := strings.TrimSuffix(strings.TrimPrefix(line, "=== "), " ===")
		_, rest, _ := strings.Cut(header, " ") // first field is the timestamp
		event, _, _ := strings.Cut(rest, " (run ")
		// Only the writer's own round grammar counts. Anything else a
		// header line can carry — notably the raw test command embedded in
		// test-suite events — must never flip the derived state (see
		// backlog/bugs/tasklog-test-command-round-marker-corruption.md).
		const started = " round started: "
		if strings.HasPrefix(event, "jailed ") {
			if i := strings.Index(event, started); i >= 0 {
				name = strings.TrimSpace(event[len("jailed "):i])
				st, fields, _ := strings.Cut(event[i+len(started):], " ")
				stage = strings.TrimPrefix(st, "stage=")
				worktree = strings.TrimPrefix(fields, "worktree=")
				if j := strings.LastIndex(worktree, " pgid="); j >= 0 {
					worktree = worktree[:j]
				}
				running = true
				continue
			}
			if strings.Contains(event, " round exited ") {
				running = false
			}
		}
	}
	return name, stage, worktree, running
}

// newestTranscript returns the newest transcript written for a worktree,
// naming the agent that wrote it: claude's project dir, pi's session dir,
// and codex's rollout tree (the cwd recorded in each rollout file
// attributes it) are consulted newest-wins — no source is ever cleaned, so
// a previous run's stale transcripts on one side must not shadow another
// agent's live session (and the newest file is the live writer's either
// way). opencode keeps no files: its session database's newest updated
// stamp wins the same way, and the path comes back empty — there is no
// body to digest. ok is false when no source holds a transcript. err —
// returned alone, without a path — means claude's dir could not even be
// located, which --status degrades to its "last update: unknown" line.
func newestTranscript(worktree string) (path, agent string, mtime time.Time, ok bool, err error) {
	cdir, err := claudeProjectsDir(worktree)
	if err != nil {
		return "", "", time.Time{}, false, err
	}
	path, mtime, ok = newestTranscriptFile(cdir)
	agent = "claude"
	if pdir, err := piSessionsDir(worktree); err == nil {
		if p, t, pok := newestTranscriptFile(pdir); pok && (path == "" || t.After(mtime)) {
			path, agent, mtime, ok = p, "pi", t, true
		}
	}
	if p, t, cok := codexNewestRollout(worktree); cok && (path == "" || t.After(mtime)) {
		path, agent, mtime, ok = p, "codex", t, true
	}
	if t, ook := opencodeNewestUpdate(worktree); ook && (path == "" || t.After(mtime)) {
		path, agent, mtime, ok = "", "opencode", t, true
	}
	return path, agent, mtime, ok, nil
}

// newestTranscriptFile returns the path and mtime of the newest *.jsonl
// transcript in dir; ok is false when the dir holds no transcripts.
func newestTranscriptFile(dir string) (path string, mtime time.Time, ok bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", time.Time{}, false
	}
	var newest string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if newest == "" || info.ModTime().After(mtime) {
			newest, mtime = name, info.ModTime()
		}
	}
	if newest == "" {
		return "", time.Time{}, false
	}
	return filepath.Join(dir, newest), mtime, true
}

// lastTranscriptUpdate stats the newest *.jsonl transcript in dir and
// digests its last parseable entry. ok is false when the dir holds no
// transcripts. ponytail: production callers moved to newestTranscript —
// this stays because its tests pin it; the test round may fold it away.
func lastTranscriptUpdate(dir string) (mtime time.Time, digest string, ok bool) {
	path, mtime, ok := newestTranscriptFile(dir)
	if !ok {
		return time.Time{}, "", false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return mtime, "<unreadable transcript>", true
	}
	return mtime, digestLastEntry(string(data)), true
}

// digestLastEntry digests the transcript's last parseable json line;
// trailing partial lines (an entry mid-write) are skipped. A file with no
// parseable line at all renders as <unreadable transcript>.
func digestLastEntry(data string) string {
	lines := strings.Split(data, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		if d, ok := digestEntry(line); ok {
			return d
		}
	}
	return "<unreadable transcript>"
}

// digestEntry renders one transcript entry as a one-line digest: the entry
// type plus the tool name for tool_use content (e.g. "assistant Bash"), or
// the first ~80 chars of text content. codex's rollout lines carry
// everything one level down in a payload envelope, and are digested by the
// same rule there: role-attributed message text when there is any, the
// tool name for a function call, else the bare item type. A shape it does
// not recognize falls back to the bare entry type.
func digestEntry(line string) (string, bool) {
	var e struct {
		Type    string `json:"type"`
		Message struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
		Payload struct {
			Type    string `json:"type"`
			Role    string `json:"role"`
			Name    string `json:"name"`
			Message string `json:"message"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"payload"`
	}
	if err := json.Unmarshal([]byte(line), &e); err != nil {
		return "", false
	}
	// claude's and pi's entries have no payload; only codex rollout lines
	// land in this branch.
	if p := e.Payload; p.Type != "" {
		switch {
		case p.Message != "":
			return briefSnippet(p.Type, p.Message), true
		case p.Type == "message":
			for _, it := range p.Content {
				if it.Text != "" {
					return briefSnippet(p.Role, it.Text), true
				}
			}
		case p.Name != "":
			return p.Type + " " + p.Name, true
		}
		return p.Type, true
	}
	if len(e.Message.Content) == 0 {
		return e.Type, true
	}
	if e.Message.Content[0] == '[' {
		var items []struct {
			Type string `json:"type"`
			Name string `json:"name"`
			Text string `json:"text"`
		}
		if json.Unmarshal(e.Message.Content, &items) != nil {
			return "", false
		}
		for _, it := range items {
			if it.Type == "tool_use" {
				return strings.TrimSpace(e.Type + " " + it.Name), true
			}
		}
		for _, it := range items {
			if it.Text != "" {
				return briefSnippet(e.Type, it.Text), true
			}
		}
		return e.Type, true
	}
	var text string
	if json.Unmarshal(e.Message.Content, &text) == nil && text != "" {
		return briefSnippet(e.Type, text), true
	}
	return e.Type, true
}

// briefSnippet prefixes s's first ~80 characters with the entry type.
// Transcript text routinely carries newlines; the digest is one line by
// contract, so whitespace collapses before truncation — overflow lines
// would otherwise masquerade as further brief fields (see
// backlog/bugs/tasklog-multiline-digest.md).
func briefSnippet(typ, s string) string {
	s = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ").Replace(s)
	s = strings.TrimSpace(s)
	if runes := []rune(s); len(runes) > 80 {
		s = string(runes[:80]) + "…"
	}
	if typ == "" {
		return s
	}
	return typ + " " + s
}
