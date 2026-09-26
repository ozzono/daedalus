package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
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
)

// claudeProjectsDir returns claude's transcript directory for a worktree.
// A package var so tests can point it at a fake tree.
var claudeProjectsDir = activities.ClaudeProjectDir

// piSessionsDir returns pi's per-worktree session directory for a worktree.
// A package var so tests can point it at a fake tree.
var piSessionsDir = activities.PiSessionsDir

// runTaskLog implements `daedalus log <workflow-id>`: print the task's
// captured log file, or — with --status — a short status brief instead.
// File-derived only: no temporal connection, no config.
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
}

// runTaskLogCot implements `daedalus log <workflow-id> -cot`: print the
// run's chain-of-thought logs instead of the task
// log — one complete section per jailed round (implementation and review),
// never truncated. Rounds with a native thinking channel (claude, amp, pi)
// show their captured Thinking; openai-wire rounds whose reasoning arrived
// inline show the <think>-fenced part; rounds with no CoT at all say so
// rather than rendering an empty section. The Temporal address comes from
// the resolved config when one is found — an unloadable one fails like
// every other client command — else the default, so the command works
// wherever the Temporal service is reachable. When a round is in flight
// right now, a live section rendered from the agent's host-side transcript
// follows the completed ones (see liveCotSection).
func runTaskLogCot(workflowID string) {
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

	// Walk each run's history like the usage report does: pair every jailed
	// round's completed result with its scheduled input, which carries the
	// round's role and (when the run pinned one) its agent.
	dc := converter.GetDefaultDataConverter()
	type scheduledRound struct {
		typ   string
		role  activities.SessionRole
		agent string
	}
	rounds := 0
	for _, runID := range runs {
		scheduled := map[int64]scheduledRound{}
		iter := c.GetWorkflowHistory(context.Background(), workflowID, runID,
			false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			ev, err := iter.Next()
			if err != nil {
				exitf("read history of %s: %v", workflowID, err)
			}
			if s := ev.GetActivityTaskScheduledEventAttributes(); s != nil {
				typ := s.GetActivityType().GetName()
				if !agentActivityTypes[typ] {
					continue
				}
				sr := scheduledRound{typ: typ}
				if ps := s.GetInput().GetPayloads(); len(ps) > 0 {
					// Same shape-vs-type dispatch as the result decode below:
					// each activity type carries its own input struct.
					if typ == "RunJailedClaudeActivity" {
						var in activities.AgentRunInput
						if dc.FromPayload(ps[0], &in) == nil {
							sr.role, sr.agent = in.Role, in.Agent
						}
					} else {
						var in activities.ReviewInput
						if dc.FromPayload(ps[0], &in) == nil {
							sr.role, sr.agent = in.Role, in.Agent
						}
					}
				}
				scheduled[ev.GetEventId()] = sr
				continue
			}
			a := ev.GetActivityTaskCompletedEventAttributes()
			if a == nil {
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
			case "RunJailedReviewerActivity":
				var r activities.ReviewResult
				if dc.FromPayload(ps[0], &r) != nil {
					continue
				}
				// The reviewer activity discards the parsed thinking at
				// capture (see backlog/bugs/reviewer-thinking-discarded.md),
				// so only inline, fenced CoT can surface for review rounds.
				cot = inlineCoT(r.Comments)
			default:
				continue
			}
			rounds++
			header := fmt.Sprintf("=== round %d: %s", rounds, sr.role)
			if sr.agent != "" {
				header += fmt.Sprintf(" (%s)", sr.agent)
			}
			fmt.Println(header + " ===")
			if cot == "" {
				fmt.Println("no CoT captured for this round")
			} else {
				fmt.Println(cot)
			}
			fmt.Println()
		}
	}
	// The in-flight section replaces, never accompanies, the no-rounds
	// message: a round running with zero completed rounds is exactly the
	// case the live view exists for.
	live := liveCotSection(workflowID)
	if live != "" {
		fmt.Print(live)
	}
	if rounds == 0 && live == "" {
		exitf("no jailed agent rounds in %s's Temporal history — the run has not started any rounds yet, or predates structured round capture", workflowID)
	}
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

// liveCotSection renders the round in flight as a live section for the
// -cot view: the agent's reasoning and assistant text, read from its
// host-side transcript as the agent writes it. The section's inputs are
// file-derived — the task log for the round state, the transcript for the
// content — though -cot itself only reaches it while Temporal answers,
// since the command walks history first. Returns ""
// when no round is in flight: between rounds the newest transcript is the
// just-finished round's, and rendering it live would duplicate that
// round's completed section and mislabel it.
func liveCotSection(workflowID string) string {
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
	fmt.Fprintf(&b, "=== in-flight round (stage=%s, live) ===\n", stage)
	if name != "claude" && name != "pi" {
		// aider, opencode, and amp keep no host transcript; never silence,
		// never a guess.
		fmt.Fprintf(&b, "%s keeps no host transcript; CoT appears here when the round completes\n", name)
		return b.String() + "\n"
	}
	tpath, _, _, ok, err := newestTranscript(worktree)
	if err != nil || !ok {
		b.WriteString("no transcript yet — no assistant output has landed\n")
		return b.String() + "\n"
	}
	// ponytail: the transcript is picked by file freshness alone — the
	// newest file is the live writer's except in the window before the
	// in-flight round creates its own transcript, where the just-finished
	// round's file can briefly read as live. Per-round session attribution
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
		return
	}
	path, _, mtime, ok, err := newestTranscript(worktree)
	if err != nil {
		fmt.Println("last update: unknown")
		return
	}
	if !ok {
		fmt.Println("last update: no transcripts yet")
		return
	}
	digest := "<unreadable transcript>"
	if data, rerr := os.ReadFile(path); rerr == nil {
		digest = digestLastEntry(string(data))
	}
	fmt.Printf("last update: %s\n", mtime.UTC().Format(time.RFC3339))
	fmt.Printf("last update text: %s\n", digest)
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

// newestTranscript returns the newest transcript file written for a
// worktree, naming the agent that wrote it: claude's project dir and pi's
// session dir are consulted newest-wins — neither dir is ever cleaned, so
// a previous run's stale transcripts on one side must not shadow the other
// agent's live session (and the newest file is the live writer's either
// way). ok is false when neither dir holds a transcript. err — returned
// alone, without a path — means claude's dir could not even be located,
// which --status degrades to its "last update: unknown" line.
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
// the first ~80 chars of text content. A shape it does not recognize falls
// back to the bare entry type.
func digestEntry(line string) (string, bool) {
	var e struct {
		Type    string `json:"type"`
		Message struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(line), &e); err != nil {
		return "", false
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
