// Package activities implements the Temporal activities that make up a
// Daedalus pipeline: worktree lifecycle, jailed agent and reviewer runs, and
// tests. The domain logic behind the adapters lives in its own files:
// worktree.go (worktree lifecycle), jailed.go (jailed-agent process
// execution), agentstream.go (agent-stream parsing), and tests.go (native
// test suites).
package activities

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ozzono/daedalus/internal/template"
)

// Provider settings (API keys, base URLs, models) reach the jailed agent
// through the worker's environment, exported from config.yaml at startup
// when set there — never through workflow history or activity inputs (both
// would persist them in Temporal events). Nothing here requires them: an
// unset setting simply falls back to whatever the worker inherited.

// AgentRunInput describes a single jailed agent invocation.
type AgentRunInput struct {
	WorktreePath string
	Prompt       string
	// Agent, set by `run -cli/--cli`, overrides the worker's agent for this
	// run; empty falls back to DAEDALUS_AGENT (see jailedAgentCLI).
	Agent string
	// SessionID, when set, resumes the agent's previous conversation for
	// this worktree (claude -p --resume, pi --session) instead of starting
	// cold — the round inherits the prior context and its warm provider
	// prompt cache. Empty starts a fresh session; agents without a
	// session-id mechanism (opencode, amp, aider) ignore it.
	SessionID string
	// Role scopes the recorded-session fallback to one of the run's four
	// chained conversations (RoleDev, RoleTest, RoleDevReview,
	// RoleTestReview): when SessionID is empty — a first round, or a retry
	// of one killed at its ceiling — the round resumes the conversation the
	// session watcher recorded for this role, if any. Empty role (one-shot
	// callers) skips the fallback.
	Role SessionRole
}

// AgentRunResult carries the agent's visible text and chain of thought into
// Temporal history, complete — no truncation anywhere in the app; the UI and
// `temporal workflow show` expose both per round without reading worker logs.
type AgentRunResult struct {
	Text     string
	Thinking string
	// SessionID identifies the agent conversation the round ran in, so the
	// next round can resume it (see AgentRunInput.SessionID). Empty when the
	// agent does not report one.
	SessionID string
	// Usage is the round's raw provider metrics (see Usage).
	Usage Usage
}

// ReviewInput describes a review request for the current state of the
// worktree. Focus says what is being reviewed (e.g. "the implementation",
// "the test suite"); TestLogs optionally carries the latest test output for
// the reviewer to consider.
type ReviewInput struct {
	WorktreePath string
	Focus        string
	TestLogs     string
	// TestsInScope tells the reviewer prompt whether tests are part of
	// this review: false in phase 1 (code review — coverage is a later
	// phase's concern), true in phase 2 (the test-suite review, the only
	// phase carrying the REBUILD verdict).
	TestsInScope bool
	// ReproInScope marks the bug-fix framing: the diff's own tests are
	// part of the deliverable, and the reviewer must judge whether the
	// repro actually captures the reported bug — something the
	// repro-first gate cannot. Takes precedence over TestsInScope; no
	// REBUILD verdict exists in this framing.
	ReproInScope bool
	// AgentReply, when set, quotes the test agent's latest reply for the
	// test reviewer: the agent may report that the fix the work needs is
	// an implementation change its test-only scope forbids — the claim the
	// reviewer weighs (and may act on with a Rebuild verdict) or rejects.
	AgentReply string
	// Agent, set by `run -cli/--cli`, overrides the worker's agent for
	// this run; empty falls back to DAEDALUS_AGENT (see jailedAgentCLI).
	Agent string
	// SessionID, when set, resumes the reviewer's previous conversation
	// (claude -p --resume, pi --session) so a re-review verifies its
	// earlier findings with the prior context and warm prompt cache
	// instead of starting cold. Each reviewer role (code review, test
	// review) chains its own session; empty starts a fresh one. Agents
	// without a session-id mechanism (opencode, amp, aider) ignore it.
	SessionID string
	// Role scopes the recorded-session fallback to one of the run's four
	// chained conversations (RoleDev, RoleTest, RoleDevReview,
	// RoleTestReview), exactly like AgentRunInput.Role: when SessionID is
	// empty, the round resumes the conversation the session watcher
	// recorded for this role, if any. Empty role skips the fallback.
	Role SessionRole
}

// ReviewResult is a reviewer verdict. Comments holds everything the reviewer
// wrote above its verdict line, to be fed back to the implementing agent.
// NeedsMaintainer marks the third verdict, NEEDS_MAINTAINER: the task as
// stated cannot be completed by editing files in this worktree, so the
// workflow parks the run for a maintainer restart instead of requesting
// changes the agent can never satisfy.
type ReviewResult struct {
	Approved        bool
	NeedsMaintainer bool
	// Rebuild marks the fourth verdict, REBUILD (test review only): the
	// change the work needs is an implementation change rather than a test
	// change — outside the test phase's edit scope. The workflow routes the
	// finding back through the implementation ↔ code-review cycle and
	// resumes the test phase once it approves again.
	Rebuild bool
	// Comments holds everything the reviewer wrote above its verdict line,
	// to be fed back to the implementing agent.
	Comments string
	// SessionID identifies the reviewer conversation the round ran in, so
	// the next round of the same review role can resume it (see
	// ReviewInput.SessionID). Empty when the agent reports none.
	SessionID string
	// Usage is the round's raw provider metrics (see Usage).
	Usage Usage
}

// RunJailedClaudeActivity runs the jailed agent (Claude Code, opencode,
// amp, pi, or aider, per config) inside an ai-jail sandbox rooted at the
// worktree. Claude runs with json output so its visible text comes back
// structured and lands complete in the activity result (serialized into
// Temporal history, visible in the UI per round); amp's --stream-json-thinking emits
// Claude-Code-compatible events including thinking blocks, so the same
// parser applies; pi's --mode json emits pi's own event schema, read by
// its own parser branch (thinking, session id, and usage all captured);
// opencode's and aider's plain output is taken as-is (no thinking,
// session, or usage). stream-json (which also carries the chain of
// thought via --verbose) is blocked for claude for now: ai-jail's flag
// guard rejects --verbose after the command by prefix match, even
// behind --.
func RunJailedClaudeActivity(ctx context.Context, input AgentRunInput) (AgentRunResult, error) {
	agent, _, agentArgs := jailedAgentCLI(input.Agent)
	// A session id from a previous round resumes that conversation instead
	// of starting cold — the round inherits the prior context and the
	// provider's warm prompt cache for it. Only claude (--resume) and pi
	// (--session) support resume; opencode, amp, and aider ignore the
	// field and start fresh.
	//
	// With no workflow-provided id — a first round, or the retry after a
	// round cut off at its ceiling, which never produced a result carrying
	// one — the round resumes the conversation recorded from the killed
	// attempt's transcript, so the retry continues its progress instead of
	// restarting from zero.
	resumeFlag, canResume := agentResumeFlag(agent)
	resume := input.SessionID
	if resume == "" && canResume {
		logger := activityLogger(ctx)
		id, stale := recordedAgentSession(ctx, agent, input.WorktreePath, input.Role)
		if stale {
			logger.Warn("Recorded agent session transcript is gone; starting a fresh session",
				"Worktree", input.WorktreePath)
		}
		if id != "" {
			resume = id
			logger.Info("Resuming the previous attempt's recorded agent session", "SessionID", id)
		}
	}
	runArgs := agentArgs
	if resume != "" && canResume {
		runArgs = append([]string{resumeFlag, resume}, agentArgs...)
	}
	start := time.Now()
	res, err := runJailedKind(ctx, input.Role, input.Agent, input.WorktreePath, input.Prompt, runArgs...)
	if err != nil && resume != "" && canResume && brokenResume(err) {
		// The resumed session died anyway — a missing, pruned, or corrupt
		// transcript the pre-flight check missed, or a conversation claude
		// itself rejected. Give the round one fresh start instead of
		// failing it. The record is deliberately left alone: if the fresh
		// round runs at all, its own transcript overwrites the dead id; if
		// it never gets that far, a stale record only costs another
		// classification pass, never a wrong resume of a live session.
		activityLogger(ctx).Warn("Resumed agent session is broken; retrying the round with a fresh session", "SessionID", resume, "Error", err)
		res, err = runJailedKind(ctx, input.Role, input.Agent, input.WorktreePath, input.Prompt, agentArgs...)
	}
	if err != nil {
		return AgentRunResult{}, err
	}
	// Raw figures per the maintainer decision: the CLI's own duration
	// figure when it reported one, the measured wall time otherwise (a CLI
	// whose structured events never arrived, or never reported one), and
	// the worker's name for per-worker aggregation.
	thinking, text, session, usage := parseRoundOutput(agent, res.Stdout)
	if usage.Duration == 0 {
		usage.Duration = time.Since(start).Round(time.Millisecond)
	}
	usage.Worker = os.Getenv("DAEDALUS_WORKER_NAME")
	if text == "" {
		// Not json/stream-json (a CLI without the flag, or a parse miss):
		// keep whatever the agent did print rather than an empty result.
		text = res.Stdout
	}
	logger := activityLogger(ctx)
	logger.Info("Agent run completed", "Stdout", res.Stdout, "Duration", time.Since(start).Round(time.Second))
	if res.Stderr != "" {
		logger.Info("Agent run stderr", "Stderr", res.Stderr)
	}
	return AgentRunResult{
		Text:      text,
		Thinking:  thinking,
		SessionID: session,
		Usage:     usage,
	}, nil
}

// RunJailedReviewerActivity has a jailed reviewer agent review the current
// state of the worktree and return a machine-readable verdict. The diff (and,
// when provided, the latest test output) is collected inside the activity, so
// large payloads stay out of workflow history; only the verdict travels on.
// The reviewer runs with the same structured output mode as the implementing
// agent, so its conversation id comes back for later rounds to resume; the
// verdict is parsed from the agent's visible text, falling back to raw stdout
// for a CLI that printed plain text.
func RunJailedReviewerActivity(ctx context.Context, input ReviewInput) (ReviewResult, error) {
	diff, err := stagedDiff(ctx, input.WorktreePath)
	if err != nil {
		return ReviewResult{}, err
	}
	prompt, err := func() (string, error) {
		if input.ReproInScope {
			return template.ReviewRepro(input.Focus, diff, input.TestLogs, input.AgentReply)
		}
		return template.Review(input.Focus, diff, input.TestLogs, input.TestsInScope, input.AgentReply)
	}()
	if err != nil {
		return ReviewResult{}, err
	}
	agent, _, agentArgs := jailedAgentCLI(input.Agent)
	// A session id from a previous round of the same review role resumes
	// that conversation — the re-review verifies its earlier findings with
	// the prior context instead of re-deriving them cold. Only claude
	// (--resume) and pi (--session) support resume; opencode, amp, and
	// aider start fresh. As with the agent rounds, a retry with no
	// workflow-provided id resumes the conversation recorded from a
	// cut-off attempt's transcript — the exact incident this replaces: a
	// long review killed at its ceiling used to restart from zero and
	// never deliver a verdict.
	resumeFlag, canResume := agentResumeFlag(agent)
	resume := input.SessionID
	if resume == "" && canResume {
		logger := activityLogger(ctx)
		id, stale := recordedAgentSession(ctx, agent, input.WorktreePath, input.Role)
		if stale {
			logger.Warn("Recorded reviewer session transcript is gone; starting a fresh session",
				"Worktree", input.WorktreePath)
		}
		if id != "" {
			resume = id
			logger.Info("Resuming the previous attempt's recorded reviewer session", "SessionID", id)
		}
	}
	runArgs := agentArgs
	if resume != "" && canResume {
		runArgs = append([]string{resumeFlag, resume}, agentArgs...)
	}
	start := time.Now()
	res, err := runJailedKind(ctx, input.Role, input.Agent, input.WorktreePath, prompt, runArgs...)
	if err != nil && resume != "" && canResume && brokenResume(err) {
		// Same broken-resume fallback as the agent rounds: one fresh start
		// instead of failing the review; the record is left for the fresh
		// round's own transcript to overwrite.
		activityLogger(ctx).Warn("Resumed reviewer session is broken; retrying the review with a fresh session", "SessionID", resume, "Error", err)
		res, err = runJailedKind(ctx, input.Role, input.Agent, input.WorktreePath, prompt, agentArgs...)
	}
	if err != nil {
		return ReviewResult{}, fmt.Errorf("review run: %w", err)
	}
	logger := activityLogger(ctx)
	logger.Info("Reviewer run completed", "Duration", time.Since(start).Round(time.Second))
	if res.Stderr != "" {
		logger.Info("Reviewer stderr", "Stderr", res.Stderr)
	}
	_, text, session, usage := parseRoundOutput(agent, res.Stdout)
	if usage.Duration == 0 {
		usage.Duration = time.Since(start).Round(time.Millisecond)
	}
	usage.Worker = os.Getenv("DAEDALUS_WORKER_NAME")
	if text == "" {
		// Plain-text CLI (opencode, aider) or a parse miss: the whole
		// stdout is the review.
		text = res.Stdout
	}
	verdict := parseReviewVerdict(text)
	verdict.SessionID = session
	verdict.Usage = usage
	return verdict, nil
}

// stagedDiff returns the full diff of the worktree against HEAD, including
// new files, without leaving the tree staged: intent-to-add (-N) makes new
// files visible to `git diff` while the index stays effectively untouched,
// so the next agent round sees normal `git diff`/`git status` output.
func stagedDiff(ctx context.Context, worktreePath string) (string, error) {
	if _, err := runGit(ctx, "-C", worktreePath, "add", "-N", "-A"); err != nil {
		return "", fmt.Errorf("stage intent-to-add: %w", err)
	}
	out, err := runGit(ctx, "-C", worktreePath, "diff")
	if err != nil {
		return "", fmt.Errorf("collect diff: %w", err)
	}
	return out, nil
}
