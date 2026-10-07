// Package activities implements the Temporal activities that make up a
// Daedalus pipeline: worktree lifecycle, jailed agent and reviewer runs, and
// tests. The domain logic behind the adapters lives in its own files:
// worktree.go (worktree lifecycle), jailed.go (jailed-agent process
// execution), agentstream.go (agent-stream parsing), and tests.go (native
// test suites).
package activities

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ozzono/daedalus/internal/config"
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
	// this worktree (claude -p --resume, pi --session, opencode run -s,
	// codex exec resume) instead of starting
	// cold — the round inherits the prior context and its warm provider
	// prompt cache. Empty starts a fresh session; agents without an
	// id-addressable session (amp, aider) ignore it.
	SessionID string
	// Role scopes the recorded-session fallback to one of the run's four
	// chained conversations (RoleDev, RoleTest, RoleDevReview,
	// RoleTestReview): when SessionID is empty — a first round, or a retry
	// of one killed at its ceiling — the round resumes the conversation the
	// session watcher recorded for this role, if any. Empty role (one-shot
	// callers) skips the fallback.
	Role SessionRole
	// Folders are the run's granted host folders (PipelineInput.Folders,
	// resolved to absolute paths at submit): each is mounted read-write into
	// this round's jail under <worktree>/.daedalus-folders/<basename>
	// (FolderMounts), and a fresh conversation's prompt is told where each
	// landed. Empty — no grants, or a run whose input predates the field —
	// mounts nothing, replay-safe like every other input field. Reviewer
	// rounds carry no grants by design: grants serve the working agent's
	// task (bookkeeping), not the review.
	Folders []string
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
	// (claude -p --resume, pi --session, opencode run -s, codex exec
	// resume) so a re-review verifies its
	// earlier findings with the prior context and warm prompt cache
	// instead of starting cold. Each reviewer role (code review, test
	// review) chains its own session; empty starts a fresh one. Agents
	// without an id-addressable session (amp, aider) ignore it.
	SessionID string
	// Role scopes the recorded-session fallback to one of the run's four
	// chained conversations (RoleDev, RoleTest, RoleDevReview,
	// RoleTestReview), exactly like AgentRunInput.Role: when SessionID is
	// empty, the round resumes the conversation the session watcher
	// recorded for this role, if any. Empty role skips the fallback.
	Role SessionRole
	// TaskTouchesJail marks a task whose own text names .ai-jail
	// (submit-time derivation — template.TaskTouchesJail on the run's
	// prompt, never the diff). Only the phase-1 implementation code
	// review passes it true: the reviewer prompt then audits .ai-jail
	// (with the spec content relayed) instead of blanket-ignoring it.
	TaskTouchesJail bool
	// AcceptanceCriteria, when set, marks the slim flow's atomic
	// sub-task review: the reviewer prompt frames the diff as one
	// sub-task of a larger plan and judges it against exactly these
	// criteria instead of the whole-task lens. Empty keeps the ordinary
	// whole-change review prompt.
	AcceptanceCriteria []string
	// FreshReview marks the slim flow's context isolation: the reviewer
	// starts a brand-new conversation every round — no session resume and
	// no recorded-session fallback, ever. Everything else about the role
	// is unchanged: the reviewer endpoint (reviewerEnv), the pi
	// edit-guardrail exclusion, and session tracking all key on Role and
	// behave exactly like a flagship review's.
	FreshReview bool
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
	// NoVerdict marks a marker-less exit: the reviewer's output carried no
	// verdict line at all. The pipeline still treats such a round as
	// CHANGES_REQUESTED (the standing contract for a malformed-but-real
	// review), but the workflow also counts it against a strike budget —
	// three in a row park the run, because a reviewer that never emits any
	// verdict is failing infrastructure (e.g. pi folding every provider
	// request at its request timeout), not requesting changes, and the
	// equivalence otherwise converts the failure into an unbounded loop.
	NoVerdict bool
	// SessionID identifies the reviewer conversation the round ran in, so
	// the next round of the same review role can resume it (see
	// ReviewInput.SessionID). Empty when the agent reports none.
	SessionID string
	// Usage is the round's raw provider metrics (see Usage).
	Usage Usage
}

// RunJailedClaudeActivity runs the jailed agent (Claude Code, opencode,
// amp, pi, codex, or aider, per config) inside an ai-jail sandbox rooted at the
// worktree. Claude runs with json output so its visible text comes back
// structured and lands complete in the activity result (serialized into
// Temporal history, visible in the UI per round); amp's --stream-json-thinking emits
// Claude-Code-compatible events including thinking blocks, so the same
// parser applies; pi's --mode json emits pi's own event schema, read by
// its own parser branch (thinking, session id, and usage all captured);
// opencode's, codex's, and aider's plain output is taken as-is (no thinking,
// session, or usage). stream-json (which also carries the chain of
// thought via --verbose) is blocked for claude for now: ai-jail's flag
// guard rejects --verbose after the command by prefix match, even
// behind --.
func RunJailedClaudeActivity(ctx context.Context, input AgentRunInput) (AgentRunResult, error) {
	// Post-round bug mirror (bug_filing.mirror): on every exit path — a
	// round cut off at its ceiling may already have filed files, and its
	// worktree dies with the run's cleanup all the same. With the mirror
	// bind-mounted into the round's jail this is a no-op (the writes
	// already landed host-side; see mirrorToHost's SameFile guard); it
	// stays for the unmounted fallback shape.
	defer mirrorToHost(ctx, input.WorktreePath,
		os.Getenv(config.BugDirEnv), os.Getenv(config.BugMirrorEnv))
	agent, _, agentArgs := jailedAgentCLI(input.Agent)
	// A session id from a previous round resumes that conversation instead
	// of starting cold — the round inherits the prior context and the
	// provider's warm prompt cache for it. claude (--resume), pi
	// (--session), opencode (-s), and codex (exec resume) support resume;
	// amp and aider have no id-addressable session and ignore the field.
	//
	// With no workflow-provided id — a first round, or the retry after a
	// round cut off at its ceiling, which never produced a result carrying
	// one — the round resumes the conversation recorded from the killed
	// attempt's transcript, so the retry continues its progress instead of
	// restarting from zero.
	_, canResume := agentResumeFlag(agent)
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
	// pi session hygiene, gated to pi so every other agent's resume path is
	// byte-identical: a transcript that already embeds tool-call JSON is
	// the self-reinforcing degenerate pattern a small model imitates, so
	// the session is not resumed at all; a transcript ending in a
	// failed-edit loop keeps its session but gets the next round steered
	// (piEditSteer rides the round's prompt as the newest user message).
	steer := false
	if agent == "pi" && resume != "" {
		health := scanPiSession(input.WorktreePath, resume)
		if health.embeddedToolJSON {
			activityLogger(ctx).Warn("pi session transcript embeds tool-call JSON; starting a fresh session",
				"SessionID", resume)
			resume = ""
		} else if health.editLoop {
			activityLogger(ctx).Warn("pi session ends in a failed-edit loop; steering the next round",
				"SessionID", resume)
			steer = true
		}
	}
	runArgs := agentArgs
	if resume != "" && canResume {
		runArgs = agentResumeArgs(agent, resume, agentArgs)
	}
	prompt := input.Prompt
	// Granted host folders: a fresh conversation — any round that will not
	// resume one — is told where the mounts landed; a resumed round skips
	// it, the note already lives in that conversation. Rendered from the
	// same FolderMounts call the argv builder (runJailedRound) makes, so
	// the relayed paths can never drift from the mounted ones. The
	// broken-resume fresh retry below starts a new conversation too (it
	// drops the resume flag), so it appends the note before its second
	// launch.
	foldersNote := ""
	if len(input.Folders) > 0 {
		mounts, err := FolderMounts(input.WorktreePath, input.Folders)
		if err != nil {
			return AgentRunResult{}, err
		}
		foldersNote = folderMountNote(mounts)
		if resume == "" || !canResume {
			prompt += foldersNote
		}
	}
	if steer {
		prompt = piEditSteer + "\n\n" + prompt
	}
	start := time.Now()
	res, err := runJailedKindFolders(ctx, input.Role, input.Agent, input.WorktreePath, prompt, input.Folders, runArgs...)
	if err != nil && resume != "" && canResume && brokenResume(err) {
		// The resumed session died anyway — a missing, pruned, or corrupt
		// transcript the pre-flight check missed, or a conversation claude
		// itself rejected. Give the round one fresh start instead of
		// failing it. The record is deliberately left alone: if the fresh
		// round runs at all, its own transcript overwrites the dead id; if
		// it never gets that far, a stale record only costs another
		// classification pass, never a wrong resume of a live session.
		activityLogger(ctx).Warn("Resumed agent session is broken; retrying the round with a fresh session", "SessionID", resume, "Error", err)
		// The retry launches fresh (no resume flag), so it needs the mounts
		// note the resumed first launch skipped.
		res, err = runJailedKindFolders(ctx, input.Role, input.Agent, input.WorktreePath, prompt+foldersNote, input.Folders, agentArgs...)
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
	// Post-round bug mirror: same shape and reasoning as the implementing
	// rounds — the reviewer's prompt carries the bug policy too. A no-op
	// under the jail mount (see mirrorToHost's SameFile guard).
	defer mirrorToHost(ctx, input.WorktreePath,
		os.Getenv(config.BugDirEnv), os.Getenv(config.BugMirrorEnv))
	diff, handoff, err := stagedDiff(ctx, input.WorktreePath)
	if err != nil {
		return ReviewResult{}, err
	}
	prompt, err := func() (string, error) {
		// The configured out-of-scope-bug filing folder, exported at worker
		// startup iff bug_filing is enabled; empty drops the file-filing
		// instruction from the reviewer's bug policy.
		bugDir := os.Getenv(config.BugDirEnv)
		// A jail-touching task's audit target never appears in the git
		// diff — the repo ignores .ai-jail and the jail drops it untracked
		// — so relay the worktree's spec content as its own labeled
		// section. An emptied spec is a deliverable shape too ("clear the
		// permissions"): relay the emptiness explicitly instead of
		// omitting the section and leaving the audit blind.
		jailSpec := ""
		if input.TaskTouchesJail {
			if b, rerr := os.ReadFile(filepath.Join(input.WorktreePath, ".ai-jail")); rerr == nil {
				jailSpec = string(b)
				if jailSpec == "" {
					jailSpec = "(the spec file is empty)"
				}
			}
		}
		// The jail carve-out always rides along; the diff file handoff
		// joins it only when the diff was too large to embed.
		extras := []template.ReviewExtra{
			template.Jail{Touches: input.TaskTouchesJail, Spec: jailSpec},
		}
		if handoff != nil {
			extras = append(extras, handoff)
		}
		if input.ReproInScope {
			return template.ReviewRepro(input.Focus, diff, input.TestLogs, input.AgentReply, bugDir, extras...)
		}
		if len(input.AcceptanceCriteria) > 0 {
			return template.SlimReview(input.Focus, diff, input.TestLogs, bugDir, input.AcceptanceCriteria, extras...)
		}
		return template.Review(input.Focus, diff, input.TestLogs, input.TestsInScope, input.AgentReply, bugDir, extras...)
	}()
	if err != nil {
		return ReviewResult{}, err
	}
	agent, _, agentArgs := jailedAgentCLI(input.Agent)
	// A session id from a previous round of the same review role resumes
	// that conversation — the re-review verifies its earlier findings with
	// the prior context instead of re-deriving them cold. claude
	// (--resume), pi (--session), opencode (-s), and codex (exec resume)
	// support resume; amp and aider start fresh. As with the agent rounds,
	// a retry with no workflow-provided id resumes the conversation
	// recorded from a cut-off attempt's transcript — the exact incident
	// this replaces: a long review killed at its ceiling used to restart
	// from zero and never deliver a verdict.
	_, canResume := agentResumeFlag(agent)
	resume := input.SessionID
	if resume == "" && canResume && !input.FreshReview {
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
	// pi session hygiene for reviewer rounds, same gating and reasoning as
	// the agent rounds above: a transcript that embeds tool-call JSON is
	// not resumed. The failed-edit steering does not apply — reviewers do
	// not edit.
	if agent == "pi" && resume != "" &&
		scanPiSession(input.WorktreePath, resume).embeddedToolJSON {
		activityLogger(ctx).Warn("pi session transcript embeds tool-call JSON; starting a fresh session",
			"SessionID", resume)
		resume = ""
	}
	runArgs := agentArgs
	if resume != "" && canResume {
		runArgs = agentResumeArgs(agent, resume, agentArgs)
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
		// Plain-text CLI (opencode, codex, aider) or a parse miss: the whole
		// stdout is the review.
		text = res.Stdout
	}
	verdict := parseReviewVerdict(text)
	verdict.SessionID = session
	verdict.Usage = usage
	return verdict, nil
}

// mirrorToHost copies each file directly under <worktreePath>/<relDir>
// into the host mirror directory (bug_filing.mirror / test_output.mirror,
// exported at worker startup; either side empty means off — a no-op):
// copy-on-first-see preserving the file name, an existing same-name mirror
// file overwritten only when the worktree copy is newer (files are
// written and edited across rounds). Best-effort and logged: a mirror
// failure never fails the round or changes the verdict. The branch copy
// stays the designed flow — the mirror is a host-side reflection that
// survives worktree cleanup, not a relocation. Subdirectories are skipped:
// bug files and dumps are flat files by convention. A missing source dir
// (nothing filed yet) is silent; anything else is a logged warning. When
// the source dir IS the mirror — the bug dir bind-mounted into the round's
// jail (runJailedRound's --rw-map), writes already landed host-side — the
// copy is a silent no-op: SameFile catches the bind mount, whose two host
// paths differ but share dev+inode, where a copy would read and truncate
// the very file it mirrors. This keeps the copy alive as the fallback for
// an environment where the mount is absent.
func mirrorToHost(ctx context.Context, worktreePath, relDir, mirrorDir string) {
	if relDir == "" || mirrorDir == "" {
		return
	}
	logger := activityLogger(ctx)
	srcDir := filepath.Join(worktreePath, relDir)
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			logger.Warn("Mirror source dir unreadable", "Dir", srcDir, "Error", err)
		}
		return
	}
	if srcInfo, err := os.Stat(srcDir); err == nil {
		if dstInfo, err := os.Stat(mirrorDir); err == nil && os.SameFile(srcInfo, dstInfo) {
			return
		}
	}
	if err := os.MkdirAll(mirrorDir, 0o755); err != nil {
		logger.Warn("Mirror dir could not be created", "Dir", mirrorDir, "Error", err)
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		src := filepath.Join(srcDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			logger.Warn("Mirror source stat failed", "Path", src, "Error", err)
			continue
		}
		if dstInfo, err := os.Stat(filepath.Join(mirrorDir, entry.Name())); err == nil &&
			!info.ModTime().After(dstInfo.ModTime()) {
			continue
		}
		dst := filepath.Join(mirrorDir, entry.Name())
		if err := copyFile(src, dst, info.Mode()); err != nil {
			logger.Warn("Mirror copy failed", "From", src, "To", dst, "Error", err)
		}
	}
}

// copyFile streams src into dst with src's mode, replacing any existing
// file — the newer-wins decision belongs to mirrorToHost.
func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// reviewDiffDir is the reviewer's worktree scratch dir, holding the diff
// handoff file when a review diff is too large to embed (stagedDiff);
// excludeAgentArtifacts keeps it out of git's sight.
const reviewDiffDir = ".daedalus-review"

// diffHandoffPath is the handoff file's worktree-relative path — the path
// both the reviewer prompt and the stale-file removal address (jailed
// rounds run with the worktree as their directory).
const diffHandoffPath = reviewDiffDir + "/diff.patch"

// reviewDiffBudget is the byte ceiling under which a review diff still
// embeds inline in the reviewer prompt: half the serving model's context
// window at a conservative 3 bytes per token, the other half reserved for
// the rest of the prompt, the reviewer's tool traffic, and its verdict.
// The window is the worker's anthropic.context_tokens (exported as
// ContextTokensEnv for the jailed rounds); 200000 is the default window of
// the serving models — the one whose overflow killed the run this handoff
// replaces. An operator on a smaller model sets the knob and the budget
// follows.
func reviewDiffBudget() int {
	tokens := 200000
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(config.ContextTokensEnv))); err == nil && v > 0 {
		tokens = v
	}
	return tokens / 2 * 3
}

// stagedDiff returns the reviewer's view of the worktree's diff against
// HEAD, including new files, without staging the work itself: intent-to-add
// (-N) makes new files visible to `git diff` while modifications stay
// unstaged, so the next agent round sees normal `git diff`/`git status`
// output (one removal caveat — see the ALERT below). A diff at or under
// reviewDiffBudget returns
// inline — the prompt shape is byte-identical to the original. Above the
// budget the same complete diff goes to <worktree>/.daedalus-review/
// diff.patch — never cropped — and the return carries the prompt's file
// handoff instead: the digest, the per-file table of contents, and the
// line counts the prompt builds its reading instructions on. The file
// lives in git's blind spot (info/exclude), so no later round's diff or
// status, and no preserved branch, ever sees it.
//
// ALERT (recorded under backlog/bugs/): `add -N -A` has no intent-to-add
// form for a removal — it stages deletions and renames fully, so they
// leave the worktree-vs-index diff and the reviewer never sees them. The
// handoff file carries the same `git diff`, so the gap predates and
// outlives this mechanism; the fix (a `git diff HEAD` shape, or
// unstaging after collection) changes the diff contract every review
// prompt and pin rides on, so it stays its own round.
func stagedDiff(ctx context.Context, worktreePath string) (string, *template.DiffHandoff, error) {
	if _, err := runGit(ctx, "-C", worktreePath, "add", "-N", "-A"); err != nil {
		return "", nil, fmt.Errorf("stage intent-to-add: %w", err)
	}
	out, err := runGit(ctx, "-C", worktreePath, "diff")
	if err != nil {
		return "", nil, fmt.Errorf("collect diff: %w", err)
	}
	if len(out) <= reviewDiffBudget() {
		// A handoff file left by a previous oversized round is stale the
		// moment this diff embeds inline: remove it (best-effort — the
		// exclusion line keeps git blind either way) so no round reads a
		// superseded diff, and drop the now-empty scratch dir with it
		// (a failure means the dir holds something else — leave it).
		if err := os.Remove(filepath.Join(worktreePath, diffHandoffPath)); err != nil && !os.IsNotExist(err) {
			activityLogger(ctx).Warn("Stale review diff handoff could not be removed",
				"Path", diffHandoffPath, "Error", err)
		}
		_ = os.Remove(filepath.Join(worktreePath, reviewDiffDir))
		return out, nil, nil
	}
	h, err := writeDiffHandoff(ctx, worktreePath, out)
	if err != nil {
		return "", nil, err
	}
	return "", h, nil
}

// writeDiffHandoff routes an oversized review diff to the worktree: the
// complete, uncropped diff is written to .daedalus-review/diff.patch (the
// dir excluded from git via excludeAgentArtifacts) and the prompt's
// handoff is gathered — the compact-summary digest, and the per-file table
// of contents read off the very text being written.
func writeDiffHandoff(ctx context.Context, worktreePath, diff string) (*template.DiffHandoff, error) {
	if err := excludeAgentArtifacts(worktreePath, reviewDiffDir); err != nil {
		return nil, fmt.Errorf("exclude review diff dir: %w", err)
	}
	if err := os.WriteFile(filepath.Join(worktreePath, diffHandoffPath), []byte(diff), 0o644); err != nil {
		return nil, fmt.Errorf("write review diff handoff: %w", err)
	}
	stat, err := runGit(ctx, "-C", worktreePath, "diff", "--compact-summary")
	if err != nil {
		return nil, fmt.Errorf("diff digest: %w", err)
	}
	return &template.DiffHandoff{
		Path:  diffHandoffPath,
		Lines: diffLineCount(diff),
		Stat:  stat,
		Files: diffSections(diff),
	}, nil
}

// diffLineCount counts the diff's lines the way a slice read addresses
// them: one per newline, plus a final unterminated line when the diff
// lacks the trailing newline.
func diffLineCount(diff string) int {
	n := strings.Count(diff, "\n")
	if len(diff) > 0 && !strings.HasSuffix(diff, "\n") {
		n++
	}
	return n
}

// diffSections maps every file's section of the diff text to one table-of-
// contents entry, precomputed in the prompt's final form ("lines a-b:
// path" — the 1-based range a slice read, sed -n 'a,bp', addresses): each
// `diff --git` header opens a section that runs to the next header, or to
// the last line. An entry's path prefers the section's `+++ b/` line — the
// exact path git means even where the header is ambiguous (spaces, quoted
// metachars) — falling back to the header's b-side tail, which still names
// the sections that carry no +++ line (deletions, binary files); git
// appends a tab after a space-carrying path on the +++ line, so it is
// trimmed. The +++ line is honored only inside a section's header block
// (before its first @@ hunk): a hunk body can carry a literal `+++ b/…`
// line — a diff that adds the line `++ b/x` — and that must not retitle
// the section.
// ponytail: a header whose b-path itself embeds " b/", or that git had to
// C-quote, yields a cosmetic path only — the load-bearing half of an entry
// is its line range, which is always exact.
func diffSections(diff string) []string {
	var lines []string
	path := ""
	first := 0
	header := false
	n := 0
	emit := func(last int) {
		lines = append(lines, fmt.Sprintf("lines %d-%d: %s", first, last, path))
	}
	for line := range strings.SplitSeq(diff, "\n") {
		n++
		switch {
		case strings.HasPrefix(line, "diff --git "):
			if first > 0 {
				emit(n - 1)
			}
			first = n
			header = true
			path = diffHeaderPath(line)
		case first > 0 && strings.HasPrefix(line, "@@ "):
			header = false
		case first > 0 && header && strings.HasPrefix(line, "+++ b/"):
			path = strings.TrimRight(strings.TrimPrefix(line, "+++ b/"), "\t")
		}
	}
	if first > 0 {
		emit(diffLineCount(diff))
	}
	return lines
}

// diffHeaderPath extracts the b-side path from a `diff --git a/x b/y`
// header line — the fallback for sections without a `+++ b/` line. A
// header git had to quote, or whose path embeds " b/", yields a
// best-effort string; see diffSections's ponytail note.
func diffHeaderPath(line string) string {
	tail := strings.TrimPrefix(line, "diff --git ")
	if i := strings.LastIndex(tail, " b/"); i >= 0 {
		return tail[i+3:]
	}
	return tail
}
