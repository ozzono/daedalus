package activities

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
)

// Jailed rounds chain into one conversation per role per workflow run: the
// workflow passes each round's SessionID into the next (see
// AgentRunInput.Role and SessionID). The id reaches the workflow only
// inside a round's *completed* result, so a round killed at its ceiling
// used to strand its session — the retry started a fresh conversation and
// the killed round's accumulated work was lost. Session tracking closes
// that gap: the round's conversation id is persisted to a small state file
// while the round still runs, and a retry whose workflow-provided id is
// empty resumes the recorded conversation (recordedAgentSession). The id's
// source is the transcript file the CLI itself writes — claude's
// `<uuid>.jsonl` under the worktree's project dir, pi's
// `<timestamp>_<uuid>.jsonl` under the worktree's session dir, each created
// the moment the session starts and independent of the process afterwards —
// detected as "new since the snapshot taken at spawn" (transcript
// tracking). Nothing the round
// printed is trusted: claude's json output emits a single result object at
// completion, so its stdout carries the id only on success, and command
// output the jailed agent controls must not decide anything (see the
// ErrAgentKilled wrap note in runJailedRound). The record is keyed to the
// worktree path, the workflow run id, and the role, so it can never resume
// into another tree, another run (sessions do not cross runs), or the
// wrong one of the run's four conversations.

// SessionRole names which of a run's chained conversations a jailed round
// belongs to. The dev agent, the test agent, and the two reviewers run
// four separate conversations in the same worktree (see FeatureDevWorkflow),
// so one role's id must never resume another's. The workflow stamps the
// role into the activity input; an empty role (one-shot rounds like
// test-command discovery, which never chain) skips session tracking.
type SessionRole string

const (
	// RoleDev scopes the implementation, fix, and rebuild rounds.
	RoleDev SessionRole = "dev"
	// RoleTest scopes the tests and test-fix rounds.
	RoleTest SessionRole = "test"
	// RoleDevReview scopes the code reviews.
	RoleDevReview SessionRole = "dev-review"
	// RoleTestReview scopes the test reviews.
	RoleTestReview SessionRole = "test-review"
)

// sessionState is the persisted record: one file per worktree, holding the
// workflow run's conversation ids per role. Best-effort state — a lost or
// corrupt file only costs a fresh start, never a failed round.
type sessionState struct {
	Worktree   string `json:"worktree"`
	RunID      string `json:"run_id"`
	Dev        string `json:"dev,omitempty"`
	Test       string `json:"test,omitempty"`
	DevReview  string `json:"dev_review,omitempty"`
	TestReview string `json:"test_review,omitempty"`
}

func (s *sessionState) role(role SessionRole) string {
	switch role {
	case RoleTest:
		return s.Test
	case RoleDevReview:
		return s.DevReview
	case RoleTestReview:
		return s.TestReview
	default:
		return s.Dev
	}
}

func (s *sessionState) set(role SessionRole, id string) {
	switch role {
	case RoleTest:
		s.Test = id
	case RoleDevReview:
		s.DevReview = id
	case RoleTestReview:
		s.TestReview = id
	default:
		s.Dev = id
	}
}

// pathSlug encodes a path as a file-name-safe token, matching claude's own
// transcript-directory encoding: every character outside [A-Za-z0-9]
// becomes '-' (e.g. /home/u/.repo → -home-u--repo).
func pathSlug(p string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '-'
	}, p)
}

// sessionStatePath keys the record file by the worktree path (pathSlug,
// like claude's transcript dirs) under ~/.daedalus, next to the worktrees
// the activities already manage.
func sessionStatePath(worktree string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".daedalus", "sessions", pathSlug(worktree)+".json"), nil
}

// claudeProjectDir returns claude's per-project transcript directory for a
// working directory (the pathSlug of the path under ~/.claude/projects).
// Transcripts are flat <session-id>.jsonl files, one per conversation.
func claudeProjectDir(worktree string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".claude", "projects", pathSlug(worktree)), nil
}

// ClaudeProjectDir is claudeProjectDir's exported form, for the CLI's
// task-status brief (`daedalus log <id> --status`), which locates an
// agent's transcripts from the worktree path recorded in the task log.
func ClaudeProjectDir(worktree string) (string, error) {
	return claudeProjectDir(worktree)
}

// PiSessionsDir is piSessionsDir's exported form, for the CLI's
// task-status brief (`daedalus log <id> --status`), which consults both
// claude's and pi's transcript dirs and reports whichever transcript is
// newest — neither dir is ever cleaned, so a previous run's stale
// transcripts must not shadow the other agent's live session.
func PiSessionsDir(worktree string) (string, error) {
	return piSessionsDir(worktree)
}

// SessionStatePath is sessionStatePath's exported form, for the CLI's wipe
// command: the per-worktree session record file it must erase alongside the
// worktree it tracks.
func SessionStatePath(worktree string) (string, error) {
	return sessionStatePath(worktree)
}

// piSessionsDir returns pi's per-working-directory session directory for a
// worktree. Sessions auto-save as JSONL files under
// ~/.pi/agent/sessions/--<path>--/, where <path> is pi's own encoding of
// the working directory — leading separator stripped, then '/', '\', and
// ':' each replaced with '-', wrapped in leading/trailing dashes
// (pi.dev/docs/latest/session-format) — and each file is named
// <timestamp>_<session-id>.jsonl. The host sees these because the jail
// bridges the home: ai-jail's pi preset mounts ~/.pi read-write
// (probe-verified; see jailedAgentCLI), like claude's ~/.claude.
func piSessionsDir(worktree string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	enc := strings.NewReplacer("/", "-", "\\", "-", ":", "-").Replace(strings.TrimPrefix(worktree, "/"))
	return filepath.Join(home, ".pi", "agent", "sessions", "--"+enc+"--"), nil
}

// piTranscriptExists reports whether pi holds a session file for session id
// in this worktree's session dir.
func piTranscriptExists(worktree, id string) bool {
	dir, err := piSessionsDir(worktree)
	if err != nil {
		return false
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if pid, ok := piSessionID(e.Name()); ok && pid == id {
			return true
		}
	}
	return false
}

// workflowRunID returns the workflow execution run id of the running
// activity — the run-scoping half of the session record — "" outside a
// real activity context (unit tests invoke activities directly; the SDK
// panics in that case by design).
func workflowRunID(ctx context.Context) (runID string) {
	defer func() { recover() }()
	return activity.GetInfo(ctx).WorkflowExecution.RunID
}

// recordSession stores the conversation id for this worktree, run, and
// role. Best-effort: every failure is swallowed — a lost record costs a
// fresh start on some future retry, never a failed round.
func recordSession(ctx context.Context, worktree string, role SessionRole, id string) {
	path, err := sessionStatePath(worktree)
	if err != nil {
		return
	}
	var st sessionState
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &st)
	}
	// The file name is a slug of the worktree path, which is not injective
	// — the stored path arbitrates, so a collision restarts the record
	// rather than mixing two trees' sessions.
	if st.Worktree != "" && st.Worktree != worktree {
		st = sessionState{}
	}
	st.Worktree = worktree
	st.RunID = workflowRunID(ctx)
	st.set(role, id)
	data, err := json.Marshal(st)
	if err != nil {
		return
	}
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	_ = os.WriteFile(path, data, 0o644)
}

// recordedSession returns the conversation id on record for this worktree,
// workflow run, and role — what a retry whose workflow-provided SessionID
// is empty should resume. A record from another run or worktree never
// resumes (sessions do not cross runs); stale reports that a session WAS
// on record but its transcript is gone (claude prunes old transcripts), so
// the caller can warn and start fresh instead of resuming blindly.
func recordedSession(ctx context.Context, worktree string, role SessionRole) (id string, stale bool) {
	return recordedSessionCheck(ctx, worktree, role, claudeTranscriptExists)
}

// recordedPiSession is recordedSession for pi rounds: same record, but the
// staleness check looks in pi's per-worktree session dir.
func recordedPiSession(ctx context.Context, worktree string, role SessionRole) (id string, stale bool) {
	return recordedSessionCheck(ctx, worktree, role, piTranscriptExists)
}

// recordedAgentSession dispatches recordedSession per agent: only claude
// and pi have transcripts the tracking can address; every caller is gated
// on agentResumeFlag, so anything else never reaches here.
func recordedAgentSession(ctx context.Context, agent, worktree string, role SessionRole) (string, bool) {
	if agent == "pi" {
		return recordedPiSession(ctx, worktree, role)
	}
	return recordedSession(ctx, worktree, role)
}

// recordedSessionCheck is the shared body of recordedSession and
// recordedPiSession; transcriptExists arbitrates staleness against the
// agent's own transcript layout.
func recordedSessionCheck(ctx context.Context, worktree string, role SessionRole, transcriptExists func(worktree, id string) bool) (id string, stale bool) {
	if role == "" {
		return "", false
	}
	path, err := sessionStatePath(worktree)
	if err != nil {
		return "", false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	var st sessionState
	if json.Unmarshal(data, &st) != nil {
		return "", false
	}
	if st.Worktree != worktree || st.RunID != workflowRunID(ctx) {
		return "", false
	}
	id = st.role(role)
	if id == "" {
		return "", false
	}
	if !transcriptExists(worktree, id) {
		return "", true
	}
	return id, false
}

// claudeTranscriptExists reports whether claude holds a transcript
// <id>.jsonl for this worktree.
func claudeTranscriptExists(worktree, id string) bool {
	dir, err := claudeProjectDir(worktree)
	if err != nil {
		return false
	}
	_, err = os.Stat(filepath.Join(dir, id+".jsonl"))
	return err == nil
}

// transcriptScanInterval is how often a tracked round polls for the
// transcript its session creates. The file appears at session start, well
// within any ceiling; the interval only sets how soon the record lands
// after it. A var (not a const) so tests can shorten it.
var transcriptScanInterval = 2 * time.Second

// transcriptIDs lists the session ids claude holds transcripts for in this
// worktree's project dir. ok is false when the dir cannot be read for a
// reason other than not existing yet — detection then stays off for the
// round rather than risk mistaking an old session for this round's.
func transcriptIDs(worktree string) (ids map[string]bool, ok bool) {
	dir, err := claudeProjectDir(worktree)
	if err != nil {
		return nil, false
	}
	return transcriptIDsIn(dir, claudeSessionID)
}

// piTranscriptIDs is transcriptIDs for pi: the session ids pi holds session
// files for in the worktree's session dir.
func piTranscriptIDs(worktree string) (ids map[string]bool, ok bool) {
	dir, err := piSessionsDir(worktree)
	if err != nil {
		return nil, false
	}
	return transcriptIDsIn(dir, piSessionID)
}

// transcriptIDsIn lists session ids for every transcript file in dir that
// idFromName accepts. ok is false when the dir cannot be read for a reason
// other than not existing yet.
func transcriptIDsIn(dir string, idFromName func(string) (string, bool)) (map[string]bool, bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// A first round in a fresh worktree: no transcripts yet, which
			// is a valid empty snapshot — the CLI creates the dir itself.
			return map[string]bool{}, true
		}
		return nil, false
	}
	ids := make(map[string]bool, len(entries))
	for _, e := range entries {
		if id, ok := idFromName(e.Name()); ok {
			ids[id] = true
		}
	}
	return ids, true
}

// claudeSessionID extracts claude's session id from a transcript file name
// (flat <session-id>.jsonl).
func claudeSessionID(name string) (string, bool) {
	if !strings.HasSuffix(name, ".jsonl") {
		return "", false
	}
	return strings.TrimSuffix(name, ".jsonl"), true
}

// piSessionID extracts pi's session id from a session file name
// (<timestamp>_<session-id>.jsonl); the id is the last underscore-separated
// field.
func piSessionID(name string) (string, bool) {
	if !strings.HasSuffix(name, ".jsonl") {
		return "", false
	}
	base := strings.TrimSuffix(name, ".jsonl")
	i := strings.LastIndex(base, "_")
	if i < 0 {
		return "", false
	}
	return base[i+1:], true
}

// recordNewTranscript records the conversation id of a transcript created
// after the round's snapshot — the round's own session — and reports
// whether one was found.
func recordNewTranscript(ctx context.Context, known map[string]bool, worktree string, role SessionRole) bool {
	dir, err := claudeProjectDir(worktree)
	if err != nil {
		return false
	}
	return recordNewTranscriptIn(ctx, dir, claudeSessionID, known, worktree, role)
}

// recordNewPiTranscript is recordNewTranscript for pi: new session files in
// pi's per-worktree session dir.
func recordNewPiTranscript(ctx context.Context, known map[string]bool, worktree string, role SessionRole) bool {
	dir, err := piSessionsDir(worktree)
	if err != nil {
		return false
	}
	return recordNewTranscriptIn(ctx, dir, piSessionID, known, worktree, role)
}

// recordNewTranscriptIn is the shared body of recordNewTranscript and
// recordNewPiTranscript.
func recordNewTranscriptIn(ctx context.Context, dir string, idFromName func(string) (string, bool), known map[string]bool, worktree string, role SessionRole) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		id, ok := idFromName(e.Name())
		if !ok || known[id] {
			continue
		}
		recordSession(ctx, worktree, role, id)
		activityLogger(ctx).Info("Conversation transcript recorded for retry resume",
			"Role", string(role), "SessionID", id)
		return true
	}
	return false
}

// watchTranscripts polls until the round's transcript appears, the round
// ends (stop closes), or the activity context ends.
func watchTranscripts(ctx context.Context, known map[string]bool, worktree string, role SessionRole, stop <-chan struct{}) {
	watchTranscriptsIn(ctx, known, worktree, role, stop, recordNewTranscript)
}

// watchPiTranscripts is watchTranscripts for pi: it polls pi's per-worktree
// session dir.
func watchPiTranscripts(ctx context.Context, known map[string]bool, worktree string, role SessionRole, stop <-chan struct{}) {
	watchTranscriptsIn(ctx, known, worktree, role, stop, recordNewPiTranscript)
}

// watchTranscriptsIn is the shared body of watchTranscripts and
// watchPiTranscripts.
func watchTranscriptsIn(ctx context.Context, known map[string]bool, worktree string, role SessionRole, stop <-chan struct{}, recordNew func(context.Context, map[string]bool, string, SessionRole) bool) {
	ticker := time.NewTicker(transcriptScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if recordNew(ctx, known, worktree, role) {
				return
			}
		}
	}
}

// brokenResume reports whether err is consistent with the resumed
// conversation itself being dead — the agent launched and exited with an
// error of its own (a missing, pruned, or corrupt session chief among
// them), worth one fresh-start retry. Everything else keeps the round as
// it is: a killed round — including the activity-ceiling kill — resumes
// fine; an exhausted round never really ran; and a slots-busy, cancelled,
// or never-spawned round says nothing about the conversation. The
// classification rides the in-package sentinels (errors.Is), never output
// text — and every excluded class wraps its sentinel with %w across
// runJailedRound's error paths.
func brokenResume(err error) bool {
	return err != nil &&
		!errors.Is(err, ErrAgentKilled) &&
		!errors.Is(err, ErrAPIExhausted) &&
		!errors.Is(err, ErrAgentSlotsBusy) &&
		!errors.Is(err, errAgentStart) &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded)
}
