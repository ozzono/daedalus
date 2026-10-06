package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"

	"github.com/ozzono/daedalus/internal/config"
)

// jailResult captures the jailed process's output streams separately: the
// verdict parser must see stdout only, so trailing stderr noise cannot flip
// a verdict, while stderr stays available for error reporting.
type jailResult struct {
	Stdout string
	Stderr string
}

// jailedAgentCLI returns the jailed agent CLI, its headless flags, and the
// output-mode flags for full agent rounds. The selection (config.yaml
// `agent`, default claude) travels via the worker's environment like the
// provider settings: DAEDALUS_AGENT, exported at worker startup. A run
// started with `run -cli/--cli` overrides it per run — agent wins over the
// environment here; empty means no override. Every CLI except aider reads
// the prompt from piped stdin (aider has no stdin mode — runJailedRound
// bridges it through --message-file); each headless flag set approves every
// tool call, which is only safe inside the jail.
func jailedAgentCLI(agent string) (selected string, headless, output []string) {
	if agent == "" {
		agent = os.Getenv("DAEDALUS_AGENT")
	}
	switch agent {
	case "opencode":
		// opencode reads the prompt from piped stdin just like claude -p;
		// --auto approves everything not explicitly denied. Plain text
		// mode, so Thinking stays empty; capturing it means parsing
		// opencode's --format json event stream.
		return "opencode", []string{"run", "--auto"}, nil
	case "amp":
		// -x is amp's execute mode (single-shot, prompt from stdin);
		// --dangerously-allow-all approves all tool calls. It is absent
		// from amp's --help (only the settings key amp.dangerouslyAllowAll
		// is documented), so its disappearance across updates is the first
		// thing to check if amp rounds start failing on unknown flags.
		// --stream-json-thinking implies --stream-json and adds thinking
		// blocks, which parseAgentStream reads like claude's.
		return "amp", []string{"-x", "--dangerously-allow-all"}, []string{"--stream-json-thinking"}
	case "pi":
		// -p is pi's print mode: one prompt (piped stdin is merged into it,
		// matching the transport below), response printed, exit. pi has no
		// permission-approval flag by design — built-in tools run with the
		// process's permissions, which is what the jail is for, and
		// non-interactive modes ignore untrusted project resources, so no
		// project trust is granted inside the worktree. --mode json emits
		// JSON lines in pi's own schema (session header, message_end,
		// usage on message_update) — a parser branch of its own
		// (parsePiStream), unlike amp whose events reuse claude's.
		// Auth rides the provider env vars (ANTHROPIC_API_KEY, ...) the
		// worker already exports — except the openai section, whose env
		// vars pi only half-reads (key yes, base URL no): those rounds are
		// bridged through the host's ~/.pi/agent/models.json per round
		// instead (stagePiProvider in runJailedRound). Probing
		// ai-jail with a fake pi verified that the pi preset bridges ~/.pi
		// into the jail read-write, so pi's host auth.json really does
		// take priority over these env vars for the same provider (pi's
		// documented resolution order) — a stale host /login wins, so
		// keep auth.json clean or aligned. The same mount is what makes
		// the session tracking below work: the host sees
		// ~/.pi/agent/sessions.
		return "pi", []string{"-p"}, []string{"--mode", "json"}
	case "codex":
		// codex exec is the headless entry: it reads the prompt from piped
		// stdin like claude -p (probe-verified banner, codex-cli 0.160.0:
		// "Reading additional input from stdin..."), and
		// --dangerously-bypass-approvals-and-sandbox is codex's own lever
		// for "environments that are externally sandboxed" — which is
		// exactly the jail; codex's own --worktree/-C stay unset, daedalus
		// owns the worktree cwd. Output is plain text, so Thinking stays
		// empty and the raw stdout is the round's text. codex speaks only
		// OpenAI wire formats, so an anthropic-section round has no codex
		// channel at all (pi's asymmetry class — see
		// backlog/bugs/pi-anthropic-fallback-unserveable.md); the openai
		// section is bridged per invocation by stageCodexProvider, and the
		// state dir the jail must bridge for auth and session tracking is
		// resolved by codexJailMounts.
		return "codex", []string{"exec", "--dangerously-bypass-approvals-and-sandbox"}, nil
	case "aider":
		// DEPRECATED, amp-parity: it keeps working exactly as it does
		// today, but it is no longer changed or maintained — no new flags,
		// fixes, or probes. Its known gaps are accepted limitations, not
		// work items: no id-addressable resume (below), openai-only model
		// wiring (the litellm anthropic probe is still pending), and the
		// uv-tools-only install layout.
		// --yes-always approves every confirmation. (--yes is not a real
		// flag: it only works via argparse prefix abbreviation and would
		// break loudly if a second --yes* option ever appears.)
		// --no-auto-commits keeps aider from committing to the worktree
		// itself — auto-commit is aider's default and would hide the
		// round's edits from the reviewer's `git diff` against HEAD.
		// --no-gitignore stops aider from appending .aider* to the repo's
		// .gitignore — its default does that on every startup, and the
		// hunk would land in every review diff and every preserved
		// branch. Its chat/input history files are pointed into
		// .daedalus-aider/ alongside the staged prompt; see
		// excludeAiderArtifacts for how that stays out of git's sight.
		// Output is plain markdown chat (no JSON mode), taken as-is like
		// opencode's, so no thinking, session id, or usage is captured.
		// The prompt cannot ride stdin (aider has none; whether
		// --message-file - reads stdin is undocumented — check before
		// ever wiring it); runJailedRound stages it into --message-file.
		// Auth: aider reads the provider env vars the worker exports AND
		// .env files — its dotenv load overrides already-set process env
		// (load_dotenv override=True), so outside the jail a .env in the
		// target repo would win over the config-derived exports, base
		// URLs and keys alike. Inside jailed rounds that is moot: the
		// jail masks ./.env to empty (probe-verified) and ~/.env is not
		// among the bridged home dirs, so the exports stand. The flag
		// set is verified against the aider installed on this host
		// (0.86.2) — like amp's --dangerously-allow-all, a rename across
		// releases is the first thing to check if aider rounds start
		// failing on unknown flags. Model selection (--model
		// openai/<model>) and the jail mounts for aider's uv install are
		// wired per-round in runJailedRound — both depend on the round's
		// provider env and the host install, not on this fixed flag set.
		return "aider", []string{"--yes-always", "--no-auto-commits", "--no-gitignore",
			"--chat-history-file", ".daedalus-aider/chat.history.md",
			"--input-history-file", ".daedalus-aider/input.history"}, nil
	default:
		return "claude", []string{"-p", "--dangerously-skip-permissions"}, []string{"--output-format", "json"}
	}
}

// aiderJailMounts resolves the host paths a jailed aider round needs mapped
// into the jail and returns them as ai-jail arguments. ai-jail has no aider
// preset, and its generic passthrough blank-slates /home/hugo — where
// aider's whole uv-tools install lives — so an unmounted round dies at
// execve ("Failed to exec aider", wa-termo 2026-09-24). The install is
// resolved from the real launcher, never from hardcoded uv paths: the
// directory carrying the symlink chain (LookPath's hit, typically
// ~/.local/bin), the tool venv root (<venv>/bin/aider two levels up), and —
// only when the launcher's shebang resolves under it — the uv-managed
// interpreter tree next to tools/. Source and destination are identical on
// purpose: the launcher's shebang and the venv's scripts embed absolute
// host paths, so the jail must see the install exactly where the host has
// it. ponytail: only the uv-tools layout is supported (what this host
// uses); anything else fails the round before launch with the offending
// path named rather than launch into a jail known to be missing the
// interpreter. The `--map src:dst` spelling, git availability in the
// generic-passthrough jail (aider wants git for its repo map even with
// --no-auto-commits), and the launcher's real shebang/symlink form on the
// host follow the host probes of 2026-09-24; this sandbox has no ai-jail
// or aider binary to re-probe any of them.
func aiderJailMounts() ([]string, error) {
	bin, err := exec.LookPath("aider")
	if err != nil {
		return nil, fmt.Errorf("resolve aider on PATH: %w", err)
	}
	real, err := filepath.EvalSymlinks(bin)
	if err != nil {
		return nil, fmt.Errorf("resolve aider launcher %s: %w", bin, err)
	}
	// uv-tools layout shape: the resolved launcher is <venv>/bin/aider.
	if filepath.Base(filepath.Dir(real)) != "bin" {
		return nil, fmt.Errorf("aider launcher %s does not match the expected <venv>/bin layout", real)
	}
	venv := filepath.Dir(filepath.Dir(real))
	paths := []string{filepath.Dir(bin), venv}
	// The launcher's #! line names its interpreter.
	data, err := os.ReadFile(real)
	if err != nil {
		return nil, fmt.Errorf("read aider launcher %s: %w", real, err)
	}
	first := strings.SplitN(string(data), "\n", 2)[0]
	if !strings.HasPrefix(first, "#!") {
		return nil, fmt.Errorf("aider launcher %s has no #! line", real)
	}
	fields := strings.Fields(strings.TrimPrefix(first, "#!"))
	if len(fields) == 0 {
		return nil, fmt.Errorf("aider launcher %s has an empty #! line", real)
	}
	// The shebang names the venv's own interpreter, which uv symlinks
	// into the shared tree — the raw shebang text never carries the tree
	// prefix, so resolve the symlink before comparing. An env-style shebang
	// names no concrete interpreter, so it cannot be mapped or validated:
	// reject it rather than silently launch a round whose interpreter we
	// could not account for.
	if filepath.Base(fields[0]) == "env" {
		return nil, fmt.Errorf("aider launcher %s uses an env-style #! — unsupported install layout", real)
	}
	interp, err := filepath.EvalSymlinks(fields[0])
	if err != nil {
		return nil, fmt.Errorf("resolve aider interpreter %s: %w", fields[0], err)
	}
	// Mount the uv interpreter tree only when the resolved interpreter
	// really sits under it — the venv (.../uv/tools/aider-chat) is a
	// sibling of the python tree (.../uv/python/...), not nested with it.
	// An interpreter copied into the venv (no symlink) is already covered
	// by the venv mount, so no tree mount then.
	pyTree := filepath.Join(filepath.Dir(filepath.Dir(venv)), "python")
	if interp == pyTree || strings.HasPrefix(interp, pyTree+string(filepath.Separator)) {
		paths = append(paths, pyTree)
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			return nil, fmt.Errorf("aider jail mount %s unavailable: %w", p, err)
		}
	}
	// A direct install with no symlink chain puts the launcher inside the
	// venv bin dir itself; mapping the same directory twice is at best
	// confusing, so dedupe.
	uniq := paths[:0]
	for _, p := range paths {
		if !slices.Contains(uniq, p) {
			uniq = append(uniq, p)
		}
	}
	paths = uniq
	args := make([]string, 0, 2*len(paths))
	for _, p := range paths {
		args = append(args, "--map", p+":"+p)
	}
	return args, nil
}

// codexJailMounts resolves the codex state dir a jailed codex round needs
// mapped into the jail and returns it as an ai-jail argument. ai-jail's
// agent-state presets bridge ~/.claude and ~/.pi but not ~/.codex — where
// codex keeps both its auth (auth.json) and the session rollouts the
// tracking (codexTranscriptIDs) reads — so an unmounted round can neither
// authenticate nor record a resumable session. The dir is CODEX_HOME when
// the operator set it, ~/.codex otherwise, and it is mapped read-write at
// its host path: codex resolves both from CODEX_HOME/its default, so the
// jail must see the dir exactly where the host has it, and the rollouts
// must land on the host for a retry to resume them. A missing dir is
// created before spawn (a fresh host has none yet; an empty dir is what
// codex's first session would create anyway) so the mount cannot fail on
// it. ponytail: assembled but never probed against a real ai-jail — this
// sandbox has no binary; that the extra --rw-map nests inside the
// --worktree mount and composes with the bug-mirror/folder-grant maps is
// the same verification class as those blocks. Probe on the first live
// host, alongside them.
func codexJailMounts() ([]string, error) {
	dir := os.Getenv("CODEX_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("resolve codex state dir: %w", err)
		}
		dir = filepath.Join(home, ".codex")
	}
	// Same invariant FolderMounts and the bug-mirror block enforce for
	// their --rw-map specs: a colon in the path splits the spec at the
	// wrong boundary.
	if strings.ContainsRune(dir, ':') {
		return nil, fmt.Errorf("codex state dir %s must not contain %q — it composes into the jail's --rw-map <source>:<dest> mount spec", dir, ":")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("prepare codex state dir %s: %w", dir, err)
	}
	return []string{"--rw-map", dir + ":" + dir}, nil
}

// opencodeJailMounts resolves the opencode state dir a jailed opencode
// round needs mapped into the jail and returns it as an ai-jail argument.
// The generic jail passthrough blank-slates home (ai-jail's agent-state
// presets bridge ~/.claude and ~/.pi only), and opencode keeps its session
// database — and its auth.json — under <data home>/opencode, so an
// unmounted round's sessions die with the sandbox and the host-side
// tracking (opencodeSessionList) never sees them: the mount is what makes
// opencode's resume path real under the jail. The data home is
// XDG_DATA_HOME when the operator set it (opencode's documented data-dir
// root), ~/.local/share otherwise; like codexJailMounts the dir is mapped
// read-write at its host path so the child's own resolution lands on it,
// which requires the child to see the same XDG_DATA_HOME — the caller
// passes it through with --env (the amp pattern) when set. The accepted
// trade is pi's and codex's: the bridged dir carries opencode's
// auth.json, so host login state reaches jailed rounds read-write.
// ponytail: assembled but never probed against a real ai-jail — this
// sandbox has no binary; and that jailed opencode honors XDG_DATA_HOME is
// probe-derived (its documented XDG layout), not exercised live.
func opencodeJailMounts() ([]string, error) {
	dataHome := os.Getenv("XDG_DATA_HOME")
	if dataHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("resolve opencode state dir: %w", err)
		}
		dataHome = filepath.Join(home, ".local", "share")
	}
	dir := filepath.Join(dataHome, "opencode")
	// Same invariant FolderMounts and the bug-mirror block enforce for
	// their --rw-map specs: a colon in the path splits the spec at the
	// wrong boundary.
	if strings.ContainsRune(dir, ':') {
		return nil, fmt.Errorf("opencode state dir %s must not contain %q — it composes into the jail's --rw-map <source>:<dest> mount spec", dir, ":")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("prepare opencode state dir %s: %w", dir, err)
	}
	return []string{"--rw-map", dir + ":" + dir}, nil
}

// agentResumeFlag returns the flag that resumes a previous conversation on
// the named CLI — claude: --resume; pi: --session (it takes a session file
// path or a partial UUID, so the full recorded UUID works); opencode: -s
// (on its run subcommand). codex has no flag — its resume is a subcommand
// nested under exec (`codex exec resume <id>`), composed whole by
// agentResumeArgs, and "" marks that. ok is false for agents with no
// id-addressable resume (amp, aider — aider's chat history persists to a
// markdown file, not anything id-addressable), whose rounds ignore a set
// SessionID and always start fresh.
func agentResumeFlag(agent string) (flag string, ok bool) {
	switch agent {
	case "claude":
		return "--resume", true
	case "pi":
		return "--session", true
	case "opencode":
		return "-s", true
	case "codex":
		return "", true
	}
	return "", false
}

// agentResumeArgs composes a resuming round's agent args. claude and pi
// take their resume flag anywhere before the headless set, so the generic
// [flag, id] prefix composes ahead of the fixed agentArgs (see
// jailedAgentCLI) and the headless set still appends at the jail. opencode
// and codex resume only after their subcommand (opencode: `run -s <id>`;
// codex: `exec resume <id> -`, the `-` pointing the prompt at stdin after
// the id), so those two compose the whole post-`--` argv — subcommand and
// fixed headless flags embedded — and runJailedRoundFolders skips its own
// headless append for them (a non-empty caller agent-arg list for exactly
// those two CLIs is always such a precomposed round; their fixed list is
// empty). Callers gate on agentResumeFlag first: this assumes ok was true.
func agentResumeArgs(agent, id string, agentArgs []string) []string {
	flag, _ := agentResumeFlag(agent)
	switch agent {
	case "opencode":
		_, headless, _ := jailedAgentCLI(agent)
		// -s belongs to the run subcommand: it composes right after the
		// subcommand head, before the rest of the headless set.
		return append([]string{headless[0], flag, id}, headless[1:]...)
	case "codex":
		_, headless, _ := jailedAgentCLI(agent)
		// The resume subcommand nests under exec; the rest of the headless
		// set trails.
		return append([]string{headless[0], "resume", id, "-"}, headless[1:]...)
	}
	return append([]string{flag, id}, agentArgs...)
}

// agentConcurrency reads the worker-level cap on concurrent jailed-agent
// rounds, exported at worker startup from config max_concurrent_agent_runs
// (DAEDALUS_MAX_CONCURRENT_AGENT_RUNS). Unset, malformed, or non-positive
// values fall back to the config default.
func agentConcurrency() int {
	if v := os.Getenv("DAEDALUS_MAX_CONCURRENT_AGENT_RUNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return config.DefaultMaxConcurrentAgentRuns
}

// agentLimiter is the process-wide semaphore bounding concurrent jailed
// rounds (agent and reviewer alike — both hit the provider). All of a
// worker's workflows share it: concurrent cold agent sessions compete for
// one provider account, so queuing extras until a slot frees runs every
// round faster than running them all at once.
var (
	limiterOnce sync.Once
	limiter     chan struct{}
)

func agentLimiter() chan struct{} {
	limiterOnce.Do(func() {
		limiter = make(chan struct{}, agentConcurrency())
	})
	return limiter
}

// agentWaiters counts jailed rounds currently queued for a concurrency
// slot — the queue-depth half of the worker's published slot state
// (SlotStats), which `daedalus report` surfaces.
var agentWaiters atomic.Int64

// SlotStats reports this worker's jailed-round semaphore state: slots busy
// and total, and rounds currently queued waiting for one. The worker daemon
// publishes it next to its pid file (worker-<name>.status) so `daedalus
// report` can show slot occupancy and queue depth; the counters live here
// because the semaphore does.
func SlotStats() (busy, total, waiting int) {
	lim := agentLimiter()
	return len(lim), cap(lim), int(agentWaiters.Load())
}

// heartbeatInterval is how often a jailed round heartbeats while it waits
// for a semaphore slot or runs. Heartbeats make Temporal's history show a
// long round as alive (without them, a round thinking for forty minutes is
// indistinguishable from a hung one in the UI) and would feed a future
// HeartbeatTimeout; the interval is far below any plausible timeout.
const heartbeatInterval = 30 * time.Second

// slotWaitTimeout bounds how long a jailed round queues for a concurrency
// slot before reporting ErrAgentSlotsBusy. It sits well under the narrowest
// round's StartToClose (the reviewer's review_timeout, 15 minutes by
// default): while queued the round
// produces nothing, so letting the wait run to the budget's end would
// surface as a plain timeout the workflow would count against its timeout
// streak and answer with a continuation prompt for work that never started.
// A var (not a const) so tests can shorten it.
var slotWaitTimeout = 5 * time.Minute

// heartbeatLoop records activity heartbeats every heartbeatInterval until
// done closes or ctx ends. Outside a real activity context (unit tests
// invoke activities directly) the SDK panics by design; like activityLogger,
// the panic is recovered away.
func heartbeatLoop(ctx context.Context, done <-chan struct{}) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				defer func() { recover() }()
				activity.RecordHeartbeat(ctx)
			}()
		}
	}
}

// foldersMountRoot is the worktree-relative folder each granted host folder
// is mounted into inside a round's jail, named by the folder's basename so
// the paths in the opening prompt stay meaningful. A dot-folder keeps the
// worktree root visually clean (same convention as .daedalus-aider/), and it
// only ever exists as empty mountpoint dirs, which git does not see.
const foldersMountRoot = ".daedalus-folders"

// FolderMount is one granted host folder's read-write window into a round's
// jail: Source is the host absolute path, Dest the worktree-absolute
// mountpoint it appears at inside the sandbox.
type FolderMount struct {
	Source string
	Dest   string
}

// FolderMounts resolves a run's folder grants (PipelineInput.Folders) into
// the jail mount list: one read-write mount per grant, the host folder
// appearing at <worktree>/.daedalus-folders/<basename>. It revalidates every
// grant the way the CLI did at submit (exists, is a directory, not the
// filesystem root, colon-free — the path composes into ai-jail's
// --rw-map <source>:<dest> spec, and the worktree root naming Dest is not
// load-validated), so a grant that rotted between submit and the round fails
// it before spawn instead of half-mounting; identical duplicates collapse,
// and two grants sharing a basename are rejected — the second would silently
// shadow the first inside the sandbox. This is the one helper both the argv
// builder (runJailedRound) and the prompt relay (RunJailedClaudeActivity)
// render from, so the paths the agent is told can never drift from the
// mounted ones.
func FolderMounts(worktreePath string, folders []string) ([]FolderMount, error) {
	if len(folders) == 0 {
		return nil, nil
	}
	var mounts []FolderMount
	seen, byBase := map[string]bool{}, map[string]bool{}
	for _, folder := range folders {
		abs, err := filepath.Abs(folder)
		if err != nil {
			return nil, fmt.Errorf("folder grant %s: %w", folder, err)
		}
		if abs == string(filepath.Separator) {
			return nil, errors.New("folder grant must not be the filesystem root")
		}
		if strings.ContainsRune(abs, ':') {
			return nil, fmt.Errorf("folder grant %s must not contain %q — it composes into the jail's --rw-map <source>:<dest> mount spec", abs, ":")
		}
		info, err := os.Stat(abs)
		if err != nil {
			return nil, fmt.Errorf("folder grant %s: %w", abs, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("folder grant %s is not a directory", abs)
		}
		if seen[abs] {
			continue
		}
		seen[abs] = true
		base := filepath.Base(abs)
		dest := filepath.Join(worktreePath, foldersMountRoot, base)
		if byBase[base] {
			return nil, fmt.Errorf("two folder grants share the basename %q — the second would shadow the first inside %s", base, foldersMountRoot)
		}
		byBase[base] = true
		if strings.ContainsRune(dest, ':') {
			return nil, fmt.Errorf("folder mountpoint %s must not contain %q — it is composed into the jail's --rw-map <source>:<dest> mount spec", dest, ":")
		}
		mounts = append(mounts, FolderMount{Source: abs, Dest: dest})
	}
	return mounts, nil
}

// folderMountNote renders the prompt paragraph mapping the run's sandbox
// mount paths to their host folders: the agent cannot see host paths, and
// the note is what makes brief-mandated bookkeeping (a shared done-index,
// retiring the brief itself) executable inside the jail. The agent's working
// directory is the worktree root, so the worktree-relative mountpoint is the
// addressable path.
func folderMountNote(mounts []FolderMount) string {
	var b strings.Builder
	b.WriteString("\n\nGRANTED HOST FOLDERS — read-write mounts inside this sandbox; edits through these paths land directly on the host and never appear in this worktree's git tree:\n")
	for _, m := range mounts {
		fmt.Fprintf(&b, "- %s — host %s\n",
			filepath.Join(foldersMountRoot, filepath.Base(m.Source)), m.Source)
	}
	return b.String()
}

// runJailed runs one autonomous agent invocation inside an ai-jail sandbox
// rooted at the worktree, with provider failover (see providerAvailability):
// the round runs on the primary provider unless it is in a dry hold, in
// which case the fallback provider serves it, and a round that fails with
// the primary's quota exhausted is retried once on the fallback immediately
// rather than waiting out the workflow's hourly sleep. agent names the
// jailed CLI to run — a `run -cli/--cli` override, empty for the worker's
// DAEDALUS_AGENT default. agentArgs are appended to the agent's fixed
// argument set before its headless flags. The prompt travels via stdin,
// not argv: argv is visible in `ps` and per-argument size limits would make
// large review prompts (which embed the full diff) fail the run — aider is
// the one exception, bridged through a worktree temp file and
// --message-file (see runJailedRound). While
// waiting for a concurrency slot and while the agent runs, the round
// heartbeats so Temporal history shows liveness; the slot wait itself is
// bounded by slotWaitTimeout so a queued round cannot burn its whole
// StartToClose budget without the agent ever launching. Rounds run through
// runJailed are untracked — one-shot rounds like test-command discovery
// never chain conversations; chained rounds go through runJailedKind.
func runJailed(ctx context.Context, agent, worktreePath, prompt string, agentArgs ...string) (jailResult, error) {
	return runJailedKind(ctx, "", agent, worktreePath, prompt, agentArgs...)
}

// runJailedKind is runJailed for a round that belongs to a chained
// conversation: role names it (RoleDev/RoleTest/RoleDevReview/
// RoleTestReview), and the round's conversation id is recorded for retry
// resume as soon as the CLI creates its session state (see agentsession.go;
// claude, pi, opencode, and codex keep session state daedalus can address).
// An empty role — discovery, and every direct caller that does not chain —
// skips the tracking.
func runJailedKind(ctx context.Context, role SessionRole, agent, worktreePath, prompt string, agentArgs ...string) (jailResult, error) {
	return runJailedKindFolders(ctx, role, agent, worktreePath, prompt, nil, agentArgs...)
}

// runJailedKindFolders is runJailedKind with the run's granted host folders
// (AgentRunInput.Folders); nil for every caller but the implementing agent —
// reviewer rounds carry no grants by design. (Split from runJailedKind so
// that function's and runJailedRound's existing signatures — and the tests
// pinning them — stay untouched.)
func runJailedKindFolders(ctx context.Context, role SessionRole, agent, worktreePath, prompt string, folders []string, agentArgs ...string) (jailResult, error) {
	env, side := selectProvider()
	if env == nil {
		return jailResult{}, fmt.Errorf("%w: primary and fallback providers are both in dry holds", ErrAPIExhausted)
	}
	res, err := runJailedRoundFolders(ctx, env, role, agent, worktreePath, prompt, folders, agentArgs...)
	if err == nil || !errors.Is(err, ErrAPIExhausted) {
		return res, err
	}
	logger := activityLogger(ctx)
	until := markProviderDry(side, err.Error())
	logger.Info("provider api exhausted", "side", side, "dryUntil", until.Format(time.RFC3339))
	if other, otherSide := otherProviderEnv(side); other != nil {
		// Only spend the retry when the activity budget can fit another
		// full round: a fallback round truncated at the deadline would die
		// to our own context kill and masquerade as ErrAgentKilled.
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < fallbackRetryFloor {
			logger.Info("skipping fallback retry: activity budget nearly spent", "side", otherSide)
			return res, err
		}
		logger.Info("failing round over to fallback provider", "side", otherSide)
		res2, err2 := runJailedRoundFolders(ctx, other, role, agent, worktreePath, prompt, folders, agentArgs...)
		if err2 == nil {
			return res2, nil
		}
		if !errors.Is(err2, ErrAPIExhausted) {
			// A fallback round that died any other way (killed, slots
			// busy) must not overwrite the primary's exhaustion
			// classification: the workflow answers those differently
			// (continuation prompts, re-queues), and the fact that
			// matters downstream is that the primary is dry.
			return res, err
		}
		until2 := markProviderDry(otherSide, err2.Error())
		logger.Info("fallback provider api exhausted too", "side", otherSide, "dryUntil", until2.Format(time.RFC3339))
		return res2, err2
	}
	return res, err
}

// runJailedRound is runJailedKind's single invocation against one provider's
// environment. A tracked round (non-empty role) also snapshots the CLI's
// transcript dir before spawn and records any transcript the round's
// session creates — the file on disk, not anything the round printed — so
// the conversation id is on record for retry resume within seconds of the
// session starting, before any ceiling can land.
func runJailedRound(ctx context.Context, env []string, role SessionRole, agent, worktreePath, prompt string, agentArgs ...string) (jailResult, error) {
	return runJailedRoundFolders(ctx, env, role, agent, worktreePath, prompt, nil, agentArgs...)
}

// runJailedRoundFolders is runJailedRound with the run's granted host
// folders (AgentRunInput.Folders); nil mounts nothing (see
// runJailedKindFolders for the split).
func runJailedRoundFolders(ctx context.Context, env []string, role SessionRole, agent, worktreePath, prompt string, folders []string, agentArgs ...string) (jailResult, error) {
	// Reviewer rounds dial the config's own reviewer endpoint when one is
	// configured (see reviewerEnv); every other role keeps the serving
	// provider's environment untouched. Applied before anything reads the
	// round env — pi's provider staging included.
	env = reviewerEnv(env, role)
	selected, headless, _ := jailedAgentCLI(agent)
	// opencode and codex resumed rounds arrive with the whole post-`--`
	// argv precomposed (agentResumeArgs — their resume tokens follow the
	// run/exec subcommand, so the fixed headless set is already embedded);
	// appending the fixed set behind it would repeat it. Their fixed list
	// is empty, so a non-empty caller list for exactly those two CLIs is
	// always such a precomposed round — a fresh or one-shot round (no
	// caller args) takes the fixed set as usual.
	if len(agentArgs) > 0 && (selected == "opencode" || selected == "codex") {
		headless = nil
	}
	// pi-served implementing rounds carry a small-model edit-discipline
	// guardrail (self-hosted models otherwise retry failing edits without
	// re-reading; wa-termo 2026-09-29). Gated to pi and the edit-carrying
	// roles — reviewer rounds do not edit — so every flagship prompt stays
	// byte-identical. Appended per invocation, so a provider-failover retry
	// (which re-enters with the original prompt) is not double-prefixed.
	if selected == "pi" && role != RoleDevReview && role != RoleTestReview {
		prompt += piEditGuardrail
	}
	lim := agentLimiter()
	hbDone := make(chan struct{})
	defer close(hbDone)
	go heartbeatLoop(ctx, hbDone)
	wait := time.NewTimer(slotWaitTimeout)
	defer wait.Stop()
	agentWaiters.Add(1)
	select {
	case lim <- struct{}{}:
		agentWaiters.Add(-1)
		defer func() { <-lim }()
	case <-wait.C:
		agentWaiters.Add(-1)
		return jailResult{}, fmt.Errorf("%w: no slot freed within %s", ErrAgentSlotsBusy, slotWaitTimeout)
	case <-ctx.Done():
		agentWaiters.Add(-1)
		return jailResult{}, fmt.Errorf("agent slot: %w", ctx.Err())
	}
	args := []string{"--worktree", "--network"}
	// Mask the worktree's .claude/settings*.json: Claude Code applies a
	// repo-committed settings file's env block over process env — a stale
	// ANTHROPIC_AUTH_TOKEN there authenticated every jailed round as the
	// repo's dead credential and 429'd regardless of provider failover
	// (c360 post-mortem, 2026-09-24). Masking (empty file) rather than
	// unmounting keeps Claude's settings loader happy, and the permission
	// allowlists those files carry are moot inside the jail, where the
	// headless flags pre-approve everything. The mask is imposed here, in
	// the invoker's argv: a repo-controlled .ai-jail file sits on the wrong
	// side of the trust boundary (ai-jail writes it into the worktree).
	// Relative masks resolve against the worktree root the way the
	// --worktree default masks do (same resolution the probed .env default
	// uses). Other jailed agents ignore .claude/, so the masks are
	// unconditional. ponytail: appended-mask resolution itself is unprobed
	// on this host (no ai-jail binary here) — confirm once against a real
	// jail if a repo settings file is ever seen reaching a round.
	// Mask the user-level ~/.claude/settings.json too: the jail's
	// home-dotdir passthrough mounts it, and Claude Code applies a settings
	// file's env block over process env — the exact precedence the
	// worktree masks above guard against — so the operator's own
	// ANTHROPIC_BASE_URL/API_KEY/MODEL there override AgentEnv's injected
	// values and every jailed claude round dials the operator's config
	// instead of the worker's (live 2026-10-05: maestro workflows served
	// by Mistral model names nothing in the worker config produces). It
	// also removes the AUTH_TOKEN trap: claude prefers
	// ANTHROPIC_AUTH_TOKEN over ANTHROPIC_API_KEY when both are set, and
	// operator home files typically carry the former while daedalus only
	// ever injects the latter. Masking (not unmounting) keeps the settings
	// loader happy, and the rest of ~/.claude — session state, the
	// transcripts the watcher reads — stays mounted. Other jailed agents
	// ignore .claude/, so this mask is unconditional like the worktree
	// ones. ponytail: an absolute mask outside the worktree root is
	// probe-owed on the same real-jail pass as the appended masks above,
	// as is --mask's tolerance for a home path that does not exist.
	home, err := os.UserHomeDir()
	if err != nil {
		return jailResult{}, fmt.Errorf("resolve home directory: %w", err)
	}
	args = append(args, "--mask", ".claude/settings.json",
		"--mask", ".claude/settings.local.json",
		"--mask", filepath.Join(home, ".claude", "settings.json"))
	// amp's host login state (~/.config/amp) is not among ai-jail's
	// agent-state dirs and AMP_API_KEY is not in its default env allowlist,
	// so neither documented auth route reaches the jailed child on a stock
	// jail. --env copies the value from this process's environment (set
	// below), making the key route work without operator jail config.
	if selected == "amp" && os.Getenv("AMP_API_KEY") != "" {
		args = append(args, "--env", "AMP_API_KEY")
	}
	// ai-jail has no aider preset: its generic passthrough blank-slates
	// /home/hugo, where aider's entire uv-tools install lives, so the round
	// would die at execve before aider's own code runs (wa-termo
	// 2026-09-24). Map the install in before launch — a resolution failure
	// names the missing path instead of starting a doomed round.
	if selected == "aider" {
		mounts, err := aiderJailMounts()
		if err != nil {
			return jailResult{}, err
		}
		args = append(args, mounts...)
	}
	// ai-jail has no codex preset: its agent-state dirs bridge ~/.claude
	// and ~/.pi, not ~/.codex — map the state dir in read-write before
	// launch (codexJailMounts), or the round can neither authenticate nor
	// record the session a retry would resume. CODEX_HOME rides the amp
	// pattern (--env copies the value from this process's environment)
	// when the operator set it: the mount follows the var host-side, so
	// the jailed codex must resolve the same dir — without it the child
	// falls back to $HOME/.codex and misses the mounted state entirely.
	if selected == "codex" {
		mounts, err := codexJailMounts()
		if err != nil {
			return jailResult{}, err
		}
		args = append(args, mounts...)
		if os.Getenv("CODEX_HOME") != "" {
			args = append(args, "--env", "CODEX_HOME")
		}
	}
	// Same for opencode: its session database (and auth.json) lives under
	// the data home (opencodeJailMounts), and an operator-set
	// XDG_DATA_HOME must reach the child or it resolves a different root
	// than the one mounted.
	if selected == "opencode" {
		mounts, err := opencodeJailMounts()
		if err != nil {
			return jailResult{}, err
		}
		args = append(args, mounts...)
		if os.Getenv("XDG_DATA_HOME") != "" {
			args = append(args, "--env", "XDG_DATA_HOME")
		}
	}
	// The bug-file mirror (bug_filing.mirror), mounted read-write into the
	// round's sandbox at the worktree-relative bug dir when configured: the
	// agent writes through the familiar worktree-relative path and the
	// writes are bind-mounted straight onto the host dir — the real
	// worktree never holds the files, so host-side git status stays clean
	// and the branch copies nothing. Both envs are exported together at
	// worker startup iff filing is enabled and a mirror configured; either
	// absent means off and the args stay byte-identical to the unmounted
	// shape. The mirror is an accepted operator trade (mirror-mapping task,
	// 2026-09-28): it hands the jailed agent a read-write window onto a
	// host path, steerable by repo content — the operator chooses the path.
	// Fail-fast by construction: the mountpoint is created host-side before
	// spawn (an empty dir is invisible to git) and an unusable mirror fails
	// the round here, because a round that ran unmounted would write bug
	// files into the real worktree and stage them into the branch — the
	// exact pollution the mirror exists to prevent. ai-jail itself failing
	// the mount (rejected flag, bwrap error) exits nonzero before the agent
	// runs, which the wait below already surfaces as a failed round; there
	// is no proceed-unmounted path. The spec's SOURCE:DEST split is kept
	// unambiguous: ResolveMirror rejects a colon in the mirror and
	// config.validate rejects one in dir while a mirror is configured —
	// but the worktree root is not load-validated (task_queue names it),
	// so the block below checks the composed DEST itself.
	// ponytail: assembled here but never
	// probed against a real ai-jail — this sandbox has no binary; that
	// --rw-map nests inside the --worktree rw mount, tolerates the argument
	// ordering, and leaves the per-agent masks (pi auth, aider venv,
	// .claude/settings*) unaffected is verified only against the host-side
	// `ai-jail --help` of 2026-09-28. Probe per preset (claude first) on
	// the first live host.
	if bugDir, mirror := os.Getenv(config.BugDirEnv), os.Getenv(config.BugMirrorEnv); bugDir != "" && mirror != "" {
		if err := os.MkdirAll(mirror, 0o755); err != nil {
			return jailResult{}, fmt.Errorf("prepare bug mirror dir %s: %w", mirror, err)
		}
		dest := filepath.Join(worktreePath, bugDir)
		// The load gates keep mirror and dir colon-free, but the worktree
		// root is not load-validated (task_queue names it, and only
		// emptiness and the -test suffix are checked) — so the composed
		// DEST is checked here, fail-fast like the rest of the block.
		if strings.ContainsRune(dest, ':') {
			return jailResult{}, fmt.Errorf("bug dir mountpoint %s must not contain %q — it is composed into the jail's --rw-map <mirror>:<dir> mount spec", dest, ":")
		}
		if err := os.MkdirAll(dest, 0o755); err != nil {
			return jailResult{}, fmt.Errorf("prepare bug dir mountpoint %s: %w", dest, err)
		}
		args = append(args, "--rw-map", mirror+":"+dest)
	}
	// Granted host folders (AgentRunInput.Folders, `run -folder/--folder`
	// plus the -f task file's folder): one read-write mount per grant under
	// <worktree>/.daedalus-folders/<basename>, so a jailed round can reach
	// operator-chosen host folders — the task-brief bookkeeping (a shared
	// done-index, retiring the brief itself) that the worktree-only jail
	// makes unsatisfiable. Same accepted operator trade as the bug mirror
	// above: a read-write window onto a host path, chosen by the operator's
	// own argv and validated at submit. Fail-fast like the mirror: the
	// mountpoints are created host-side before spawn (an empty dir is
	// invisible to git) and an unusable grant fails the round here — a round
	// that ran unmounted would write the bookkeeping into the real worktree
	// and stage it into the branch. ai-jail itself failing the mount exits
	// nonzero before the agent runs, which the wait below already surfaces
	// as a failed round; there is no proceed-unmounted path.
	// ponytail: the repeated --rw-map form below is assembled but never
	// probed against a real ai-jail (no binary in this sandbox) — pin the
	// accepted argv shape on the first live probe, alongside the mirror
	// block's own.
	mounts, ferr := FolderMounts(worktreePath, folders)
	if ferr != nil {
		return jailResult{}, ferr
	}
	for _, m := range mounts {
		if err := os.MkdirAll(m.Dest, 0o755); err != nil {
			return jailResult{}, fmt.Errorf("prepare folder mountpoint %s: %w", m.Dest, err)
		}
		args = append(args, "--rw-map", m.Source+":"+m.Dest)
	}
	// "--" ends ai-jail's own flags: everything after it is the jailed
	// command, verbatim — otherwise ai-jail rejects child flags that
	// resemble its own (e.g. claude's --verbose) as misplaced.
	args = append(args, "--", selected)
	args = append(args, agentArgs...)
	// pi ignores the base-URL env vars entirely (only OPENAI_API_KEY reaches
	// its built-in openai provider — the 401-to-api.openai.com of
	// 2026-09-26), so a pi round served by the openai section needs the
	// section bridged into pi's own provider config: stagePiProvider
	// rewrites the daedalus-owned entry in the host's
	// ~/.pi/agent/models.json (rw-mounted by the jail's pi preset) and
	// returns the --model flag selecting it. Failover rides the same bridge
	// only for openai-type fallbacks — an anthropic-type fallback has no
	// pi channel at all (pi reads no ANTHROPIC_BASE_URL), so the round
	// stays pinned to the primary's staged entry (see
	// backlog/bugs/pi-anthropic-fallback-unserveable.md).
	if selected == "pi" {
		modelArgs, err := stagePiProvider(env)
		if err != nil {
			return jailResult{}, err
		}
		args = append(args, modelArgs...)
	}
	// aider takes its prompt via --message-file, not stdin — it has no
	// read-the-prompt-from-stdin mode (--message-file - is undocumented),
	// and --message would put the prompt in argv, visible in `ps` and
	// bounded by per-argument size limits, both of which the stdin
	// transport exists to avoid. The prompt lives under .daedalus-aider/
	// (next to aider's chat/input history), which excludeAiderArtifacts
	// keeps out of git's sight — so a hard host kill that orphans the
	// file cannot leak the prompt (for reviewer rounds, the entire diff)
	// into a review diff or a preserved branch via add -A/-N -A.
	if selected == "aider" {
		if err := excludeAiderArtifacts(worktreePath); err != nil {
			return jailResult{}, fmt.Errorf("prepare aider scratch dir: %w", err)
		}
		f, err := os.CreateTemp(filepath.Join(worktreePath, ".daedalus-aider"), "prompt-*.md")
		if err != nil {
			return jailResult{}, fmt.Errorf("stage aider prompt file: %w", err)
		}
		if _, err := f.WriteString(prompt); err != nil {
			f.Close()
			os.Remove(f.Name())
			return jailResult{}, fmt.Errorf("stage aider prompt file: %w", err)
		}
		f.Close()
		defer os.Remove(f.Name())
		args = append(args, "--message-file", f.Name())
		// Model wiring (openai section only — the anthropic half is a
		// follow-up pending a probe of which env var the installed
		// litellm's anthropic provider honors). The argv flag, not the
		// exported OPENAI_MODEL env var, selects aider's model — aider
		// ignores that var — and the openai/ prefix is what makes aider's
		// litellm layer dial the custom base URL instead of api.openai.com,
		// so neither half of the flag is redundant. When no openai section
		// is configured, no flag and no extra exports: aider's own default.
		if model := envLookup(env, "OPENAI_MODEL"); model != "" {
			prefixed := "openai/" + model
			// The configured context window (anthropic.context_tokens) rides
			// the same export claude reads; here it replaces the staged
			// metadata's input side. The completion cap below has its own
			// knob (anthropic.max_output_tokens, same channel) — the window
			// says nothing about output size.
			in := aiderMaxInputTokens
			if n, err := strconv.Atoi(envLookup(env, config.ContextTokensEnv)); err == nil && n > 0 {
				in = n
			}
			out := aiderMaxOutputTokens
			if n, err := strconv.Atoi(envLookup(env, config.MaxOutputTokensEnv)); err == nil && n > 0 {
				out = n
			}
			if err := stageAiderModelFiles(worktreePath, prefixed, in, out, samplerFromEnv(env)); err != nil {
				return jailResult{}, fmt.Errorf("stage aider model files: %w", err)
			}
			args = append(args, "--model", prefixed,
				"--model-metadata-file", aiderModelMetadataArg,
				"--model-settings-file", aiderModelSettingsArg)
			// OPENAI_API_BASE rides the same value as OPENAI_BASE_URL:
			// litellm versions disagree on which var they honor. AgentEnv
			// exports both; this bridges failover and ambient environments.
			if base := envLookup(env, "OPENAI_BASE_URL"); base != "" {
				env = setEnvVar(env, "OPENAI_API_BASE", base)
			}
		}
	}
	args = append(args, headless...)
	// codex reads no provider env var natively, so an openai-served codex
	// round is bridged per invocation: stageCodexProvider returns the -c
	// overrides selecting the staged provider and -m the model, layered
	// over the host config the jail maps in (codexJailMounts) without
	// rewriting it. Appended after the headless set — the overrides must
	// follow the exec subcommand (their flags are defined there), in both
	// this fresh shape and the precomposed resumed shape (headless nil).
	// An anthropic-section round has no codex channel (see jailedAgentCLI)
	// — the anthropic env simply stages nothing, and the round dials
	// whatever the host's ~/.codex/config.toml says.
	if selected == "codex" {
		modelArgs, err := stageCodexProvider(env)
		if err != nil {
			return jailResult{}, err
		}
		args = append(args, modelArgs...)
	}
	// pi's thinking follows the config: an explicit thinking: false is
	// exported as DAEDALUS_THINKING=off at worker startup. The builder
	// translates that one signal to each agent's own off lever here —
	// per-agent mapping of a single agnostic config knob, with agents
	// without a lever staying silent: pi gains `--thinking off`; aider
	// gains `--thinking-tokens 0` (argv, like --model — its --help
	// documents that 0 disables); claude's round env gains
	// MAX_THINKING_TOKENS=0 (verified in binary 2.1.283's own code: 0
	// selects {type:"disabled"}). opencode and amp have no verified off
	// lever and ignore the setting. The flags are appended here rather
	// than in jailedAgentCLI so discovery and every other caller of that
	// function stay flag-blind.
	if os.Getenv("DAEDALUS_THINKING") == "off" {
		switch selected {
		case "pi":
			args = append(args, "--thinking", "off")
		case "aider":
			args = append(args, "--thinking-tokens", "0")
		case "claude":
			env = setEnvVar(env, "MAX_THINKING_TOKENS", "0")
		}
	}
	// The configured completion cap (anthropic.max_output_tokens, riding
	// the round env on MaxOutputTokensEnv) has a native claude lever too:
	// CLAUDE_CODE_MAX_OUTPUT_TOKENS — the var claude 2.1.283's own error
	// text names for its request-level max_tokens override (probe-verified
	// in the installed binary; the earlier "no lever" claim in this file's
	// history was a missed strings hit). The aider staging reads
	// MaxOutputTokensEnv directly and agents without a route ignore the
	// knob, per the agnostic rule.
	if selected == "claude" {
		if v := envLookup(env, config.MaxOutputTokensEnv); v != "" {
			env = setEnvVar(env, "CLAUDE_CODE_MAX_OUTPUT_TOKENS", v)
		}
	}
	// The stream toggle (stream: true/false → DAEDALUS_STREAM=on/off at
	// worker startup) translates to each agent's own streaming lever:
	// aider gains --stream/--no-stream, its documented option pair
	// (unprobed on this host — no aider binary; same verification class as
	// --thinking-tokens). The other agents have no verified streaming
	// lever and ignore the setting, per the agnostic rule. The values are
	// matched strictly, like DAEDALUS_THINKING above — an ambient
	// garbage value is ignored, never silently read as an explicit off.
	if selected == "aider" {
		switch os.Getenv("DAEDALUS_STREAM") {
		case "on":
			args = append(args, "--stream")
		case "off":
			args = append(args, "--no-stream")
		}
	}
	cmd := exec.CommandContext(ctx, "ai-jail", args...)
	setProcessGroup(cmd)
	defer killGroup(cmd)
	cmd.Dir = worktreePath
	// env carries the provider settings the worker exported from
	// config.yaml (primary, or the fallback overrides for a failover
	// round); everything else passes through from os.Environ(). Under slim
	// mode, jailed aider rounds pin aider's weak/editor wires to the
	// effective AIDER_MODEL — without it those wires silently dial a cloud
	// default, i.e. a self-hosted deployment making surprise paid-API
	// calls (see slimAiderEnv).
	if selected == "aider" {
		env = slimAiderEnv(env)
	}
	cmd.Env = env
	cmd.Stdin = strings.NewReader(prompt)

	// Session tracking snapshot: the session state the CLI already holds
	// for this worktree, taken before spawn so the round's own session is
	// the new one (claude: its transcript dir; pi: its per-worktree session
	// dir; codex: its rollout tree, arbitrated per worktree by the
	// recorded cwd; opencode: its session database, listed by shelling out
	// — amp and aider create nothing the watcher would find, so the claude
	// snapshot stays harmless for them). If the source cannot be read,
	// tracking stays off for the round (known stays nil) rather than risk
	// recording an old session. The capture itself is the session state on
	// disk — not anything the round printed: pi's json output carries
	// the session id from its first line, but nothing a jailed round
	// prints may decide anything (see the ErrAgentKilled wrap note below).
	var known map[string]bool
	idsFn, recordFn, watchFn := transcriptIDs, recordNewTranscript, watchTranscripts
	if role != "" {
		switch selected {
		case "pi":
			idsFn, recordFn, watchFn = piTranscriptIDs, recordNewPiTranscript, watchPiTranscripts
		case "opencode":
			idsFn, recordFn, watchFn = opencodeTranscriptIDs, recordNewOpencodeTranscript, watchOpencodeTranscripts
		case "codex":
			idsFn, recordFn, watchFn = codexTranscriptIDs, recordNewCodexTranscript, watchCodexTranscripts
		}
		if ids, ok := idsFn(worktreePath); ok {
			known = ids
		}
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	start := time.Now()
	logger := activityLogger(ctx)
	if err := cmd.Start(); err != nil {
		return jailResult{}, fmt.Errorf("%w: %w", errAgentStart, err)
	}
	// Light polling until the round's transcript appears; the final sweep
	// after Wait covers a transcript landing inside the last tick.
	watchDone := make(chan struct{})
	watchStop := make(chan struct{})
	if known != nil {
		go func() {
			defer close(watchDone)
			watchFn(ctx, known, worktreePath, role, watchStop)
		}()
	}
	// The jailed process leads its own group (Setpgid), so pid == pgid.
	// Logged at spawn so a round that dies to an outside SIGKILL — not one
	// of daedalus's own kill paths — can be correlated with host logs
	// afterward.
	logger.Info("Jailed agent started", "Agent", selected, "PID", cmd.Process.Pid)
	// Task-log round-start block, written as soon as the group leader (and
	// so the pgid) exists — before the agent can produce anything. The
	// stage is the round's session role (empty for one-shot rounds); the
	// paired exit block below makes "currently running" derivable from the
	// log alone.
	appendTaskLog(ctx, fmt.Sprintf("jailed %s round started: stage=%s worktree=%s pgid=%d",
		selected, role, worktreePath, cmd.Process.Pid), "")
	err = waitCommand(ctx, cmd)
	if known != nil {
		close(watchStop)
		<-watchDone
		recordFn(ctx, known, worktreePath, role)
	}
	res := jailResult{Stdout: stdout.String(), Stderr: stderr.String()}
	// Task-log round-exit block: the full output, captured before
	// classification — killed and exhausted rounds included,
	// whose output the callers otherwise discard.
	body := res.Stdout
	if res.Stderr != "" {
		body += "\n--- stderr ---\n" + res.Stderr
	}
	appendTaskLog(ctx, fmt.Sprintf("jailed %s round exited after %s",
		selected, time.Since(start).Round(time.Second)), body)
	// Structured api_error classification: the CLI's own machine-readable
	// report (claude/amp result JSON, pi json stream) of a round that died
	// on a provider error. It routes into ErrAPIExhausted — whose path is
	// the rescue: immediate failover to the other provider here, then the
	// workflow's hourly heartbeat and, past its ceiling, a park to the
	// preserved aborted/ branch — instead of today's outcomes (a fatal
	// wipe for claude's exit 1, a silent success for pi's exit 0). The
	// worktree survives until the rescue decision: only the cleanup that
	// runs after the workflow ends touches it. A signaled death below
	// still outranks this report, exactly as it outranks the text markers.
	apiErr := agentAPIError(selected, res.Stdout)
	if err != nil && !isWaitDelay(err) {
		if sig := deathSignal(err); sig != "" {
			logDeath(logger, cmd, err, start, sig)
			// A signaled death is wait-status-derived, so it outranks the
			// output-text exhaustion markers below: an externally killed
			// round whose earlier stdout happens to contain one must not
			// park the run in the quota heartbeat. The wrap deliberately
			// carries no output — the diagnostics live in the death log
			// above, and embedding output here would let agent text forge
			// the workflow-side classification.
			return res, fmt.Errorf("%w: %w", ErrAgentKilled, err)
		}
		if apiErr != "" {
			return res, fmt.Errorf("%w: %s round died on an api error: %s", ErrAPIExhausted, selected, apiErr)
		}
		out := res.Stdout + res.Stderr
		if matchesAny(out, apiExhaustionMarkers) || matchesAny(out, apiErrorTextMarkers) {
			return res, fmt.Errorf("%w: %w: %s", ErrAPIExhausted, err, out)
		}
		return res, fmt.Errorf("ai-jail agent run: %w: %s", err, out)
	}
	// A clean exit can still carry a structured api_error report (pi folds
	// provider failures and exits 0) — same rescue routing as above.
	if apiErr != "" {
		return res, fmt.Errorf("%w: %s round died on an api error: %s", ErrAPIExhausted, selected, apiErr)
	}
	// Exit-0 plain-text faces (aider, opencode, codex): their API failures
	// surface only as litellm/connection error text in otherwise-successful
	// output, so classify a marker hit as a failed round instead of a silent
	// success. All three streams are read, symmetric with the failed path
	// above — which CLI prints its failure face where is its business, not
	// this check's. The structured faces never reach this branch
	// (agentAPIError handled them above), which is what keeps the
	// forgeable match off claude and pi.
	// ponytail: this match IS forgeable — chat text an agent printed can
	// park a healthy run in the heartbeat; accepted because the markers
	// are litellm-internal shapes an agent has no honest reason to print,
	// and the cost is bounded (failover, heartbeat, park), never silent
	// wrong work.
	if selected == "aider" || selected == "opencode" || selected == "codex" {
		if out := res.Stdout + res.Stderr; matchesAny(out, apiErrorTextMarkers) {
			return res, fmt.Errorf("%w: %s round reported an api error and exited clean: %s",
				ErrAPIExhausted, selected, out)
		}
	}
	return res, nil
}

// excludeAiderArtifacts creates aider's scratch dir (.daedalus-aider/,
// holding the staged prompt and its chat/input history) and keeps it out of
// git's sight by adding it to the worktree's info/exclude. info/exclude is
// what add -A/-N -A and git diff honor without ever touching a tracked
// file — the alternatives each pollute the deliverable: a .gitignore entry
// becomes a tracked change, and leaving the files plain lands them in
// every review diff and preserved branch. Idempotent: the pattern is
// appended only once per worktree.
func excludeAiderArtifacts(worktreePath string) error {
	if err := os.MkdirAll(filepath.Join(worktreePath, ".daedalus-aider"), 0o755); err != nil {
		return err
	}
	out, err := runGit(context.Background(), "-C", worktreePath, "rev-parse", "--git-common-dir")
	if err != nil {
		return fmt.Errorf("resolve git dir: %w", err)
	}
	dir := strings.TrimSpace(string(out))
	if dir == "." || !filepath.IsAbs(dir) {
		dir = filepath.Join(worktreePath, dir)
	}
	const line = ".daedalus-aider/"
	exclude := filepath.Join(dir, "info", "exclude")
	data, _ := os.ReadFile(exclude)
	if strings.Contains(string(data), line) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(exclude), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(exclude, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	var entry strings.Builder
	if len(data) > 0 && data[len(data)-1] != '\n' {
		entry.WriteByte('\n')
	}
	entry.WriteString(line)
	entry.WriteByte('\n')
	_, err = f.WriteString(entry.String())
	return err
}

// aiderModelMetadataArg and aiderModelSettingsArg are the scratch-dir paths
// handed to aider's --model-metadata-file / --model-settings-file, next to
// the staged prompt (excluded from git's sight by excludeAiderArtifacts).
// The metadata file is JSON despite the sibling settings file being YAML:
// aider's register_litellm_models parses the metadata file with json5.loads
// — no extension dispatch, no YAML fallback — and requires an object keyed
// by model name.
const (
	aiderModelMetadataArg = ".daedalus-aider/model.metadata.json"
	aiderModelSettingsArg = ".daedalus-aider/model.settings.yml"
)

// The self-hosted models aider rounds serve are absent from litellm's model
// DB, so litellm assumes a tiny default context window and aider over-fills
// context until the backend errors. 64k in / 8k out restore sane budgeting
// for aider's chat-history trimming and repo map. These are the fallbacks
// for an unset anthropic.context_tokens (which rides ContextTokensEnv and
// replaces the input side per round) and an unset anthropic.max_output_tokens
// (which rides MaxOutputTokensEnv and replaces the output side per round).
const (
	aiderMaxInputTokens  = 65536
	aiderMaxOutputTokens = 8192
)

// aiderSampler carries the openai section's sampler knobs, parsed from the
// round env. A nil field is omitted from the staged settings — absent from
// the wire request, never sent as null/0, so the backend's own sampler
// defaults stand (a sent default overriding them was the exact bug class
// that motivated these knobs).
type aiderSampler struct {
	TopP, Temperature, PresencePenalty, TopK, MinP, RepetitionPenalty *float64
}

// samplerFromEnv parses the DAEDALUS_* sampler exports out of the round
// env. An export is only written when its config field is set, so an
// absent (or stale hand-set malformed) var simply stays nil. Non-finite
// values (e.g. the stale-ambient DAEDALUS_TOP_P=nan class) are rejected
// like any other malformed var — mirroring Config.validate, which the env
// channel bypasses — so they can never reach a staged file and blow up
// json.Marshal (pi) or render as a junk YAML float (aider).
func samplerFromEnv(env []string) aiderSampler {
	parse := func(name string) *float64 {
		v, err := strconv.ParseFloat(envLookup(env, name), 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return nil
		}
		return &v
	}
	return aiderSampler{
		TopP:              parse(config.TopPEnv),
		Temperature:       parse(config.TemperatureEnv),
		PresencePenalty:   parse(config.PresencePenaltyEnv),
		TopK:              parse(config.TopKEnv),
		MinP:              parse(config.MinPEnv),
		RepetitionPenalty: parse(config.RepetitionPenaltyEnv),
	}
}

// yamlFloat renders v as a YAML 1.1 float literal — always carrying a
// decimal point, because aider's settings loader (pyyaml) reads a bare
// "1e-05" as a string.
func yamlFloat(v float64) string {
	s := strconv.FormatFloat(v, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}

// stageAiderModelFiles writes aider's model metadata and settings for the
// prefixed model name the argv flag passes, keyed by that same name.
// maxInputTokens is the context window the metadata declares — the config's
// anthropic.context_tokens when set, the constant otherwise; maxOutputTokens
// is the completion cap — anthropic.max_output_tokens when set, the
// constant otherwise. sampler's non-nil fields join the settings' request
// params (see aiderSampler). The metadata file is a JSON object keyed by
// model name — a YAML sequence fails aider's json5 parse and aider then
// only logs a stderr warning before continuing on litellm's tiny default
// budget, which is exactly the silent over-fill this file exists to
// prevent. Both files are needed: the metadata alone changes aider's
// internal arithmetic (context budgeting, history trimming) but not the
// wire request, so the completion cap must also ride
// extra_params.max_tokens. Standard litellm params (temperature, top_p,
// presence_penalty, repetition_penalty) sit at extra_params' top level;
// the provider-specific top_k/min_p sit under extra_body — litellm's
// syntax for params it does not map natively.
func stageAiderModelFiles(worktreePath, prefixedModel string, maxInputTokens, maxOutputTokens int, sampler aiderSampler) error {
	metadata, err := json.Marshal(map[string]any{
		prefixedModel: map[string]int{
			"max_input_tokens":  maxInputTokens,
			"max_output_tokens": maxOutputTokens,
		},
	})
	if err != nil {
		return err
	}
	var settings strings.Builder
	fmt.Fprintf(&settings, "- name: %s\n  extra_params:\n    max_tokens: %d\n",
		prefixedModel, maxOutputTokens)
	if sampler.TopP != nil {
		fmt.Fprintf(&settings, "    top_p: %s\n", yamlFloat(*sampler.TopP))
	}
	if sampler.Temperature != nil {
		fmt.Fprintf(&settings, "    temperature: %s\n", yamlFloat(*sampler.Temperature))
	}
	if sampler.PresencePenalty != nil {
		fmt.Fprintf(&settings, "    presence_penalty: %s\n", yamlFloat(*sampler.PresencePenalty))
	}
	if sampler.RepetitionPenalty != nil {
		fmt.Fprintf(&settings, "    repetition_penalty: %s\n", yamlFloat(*sampler.RepetitionPenalty))
	}
	if sampler.TopK != nil || sampler.MinP != nil {
		settings.WriteString("    extra_body:\n")
		if sampler.TopK != nil {
			fmt.Fprintf(&settings, "      top_k: %s\n", yamlFloat(*sampler.TopK))
		}
		if sampler.MinP != nil {
			fmt.Fprintf(&settings, "      min_p: %s\n", yamlFloat(*sampler.MinP))
		}
	}
	dir := filepath.Join(worktreePath, ".daedalus-aider")
	if err := os.WriteFile(filepath.Join(dir, filepath.Base(aiderModelMetadataArg)), metadata, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, filepath.Base(aiderModelSettingsArg)), []byte(settings.String()), 0o644)
}

// logDeath records post-mortem telemetry for a jailed round that died to
// a signal: which signal (daedalus's own kill paths fire only at activity
// timeouts and cancel via context, so a signal here means something
// outside daedalus killed the group leader), what survived in the process
// group after the leader died (escaped descendants, with their memory
// footprint), and how long the round ran. Logged before killGroup sweeps,
// so the snapshot shows the survivors.
func logDeath(logger log.Logger, cmd *exec.Cmd, err error, start time.Time, sig string) {
	logger.Error("Jailed agent died abruptly",
		"Error", err,
		"Exit", sig,
		"PGID", cmd.Process.Pid,
		"Elapsed", time.Since(start).Round(time.Second),
		"GroupAfterDeath", groupSnapshot(cmd.Process.Pid))
}

// deathSignal describes the exit of a failed command when it was killed by
// a signal rather than exiting on its own, "" otherwise. Only abrupt
// deaths are worth post-mortem telemetry.
func deathSignal(err error) string {
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		return ""
	}
	ws, ok := ee.Sys().(syscall.WaitStatus)
	if !ok || !ws.Signaled() {
		return ""
	}
	return fmt.Sprintf("signal %d (%s)", ws.Signal(), ws.Signal())
}

// snapshotTimeout bounds a process-group snapshot: telemetry must never
// wedge the activity it is trying to explain.
const snapshotTimeout = 2 * time.Second

// groupSnapshot lists the surviving members of process group pgid (pid,
// parent, RSS in KB, command) for post-mortem telemetry. macOS has no
// /proc, so the whole table is listed and filtered. Best-effort: any
// failure yields "".
func groupSnapshot(pgid int) string {
	if pgid <= 0 {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ps", "-axo", "pid=,ppid=,pgid=,rss=,command=").Output()
	if err != nil {
		return ""
	}
	want := strconv.Itoa(pgid)
	var lines []string
	for line := range strings.SplitSeq(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 4 && fields[2] == want {
			lines = append(lines, strings.TrimSpace(line))
		}
	}
	return strings.Join(lines, "\n")
}

// apiExhaustionMarkers are the phrases provider APIs (and their CLIs) emit
// when the account is out of quota, rate-limited, or the service is
// overloaded. Matching them labels the halt so operators can tell "resume
// with `daedalus continue` later" apart from a code failure.
var apiExhaustionMarkers = []string{
	"rate limit", "rate_limit", "quota", "credit balance", "insufficient", "usage limit", "402", "429", "overloaded",
}

// apiErrorTextMarkers are the plain-text faces of an API-level failure —
// the litellm error classes and connection failures aider (and, in plain
// text mode, opencode and codex) print into their output when the provider fails,
// with no structured event and often a clean exit 0. They extend the
// exhaustion match on a failed round's output and are the only
// classification a clean-exiting plain-text round can get (see the
// success-path check in runJailedRoundFolders). The structured faces
// (claude/amp result JSON, pi json stream) never depend on these — text an
// agent printed cannot decide their rounds.
var apiErrorTextMarkers = []string{
	"litellm.internalservererror",
	"litellm.serviceunavailableerror",
	"litellm.apitimeouterror",
	"litellm.ratelimiterror",
	"api connection error",
}

// errAgentStart marks a jailed round whose agent process never launched —
// ai-jail itself failed to spawn. It says nothing about any conversation,
// so the session-resume fallback must not treat it as a dead session.
// Unexported: it only feeds brokenResume's exclusion set in this package.
var errAgentStart = errors.New("ai-jail agent start failed")

// ErrAPIExhausted marks an agent or reviewer run that failed because the
// provider API is out of quota or unavailable. Activities do not retry;
// the workflow heartbeats — sleeping an hour and retrying the same round,
// up to five times — and parks the run for a maintainer restart once the
// API is still exhausted past that. Either way the work is preserved on
// the aborted/ branch and the run is resumable via `daedalus continue`.
var ErrAPIExhausted = errors.New("agent api exhausted or unavailable")

// ErrAgentSlotsBusy marks a jailed round that queued for a concurrency
// slot past slotWaitTimeout and gave up without launching the agent. The
// workflow answers it like the quota heartbeat, not like a timeout: back
// off briefly and re-queue the same round unchanged — a timeout would be
// miscounted against the streak and answered with a continuation prompt
// for partial work that never happened. Its text deliberately avoids every
// apiExhaustionMarker so isAPIExhaustion cannot claim it.
var ErrAgentSlotsBusy = errors.New("all jailed-agent slots busy")

// ErrAgentKilled marks a jailed round whose process died to a signal —
// an abrupt death without a report: an external kill, or one of daedalus's
// own group kills (the activity-timeout context kill, or the shutdown
// drain's kill once the restart grace expired — see waitCommand; in both
// the ExitError from the SIGKILL outranks the context error in cmd.Wait).
// Derived from the wait status in-worker, never from output text, so the
// workflow can trust the classification: it answers a killed round with a
// continuation from partial work, like a timeout — which is also how a
// worker-status recapture classifies a round its dead worker never
// reported. The wrap carries no output — the workflow must not classify on
// text an agent printed.
var ErrAgentKilled = errors.New("jailed agent killed by signal")

// matchesAny reports whether s contains any marker, case-insensitively.
func matchesAny(s string, markers []string) bool {
	s = strings.ToLower(s)
	return slices.ContainsFunc(markers, func(m string) bool {
		return strings.Contains(s, m)
	})
}

// activityLogger returns the activity-scoped logger, falling back to a
// discard logger outside a real activity context (unit tests invoke
// activities directly; the SDK panics in that case by design).
func activityLogger(ctx context.Context) (l log.Logger) {
	l = log.NewStructuredLogger(slog.New(slog.DiscardHandler))
	defer func() { recover() }()
	if al := activity.GetLogger(ctx); al != nil {
		l = al
	}
	return l
}

// pipeDrainDelay bounds how long Cmd.Wait keeps waiting once the command has
// exited or been killed: a descendant that escaped the process group (setsid)
// can inherit and hold the output pipes, and without this deadline the copy
// goroutines block on it forever — leaking the whole activity goroutine past
// every Temporal timeout.
const pipeDrainDelay = 5 * time.Second

// setProcessGroup puts the subprocess in its own process group and arranges
// for the whole group to be SIGKILLed on context cancellation, so a timeout
// cannot orphan the subprocess's children; pipeDrainDelay keeps Wait from
// hanging on a descendant that slipped out of the group with the pipes.
// Callers pair it with killGroup, which sweeps the group when the round
// finishes (see killGroup for why cancellation alone is not enough).
func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	cmd.WaitDelay = pipeDrainDelay
}

// killGroup SIGKILLs the command's whole process group and is deferred at
// every spawn site so the round leaves no descendants behind, however the
// child exited. Cancellation-only killing is not enough: a child that dies
// or exits on its own — a crashed jail, a test runner that backgrounded
// workers and quit — leaves its children running in the group, orphaned.
// After the direct child is gone the sweep is harmless: the group is empty
// (ESRCH) or holds only descendants, and the pid-recycling window between
// Wait returning and this kill is nanoseconds.
func killGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}

// isWaitDelay reports whether err is exec.ErrWaitDelay, which Wait returns
// only when the child itself exited successfully but a leftover descendant
// was still holding the output pipes when WaitDelay expired. That is a
// success for the round — the collected output stands and killGroup sweeps
// the descendant — not a failure.
func isWaitDelay(err error) bool {
	return errors.Is(err, exec.ErrWaitDelay)
}

// shutdownDrainGrace is how long an in-flight child command keeps running
// on its own after the activity worker begins shutting down (a worker
// restart's SIGTERM) before it is killed and reaped so the round's outcome
// still reaches Temporal. The kill's worst-case teardown — pipeDrainDelay
// for a pipe-holding escaped descendant, then logDeath's groupSnapshot —
// must fit with it inside the daemon's WorkerStopTimeout
// (cmd/daedalus workerStopGrace, 30s), leaving margin for the result or
// failure RPC to flush: 20s + 5s + 2s ≈ 27s. A var so tests can shorten it.
var shutdownDrainGrace = 20 * time.Second

// workerStopChannel returns the channel closed when the activity worker is
// stopping, nil outside a real activity context (unit tests invoke
// activities directly; the SDK panics in that case by design).
func workerStopChannel(ctx context.Context) <-chan struct{} {
	defer func() { recover() }()
	return activity.GetWorkerStopChannel(ctx)
}

// waitCommand waits for a started command the way cmd.Wait does. When the
// activity worker begins shutting down, the command keeps
// shutdownDrainGrace to finish on its own — a near-done round still reports
// success — then the whole process group is SIGKILLed and reaped, so the
// activity returns through its normal error classification and Temporal
// records an outcome instead of holding a silent ghost attempt for a worker
// identity that no longer exists (the wedge that orphaned jailed rounds on
// every restart before the drain existed).
func waitCommand(ctx context.Context, cmd *exec.Cmd) error {
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	stop := workerStopChannel(ctx)
	if stop == nil {
		return <-waitDone
	}
	timer := time.NewTimer(shutdownDrainGrace)
	defer timer.Stop()
	select {
	case err := <-waitDone:
		return err
	case <-ctx.Done():
		// Activity-level cancellation (a round timeout, a server cancel):
		// CommandContext's Cancel kills the group; reap the exit here.
		return <-waitDone
	case <-stop:
		// The worker is shutting down: let the round finish within the
		// drain grace, then kill it so its outcome lands inside the
		// daemon's WorkerStopTimeout window.
		select {
		case err := <-waitDone:
			return err
		case <-ctx.Done():
			return <-waitDone
		case <-timer.C:
			if cmd.Process != nil {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
			return <-waitDone
		}
	}
}
