package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ozzono/daedalus/internal/config"
)

// hands staging: the standalone hands harness reads its provider wiring
// from one strict-schema config file at $HOME/.config/hands/config.yaml —
// provider-only by design ("task prompts, roles, and verdict policy stay
// with the caller", unknown fields are load errors), with no CLI flag, no
// env channel, and no XDG override (probe-verified 2026-10-25 against the
// installed binary: config init honors only HOME). daedalus therefore
// stages a per-round config into a per-worktree scratch dir and mounts it
// onto the real path inside the jail (handsJailMounts) — the same dir
// then carries hands's session store, so a worktree's fix rounds resume
// their conversations and the host-side staleness check reads the same
// rows (handsTranscriptExists). The openai section wins when both
// provider families are exported (the staged-config-agent convention —
// pi, codex, opencode, and aider all stage from it); the anthropic family
// serves when no openai endpoint exists, because hands speaks both wires
// itself (provider.type, probed live: the openai face dials
// <url>/v1/chat/completions, the anthropic face <url>/v1/messages — both
// on a /v1-less base, so an openai URL staged from the shared section is
// stripped of its /v1 tail, the shape the pi-style convention carries, to
// keep every openai-wire agent dialing the same URL from one config).

// handsScratchDir is the worktree scratch dir holding the staged config
// and hands's session store (<dir>/.config/hands/); excludeAgentArtifacts
// keeps it out of git's sight (same pattern as .daedalus-aider/ and
// .daedalus-opencode/). The dot-name keeps the worktree root visually
// clean and the name mirrors the mounted path inside the jail.
const handsScratchDir = ".daedalus-hands"

// handsStateHome returns the scratch dir that stands in for HOME where
// hands's own state is read host-side (the session-staleness check): the
// jail mounts <scratch>/.config/hands onto ~/.config/hands, so pointing
// HOME at the scratch makes a host-side hands resolve the same store the
// jailed rounds wrote.
func handsStateHome(worktreePath string) string {
	return filepath.Join(worktreePath, handsScratchDir)
}

// handsWireDefaults are the numeric defaults `hands config init` writes
// (probe-recorded 2026-10-25). They are staged explicitly — not left to
// hands's loader — because zero and unset are indistinguishable to a yaml
// decode, and a literal zero is meaningless for each of them (an instant
// timeout, a zero window, an always-warning threshold); pinning hands's
// own written defaults keeps the staged file byte-equivalent to what the
// binary itself scaffolds.
const (
	handsDefaultTimeoutMS     = 480000
	handsDefaultContextTokens = 200000
	handsDefaultMaxOutTokens  = 16384
	handsDefaultWarnFrac      = "0.70"
)

// stageHandsProvider writes one round's hands config from the round's
// provider env into the worktree scratch dir. The family resolution
// follows the staged-config-agent convention: any openai export selects
// the openai wire (and then url and model are both required — a
// half-specified section must fail the round before launch rather than
// let it dial a cloud default, the exact posture of stagePiProvider and
// stageOpencodeProvider); no openai export falls to the anthropic family,
// where model is required and url is optional (absent means hands's own
// public default, matching claude's key-alone posture). The sampler knobs
// have no hands config field, so they never stage and the backend's own
// defaults stand, per the agnostic rule.
func stageHandsProvider(env []string, worktreePath string) error {
	url := envLookup(env, "OPENAI_BASE_URL")
	if url == "" {
		url = envLookup(env, "OPENAI_API_BASE")
	}
	key := envLookup(env, "OPENAI_API_KEY")
	model := envLookup(env, "OPENAI_MODEL")
	wire := "openai"
	if url == "" && key == "" && model == "" {
		wire = "anthropic"
		url = envLookup(env, "ANTHROPIC_BASE_URL")
		key = envLookup(env, "ANTHROPIC_API_KEY")
		model = envLookup(env, "ANTHROPIC_MODEL")
		if model == "" {
			return fmt.Errorf("hands round: no provider section exported (neither OPENAI_* nor an ANTHROPIC_MODEL) — hands reads its provider only from the staged config, and a round without one fails at launch; set a provider url/key/model, or run an agent with its own ambient wiring")
		}
	} else if url == "" || model == "" {
		return fmt.Errorf("hands round: the openai section is half-specified (url %q, model %q) — it cannot be staged and the round would fail at launch; set openai.url and openai.model, or unset the ambient OPENAI_* exports", url, model)
	}
	if wire == "openai" {
		// hands appends /v1/chat/completions to its base; the shared
		// openai.url carries the /v1 tail for the SDK-convention agents
		// (pi, aider). Strip one tail so every openai-wire agent dials the
		// same URL from one config. The relay composes correctly: its
		// staged prefix already carries the path, and the path hands
		// appends is what the relay forwards upstream.
		url = strings.TrimSuffix(strings.TrimRight(url, "/"), "/v1")
	}
	timeout := handsDefaultTimeoutMS
	if n, err := strconv.Atoi(strings.TrimSpace(envLookup(env, "API_TIMEOUT_MS"))); err == nil && n > 0 {
		timeout = n
	}
	window := handsDefaultContextTokens
	if n, err := strconv.Atoi(envLookup(env, config.ContextTokensEnv)); err == nil && n > 0 {
		window = n
	}
	maxOut := handsDefaultMaxOutTokens
	if n, err := strconv.Atoi(envLookup(env, config.MaxOutputTokensEnv)); err == nil && n > 0 {
		maxOut = n
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# staged per round by daedalus — the provider wiring hands dials;\n# task prompts, roles, and verdict policy travel in the round prompt.\nprovider:\n  type: %s\n", wire)
	if url != "" {
		// Quoted like the sibling fields: quoting stages the env-supplied
		// value as a literal YAML scalar — yaml.v3 (hands's loader,
		// verified via go version -m) truncates only at a whitespace-
		// preceded '#' (`https://llm.example/v1 #frag` loads as
		// https://llm.example/v1) and treats other characters (': ',
		// leading indicators, control chars) as structure; a bare
		// mid-word '#' does not truncate.
		fmt.Fprintf(&b, "  url: %s\n", strconv.Quote(url))
	}
	// The key lands on disk literally: hands shows no env-template
	// expansion (nothing in its staged schema or docs; probe-verified
	// default file), so the pi/opencode ${OPENAI_API_KEY} pattern has no
	// counterpart. The file sits in the git-excluded scratch dir at 0600 —
	// the exposure is the staged-file class, never a review diff or a
	// preserved branch (excludeAgentArtifacts).
	if key != "" {
		fmt.Fprintf(&b, "  key: %s\n", strconv.Quote(key))
	}
	fmt.Fprintf(&b, "  model: %s\n", strconv.Quote(model))
	fmt.Fprintf(&b, "  timeout_ms: %d\n  context_tokens: %d\n  max_output_tokens: %d\n", timeout, window, maxOut)
	if envLookup(env, config.WireThinkingEnv) == "off" {
		b.WriteString("  thinking: off\n")
	}
	b.WriteString("context:\n  import: []\nrun:\n  budget_usd_cents: 0\n  warn_frac: " + handsDefaultWarnFrac + "\n")
	if err := excludeAgentArtifacts(worktreePath, handsScratchDir); err != nil {
		return fmt.Errorf("prepare hands scratch dir: %w", err)
	}
	// hands also keeps worktree-local context under .hands/ (operator
	// territory daedalus stages nothing into); exclude it the same way so
	// a hands-created scratchpad can never reach a review diff.
	if err := excludeAgentArtifacts(worktreePath, ".hands"); err != nil {
		return fmt.Errorf("prepare hands context dir: %w", err)
	}
	path := filepath.Join(handsStateHome(worktreePath), ".config", "hands", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(b.String()), 0o600)
}

// handsJailMounts resolves the host paths a jailed hands round needs
// mapped into the jail and returns them as ai-jail arguments: the staged
// config dir (<worktree>/.daedalus-hands/.config/hands, written by
// stageHandsProvider before this runs) mounted read-write onto the real
// ~/.config/hands, so the child reads its staged provider and its session
// store lands back in the scratch dir — per-worktree resume state that
// survives a worker restart and stays out of git. The destination is
// created host-side before spawn (an empty dir is what hands's first
// round would create anyway), so the mount cannot fail on it, and ai-jail
// itself failing the mount exits nonzero before the agent runs, which the
// wait in runJailedRoundFolders already surfaces as a failed round; there
// is no proceed-unmounted path. HOME itself is never redirected: the jail
// derives its home blank-slate from the environment it is spawned with,
// and feeding it a staged HOME would repoint jail-internal resolution
// this sandbox has no binary to probe (the codexJailMounts posture).
// ponytail: assembled but never probed against a real ai-jail — that the
// extra --rw-map nests inside the --worktree mount and composes with the
// folder-grant maps is the same verification class as those blocks; probe
// on the first live host, alongside them.
func handsJailMounts(worktreePath string) ([]string, error) {
	src := filepath.Join(handsStateHome(worktreePath), ".config", "hands")
	if err := os.MkdirAll(src, 0o700); err != nil {
		return nil, fmt.Errorf("prepare hands staged config dir %s: %w", src, err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home directory: %w", err)
	}
	dst := filepath.Join(home, ".config", "hands")
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return nil, fmt.Errorf("prepare hands state dir %s: %w", dst, err)
	}
	// Same invariant FolderMounts and the bug-mirror block enforce for
	// their --rw-map specs: a colon in either path splits the spec at the
	// wrong boundary.
	for _, p := range []string{src, dst} {
		if strings.ContainsRune(p, ':') {
			return nil, fmt.Errorf("hands state path %s must not contain %q — it composes into the jail's --rw-map <source>:<dest> mount spec", p, ":")
		}
	}
	return []string{"--rw-map", src + ":" + dst}, nil
}

// handsExportTimeout bounds one host-side `hands export` invocation — the
// session-staleness check shells out to it once per resumed round, and a
// wedged hands must cost the staleness check, not the round (the
// opencodeSessionList posture).
const handsExportTimeout = 10 * time.Second

// handsTranscriptExists reports whether hands's store still holds session
// id in this worktree's scratch state — the staleness check for a
// recorded hands session. The check shells out to `hands export <id>`
// with HOME pointed at the scratch dir (the mounted store's host path)
// and reads only the exit code: the export's row format is hands's own
// and no daedalus decision needs its body. A LookPath miss, a timeout, or
// a nonzero exit reports false — a recorded session that cannot be
// exported is stale by the only definition that matters (its resume would
// fail the same way), and the caller starts fresh.
func handsTranscriptExists(worktreePath, id string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), handsExportTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "hands", "export", id)
	cmd.Env = []string{"HOME=" + handsStateHome(worktreePath), "PATH=" + os.Getenv("PATH")}
	return cmd.Run() == nil
}

// handsSessionID extracts the session id a hands round minted, from the
// round's captured stdout: the started event's payload.session — a uuid
// distinct from round_id, emitted before the first model turn (the
// transcript watchers have no hands face; this stream is the only source).
// The last started event wins, though one round mints exactly one. A line
// that does not parse, or a stream with no started event (a round killed
// before hands wrote anything), reports false and nothing is recorded.
func handsSessionID(stdout string) (string, bool) {
	id := ""
	for _, line := range strings.Split(stdout, "\n") {
		var ev handsEvent
		if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &ev); err != nil || ev.Kind != "started" || ev.Payload.Session == "" {
			continue
		}
		id = ev.Payload.Session
	}
	return id, id != ""
}
