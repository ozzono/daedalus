// Per-worker provider availability and failover: which of the primary and
// fallback providers is currently serving jailed rounds, when a benched
// provider may be retried, and how a round's environment is built for
// either side. The state is in-memory — a restarted worker re-learns it
// from the first failed round — and the fallback's settings reach this
// process the same env-only channel as the primary's
// (DAEDALUS_FALLBACK_* exported by runWorker from config.yaml's
// top-level fallback section).
package activities

import (
	"os"
	"sync"
	"time"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/provider"
)

// fallbackRetryFloor is the remaining activity budget below which an
// exhausted primary round is not retried on the fallback: a round
// truncated at the deadline dies to our own context kill and would
// masquerade as ErrAgentKilled, misrouting the workflow. Better to return
// the exhaustion and let the workflow's hourly loop retry.
const fallbackRetryFloor = 5 * time.Minute

// dryHolds escalate how long a provider stays benched when its exhaustion
// output carries no parseable reset time: 20m after the first dry round,
// 40m after the second, then 60m each. The schedule bounds how eagerly a
// recovering provider is rechecked without burning full agent rounds on
// one that is still dry.
var dryHolds = []time.Duration{20 * time.Minute, 40 * time.Minute, time.Hour}

// dryHoldBudget caps the cumulative benched time the escalating schedule
// will hand a provider that never parses (5h): past it the holds stop
// extending, so rounds recheck the provider directly rather than benching
// it forever.
const dryHoldBudget = 5 * time.Hour

// providerAvailability is one provider's bench state. The zero value is
// fully available.
type providerAvailability struct {
	// downUntil is when the provider may serve rounds again; the zero
	// value means now (up).
	downUntil time.Time
	// holdStep indexes dryHolds for the next escalating hold.
	holdStep int
	// drySince anchors the cumulative dryHoldBudget accounting.
	drySince time.Time
}

// up reports whether the provider may serve a round at t.
func (p *providerAvailability) up(t time.Time) bool {
	return !t.Before(p.downUntil)
}

// providerState tracks both sides for this worker process.
var providerState = struct {
	sync.Mutex
	primary  providerAvailability
	fallback providerAvailability
}{}

// providerSide names which provider a round runs on.
type providerSide string

const (
	sidePrimary  providerSide = "primary"
	sideFallback providerSide = "fallback"
)

// fallbackConfigured reports whether the worker environment carries a
// usable fallback (config fallback.enabled with url, key, and model).
// Without one, rounds always run on the primary exactly as they did
// before failover existed — holds would only fast-fail rounds with
// nothing to fail over to.
func fallbackConfigured() bool {
	return os.Getenv("DAEDALUS_FALLBACK_BASE_URL") != "" &&
		os.Getenv("DAEDALUS_FALLBACK_API_KEY") != "" &&
		os.Getenv("DAEDALUS_FALLBACK_MODEL") != ""
}

// selectProvider picks the provider for the next round: the primary
// whenever it is up (so a fallback stint ends automatically once the
// primary's hold expires), else the fallback, else neither (nil env).
func selectProvider() ([]string, providerSide) {
	if !fallbackConfigured() {
		return os.Environ(), sidePrimary
	}
	providerState.Lock()
	defer providerState.Unlock()
	now := time.Now()
	if providerState.primary.up(now) {
		return os.Environ(), sidePrimary
	}
	if providerState.fallback.up(now) {
		return fallbackEnv(), sideFallback
	}
	return nil, sidePrimary
}

// otherProviderEnv returns the environment of the other side, but only
// when that provider is up and (for the fallback side) configured — the
// candidate for an immediate failover retry.
func otherProviderEnv(side providerSide) ([]string, providerSide) {
	if !fallbackConfigured() {
		return nil, side
	}
	providerState.Lock()
	defer providerState.Unlock()
	now := time.Now()
	if side == sidePrimary {
		if providerState.fallback.up(now) {
			return fallbackEnv(), sideFallback
		}
		return nil, side
	}
	if providerState.primary.up(now) {
		return os.Environ(), sidePrimary
	}
	return nil, side
}

// markProviderDry benches a provider that just failed a round exhausted.
// A parseable quota-reset stamp sets the hold to it (never earlier than
// now); otherwise the escalating dryHolds schedule applies, cumulative
// holds capped at dryHoldBudget. Returns the new downUntil for logging.
func markProviderDry(side providerSide, roundOutput string) time.Time {
	providerState.Lock()
	defer providerState.Unlock()
	p := &providerState.primary
	if side == sideFallback {
		p = &providerState.fallback
	}
	now := time.Now()
	var hold time.Duration
	if reset := provider.ParseQuotaReset(roundOutput); !reset.IsZero() {
		hold = max(reset.Sub(now), 0)
	} else {
		if p.drySince.IsZero() {
			p.drySince = now
		}
		step := dryHolds[min(p.holdStep, len(dryHolds)-1)]
		if spent := now.Sub(p.drySince); spent+step > dryHoldBudget {
			step = max(dryHoldBudget-spent, 0)
		}
		hold = step
		p.holdStep++
	}
	p.downUntil = now.Add(hold)
	return p.downUntil
}

// fallbackEnv builds a round's environment for the fallback provider: the
// worker environment with the primary's provider settings overridden by
// the fallback's. The fallback's type picks the vars its values travel
// on: an openai-style fallback sets OPENAI_BASE_URL/OPENAI_API_KEY/
// OPENAI_MODEL (leaving the primary's ANTHROPIC_* untouched — so it serves
// only agents that dial OPENAI_BASE_URL; claude keeps dialing the primary
// and cannot use it); the anthropic default overrides
// ANTHROPIC_BASE_URL/ANTHROPIC_API_KEY/ANTHROPIC_MODEL and sets the
// small/fast model to the fallback's heartbeat model — falling back to its
// main model, or cleared entirely when neither is set so the agent's own
// default applies.
func fallbackEnv() []string {
	if os.Getenv("DAEDALUS_FALLBACK_TYPE") == config.FallbackTypeOpenAI {
		env := setEnvVar(os.Environ(), "OPENAI_BASE_URL", os.Getenv("DAEDALUS_FALLBACK_BASE_URL"))
		// OPENAI_API_BASE rides the same value as OPENAI_BASE_URL (litellm
		// versions disagree on which they honor) — a stale primary value
		// must not survive the override.
		env = setEnvVar(env, "OPENAI_API_BASE", os.Getenv("DAEDALUS_FALLBACK_BASE_URL"))
		env = setEnvVar(env, "OPENAI_API_KEY", os.Getenv("DAEDALUS_FALLBACK_API_KEY"))
		// The slim tool relay's upstream is pinned to the primary's
		// openai.url (fixed at worker boot), so it must not route a
		// fallback round — the relay var loses to any explicit endpoint
		// override (removed here and in reviewerEnv), putting the round
		// back on the overridden OPENAI_BASE_URL directly. The metrics
		// relay vars lose the same way: the registration's upstreams are
		// the primary's too, and metricsRelayEnv staging a fallback round
		// would silently misroute it to the primary.
		env = removeEnvVar(env, config.ToolRelayURLEnv)
		env = removeEnvVar(env, config.MetricsRelayAnthropicEnv)
		env = removeEnvVar(env, config.MetricsRelayOpenAIEnv)
		return setEnvVar(env, "OPENAI_MODEL", os.Getenv("DAEDALUS_FALLBACK_MODEL"))
	}
	hb := os.Getenv("DAEDALUS_FALLBACK_HEARTBEAT_MODEL")
	if hb == "" {
		hb = os.Getenv("DAEDALUS_FALLBACK_MODEL")
	}
	env := setEnvVar(os.Environ(), "ANTHROPIC_BASE_URL", os.Getenv("DAEDALUS_FALLBACK_BASE_URL"))
	env = setEnvVar(env, "ANTHROPIC_API_KEY", os.Getenv("DAEDALUS_FALLBACK_API_KEY"))
	env = setEnvVar(env, "ANTHROPIC_MODEL", os.Getenv("DAEDALUS_FALLBACK_MODEL"))
	if hb == "" {
		env = removeEnvVar(env, "ANTHROPIC_DEFAULT_HAIKU_MODEL")
	} else {
		env = setEnvVar(env, "ANTHROPIC_DEFAULT_HAIKU_MODEL", hb)
	}
	// The metrics relay vars lose here too (see the openai branch): the
	// registry's anthropic upstream is the primary's, and staging the
	// overridden ANTHROPIC_BASE_URL onto it would route the fallback
	// round back to the primary.
	env = removeEnvVar(env, config.MetricsRelayAnthropicEnv)
	env = removeEnvVar(env, config.MetricsRelayOpenAIEnv)
	return env
}

// reviewRole reports whether the role is one of the review conversations —
// the roles that never edit and stage from the reviewer chain. The pi edit
// guardrail and the per-role provider resolution both route on it.
func reviewRole(role SessionRole) bool {
	return role == RoleDevReview || role == RoleTestReview || role == RoleFinalReview
}

// applyRoleOverride writes one resolved role override onto the round env:
// both wire families' endpoint/key/model vars — the section serves
// regardless of which provider is up, and the model override covers both
// families so every staging path (claude's env, aider's --model, the
// pi/codex/opencode staged model entries) picks it up from the round env —
// plus the context-window var the stagings read (pi's ContextWindow,
// aider's staged metadata input side, opencode's limit.context, claude's
// own var). All four values resolved: empty leaves that dimension
// untouched. An endpoint or key override removes the relay vars — the
// key-only form included: the relays' upstreams are the primary's, so a
// round left on the metrics relay would have its key swapped for the
// synthetic attribution key (billed to the primary), and left on the slim
// tool relay it would forward the role's key to the primary-pinned openai
// upstream. A model-only or window-only override keeps them (the relays'
// upstream is exactly the endpoint such a round still dials).
func applyRoleOverride(env []string, url, key, model, window string) []string {
	if url == "" && key == "" && model == "" && window == "" {
		return env
	}
	if url != "" || key != "" {
		env = removeEnvVar(env, config.ToolRelayURLEnv)
		env = removeEnvVar(env, config.MetricsRelayAnthropicEnv)
		env = removeEnvVar(env, config.MetricsRelayOpenAIEnv)
	}
	if url != "" {
		env = setEnvVar(env, "ANTHROPIC_BASE_URL", url)
		// OPENAI_API_BASE rides the same value as OPENAI_BASE_URL, per
		// fallbackEnv.
		env = setEnvVar(env, "OPENAI_BASE_URL", url)
		env = setEnvVar(env, "OPENAI_API_BASE", url)
	}
	if key != "" {
		env = setEnvVar(env, "ANTHROPIC_API_KEY", key)
		env = setEnvVar(env, "OPENAI_API_KEY", key)
	}
	if model != "" {
		env = setEnvVar(env, "ANTHROPIC_MODEL", model)
		env = setEnvVar(env, "OPENAI_MODEL", model)
	}
	if window != "" {
		env = setEnvVar(env, config.ContextTokensEnv, window)
	}
	return env
}

// reviewerEnv pins a reviewer round's endpoint, key, model, and context
// window to the config's reviewer chain (ReviewerURLEnv / ReviewerKeyEnv /
// ReviewerModelEnv / ReviewerContextTokensEnv, exported by runWorker when
// reviewer.url/key/model/context_tokens are set): reviewers dial their own
// url/key and request their own model and window while dev and test rounds
// keep the serving provider's. Applied to the round's assembled
// environment — primary or fallback side alike — in runJailedRound, the
// one invoker every jailed round goes through. The slim flow's terminal
// review (RoleFinalReview, the last sub-task's review loop — the rounds
// that see the cumulative diff of the whole run) resolves the same chain
// with one precedence step: a final_reviewer field set wins, an unset
// field falls back to the reviewer section's, and an unset reviewer field
// leaves the serving value — the fallback chain, resolved per field, per
// round. All vars absent (the default) leaves the environment untouched.
func reviewerEnv(env []string, role SessionRole) []string {
	if !reviewRole(role) {
		return env
	}
	final := role == RoleFinalReview
	// pick resolves one dimension along the chain: the reviewer section's
	// value for a plain reviewer round; the final_reviewer section's value
	// when set — else the reviewer section's — for the terminal review.
	pick := func(reviewerVar, finalVar string) string {
		if final {
			if v := os.Getenv(finalVar); v != "" {
				return v
			}
			return os.Getenv(reviewerVar)
		}
		return os.Getenv(reviewerVar)
	}
	return applyRoleOverride(env,
		pick(config.ReviewerURLEnv, config.FinalReviewerURLEnv),
		pick(config.ReviewerKeyEnv, config.FinalReviewerKeyEnv),
		pick(config.ReviewerModelEnv, config.FinalReviewerModelEnv),
		pick(config.ReviewerContextTokensEnv, config.FinalReviewerContextTokensEnv),
	)
}

// devEnv pins a dev round's endpoint, key, model, and context window to
// the config's dev section (DevURLEnv / DevKeyEnv / DevModelEnv /
// DevContextTokensEnv, exported by runWorker when dev.url/key/model/
// context_tokens are set): the implementing agent's rounds — the slim
// flow's plan, parse, step, and fix rounds among them — dial their own
// url/key and request their own model and window while review roles keep
// theirs. Same application rules as reviewerEnv (both wire families,
// relay-strip on an endpoint or key override); every var absent (the
// default) leaves the serving environment untouched.
func devEnv(env []string, role SessionRole) []string {
	if role != RoleDev {
		return env
	}
	return applyRoleOverride(env,
		os.Getenv(config.DevURLEnv),
		os.Getenv(config.DevKeyEnv),
		os.Getenv(config.DevModelEnv),
		os.Getenv(config.DevContextTokensEnv),
	)
}

// wireThinkingEnv resolves the round's wire-level thinking knob into the
// round env (WireThinkingEnv) — the one channel the provider stagings read
// for it, since their signatures take the env alone. Each role keys on its
// own section's disable_thinking alone, and an unset knob sends nothing
// even when another section toggles (never-send-a-default) — with two
// chain steps: the terminal review (RoleFinalReview) falls back to the
// reviewer section's knob when its own is unset, and the dev role falls
// back to the openai section's, which is the dev rounds' pre-role behavior
// (review rounds fall back to nothing: each review role controls its own
// brain). The exports carry only the off value ("off"; runWorker's
// disable_thinking export), matched strictly like DAEDALUS_THINKING — an
// ambient garbage value is never read as an off. Set-or-removed every
// round, so a stale foreground export can never leak into a round whose
// config says nothing.
func wireThinkingEnv(env []string, role SessionRole) []string {
	name := config.OpenAIThinkingEnv
	switch {
	case role == RoleFinalReview:
		name = config.FinalReviewerThinkingEnv
		if os.Getenv(name) != "off" {
			// The fallback chain: the final reviewer's unset knob resolves
			// to the reviewer section's.
			name = config.ReviewerThinkingEnv
		}
	case reviewRole(role):
		name = config.ReviewerThinkingEnv
	case role == RoleDev:
		name = config.DevThinkingEnv
		if os.Getenv(name) != "off" {
			// The dev role's unset knob resolves to the serving section's —
			// exactly what dev rounds keyed on before the section existed.
			name = config.OpenAIThinkingEnv
		}
	}
	if os.Getenv(name) == "off" {
		return setEnvVar(env, config.WireThinkingEnv, "off")
	}
	return removeEnvVar(env, config.WireThinkingEnv)
}

// lookupEnv returns the value of name in env, "" when absent.
func lookupEnv(env []string, name string) string {
	for _, kv := range env {
		if n, v, ok := cutEnv(kv); ok && n == name {
			return v
		}
	}
	return ""
}

// slimAiderEnv is the slim-mode wiring for jailed aider rounds: aider's
// commit-message/chat-summary wire (the weak model) and its editor wire
// default to a cloud model — a real footgun on an airgapped self-hosted
// box, where those wires would make surprise paid-API calls. Under
// DAEDALUS_SLIM, both are pinned to the effective AIDER_MODEL — but only
// when AIDER_MODEL is actually set (it is an operator export, not daedalus
// config), and never overriding explicit AIDER_WEAK_MODEL/AIDER_EDITOR_MODEL
// the operator already exported. env is the round's assembled environment
// (os.Environ()-derived on either provider side), so the pinning applies to
// primary and failover rounds alike.
func slimAiderEnv(env []string) []string {
	if os.Getenv("DAEDALUS_SLIM") != "1" {
		return env
	}
	model := lookupEnv(env, "AIDER_MODEL")
	if model == "" {
		return env
	}
	if lookupEnv(env, "AIDER_WEAK_MODEL") == "" {
		env = setEnvVar(env, "AIDER_WEAK_MODEL", model)
	}
	if lookupEnv(env, "AIDER_EDITOR_MODEL") == "" {
		env = setEnvVar(env, "AIDER_EDITOR_MODEL", model)
	}
	return env
}

// roundEnvAllowlist filters an assembled round environment down to
// config.RoundEnvVars — the spawn gate in runJailedRoundFolders, run
// after every staging step has read its inputs from the inherited
// environment. Unlisted entries stop at the sandbox door: the jailed CLI
// sees the provider wires it dials, the overlay the builder staged, and
// the ambient base, and nothing else — no unselected family's key, no
// worker control knob, no ambient value.
func roundEnvAllowlist(env []string) []string {
	allow := make(map[string]bool)
	for _, name := range config.RoundEnvVars() {
		allow[name] = true
	}
	out := make([]string, 0, len(env))
	for _, kv := range env {
		if n, _, ok := cutEnv(kv); ok && allow[n] {
			out = append(out, kv)
		}
	}
	return out
}

// setEnvVar returns env with name set to value (appended when absent).
func setEnvVar(env []string, name, value string) []string {
	for i, kv := range env {
		if n, _, ok := cutEnv(kv); ok && n == name {
			env[i] = name + "=" + value
			return env
		}
	}
	return append(env, name+"="+value)
}

// removeEnvVar returns env with name removed.
func removeEnvVar(env []string, name string) []string {
	out := env[:0]
	for _, kv := range env {
		if n, _, ok := cutEnv(kv); ok && n == name {
			continue
		}
		out = append(out, kv)
	}
	return out
}

func cutEnv(kv string) (name, value string, ok bool) {
	for i := 0; i < len(kv); i++ {
		if kv[i] == '=' {
			return kv[:i], kv[i+1:], true
		}
	}
	return kv, "", false
}

// envLookup reads name from an explicit environment slice (the round's env,
// not the process's — the value the jailed child will actually see).
func envLookup(env []string, name string) string {
	for _, kv := range env {
		if n, v, ok := cutEnv(kv); ok && n == name {
			return v
		}
	}
	return ""
}
