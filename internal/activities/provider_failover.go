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
	return env
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
