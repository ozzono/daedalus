// Per-round metrics-relay staging: when the worker enabled metrics it
// exported the relay roots (config.MetricsRelayAnthropicEnv /
// MetricsRelayOpenAIEnv) and registered its upstreams — including the
// real API keys — with the metrics daemon, minting the registration's
// secret restoration token itself and exporting it on
// config.MetricsRelayTokenEnv (the relay stores it, never returns it).
// metricsRelayEnv rewrites a round's
// provider environment so the round dials the relay instead of
// its provider (staged path /relay/<worker>/<wire><upstream-path>) and,
// on a keyed round, swaps the credential slot for the round's synthetic
// attribution key task-<workflow-id>-role-<role>-round-<activity-id>
// with the registration token as its -rk- tail — the worker's
// attribution encoding, which the relay reads for the ledger and, on the
// token matching the registration, restores the registered real key
// from, never second-guessing the parts. An unkeyed round (OAuth pass-through) keeps its own
// credentials untouched and attributes at worker level. Rounds whose
// endpoint was explicitly overridden — fallback, the reviewer section —
// had the relay vars removed upstream of here (see fallbackEnv /
// reviewerEnv: the registry's upstreams are the primary's, so staging a
// round dialed elsewhere would silently misroute it back to the primary)
// and stage nothing.
package activities

import (
	"context"
	"strings"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
	"go.temporal.io/sdk/activity"
)

// metricsRelayEnv stages a round's provider environment onto the metrics
// relay. Called from runJailedRoundFolders right after reviewerEnv and
// wireThinkingEnv, before any provider staging reads the env; a no-op
// when the relay vars are absent (metrics disabled, or an override
// removed them upstream).
func metricsRelayEnv(ctx context.Context, env []string, role SessionRole) []string {
	// One relay root serves both wires; either var's presence means the
	// worker staged metrics on.
	root := envLookup(env, config.MetricsRelayAnthropicEnv)
	if root == "" {
		root = envLookup(env, config.MetricsRelayOpenAIEnv)
	}
	if root == "" {
		return env
	}
	worker := envLookup(env, "DAEDALUS_WORKER_NAME")
	if worker == "" {
		// No registry segment to stage under — leave the round on its
		// direct provider path.
		return env
	}
	key := metricsAttributionKey(ctx, role)
	// The registration token rides the synthetic key's tail (the relay's
	// parseAttribution splits it back off): the credential slot is the one
	// channel a jailed CLI reliably presents — headers are the CLI's own
	// construction from env, and no CLI takes a custom one — and it is
	// what gates the relay's key restoration. Without it (a worker
	// registered against a daemon predating the token) the bare shape
	// stages and a token-checking relay refuses to restore.
	if token := envLookup(env, config.MetricsRelayTokenEnv); key != "" && token != "" {
		key += "-rk-" + token
	}
	// Anthropic wire: claude-family rounds.
	if base := envLookup(env, "ANTHROPIC_BASE_URL"); base != "" {
		if staged := metricsrelay.StagedBase(root, worker, metricsrelay.WireAnthropic, base); staged != "" {
			env = setEnvVar(env, "ANTHROPIC_BASE_URL", staged)
			// Keyed round: the synthetic key rides the credential slot
			// for the relay to read; an unkeyed round keeps its own
			// (absent — the CLI then uses its ambient OAuth token).
			if key != "" && envLookup(env, "ANTHROPIC_API_KEY") != "" {
				env = setEnvVar(env, "ANTHROPIC_API_KEY", key)
			}
		}
	}
	// OpenAI wire: pi, aider, codex-family rounds. OPENAI_API_BASE rides
	// the same value as OPENAI_BASE_URL when present (litellm versions
	// disagree on which they honor — see fallbackEnv).
	base := envLookup(env, "OPENAI_BASE_URL")
	if base == "" {
		base = envLookup(env, "OPENAI_API_BASE")
	}
	if base != "" {
		if staged := metricsrelay.StagedBase(root, worker, metricsrelay.WireOpenAI, base); staged != "" {
			env = setEnvVar(env, "OPENAI_BASE_URL", staged)
			if envLookup(env, "OPENAI_API_BASE") != "" {
				env = setEnvVar(env, "OPENAI_API_BASE", staged)
			}
			if key != "" && envLookup(env, "OPENAI_API_KEY") != "" {
				env = setEnvVar(env, "OPENAI_API_KEY", key)
			}
		}
	}
	return env
}

// metricsAttributionKey builds the synthetic per-round API key the relay
// parses for the ledger. The activity id is Temporal's per-schedule id —
// stable across retries, unique per round, so it is the round number.
// "" when identity is unavailable (outside a real activity context —
// tests): the round then stages the relay path only and attributes at
// worker level.
func metricsAttributionKey(ctx context.Context, role SessionRole) string {
	workflowID, _ := taskLogIdentity(ctx)
	activityID := metricsActivityID(ctx)
	if workflowID == "" || activityID == "" {
		return ""
	}
	name := string(role)
	if name == "" {
		name = "none"
	}
	// The delimiters are the relay's parse anchors (see
	// metricsrelay.parseAttribution): the workflow id is scrubbed of them
	// so the first "-role-" always starts the role part.
	return "task-" + strings.NewReplacer("-role-", "_role_", "-round-", "_round_").Replace(workflowID) +
		"-role-" + name + "-round-" + activityID
}

// metricsActivityID recovers the scheduled activity's id; "" outside an
// activity context (taskLogIdentity's same recover discipline).
func metricsActivityID(ctx context.Context) string {
	defer func() { recover() }()
	return activity.GetInfo(ctx).ActivityID
}
