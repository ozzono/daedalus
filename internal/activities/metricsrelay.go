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
// endpoint was explicitly overridden — fallback, the per-role provider
// sections (reviewer / dev / final-reviewer) — had the relay vars removed
// upstream of here (see fallbackEnv / applyRoleOverride: the registry's
// upstreams are the primary's, so staging a round dialed elsewhere would
// silently misroute it back to the primary) and stage nothing.
//
// metricsRoundEnv is the selector above it: with metrics enabled (the
// worker exported config.MetricsOtelEndpointEnv beside the relay roots)
// a claude or codex round whose binary probes at or above its version
// floor stages agent-native OTLP instead — claude the eight OTLP env
// names (claudeOtelEnv), codex the per-invocation -c overrides
// (codexOtelArgs) — and skips the relay rewrite: exactly one capture
// path per round, with the receiver folding those exports back into the
// same ledger (internal/metricsrelay's otel.go). The endpoint var is
// deliberately not stripped by the override paths (see its config
// comment): an otel-staged round dials the daemon directly, so fallback
// and override rounds keep telemetry coverage.
package activities

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
	"go.temporal.io/sdk/activity"
)

// metricsRoundEnv selects and stages the round's capture path: the
// agent-native OTLP path when the worker exported the otel endpoint
// (metrics on), the resolved agent is claude or codex, and the binary
// probes at or above its version floor — one capture path per round, so
// the staged round skips the relay rewrite entirely (claude's staging
// rides the env; codex's comes back as -c overrides for the argv build).
// Everything else stages the relay exactly as before. The one dark
// corner is the keyed-only fallback's pinned consequence: metrics on,
// an otel agent below its floor, and an unkeyed round (no activity
// identity — the synthetic attribution's contract) stages nothing and
// the round is invisible to the ledger, exactly as the design accepted.
func metricsRoundEnv(ctx context.Context, env []string, role SessionRole, agent string) ([]string, []string) {
	endpoint := envLookup(env, config.MetricsOtelEndpointEnv)
	if endpoint == "" || (agent != "claude" && agent != "codex") {
		return metricsRelayEnv(ctx, env, role), nil
	}
	if agentOTelCapable(agent) {
		if agent == "claude" {
			worker := envLookup(env, "DAEDALUS_WORKER_NAME")
			task, roleName, round := metricsAttributionParts(ctx, role)
			return claudeOtelEnv(env, endpoint, worker, task, roleName, round), nil
		}
		return env, codexOtelArgs(endpoint)
	}
	if metricsAttributionKey(ctx, role) == "" {
		return env, nil
	}
	return metricsRelayEnv(ctx, env, role), nil
}

// claudeOtelEnv stages the eight documented env names a metrics-on
// claude round exports to the receiver: the enable switch, the otlp
// metrics exporter over http/json pointed at the endpoint, the
// account-uuid cardinality trim, the explicitly-disabled traces and logs
// (nothing unexpected dials the endpoint), and the resource attributes
// the receiver's provenance check reads (D4) — the same parts the
// synthetic attribution key encodes plus the worker the usage-by-worker
// fold groups by — staged as literal values (the round env is never
// shell-expanded, so ${}-forms would export unexpanded). Temporality
// stays at claude's delta default.
func claudeOtelEnv(env []string, endpoint, worker, task, role, round string) []string {
	env = setEnvVar(env, "CLAUDE_CODE_ENABLE_TELEMETRY", "1")
	env = setEnvVar(env, "OTEL_METRICS_EXPORTER", "otlp")
	env = setEnvVar(env, "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "http/json")
	env = setEnvVar(env, "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint)
	env = setEnvVar(env, "OTEL_METRICS_INCLUDE_ACCOUNT_UUID", "false")
	env = setEnvVar(env, "OTEL_LOGS_EXPORTER", "none")
	env = setEnvVar(env, "OTEL_TRACES_EXPORTER", "none")
	env = setEnvVar(env, "OTEL_RESOURCE_ATTRIBUTES",
		"service.name=claude-code,daedalus.worker="+worker+",daedalus.task="+task+",daedalus.role="+role+",daedalus.round="+round)
	return env
}

// codexOtelArgs are the per-invocation -c overrides staging a
// metrics-on codex round's OTLP export: the otlp-http metrics exporter (which also
// silences codex's statsig default — its release build otherwise ships
// metrics to OpenAI), its exporter table's endpoint and protocol
// sub-keys, and the environment label the receiver's provenance check
// reads (D4). Argv is the only route — codex ignores [otel] in
// project-local config, and -c overrides already are the codex staging
// mechanism (stageCodexProvider).
func codexOtelArgs(endpoint string) []string {
	return []string{
		"-c", `otel.metrics_exporter="otlp-http"`,
		"-c", "otel.metrics_exporter.otlp-http.endpoint=" + tomlQuote(endpoint),
		"-c", `otel.metrics_exporter.otlp-http.protocol="json"`,
		"-c", `otel.environment="daedalus"`,
	}
}

// otelVersionFloors are the probe-verified minimum versions whose OTLP
// surface the staging requires: below them the CLIs ignore the staged
// config silently — unknown env names dropped, unknown -c keys ignored,
// no telemetry, no error — exactly the indistinguishable-from-working
// state the probe exists to prevent.
var otelVersionFloors = map[string][]int{
	"claude": {2, 1, 304},
	"codex":  {0, 160, 0},
}

var (
	otelProbeOnce sync.Once
	otelCapable   map[string]bool
)

// agentOTelCapable reports whether the agent binary on PATH probes at or
// above its floor. Probed once per process, both binaries at the first
// ask, so an otel-enabled deployment pays one --version pair, not one per
// round. A LookPath miss, an unparsable --version, or a below-floor
// version all read not-capable; the round falls back per
// metricsRoundEnv.
func agentOTelCapable(agent string) bool {
	otelProbeOnce.Do(func() {
		otelCapable = make(map[string]bool, len(otelVersionFloors))
		for name, floor := range otelVersionFloors {
			otelCapable[name] = probeOTelVersion(name, floor)
		}
	})
	return otelCapable[agent]
}

// probeOTelVersion resolves bin host-side (the jail maps the same binaries
// in) and runs --version, bounded by a short timeout — a wedged --version
// must not stall round staging.
func probeOTelVersion(bin string, floor []int) bool {
	path, err := exec.LookPath(bin)
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, path, "--version").Output()
	if err != nil {
		return false
	}
	v, ok := parseVersion(string(out))
	if !ok {
		return false
	}
	for i, f := range floor {
		if i >= len(v) {
			return false // shorter than the floor cannot reach it
		}
		if v[i] != f {
			return v[i] > f
		}
	}
	return true
}

// parseVersion extracts the first dotted numeric run in a --version
// output — "2.1.304 (Claude Code)", "codex-cli 0.160.0" — as ints.
func parseVersion(out string) ([]int, bool) {
	start := -1
	for i := 0; i < len(out); i++ {
		if out[i] >= '0' && out[i] <= '9' {
			start = i
			break
		}
	}
	if start < 0 {
		return nil, false
	}
	end := start
	for end < len(out) && (out[end] == '.' || (out[end] >= '0' && out[end] <= '9')) {
		end++
	}
	var v []int
	for _, part := range strings.Split(out[start:end], ".") {
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, false
		}
		v = append(v, n)
	}
	return v, len(v) > 0
}

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

// attributionScrub is the key builder's parse-anchor scrub: the workflow
// id is stripped of the delimiters the relay's parseAttribution splits on
// so the first "-role-" always starts the role part.
var attributionScrub = strings.NewReplacer("-role-", "_role_", "-round-", "_round_")

// metricsAttributionParts returns the synthetic attribution's three parts
// — the workflow id scrubbed of the parse anchors, the role (empty named
// none, matching the key's naming), and the activity id as the round
// number — the shape both the attribution key and the otel resource
// attributes carry. Empty task/round outside a real activity context
// (tests): the key is "" and staged resource attributes classify as
// foreign at the receiver.
func metricsAttributionParts(ctx context.Context, role SessionRole) (task, roleName, round string) {
	workflowID, _ := taskLogIdentity(ctx)
	task = attributionScrub.Replace(workflowID)
	round = metricsActivityID(ctx)
	roleName = string(role)
	if roleName == "" {
		roleName = "none"
	}
	return task, roleName, round
}

// metricsAttributionKey builds the synthetic per-round API key the relay
// parses for the ledger. The activity id is Temporal's per-schedule id —
// stable across retries, unique per round, so it is the round number.
// "" when identity is unavailable (outside a real activity context —
// tests): the round then stages the relay path only and attributes at
// worker level.
func metricsAttributionKey(ctx context.Context, role SessionRole) string {
	task, roleName, round := metricsAttributionParts(ctx, role)
	if task == "" || round == "" {
		return ""
	}
	// The delimiters are the relay's parse anchors (see
	// metricsrelay.parseAttribution).
	return "task-" + task + "-role-" + roleName + "-round-" + round
}

// metricsActivityID recovers the scheduled activity's id; "" outside an
// activity context (taskLogIdentity's same recover discipline).
func metricsActivityID(ctx context.Context) string {
	defer func() { recover() }()
	return activity.GetInfo(ctx).ActivityID
}
