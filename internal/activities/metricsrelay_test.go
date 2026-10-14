package activities

import (
	"context"
	"slices"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
)

// metricsRoundBaseEnv is the environment a metrics-enabled worker hands a
// round: the provider settings, the worker's name, and the relay roots the
// worker exported at boot, plus the registration token.
func metricsRoundBaseEnv() []string {
	return []string{
		"ANTHROPIC_BASE_URL=https://primary.example",
		"ANTHROPIC_API_KEY=sk-primary",
		"OPENAI_BASE_URL=https://oai.example/v1",
		"OPENAI_API_BASE=https://oai.example/v1",
		"OPENAI_API_KEY=sk-oai",
		"DAEDALUS_WORKER_NAME=w-metrics",
		config.MetricsRelayAnthropicEnv + "=http://127.0.0.1:7778",
		config.MetricsRelayOpenAIEnv + "=http://127.0.0.1:7778",
		config.MetricsRelayTokenEnv + "=regtok",
	}
}

// keyedRoundEnv runs fn inside a real activity context — the synthetic
// attribution key needs the SDK's identity (activity.GetInfo panics
// outside one, and the "" that the recover leaves is the unkeyed round's
// contract, pinned separately) — and returns the env slice it built.
func keyedRoundEnv(t *testing.T, fn func(ctx context.Context) []string) []string {
	t.Helper()
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestActivityEnvironment()
	probe := func(ctx context.Context) ([]string, error) { return fn(ctx), nil }
	env.RegisterActivity(probe)
	v, err := env.ExecuteActivity(probe)
	if err != nil {
		t.Fatalf("activity environment: %v", err)
	}
	var got []string
	if err := v.Get(&got); err != nil {
		t.Fatalf("decode env: %v", err)
	}
	return got
}

// activityIdentity returns the running activity's workflow and activity
// ids — the parts the attribution key encodes — for building expectations.
func activityIdentity(ctx context.Context) (workflowID, activityID string) {
	info := activity.GetInfo(ctx)
	return info.WorkflowExecution.ID, info.ActivityID
}

// TestMetricsRelayEnvNoOps pins the off switches: without a relay root the
// staging is a no-op whatever else the env carries (metrics disabled, or
// an endpoint override removed the vars upstream), and with a root but no
// worker name there is no registry segment to stage under, so the round
// stays on its direct provider path.
func TestMetricsRelayEnvNoOps(t *testing.T) {
	base := reviewerRoundBaseEnv()

	t.Run("no relay roots", func(t *testing.T) {
		env := append(base,
			"DAEDALUS_WORKER_NAME=w",
			config.MetricsRelayTokenEnv+"=regtok")
		if got := metricsRelayEnv(context.Background(), slices.Clone(env), RoleDev); !slices.Equal(got, env) {
			t.Errorf("metricsRelayEnv without relay roots rewired the env: %v", got)
		}
	})

	t.Run("no worker name", func(t *testing.T) {
		env := append(base,
			config.MetricsRelayAnthropicEnv+"=http://127.0.0.1:7778",
			config.MetricsRelayOpenAIEnv+"=http://127.0.0.1:7778")
		if got := metricsRelayEnv(context.Background(), slices.Clone(env), RoleDev); !slices.Equal(got, env) {
			t.Errorf("metricsRelayEnv without a worker name rewired the env: %v", got)
		}
	})
}

// TestMetricsRelayEnvStagesUnkeyed pins the unkeyed round (no activity
// identity — tests, or identity-less contexts): the base URLs are
// rewritten onto the staged path on both wires, OPENAI_API_BASE rides the
// same value, and every credential is left exactly as the round carried
// it — an absent key stays absent, never minted.
func TestMetricsRelayEnvStagesUnkeyed(t *testing.T) {
	t.Run("both wires staged, credentials untouched", func(t *testing.T) {
		env := metricsRelayEnv(context.Background(), metricsRoundBaseEnv(), RoleDev)

		wantAnthropic := metricsrelay.StagedBase("http://127.0.0.1:7778", "w-metrics", metricsrelay.WireAnthropic, "https://primary.example")
		if got := envValue(env, "ANTHROPIC_BASE_URL"); got != wantAnthropic {
			t.Errorf("ANTHROPIC_BASE_URL = %q, want staged %q", got, wantAnthropic)
		}
		if got := envValue(env, "ANTHROPIC_API_KEY"); got != "sk-primary" {
			t.Errorf("ANTHROPIC_API_KEY = %q, want the round's own untouched", got)
		}
		wantOpenAI := metricsrelay.StagedBase("http://127.0.0.1:7778", "w-metrics", metricsrelay.WireOpenAI, "https://oai.example/v1")
		for _, name := range []string{"OPENAI_BASE_URL", "OPENAI_API_BASE"} {
			if got := envValue(env, name); got != wantOpenAI {
				t.Errorf("%s = %q, want staged %q", name, got, wantOpenAI)
			}
		}
		if got := envValue(env, "OPENAI_API_KEY"); got != "sk-oai" {
			t.Errorf("OPENAI_API_KEY = %q, want the round's own untouched", got)
		}
	})

	t.Run("API_BASE-only openai form stages too", func(t *testing.T) {
		env := metricsRoundBaseEnv()
		env = setEnvVar(env, "OPENAI_BASE_URL", "")
		env = removeEnvVar(env, "OPENAI_BASE_URL")
		env = removeEnvVar(env, "OPENAI_API_BASE")
		env = append(env, "OPENAI_API_BASE=https://legacy.example/v1")

		got := metricsRelayEnv(context.Background(), env, RoleDev)
		want := metricsrelay.StagedBase("http://127.0.0.1:7778", "w-metrics", metricsrelay.WireOpenAI, "https://legacy.example/v1")
		if got := envValue(got, "OPENAI_API_BASE"); got != want {
			t.Errorf("OPENAI_API_BASE = %q, want staged %q", got, want)
		}
		if slices.Contains(got, "OPENAI_BASE_URL=") {
			t.Errorf("OPENAI_BASE_URL minted for an API_BASE-only env: %v", got)
		}
	})

	t.Run("keyless round stays keyless", func(t *testing.T) {
		env := metricsRoundBaseEnv()
		env = removeEnvVar(env, "ANTHROPIC_API_KEY")
		env = removeEnvVar(env, "OPENAI_API_KEY")

		got := metricsRelayEnv(context.Background(), env, RoleDev)
		for _, name := range []string{"ANTHROPIC_API_KEY", "OPENAI_API_KEY"} {
			if slices.ContainsFunc(got, func(kv string) bool { return strings.HasPrefix(kv, name+"=") }) {
				t.Errorf("%s minted for a keyless round: %v", name, got)
			}
		}
	})
}

// TestMetricsRelayEnvStagesKeyed pins the keyed round: with the activity
// identity and the registration token present, the credential slot carries
// the synthetic attribution key with the token as its -rk- tail — the
// worker's attribution encoding the relay parses for the ledger and gates
// key restoration on. The token absent stages the bare shape (a
// pre-token daemon registers nothing restorable anyway).
func TestMetricsRelayEnvStagesKeyed(t *testing.T) {
	wantKey := func(t *testing.T, env []string, role SessionRole, token string) {
		t.Helper()
		if got := envValue(env, "ANTHROPIC_API_KEY"); !strings.HasPrefix(got, "task-") ||
			!strings.Contains(got, "-role-"+string(role)+"-round-") || !strings.HasSuffix(got, token) {
			t.Errorf("ANTHROPIC_API_KEY = %q, want the synthetic task/role/round shape with the %q tail", got, token)
		}
	}

	t.Run("token stages the -rk- tail", func(t *testing.T) {
		env := keyedRoundEnv(t, func(ctx context.Context) []string {
			identity := metricsAttributionKey(ctx, RoleDev)
			env := metricsRelayEnv(ctx, metricsRoundBaseEnv(), RoleDev)
			return append(env, "DAEDALUS_TEST_ATTRIBUTION="+identity)
		})
		// The expectation is built from the same activity identity the
		// staging read: task-<workflow>-role-dev-round-<activity>-rk-<tok>.
		identity := envValue(env, "DAEDALUS_TEST_ATTRIBUTION")
		if identity == "" {
			t.Fatal("no attribution key built inside the activity context")
		}
		if got := envValue(env, "ANTHROPIC_API_KEY"); got != identity+"-rk-regtok" {
			t.Errorf("ANTHROPIC_API_KEY = %q, want the attribution key %q with the registration token as its -rk- tail", got, identity)
		}
		wantKey(t, env, RoleDev, "-rk-regtok")
		if got := envValue(env, "OPENAI_API_KEY"); !strings.HasSuffix(got, "-rk-regtok") {
			t.Errorf("OPENAI_API_KEY = %q, want the same synthetic key with the token tail", got)
		}
	})

	t.Run("no token stages the bare shape", func(t *testing.T) {
		base := metricsRoundBaseEnv()
		base = removeEnvVar(base, config.MetricsRelayTokenEnv)
		env := keyedRoundEnv(t, func(ctx context.Context) []string {
			return metricsRelayEnv(ctx, base, RoleTest)
		})
		wantKey(t, env, RoleTest, "")
		if got := envValue(env, "ANTHROPIC_API_KEY"); strings.Contains(got, "-rk-") {
			t.Errorf("ANTHROPIC_API_KEY = %q, want no -rk- tail without a registration token", got)
		}
	})

	t.Run("empty role attributes as none", func(t *testing.T) {
		env := keyedRoundEnv(t, func(ctx context.Context) []string {
			return append(metricsRelayEnv(ctx, metricsRoundBaseEnv(), ""),
				"DAEDALUS_TEST_ATTRIBUTION="+metricsAttributionKey(ctx, ""))
		})
		identity := envValue(env, "DAEDALUS_TEST_ATTRIBUTION")
		if !strings.Contains(identity, "-role-none-round-") {
			t.Errorf("attribution key %q, want the empty role named none", identity)
		}
		if got := envValue(env, "ANTHROPIC_API_KEY"); got != identity+"-rk-regtok" {
			t.Errorf("ANTHROPIC_API_KEY = %q, want the none-role key with the token tail", got)
		}
	})
}

// TestMetricsAttributionKey pins the key's shape at its source: the
// workflow id scrubbed of the parse anchors, the role part, and the
// activity id as the round number — the anchors metricsrelay's
// parseAttribution splits on. Outside an activity context there is no
// identity, and the key is "" — the unkeyed round's staging.
func TestMetricsAttributionKey(t *testing.T) {
	if got := metricsAttributionKey(context.Background(), RoleDev); got != "" {
		t.Errorf("metricsAttributionKey(outside an activity) = %q, want empty (unkeyed round)", got)
	}

	keyedRoundEnv(t, func(ctx context.Context) []string {
		workflowID, activityID := activityIdentity(ctx)
		want := "task-" + strings.NewReplacer("-role-", "_role_", "-round-", "_round_").Replace(workflowID) +
			"-role-dev-round-" + activityID
		if got := metricsAttributionKey(ctx, RoleDev); got != want {
			t.Errorf("metricsAttributionKey = %q, want %q (task-<workflow>-role-dev-round-<activity>)", got, want)
		}
		return nil
	})
}

// TestRunJailedMetricsStaging pins the wiring seam: a round launched with
// the relay roots and the worker name in its environment dials the staged
// path, not its provider — metricsRelayEnv runs in runJailedRoundFolders
// after reviewerEnv, before anything reads the env — and an unkeyed round
// keeps its own credentials. Without the roots the round dials its
// provider exactly as before.
func TestRunJailedMetricsStaging(t *testing.T) {
	t.Setenv("DAEDALUS_AGENT", "claude")
	t.Setenv("ANTHROPIC_BASE_URL", "https://primary.example")
	t.Setenv("ANTHROPIC_API_KEY", "sk-primary")
	t.Setenv("OPENAI_BASE_URL", "https://oai.example/v1")
	t.Setenv("DAEDALUS_WORKER_NAME", "w-metrics")
	t.Setenv(config.MetricsRelayAnthropicEnv, "http://127.0.0.1:7778")
	t.Setenv(config.MetricsRelayOpenAIEnv, "http://127.0.0.1:7778")

	log := newStubLog(t)
	stubBin(t, "ai-jail", "exit 0")

	if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: t.TempDir(),
		Prompt:       "fix the bug",
	}); err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}

	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
	}
	env := calls[0].Env
	wantAnthropic := metricsrelay.StagedBase("http://127.0.0.1:7778", "w-metrics", metricsrelay.WireAnthropic, "https://primary.example")
	if got := env["ANTHROPIC_BASE_URL"]; got != wantAnthropic {
		t.Errorf("round env ANTHROPIC_BASE_URL = %q, want staged %q", got, wantAnthropic)
	}
	wantOpenAI := metricsrelay.StagedBase("http://127.0.0.1:7778", "w-metrics", metricsrelay.WireOpenAI, "https://oai.example/v1")
	if got := env["OPENAI_BASE_URL"]; got != wantOpenAI {
		t.Errorf("round env OPENAI_BASE_URL = %q, want staged %q", got, wantOpenAI)
	}
	if got := env["ANTHROPIC_API_KEY"]; got != "sk-primary" {
		t.Errorf("round env ANTHROPIC_API_KEY = %q, want the unkeyed round's own untouched", got)
	}
}
