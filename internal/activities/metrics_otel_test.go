package activities

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/metricsrelay"
)

// TestParseVersion pins the --version extract: the first dotted numeric run
// in the output, as ints — whatever prefixes or suffixes the CLI decorates
// it with. No digits, or a run with an empty part, is unparsable.
func TestParseVersion(t *testing.T) {
	cases := []struct {
		out  string
		want []int
		ok   bool
	}{
		{"2.1.308 (Claude Code)", []int{2, 1, 308}, true},
		{"codex-cli 0.160.0", []int{0, 160, 0}, true},
		{"2.0", []int{2, 0}, true},
		{"1.2.3.4-x", []int{1, 2, 3, 4}, true},
		{"no version here", nil, false},
		{"", nil, false},
		{"1..2", nil, false},
	}
	for _, c := range cases {
		got, ok := parseVersion(c.out)
		if ok != c.ok || !slices.Equal(got, c.want) {
			t.Errorf("parseVersion(%q) = (%v, %v), want (%v, %v)", c.out, got, ok, c.want, c.ok)
		}
	}
}

// writeVersionBin drops an executable named bin into a fresh PATH dir that
// prints the given --version output, and points the process at the dir —
// probeOTelVersion resolves through LookPath, so a fake bin on a fake PATH
// is the whole probe surface, deterministically, with no host CLI involved.
func writeVersionBin(t *testing.T, bin, out string) {
	t.Helper()
	dir := t.TempDir()
	if prev, ok := os.LookupEnv("PATH"); ok && strings.Contains(prev, dir) {
		t.Fatalf("PATH already carries %q", dir)
	}
	t.Setenv("PATH", dir+string(filepath.ListSeparator)+os.Getenv("PATH"))
	path := filepath.Join(dir, bin)
	if err := os.WriteFile(path, []byte("#!/bin/sh\ncat <<'EOF'\n"+out+"EOF\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}

// TestProbeOTelVersionFloors pins the capability probe against the staged
// floors: at or above the floor is capable, below it — or a shorter
// version, an unparsable one, or a missing binary — is not, the state the
// fallback exists for (below the floor the CLIs drop the staged config
// silently, indistinguishable from working).
func TestProbeOTelVersionFloors(t *testing.T) {
	claudeFloor := otelVersionFloors["claude"]
	codexFloor := otelVersionFloors["codex"]

	t.Run("at and above the floor", func(t *testing.T) {
		writeVersionBin(t, "claude", "2.1.304 (Claude Code)\n")
		if !probeOTelVersion("claude", claudeFloor) {
			t.Errorf("claude 2.1.304 probed not capable; the floor itself must pass")
		}
		writeVersionBin(t, "claude2", "2.2.0 (Claude Code)\n")
		if !probeOTelVersion("claude2", claudeFloor) {
			t.Errorf("claude 2.2.0 probed not capable")
		}
	})

	t.Run("below the floor", func(t *testing.T) {
		writeVersionBin(t, "claude", "2.1.303 (Claude Code)\n")
		if probeOTelVersion("claude", claudeFloor) {
			t.Errorf("claude 2.1.303 probed capable")
		}
	})

	t.Run("shorter than the floor", func(t *testing.T) {
		writeVersionBin(t, "codex", "codex-cli 0.160\n")
		if probeOTelVersion("codex", codexFloor) {
			t.Errorf("a two-part 0.160 probed capable against the three-part floor")
		}
	})

	t.Run("unparsable and missing", func(t *testing.T) {
		writeVersionBin(t, "codex", "codex-cli (dev build)\n")
		if probeOTelVersion("codex", codexFloor) {
			t.Errorf("an unparsable version probed capable")
		}
		if probeOTelVersion("ghost-cli", codexFloor) {
			t.Errorf("a missing binary probed capable")
		}
	})
}

// TestClaudeOtelEnvStagesTheEightNames pins the toggle-on claude staging:
// the eight documented env names with their exact values — the enable
// switch, the otlp http/json exporter pointed at the endpoint, the
// cardinality trim, the explicitly-disabled traces and logs, and the
// resource attributes the receiver's provenance check reads — staged over
// the round's env without disturbing anything else, and re-staging
// replaces rather than duplicates.
func TestClaudeOtelEnvStagesTheEightNames(t *testing.T) {
	endpoint := "http://127.0.0.1:7778/v1/metrics"
	env := claudeOtelEnv([]string{"ANTHROPIC_BASE_URL=https://primary.example", "ANTHROPIC_API_KEY=sk-primary"}, endpoint, "wf-one", "dev", "3")

	want := map[string]string{
		"CLAUDE_CODE_ENABLE_TELEMETRY":        "1",
		"OTEL_METRICS_EXPORTER":               "otlp",
		"OTEL_EXPORTER_OTLP_METRICS_PROTOCOL": "http/json",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": endpoint,
		"OTEL_METRICS_INCLUDE_ACCOUNT_UUID":   "false",
		"OTEL_LOGS_EXPORTER":                  "none",
		"OTEL_TRACES_EXPORTER":                "none",
		"OTEL_RESOURCE_ATTRIBUTES":            "service.name=claude-code,daedalus.task=wf-one,daedalus.role=dev,daedalus.round=3",
		"ANTHROPIC_BASE_URL":                  "https://primary.example",
		"ANTHROPIC_API_KEY":                   "sk-primary",
	}
	for name, wantVal := range want {
		if got := envValue(env, name); got != wantVal {
			t.Errorf("%s = %q, want %q", name, got, wantVal)
		}
	}
	for name := range want {
		n := 0
		for _, kv := range env {
			if strings.HasPrefix(kv, name+"=") {
				n++
			}
		}
		if n != 1 {
			t.Errorf("%s appears %d times in the staged env, want staged once", name, n)
		}
	}

	// Re-staging over an already-staged env replaces in place — a retry
	// over a retry's env must not grow the env — and the new parts win.
	restaged := claudeOtelEnv(slices.Clone(env), endpoint, "wf-one", "test", "4")
	for name := range want {
		n := 0
		for _, kv := range restaged {
			if strings.HasPrefix(kv, name+"=") {
				n++
			}
		}
		if n != 1 {
			t.Errorf("%s appears %d times after re-staging, want one", name, n)
		}
	}
	if got := envValue(restaged, "OTEL_RESOURCE_ATTRIBUTES"); got != "service.name=claude-code,daedalus.task=wf-one,daedalus.role=test,daedalus.round=4" {
		t.Errorf("re-staged OTEL_RESOURCE_ATTRIBUTES = %q, want the new parts", got)
	}
}

// TestCodexOtelArgsStagesTheOverrides pins the toggle-on codex staging:
// the four -c overrides — the otlp-http exporter (which also silences
// codex's statsig default), its endpoint and protocol sub-keys, and the
// environment label the receiver's provenance check reads — in argv form,
// the only route codex honors.
func TestCodexOtelArgsStagesTheOverrides(t *testing.T) {
	endpoint := "http://127.0.0.1:7778/v1/metrics"
	want := []string{
		"-c", `otel.metrics_exporter="otlp-http"`,
		"-c", `otel.metrics_exporter.otlp-http.endpoint="` + endpoint + `"`,
		"-c", `otel.metrics_exporter.otlp-http.protocol="json"`,
		"-c", `otel.environment="daedalus"`,
	}
	if got := codexOtelArgs(endpoint); !slices.Equal(got, want) {
		t.Errorf("codexOtelArgs = %v, want %v", got, want)
	}
}

// TestMetricsAttributionParts pins the attribution's three parts at their
// shared source: the workflow id scrubbed of the parse anchors, the role
// named (an empty one as none, matching the key's naming), and the
// activity id as the round number — the shape both the synthetic key and
// the otel resource attributes carry. Outside a real activity context the
// task and round are empty: the key is "" and staged resource attributes
// classify as foreign at the receiver.
func TestMetricsAttributionParts(t *testing.T) {
	if got := attributionScrub.Replace("task-x-role-y-round-z"); got != "task-x_role_y_round_z" {
		t.Errorf("attributionScrub = %q, want the parse anchors rewritten", got)
	}

	task, roleName, round := metricsAttributionParts(context.Background(), RoleDev)
	if task != "" || round != "" {
		t.Errorf("outside an activity: task=%q round=%q, want empty", task, round)
	}
	if roleName != "dev" {
		t.Errorf("roleName = %q, want dev", roleName)
	}
	if _, roleName, _ := metricsAttributionParts(context.Background(), ""); roleName != "none" {
		t.Errorf("empty role named %q, want none", roleName)
	}

	keyedRoundEnv(t, func(ctx context.Context) []string {
		workflowID, activityID := activityIdentity(ctx)
		task, roleName, round := metricsAttributionParts(ctx, RoleTest)
		if task != attributionScrub.Replace(workflowID) {
			t.Errorf("task = %q, want the workflow id scrubbed of the parse anchors", task)
		}
		if roleName != "test" {
			t.Errorf("roleName = %q, want test", roleName)
		}
		if round != activityID {
			t.Errorf("round = %q, want the activity id %q", round, activityID)
		}
		// The otel attributes and the synthetic key encode the same parts.
		if key := metricsAttributionKey(ctx, RoleTest); key != "task-"+task+"-role-"+roleName+"-round-"+round {
			t.Errorf("key %q diverges from the parts (%q %q %q)", key, task, roleName, round)
		}
		return nil
	})
}

// TestMetricsRoundEnvWithoutTheToggle pins the selector's deterministic
// faces: without the endpoint var the staging is the relay's — for every
// agent, however capable the binary might be — and a non-otel agent with
// the toggle on keeps the relay too. The claude/codex capable branches
// dispatch on a host binary probe (agentOTelCapable), so they are pinned
// at their parts instead: claudeOtelEnv, codexOtelArgs, and
// probeOTelVersion above.
func TestMetricsRoundEnvWithoutTheToggle(t *testing.T) {
	t.Run("no endpoint stages the relay even for claude", func(t *testing.T) {
		env := metricsRoundBaseEnv()
		got, args := metricsRoundEnv(context.Background(), slices.Clone(env), RoleDev, "claude")
		if args != nil {
			t.Errorf("otel args = %v, want none off the toggle", args)
		}
		want := metricsrelay.StagedBase("http://127.0.0.1:7778", "w-metrics", metricsrelay.WireAnthropic, "https://primary.example")
		if gotVal := envValue(got, "ANTHROPIC_BASE_URL"); gotVal != want {
			t.Errorf("ANTHROPIC_BASE_URL = %q, want the relay's staged %q", gotVal, want)
		}
	})

	t.Run("an empty endpoint value counts as off", func(t *testing.T) {
		env := append(metricsRoundBaseEnv(), config.MetricsOtelEndpointEnv+"=")
		got, args := metricsRoundEnv(context.Background(), slices.Clone(env), RoleDev, "claude")
		if args != nil {
			t.Errorf("otel args = %v, want none off the toggle", args)
		}
		want := metricsrelay.StagedBase("http://127.0.0.1:7778", "w-metrics", metricsrelay.WireOpenAI, "https://oai.example/v1")
		if gotVal := envValue(got, "OPENAI_BASE_URL"); gotVal != want {
			t.Errorf("OPENAI_BASE_URL = %q, want the relay's staged %q", gotVal, want)
		}
	})

	t.Run("a non-otel agent keeps the relay with the toggle on", func(t *testing.T) {
		env := append(metricsRoundBaseEnv(), config.MetricsOtelEndpointEnv+"=http://127.0.0.1:7778/v1/metrics")
		for _, agent := range []string{"aider", "opencode", "amp"} {
			got, args := metricsRoundEnv(context.Background(), slices.Clone(env), RoleDev, agent)
			if args != nil {
				t.Errorf("%s: otel args = %v, want none (the agent has no otel surface)", agent, args)
			}
			want := metricsrelay.StagedBase("http://127.0.0.1:7778", "w-metrics", metricsrelay.WireAnthropic, "https://primary.example")
			if gotVal := envValue(got, "ANTHROPIC_BASE_URL"); gotVal != want {
				t.Errorf("%s: ANTHROPIC_BASE_URL = %q, want the relay's staged %q", agent, gotVal, want)
			}
		}
	})
}
