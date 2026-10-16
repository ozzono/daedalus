package activities

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/ozzono/daedalus/internal/config"
)

// TestRoundEnvAllowlist pins the spawn-gate filter itself: allow-listed
// entries pass through with values and relative order intact, everything
// unlisted — another provider family's key, a worker control knob, a test
// transport var — stops, and a malformed entry (no "=") is dropped rather
// than passed.
func TestRoundEnvAllowlist(t *testing.T) {
	env := []string{
		"PATH=/usr/bin:/bin",
		"ANTHROPIC_API_KEY=sk-primary",
		"DAEDALUS_WORKER_NAME=worker-fixture",
		"STUB_LOG=/tmp/leak.log",
		"OPENAI_BASE_URL=https://oai.example",
		"DAEDALUS_FALLBACK_API_KEY=sk-fallback",
		"MALFORMED_NO_EQUALS",
		"HOME=/home/fixture",
	}
	got := roundEnvAllowlist(env)

	want := []string{
		"PATH=/usr/bin:/bin",
		"ANTHROPIC_API_KEY=sk-primary",
		"OPENAI_BASE_URL=https://oai.example",
		"HOME=/home/fixture",
	}
	// Strict equality: the fixture order equals a correct filter's output,
	// so a pass-through, an addition, a drop, and a reorder must all fail —
	// a one-sided subsequence check would let the filter silently widen.
	if !slices.Equal(got, want) {
		t.Errorf("roundEnvAllowlist = %v, want exactly %v (listed entries in order, nothing else)", got, want)
	}
}

// TestRoundEnvGateAdmitsOnlyAllowListedVars pins the sandbox-door contract
// end to end at the jail spawn: a round env assembled the way production
// assembles it — this process's full inherited environment plus a whole
// serving worker's export riding the passthrough — reaches the jailed CLI
// as nothing but config.RoundEnvVars. Seeded worker families (fallback,
// reviewer, verdict-repair, metrics token, bug dir, the control knobs) and
// plain ambient junk must be absent; the serving provider's wires must
// survive.
//
// The dump is taken inside the stub's own shell, which injects PWD/SHLVL/_
// itself; those are exempted as shell bookkeeping, not passthrough.
// STUB_LOG is exempted for a different reason: the stub self-stages it (see
// stubBin), so the dump cannot distinguish its own copy from an inherited
// one — the filter-level exclusion is pinned by TestRoundEnvAllowlist.
func TestRoundEnvGateAdmitsOnlyAllowListedVars(t *testing.T) {
	log := newStubLog(t)
	stubBin(t, "ai-jail", `env | LC_ALL=C sort >> "$STUB_LOG"
exit 0`)

	base := append(os.Environ(),
		"ANTHROPIC_BASE_URL=https://primary.example",
		"ANTHROPIC_API_KEY=sk-primary",
		"ANTHROPIC_MODEL=claude-opus-5",
		"DAEDALUS_FALLBACK_API_KEY=sk-fallback",
		"DAEDALUS_FALLBACK_MODEL=glm-backup",
		config.ReviewerKeyEnv+"=sk-reviewer",
		config.VerdictRepairKeyEnv+"=sk-repair",
		config.MetricsRelayTokenEnv+"=reg-token",
		config.BugDirEnv+"=/host/bugs",
		"DAEDALUS_WORKER_NAME=worker-fixture",
		"DAEDALUS_SLIM=1",
		"DAEDALUS_AGENT=aider",
		"NODE_EXTRA_CA_CERTS=/host/extra-ca.pem",
		"AWS_SECRET_ACCESS_KEY=ambient-junk",
	)
	if _, err := runJailedRound(context.Background(), base, "", "claude", t.TempDir(), "round prompt"); err != nil {
		t.Fatalf("runJailedRound: %v", err)
	}

	// Everything after the preamble's STDIN line is the sorted `env` dump;
	// the preamble's own ENV:-prefixed lines (they precede the body) and
	// empty lines are not part of it.
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatalf("read stub log: %v", err)
	}
	lines := strings.Split(string(data), "\n")
	start := slices.IndexFunc(lines, func(l string) bool { return strings.HasPrefix(l, "STDIN:") })
	if start < 0 {
		t.Fatalf("stub preamble never completed (log: %q)", data)
	}
	var names []string
	for _, line := range lines[start+1:] {
		if line == "" || strings.HasPrefix(line, "ENV:") {
			continue
		}
		name, _, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("env dump line %q is not NAME=VALUE", line)
		}
		names = append(names, name)
	}

	allow := make(map[string]bool, len(config.RoundEnvVars()))
	for _, name := range config.RoundEnvVars() {
		allow[name] = true
	}
	shellInjected := map[string]bool{"PWD": true, "SHLVL": true, "_": true, "STUB_LOG": true}
	reported := make(map[string]bool)
	for _, name := range names {
		if shellInjected[name] || reported[name] {
			continue
		}
		reported[name] = true
		if !allow[name] {
			t.Errorf("jailed round env carries unlisted %q; the allow-list gate let it through", name)
		}
	}

	for _, leaked := range []string{
		"DAEDALUS_FALLBACK_API_KEY",
		config.ReviewerKeyEnv,
		config.VerdictRepairKeyEnv,
		config.MetricsRelayTokenEnv,
		config.BugDirEnv,
		"DAEDALUS_WORKER_NAME",
		"DAEDALUS_SLIM",
		"DAEDALUS_AGENT",
		"NODE_EXTRA_CA_CERTS",
		"AWS_SECRET_ACCESS_KEY",
	} {
		if slices.Contains(names, leaked) {
			t.Errorf("jailed round env leaked seeded %q", leaked)
		}
	}
	for _, served := range []string{"ANTHROPIC_BASE_URL", "ANTHROPIC_API_KEY", "ANTHROPIC_MODEL"} {
		if !slices.Contains(names, served) {
			t.Errorf("the gate stripped the serving provider's %s", served)
		}
	}
}
