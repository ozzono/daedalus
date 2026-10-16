package activities

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ozzono/daedalus/internal/config"
)

// The suite must be hermetic against the env a serving worker exports:
// runWorker mints its registration and sets the metrics-relay roots and
// token plus DAEDALUS_WORKER_NAME into its own process env, the jailed
// env passthrough rides them into every `go test` the worker's jailed
// rounds serve, and metricsRelayEnv — correctly, given its inputs —
// rewrites the staged provider endpoints, breaking every exact-match
// staging pin. The fix is a TestMain scrub, so this regression test
// exercises the whole-process path: the driver seeds the worker's
// ambient values and runs the probe in a child test binary whose
// TestMain stands between the inherited env and the probe's assertions.
// Per-test t.Setenv cannot do that job — it applies after TestMain, and
// re-seeding in-process would re-pollute the very env the scrub cleaned.

// hermeticProbeTest is the child-side test the driver runs; the exact
// name is the -test.run anchor that keeps the child from recursing into
// the driver.
const hermeticProbeTest = "TestHermeticWorkerEnvStagingProbe"

// workerAmbientSeed is the env slice a metrics-enabled worker exports
// into its own process: the relay roots and registration token plus the
// worker name metricsRelayEnv keys the registry segment off. The names
// are neutral fixtures — no real deployment's worker or relay path.
var workerAmbientSeed = map[string]string{
	config.MetricsRelayAnthropicEnv: "http://127.0.0.1:7778/relay/worker-fixture/anthropic",
	config.MetricsRelayOpenAIEnv:    "http://127.0.0.1:7778/relay/worker-fixture/openai",
	config.MetricsRelayTokenEnv:     "reg-token-ambient",
	"DAEDALUS_WORKER_NAME":          "worker-fixture",
}

// probeTimeout bounds one probe child: a wedged child would otherwise
// stall the package into the global go-test timeout with a context dump
// instead of this targeted failure.
const probeTimeout = 30 * time.Second

// TestHermeticWorkerEnvStagingProbe is the representative staging
// assertion — the aider openai endpoint wiring, asserted through the
// same fixture and assertion helpers as
// TestRunJailedClaudeActivityAiderOpenAIModel so the two cannot drift.
// It passes standalone in a clean env and is the child the hermeticity
// driver seeds and executes.
func TestHermeticWorkerEnvStagingProbe(t *testing.T) {
	calls, _ := stageAiderOpenAIRound(t)
	assertAiderOpenAIEndpoint(t, calls)
}

// TestSuiteHermeticAgainstWorkerExportedEnv pins the suite-wide
// hermeticity invariant. Metrics on: the seed rides the child's exec
// env, and the child's TestMain scrub must neutralize it so the probe's
// staging assertions see the probe's own inputs — the whole-process
// hermeticity proof, so this is the one child exec. Metrics off needs
// no exec: the plain-environment behavior is pinned by this suite's own
// in-process run of TestHermeticWorkerEnvStagingProbe (the package
// TestMain scrub guarantees a clean env even when the suite itself runs
// on a serving worker), so a second child exec would only re-assert
// what the parent run already asserts at a full binary exec's cost.
// Nothing here mutates the parent env, so the driver is parallel-safe
// like any other test. Hermeticity coverage is validated by the project
// final gate, `go test -count=1 ./...`: plain `go test` may replay
// cached results without re-running against an ambient worker env
// (Go test caching does not track environment), so it can miss exactly
// the regression this file pins.
func TestSuiteHermeticAgainstWorkerExportedEnv(t *testing.T) {
	bin, err := os.Executable()
	if err != nil {
		t.Fatalf("find test binary: %v", err)
	}
	runHermeticProbe(t, bin, workerAmbientSeed)
}

// runHermeticProbe execs this package's test binary down to the probe
// with the worker's ambient values riding the child's env and requires
// it green; -test.run anchors on the probe's exact name so the child
// never recurses into the driver. The child's TestMain scrub is what
// stands between the inherited env and the probe's staging assertions.
func runHermeticProbe(t *testing.T, bin string, seeds map[string]string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "-test.run", "^"+hermeticProbeTest+"$", "-test.v")
	if len(seeds) > 0 {
		cmd.Env = os.Environ()
		for k, v := range seeds {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("seeded hermetic probe failed within %s: %v\n%s", probeTimeout, err, out)
	}
	// A zero-match -test.run exits 0 with "no tests to run", so exit
	// status alone cannot tell a passing probe from a stale anchor: a
	// rename of the probe test would silence the whole regression. The
	// -test.v RUN header is the proof the child actually executed it
	// (matched by marker + name rather than one spacing-sensitive
	// substring — the header's run of spaces is the testing package's
	// formatting detail).
	if !strings.Contains(string(out), "=== RUN") || !strings.Contains(string(out), hermeticProbeTest) {
		t.Fatalf("seeded hermetic probe never ran %s (stale anchor?):\n%s", hermeticProbeTest, out)
	}
}
