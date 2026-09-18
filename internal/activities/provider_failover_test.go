package activities

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// resetProviderState clears the per-worker failover state around a test so
// one test's holds cannot leak into another's.
func resetProviderState(t *testing.T) {
	t.Helper()
	providerState.Lock()
	providerState.primary = providerAvailability{}
	providerState.fallback = providerAvailability{}
	providerState.Unlock()
}

// armFallback points the worker environment at a fallback provider and
// returns a cleanup restoring the previous values.
func armFallback(t *testing.T) {
	t.Helper()
	t.Setenv("DAEDALUS_FALLBACK_BASE_URL", "https://backup.example")
	t.Setenv("DAEDALUS_FALLBACK_API_KEY", "sk-backup")
	t.Setenv("DAEDALUS_FALLBACK_MODEL", "glm-backup")
	t.Setenv("DAEDALUS_FALLBACK_HEARTBEAT_MODEL", "glm-backup-air")
}

// exhaustedStubBody fails the round with the z.ai quota text when it sees
// the primary key, and succeeds otherwise — the fallback side of the
// branch is what makes failover observable.
const exhaustedStubBody = `if [ "$ANTHROPIC_API_KEY" = "sk-primary" ]; then
  echo '{"type":"result","is_error":true,"result":"API Error: Request rejected (429) · [1308][Usage limit reached for 5 hour. Your limit will reset at 2099-01-01 09:13:00]"}'
  exit 1
fi
exit 0
`

// TestRunJailedFailsOverOnExhaustion pins the failover contract end to
// end: a round that dies on the primary's quota is retried once on the
// fallback's url/key/model, the retry's success clears the error, and the
// primary is benched until the parsed reset stamp.
func TestRunJailedFailsOverOnExhaustion(t *testing.T) {
	resetProviderState(t)
	armFallback(t)
	t.Setenv("ANTHROPIC_API_KEY", "sk-primary")
	log := newStubLog(t)
	stubBin(t, "ai-jail", exhaustedStubBody)

	_, err := runJailed(context.Background(), "", t.TempDir(), "do the thing")
	if err != nil {
		t.Fatalf("runJailed: %v", err)
	}
	calls := readCalls(t, log)
	if len(calls) != 2 {
		t.Fatalf("got %d jailed calls, want 2 (primary then fallback)", len(calls))
	}
	if got := calls[0].Env["ANTHROPIC_API_KEY"]; got != "sk-primary" {
		t.Errorf("first round key = %q, want the primary's", got)
	}
	if got := calls[1].Env["ANTHROPIC_API_KEY"]; got != "sk-backup" {
		t.Errorf("retry round key = %q, want the fallback's", got)
	}
	if got := calls[1].Env["ANTHROPIC_BASE_URL"]; got != "https://backup.example" {
		t.Errorf("retry round base url = %q, want the fallback's", got)
	}
	if got := calls[1].Env["ANTHROPIC_MODEL"]; got != "glm-backup" {
		t.Errorf("retry round model = %q, want the fallback's", got)
	}
	if got := calls[1].Env["ANTHROPIC_DEFAULT_HAIKU_MODEL"]; got != "glm-backup-air" {
		t.Errorf("retry round heartbeat model = %q, want the fallback's", got)
	}
	providerState.Lock()
	defer providerState.Unlock()
	if providerState.primary.up(time.Now()) {
		t.Error("primary still up after an exhausted round; want it benched until the reset stamp")
	}
	if !providerState.fallback.up(time.Now()) {
		t.Error("fallback benched; it succeeded")
	}
}

// primaryExhaustedFallbackFailsStubBody fails the primary round with the
// z.ai quota text and the fallback round with a plain non-exhaustion
// failure — the retry branch's "keep the primary's classification" case.
const primaryExhaustedFallbackFailsStubBody = `if [ "$ANTHROPIC_API_KEY" = "sk-primary" ]; then
  echo '{"type":"result","is_error":true,"result":"API Error: Request rejected (429) · [1308][Usage limit reached for 5 hour. Your limit will reset at 2099-01-01 09:13:00]"}'
  exit 1
fi
echo "agent killed by signal"
exit 1
`

// alwaysExhaustedStubBody fails every round with the z.ai quota text —
// both sides of the failover exhaust.
const alwaysExhaustedStubBody = `echo '{"type":"result","is_error":true,"result":"API Error: Request rejected (429) · [1308][Usage limit reached for 5 hour. Your limit will reset at 2099-01-01 09:13:00]"}'
exit 1
`

// TestRunJailedSkipsFallbackRetryWhenBudgetShort pins the deadline gate:
// with less than fallbackRetryFloor of activity budget left, an exhausted
// primary round is not retried on the fallback — the primary's exhaustion
// is returned unchanged and the fallback stays untouched for the workflow's
// hourly loop to reach.
func TestRunJailedSkipsFallbackRetryWhenBudgetShort(t *testing.T) {
	resetProviderState(t)
	armFallback(t)
	t.Setenv("ANTHROPIC_API_KEY", "sk-primary")
	log := newStubLog(t)
	stubBin(t, "ai-jail", exhaustedStubBody)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()
	_, err := runJailed(ctx, "", t.TempDir(), "prompt")
	if !errors.Is(err, ErrAPIExhausted) {
		t.Fatalf("runJailed error = %v, want ErrAPIExhausted returned unchanged", err)
	}
	if calls := readCalls(t, log); len(calls) != 1 {
		t.Fatalf("got %d jailed calls, want 1 (retry skipped: budget nearly spent)", len(calls))
	}
	providerState.Lock()
	defer providerState.Unlock()
	if providerState.primary.up(time.Now()) {
		t.Error("primary still up; the exhausted round must still bench it")
	}
	if !providerState.fallback.up(time.Now()) {
		t.Error("fallback benched; the skipped retry must not touch it")
	}
}

// TestRunJailedFallbackFailureKeepsPrimaryClassification pins the "never
// overwrite the classification" claim: a fallback round that fails any way
// other than exhaustion leaves the returned error the primary's
// ErrAPIExhausted — the workflow answers killed/busy rounds differently,
// and what matters downstream is that the primary is dry. The fallback is
// not benched by a non-exhaustion failure.
func TestRunJailedFallbackFailureKeepsPrimaryClassification(t *testing.T) {
	resetProviderState(t)
	armFallback(t)
	t.Setenv("ANTHROPIC_API_KEY", "sk-primary")
	log := newStubLog(t)
	stubBin(t, "ai-jail", primaryExhaustedFallbackFailsStubBody)

	_, err := runJailed(context.Background(), "", t.TempDir(), "prompt")
	if !errors.Is(err, ErrAPIExhausted) {
		t.Fatalf("runJailed error = %v, want the primary's ErrAPIExhausted", err)
	}
	if strings.Contains(err.Error(), "agent killed by signal") {
		t.Errorf("error = %q; the fallback's own failure must not leak into the returned error", err)
	}
	calls := readCalls(t, log)
	if len(calls) != 2 {
		t.Fatalf("got %d jailed calls, want 2 (primary round then fallback retry)", len(calls))
	}
	if got := calls[1].Env["ANTHROPIC_API_KEY"]; got != "sk-backup" {
		t.Errorf("retry round key = %q, want the fallback's", got)
	}
	providerState.Lock()
	defer providerState.Unlock()
	if providerState.primary.up(time.Now()) {
		t.Error("primary still up; its exhausted round must bench it")
	}
	if !providerState.fallback.up(time.Now()) {
		t.Error("fallback benched by a non-exhaustion failure; only exhaustion benches")
	}
}

// TestRunJailedFallbackAlsoExhaustedBenchesBoth pins the both-dry path: a
// fallback retry that also exhausts benches the fallback, so the next round
// fast-fails, and the returned error is still the exhaustion sentinel.
func TestRunJailedFallbackAlsoExhaustedBenchesBoth(t *testing.T) {
	resetProviderState(t)
	armFallback(t)
	t.Setenv("ANTHROPIC_API_KEY", "sk-primary")
	log := newStubLog(t)
	stubBin(t, "ai-jail", alwaysExhaustedStubBody)

	_, err := runJailed(context.Background(), "", t.TempDir(), "prompt")
	if !errors.Is(err, ErrAPIExhausted) {
		t.Fatalf("runJailed error = %v, want ErrAPIExhausted", err)
	}
	calls := readCalls(t, log)
	if len(calls) != 2 {
		t.Fatalf("got %d jailed calls, want 2 (primary round then fallback retry)", len(calls))
	}
	if got := calls[0].Env["ANTHROPIC_API_KEY"]; got != "sk-primary" {
		t.Errorf("first round key = %q, want the primary's", got)
	}
	if got := calls[1].Env["ANTHROPIC_API_KEY"]; got != "sk-backup" {
		t.Errorf("retry round key = %q, want the fallback's", got)
	}
	providerState.Lock()
	defer providerState.Unlock()
	if providerState.primary.up(time.Now()) || providerState.fallback.up(time.Now()) {
		t.Error("both providers must be benched after each side exhausted a round")
	}
}

// TestRunJailedHeldPrimarySkipsStraightToFallback pins the sticky side:
// while the primary is benched, rounds start on the fallback directly —
// no wasted primary round — and revert automatically once the hold
// expires.
func TestRunJailedHeldPrimarySkipsStraightToFallback(t *testing.T) {
	resetProviderState(t)
	armFallback(t)
	t.Setenv("ANTHROPIC_API_KEY", "sk-primary")
	log := newStubLog(t)
	stubBin(t, "ai-jail", "exit 0")

	providerState.Lock()
	providerState.primary.downUntil = time.Now().Add(time.Hour)
	providerState.Unlock()

	if _, err := runJailed(context.Background(), "", t.TempDir(), "prompt"); err != nil {
		t.Fatalf("runJailed: %v", err)
	}
	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("got %d jailed calls, want 1", len(calls))
	}
	if got := calls[0].Env["ANTHROPIC_API_KEY"]; got != "sk-backup" {
		t.Errorf("round key = %q, want the fallback's while the primary is held", got)
	}

	// The hold expiring reverts rounds to the primary.
	providerState.Lock()
	providerState.primary.downUntil = time.Now().Add(-time.Second)
	providerState.Unlock()
	if _, err := runJailed(context.Background(), "", t.TempDir(), "prompt"); err != nil {
		t.Fatalf("runJailed: %v", err)
	}
	calls = readCalls(t, log)
	if len(calls) != 2 {
		t.Fatalf("got %d jailed calls, want 2", len(calls))
	}
	if got := calls[1].Env["ANTHROPIC_API_KEY"]; got != "sk-primary" {
		t.Errorf("round key = %q after the hold expired, want the primary's", got)
	}
}

// TestRunJailedBothHeldFastFails pins that a round arriving while both
// providers are benched returns the exhaustion sentinel without launching
// an agent at all — the workflow's hourly loop is the recheck.
func TestRunJailedBothHeldFastFails(t *testing.T) {
	resetProviderState(t)
	armFallback(t)
	log := newStubLog(t)
	stubBin(t, "ai-jail", "exit 0")

	providerState.Lock()
	providerState.primary.downUntil = time.Now().Add(time.Hour)
	providerState.fallback.downUntil = time.Now().Add(time.Hour)
	providerState.Unlock()

	_, err := runJailed(context.Background(), "", t.TempDir(), "prompt")
	if !errors.Is(err, ErrAPIExhausted) {
		t.Fatalf("runJailed error = %v, want ErrAPIExhausted", err)
	}
	// The stub never ran, so its log may not exist — no file IS the
	// zero-call assertion.
	if _, statErr := os.Stat(log); statErr == nil {
		if calls := readCalls(t, log); len(calls) != 0 {
			t.Fatalf("got %d jailed calls, want 0 while both providers are held", len(calls))
		}
	}
}

// TestRunJailedWithoutFallbackKeepsLegacyBehavior pins that with no
// fallback armed, an exhausted round is exactly what it was before
// failover existed: one round, the exhaustion error, no benching that
// changes the next round's provider.
func TestRunJailedWithoutFallbackKeepsLegacyBehavior(t *testing.T) {
	resetProviderState(t)
	t.Setenv("ANTHROPIC_API_KEY", "sk-primary")
	log := newStubLog(t)
	stubBin(t, "ai-jail", exhaustedStubBody)

	_, err := runJailed(context.Background(), "", t.TempDir(), "prompt")
	if !errors.Is(err, ErrAPIExhausted) {
		t.Fatalf("runJailed error = %v, want ErrAPIExhausted", err)
	}
	if calls := readCalls(t, log); len(calls) != 1 {
		t.Fatalf("got %d jailed calls, want 1 (no fallback to retry on)", len(calls))
	}
}

// TestMarkProviderDryEscalation pins the 20m/40m/60m hold schedule for
// exhaustion output with no parseable reset stamp, and the 5h cumulative
// cap after which holds stop extending.
func TestMarkProviderDryEscalation(t *testing.T) {
	resetProviderState(t)
	// Freeze the schedule's reference times by driving drySince directly.
	now := time.Now()
	providerState.Lock()
	providerState.primary.drySince = now
	providerState.Unlock()

	for _, want := range []time.Duration{20 * time.Minute, 40 * time.Minute, time.Hour, time.Hour} {
		until := markProviderDry(sidePrimary, "no reset stamp in here")
		providerState.Lock()
		got := until.Sub(now)
		spent := now.Sub(providerState.primary.drySince)
		providerState.Unlock()
		_ = spent
		// The hold is measured from each call's now; the first three land
		// within the same instant in a test, so compare against the
		// schedule entry with slack for the call-to-call drift.
		if got < want-time.Minute || got > want+time.Minute {
			t.Errorf("hold = %v, want %v", got, want)
		}
	}

	// Past the 5h budget the hold stops extending (step clamps to zero).
	providerState.Lock()
	providerState.primary.drySince = now.Add(-dryHoldBudget - time.Minute)
	providerState.Unlock()
	until := markProviderDry(sidePrimary, "still no stamp")
	if until.After(time.Now().Add(time.Minute)) {
		t.Errorf("hold = %v past the budget, want no extension", time.Until(until))
	}
}

// TestMarkProviderDryParsesResetStamp pins that a parseable reset stamp
// wins over the escalating schedule and never benches into the past.
func TestMarkProviderDryParsesResetStamp(t *testing.T) {
	resetProviderState(t)
	// Far-future stamp in the observed z.ai format (UTC+8).
	until := markProviderDry(sideFallback, `429 · [1308][Usage limit reached for 5 hour. Your limit will reset at 2099-01-01 09:13:00]`)
	want := time.Date(2099, 1, 1, 1, 13, 0, 0, time.UTC)
	if until.Sub(want) > time.Minute || want.Sub(until) > time.Minute {
		t.Errorf("downUntil = %v, want %v", until, want)
	}

	// A stamp already in the past holds nothing. The message carries the
	// z.ai signature so the stamp is actually parsed (a signature-less
	// one takes the escalating schedule instead — pinned in probe_test).
	until = markProviderDry(sideFallback, `429 · [1308][Usage limit reached for 5 hour. Your limit will reset at 2000-01-01 00:00:00]`)
	if until.After(time.Now().Add(time.Minute)) {
		t.Errorf("downUntil = %v for a past stamp, want ~now", until)
	}
}

// TestOtherProviderEnvFallbackSide pins the fallback round's failover
// candidate: with the fallback benched and the primary up, the candidate is
// the primary's env (the auto-revert path), and with both sides benched
// there is no immediate retry in either direction.
func TestOtherProviderEnvFallbackSide(t *testing.T) {
	resetProviderState(t)
	armFallback(t)
	t.Setenv("ANTHROPIC_API_KEY", "sk-primary")

	t.Run("benched fallback reverts to the primary", func(t *testing.T) {
		markProviderDry(sideFallback, `429 · [1308][Usage limit reached for 5 hour. Your limit will reset at 2099-01-01 09:13:00]`)
		env, side := otherProviderEnv(sideFallback)
		if env == nil || side != sidePrimary {
			t.Fatalf("otherProviderEnv(sideFallback) = env:%v side:%s, want the primary env", env, side)
		}
		if got := envValue(env, "ANTHROPIC_API_KEY"); got != "sk-primary" {
			t.Errorf("candidate key = %q, want the primary's", got)
		}
		if got := envValue(env, "ANTHROPIC_BASE_URL"); got == "https://backup.example" {
			t.Errorf("candidate base url = %q; the primary candidate must not carry fallback overrides", got)
		}
	})

	t.Run("both benched suppresses the retry both ways", func(t *testing.T) {
		providerState.Lock()
		providerState.primary.downUntil = time.Now().Add(time.Hour)
		providerState.fallback.downUntil = time.Now().Add(time.Hour)
		providerState.Unlock()
		if env, side := otherProviderEnv(sidePrimary); env != nil || side != sidePrimary {
			t.Errorf("otherProviderEnv(sidePrimary) = env:%v side:%s, want nil with the side unchanged", env, side)
		}
		if env, side := otherProviderEnv(sideFallback); env != nil || side != sideFallback {
			t.Errorf("otherProviderEnv(sideFallback) = env:%v side:%s, want nil with the side unchanged", env, side)
		}
	})
}

// envValue returns the value of name in an env slice, "" when absent.
func envValue(env []string, name string) string {
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, name+"="); ok {
			return v
		}
	}
	return ""
}

// TestFallbackEnvHeartbeatFallback pins that a fallback round without a
// heartbeat model of its own names the fallback's main model as the
// small/fast model (the neither-set clearing branch is pinned separately,
// in TestFallbackEnvWithoutAnyModelClearsHeartbeatVar).
func TestFallbackEnvHeartbeatFallback(t *testing.T) {
	t.Setenv("DAEDALUS_FALLBACK_BASE_URL", "https://backup.example")
	t.Setenv("DAEDALUS_FALLBACK_API_KEY", "sk-backup")
	t.Setenv("DAEDALUS_FALLBACK_MODEL", "glm-backup")
	t.Setenv("ANTHROPIC_DEFAULT_HAIKU_MODEL", "primary-air")
	t.Setenv("DAEDALUS_FALLBACK_HEARTBEAT_MODEL", "")
	env := fallbackEnv()
	got := ""
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, "ANTHROPIC_DEFAULT_HAIKU_MODEL="); ok {
			got = v
		}
	}
	if got != "glm-backup" {
		t.Errorf("fallback heartbeat model = %q, want the fallback main model", got)
	}
}

// TestFallbackEnvWithoutAnyModelClearsHeartbeatVar pins the neither-model
// branch of fallbackEnv: with no fallback heartbeat model and no fallback
// main model, the primary's inherited ANTHROPIC_DEFAULT_HAIKU_MODEL is
// removed from the round env — absence of a new value would not be enough,
// the stale primary value must not survive — so the agent's own default
// applies. Production-dead today (fallbackConfigured requires
// DAEDALUS_FALLBACK_MODEL, so hb is never empty on a real failover round);
// tested directly to pin the defensive behavior, flagged for the reviewer.
func TestFallbackEnvWithoutAnyModelClearsHeartbeatVar(t *testing.T) {
	t.Setenv("DAEDALUS_FALLBACK_BASE_URL", "https://backup.example")
	t.Setenv("DAEDALUS_FALLBACK_API_KEY", "sk-backup")
	t.Setenv("DAEDALUS_FALLBACK_MODEL", "")
	t.Setenv("DAEDALUS_FALLBACK_HEARTBEAT_MODEL", "")
	t.Setenv("ANTHROPIC_DEFAULT_HAIKU_MODEL", "primary-air")
	for _, kv := range fallbackEnv() {
		if name, _, ok := cutEnv(kv); ok && name == "ANTHROPIC_DEFAULT_HAIKU_MODEL" {
			t.Errorf("fallback env still carries %q; the inherited heartbeat model must be removed", kv)
		}
	}
}
