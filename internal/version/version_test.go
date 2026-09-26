package version

import "testing"

// TestLdflagsVersionWins pins the precedence: an ldflags-injected value (set
// at link time by `make build`) beats the build-info fallback, and stray
// whitespace is trimmed.
func TestLdflagsVersionWins(t *testing.T) {
	old := ldflagsVersion
	defer func() { ldflagsVersion = old }()
	ldflagsVersion = "v9.9.9\n"
	if got := String(); got != "v9.9.9" {
		t.Errorf("String() = %q, want %q", got, "v9.9.9")
	}
}

// TestFallbackNeverEmpty pins the local-build fallback: with no ldflags
// value, String still reports something non-empty ("(devel)" unless Go
// recorded a real module version at build time).
func TestFallbackNeverEmpty(t *testing.T) {
	if ldflagsVersion != "" {
		t.Skip("ldflags value set — fallback not exercised")
	}
	if v := String(); v == "" {
		t.Error(`String() = "", want a non-empty fallback version`)
	}
}
