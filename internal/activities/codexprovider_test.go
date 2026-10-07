package activities

import (
	"slices"
	"strings"
	"testing"
)

// codexEnv builds a round env slice from K=V pairs, the shape
// stageCodexProvider reads (the round's explicit env, not the process's).
func codexEnv(kv ...string) []string {
	return kv
}

// stagedCodexArgs returns the -c/-m overrides stageCodexProvider composes
// for a url+model round — the argv the round-level pins expect after the
// headless set. keyed adds the env_key pair (the key rides the env var
// codex reads at request time, never argv).
func stagedCodexArgs(url string, keyed bool, model string) []string {
	provider := "model_providers." + codexProviderID
	args := []string{
		"-c", provider + `.base_url="` + url + `"`,
		"-c", provider + `.wire_api="responses"`,
	}
	if keyed {
		args = append(args, "-c", provider+`.env_key="OPENAI_API_KEY"`)
	}
	return append(args,
		"-c", `model_provider="`+codexProviderID+`"`,
		"-m", model)
}

// TestStageCodexProviderNoOpenaiVars pins the pass-through shape: a round
// env with none of the openai exports stages nothing and adds no flag —
// codex follows its own host config untouched (the -c overrides layer over
// it per launch; with no section there is nothing to layer).
func TestStageCodexProviderNoOpenaiVars(t *testing.T) {
	args, err := stageCodexProvider(codexEnv("PATH=/bin", "ANTHROPIC_API_KEY=sk-a"))
	if err != nil {
		t.Fatalf("stageCodexProvider: %v", err)
	}
	if args != nil {
		t.Errorf("stageCodexProvider args = %v, want nil", args)
	}
}

// TestStageCodexProviderUnbridgeableShapes pins the fail-before-launch
// guard: codex reads no OPENAI_* var natively, so a half-specified section
// cannot be bridged — a key or model without a URL would leave the round
// dialing the host's own codex config, and a URL without a model has
// nothing to select. Both shapes fail the round and stage nothing.
func TestStageCodexProviderUnbridgeableShapes(t *testing.T) {
	for _, c := range []struct {
		name string
		env  []string
		want string
	}{
		{"key without url", codexEnv("OPENAI_API_KEY=sk-ambient"), "without a base URL"},
		{"model without url", codexEnv("OPENAI_MODEL=qwen"), "without a base URL"},
		{"url without model", codexEnv("OPENAI_BASE_URL=http://localhost:4000"), "without openai.model"},
	} {
		t.Run(c.name, func(t *testing.T) {
			args, err := stageCodexProvider(c.env)
			if err == nil {
				t.Fatalf("stageCodexProvider(%v) = %v, want an error", c.env, args)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error = %q, want it to name the missing half (%q)", err, c.want)
			}
			if args != nil {
				t.Errorf("args = %v, want nil on the failure path", args)
			}
		})
	}
}

// TestStageCodexProviderStagesOverrides pins the happy bridge: a url+model
// round returns the -c overrides selecting the staged daedalus provider —
// base_url at the round's endpoint, wire_api pinned to codex's responses
// wire, model_provider selecting the entry — plus -m for the model. With a
// key the entry names the env var codex reads it from at request time (the
// literal key value must never land in argv); without one the pair is
// omitted entirely so a keyless backend gets no auth header.
func TestStageCodexProviderStagesOverrides(t *testing.T) {
	t.Run("keyed backend names the env var, never the key", func(t *testing.T) {
		args, err := stageCodexProvider(codexEnv(
			"OPENAI_BASE_URL=https://llm.example/v1",
			"OPENAI_API_KEY=sk-secret-do-not-stage",
			"OPENAI_MODEL=glm",
		))
		if err != nil {
			t.Fatalf("stageCodexProvider: %v", err)
		}
		want := stagedCodexArgs("https://llm.example/v1", true, "glm")
		if !slices.Equal(args, want) {
			t.Errorf("args = %v, want %v", args, want)
		}
		if contains(args, "sk-secret-do-not-stage") {
			t.Error("staged overrides embed the literal key value; only the env var name may travel")
		}
	})

	t.Run("keyless backend omits env_key", func(t *testing.T) {
		args, err := stageCodexProvider(codexEnv(
			"OPENAI_BASE_URL=http://localhost:11434/v1",
			"OPENAI_MODEL=qwen2.5-coder:3b",
		))
		if err != nil {
			t.Fatalf("stageCodexProvider: %v", err)
		}
		want := stagedCodexArgs("http://localhost:11434/v1", false, "qwen2.5-coder:3b")
		if !slices.Equal(args, want) {
			t.Errorf("args = %v, want %v", args, want)
		}
		if contains(args, "env_key") {
			t.Errorf("args %v stage an env_key without a key exported", args)
		}
	})
}

// TestStageCodexProviderAPIBaseFallback pins the var precedence: an env
// carrying only OPENAI_API_BASE (litellm's channel — AgentEnv exports both,
// failover sets both) is bridged at that URL, not rejected as URL-less.
func TestStageCodexProviderAPIBaseFallback(t *testing.T) {
	args, err := stageCodexProvider(codexEnv(
		"OPENAI_API_BASE=http://localhost:4000",
		"OPENAI_MODEL=glm-backup",
	))
	if err != nil {
		t.Fatalf("stageCodexProvider: %v", err)
	}
	want := stagedCodexArgs("http://localhost:4000", false, "glm-backup")
	if !slices.Equal(args, want) {
		t.Errorf("args = %v, want %v", args, want)
	}
}

// TestStageCodexProviderQuotesURL pins the TOML-basic-string escaping of
// the bridged URL: both config load routes accept an openai.url carrying a
// quote or a backslash, and raw interpolation into the -c override would
// let the quote terminate the value early and the backslash start an
// escape — so the URL must reach codex's override parser as one valid TOML
// string, control characters included (TOML forbids them raw inside basic
// strings).
func TestStageCodexProviderQuotesURL(t *testing.T) {
	for _, c := range []struct {
		name, url, wantQuoted string
	}{
		{"quote and backslash", `https://llm.example/v1"a\b`, `"https://llm.example/v1\"a\\b"`},
		{"control characters", "https://llm.example/v1?x=\n\a", `"https://llm.example/v1?x=\n\u0007"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			args, err := stageCodexProvider(codexEnv(
				"OPENAI_BASE_URL="+c.url,
				"OPENAI_MODEL=glm",
			))
			if err != nil {
				t.Fatalf("stageCodexProvider: %v", err)
			}
			provider := "model_providers." + codexProviderID
			want := []string{
				"-c", provider + ".base_url=" + c.wantQuoted,
				"-c", provider + `.wire_api="responses"`,
				"-c", `model_provider="` + codexProviderID + `"`,
				"-m", "glm",
			}
			if !slices.Equal(args, want) {
				t.Errorf("args = %v, want %v", args, want)
			}
		})
	}
}
