package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ozzono/daedalus/internal/config"
	"gopkg.in/yaml.v3"
)

// fakeAiderInstall lays out a uv-tools-shaped aider install under a fresh
// temp dir and prepends the launcher's bin dir to PATH, the way LookPath
// finds the real install on a host: <binDir>/aider (symlink) ->
// <venv>/bin/aider, whose #! line names an interpreter per layout — "tree"
// puts it in the shared uv python tree next to tools/ (this host's
// layout), "venv" copies it into the venv itself, "env" writes an
// env-style #!. It returns the bin dir LookPath hits, the tool venv root,
// and the uv python tree root.
func fakeAiderInstall(t *testing.T, layout string) (binDir, venv, pyTree string) {
	t.Helper()
	root := t.TempDir()
	binDir = filepath.Join(root, "local", "bin")
	venv = filepath.Join(root, "uv", "tools", "aider-chat")
	pyTree = filepath.Join(root, "uv", "python", "cpython-3.12.7")
	for _, dir := range []string{binDir, filepath.Join(venv, "bin"), filepath.Join(pyTree, "bin")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("fake aider install: %v", err)
		}
	}
	var shebang string
	switch layout {
	case "tree":
		shebang = "#!" + filepath.Join(pyTree, "bin", "python3")
		if err := os.WriteFile(filepath.Join(pyTree, "bin", "python3"), []byte("fake\n"), 0o755); err != nil {
			t.Fatalf("fake aider install: %v", err)
		}
	case "venv":
		shebang = "#!" + filepath.Join(venv, "bin", "python3")
		if err := os.WriteFile(filepath.Join(venv, "bin", "python3"), []byte("fake\n"), 0o755); err != nil {
			t.Fatalf("fake aider install: %v", err)
		}
	case "env":
		shebang = "#!/usr/bin/env aider"
	default:
		t.Fatalf("fake aider install: unknown layout %q", layout)
	}
	launcher := filepath.Join(venv, "bin", "aider")
	if err := os.WriteFile(launcher, []byte(shebang+"\n"), 0o755); err != nil {
		t.Fatalf("fake aider install: %v", err)
	}
	if err := os.Symlink(launcher, filepath.Join(binDir, "aider")); err != nil {
		t.Fatalf("fake aider install: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return binDir, venv, pyTree
}

// TestAiderJailMounts pins the install resolution behind the jail mounts:
// a uv-tools install yields one identical-src:dst --map pair per path the
// launcher chain touches — the bin dir carrying the symlink, the tool
// venv, and (only when the shebang's interpreter really resolves under
// it) the shared uv python tree — and every unusable layout fails with the
// offending shape named rather than launch a round whose interpreter the
// jail would be missing.
func TestAiderJailMounts(t *testing.T) {
	t.Run("uv tools tree layout", func(t *testing.T) {
		binDir, venv, pyTree := fakeAiderInstall(t, "tree")
		args, err := aiderJailMounts()
		if err != nil {
			t.Fatalf("aiderJailMounts: %v", err)
		}
		// The whole uv python tree is mounted (root/uv/python), not just
		// the versioned subtree the interpreter resolves under.
		pyRoot := filepath.Dir(pyTree)
		want := []string{
			"--map", binDir + ":" + binDir,
			"--map", venv + ":" + venv,
			"--map", pyRoot + ":" + pyRoot,
		}
		if !slices.Equal(args, want) {
			t.Errorf("aiderJailMounts() = %v, want %v", args, want)
		}
	})

	t.Run("interpreter inside the venv needs no python tree", func(t *testing.T) {
		binDir, venv, _ := fakeAiderInstall(t, "venv")
		args, err := aiderJailMounts()
		if err != nil {
			t.Fatalf("aiderJailMounts: %v", err)
		}
		want := []string{"--map", binDir + ":" + binDir, "--map", venv + ":" + venv}
		if !slices.Equal(args, want) {
			t.Errorf("aiderJailMounts() = %v, want %v", args, want)
		}
	})

	t.Run("env-style shebang rejected", func(t *testing.T) {
		fakeAiderInstall(t, "env")
		if _, err := aiderJailMounts(); err == nil || !strings.Contains(err.Error(), "env-style") {
			t.Errorf("aiderJailMounts() err = %v, want an env-style rejection", err)
		}
	})

	t.Run("non-venv launcher layout rejected", func(t *testing.T) {
		root := t.TempDir()
		binDir := filepath.Join(root, "bin")
		real := filepath.Join(root, "opt", "aider")
		for _, dir := range []string{binDir, filepath.Join(root, "opt")} {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
		}
		if err := os.WriteFile(real, []byte("#!/usr/bin/python3\n"), 0o755); err != nil {
			t.Fatalf("write launcher: %v", err)
		}
		if err := os.Symlink(real, filepath.Join(binDir, "aider")); err != nil {
			t.Fatalf("symlink launcher: %v", err)
		}
		t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
		if _, err := aiderJailMounts(); err == nil || !strings.Contains(err.Error(), "does not match") {
			t.Errorf("aiderJailMounts() err = %v, want a layout rejection", err)
		}
	})

	t.Run("no aider on PATH", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		if _, err := aiderJailMounts(); err == nil || !strings.Contains(err.Error(), "resolve aider on PATH") {
			t.Errorf("aiderJailMounts() err = %v, want a PATH resolution failure", err)
		}
	})
}

// stageAiderOpenAIRound is the fixture truth shared by
// TestRunJailedAgentActivityAiderOpenAIModel and the hermeticity
// probe: the aider openai wiring round — stubs, agent/model/endpoint
// env, worktree, run — returning the recorded ai-jail call and the
// worktree for the deeper assertions.
func stageAiderOpenAIRound(t *testing.T) ([]stubCall, string) {
	t.Helper()
	fakeAiderInstall(t, "tree")
	log := newStubLog(t)
	stubBin(t, "ai-jail", "exit 0")
	t.Setenv("DAEDALUS_AGENT", "aider")
	t.Setenv("OPENAI_MODEL", "glm-selfhost")
	t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
	wt := gitRepo(t)

	if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "fix the bug",
	}); err != nil {
		t.Fatalf("RunJailedAgentActivity: %v", err)
	}

	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
	}
	return calls, wt
}

// assertAiderOpenAIEndpoint pins the endpoint wiring both tests assert:
// the round's OPENAI_BASE_URL and the OPENAI_API_BASE bridge beside it
// carry the staged endpoint value, never a rewritten one.
func assertAiderOpenAIEndpoint(t *testing.T, calls []stubCall) {
	t.Helper()
	for _, key := range []string{"OPENAI_BASE_URL", "OPENAI_API_BASE"} {
		if got := calls[0].Env[key]; got != "https://selfhost.example/v1" {
			t.Errorf("round env %s = %q, want OPENAI_BASE_URL's value", key, got)
		}
	}
}

// TestRunJailedAgentActivityAiderOpenAIModel pins the model wiring an
// openai-configured round gets on top of the base aider argv: the argv
// flags select the prefixed model and point at the staged metadata and
// settings files (the prefix is what routes aider's litellm layer to the
// custom endpoint, so neither half is redundant), the metadata file is a
// JSON object keyed by that prefixed model name (aider parses it with
// json5.loads — a YAML shape would silently fall back to litellm's tiny
// default budget), the settings file caps the wire request's max_tokens,
// and OPENAI_API_BASE rides the round env next to OPENAI_BASE_URL — the
// failover bridge for environments that never went through AgentEnv.
func TestRunJailedAgentActivityAiderOpenAIModel(t *testing.T) {
	calls, wt := stageAiderOpenAIRound(t)
	args := calls[0].Args
	mi := slices.Index(args, "--message-file")
	if mi < 0 {
		t.Fatalf("aider args %v carry no --message-file", args)
	}
	// The wiring rides directly after the staged prompt file, before the
	// fixed flag set and headless flags.
	assertArgs(t, args[mi:mi+8], []string{
		"--message-file", args[mi+1],
		"--model", "openai/glm-selfhost",
		"--model-metadata-file", ".daedalus-aider/model.metadata.json",
		"--model-settings-file", ".daedalus-aider/model.settings.yml",
	}, "aider model wiring")

	assertAiderOpenAIEndpoint(t, calls)

	metadata, err := os.ReadFile(filepath.Join(wt, ".daedalus-aider", "model.metadata.json"))
	if err != nil {
		t.Fatalf("read staged metadata: %v", err)
	}
	var parsed map[string]struct {
		MaxInputTokens  int `json:"max_input_tokens"`
		MaxOutputTokens int `json:"max_output_tokens"`
	}
	if err := json.Unmarshal(metadata, &parsed); err != nil {
		t.Fatalf("staged metadata is not a JSON object keyed by model name: %v\n%s", err, metadata)
	}
	entry, ok := parsed["openai/glm-selfhost"]
	if !ok {
		t.Fatalf("staged metadata keys = %v, want an entry for openai/glm-selfhost", keysOf(parsed))
	}
	if entry.MaxInputTokens != 65536 || entry.MaxOutputTokens != 8192 {
		t.Errorf("staged metadata budget = %d/%d, want 65536/8192", entry.MaxInputTokens, entry.MaxOutputTokens)
	}

	settings, err := os.ReadFile(filepath.Join(wt, ".daedalus-aider", "model.settings.yml"))
	if err != nil {
		t.Fatalf("read staged settings: %v", err)
	}
	if want := "- name: openai/glm-selfhost\n  extra_params:\n    max_tokens: 8192\n"; string(settings) != want {
		t.Errorf("staged settings =\n%s\nwant\n%s", settings, want)
	}
}

// TestRunJailedAgentActivityAiderContextTokensOverride pins the
// context-window channel for aider rounds: a config.ContextTokensEnv
// export (anthropic.context_tokens) replaces the staged metadata's input
// side per round, while the completion cap stays at the constant — the
// window says nothing about output size. A non-positive value is not a
// window and falls back to the constant. The fallback values with the env
// unset are pinned by TestRunJailedAgentActivityAiderOpenAIModel;
// TestMain scrubs the ambient var so both pins are host-independent.
func TestRunJailedAgentActivityAiderContextTokensOverride(t *testing.T) {
	for _, c := range []struct {
		name     string
		envValue string
		wantIn   int
	}{
		{"positive value replaces the input side", "131072", 131072},
		{"non-positive value falls back", "0", aiderMaxInputTokens},
	} {
		t.Run(c.name, func(t *testing.T) {
			fakeAiderInstall(t, "tree")
			newStubLog(t)
			stubBin(t, "ai-jail", "exit 0")
			t.Setenv("DAEDALUS_AGENT", "aider")
			t.Setenv("OPENAI_MODEL", "glm-selfhost")
			t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
			t.Setenv(config.ContextTokensEnv, c.envValue)
			wt := gitRepo(t)

			if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
				WorktreePath: wt,
				Prompt:       "fix the bug",
			}); err != nil {
				t.Fatalf("RunJailedAgentActivity: %v", err)
			}

			metadata, err := os.ReadFile(filepath.Join(wt, ".daedalus-aider", "model.metadata.json"))
			if err != nil {
				t.Fatalf("read staged metadata: %v", err)
			}
			var parsed map[string]struct {
				MaxInputTokens  int `json:"max_input_tokens"`
				MaxOutputTokens int `json:"max_output_tokens"`
			}
			if err := json.Unmarshal(metadata, &parsed); err != nil {
				t.Fatalf("staged metadata is not a JSON object keyed by model name: %v\n%s", err, metadata)
			}
			entry, ok := parsed["openai/glm-selfhost"]
			if !ok {
				t.Fatalf("staged metadata keys = %v, want an entry for openai/glm-selfhost", keysOf(parsed))
			}
			if entry.MaxInputTokens != c.wantIn || entry.MaxOutputTokens != aiderMaxOutputTokens {
				t.Errorf("staged metadata budget = %d/%d, want %d/%d",
					entry.MaxInputTokens, entry.MaxOutputTokens, c.wantIn, aiderMaxOutputTokens)
			}
		})
	}
}

// keysOf returns a sorted key list — only for failure messages.
func keysOf[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// TestRunJailedAgentActivityAiderOutputTokensOverride pins the
// completion-cap channel for aider rounds: a config.MaxOutputTokensEnv
// export (anthropic.max_output_tokens) replaces both sides of the 8192
// constant per round — the staged metadata's max_output_tokens and the
// settings' extra_params.max_tokens — while a non-positive value is not a
// cap and falls back to the constant. The fallback pins with the env unset
// live in TestRunJailedAgentActivityAiderOpenAIModel; TestMain scrubs the
// ambient var so all pins are host-independent.
func TestRunJailedAgentActivityAiderOutputTokensOverride(t *testing.T) {
	for _, c := range []struct {
		name     string
		envValue string
		wantOut  int
	}{
		{"positive value replaces both sides", "16384", 16384},
		{"non-positive value falls back", "0", aiderMaxOutputTokens},
	} {
		t.Run(c.name, func(t *testing.T) {
			fakeAiderInstall(t, "tree")
			newStubLog(t)
			stubBin(t, "ai-jail", "exit 0")
			t.Setenv("DAEDALUS_AGENT", "aider")
			t.Setenv("OPENAI_MODEL", "glm-selfhost")
			t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
			t.Setenv(config.MaxOutputTokensEnv, c.envValue)
			wt := gitRepo(t)

			if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
				WorktreePath: wt,
				Prompt:       "fix the bug",
			}); err != nil {
				t.Fatalf("RunJailedAgentActivity: %v", err)
			}

			metadata, err := os.ReadFile(filepath.Join(wt, ".daedalus-aider", "model.metadata.json"))
			if err != nil {
				t.Fatalf("read staged metadata: %v", err)
			}
			var parsed map[string]struct {
				MaxInputTokens  int `json:"max_input_tokens"`
				MaxOutputTokens int `json:"max_output_tokens"`
			}
			if err := json.Unmarshal(metadata, &parsed); err != nil {
				t.Fatalf("staged metadata is not a JSON object keyed by model name: %v\n%s", err, metadata)
			}
			entry, ok := parsed["openai/glm-selfhost"]
			if !ok {
				t.Fatalf("staged metadata keys = %v, want an entry for openai/glm-selfhost", keysOf(parsed))
			}
			if entry.MaxInputTokens != aiderMaxInputTokens || entry.MaxOutputTokens != c.wantOut {
				t.Errorf("staged metadata budget = %d/%d, want %d/%d",
					entry.MaxInputTokens, entry.MaxOutputTokens, aiderMaxInputTokens, c.wantOut)
			}

			settings, err := os.ReadFile(filepath.Join(wt, ".daedalus-aider", "model.settings.yml"))
			if err != nil {
				t.Fatalf("read staged settings: %v", err)
			}
			want := fmt.Sprintf("- name: openai/glm-selfhost\n  extra_params:\n    max_tokens: %d\n", c.wantOut)
			if string(settings) != want {
				t.Errorf("staged settings =\n%s\nwant\n%s", settings, want)
			}
		})
	}
}

// TestRunJailedAgentActivityAiderSamplers pins the sampler channel for
// aider rounds: the DAEDALUS_* exports (config.yaml's openai section) are
// staged into the settings file — top_p, presence_penalty, and
// repetition_penalty at extra_params' top level, top_k and min_p under
// extra_body — every float renders as a YAML float literal (always a
// decimal point, so pyyaml never reads it as a string), a malformed export
// is silently ignored rather than killing the round, and the whole file
// must still parse as YAML with float-typed params.
func TestRunJailedAgentActivityAiderSamplers(t *testing.T) {
	for _, c := range []struct {
		name       string
		topP       string
		wantTopP   string
		wantSubstr string
	}{
		{"set value stages a float literal", "0.95", "0.95", "    top_p: 0.95\n"},
		{"malformed value is ignored", "abc", "", "- name: openai/glm-selfhost\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			fakeAiderInstall(t, "tree")
			newStubLog(t)
			stubBin(t, "ai-jail", "exit 0")
			t.Setenv("DAEDALUS_AGENT", "aider")
			t.Setenv("OPENAI_MODEL", "glm-selfhost")
			t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
			t.Setenv(config.TopPEnv, c.topP)
			t.Setenv(config.TemperatureEnv, "0.3")
			t.Setenv(config.TopKEnv, "40")
			wt := gitRepo(t)

			if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
				WorktreePath: wt,
				Prompt:       "fix the bug",
			}); err != nil {
				t.Fatalf("RunJailedAgentActivity: %v", err)
			}

			settings, err := os.ReadFile(filepath.Join(wt, ".daedalus-aider", "model.settings.yml"))
			if err != nil {
				t.Fatalf("read staged settings: %v", err)
			}
			if c.wantTopP == "" {
				if strings.Contains(string(settings), "top_p") {
					t.Errorf("staged settings staged a malformed top_p:\n%s", settings)
				}
			} else if !strings.Contains(string(settings), c.wantSubstr) {
				t.Errorf("staged settings are missing %q:\n%s", c.wantSubstr, settings)
			}
			if !strings.Contains(string(settings), "    extra_body:\n      top_k: 40.0\n") {
				t.Errorf("staged settings are missing the extra_body top_k float literal:\n%s", settings)
			}

			// The file must parse as YAML with float-typed params — the
			// staged file is aider's own settings input, and a string-typed
			// number would ride the wire request as a string.
			var parsed []struct {
				Name        string         `yaml:"name"`
				ExtraParams map[string]any `yaml:"extra_params"`
			}
			if err := yaml.Unmarshal(settings, &parsed); err != nil {
				t.Fatalf("staged settings are not valid YAML: %v\n%s", err, settings)
			}
			if len(parsed) != 1 || parsed[0].Name != "openai/glm-selfhost" {
				t.Fatalf("staged settings parse = %+v, want one openai/glm-selfhost entry", parsed)
			}
			if c.wantTopP == "" {
				if _, ok := parsed[0].ExtraParams["top_p"]; ok {
					t.Errorf("staged extra_params carried top_p for a malformed export: %v", parsed[0].ExtraParams)
				}
			} else if got, ok := parsed[0].ExtraParams["top_p"].(float64); !ok || got != 0.95 {
				t.Errorf("staged extra_params top_p = %#v, want the float 0.95", parsed[0].ExtraParams["top_p"])
			}
			if got, ok := parsed[0].ExtraParams["temperature"].(float64); !ok || got != 0.3 {
				t.Errorf("staged extra_params temperature = %#v, want the float 0.3 (temperature stages independently of top_p)", parsed[0].ExtraParams["temperature"])
			}
			body, ok := parsed[0].ExtraParams["extra_body"].(map[string]any)
			if !ok {
				t.Fatalf("staged extra_params carry no extra_body map: %v", parsed[0].ExtraParams)
			}
			if got, ok := body["top_k"].(float64); !ok || got != 40.0 {
				t.Errorf("staged extra_body top_k = %#v, want the float 40", body["top_k"])
			}
		})
	}
}

// TestRunJailedAiderStreamFlag pins the streaming toggle's argv seam: only
// a strict DAEDALUS_STREAM=on/off — what an explicit stream: true/false
// exports at worker startup — appends aider's --stream/--no-stream pair
// member after the headless flags; unset (or any other value) sends no
// flag so aider follows its own default, and no other agent ever sees one.
func TestRunJailedAiderStreamFlag(t *testing.T) {
	for _, c := range []struct {
		name   string
		stream string
		set    bool
		agent  string
		want   []string
	}{
		{"on appends --stream", "on", true, "aider", []string{"--stream"}},
		{"off appends --no-stream", "off", true, "aider", []string{"--no-stream"}},
		{"unset stays on the default", "", false, "aider", nil},
		{"garbage value ignored", "auto", true, "aider", nil},
		{"off does not leak to claude", "off", true, "claude", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.set {
				t.Setenv("DAEDALUS_STREAM", c.stream)
			}
			t.Setenv("DAEDALUS_AGENT", c.agent)
			if c.agent == "aider" {
				fakeAiderInstall(t, "tree")
			}
			log := newStubLog(t)
			stubBin(t, "ai-jail", "exit 0")
			wt := t.TempDir()
			if c.agent == "aider" {
				wt = gitRepo(t)
			}

			if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
				WorktreePath: wt,
				Prompt:       "fix the bug",
			}); err != nil {
				t.Fatalf("RunJailedAgentActivity: %v", err)
			}

			calls := readCalls(t, log)
			if len(calls) != 1 {
				t.Fatalf("ai-jail called %d times, want 1", len(calls))
			}
			args := calls[0].Args
			if c.want == nil {
				if slices.Contains(args, "--stream") || slices.Contains(args, "--no-stream") {
					t.Errorf("args %v carry a stream flag, want the agent's own default", args)
				}
				return
			}
			if !slices.Equal(args[len(args)-len(c.want):], c.want) {
				t.Errorf("stream args tail = %v, want %v appended after the headless flags", args[len(args)-len(c.want):], c.want)
			}
		})
	}

	t.Run("thinking and stream flags compose in order", func(t *testing.T) {
		t.Setenv("DAEDALUS_AGENT", "aider")
		t.Setenv("DAEDALUS_THINKING", "off")
		t.Setenv("DAEDALUS_STREAM", "off")
		fakeAiderInstall(t, "tree")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: gitRepo(t),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}

		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		args := calls[0].Args
		tail := args[len(args)-3:]
		if !slices.Equal(tail, []string{"--thinking-tokens", "0", "--no-stream"}) {
			t.Errorf("toggle args tail = %v, want [--thinking-tokens 0 --no-stream]", tail)
		}
	})
}

// TestRunJailedThinkingOffLevers pins the per-agent translation of the one
// thinking-off signal (DAEDALUS_THINKING=off): aider's lever is argv
// (--thinking-tokens 0, appended after the headless flags) and claude's is
// the round env (MAX_THINKING_TOKENS=0, overwriting any preset), while a
// value other than the exact export leaves both agents untouched. pi's
// --thinking off and the no-leak-to-claude argv pin live in
// TestRunJailedPiThinkingFlag; TestMain scrubs the ambient var.
func TestRunJailedThinkingOffLevers(t *testing.T) {
	t.Run("aider gains --thinking-tokens 0", func(t *testing.T) {
		t.Setenv("DAEDALUS_AGENT", "aider")
		t.Setenv("DAEDALUS_THINKING", "off")
		fakeAiderInstall(t, "tree")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: gitRepo(t),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}

		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		args := calls[0].Args
		if n := len(args); n < 2 || args[n-2] != "--thinking-tokens" || args[n-1] != "0" {
			t.Errorf("thinking-off args tail = %v, want --thinking-tokens 0 appended after the headless flags", args)
		}
	})

	t.Run("aider garbage thinking value ignored", func(t *testing.T) {
		t.Setenv("DAEDALUS_AGENT", "aider")
		t.Setenv("DAEDALUS_THINKING", "nope")
		fakeAiderInstall(t, "tree")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: gitRepo(t),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}

		for _, call := range readCalls(t, log) {
			if slices.Contains(call.Args, "--thinking-tokens") {
				t.Errorf("args %v carry a thinking flag for a non-export value", call.Args)
			}
		}
	})

	for _, c := range []struct {
		name     string
		thinking string
		want     string
	}{
		{"off overwrites a preset", "off", "0"},
		{"on preserves a preset", "on", "5"},
	} {
		t.Run("claude round env MAX_THINKING_TOKENS "+c.name, func(t *testing.T) {
			t.Setenv("DAEDALUS_AGENT", "claude")
			t.Setenv("DAEDALUS_THINKING", c.thinking)
			t.Setenv("MAX_THINKING_TOKENS", "5")
			log := newStubLog(t)
			stubBin(t, "ai-jail", `echo "ENV:MAX_THINKING_TOKENS=$MAX_THINKING_TOKENS" >> "$STUB_LOG"
exit 0`)

			if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
				WorktreePath: t.TempDir(),
				Prompt:       "fix the bug",
			}); err != nil {
				t.Fatalf("RunJailedAgentActivity: %v", err)
			}

			calls := readCalls(t, log)
			if len(calls) != 1 {
				t.Fatalf("ai-jail called %d times, want 1", len(calls))
			}
			if got := calls[0].Env["MAX_THINKING_TOKENS"]; got != c.want {
				t.Errorf("round env MAX_THINKING_TOKENS = %q under thinking %s, want %q", got, c.thinking, c.want)
			}
		})
	}
}

// TestRunJailedAiderWireThinking pins the aider staging of the wire-level
// disable-thinking signal end to end: a DAEDALUS_OPENAI_THINKING=off
// export (runWorker's openai disable_thinking export, resolved per role by
// wireThinkingEnv into the round env) lands in the staged model settings as
// extra_body chat_template_kwargs {enable_thinking: false} — the Qwen3-
// template convention, under litellm's extra_body like top_k/min_p — with
// the file typed-parsed so the value rides as a real YAML bool. Only the
// strict "off" stages; the reviewer section's knob never arms a dev round
// (the never-cross pin, through the full chain); and the encoding composes
// with the sampler knobs in one extra_body.
func TestRunJailedAiderWireThinking(t *testing.T) {
	parseSettings := func(t *testing.T, wt string) map[string]any {
		t.Helper()
		settings, err := os.ReadFile(filepath.Join(wt, ".daedalus-aider", "model.settings.yml"))
		if err != nil {
			t.Fatalf("read staged settings: %v", err)
		}
		var parsed []struct {
			ExtraParams map[string]any `yaml:"extra_params"`
		}
		if err := yaml.Unmarshal(settings, &parsed); err != nil {
			t.Fatalf("staged settings are not valid YAML: %v\n%s", err, settings)
		}
		if len(parsed) != 1 {
			t.Fatalf("staged settings parse to %d entries, want 1:\n%s", len(settings), settings)
		}
		return parsed[0].ExtraParams
	}
	checkNoStaging := func(t *testing.T, extra map[string]any) {
		t.Helper()
		if body, ok := extra["extra_body"].(map[string]any); ok {
			if _, ok := body["chat_template_kwargs"]; ok {
				t.Errorf("staged extra_body carried chat_template_kwargs: %v", body)
			}
		}
	}

	t.Run("off stages the disable-thinking encoding", func(t *testing.T) {
		t.Setenv("DAEDALUS_AGENT", "aider")
		t.Setenv(config.OpenAIThinkingEnv, "off")
		fakeAiderInstall(t, "tree")
		newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")
		t.Setenv("OPENAI_MODEL", "glm-selfhost")
		t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
		wt := gitRepo(t)

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: wt,
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}

		body, ok := parseSettings(t, wt)["extra_body"].(map[string]any)
		if !ok {
			t.Fatal("staged extra_params carry no extra_body map")
		}
		kwargs, ok := body["chat_template_kwargs"].(map[string]any)
		if !ok {
			t.Fatalf("staged extra_body chat_template_kwargs = %v, want the disable-thinking map", body["chat_template_kwargs"])
		}
		if v, ok := kwargs["enable_thinking"].(bool); !ok || v {
			t.Errorf("staged enable_thinking = %v, want the bool false", kwargs["enable_thinking"])
		}
	})

	for _, c := range []struct {
		name    string
		envName string
		envVal  string
	}{
		{"garbage value stages nothing", config.OpenAIThinkingEnv, "nope"},
		{"the reviewer knob never arms a dev round", config.ReviewerThinkingEnv, "off"},
		{"unset stages nothing", "", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.envName != "" {
				t.Setenv(c.envName, c.envVal)
			}
			t.Setenv("DAEDALUS_AGENT", "aider")
			fakeAiderInstall(t, "tree")
			newStubLog(t)
			stubBin(t, "ai-jail", "exit 0")
			t.Setenv("OPENAI_MODEL", "glm-selfhost")
			t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
			wt := gitRepo(t)

			if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
				WorktreePath: wt,
				Prompt:       "fix the bug",
			}); err != nil {
				t.Fatalf("RunJailedAgentActivity: %v", err)
			}

			checkNoStaging(t, parseSettings(t, wt))
		})
	}

	t.Run("composes with top_k in one extra_body", func(t *testing.T) {
		t.Setenv("DAEDALUS_AGENT", "aider")
		t.Setenv(config.OpenAIThinkingEnv, "off")
		t.Setenv(config.TopKEnv, "40")
		fakeAiderInstall(t, "tree")
		newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")
		t.Setenv("OPENAI_MODEL", "glm-selfhost")
		t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
		wt := gitRepo(t)

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: wt,
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}

		body, ok := parseSettings(t, wt)["extra_body"].(map[string]any)
		if !ok {
			t.Fatal("staged extra_params carry no extra_body map")
		}
		if got, ok := body["top_k"].(float64); !ok || got != 40.0 {
			t.Errorf("staged extra_body top_k = %#v, want the float 40", body["top_k"])
		}
		kwargs, ok := body["chat_template_kwargs"].(map[string]any)
		if !ok {
			t.Fatalf("staged extra_body chat_template_kwargs = %v, want the disable-thinking map beside top_k", body["chat_template_kwargs"])
		}
		if v, ok := kwargs["enable_thinking"].(bool); !ok || v {
			t.Errorf("staged enable_thinking = %v, want the bool false", kwargs["enable_thinking"])
		}
	})
}

// TestRunJailedClaudeMaxOutputEnv pins claude's completion-cap lever: a
// MaxOutputTokensEnv export (anthropic.max_output_tokens riding the round
// env) is re-exported to the jailed claude as CLAUDE_CODE_MAX_OUTPUT_TOKENS,
// an unset export leaves any existing value untouched, and aider never
// gets the var — its route is the staged file, and only claude's builder
// branch re-exports.
func TestRunJailedClaudeMaxOutputEnv(t *testing.T) {
	for _, c := range []struct {
		name   string
		agent  string
		capSet bool
		want   string
	}{
		{"claude re-exports the cap", "claude", true, "16384"},
		{"claude without a cap keeps the ambient value", "claude", false, "ambient"},
		{"aider ignores claude's lever", "aider", true, "ambient"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("DAEDALUS_AGENT", c.agent)
			t.Setenv("CLAUDE_CODE_MAX_OUTPUT_TOKENS", "ambient")
			if c.capSet {
				t.Setenv(config.MaxOutputTokensEnv, "16384")
			}
			if c.agent == "aider" {
				fakeAiderInstall(t, "tree")
			}
			log := newStubLog(t)
			stubBin(t, "ai-jail", `echo "ENV:CLAUDE_CODE_MAX_OUTPUT_TOKENS=$CLAUDE_CODE_MAX_OUTPUT_TOKENS" >> "$STUB_LOG"
exit 0`)
			wt := t.TempDir()
			if c.agent == "aider" {
				wt = gitRepo(t)
			}

			if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
				WorktreePath: wt,
				Prompt:       "fix the bug",
			}); err != nil {
				t.Fatalf("RunJailedAgentActivity: %v", err)
			}

			calls := readCalls(t, log)
			if len(calls) != 1 {
				t.Fatalf("ai-jail called %d times, want 1", len(calls))
			}
			if got := calls[0].Env["CLAUDE_CODE_MAX_OUTPUT_TOKENS"]; got != c.want {
				t.Errorf("round env CLAUDE_CODE_MAX_OUTPUT_TOKENS = %q for %s, want %q", got, c.agent, c.want)
			}
		})
	}
}

// TestRunJailedBugMirrorMount pins the bug-mirror mount seam in
// runJailedRound: with both mirror envs exported, the round argv gains
// --rw-map <mirror>:<worktree>/<bug dir> before the "--" terminator, and
// both host-side dirs (mirror and mountpoint) are created before spawn —
// an empty mountpoint is invisible to git, and an unusable dir fails the
// round here, because a round that ran unmounted would write bug files
// into the real worktree and stage them onto the branch. Either env empty
// means off: the argv stays free of --rw-map and nothing is created. A
// colon in the composed DEST fails the round before spawn for the same
// SOURCE:DEST ambiguity (the worktree root is not load-validated, so the
// composed path is what gets checked).
func TestRunJailedBugMirrorMount(t *testing.T) {
	runRound := func(t *testing.T, wt string) ([]stubCall, error) {
		t.Helper()
		log := newStubLog(t)
		// Pre-create the log so a round that fails before spawn — never
		// invoking ai-jail, hence never appending — parses as zero calls.
		if err := os.WriteFile(log, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		stubBin(t, "ai-jail", "exit 0")
		_, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: wt,
			Prompt:       "fix the bug",
		})
		return readCalls(t, log), err
	}

	t.Run("configured mirror appends --rw-map and pre-creates dirs", func(t *testing.T) {
		t.Setenv("DAEDALUS_AGENT", "claude")
		wt := t.TempDir()
		mirror := filepath.Join(t.TempDir(), "mirror")
		t.Setenv(config.BugDirEnv, "backlog/bugs")
		t.Setenv(config.BugMirrorEnv, mirror)

		calls, err := runRound(t, wt)
		if err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		args := calls[0].Args
		i := slices.Index(args, "--rw-map")
		if i == -1 || i+1 >= len(args) {
			t.Fatalf("args %v carry no --rw-map <mirror>:<dir> pair", args)
		}
		want := mirror + ":" + filepath.Join(wt, "backlog/bugs")
		if args[i+1] != want {
			t.Errorf("--rw-map spec = %q, want %q", args[i+1], want)
		}
		if term := slices.Index(args, "--"); term == -1 || term < i {
			t.Errorf("--rw-map at %d is not before the -- terminator in %v", i, args)
		}
		for _, dir := range []string{mirror, filepath.Join(wt, "backlog/bugs")} {
			info, err := os.Stat(dir)
			if err != nil || !info.IsDir() {
				t.Errorf("mount dir %s not created before spawn: %v", dir, err)
			}
		}
	})

	for _, c := range []struct{ name, bugDir, mirrorEnv string }{
		{"both envs empty", "", ""},
		{"dir without mirror", "backlog/bugs", ""},
		{"mirror without dir", "", "/host/bugs"},
	} {
		t.Run("off leaves argv unmounted: "+c.name, func(t *testing.T) {
			t.Setenv("DAEDALUS_AGENT", "claude")
			wt := t.TempDir()
			t.Setenv(config.BugDirEnv, c.bugDir)
			t.Setenv(config.BugMirrorEnv, c.mirrorEnv)

			calls, err := runRound(t, wt)
			if err != nil {
				t.Fatalf("RunJailedAgentActivity: %v", err)
			}
			for _, call := range calls {
				if slices.Contains(call.Args, "--rw-map") {
					t.Errorf("args %v carry --rw-map with the mirror off (%s)", call.Args, c.name)
				}
			}
			if _, err := os.Stat(filepath.Join(wt, "backlog/bugs")); !os.IsNotExist(err) {
				t.Errorf("bug mountpoint created under the worktree while off: %v", err)
			}
		})
	}

	t.Run("colon in the composed mountpoint fails before spawn", func(t *testing.T) {
		t.Setenv("DAEDALUS_AGENT", "claude")
		wt := t.TempDir()
		t.Setenv(config.BugDirEnv, "back:log/bugs")
		t.Setenv(config.BugMirrorEnv, filepath.Join(t.TempDir(), "mirror"))

		calls, err := runRound(t, wt)
		if err == nil || !strings.Contains(err.Error(), "--rw-map") || !strings.Contains(err.Error(), ":") {
			t.Fatalf("RunJailedAgentActivity error = %v, want a mount-spec colon rejection", err)
		}
		if len(calls) != 0 {
			t.Errorf("ai-jail called %d times, want the round to fail before spawn", len(calls))
		}
		if _, serr := os.Stat(filepath.Join(wt, "back:log")); !os.IsNotExist(serr) {
			t.Errorf("colon mountpoint created in the real worktree: %v", serr)
		}
	})

	t.Run("unusable mirror dir fails the round", func(t *testing.T) {
		t.Setenv("DAEDALUS_AGENT", "claude")
		wt := t.TempDir()
		blocker := filepath.Join(t.TempDir(), "notdir")
		if err := os.WriteFile(blocker, []byte("file\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Setenv(config.BugDirEnv, "backlog/bugs")
		t.Setenv(config.BugMirrorEnv, filepath.Join(blocker, "mirror"))

		calls, err := runRound(t, wt)
		if err == nil || !strings.Contains(err.Error(), "prepare bug mirror dir") {
			t.Fatalf("RunJailedAgentActivity error = %v, want a mirror-dir preparation failure", err)
		}
		if len(calls) != 0 {
			t.Errorf("ai-jail called %d times, want the round to fail before spawn", len(calls))
		}
	})

	t.Run("unusable mountpoint fails the round", func(t *testing.T) {
		t.Setenv("DAEDALUS_AGENT", "claude")
		wt := t.TempDir()
		if err := os.WriteFile(filepath.Join(wt, "bugs"), []byte("file\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Setenv(config.BugDirEnv, "bugs")
		t.Setenv(config.BugMirrorEnv, filepath.Join(t.TempDir(), "mirror"))

		calls, err := runRound(t, wt)
		if err == nil || !strings.Contains(err.Error(), "prepare bug dir mountpoint") {
			t.Fatalf("RunJailedAgentActivity error = %v, want a mountpoint preparation failure", err)
		}
		if len(calls) != 0 {
			t.Errorf("ai-jail called %d times, want the round to fail before spawn", len(calls))
		}
	})
}

// TestFolderMounts pins the worker-side grant resolution behind the folder
// mounts: no grants mounts nothing; each grant resolves to an absolute
// path (a relative one against the process working directory) appearing at
// <worktree>/.daedalus-folders/<basename>; identical duplicates collapse;
// and every unusable grant fails with the offending shape named — a
// missing path, a plain file, the filesystem root, a colon (the path
// composes into ai-jail's --rw-map <source>:<dest> spec), and two grants
// sharing a basename, the second shadowing the first inside the sandbox.
func TestFolderMounts(t *testing.T) {
	host := t.TempDir()
	shared := filepath.Join(host, "shared")
	if err := os.MkdirAll(shared, 0o755); err != nil {
		t.Fatal(err)
	}
	wt := t.TempDir()

	t.Run("no grants mounts nothing", func(t *testing.T) {
		mounts, err := FolderMounts(wt, nil)
		if err != nil || mounts != nil {
			t.Errorf("FolderMounts(wt, nil) = %v, %v, want nil/nil", mounts, err)
		}
	})

	t.Run("grant resolves to its basename under the mount root", func(t *testing.T) {
		// A relative grant resolves against the process working directory —
		// the same dir Stat sees, so the grant exists from both spellings.
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		relShared, err := filepath.Rel(cwd, shared)
		if err != nil {
			t.Fatal(err)
		}
		mounts, err := FolderMounts(wt, []string{shared, relShared})
		if err != nil {
			t.Fatalf("FolderMounts: %v", err)
		}
		// The two spellings resolve to the same grant and collapse to one
		// mount.
		want := []FolderMount{{Source: shared, Dest: filepath.Join(wt, foldersMountRoot, "shared")}}
		if !slices.Equal(mounts, want) {
			t.Errorf("FolderMounts = %v, want %v", mounts, want)
		}
	})

	for _, c := range []struct{ name, wantSubstr string }{
		{"missing grant", "no such file or directory"},
		{"filesystem root", "filesystem root"},
		{"colon in grant", "--rw-map"},
	} {
		t.Run(c.name+" fails", func(t *testing.T) {
			bad := filepath.Join(host, "gone")
			switch c.name {
			case "filesystem root":
				bad = "/"
			case "colon in grant":
				bad = filepath.Join(host, "co:lon")
				if err := os.MkdirAll(bad, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			mounts, err := FolderMounts(wt, []string{bad})
			if err == nil || !strings.Contains(err.Error(), c.wantSubstr) {
				t.Errorf("FolderMounts(%q) = %v, %v, want it to name %q", bad, mounts, err, c.wantSubstr)
			}
		})
	}

	t.Run("plain file rejected", func(t *testing.T) {
		file := filepath.Join(host, "brief.md")
		if err := os.WriteFile(file, []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := FolderMounts(wt, []string{file}); err == nil || !strings.Contains(err.Error(), "is not a directory") {
			t.Errorf("FolderMounts(file) err = %v, want a not-a-directory rejection", err)
		}
	})

	t.Run("shared basename rejected", func(t *testing.T) {
		second := filepath.Join(t.TempDir(), "shared")
		if err := os.MkdirAll(second, 0o755); err != nil {
			t.Fatal(err)
		}
		_, err := FolderMounts(wt, []string{shared, second})
		if err == nil || !strings.Contains(err.Error(), "share the basename") {
			t.Errorf("FolderMounts(two basenames) err = %v, want a basename-collision rejection", err)
		}
	})
}

// TestRunJailedAgentActivityFolderGrants pins the folder-grant round seam
// in runJailedRound: with grants, the argv gains one --rw-map
// <source>:<dest> pair per grant before the "--" terminator and each
// mountpoint is created host-side before spawn (an empty dir is invisible
// to git), while a fresh conversation's prompt is told where the mounts
// landed. A resumed round still mounts but skips the note (it already
// lives in that conversation), a rotted grant fails the round before
// spawn, and the broken-resume fresh retry — which drops the resume flag —
// relays the note its resumed first launch skipped.
func TestRunJailedAgentActivityFolderGrants(t *testing.T) {
	grant := func(t *testing.T) string {
		t.Helper()
		dir := filepath.Join(t.TempDir(), "shared")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		return dir
	}

	t.Run("grants mount and a fresh prompt is told", func(t *testing.T) {
		scrubBugFilingEnv(t)
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")
		first, second := grant(t), grant(t)
		second = filepath.Join(filepath.Dir(second), "notes")
		if err := os.MkdirAll(second, 0o755); err != nil {
			t.Fatal(err)
		}
		wt := t.TempDir()

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: wt,
			Prompt:       "fix the bug",
			Folders:      []string{first, second},
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}

		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		args := calls[0].Args
		for _, src := range []string{first, second} {
			want := src + ":" + filepath.Join(wt, foldersMountRoot, filepath.Base(src))
			i := slices.Index(args, want)
			if i == -1 {
				t.Fatalf("args %v carry no --rw-map spec %q", args, want)
			}
			if args[i-1] != "--rw-map" {
				t.Errorf("spec %q at %d is not a --rw-map value in %v", want, i, args)
			}
			if term := slices.Index(args, "--"); term == -1 || term < i {
				t.Errorf("--rw-map spec at %d is not before the -- terminator in %v", i, args)
			}
			info, err := os.Stat(filepath.Join(wt, foldersMountRoot, filepath.Base(src)))
			if err != nil || !info.IsDir() {
				t.Errorf("mountpoint for %s not created before spawn: %v", src, err)
			}
			// The prompt names the addressable mount path and the host
			// folder behind it — the agent cannot see host paths.
			if !strings.Contains(calls[0].Stdin, filepath.Join(foldersMountRoot, filepath.Base(src))) ||
				!strings.Contains(calls[0].Stdin, src) {
				t.Errorf("prompt %q says nothing about the mount of %s", calls[0].Stdin, src)
			}
		}
		if !strings.HasPrefix(calls[0].Stdin, "fix the bug") {
			t.Errorf("prompt %q does not start with the round's own prompt", calls[0].Stdin)
		}
	})

	t.Run("no grants mounts nothing", func(t *testing.T) {
		scrubBugFilingEnv(t)
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")
		wt := t.TempDir()

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: wt,
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}

		for _, call := range readCalls(t, log) {
			if slices.Contains(call.Args, "--rw-map") {
				t.Errorf("args %v carry --rw-map without grants", call.Args)
			}
			if call.Stdin != "fix the bug" {
				t.Errorf("stdin = %q, want the bare prompt", call.Stdin)
			}
		}
		if _, err := os.Stat(filepath.Join(wt, foldersMountRoot)); !os.IsNotExist(err) {
			t.Errorf("mount root created in the worktree without grants: %v", err)
		}
	})

	t.Run("a resumed round mounts but skips the note", func(t *testing.T) {
		scrubBugFilingEnv(t)
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")
		src := grant(t)
		wt := t.TempDir()

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: wt,
			Prompt:       "fix the bug",
			SessionID:    "sess-7",
			Folders:      []string{src},
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}

		calls := readCalls(t, log)
		if len(calls) != 1 || !slices.Contains(calls[0].Args, "--rw-map") {
			t.Fatalf("resumed args %v carry no folder mount (%d calls)", calls[0].Args, len(calls))
		}
		if calls[0].Stdin != "fix the bug" {
			t.Errorf("resumed prompt = %q, want the bare prompt — the note already lives in the conversation", calls[0].Stdin)
		}
	})

	t.Run("rotted grant fails before spawn", func(t *testing.T) {
		scrubBugFilingEnv(t)
		log := newStubLog(t)
		// Pre-create the log so a round that fails before spawn — never
		// invoking ai-jail, hence never appending — parses as zero calls.
		if err := os.WriteFile(log, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		stubBin(t, "ai-jail", "exit 0")
		wt := t.TempDir()

		_, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: wt,
			Prompt:       "fix the bug",
			Folders:      []string{filepath.Join(t.TempDir(), "gone")},
		})
		if err == nil {
			t.Fatal("RunJailedAgentActivity on a rotted grant should fail")
		}
		if calls := readCalls(t, log); len(calls) != 0 {
			t.Errorf("ai-jail called %d times, want the round to fail before spawn", len(calls))
		}
		if _, serr := os.Stat(filepath.Join(wt, foldersMountRoot)); !os.IsNotExist(serr) {
			t.Errorf("mount root created in the real worktree for a failed round: %v", serr)
		}
	})

	t.Run("broken resume's fresh retry relays the note", func(t *testing.T) {
		scrubBugFilingEnv(t)
		fakeHome(t)
		src := grant(t)
		wt := t.TempDir()
		log := newStubLog(t)
		stubBin(t, "ai-jail", `for a in "$@"; do
  if [ "$a" = "sess-dead" ]; then
    echo 'claude: no conversation found with session ID sess-dead' >&2
    exit 1
  fi
done
exit 0`)
		// Live-looking record: the pre-flight passes, claude rejects the
		// resume.
		recordSession(context.Background(), wt, RoleDev, "sess-dead")
		plantTranscript(t, wt, "sess-dead")

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: wt,
			Prompt:       "fix the bug",
			Role:         RoleDev,
			Folders:      []string{src},
		}); err != nil {
			t.Fatalf("broken resume must get one fresh start, got %v", err)
		}

		calls := readCalls(t, log)
		if len(calls) != 2 {
			t.Fatalf("ai-jail called %d times, want 2 (failed resume, fresh retry)", len(calls))
		}
		if calls[0].Stdin != "fix the bug" {
			t.Errorf("resumed attempt prompt = %q, want the bare prompt", calls[0].Stdin)
		}
		if !strings.Contains(calls[1].Stdin, filepath.Join(foldersMountRoot, filepath.Base(src))) {
			t.Errorf("fresh retry prompt %q carries no mounts note", calls[1].Stdin)
		}
		for i, call := range calls {
			if !slices.Contains(call.Args, "--rw-map") {
				t.Errorf("attempt %d args %v carry no folder mount", i, call.Args)
			}
		}
	})
}

// opencodeTestEnv points XDG_DATA_HOME at a fresh temp dir and returns the
// jail args the opencode bridge composes from it: the read-write state-dir
// mount (session database and auth) plus the var pass-through that keeps
// the child's own resolution on the mounted root. Exact-argv pins call it
// so a host's real data home can never leak into the expectation.
func opencodeTestEnv(t *testing.T) []string {
	t.Helper()
	dataHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)
	dir := filepath.Join(dataHome, "opencode")
	// Only the mount segment: the XDG_DATA_HOME rider rides the generic
	// env-rider loop (roundEnvRiders covers it), not the mount.
	return []string{"--rw-map", dir + ":" + dir}
}

// codexTestEnv points CODEX_HOME at a fresh temp dir and returns the jail
// args the codex bridge composes from it — same shape as opencodeTestEnv;
// the CODEX_HOME rider likewise rides the generic env-rider loop.
func codexTestEnv(t *testing.T) []string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	return []string{"--rw-map", home + ":" + home}
}

// TestCodexJailMounts pins the state-dir resolution behind the codex jail
// mount: CODEX_HOME wins when the operator set it, ~/.codex otherwise, the
// dir is created before spawn (a fresh host has none yet — the mount cannot
// fail on it), the mount is read-write at the host path, and a colon in the
// dir is rejected before it can compose into the --rw-map spec.
func TestCodexJailMounts(t *testing.T) {
	t.Run("CODEX_HOME honored and created", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "operator-codex-home")
		t.Setenv("CODEX_HOME", dir)
		args, err := codexJailMounts()
		if err != nil {
			t.Fatalf("codexJailMounts: %v", err)
		}
		want := []string{"--rw-map", dir + ":" + dir}
		if !slices.Equal(args, want) {
			t.Errorf("codexJailMounts() = %v, want %v", args, want)
		}
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			t.Errorf("codex state dir stat err = %v, want the dir created before spawn", err)
		}
	})

	t.Run("default is ~/.codex under the home", func(t *testing.T) {
		t.Setenv("CODEX_HOME", "")
		home := fakeHome(t)
		args, err := codexJailMounts()
		if err != nil {
			t.Fatalf("codexJailMounts: %v", err)
		}
		dir := filepath.Join(home, ".codex")
		want := []string{"--rw-map", dir + ":" + dir}
		if !slices.Equal(args, want) {
			t.Errorf("codexJailMounts() = %v, want %v", args, want)
		}
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("default codex state dir stat err = %v, want it created", err)
		}
	})

	t.Run("colon in the dir rejected", func(t *testing.T) {
		t.Setenv("CODEX_HOME", filepath.Join(t.TempDir(), "weird:dir"))
		if _, err := codexJailMounts(); err == nil || !strings.Contains(err.Error(), "must not contain") {
			t.Errorf("codexJailMounts() err = %v, want the colon-guard rejection", err)
		}
	})

	t.Run("unresolvable home fails", func(t *testing.T) {
		t.Setenv("CODEX_HOME", "")
		t.Setenv("HOME", "")
		if _, err := codexJailMounts(); err == nil || !strings.Contains(err.Error(), "resolve codex state dir") {
			t.Errorf("codexJailMounts() err = %v, want a resolution failure", err)
		}
	})
}

// TestOpencodeJailMounts pins the state-dir resolution behind the opencode
// jail mount: XDG_DATA_HOME wins when the operator set it, the home's
// .local/share otherwise, the <data home>/opencode dir is created before
// spawn, and a colon is rejected before composing the --rw-map spec.
func TestOpencodeJailMounts(t *testing.T) {
	t.Run("XDG_DATA_HOME honored and created", func(t *testing.T) {
		dataHome := t.TempDir()
		t.Setenv("XDG_DATA_HOME", dataHome)
		args, err := opencodeJailMounts()
		if err != nil {
			t.Fatalf("opencodeJailMounts: %v", err)
		}
		dir := filepath.Join(dataHome, "opencode")
		want := []string{"--rw-map", dir + ":" + dir}
		if !slices.Equal(args, want) {
			t.Errorf("opencodeJailMounts() = %v, want %v", args, want)
		}
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			t.Errorf("opencode state dir stat err = %v, want the dir created before spawn", err)
		}
	})

	t.Run("default is the home's .local/share", func(t *testing.T) {
		t.Setenv("XDG_DATA_HOME", "")
		home := fakeHome(t)
		args, err := opencodeJailMounts()
		if err != nil {
			t.Fatalf("opencodeJailMounts: %v", err)
		}
		dir := filepath.Join(home, ".local", "share", "opencode")
		want := []string{"--rw-map", dir + ":" + dir}
		if !slices.Equal(args, want) {
			t.Errorf("opencodeJailMounts() = %v, want %v", args, want)
		}
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("default opencode state dir stat err = %v, want it created", err)
		}
	})

	t.Run("colon in the dir rejected", func(t *testing.T) {
		t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "weird:root"))
		if _, err := opencodeJailMounts(); err == nil || !strings.Contains(err.Error(), "must not contain") {
			t.Errorf("opencodeJailMounts() err = %v, want the colon-guard rejection", err)
		}
	})

	t.Run("unresolvable home fails", func(t *testing.T) {
		t.Setenv("XDG_DATA_HOME", "")
		t.Setenv("HOME", "")
		if _, err := opencodeJailMounts(); err == nil || !strings.Contains(err.Error(), "resolve opencode state dir") {
			t.Errorf("opencodeJailMounts() err = %v, want a resolution failure", err)
		}
	})
}

// TestAgentResumeFlag pins the resume-capability table: claude, pi,
// opencode, codex, and hands take an id-addressable resume (codex and
// hands via a subcommand, so their flag is empty — agentResumeArgs
// composes that shape), while aider has none and its rounds
// ignore a set SessionID.
func TestAgentResumeFlag(t *testing.T) {
	for _, c := range []struct {
		agent string
		flag  string
		ok    bool
	}{
		{"claude", "--resume", true},
		{"pi", "--session", true},
		{"opencode", "-s", true},
		{"codex", "", true},
		{"hands", "", true},
		{"aider", "", false},
	} {
		flag, ok := agentResumeFlag(c.agent)
		if flag != c.flag || ok != c.ok {
			t.Errorf("agentResumeFlag(%s) = %q, %v; want %q, %v", c.agent, flag, ok, c.flag, c.ok)
		}
	}
}

// TestJailedAgentCLIRemovedAgentRejectsLoudly pins the amp removal's
// in-flight face: a recorded Agent="amp" resolves to an error naming the
// removal and the -cli restart path — never a silent fallback that would
// run the round on a different CLI and drop its recorded session.
func TestJailedAgentCLIRemovedAgentRejectsLoudly(t *testing.T) {
	selected, _, _, err := jailedAgentCLI("amp")
	if err == nil {
		t.Fatalf("jailedAgentCLI(amp) = %q, nil error; want the loud removal rejection", selected)
	}
	if !strings.Contains(err.Error(), "removed") || !strings.Contains(err.Error(), "-cli") {
		t.Errorf("jailedAgentCLI(amp) error = %q, want the removal notice naming the -cli restart path", err)
	}
}

// TestAgentResumeArgs pins the per-CLI resume compose: claude and pi take
// the generic [flag, id] prefix ahead of their fixed args, while opencode,
// codex, and hands resume only after their subcommand, so the whole
// post-`--` argv is composed — subcommand, resume tokens, and the fixed
// headless set embedded.
func TestAgentResumeArgs(t *testing.T) {
	for _, c := range []struct {
		name      string
		agent     string
		id        string
		agentArgs []string
		want      []string
	}{
		{"claude", "claude", "sess-1", []string{"--output-format", "json"},
			[]string{"--resume", "sess-1", "--output-format", "json"}},
		{"pi", "pi", "sess-2", []string{"-p"},
			[]string{"--session", "sess-2", "-p"}},
		{"opencode", "opencode", "oc-3", nil,
			[]string{"run", "-s", "oc-3", "--auto", "--standalone"}},
		{"codex", "codex", "cx-4", nil,
			[]string{"exec", "resume", "cx-4", "-", "--dangerously-bypass-approvals-and-sandbox"}},
		{"hands", "hands", "h-5", nil,
			[]string{"resume", "h-5", "--format", "json"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := agentResumeArgs(c.agent, c.id, c.agentArgs)
			if !slices.Equal(got, c.want) {
				t.Errorf("agentResumeArgs(%s, %s) = %v, want %v", c.agent, c.id, got, c.want)
			}
		})
	}
}

// TestCodexRoundStagesProvider pins the runJailedRound wiring end to end:
// a codex round served by the openai section launches ai-jail with the
// state-dir bridge and the staged -c overrides (appended after the headless
// set, where the exec subcommand defines them), a codex round with no
// openai exports launches with no -c/-m at all, and a half-specified
// section fails the round before launch.
func TestCodexRoundStagesProvider(t *testing.T) {
	t.Run("openai-served round carries the staged overrides", func(t *testing.T) {
		scrubBugFilingEnv(t)
		cxMount := codexTestEnv(t)
		t.Setenv("DAEDALUS_AGENT", "codex")
		t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
		t.Setenv("OPENAI_API_KEY", "sk-local")
		t.Setenv("OPENAI_MODEL", "glm-selfhost")
		t.Setenv("OPENAI_API_BASE", "https://selfhost.example/v1")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "echo CODEX-OUTPUT; exit 0")

		res, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		})
		if err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}
		if res.Text != "CODEX-OUTPUT\n" {
			t.Errorf("result.Text = %q, want the raw plain-mode output", res.Text)
		}

		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		want := slices.Concat(
			[]string{"--worktree",
				"--no-save-config",
				"--allow-host",
				"selfhost.example"},
			roundEnvRiders(calls[0].Env),
			[]string{"--mask",
				".claude/settings.json",
				"--mask",
				".claude/settings.local.json",
				"--mask",
				homeClaudeMask(t)},
			cxMount,
			[]string{"--",
				"codex",
				"exec",
				"--dangerously-bypass-approvals-and-sandbox"},
			stagedCodexArgs("https://selfhost.example/v1", true, "glm-selfhost"),
		)
		assertArgs(t, calls[0].Args, want, "ai-jail")
	})

	t.Run("plain round launches with no overrides", func(t *testing.T) {
		scrubBugFilingEnv(t)
		cxMount := codexTestEnv(t)
		t.Setenv("DAEDALUS_AGENT", "codex")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}

		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		assertArgs(t, calls[0].Args, slices.Concat(
			[]string{"--worktree",
				"--no-save-config",
				"--allow-host",
				"api.openai.com"},
			roundEnvRiders(calls[0].Env),
			[]string{"--mask",
				".claude/settings.json",
				"--mask",
				".claude/settings.local.json",
				"--mask",
				homeClaudeMask(t)},
			cxMount,
			[]string{"--",
				"codex",
				"exec",
				"--dangerously-bypass-approvals-and-sandbox"},
		), "ai-jail")
	})

	t.Run("ambient key without url fails before launch", func(t *testing.T) {
		scrubBugFilingEnv(t)
		codexTestEnv(t)
		t.Setenv("DAEDALUS_AGENT", "codex")
		t.Setenv("OPENAI_API_KEY", "sk-ambient")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		_, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		})
		if err == nil {
			t.Fatal("RunJailedAgentActivity = nil error, want the unbridgeable-shape failure")
		}
		if !strings.Contains(err.Error(), "without a base URL") {
			t.Errorf("error = %q, want the codex no-base-URL guard message", err)
		}
		if _, statErr := os.Stat(log); !os.IsNotExist(statErr) {
			calls := readCalls(t, log)
			t.Errorf("ai-jail called %d times, want the round failed before launch (%v)", len(calls), calls)
		}
	})

	t.Run("url without model fails before launch", func(t *testing.T) {
		scrubBugFilingEnv(t)
		codexTestEnv(t)
		t.Setenv("DAEDALUS_AGENT", "codex")
		t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		_, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		})
		if err == nil {
			t.Fatal("RunJailedAgentActivity = nil error, want the unbridgeable-shape failure")
		}
		if !strings.Contains(err.Error(), "without openai.model") {
			t.Errorf("error = %q, want the codex no-model guard message", err)
		}
		if _, statErr := os.Stat(log); !os.IsNotExist(statErr) {
			calls := readCalls(t, log)
			t.Errorf("ai-jail called %d times, want the round failed before launch (%v)", len(calls), calls)
		}
	})
}

// TestCodexRoundAPIErrorText pins the plain-text face classification: a
// codex round that exits 0 with a litellm marker in its output is a failed
// round (ErrAPIExhausted), never a silent success — codex joined aider and
// opencode in the exit-0 marker match.
func TestCodexRoundAPIErrorText(t *testing.T) {
	codexTestEnv(t)
	t.Setenv("DAEDALUS_AGENT", "codex")
	newStubLog(t)
	stubBin(t, "ai-jail", "printf 'litellm.ratelimiterror: 429\n'; exit 0")

	_, err := runJailed(context.Background(), "codex", t.TempDir(), "do things")
	if !errors.Is(err, ErrAPIExhausted) {
		t.Fatalf("runJailed error = %v, want ErrAPIExhausted", err)
	}
	if !strings.Contains(err.Error(), "codex round reported an api error and exited clean") {
		t.Errorf("error = %q, want the exit-0 marker classification", err)
	}
}

// argvCarriesPair reports whether args holds flag immediately followed by
// value — the shape every ai-jail egress grant and env rider takes.
func argvCarriesPair(args []string, flag, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

// TestJailEgressHosts pins the filtered-egress derivation table: the host
// of a staged base URL is the grant (hostname only — path and port never
// ride), the agent's serving channel gets its public API host when and
// only when that channel stages no base URL (trigger is URL absence, the
// keyless ambient posture and the default-authed otel path included —
// never credential presence), hands takes either wire through its staged
// provider and has no unconditional default, OPENAI_API_BASE is the
// legacy spelling of the openai URL, a failover swap of the base URL
// moves the grant, both channels behind one host collapse into one entry,
// 127.0.0.1/localhost forward their port while unbridgeable loopback
// addresses fail loud on a strict channel and skip best-effort, the
// capture endpoint and the go/proxy ambients are best-effort, strictness
// follows the serving channel, and a staged strict URL without a usable
// target fails rather than grant a guess.
func TestJailEgressHosts(t *testing.T) {
	t.Run("derivation table", func(t *testing.T) {
		cases := []struct {
			name        string
			agent       string
			env         []string
			wantHosts   []string
			wantPrivate []string
			wantPorts   []int
		}{
			{"custom anthropic url grants its host", "claude", []string{"ANTHROPIC_BASE_URL=https://relay.example/v1", "ANTHROPIC_API_KEY=sk"}, []string{"relay.example"}, nil, nil},
			{"claude key without url grants the channel default", "claude", []string{"ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com"}, nil, nil},
			{"claude model without url grants the channel default", "claude", []string{"ANTHROPIC_MODEL=glm"}, []string{"api.anthropic.com"}, nil, nil},
			{"claude nothing staged still grants the channel default", "claude", nil, []string{"api.anthropic.com"}, nil, nil},
			{"claude auth token alone grants the channel default, trigger is url absence", "claude", []string{"ANTHROPIC_AUTH_TOKEN=sk"}, []string{"api.anthropic.com"}, nil, nil},
			{"custom openai url grants its host, port stripped", "codex", []string{"OPENAI_BASE_URL=https://selfhost.example:8443/v1", "OPENAI_API_KEY=sk"}, []string{"selfhost.example"}, nil, nil},
			{"legacy openai api-base grants its host", "codex", []string{"OPENAI_API_BASE=https://legacy.example/v1", "OPENAI_API_KEY=sk"}, []string{"legacy.example"}, nil, nil},
			{"openai base url wins over the legacy spelling", "codex", []string{"OPENAI_BASE_URL=https://new.example/v1", "OPENAI_API_BASE=https://legacy.example/v1", "OPENAI_API_KEY=sk"}, []string{"new.example"}, nil, nil},
			{"codex key without url grants the channel default", "codex", []string{"OPENAI_API_KEY=sk"}, []string{"api.openai.com"}, nil, nil},
			{"codex model without url grants the channel default", "codex", []string{"OPENAI_MODEL=glm"}, []string{"api.openai.com"}, nil, nil},
			{"codex nothing staged still grants the channel default", "codex", nil, []string{"api.openai.com"}, nil, nil},
			{"openai-wire agent with a bare anthropic credential grants both defaults", "pi", []string{"ANTHROPIC_API_KEY=sk"}, []string{"api.openai.com", "api.anthropic.com"}, nil, nil},
			{"claude staging only openai tooling vars skips the openai default", "claude", []string{"OPENAI_MODEL=glm"}, []string{"api.anthropic.com"}, nil, nil},
			{"unused-channel openai url still grants its host beside the anthropic default", "claude", []string{"OPENAI_BASE_URL=https://selfhost.example/v1", "OPENAI_API_KEY=sk"}, []string{"selfhost.example", "api.anthropic.com"}, nil, nil},
			{"hands stages no unconditional default", "hands", nil, nil, nil, nil},
			{"hands anthropic credential without url grants the anthropic default", "hands", []string{"ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com"}, nil, nil},
			{"hands openai credential without url grants the openai default", "hands", []string{"OPENAI_API_KEY=sk"}, []string{"api.openai.com"}, nil, nil},
			{"hands custom openai url grants its host", "hands", []string{"OPENAI_BASE_URL=https://selfhost.example/v1", "OPENAI_API_KEY=sk"}, []string{"selfhost.example"}, nil, nil},
			{"hands custom anthropic url grants its host", "hands", []string{"ANTHROPIC_BASE_URL=https://relay.example/v1", "ANTHROPIC_API_KEY=sk"}, []string{"relay.example"}, nil, nil},
			{"both channels behind one host collapse", "claude", []string{"ANTHROPIC_BASE_URL=https://relay.example/v1", "ANTHROPIC_API_KEY=sk", "OPENAI_BASE_URL=https://relay.example:8443/v1", "OPENAI_API_KEY=sk"}, []string{"relay.example"}, nil, nil},
			{"failover-swapped url grants the swapped host", "claude", []string{"ANTHROPIC_BASE_URL=https://backup.example", "ANTHROPIC_API_KEY=sk-backup"}, []string{"backup.example"}, nil, nil},
			{"relay-staged anthropic base url forwards its port, default suppressed", "claude", []string{"ANTHROPIC_BASE_URL=http://127.0.0.1:7778/relay/w/anthropic", "ANTHROPIC_API_KEY=sk"}, nil, nil, []int{7778}},
			{"localhost gateway forwards its port", "codex", []string{"OPENAI_BASE_URL=http://localhost:1234/v1", "OPENAI_API_KEY=sk"}, nil, nil, []int{1234}},
			{"hostname stays a plain allow-host", "claude", []string{"ANTHROPIC_BASE_URL=https://selfhost.example/v1", "ANTHROPIC_API_KEY=sk"}, []string{"selfhost.example"}, nil, nil},
			{"private ip gets the private-host grant", "claude", []string{"ANTHROPIC_BASE_URL=http://192.168.1.10:8000", "ANTHROPIC_API_KEY=sk"}, nil, []string{"192.168.1.10"}, nil},
			{"cgnat address gets the private-host grant", "claude", []string{"ANTHROPIC_BASE_URL=http://100.64.1.1:8000", "ANTHROPIC_API_KEY=sk"}, nil, []string{"100.64.1.1"}, nil},
			{"relay and gateway ports collapse", "claude", []string{"ANTHROPIC_BASE_URL=http://127.0.0.1:7778/a", "OPENAI_BASE_URL=http://localhost:7778/b", "ANTHROPIC_API_KEY=sk", "OPENAI_API_KEY=sk"}, nil, nil, []int{7778}},
		}
		for _, tc := range cases {
			gotHosts, gotPrivate, gotPorts, err := jailEgressHosts(tc.env, tc.agent, "")
			if err != nil {
				t.Errorf("%s: jailEgressHosts error = %v, want none", tc.name, err)
				continue
			}
			if !slices.Equal(gotHosts, tc.wantHosts) || !slices.Equal(gotPrivate, tc.wantPrivate) || !slices.Equal(gotPorts, tc.wantPorts) {
				t.Errorf("%s: jailEgressHosts = (%v, %v, %v), want (%v, %v, %v)", tc.name, gotHosts, gotPrivate, gotPorts, tc.wantHosts, tc.wantPrivate, tc.wantPorts)
			}
		}
	})

	// Strictness follows the serving channel: a malformed or unbridgeable
	// URL on the channel the agent actually dials fails the round before
	// launch — a grant derived from it would be a guess, and no reachable
	// grant exists for a loopback --forward-port cannot bridge — while the
	// unused channel's junk skips silently. Hands is strict on both.
	t.Run("strict on the serving channel, silent on the unused one", func(t *testing.T) {
		cases := []struct {
			name  string
			agent string
			env   []string
			want  string
		}{
			{"claude hostless anthropic url fails", "claude", []string{"ANTHROPIC_BASE_URL=not-a-url", "ANTHROPIC_API_KEY=sk"}, "carries no host"},
			{"codex hostless openai url fails", "codex", []string{"OPENAI_BASE_URL=not-a-url", "OPENAI_API_KEY=sk"}, "carries no host"},
			{"hands hostless openai api-base fails", "hands", []string{"OPENAI_API_BASE=not-a-url", "OPENAI_API_KEY=sk"}, "carries no host"},
			{"codex hostless anthropic url skips silently", "codex", []string{"ANTHROPIC_BASE_URL=not-a-url", "OPENAI_API_KEY=sk"}, ""},
			{"claude hostless openai url skips silently", "claude", []string{"ANTHROPIC_API_KEY=sk", "OPENAI_BASE_URL=not-a-url"}, ""},
			{"claude unbridgeable loopback url fails loud", "claude", []string{"ANTHROPIC_BASE_URL=http://127.0.0.2:9000"}, "cannot bridge"},
			{"codex ipv6 loopback url fails loud", "codex", []string{"OPENAI_BASE_URL=http://[::1]:8123"}, "cannot bridge"},
			{"hands unbridgeable loopback url fails on either channel", "hands", []string{"ANTHROPIC_BASE_URL=http://127.0.0.2:9000"}, "cannot bridge"},
			{"codex unbridgeable anthropic loopback skips silently", "codex", []string{"ANTHROPIC_BASE_URL=http://127.0.0.2:9000", "OPENAI_API_KEY=sk"}, ""},
			{"claude link-local url fails loud", "claude", []string{"ANTHROPIC_BASE_URL=http://169.254.1.1:8000"}, "link-local"},
			{"codex metadata address fails loud", "codex", []string{"OPENAI_BASE_URL=http://169.254.169.254"}, "link-local"},
			{"hands link-local url fails on either channel", "hands", []string{"OPENAI_API_BASE=http://169.254.1.1:8000", "OPENAI_API_KEY=sk"}, "link-local"},
			{"codex link-local anthropic url skips silently", "codex", []string{"ANTHROPIC_BASE_URL=http://169.254.1.1:8000", "OPENAI_API_KEY=sk"}, ""},
		}
		for _, tc := range cases {
			hosts, private, ports, err := jailEgressHosts(tc.env, tc.agent, "")
			if tc.want == "" {
				if err != nil {
					t.Errorf("%s: jailEgressHosts error = %v, want a silent skip", tc.name, err)
					continue
				}
				if len(hosts) == 0 && len(private) == 0 && len(ports) == 0 {
					t.Errorf("%s: jailEgressHosts granted nothing, want the serving channel's default", tc.name)
				}
				continue
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%s: jailEgressHosts = (%v, %v, %v, %v), want the %q failure", tc.name, hosts, private, ports, err, tc.want)
			}
		}
	})

	// A portless loopback URL still names a concrete listener through its
	// scheme default — http forwards 80, https 443 — while an unknown
	// scheme has no derivable port: strict fails loud, best-effort skips.
	t.Run("portless loopback urls derive their scheme default port", func(t *testing.T) {
		hosts, private, ports, err := jailEgressHosts([]string{"ANTHROPIC_BASE_URL=http://127.0.0.1/v1", "ANTHROPIC_API_KEY=sk"}, "claude", "")
		if err != nil || len(hosts) != 0 || len(private) != 0 || !slices.Equal(ports, []int{80}) {
			t.Errorf("portless http loopback = (%v, %v, %v, %v), want (nil, nil, 80, nil)", hosts, private, ports, err)
		}
		_, _, ports, err = jailEgressHosts([]string{"ANTHROPIC_BASE_URL=https://127.0.0.1"}, "claude", "")
		if err != nil || !slices.Equal(ports, []int{443}) {
			t.Errorf("portless https loopback = (%v, %v), want (443, nil)", ports, err)
		}
		if _, _, _, err = jailEgressHosts([]string{"ANTHROPIC_BASE_URL=ftp://127.0.0.1"}, "claude", ""); err == nil || !strings.Contains(err.Error(), "no derivable port") {
			t.Errorf("unknown-scheme portless loopback error = %v, want the no-derivable-port failure", err)
		}
	})

	// The capture endpoint is loopback by construction and rides the same
	// listener as the relay: it forwards its port beside the channel
	// default (otel staging never suppresses the default — the
	// default-authed path stages no URL). It is best-effort — a value
	// without a usable target skips silently, and so does an unbridgeable
	// loopback one, unlike the strict provider URLs.
	t.Run("capture endpoint forwards its port and skips best-effort garbage", func(t *testing.T) {
		env := []string{"ANTHROPIC_API_KEY=sk"}
		hosts, _, ports, err := jailEgressHosts(env, "claude", "http://127.0.0.1:7778/v1/metrics")
		if err != nil || !slices.Equal(hosts, []string{"api.anthropic.com"}) || !slices.Equal(ports, []int{7778}) {
			t.Errorf("jailEgressHosts with capture endpoint = (%v, %v, %v), want (api.anthropic.com, 7778, nil)", hosts, ports, err)
		}
		_, _, ports, _ = jailEgressHosts(env, "claude", "")
		if len(ports) != 0 {
			t.Errorf("jailEgressHosts without a capture endpoint forwarded %v, want none", ports)
		}
		hosts, _, ports, err = jailEgressHosts(env, "claude", "not-a-url")
		if err != nil || !slices.Equal(hosts, []string{"api.anthropic.com"}) || len(ports) != 0 {
			t.Errorf("jailEgressHosts with a hostless capture endpoint = (%v, %v, %v), want the channel default only and the endpoint skipped silently", hosts, ports, err)
		}
		_, _, ports, err = jailEgressHosts(env, "claude", "http://127.0.0.2:9000/x")
		if err != nil || len(ports) != 0 {
			t.Errorf("jailEgressHosts with an unbridgeable capture endpoint = (%v, %v), want it skipped silently", ports, err)
		}
	})

	// GOPROXY (every CSV entry that parses to a usable URL — a mid-round
	// fallthrough to a later mirror must not die on egress; direct/off and
	// scheme-less segments skip) and HTTPS_PROXY (uppercase first,
	// net/http's precedence) are operator ambients a cold jailed go test
	// may need: granted when parseable, skipped silently when not — a
	// proxy must carry its scheme — and never failing the round.
	t.Run("go proxy and https proxy grant best-effort", func(t *testing.T) {
		cases := []struct {
			name        string
			env         []string
			wantHosts   []string
			wantPrivate []string
			wantPorts   []int
		}{
			{"goproxy csv head grants its host", []string{"GOPROXY=https://proxy.example/v1,direct", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com", "proxy.example"}, nil, nil},
			{"direct head grants nothing", []string{"GOPROXY=direct", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com"}, nil, nil},
			{"usable entry behind off still grants", []string{"GOPROXY=off,https://proxy.example", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com", "proxy.example"}, nil, nil},
			{"multi-entry chain grants every usable url, skips direct and off", []string{"GOPROXY=https://p1.example,direct,https://p2.example,off", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com", "p1.example", "p2.example"}, nil, nil},
			{"scheme-less goproxy skips", []string{"GOPROXY=127.0.0.1:8080,direct", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com"}, nil, nil},
			{"loopback goproxy forwards its port", []string{"GOPROXY=http://127.0.0.1:8080,direct", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com"}, nil, []int{8080}},
			{"unknown-scheme portless loopback goproxy skips", []string{"GOPROXY=ftp://127.0.0.1", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com"}, nil, nil},
			{"unbridgeable loopback goproxy skips", []string{"GOPROXY=http://127.0.0.2:8080,direct", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com"}, nil, nil},
			{"link-local goproxy head skips", []string{"GOPROXY=http://169.254.1.1:8000,direct", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com"}, nil, nil},
			{"https proxy grants its host", []string{"HTTPS_PROXY=https://proxy.example:3128", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com", "proxy.example"}, nil, nil},
			{"lowercase https proxy grants when uppercase is absent", []string{"https_proxy=https://proxy.example:3128", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com", "proxy.example"}, nil, nil},
			{"uppercase https proxy wins when both are spelled", []string{"HTTPS_PROXY=https://upper.example:3128", "https_proxy=https://lower.example:3128", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com", "upper.example"}, nil, nil},
			{"scheme-less https proxy skips", []string{"HTTPS_PROXY=127.0.0.1:3128", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com"}, nil, nil},
			{"hostless best-effort urls skip where a provider url fails", []string{"GOPROXY=not-a-url", "HTTPS_PROXY=also-junk", "ANTHROPIC_API_KEY=sk"}, []string{"api.anthropic.com"}, nil, nil},
		}
		for _, tc := range cases {
			gotHosts, gotPrivate, gotPorts, err := jailEgressHosts(tc.env, "claude", "")
			if err != nil {
				t.Errorf("%s: jailEgressHosts error = %v, want none", tc.name, err)
				continue
			}
			if !slices.Equal(gotHosts, tc.wantHosts) || !slices.Equal(gotPrivate, tc.wantPrivate) || !slices.Equal(gotPorts, tc.wantPorts) {
				t.Errorf("%s: jailEgressHosts = (%v, %v, %v), want (%v, %v, %v)", tc.name, gotHosts, gotPrivate, gotPorts, tc.wantHosts, tc.wantPrivate, tc.wantPorts)
			}
		}
	})
}

// TestJailArgvEgressPosture pins the composed argv's egress and rider
// posture end to end — the ai-jail 3.x break class: --network must never
// return to the argv (3.x made it unrestricted and mutually exclusive
// with --allow-host, killing every round at execve), an ambient provider
// stray outside the round-env allowlist must not open a rider, relay and
// otel staging turn the loopback listener into --forward-port instead of
// a dead allow-host, and a staged hostless provider URL fails the round
// before ai-jail is ever invoked.
func TestJailArgvEgressPosture(t *testing.T) {
	t.Run("no network flag on claude and codex rounds", func(t *testing.T) {
		scrubBugFilingEnv(t)
		fakeHome(t)
		t.Setenv("ANTHROPIC_API_KEY", "sk-local")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}
		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		if contains(calls[0].Args, "--network") {
			t.Errorf("claude argv %v carries --network, want filtered --allow-host egress only", calls[0].Args)
		}
		if contains(calls[0].Args, "--forward-port") {
			t.Errorf("metrics-off argv %v carries a --forward-port, want none", calls[0].Args)
		}
		if !argvCarriesPair(calls[0].Args, "--allow-host", "api.anthropic.com") {
			t.Errorf("claude argv %v carries no --allow-host api.anthropic.com grant", calls[0].Args)
		}
	})

	t.Run("codex round keeps the same posture on the openai side", func(t *testing.T) {
		scrubBugFilingEnv(t)
		codexTestEnv(t)
		t.Setenv("DAEDALUS_AGENT", "codex")
		t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
		t.Setenv("OPENAI_API_KEY", "sk-local")
		t.Setenv("OPENAI_MODEL", "glm-selfhost")
		t.Setenv("OPENAI_API_BASE", "https://selfhost.example/v1")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}
		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		if contains(calls[0].Args, "--network") {
			t.Errorf("codex argv %v carries --network, want filtered --allow-host egress only", calls[0].Args)
		}
		if !argvCarriesPair(calls[0].Args, "--allow-host", "selfhost.example") {
			t.Errorf("codex argv %v carries no --allow-host selfhost.example grant", calls[0].Args)
		}
		// AgentEnv exports OPENAI_API_BASE alongside OPENAI_BASE_URL, so a
		// real openai-served round carries four openai riders — the legacy
		// spelling's rider drops only if the staging loses the var.
		if !argvCarriesPair(calls[0].Args, "--env", "OPENAI_API_BASE") {
			t.Errorf("codex argv %v carries no rider for the legacy OPENAI_API_BASE spelling", calls[0].Args)
		}
	})

	t.Run("relay-staged round forwards the relay port instead of a dead loopback allow-host", func(t *testing.T) {
		scrubBugFilingEnv(t)
		fakeHome(t)
		t.Setenv("ANTHROPIC_BASE_URL", "https://primary.example")
		t.Setenv("ANTHROPIC_API_KEY", "sk-primary")
		t.Setenv("DAEDALUS_WORKER_NAME", "w-jail")
		t.Setenv(config.MetricsRelayAnthropicEnv, "http://127.0.0.1:7778")
		t.Setenv(config.MetricsRelayTokenEnv, "regtok")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}
		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		if argvCarriesPair(calls[0].Args, "--allow-host", "127.0.0.1") {
			t.Errorf("relay-staged argv %v grants a dead loopback allow-host", calls[0].Args)
		}
		if argvCarriesPair(calls[0].Args, "--allow-host", "primary.example") {
			t.Errorf("relay-staged argv %v still grants the rewritten provider host", calls[0].Args)
		}
		if !argvCarriesPair(calls[0].Args, "--forward-port", "7778") {
			t.Errorf("relay-staged argv %v carries no --forward-port for the relay listener", calls[0].Args)
		}
		if !argvCarriesPair(calls[0].Args, "--env", "ANTHROPIC_API_KEY") {
			t.Errorf("relay-staged argv %v carries no key rider", calls[0].Args)
		}
	})

	t.Run("claude otel round grants the provider host plus the capture forward-port", func(t *testing.T) {
		writeVersionBin(t, "claude", "2.1.304 (Claude Code)\n")
		otelProbeOnce = sync.Once{}
		otelCapable = nil
		t.Cleanup(func() { otelProbeOnce = sync.Once{}; otelCapable = nil })
		scrubBugFilingEnv(t)
		fakeHome(t)
		t.Setenv("ANTHROPIC_API_KEY", "sk-local")
		t.Setenv(config.MetricsOtelEndpointEnv, "http://127.0.0.1:7778/v1/metrics")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}
		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		if !argvCarriesPair(calls[0].Args, "--allow-host", "api.anthropic.com") {
			t.Errorf("otel argv %v carries no provider host grant", calls[0].Args)
		}
		if !argvCarriesPair(calls[0].Args, "--forward-port", "7778") {
			t.Errorf("otel argv %v carries no capture-endpoint forward-port", calls[0].Args)
		}
	})

	t.Run("codex otel round grants the same pair with the endpoint riding argv", func(t *testing.T) {
		writeVersionBin(t, "codex", "codex-cli 0.160.0\n")
		otelProbeOnce = sync.Once{}
		otelCapable = nil
		t.Cleanup(func() { otelProbeOnce = sync.Once{}; otelCapable = nil })
		scrubBugFilingEnv(t)
		codexTestEnv(t)
		t.Setenv("DAEDALUS_AGENT", "codex")
		t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
		t.Setenv("OPENAI_API_KEY", "sk-local")
		t.Setenv("OPENAI_MODEL", "glm-selfhost")
		t.Setenv("OPENAI_API_BASE", "https://selfhost.example/v1")
		t.Setenv(config.MetricsOtelEndpointEnv, "http://127.0.0.1:7778/v1/metrics")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}
		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		if !argvCarriesPair(calls[0].Args, "--allow-host", "selfhost.example") {
			t.Errorf("otel argv %v carries no provider host grant", calls[0].Args)
		}
		if !argvCarriesPair(calls[0].Args, "--forward-port", "7778") {
			t.Errorf("otel argv %v carries no capture-endpoint forward-port", calls[0].Args)
		}
		// The codex round's endpoint never stages into the env (it rides
		// argv overrides) — the grant arrives via the captureEndpoint
		// parameter, proven by the argv overrides riding alongside.
		if !slices.Contains(calls[0].Args, `otel.environment="daedalus"`) {
			t.Errorf("otel argv %v carries no codex -c otel overrides", calls[0].Args)
		}
	})

	t.Run("ambient provider stray outside the round-env allowlist rides no rider", func(t *testing.T) {
		scrubBugFilingEnv(t)
		fakeHome(t)
		t.Setenv("ANTHROPIC_API_KEY", "sk-local")
		// An operator's ambient export: ANTHROPIC_AUTH_TOKEN is not in
		// config.RoundEnvVars — so composition must not open a rider
		// for it even though the invocation env carries it.
		t.Setenv("ANTHROPIC_AUTH_TOKEN", "sk-ambient-stray")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}
		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		if argvCarriesPair(calls[0].Args, "--env", "ANTHROPIC_AUTH_TOKEN") {
			t.Errorf("ambient ANTHROPIC_AUTH_TOKEN leaked a rider into %v", calls[0].Args)
		}
		if !argvCarriesPair(calls[0].Args, "--env", "ANTHROPIC_API_KEY") {
			t.Errorf("staged key lost its rider in %v", calls[0].Args)
		}
	})

	t.Run("openai model-only staging rides its rider but no openai default on the anthropic channel", func(t *testing.T) {
		scrubBugFilingEnv(t)
		fakeHome(t)
		t.Setenv("ANTHROPIC_API_KEY", "sk-local")
		t.Setenv("OPENAI_MODEL", "glm-tooling")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		if _, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedAgentActivity: %v", err)
		}
		calls := readCalls(t, log)
		if len(calls) != 1 {
			t.Fatalf("ai-jail called %d times, want 1", len(calls))
		}
		// The channel default is per serving channel: claude dials the
		// anthropic wire, so its URL absence — not the openai side's
		// staging — decides, and the openai default never fires here.
		if !argvCarriesPair(calls[0].Args, "--allow-host", "api.anthropic.com") {
			t.Errorf("argv %v carries no anthropic channel default", calls[0].Args)
		}
		if argvCarriesPair(calls[0].Args, "--allow-host", "api.openai.com") {
			t.Errorf("argv %v carries an openai default beside the anthropic serving channel", calls[0].Args)
		}
		if !argvCarriesPair(calls[0].Args, "--env", "OPENAI_MODEL") {
			t.Errorf("staged OPENAI_MODEL lost its rider in %v", calls[0].Args)
		}
		if argvCarriesPair(calls[0].Args, "--env", "OPENAI_API_KEY") {
			t.Errorf("unstaged OPENAI_API_KEY gained a rider in %v", calls[0].Args)
		}
	})

	t.Run("unbridgeable loopback url fails the round loud before launch", func(t *testing.T) {
		scrubBugFilingEnv(t)
		fakeHome(t)
		t.Setenv("ANTHROPIC_API_KEY", "sk-local")
		t.Setenv("ANTHROPIC_BASE_URL", "http://127.0.0.2:9000")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		_, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		})
		if err == nil {
			t.Fatal("RunJailedAgentActivity = nil error, want the unbridgeable-loopback failure")
		}
		if !strings.Contains(err.Error(), "cannot bridge") {
			t.Errorf("error = %q, want the loopback bridge failure", err)
		}
		if _, statErr := os.Stat(log); !os.IsNotExist(statErr) {
			calls := readCalls(t, log)
			t.Errorf("ai-jail called %d times, want the round failed before launch (%v)", len(calls), calls)
		}
	})

	// The egress derivation hoists above the concurrency-slot wait: with
	// the slot held, a doomed egress target still fails fast with the
	// egress error — never queued for five minutes to die at the same
	// diagnosis.
	t.Run("egress failure preempts the slot wait", func(t *testing.T) {
		scrubBugFilingEnv(t)
		fakeHome(t)
		t.Setenv("ANTHROPIC_API_KEY", "sk-local")
		t.Setenv("ANTHROPIC_BASE_URL", "not-a-url")
		log := newStubLog(t)
		stubBin(t, "ai-jail", "exit 0")

		lim := agentLimiter()
		lim <- struct{}{}
		defer func() { <-lim }()
		prevWait := slotWaitTimeout
		slotWaitTimeout = 50 * time.Millisecond
		t.Cleanup(func() { slotWaitTimeout = prevWait })

		start := time.Now()
		_, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
			WorktreePath: t.TempDir(),
			Prompt:       "fix the bug",
		})
		if err == nil {
			t.Fatal("RunJailedAgentActivity = nil error, want the hostless-URL failure")
		}
		if !strings.Contains(err.Error(), "carries no host") {
			t.Errorf("error = %q, want the egress derivation to preempt the slot wait (ErrAgentSlotsBusy after %v would mean it queued first)", err, time.Since(start))
		}
		if _, statErr := os.Stat(log); !os.IsNotExist(statErr) {
			calls := readCalls(t, log)
			t.Errorf("ai-jail called %d times, want the round failed before launch (%v)", len(calls), calls)
		}
	})
}

// TestWindowPreflight pins the round-start gate's arithmetic and its
// gate-off edges: no staged window (absent, unparseable, or non-positive)
// means no arithmetic to refuse on and the gate stays closed; the boundary
// is the biased-high chars/4 estimate against window minus the 4096-token
// output reserve, with an estimate exactly at the budget passing and the
// first estimate step over refused — the refusal wrapping the
// ErrPromptOverWindow sentinel and naming the numbers the operator needs
// (agent, estimate, window, reserve).
func TestWindowPreflight(t *testing.T) {
	// Estimate is ceil(len/4): a 16384-char prompt estimates 4096, a
	// 16388-char prompt 4097.
	const atBudget = 16384
	const oneOver = 16388
	for _, c := range []struct {
		name    string
		env     []string
		prompt  string
		wantErr bool
	}{
		{"no window staged", nil, strings.Repeat("x", oneOver), false},
		{"unparseable window", []string{config.ContextTokensEnv + "=big"}, strings.Repeat("x", oneOver), false},
		{"zero window", []string{config.ContextTokensEnv + "=0"}, strings.Repeat("x", oneOver), false},
		{"negative window", []string{config.ContextTokensEnv + "=-5"}, strings.Repeat("x", oneOver), false},
		{"estimate at the budget passes", []string{config.ContextTokensEnv + "=8192"}, strings.Repeat("x", atBudget), false},
		{"surrounding whitespace parsed", []string{config.ContextTokensEnv + "  =  8192  "}, strings.Repeat("x", atBudget), false},
		{"one estimate step over refused", []string{config.ContextTokensEnv + "=8192"}, strings.Repeat("x", oneOver), true},
	} {
		err := windowPreflight(c.prompt, c.env, "pi")
		if got := err != nil; got != c.wantErr {
			t.Errorf("%s: windowPreflight = %v, wantErr %v", c.name, err, c.wantErr)
		}
		if !c.wantErr {
			continue
		}
		if !errors.Is(err, ErrPromptOverWindow) {
			t.Errorf("%s: error does not wrap the sentinel: %v", c.name, err)
		}
		for _, want := range []string{
			"pi round refused before launch",
			"~4097 tokens",
			"window 8192",
			"reserve 4096",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: error %q should contain %q", c.name, err, want)
			}
		}
	}
}

// TestRunJailedAgentActivityWindowPreflightRefusesBeforeLaunch pins the
// gate at the activity boundary: a round whose prompt cannot fit the
// staged window is refused with ErrPromptOverWindow before ai-jail ever
// spawns (no launch, no slot wait — the stub log stays empty), while the
// same round with the gate off (no window staged) or with room in the
// budget launches normally.
func TestRunJailedAgentActivityWindowPreflightRefusesBeforeLaunch(t *testing.T) {
	for _, c := range []struct {
		name    string
		window  string
		wantErr bool
	}{
		{"window one over the reserve refuses the tiny prompt", "4097", true},
		{"no window staged, gate off", "", false},
		{"window with room launches", "131072", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			fakeHome(t)
			log := newStubLog(t)
			stubBin(t, "ai-jail", `printf '%s\n' '{"type":"session","id":"sess-ok"}' '{"type":"message_end","message":{"content":[{"type":"text","text":"done"}]}}'
exit 0`)
			t.Setenv("DAEDALUS_AGENT", "pi")
			if c.window != "" {
				t.Setenv(config.ContextTokensEnv, c.window)
			}

			_, err := RunJailedAgentActivity(context.Background(), AgentRunInput{
				WorktreePath: t.TempDir(),
				Prompt:       "fix the bug",
				Role:         RoleDev,
			})
			if c.wantErr {
				if !errors.Is(err, ErrPromptOverWindow) {
					t.Fatalf("error = %v, want ErrPromptOverWindow", err)
				}
				if _, statErr := os.Stat(log); !os.IsNotExist(statErr) {
					calls := readCalls(t, log)
					t.Errorf("ai-jail called %d times, want the round refused before launch (%v)", len(calls), calls)
				}
				return
			}
			if err != nil {
				t.Fatalf("RunJailedAgentActivity: %v", err)
			}
			calls := readCalls(t, log)
			if len(calls) != 1 {
				t.Fatalf("ai-jail called %d times, want 1", len(calls))
			}
		})
	}
}

// jailedPiRoundEnv is the serving provider's environment a jailed pi round
// arrives with, staged fixed (never os.Environ) like every round-boundary
// pin; PATH carries the stub dir so the stub's own commands resolve inside
// the child. Deliberately openai-free: an OPENAI_* export would send the
// round through stagePiProvider (a ~/.pi staging these pins do not test).
func jailedPiRoundEnv() []string {
	return []string{
		"ANTHROPIC_BASE_URL=https://pi.example",
		"ANTHROPIC_API_KEY=sk-pi",
		"ANTHROPIC_MODEL=pi-model",
		"PATH=" + os.Getenv("PATH"),
	}
}

// TestPiRoundKillsIdenticalToolLoop pins the loop kill at the round
// boundary: a pi round whose structured stream carries three consecutive
// identical tool calls — fresh generated ids, same tool and arguments, the
// degenerate livelock that only its budget would ever end — is killed
// mid-run and classified ErrAgentLoop wrapping ErrAPIExhausted (the
// exhaustion family: provider failover, then the fresh-session retry), not
// ErrAgentKilled (whose continuation reply would resume the very
// conversation that looped). The collection buffer keeps its role: the
// killed round's stdout still carries the stream.
func TestPiRoundKillsIdenticalToolLoop(t *testing.T) {
	if !signalsPermitted() {
		t.Skip("environment forbids signals; the watcher's group kill cannot fire here")
	}
	newStubLog(t)
	stubBin(t, "ai-jail", `printf '%s\n' '{"type":"session","id":"sess-loop"}'
i=0
while true; do
  printf '%s\n' "{\"type\":\"tool_execution_start\",\"id\":\"call-$i\",\"toolName\":\"bash\",\"args\":{\"command\":\"ls\"}}"
  i=$((i+1))
  sleep 0.02
done`)
	// A bounded context: a watcher that never fires leaves the stub
	// looping forever — fail the pin in 30s instead of hanging the suite.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := runJailedRound(ctx, jailedPiRoundEnv(), "", "pi", t.TempDir(), "round prompt")
	if !errors.Is(err, ErrAgentLoop) {
		t.Fatalf("error = %v, want ErrAgentLoop", err)
	}
	if !errors.Is(err, ErrAPIExhausted) {
		t.Errorf("error = %v, want the ErrAgentLoop wrap to carry ErrAPIExhausted (the exhaustion family's remediation)", err)
	}
	if errors.Is(err, ErrAgentKilled) {
		t.Errorf("error = %v, want the loop face to outrank the killed face", err)
	}
	if want := fmt.Sprintf("pi round repeated the identical tool call %d times", toolLoopThreshold); !strings.Contains(err.Error(), want) {
		t.Errorf("error %q should contain %q", err, want)
	}
	if !strings.Contains(res.Stdout, "tool_execution_start") {
		t.Errorf("killed round stdout = %q, want the streamed events (the tee must keep the collection buffer's role)", res.Stdout)
	}
}

// TestPiRoundToleratesSubLoopCalls pins the threshold's underside at the
// round boundary: a clean-exiting pi round with two identical calls (one
// legitimate retry of a flaky command) succeeds, and so does one whose
// identical calls are split by a distinct call — the streak resets, the
// count is not cumulative — with the stream still collected.
func TestPiRoundToleratesSubLoopCalls(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"two identical calls succeed", `printf '%s\n' \
  '{"type":"session","id":"sess-ok"}' \
  '{"type":"tool_execution_start","id":"c1","toolName":"bash","args":{"command":"ls"}}' \
  '{"type":"tool_execution_start","id":"c2","toolName":"bash","args":{"command":"ls"}}'
exit 0`},
		{"a distinct call resets the streak", `printf '%s\n' \
  '{"type":"session","id":"sess-reset"}' \
  '{"type":"tool_execution_start","id":"c1","toolName":"bash","args":{"command":"ls"}}' \
  '{"type":"tool_execution_start","id":"c2","toolName":"bash","args":{"command":"ls"}}' \
  '{"type":"tool_execution_start","id":"c3","toolName":"edit","args":{"path":"a.go"}}' \
  '{"type":"tool_execution_start","id":"c4","toolName":"bash","args":{"command":"ls"}}' \
  '{"type":"tool_execution_start","id":"c5","toolName":"bash","args":{"command":"ls"}}'
exit 0`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			newStubLog(t)
			stubBin(t, "ai-jail", c.body)
			res, err := runJailedRound(context.Background(), jailedPiRoundEnv(), "", "pi", t.TempDir(), "round prompt")
			if err != nil {
				t.Fatalf("runJailedRound: %v", err)
			}
			if !strings.Contains(res.Stdout, `"id":"c2"`) {
				t.Errorf("stdout = %q, want the round's stream collected through the tee", res.Stdout)
			}
		})
	}
}

// TestClaudeRoundRunsUnwrappedByLoopWatcher pins the agent gate: only pi
// rounds tee through the loop watcher — a claude round printing the same
// identical-call stream is not killed (it holds still past where a
// watcher's kill would have landed) and exits clean.
func TestClaudeRoundRunsUnwrappedByLoopWatcher(t *testing.T) {
	newStubLog(t)
	stubBin(t, "ai-jail", `printf '%s\n' \
  '{"type":"tool_execution_start","id":"c1","toolName":"bash","args":{"command":"ls"}}' \
  '{"type":"tool_execution_start","id":"c2","toolName":"bash","args":{"command":"ls"}}' \
  '{"type":"tool_execution_start","id":"c3","toolName":"bash","args":{"command":"ls"}}'
sleep 0.3
exit 0`)
	if _, err := runJailedRound(context.Background(), jailedPiRoundEnv(), "", "claude", t.TempDir(), "round prompt"); err != nil {
		t.Fatalf("runJailedRound: %v", err)
	}
}

// TestNoDeliveryCapturedText pins the salvage extractor: the text between
// the envelope markers, outer whitespace trimmed; "" for a nil error, a
// plain error, a no-delivery wrap without an envelope, and anything past
// the end marker — the cut is a slice, never a trim of the whole error.
func TestNoDeliveryCapturedText(t *testing.T) {
	plan := "Sub-task 1 (implement, adder.go): add the adder."
	enveloped := func(text string) error {
		return fmt.Errorf("%w: hands round died on a no-delivery kill: class no-delivery: strikes\n%s\n%s\n%s",
			ErrAgentNoDelivery, noDeliveryCaptureBegin, text, noDeliveryCaptureEnd)
	}
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"plain error", errors.New("no envelope here"), ""},
		{"sentinel without an envelope", fmt.Errorf("%w: hands round died on a no-delivery kill", ErrAgentNoDelivery), ""},
		{"unrelated error carrying stray text", errors.New("loose prose, no markers"), ""},
		{"envelope extracts the plan", enveloped(plan), plan},
		{"outer whitespace is trimmed", enveloped("\n  " + plan + "  \n"), plan},
		{"multi-line plan survives intact", enveloped("line one\nline two\nline three"), "line one\nline two\nline three"},
		{"content past the end marker is cut off", errors.New(enveloped(plan).Error() + "\ntrailer"), plan},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := NoDeliveryCapturedText(c.err); got != c.want {
				t.Errorf("NoDeliveryCapturedText(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
}

// TestMatchesAnyDigitBoundary pins the all-digit markers' boundary rule on
// the exhausted-text scan: "402" hits "HTTP 402" and "error: 402" and a
// string-edge hit, but never the 402 inside a digit run — the live
// misclassification where a hands round's session uuid (…2a8464029e36)
// classified the round as api-exhausted. A digit-adjacent hit never masks a
// later clean one (the scan resumes past its first byte), non-digit markers
// keep the plain substring with no boundary rule, and the match stays
// case-insensitive.
func TestMatchesAnyDigitBoundary(t *testing.T) {
	cases := []struct {
		name    string
		s       string
		markers []string
		want    bool
	}{
		{"bare status hit", "error: 402", []string{"402"}, true},
		{"status in prose hits", "server said HTTP 402 today", []string{"402"}, true},
		{"string-start hit", "402 payment required", []string{"402"}, true},
		{"string-end hit", "payment required: 402", []string{"402"}, true},
		{"uuid digit run never hits", "session conv-2a8464029e36 closed", []string{"402"}, false},
		{"digit-adjacent on both sides never hits", "14022", []string{"402"}, false},
		{"swallowed occurrence never hits", "4024", []string{"402"}, false},
		{"blocked hit never masks a later clean one", "uuid 2a8464029e36 then HTTP 402", []string{"402"}, true},
		{"two adjacent runs never compose a hit", "402402", []string{"402"}, false},
		{"other four-digit statuses behave alike", "exit code 1429 recorded", []string{"429"}, false},
		{"marker after a space still hits", "exit code: 429", []string{"429"}, true},
		// The live shape, against the production marker list: a no-delivery
		// kill whose stream carries only session uuids must stay generic.
		{"production list stays deaf to uuid digit runs",
			"session conv-2a8464029e36, class no-delivery: 2 strikes",
			apiExhaustionMarkers, false},
		{"production list still hears a real exhaustion",
			"provider said HTTP 429: overloaded",
			apiExhaustionMarkers, true},
		// Non-digit markers keep the plain substring: letters cannot hide
		// inside a digit run, so there is nothing to bound.
		{"non-digit marker keeps the plain substring", "xrate limitx", []string{"rate limit"}, true},
		{"non-digit marker matches in prose", "RATE LIMIT reached for key", apiExhaustionMarkers, true},
		{"non-digit marker unbounded still matches mid-word", "quota8x", []string{"quota"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := matchesAny(c.s, c.markers); got != c.want {
				t.Errorf("matchesAny(%q) = %v, want %v", c.s, got, c.want)
			}
		})
	}
}
