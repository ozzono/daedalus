package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

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

// TestRunJailedClaudeActivityAiderOpenAIModel pins the model wiring an
// openai-configured round gets on top of the base aider argv: the argv
// flags select the prefixed model and point at the staged metadata and
// settings files (the prefix is what routes aider's litellm layer to the
// custom endpoint, so neither half is redundant), the metadata file is a
// JSON object keyed by that prefixed model name (aider parses it with
// json5.loads — a YAML shape would silently fall back to litellm's tiny
// default budget), the settings file caps the wire request's max_tokens,
// and OPENAI_API_BASE rides the round env next to OPENAI_BASE_URL — the
// failover bridge for environments that never went through AgentEnv.
func TestRunJailedClaudeActivityAiderOpenAIModel(t *testing.T) {
	fakeAiderInstall(t, "tree")
	log := newStubLog(t)
	stubBin(t, "ai-jail", "exit 0")
	t.Setenv("DAEDALUS_AGENT", "aider")
	t.Setenv("OPENAI_MODEL", "glm-selfhost")
	t.Setenv("OPENAI_BASE_URL", "https://selfhost.example/v1")
	wt := gitRepo(t)

	if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
		WorktreePath: wt,
		Prompt:       "fix the bug",
	}); err != nil {
		t.Fatalf("RunJailedClaudeActivity: %v", err)
	}

	calls := readCalls(t, log)
	if len(calls) != 1 {
		t.Fatalf("ai-jail called %d times, want 1", len(calls))
	}
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

	if got := calls[0].Env["OPENAI_API_BASE"]; got != "https://selfhost.example/v1" {
		t.Errorf("round env OPENAI_API_BASE = %q, want OPENAI_BASE_URL's value", got)
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

// TestRunJailedClaudeActivityAiderContextTokensOverride pins the
// context-window channel for aider rounds: a config.ContextTokensEnv
// export (anthropic.context_tokens) replaces the staged metadata's input
// side per round, while the completion cap stays at the constant — the
// window says nothing about output size. A non-positive value is not a
// window and falls back to the constant. The fallback values with the env
// unset are pinned by TestRunJailedClaudeActivityAiderOpenAIModel;
// TestMain scrubs the ambient var so both pins are host-independent.
func TestRunJailedClaudeActivityAiderContextTokensOverride(t *testing.T) {
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

			if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
				WorktreePath: wt,
				Prompt:       "fix the bug",
			}); err != nil {
				t.Fatalf("RunJailedClaudeActivity: %v", err)
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

// TestRunJailedClaudeActivityAiderOutputTokensOverride pins the
// completion-cap channel for aider rounds: a config.MaxOutputTokensEnv
// export (anthropic.max_output_tokens) replaces both sides of the 8192
// constant per round — the staged metadata's max_output_tokens and the
// settings' extra_params.max_tokens — while a non-positive value is not a
// cap and falls back to the constant. The fallback pins with the env unset
// live in TestRunJailedClaudeActivityAiderOpenAIModel; TestMain scrubs the
// ambient var so all pins are host-independent.
func TestRunJailedClaudeActivityAiderOutputTokensOverride(t *testing.T) {
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

			if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
				WorktreePath: wt,
				Prompt:       "fix the bug",
			}); err != nil {
				t.Fatalf("RunJailedClaudeActivity: %v", err)
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

// TestRunJailedClaudeActivityAiderSamplers pins the sampler channel for
// aider rounds: the DAEDALUS_* exports (config.yaml's openai section) are
// staged into the settings file — top_p, presence_penalty, and
// repetition_penalty at extra_params' top level, top_k and min_p under
// extra_body — every float renders as a YAML float literal (always a
// decimal point, so pyyaml never reads it as a string), a malformed export
// is silently ignored rather than killing the round, and the whole file
// must still parse as YAML with float-typed params.
func TestRunJailedClaudeActivityAiderSamplers(t *testing.T) {
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
			t.Setenv(config.TopKEnv, "40")
			wt := gitRepo(t)

			if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
				WorktreePath: wt,
				Prompt:       "fix the bug",
			}); err != nil {
				t.Fatalf("RunJailedClaudeActivity: %v", err)
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

			if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
				WorktreePath: wt,
				Prompt:       "fix the bug",
			}); err != nil {
				t.Fatalf("RunJailedClaudeActivity: %v", err)
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

		if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
			WorktreePath: gitRepo(t),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedClaudeActivity: %v", err)
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

		if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
			WorktreePath: gitRepo(t),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedClaudeActivity: %v", err)
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

		if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
			WorktreePath: gitRepo(t),
			Prompt:       "fix the bug",
		}); err != nil {
			t.Fatalf("RunJailedClaudeActivity: %v", err)
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
			if got := calls[0].Env["MAX_THINKING_TOKENS"]; got != c.want {
				t.Errorf("round env MAX_THINKING_TOKENS = %q under thinking %s, want %q", got, c.thinking, c.want)
			}
		})
	}
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

			if _, err := RunJailedClaudeActivity(context.Background(), AgentRunInput{
				WorktreePath: wt,
				Prompt:       "fix the bug",
			}); err != nil {
				t.Fatalf("RunJailedClaudeActivity: %v", err)
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
