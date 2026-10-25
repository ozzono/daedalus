package main

import (
	"os/exec"
	"strings"
	"testing"

	historypb "go.temporal.io/api/history/v1"

	"github.com/ozzono/daedalus/internal/config"
	"github.com/ozzono/daedalus/internal/workflows"
)

// TestContinuePipelineWindowGate pins the submit-time window gate on the
// continue path: the continue prompt IS the new run's initial task input,
// and the continued run dials the CURRENT config's provider channels, so a
// continue onto a config whose staged window cannot back the prompt is
// refused before any submission side effect — before the aborted branch is
// verified and before the workflow is created — with the same diagnostic
// the start path's runstart.Validate raises. A prompt the staged window
// backs dispatches unchanged.
func TestContinuePipelineWindowGate(t *testing.T) {
	prompt := "carry on with the parser fix"
	prior := func(repo string) workflows.PipelineInput {
		return workflows.PipelineInput{RepoPath: repo, IssueID: "42", Prompt: "implement the feature"}
	}

	t.Run("a staged window that cannot back the prompt refuses before any side effect", func(t *testing.T) {
		// No aborted branch exists for issue 42: if the gate failed to fire
		// (or fired late), the continue would die on verifyBranch's
		// "nothing to continue" instead — the window diagnostic proves the
		// gate ran, its precedence proves it ran first.
		fake := &continueFrontend{histories: map[string][]*historypb.HistoryEvent{
			"wf": {startedInputEvent(t, prior(t.TempDir()))},
		}}
		cfg := serveContinueFake(t, fake)
		// The staged window exactly backs the reserve alone: any prompt
		// leaves the output budget at or below the floor the gate refuses.
		cfg.Anthropic = config.AnthropicConfig{ContextTokens: 4096}

		err := continuePipeline(cfg, "wf", prompt, true)
		if err == nil {
			t.Fatal("continuePipeline = nil error, want the window gate's refusal")
		}
		for _, want := range []string{"staged context_tokens (4096)", "task input"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal %q should contain %q", err, want)
			}
		}
		if strings.Contains(err.Error(), "nothing to continue") {
			t.Errorf("refusal = %q, want the window gate to fire ahead of the branch verification", err)
		}
		if len(fake.starts) != 0 {
			t.Errorf("continue issued %d starts, want 0 (the refusal precedes any submission)", len(fake.starts))
		}
	})

	t.Run("a prompt the staged window backs dispatches unchanged", func(t *testing.T) {
		repo := initGitRepo(t)
		git := func(args ...string) {
			t.Helper()
			full := append([]string{"-C", repo, "-c", "user.email=test@example.com", "-c", "user.name=test"}, args...)
			if out, err := exec.Command("git", full...).CombinedOutput(); err != nil {
				t.Fatalf("git %v: %v: %s", args, err, out)
			}
		}
		// The preserved aborted branch is verifyBranch's precondition — the
		// resume path's own gate, not this pin's subject.
		git("commit", "--allow-empty", "--quiet", "-m", "seed")
		git("branch", "aborted/issue-42")

		fake := &continueFrontend{histories: map[string][]*historypb.HistoryEvent{
			"wf": {startedInputEvent(t, prior(repo))},
		}}
		cfg := serveContinueFake(t, fake)
		cfg.Anthropic = config.AnthropicConfig{ContextTokens: 200000}

		captureStdout(t, func() {
			if err := continuePipeline(cfg, "wf", prompt, true); err != nil {
				t.Fatalf("continuePipeline: %v", err)
			}
		})
		if len(fake.starts) != 1 {
			t.Fatalf("continue issued %d starts, want 1", len(fake.starts))
		}
		if got := dispatchedInput(t, fake.starts[0]).Prompt; got != prompt {
			t.Errorf("dispatched prompt = %q, want the continue prompt through the gate unchanged", got)
		}
	})
}
