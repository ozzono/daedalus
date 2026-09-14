# Daedalus Code Review

**Date:** 2026-08-24
**Scope:** Full codebase review against the technical requirements and design expectations for the workflow engine, activities, CLI, and test suite.

**Method caveat:** `go test ./...` could not be executed in the review environment — TLS interception blocked Go module downloads from `proxy.golang.org` (certificate verification failures), and TCC protections blocked the default module/build caches. The review is static analysis; the test suite's *design* was reviewed, not its green/red state.

**Verdict:** The codebase is clean, idiomatic, and covers most of the spec. There are **two outright spec violations** (disconnected-context cleanup, dangling branch refs), one systemic security concern (API key handling), and a self-poisoning stale-worktree issue.

---

## 1. Workflow & Orchestration (`internal/workflows/pipeline.go`)

### 🔴 1.1 — Cleanup does NOT use `workflow.NewDisconnectedContext` (spec violation)

`pipeline.go:52-60` — the deferred cleanup executes on the same `ctx` as the pipeline. If the workflow is **cancelled** (the exact scenario the spec calls out), that context is cancelled and `workflow.ExecuteActivity` for cleanup fails instantly — the worktree, branch, and jail artifacts leak. The comment says "Guarantee the workspace is cleaned up" but the guarantee only holds for non-cancellation exits. Fix:

```go
defer func() {
	dctx := workflow.NewDisconnectedContext(ctx)
	if err := workflow.ExecuteActivity(dctx, activities.CleanupWorktreeActivity, ...).Get(dctx, nil); err != nil {
		logger.Error("Failed to clean up worktree", "Error", err)
	}
}()
```

There is also **no test for the cancellation path** — every workflow test exercises ordinary returns. Add one (cancel mid-activity, assert `CleanupWorktreeActivity` still ran), which will fail until this is fixed.

### 🟡 1.2 — Retry loop semantics: confirm the intended attempt count

`pipeline.go:72-94` — the loop runs up to **3 test executions** and **3 agent runs** (initial + 2 fix rounds), feeding `result.Logs` back as the fix prompt. The spec's "up to 3 total attempts" is ambiguous between *test* attempts and *agent* attempts; the implementation (and tests at `pipeline_test.go:174-177`) commit to 3-of-each. If the intent was 3 *agent* attempts total with tests only validating, current code matches; if it was 3 rounds of the *whole* test→fix cycle, you get one fewer fix round than expected. Worth confirming. The loop structure itself, the `break` at max attempts, and the terminal error message are all correct.

### ✅ Met

- `MaximumAttempts: 1` retry policy (`pipeline.go:34-36`) — correct for non-idempotent activities.
- `StartToCloseTimeout: 15 * time.Minute` (`pipeline.go:33`) — matches the 15-minute target.
- Pipeline order: CreateWorktree → RunJailedClaude → RunNativeTests (+ fix loop) → deferred Cleanup.
- Branch name `feat/issue-<id>-<unix>` built from `workflow.Now(ctx)` (`pipeline.go:40`) — correctly deterministic (replays produce the same name), unlike `time.Now()`.

### 🟡 1.3 — Agent output from fix runs is discarded

`pipeline.go:89` — `agentOutput` is overwritten but never read after the initial run. Combined with `MaximumAttempts: 1` and 15-min timeouts, a timeout or opaque agent failure gives you nothing to debug. Log it.

---

## 2. Activities (`internal/activities/activities.go`)

### 🔴 2.1 — Cleanup leaves dangling branches (spec violation)

The spec: cleanup must not leave "dangling directories **or git refs**." `CreateWorktreeActivity` creates branch `feat/issue-<id>-<ts>` (`activities.go:56`), but `CleanupWorktreeActivity` (`activities.go:107-120`) only runs `worktree remove --force` + `prune`. **Every run permanently accumulates a dead branch** in the source repo — worse, they're timestamped so they never collide and never stop accumulating. Fix: `git branch -D <BranchName>` in cleanup (the input already carries it, currently unused).

### 🔴 2.2 — Stale-worktree handling in CreateWorktree is incomplete and self-inflicted

`activities.go:50-54` — on a leftover directory it does `os.RemoveAll`, but:

- It doesn't `git worktree prune`, so `.git/worktrees/issue-42` admin metadata survives. A subsequent `git worktree add` at that path can then fail ("already registered"/stale lock) — meaning cleanup failure in run N **breaks run N+1** instead of being self-healing.
- `RemoveAll` bypasses git entirely (no locking, no tracking cleanup), which is exactly what leaves the stale registration.

Fix: reuse `CleanupWorktreeActivity`'s logic (tolerant `worktree remove --force` + prune + branch delete) before `add`, treating "not a working tree" as success.

### 🟡 2.3 — Cleanup errors when the worktree was never registered

`activities.go:113-115` — if `worktree remove` targets a path git doesn't know about (the stale-state case above, or a partially-failed create), git exits non-zero ("is not a working tree") and the whole activity errors — after which **prune never runs**. Tolerate "missing/not-a-worktree" as success and always run prune.

### 🟡 2.4 — RunNativeTests conflates system errors with test failures

`activities.go:98-102` — any `CombinedOutput` error maps to `Passed: false`. If `go` is absent from the worker's PATH, the exec error is *not* an exit-status error, yet it becomes "tests failed" with **empty logs**, and the workflow dutifully asks the agent to fix invisible failures 3 times. Distinguish:

```go
if err != nil {
	if _, ok := err.(*exec.ExitError); !ok {
		return TestResult{}, fmt.Errorf("go test: %w", err) // system error, not a test failure
	}
	return TestResult{Passed: false, Logs: string(out)}, nil
}
```

### 🔴 2.5 — ANTHROPIC_API_KEY is handled unsafely (spec: "safely")

Two exposure paths:

1. **Plaintext in Temporal history:** the key travels `run` CLI → `PipelineInput.APIKey` (`main.go:113`) → `AgentRunInput.APIKey` (`pipeline.go:65`). All activity inputs are serialized into workflow history/events, fully readable in the Temporal UI and any history archive.
2. **Process-list exposure:** `--env ANTHROPIC_API_KEY=sk-...` is passed as a **command-line argument** (`activities.go:70`), visible to any local user via `ps` for the entire (up to 15-min) run.

**Recommendation:** don't transport the key through workflow state at all — the worker already runs in a trusted local context; read `ANTHROPIC_API_KEY` from the worker's own environment inside `RunJailedClaudeActivity` and inject it via the subprocess `cmd.Env` (or ai-jail's env mechanism), never via argv. This also removes the key from `PipelineInput`, fixing path 1.

### ✅ Met

- Worktree path `~/.daedalus/worktrees/issue-<id>` (`WorktreePathFor`, tested).
- `RunJailedClaude` wraps `claude` via `ai-jail`, `cmd.Dir = input.WorktreePath` — correctly targets the worktree.
- Non-zero git exits surfaced with combined output (`activities.go:57-59`).
- `RunNativeTests` runs `go test ./...` with `cmd.Dir = worktreePath`, captures combined output, and models test failure as data (`Passed: false`) rather than an error — the right call for the feedback loop.
- `exec.CommandContext` used throughout, so activity cancellation/timeout kills subprocesses.

### 🟡 2.6 — Subprocess kill doesn't cover grandchildren

`exec.CommandContext` kills only the direct child (`ai-jail` / `go`). If `ai-jail` doesn't forward the signal to its `claude` child, a timeout can orphan a running agent with `--dangerously-skip-permissions` inside your worktree. Consider `SysProcAttr{Setpgid: true}` + killing the process group on context cancellation.

---

## 3. CLI (`cmd/daedalus/main.go`)

### ✅ Met

- `worker`: default `127.0.0.1:7233` / `TEMPORAL_HOST_PORT` override (via `config.Load`), registers the workflow + all 4 activities on `daedalus-task-queue`, `InterruptCh()` for graceful shutdown.
- `run`: arg-count validation (`main.go:46-49`), `ANTHROPIC_API_KEY` presence check, deterministic ID `daedalus-issue-<id>`, prints Workflow/Run IDs, blocks on `run.Get`.

### 🟡 3.1 — Deterministic ID + default reuse policy breaks legitimate reruns

`main.go:106-108` — no `WorkflowIDReusePolicy` is set, so Temporal defaults to `AllowDuplicateFailedOnly`. Re-running `daedalus run` for an issue whose previous workflow **succeeded** fails with an already-exists error. Decide the intended semantic explicitly (`TerminateIfRunning`, or `AllowDuplicate` + timestamped-but-derived ID) rather than inheriting the default.

### 🟡 3.2 — `run.Get` has no deadline

`main.go:123` — `context.Background()` means the CLI can block forever (e.g., workflow stuck on an unavailable worker). Even a generous timeout (say, 60–90 min for 3 agent rounds) with a clear message beats an indefinite hang. Similarly, `repoPath` isn't validated to exist before starting the workflow — a one-line `os.Stat` fail-fast avoids a doomed workflow execution.

---

## 4. Testing

### ✅ Design is sound

- Workflow tests use `TestWorkflowEnvironment` with mocked activities (`pipeline_test.go`), covering happy path, fix-loop recovery, attempt exhaustion (asserting exactly 3 agent runs and cleanup-on-failure), agent failure, and create failure — a genuinely good matrix.
- Activity tests stub `git`/`go`/`ai-jail` as temp-dir shell scripts prepended to `PATH`, with an invocation log capturing cwd + args (`activities_test.go:29-42`) — exactly the hermetic pattern the spec requires, and they even verify the stale-dir removal and failure-output propagation.

### 🟡 Gaps

- **No cancellation test** (see 1.1 — it would fail today, which is the point).
- **No test for RunNativeTests returning an activity *error*** (vs. `Passed: false`) — the `err != nil → fail workflow` branch at `pipeline.go:74-76` is unexercised.
- **No test that Cleanup tolerates a missing/unregistered worktree** (2.3).
- Arg assertions via `strings.Join(args, " ")` equality (`activities_test.go:124`, `222`, etc.) would pass under arg re-splitting — `reflect.DeepEqual` on the slice is stricter and no more work.
- `bin/daedalus` appears to be a committed build artifact (git status was clean with the file present) — build outputs generally belong in `.gitignore`. (Tracked status could not be confirmed — git needs developer tools the review sandbox can't reach.)

### 🟡 Dead/duplicated config

`config.Config.WorktreeDir` (`config.go:20`, `config.go:30-33`) is computed by `Load()` but **consumed by nothing** — activities compute their own path via `WorktreePathFor`, with a *different* failure mode (config silently falls back to a relative path; the activity errors). Either thread config into the activities or delete the field; two sources of truth for the same path is a bug waiting to happen.

---

## Priority summary

| # | Severity | Finding |
|---|----------|---------|
| 1.1 | 🔴 | Cleanup lacks `NewDisconnectedContext` — worktree leaks on cancellation (spec violation) |
| 2.1 | 🔴 | Created branch never deleted — accumulates forever (spec violation: "no dangling git refs") |
| 2.5 | 🔴 | API key persisted in Temporal history and exposed via `ps` argv |
| 2.2 | 🔴 | Stale-worktree handling doesn't prune → one failed cleanup poisons all future runs of that issue |
| 2.3 | 🟡 | Cleanup errors (and skips prune) when worktree is unregistered |
| 2.4 | 🟡 | Missing `go` binary misreported as "tests failed" with empty logs |
| 3.1 | 🟡 | No `WorkflowIDReusePolicy` — rerunning a succeeded issue fails |
| 2.6 | 🟡 | Timeout kills jail but can orphan the `claude` child |
| 3.2 | 🟡 | `run.Get` blocks forever; no repo-path fail-fast |
| 4 / — | 🟡 | Missing cancellation & activity-error tests; unused `Config.WorktreeDir`; discarded agent output |

## Suggested remediation order

The two cleanup-related reds (1.1, 2.1, 2.2) are best fixed together: make `CleanupWorktreeActivity` tolerant-and-complete (remove-or-ignore, prune, branch delete), call that same logic proactively at the top of `CreateWorktreeActivity`, and run the deferred cleanup on a disconnected context.
