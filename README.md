# Daedalus

[![CI](https://github.com/ozzono/daedalus/actions/workflows/ci.yml/badge.svg)](https://github.com/ozzono/daedalus/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/ozzono/daedalus/badges/coverage.json)](https://github.com/ozzono/daedalus/actions/workflows/ci.yml)
[![version](https://img.shields.io/github/v/tag/ozzono/daedalus)](https://github.com/ozzono/daedalus/tags)
[![Go](https://img.shields.io/badge/go-1.26-00ADD8)](https://go.dev)

A local, sandboxed AI developer agent control plane, written in Go and
orchestrated via [Temporal](https://temporal.io).

Daedalus turns an issue into working, reviewed, tested code. Given a
repository, an issue ID, and a prompt, it spins up an isolated git worktree
and runs two review-gated loops inside it: a jailed agent —
[Claude Code](https://claude.com/claude-code) by default, or
[opencode](https://opencode.ai) — implements the change
while a jailed reviewer approves the code; the agent then writes the test
suite while the reviewer — and the repository's own `go test` — approve the
tests. Each loop runs until its reviewer approves. Temporal provides durable
execution: every step is auditable and survives worker restarts.

```
issue ──▶ CreateWorktree
             │
             ▼
   ┌─ phase 1: implementation ──────────────────────────────┐
   │  RunJailedClaude (implements, no tests yet)             │
   │   ▲                              │                      │
   │   └── review comments ── RunJailedReviewer (code)       │
   │                          until APPROVED                 │
   └─────────────────────────────────────────────────────────┘
             ▼
   ┌─ phase 2: tests ───────────────────────────────────────┐
   │  RunJailedClaude (writes/fixes/improves tests)          │
   │   ▲                              │                      │
   │   └─ test output + comments ─ RunNativeTests            │
   │                             └─ RunJailedReviewer (tests)│
   │                          until APPROVED and green       │
   └─────────────────────────────────────────────────────────┘
             ▼
          Cleanup ──▶ done        (cleanup is guaranteed on every exit path)
```

## Why

Letting an AI agent loose on your working copy is risky. Daedalus never
touches your checkout: all agent work happens in a throwaway worktree under
`~/.daedalus/worktrees/<queue>/issue-<id>`, wrapped in [ai-jail](https://github.com/anthropics/ai-jail)
for filesystem and network confinement. The agent gets repository-scoped
write access and an API key, nothing else. The worktree is always removed at
the end of the run, success or failure.

## Prerequisites

- Go 1.26+
- A Temporal server: `temporal server start-dev` — all defaults match
  (frontend `127.0.0.1:7233`, UI :8233)
- The `ai-jail` CLI on your `PATH`
- The `claude` CLI (invoked by the jail), or `opencode` when
  `agent: opencode` is set in config.yaml

## Usage

1. **Build and install** the CLI:

   ```sh
   go install ./cmd/daedalus
   ```

2. **Start a Temporal server** (first terminal — skip if one is already
   running):

   ```sh
   temporal server start-dev
   ```

3. **Create your config and start a worker** (second terminal):

   ```sh
   cp config-example.yaml config.yaml   # then edit as needed
   daedalus worker
   ```

4. **Trigger a pipeline** (third terminal):

   ```sh
   daedalus run /path/to/repo 42 "Add a /health endpoint that returns 200."
   ```

   The third argument is the **task description** — the full issue text the
   implementing agent works from. Daedalus has no issue-tracker integration:
   it never fetches the description from anywhere, so paste or write the
   entire task (context, requirements, acceptance criteria) into that quoted
   string. Use `--` first if the text itself starts with a `-`:

   ```sh
   daedalus run /path/to/repo 42 -- "-c flag parsing must survive --config= in prompts"
   ```

   For long descriptions, pass a file instead — either the argument or the
   file, never both:

   ```sh
   daedalus run -f issue-42.md /path/to/repo 42
   ```

`run` prints the Workflow ID (`daedalus-issue-42`) and Run ID, then blocks
until the pipeline finishes (up to 4 h; the workflow keeps running past
that). On success it prints the preserved branch holding the committed,
approved work, e.g. `daedalus/issue-42-1726320000`. Follow along in the
Temporal UI at http://127.0.0.1:8233, or with `temporal workflow show`.

`daedalus` or `daedalus --help` prints full usage.

## Configuration

All configuration lives in `config.yaml` (override the path with
`-c/--config`); `config-example.yaml` documents every field and its default:

| Field                 | Default                     | Purpose                              |
| --------------------- | --------------------------- | ------------------------------------ |
| `agent`               | `claude`                    | Jailed agent CLI: `claude` or `opencode` |
| `temporal.host`       | `127.0.0.1:7233`            | Temporal frontend address            |
| `temporal.ui_port`    | `8233`                      | Temporal UI port (shown at startup)  |
| `temporal.task_queue` | `daedalus`                  | Routing key; distinct projects/flows on one Temporal use distinct queues |
| `anthropic.url`       | `https://api.anthropic.com` | Anthropic API base URL               |
| `anthropic.key`       | — (optional)                | API key for the jailed agent; if unset, the agent authenticates via the worker's inherited environment or its own login |
| `anthropic.model`     | agent default               | Model for the jailed agent           |
| `openai.url`          | `https://api.openai.com/v1` | Optional OpenAI base URL             |
| `openai.key`          | —                           | Optional OpenAI key                  |
| `openai.model`        | —                           | Optional OpenAI model                |

Provider settings are exported into the worker's environment at startup and
injected into the jailed agent's process environment. The API key deliberately
never appears in CLI arguments, workflow inputs, or Temporal history — only in
`config.yaml`, the worker's environment, and the jailed process's environment.
Keep `config.yaml` out of version control (it is gitignored; the example file
is the committed template).

The Temporal task queue comes from `temporal.task_queue` (default `daedalus`);
worktrees live under `~/.daedalus/worktrees/<queue>/issue-<id>`. Re-running
`daedalus run` for an issue whose previous pipeline succeeded starts a new run
(duplicates are allowed) — the previous run's preserved branch stays.

## Pipeline details

- **Two review-gated phases**: the implementation phase loops
  agent ↔ reviewer until the reviewer approves; the test phase loops
  agent ↔ (`go test` + reviewer) until the reviewer approves *and* the suite
  passes. Review rounds are intentionally **unbounded** — the workflow ends
  only on approval, with every round durable and auditable.
- **Reviewer protocol**: the reviewer sees the staged diff of the worktree
  (plus the latest test output in phase 2) and must end its response with a
  final line `APPROVED` or `CHANGES_REQUESTED`. Anything else — including a
  malformed response — counts as changes requested, with the full output fed
  back to the implementing agent.
- **Workflows are selectable**: `daedalus run -w feature-dev ...` (the
  default) picks from a name registry; adding another flow later is one
  registry entry.
- **Activities** run with a 15-minute start-to-close timeout and no retries
  (`MaximumAttempts: 1`) — a failed activity fails the run rather than
  re-running an agent that already mutated the worktree. Cancellation kills
  the whole jailed process group, not just the jail wrapper.
- **Deliverable preservation**: once both phases pass, a finalize activity
  commits the approved work and renames the run's branch from
  `feat/issue-<id>-<unix-timestamp>` (in-flight) to
  `daedalus/issue-<id>-<unix-timestamp>` (preserved). The workflow returns
  the preserved branch name and `daedalus run` prints it.
- **Prompt transport**: prompts travel to the jailed agent via stdin, not
  argv — no `ps` visibility, no per-argument size limit on review prompts
  that embed the full diff. Verdicts are parsed from stdout only, so
  trailing stderr noise cannot flip an `APPROVED`.
- **History diet**: the agent's full output goes to the worker log (not an
  activity result), and test logs are tail-truncated to 16 KiB before
  entering Temporal history.
- **Multiple projects / flows on one Temporal**: each deployment sets its own
  `temporal.task_queue`; workflow IDs (`<queue>-issue-<id>`) and worktree
  paths (`~/.daedalus/worktrees/<queue>/issue-<id>`) are scoped by queue, so
  same-numbered issues in different projects never collide.
- **Cleanup**: a deferred activity removes the worktree (`--force`), prunes
  git's worktree metadata, deletes the run's in-flight `feat/` branch, and
  sweeps stale `feat/issue-<id>-*` branches left by crashed runs — on every
  exit path, including cancellation (via a disconnected context) and agent,
  reviewer, or test failures. Preserved `daedalus/` branches are never
  touched; leftover worktree state is healed on the next run of the same
  issue.

## Development

Run the test suite:

```sh
go test ./...
```

Tests are hermetic: subprocess-backed activities are exercised against stub
`git`/`go`/`ai-jail` executables installed on a temporary `PATH`, and the
workflow is tested in Temporal's in-process `TestWorkflowEnvironment` with
mocked activities — no server, network, or API key needed.

The CLI's version lives in `internal/version/VERSION` (`daedalus --version`
prints it). CI runs the suite on every PR and master push; after a green
master push it tags the tested code with the current version, bumps the file
to the next patch, and refreshes the coverage badge — no other release tooling.
