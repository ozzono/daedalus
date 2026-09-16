# Daedalus

[![CI](https://github.com/ozzono/daedalus/actions/workflows/ci.yml/badge.svg)](https://github.com/ozzono/daedalus/actions/workflows/ci.yml)
[![version](https://img.shields.io/github/v/tag/ozzono/daedalus)](https://github.com/ozzono/daedalus/tags)
[![Go](https://img.shields.io/badge/go-1.26-00ADD8)](https://go.dev)

A local, sandboxed AI developer agent control plane, written in Go and
orchestrated via [Temporal](https://temporal.io).

Daedalus turns an issue into working, reviewed, tested code. Given a
repository, an issue ID, and a prompt, it spins up an isolated git worktree
and runs two review-gated loops inside it: a jailed agent —
[Claude Code](https://claude.com/claude-code) by default,
[opencode](https://opencode.ai), or [Amp](https://ampcode.com) — implements the change
while a jailed reviewer approves the code; the agent then writes the test
suite while the reviewer — and the repository's own test suite — approve the
tests. Each loop runs until its reviewer approves. Temporal provides durable
execution: every step is auditable and survives worker restarts, and a run
that dies (crash, cancellation) or parks itself — provider quota exhausted
past its hourly heartbeats, or a reviewer halt on an impossible task —
leaves its work preserved and resumable.

```mermaid
flowchart TD
    PROMPT["issue"] --> CREATE["CreateWorktree"]
    CREATE --> P1_IMPL

    %% Phase 1 Nodes
    P1_IMPL["<b> PHASE 1: IMPLEMENTATION </b><hr/>RunJailedClaude<br/><i>(implements, no tests yet)</i>"]
    P1_REV["<b> PHASE 1: REVIEW </b><hr/>RunJailedReviewer<br/><i>(code)</i>"]

    P1_IMPL --> P1_REV
    P1_REV -- "review comments<br/>(until APPROVED)" --> P1_IMPL

    P1_REV -- "APPROVED" --> P2_IMPL

    %% Phase 2 Nodes
    P2_IMPL["<b> PHASE 2: TESTS </b><hr/>RunJailedClaude<br/><i>(writes/fixes/improves tests)</i>"]
    P2_TEST["<b> PHASE 2: SUITE </b><hr/>RunNativeTests"]
    P2_REV["<b> PHASE 2: REVIEW </b><hr/>RunJailedReviewer<br/><i>(tests)</i>"]

    P2_IMPL --> P2_TEST
    P2_TEST --> P2_REV
    P2_REV -- "test output + comments<br/>(until APPROVED & green)" --> P2_IMPL

    P2_REV -- "APPROVED & green" --> CLEAN

    %% Final Nodes
    CLEAN["<b> CLEANUP </b><hr/>Cleanup<br/><i>(guaranteed on every exit path)</i>"]
    DONE["done"]

    CLEAN --> DONE
```

## Why

Letting an AI agent loose on your working copy is risky. Daedalus never
touches your checkout: all agent work happens in a throwaway worktree under
`~/.daedalus/worktrees/<queue>/issue-<id>`, wrapped in [ai-jail](https://github.com/anthropics/ai-jail)
for filesystem confinement (the worktree is the only writable path; network
access is granted so the agent's CLI can reach its provider). The worktree is
always removed at the end of the run, success or failure — but never at the
cost of the work: a run that closes without approval has its commits
preserved on an `aborted/issue-<id>` branch that `daedalus continue` picks
up.

## Prerequisites

- Go 1.26+
- A Temporal server: `temporal server start-dev` — all defaults match
  (frontend `127.0.0.1:7233`, UI :8233)
- The `ai-jail` CLI on your `PATH`
- The `claude` CLI (invoked by the jail), `opencode` when
  `agent: opencode` is set in config.yaml, or `amp` when `agent: amp` is
  (amp authenticates via `AMP_API_KEY` in the worker's environment, which
  daedalus passes into the jail when set; amp's own host login does not
  reach the jail)

## Usage

1. **Install** the CLI:

   ```sh
   go install github.com/ozzono/daedalus/cmd/daedalus@latest
   ```

2. **Start a Temporal server** (first terminal — skip if one is already
   running):

   ```sh
   temporal server start-dev
   ```

3. **Create your config and start a worker** (second terminal):

   ```sh
   daedalus init                  # writes config-example.yaml
   mkdir -p ~/.config/daedalus && cp config-example.yaml ~/.config/daedalus/config.yaml   # then edit as needed
   daedalus worker
   ```

   A config in `~/.config/daedalus/config.yaml` is found from any
   directory, so `worker`, `run`, and the other commands work wherever you
   invoke them (a `./config.yaml` in the working directory wins when
   present).

   `daedalus worker` runs the worker as a detached daemon: one per task
   queue, logs appending to `/tmp/daedalus/worker-<queue>.log` (pruned to
   the past week), pid in `/tmp/daedalus/worker-<queue>.pid`. Manage it with
   `daedalus worker stop|status|restart`, or `daedalus worker foreground` to
   run it attached to a terminal (the way to debug a worker that will not
   start).

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
until the pipeline finishes (up to 12 h; the workflow keeps running past
that). On success it prints the preserved branch holding the committed,
approved work, e.g. `daedalus/issue-42-1726320000`; a parked run prints a
`daedalus continue` resume hint instead. Follow along in the
Temporal UI at http://127.0.0.1:8233, or with `temporal workflow show`.

Prefer fire-and-forget? `daedalus run -d` starts the pipeline and returns
immediately; `daedalus attach <workflow-id>` reconnects later, blocks until
it finishes, and reports the outcome (also works after a run has ended).
`daedalus list` shows the most recent sessions on this task queue —
session id, status, and last interaction time.

### Steering and resuming

- **Steer a running pipeline**: `daedalus guide <workflow-id> "<message>"`
  sends operator guidance that is folded into the agent's *next* fix
  prompt, steering a stuck review loop without restarting the run.
  `daedalus run -a <workflow-id> "<prompt>"` is the same thing with a
  fuller prompt (`-f/--file` works there too).
- **Resume a closed one**: `daedalus continue <workflow-id> "<prompt>"`
  restarts a canceled, failed, or parked session under a new prompt.
  The aborted attempt's preserved work (its `aborted/<issue>` branch)
  becomes the new run's starting point, and that attempt's last review
  feedback is folded into the opening prompt. A run that exhausts the
  provider quota first heartbeats — sleeping an hour and retrying the same
  round, up to five times — and then parks itself: it fails with a `run
  parked awaiting maintainer restart` error (the reason visible in the
  workflow history and as FAILED in `daedalus list`) so you can tell
  "continue later" apart from a code failure. A reviewer that judges the
  task impossible ends with NEEDS_MAINTAINER and parks the run the same
  way, with its comments carried in the error.

`daedalus` or `daedalus --help` prints full usage.

## Configuration

All configuration lives in `config.yaml`, read from the working directory
when one is there, otherwise from `~/.config/daedalus/config.yaml`
(override the path with `-c/--config`). Keeping the config in
`~/.config/daedalus/` makes every command — `worker`, `run`, `list`, … —
work from any directory; `daedalus init` writes a fully commented
`config-example.yaml` documenting every field and its default:

| Field                 | Default            | Purpose                              |
| --------------------- | ------------------ | ------------------------------------ |
| `agent`               | `claude`           | Jailed agent CLI: `claude`, `opencode`, or `amp` (`worker -cli/--cli` overrides per worker) |
| `branch_prefix`       | `daedalus`         | Prefix for preserved branches (`<prefix>/issue-<id>-<ts>`); `run -p/--prefix` overrides per run |
| `temporal.host`       | `127.0.0.1:7233`   | Temporal frontend address            |
| `temporal.ui_port`    | `8233`             | Temporal UI port (shown at startup)  |
| `temporal.task_queue` | `daedalus`         | Routing key; distinct projects/flows on one Temporal use distinct queues |
| `anthropic.url`       | `""` (inherit env) | Anthropic API base URL — set only to override |
| `anthropic.key`       | `""` (optional)    | API key for the jailed agent; if unset, the agent authenticates via the worker's inherited environment or its own login |
| `anthropic.model`     | `""` (agent default) | Model for the jailed agent         |
| `openai.url/key/model`| `""` (inherit env) | Optional OpenAI settings, exported as `OPENAI_*` into the agent's environment for tooling it runs; not consumed by daedalus itself |

Provider settings that are set are exported into the worker's environment
at startup and injected into the jailed agent's process environment; unset
ones are simply not exported, so the agent inherits whatever the worker's
environment provides. The API key deliberately never appears in CLI
arguments, workflow inputs, or Temporal history — only in `config.yaml`,
the worker's environment, and the jailed process's environment. Keep
`config.yaml` out of version control (it is gitignored; the example file is
the committed template).

The Temporal task queue comes from `temporal.task_queue` (default `daedalus`);
worktrees live under `~/.daedalus/worktrees/<queue>/issue-<id>`. Re-running
`daedalus run` for an issue whose previous pipeline succeeded starts a new run
(duplicates are allowed) — the previous run's preserved branch stays.

## Pipeline details

- **Two review-gated phases**: the implementation phase loops
  agent ↔ reviewer until the reviewer approves; the test phase loops
  agent ↔ (test suite + reviewer) until the reviewer approves *and* the
  suite passes. Review rounds are intentionally **unbounded** — the workflow
  ends only on approval, with every round durable and auditable.
- **Reviewer protocol**: the reviewer sees the diff of the worktree (plus
  the latest test output in phase 2) and must end its response with a final
  line `APPROVED`, `CHANGES_REQUESTED`, or `NEEDS_MAINTAINER`. The last
  parks the run for a maintainer restart — used when the task as stated
  cannot be completed by editing files in the worktree, so an impossible
  task cannot loop forever. Anything else — including a malformed response —
  counts as changes requested, with the full output fed back to the
  implementing agent.
- **The repo's own test suite**, whatever it is: the test command is
  resolved per repository — a `tests:` declaration in `.daedalus.yaml` wins;
  otherwise marker files are detected (`go.mod` → `go test ./...`,
  `package.json` with a test script → `npm test`, pytest configs →
  `pytest -q`, Makefile `test-ui`/`test-api` targets → `make …`); for
  repositories none of that recognizes, a short jailed agent run discovers
  the command. The resolved command is recorded in the workflow history.
- **Branch lifecycle**: `feat/issue-<id>-<unix>` is the in-flight branch
  (always cleaned up, along with stale `feat/` branches from crashed runs);
  `<branch_prefix>/issue-<id>-<unix>` (default `daedalus`) is the committed,
  approved deliverable;
  `aborted/issue-<id>` carries a run that closed without approval, replaced
  by each newer abort and consumed by `daedalus continue`.
- **Operator guidance**: `daedalus guide` (or `run -a`) messages arrive as a
  Temporal signal and are prefixed onto the agent's next fix prompt,
  marked as direct operator instructions taking precedence over earlier
  plan assumptions.
- **Workflows are selectable**: `daedalus run -w feature-dev ...` (the
  default) picks from a name registry; adding another flow later is one
  registry entry.
- **Activities** run with a 15-minute start-to-close timeout and no retries
  (`MaximumAttempts: 1`) — a failed activity fails the run rather than
  re-running an agent that already mutated the worktree. Cancellation kills
  the whole jailed process group, not just the jail wrapper. Agent/reviewer
  failures that look like provider quota or rate limits are labeled as API
  exhaustion. The match is heuristic (a substring test against the error
  text), so a mislabeled failure can idle a run for hours of heartbeats
  before it parks. The workflow then heartbeats — one hourly retry at a
  time, up to five — before parking the run as a labeled, `continue`-able
  failure.
- **Deliverable preservation**: once both phases pass, a finalize activity
  commits the approved work and renames the run's branch from
  `feat/issue-<id>-<unix-timestamp>` (in-flight) to
  `<branch_prefix>/issue-<id>-<unix-timestamp>` (preserved; default prefix
  `daedalus`, from `branch_prefix` or `run -p/--prefix`; prefixes colliding
  with the reserved `feat`/`aborted` namespaces are rejected up front). The
  prefix scopes the preserved branch and the finalized-deliverable check, but
  not the `aborted/issue-<id>` snapshot, which is shared per issue across
  prefixes: a failing run under another prefix replaces it and is not
  suppressed by a deliverable finalized under this prefix. The workflow
  returns the preserved branch name and `daedalus run` prints it.
- **Prompt transport**: prompts travel to the jailed agent via stdin, not
  argv — no `ps` visibility, no per-argument size limit on review prompts
  that embed the full diff. Verdicts are parsed from stdout only, so
  trailing stderr noise cannot flip an `APPROVED`.
- **History diet**: the agent's visible text and chain of thought come back
  in the activity result tail-bounded to 16 KiB each (visible per round in
  the Temporal UI), test logs likewise; the worker log keeps the full
  untruncated stream.
- **Multiple projects / flows on one Temporal**: each deployment sets its own
  `temporal.task_queue`; workflow IDs (`<queue>-issue-<id>`) and worktree
  paths (`~/.daedalus/worktrees/<queue>/issue-<id>`) are scoped by queue, so
  same-numbered issues in different projects never collide.
- **Cleanup**: a deferred activity runs on every exit path — including
  cancellation (via a disconnected context) and agent, reviewer, or test
  failures — to remove the worktree (`--force`), prune git's worktree
  metadata, sweep stale `feat/issue-<id>-*` branches left by crashed runs,
  and preserve unapproved work on `aborted/` first. Preserved
  `<branch_prefix>/` branches are never touched; leftover worktree state is healed on the next
  run of the same issue.

## Development

Run the test suite:

```sh
go test ./...
```

Tests are hermetic: subprocess-backed activities are exercised against stub
`git`/`go`/`ai-jail` executables installed on a temporary `PATH`, and the
workflow is tested in Temporal's in-process `TestWorkflowEnvironment` with
mocked activities — no server, network, or API key needed.

Releases are git tags. CI runs the suite on every PR and master push — a PR
must carry exactly one release label (`patch`, `minor`, or `major`) before it
can merge. After a green master push, CI tags the merged code with the next
version, using that PR's label to pick the bump level (`patch` for pushes
that are not PR merges) and publishes a GitHub Release for the tag with
auto-generated notes — there is no version file and no other release
tooling. `daedalus --version` reports the tag-stamped version of a
`make build` binary and of one installed via
`go install github.com/ozzono/daedalus/cmd/daedalus@latest`; a plain
`go build` from a checkout reports `(devel)`.
