# Usage

The full CLI surface is answerable from the binary itself: `daedalus -h`
lists every command with a one-line flag hint, `daedalus <command>
--help` prints that command's detail, and `daedalus worker <action>
--help` prints a sub-action's flag rules (which flags it accepts, which
it rejects). The tables below carry the same surface — this section and
the help text are kept in agreement.

| Command | Arguments | Flags | Purpose |
|---|---|---|---|
| `run` | `<repo-path> <issue-id> "<prompt>"` | `-w -d -p -cli -folder -dep -f -a` | Start an implementation pipeline for an issue |
| `run -a <workflow-id>` | `<workflow-id> ["<prompt>"]` | `-f` | Append a prompt to an already-running pipeline (same as `guide`) |
| `continue` | `<workflow-id> "<prompt>"` | `-d` | Resume a closed, failed, or parked session |
| `guide` | `<workflow-id> "<message>"` | — | Send operator instructions to a running pipeline |
| `attach` | `<workflow-id>` | — | Reconnect to an in-flight or completed pipeline |
| `list` | `[max]` | — | Display past and current sessions on the task queue |
| `log` | `<workflow-id>` | `--status -cot -cot-n` | Show a session's captured task log (raw, status brief, or chain-of-thought) |
| `wipe` | `<workflow-id>` | `--yes` | Erase a session's disk work entirely (worktree, branches, logs) |
| `worker start` | — | `-c -t` | Run the worker daemon detached (the default worker action) |
| `worker stop` | — | `-c` | Drain the daemon gracefully (SIGTERM) |
| `worker status` | — | — | List every worker on record (plus live strays) with live probes; the stuck-work recapture point |
| `worker restart` | — | `-c -t` | Stop + start this config's worker with the config re-read from disk |
| `worker restart <worker>` | `<worker>` | — | Restart one worker from its recorded config, from any directory |
| `worker restart all` | — | — | Restart every worker on record, each from its own record — host-global: every worker on record in ~/.local/state/daedalus regardless of the invoking directory |
| `worker foreground` | — | `-c -t` | Run the worker attached to this terminal |
| `worker wakeup` | `<workflow-id>` | `-c` | Interrupt a RUNNING session's quota heartbeat so the round resumes |
| `worker report` | — | `--json --table` | Summarize AI provider usage (cost, tokens, time) for this config's worker, plus worker slots |
| `metrics start` | — | `-c` | Run the host metrics daemon detached (the default metrics action): usage relay, ledger, dashboard |
| `metrics stop` / `metrics status` | — | — | Terminate the daemon / report its pid, ports, and version (record-driven, no config) |
| `metrics restart` | — | `-c` | Stop + start the daemon with the config re-read from disk |
| `metrics list` | — | `--worker --page --json` | Query the live usage ledger, newest first |
| `metrics foreground` | — | `-c` | Run the metrics daemon attached to this terminal |
| `init` | `[prompt] [slim]` | — | Generate a fully commented `config-example.yaml` |
| `config` | — | — | Print the active configuration (resolved path + every field) |
| `version` | — | — | Print the version and exit |
| `completion` | `bash\|zsh` | — | Print the tab-completion script (eval into your shell rc) |

Global flags: `-c/--config <path>`, `-v/--version`, `-h/--help`. `-c` is
accepted by every command except `log`, the record-driven worker commands
(`worker status`, `worker restart <worker>`, `worker restart all`), and
the record-driven metrics commands (`metrics status`, `metrics stop`,
`metrics list`), which act from the recorded daemon files alone.

Flag reference:

| Flag | Accepted by | Meaning |
|---|---|---|
| `-w, --workflow <name>` | `run` (fresh runs) | Flow to run: `feature-dev` (default), `dev-only`, `slim`, `investigate`, `test-only`, `refactor`, `bug-fix`. Rejected on every other command — including `run -a`, where the pipeline keeps the flow it started with |
| `-d, --detach` | `run`, `continue` | Start the pipeline and return immediately; `daedalus attach` reconnects |
| `-p, --prefix <prefix>` | fresh `run` | Name the preserved branch `<prefix>/issue-<id>-<timestamp>`, overriding the config's `branch_prefix` |
| `-cli, --cli <agent>` | fresh `run` | Jailed agent for this run: `claude`, `opencode`, `amp`, `pi`, `aider` (deprecated), `codex` |
| `-folder, --folder <path>` | fresh `run` (repeatable) | Grant the run read-write access to a host folder, mounted at `.daedalus-folders/<basename>` |
| `-dep, --depends <workflow-id>` | fresh `run` | Chain the run behind another; it starts from the dependency's preserved branch once that finishes approved. When the dependency stops without approval, the `dependency` config decides: refuse (the default) or release onto `dependency.fallback_branch` when the stopped state is skip-listed |
| `-f, --file <path>` | `run` (fresh and `-a`) | Read the task description from a file (glob-expandable); replaces the `"<prompt>"` argument |
| `-a, --append <id>` | `run` | Append mode: fold the prompt into an already-running pipeline's next fix round |
| `--status` | `log` | Print a maintainer-facing status brief instead of the raw log |
| `-cot` | `log` | Print the run's chain-of-thought logs from Temporal history |
| `-cot-n <N>` | `log -cot` | Tail the CoT view to the last N completed rounds (whole sections only) |
| `-t, --type dev\|test` | `worker start`, bare `worker restart`, `worker foreground` | Which pollers the daemon starts (`dev` or `test`; omit for both). Rejected everywhere else — `run`, `worker status`, the record-driven restarts, `worker stop`/`wakeup`, and every non-worker command |
| `--all` | `worker restart` | Restart every worker on record (same as the `all` argument; host-global — every worker on record in ~/.local/state/daedalus, any invoking directory) |
| `--force` (`-f`) | `worker restart` | Immediate swap: SIGTERM the old daemon and start the replacement at once instead of scheduling a drain. The drain restart keeps the old daemon serving until it has drained, then re-executes in place, so only a failure at or after the re-exec takes it down; a forced replacement is a fresh start — a failure after the old daemon was signaled (a relay dying mid-swap, a config deleted after it was loaded, a spawn or boot failure) leaves that worker down |
| `--yes` | `wipe` | Skip the interactive confirmation (scripted use) |
| `--json` / `--table` | `worker report`, `metrics list` | JSON vs table output (table is the default; `--table` is accepted for explicitness) |
| `--worker <name>` / `--page <n>` | `metrics list` | Filter the ledger page to one worker / pick the page (50 entries per page, newest first) |

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
   invoke them (`./config.yaml` in the working directory wins when present,
   then `./.daedalus/config.yaml` — the repo-local config of the repo your
   shell sits in). `daedalus run <repo-path>` additionally prefers the
   target repo's `.daedalus/config.yaml` over all of the above, so running
   against another repo configures the run for that repo, not the shell's
   cwd: that file replaces the default config outright for the run — no
   field merging — while an explicit `-c` still wins over it.

   `daedalus worker` runs the worker as a detached daemon: one per task
   queue, logs appending to `~/.local/state/daedalus/worker-<queue>.log` (pruned
   to the past week), pid in `~/.local/state/daedalus/worker-<queue>.pid`. Manage it with
   `daedalus worker stop|status|restart`, or `daedalus worker foreground` to
   run it attached to a terminal (the way to debug a worker that will not
   start). Each worker records the config it was started with (in
   `~/.local/state/daedalus/worker-<queue>.conf`), so `restart` reuses the recorded
   settings wherever it's invoked, as long as some config resolves to name
   the queue (pass `-c` or keep a user-level config) — stop then
   `start -c <new>` to move a worker to a different config.
   `daedalus worker restart all` restarts every worker on record, each
   with its own config, in one call — records alone, no config needed
   (an explicit `-c` is rejected there; it would have no effect). "all" is
   host-global: it signals every worker on record in `~/.local/state/daedalus`
   regardless of the invoking directory, and `--force` trades the drain —
   which keeps the old daemon serving until it has drained and re-executes
   in place, so only a failure at or after the re-exec takes it down — for
   an instant kill whose replacement is a fresh start: a failure after the
   old daemon was signaled (a relay dying mid-swap, a config deleted after
   it was loaded, a spawn or boot failure) leaves that worker down.
   `daedalus worker status` lists every worker on record — plus any live
   stray running without one, shown as "(no config record)" — queue,
   running pid, a live provider API probe, the repo path each worker's
   queue is currently executing ("idle" when none), config record, and log
   path. Status is also the recapture point for stuck work: a running
   workflow whose outstanding activity attempt is held by a worker
   identity with no live poller (a worker that died mid-round) is
   recovered automatically — the stuck attempt is failed so the workflow
   reschedules it, and a worker is restarted (or started from the queue's
   most recent config record) only when the attempt's queue has no live
   poller left. Each action prints a `[DAEDALUS-ALERT]` line to the
   status output and the worker log. With nothing stuck, status is a
   read-only no-op.

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

   The path may also be a glob (`notes/*.md`, `/path/to/specs/*`): every
   matching file is read in lexical order and joined into the prompt. A glob
   whose *directory* part carries metachars (`*?[\`) is rejected, because
   that directory is granted to the run read-write (below) and is taken
   literally, never globbed.

   `-folder/--folder <path>` (repeatable) grants the run read-write access
   to a host folder: each is mounted into the jailed rounds' sandbox at
   `.daedalus-folders/<basename>` and named in the agent's opening prompt,
   so the run can do bookkeeping outside the repo — update a shared
   done-index, retire its own task file. Passing `-f` grants the task file's
   containing folder automatically, so the run above can remove
   `issue-42.md` itself. Grants are fresh-run only (rejected in append
   mode), validated at submit, and are the same accepted operator trade as
   the bug-filing mirror: a read-write window onto a host path you chose.

   ```sh
   daedalus run -folder ~/Projects/tasks/daedalus /path/to/repo 42 -f issue-42.md
   ```

   `-cli/--cli <agent>` overrides the config's jailed agent (`claude`,
   `opencode`, `amp`, `pi`, `aider`, `codex`) for this run — the selection
   travels
   with the run, so
   it applies on whichever worker serves the task queue:

   ```sh
   daedalus run -cli amp /path/to/repo 42 "Add a /health endpoint."
   ```

`run` prints the Workflow ID (`daedalus-42`) and Run ID, then blocks
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

### Systemd supervision of the worker

A worker that dies mid-run (crash, OOM, reboot) stalls every run it was
carrying: Temporal retries the lapsed round, but a retry needs a live
worker polling the queue. `deploy/daedalus-worker.service` ships a systemd
unit that owns the worker's crashes and boots: `Restart=on-failure`
revives it with backoff, the start rate limit caps a crash loop (a broken
config surfaces in `journalctl -u daedalus-worker` and the worker log),
and `WantedBy=multi-user.target` brings it back at boot so queued runs
resume. Copy the unit to `/etc/systemd/system/`, edit the `ExecStart`
binary/config paths and the `User`/`Group` for the host, then
`systemctl daemon-reload && systemctl enable --now daedalus-worker`.

The lifecycle contract keeps one owner per job. Systemd owns crashes and
boots. The CLI keeps stop and restart: `worker stop` drains gracefully and
exits 0, which `Restart=on-failure` leaves alone; `worker restart`
SIGHUP-drains and re-execs in place, invisible to systemd. The unit sets
`DAEDALUS_WORKER_DAEMON=1`, so the supervised worker writes and clears its
own pid file and every pid-file gate (`worker start`'s double-start
refusal, `worker stop`/`restart`/`status`/`report`) treats it exactly like
a spawned daemon. What stays unsupervised-mode only: `worker start` while
the unit is active (it would double-start — the pid-file refusal catches
the live one) and `worker restart --force` (its replacement spawns outside
the unit); use systemctl for those on a supervised host. An auto-revived
worker boots from the unit's `ExecStart`, not through `worker start`: no
config record is written (the unit is the boot definition) and a drain
restart re-execs the service's own command line; a supervised boot also
opens no worker log — output lives in journald (`journalctl -u
daedalus-worker`). At boot the worker registers the pipeline's search
attributes itself (no `make custom-columns` step needed on a fresh
namespace).

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
  round, up to five times — and then parks itself: it completes with a `run
  parked awaiting maintainer restart` result (the reason visible in the
  workflow history, and the run shows green in the orchestrator — a park
  is a designed outcome, not a failure) so you can tell
  "continue later" apart from a code failure. A reviewer that judges the
  task impossible ends with NEEDS_MAINTAINER and parks the run the same
  way, with its comments carried in the result.
- **Wake a sleeping one**: `daedalus worker wakeup <workflow-id>` ends a
  RUNNING session's quota-heartbeat sleep immediately, so the round retries
  right away instead of at the top of the hour — the lever for when you have
  verified the provider recovered but the run is still waiting out its
  hourly retry. It takes no prompt (that is `guide`/`continue`); the run
  keeps its id and worktree. Closed sessions still resume with `continue`.

`daedalus` or `daedalus --help` prints full usage.
