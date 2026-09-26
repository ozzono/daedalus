package main

// usage is the root help screen, printed for -h/--help/help and on any
// argument error.
var usage = `Daedalus — A local, sandboxed AI developer agent control plane.

USAGE
  daedalus [global-flags] <command> [subcommand|arguments] [flags]

WORKFLOW COMMANDS
  run         Start an implementation pipeline for an issue
  continue    Resume a closed, failed, or parked session
  guide       Send operator instructions to a running pipeline
  attach      Reconnect to an in-flight or completed pipeline
  list        Display past and current sessions on the task queue
  log         Show a session's captured task log (/tmp/daedalus/<workflow-id>.log)
  wipe        Erase a session's disk work entirely (worktree, branches, logs)

WORKER COMMANDS
  worker      Manage the Temporal worker daemon (start, stop, status, restart, foreground, wakeup)

UTILITY COMMANDS
  init        Generate a fully commented config-example.yaml file
  config      Print the active configuration (resolved path + every field)
  report      Summarize AI provider usage (cost, tokens, time) and worker slots
  version     Print the version and exit

GLOBAL FLAGS
  -c, --config <path>   Path to config file (./config.yaml, ./.daedalus/config.yaml, or ~/.config/daedalus/config.yaml)
  -v, --version         Print version and exit
  -h, --help            Show this help message

EXAMPLES
  $ daedalus run ./my-repo 42 "Add /health endpoint"
  $ daedalus run -cli amp -f prompt.md ./my-repo 101
  $ daedalus guide daedalus-42 "Focus on the database migration first"
  $ daedalus worker restart all
  $ daedalus log daedalus-42

Use "daedalus <command> --help" for detailed information about a command.
`

// commandHelp holds the per-command help screens behind
// "daedalus <command> --help": the argument, flag, and edge-case detail
// the root screen deliberately omits (progressive disclosure). The init
// screen also carries the configuration reference table.
var commandHelp = map[string]string{
	"run": `daedalus run — start an implementation pipeline for an issue.

USAGE
  daedalus [-c config.yaml] [-w workflow] run [-d] [-p prefix] [-cli agent] <repo-path> <issue-id> "<prompt>"
  daedalus [-c config.yaml] run -a <workflow-id> ["<prompt>"]   (append mode)

ARGUMENTS
  <repo-path>    Path to the repository the pipeline works in. When the repo
                 carries .daedalus/config.yaml, that file replaces the
                 default config outright for this run (no field merging);
                 an explicit -c/--config still wins over it. This argument
                 anchor beats the working directory's own ./.daedalus/
                 config.yaml (see "daedalus init --help"), so running
                 against another repo from inside a configured one configures
                 the run for the target repo.
  <issue-id>     Issue identifier: scopes the session's workflow id and branch.
  "<prompt>"     The full task description. Pass -f/--file <path> to read it
                 from a file instead — either the argument or the file, never both.

FLAGS
  -w, --workflow <name>   Workflow (flow) to run (default: feature-dev;
                          available: ` + workflowNames() + `). investigate is
                          docs-only (analysis written as documentation, no code
                          changes); test-only modifies test files only, with
                          coverage reported per round; refactor changes
                          production code with the test suite frozen and green;
                          bug-fix is test-first (the diff must carry a test
                          that fails on the pre-fix code). Each flow's write
                          scope is verified structurally before its work is
                          finalized — an out-of-scope diff parks the run.
  -d, --detach            Start the pipeline and return immediately instead of
                          blocking until it finishes; "daedalus attach" reconnects.
  -p, --prefix <prefix>   Name this run's preserved branch
                          <prefix>/issue-<id>-<timestamp>, overriding the config's
                          branch_prefix (the issue part of the name stays as given).
                          Fresh runs only — rejected in append mode.
  -cli, --cli <agent>     Jailed agent for this run: claude, opencode, amp,
                          pi, or aider. Overrides the config's agent; travels
                          with the run's workflow input, so it applies on
                          whichever worker serves the queue. Fresh runs only —
                          rejected in append mode.
  -f, --file <path>       Read the task description from a file. A glob pattern
                          (e.g. notes/*.md) expands to every matching file,
                          concatenated in lexical order.
  -a, --append <id>       Append mode: target an already-running pipeline instead
                          of starting a new run — the prompt is folded into the
                          agent's next fix round (same as "daedalus guide").
                          -f works here too; -d has no effect and -p is rejected:
                          the run keeps the branch prefix it started with.

EXAMPLES
  $ daedalus run ./my-repo 42 "Add /health endpoint"
  $ daedalus run -d ./my-repo 43 "Refactor the config loader"
  $ daedalus run -cli amp -f prompt.md ./my-repo 101
  $ daedalus run -a daedalus-42 "Also cover the migration in tests"

The printed workflow id (<task-queue>-<issue-id>, flow-scoped as
<task-queue>-<flow>-<issue-id> for flows other than feature-dev) is what
"attach", "guide", and "continue" address (also visible in "temporal
workflow list"). Runs started before the "-issue-" infix was dropped keep
their old ids — every id is taken verbatim, old and new coexist.
`,
	"continue": `daedalus continue — resume a closed session under a new prompt.

USAGE
  daedalus [-c config.yaml] continue <workflow-id> "<prompt>"

Resume a closed session (canceled, failed, or parked — an API-exhaustion
halt past its hourly heartbeats, or a reviewer NEEDS_MAINTAINER halt on an
impossible task): the aborted attempt's work, preserved on its aborted/
branch, becomes the new run's starting point, and that attempt's last
review feedback is folded into the opening prompt.

FLAGS
  -d, --detach   Start the continued run and return immediately.

-p/--prefix does not apply: the continued run keeps the original run's
branch prefix.

EXAMPLE
  $ daedalus continue daedalus-42 "Retry, but split the migration in two"
`,
	"guide": `daedalus guide — send operator guidance to a running pipeline.

USAGE
  daedalus [-c config.yaml] guide <workflow-id> "<message>"

The message is folded into the agent's next fix prompt, steering a stuck
review loop without restarting the run.

EXAMPLE
  $ daedalus guide daedalus-42 "Focus on the database migration first"
`,
	"attach": `daedalus attach — reconnect to a pipeline and report its outcome.

USAGE
  daedalus [-c config.yaml] attach <workflow-id>

Reattach to a running (or already finished) pipeline, block until it
finishes, and report the outcome — the other half of "run -d".
<workflow-id> is the identifier printed by "run" (also visible in
"temporal workflow list").

EXAMPLE
  $ daedalus attach daedalus-42
`,
	"list": `daedalus list — display past and current sessions on the task queue.

USAGE
  daedalus [-c config.yaml] list [max]

List past and current sessions (pipeline runs) on this task queue, newest
first: session id, status, last interaction datetime (close time once
closed, start time while running). Defaults to the 10 most recent; pass a
larger max to list more.

EXAMPLE
  $ daedalus list 25
`,
	"log": `daedalus log — show a session's captured task log.

USAGE
  daedalus log <workflow-id>
  daedalus log <workflow-id> --status
  daedalus log <workflow-id> -cot

Prints the task log /tmp/daedalus/<workflow-id>.log: one block per jailed
agent or reviewer round — full stdout and stderr, untruncated — and one per
native test-suite execution. (Body lines that would look like a block
header are prefixed with one space, so output is preserved except for that
byte.) Each block header stamps the temporal run id
(first 8 characters), so a continued session's runs are distinguishable in
one file. Files untouched for 7 days are pruned along with the worker logs.

With --status, prints a short maintainer-facing brief instead: which agent
round is running right now (dev, dev-review, test, test-review, or idle),
when its transcript was last touched, and a one-line digest of the
transcript's latest entry — the "looks stuck" check. It is computed from
the task log and the agent transcripts on disk alone, so it works even
while the worker or Temporal are down, and it is always scoped to the one
workflow id given. A missing log prints the same not-started error either way.
The transcript freshness and digest read claude's transcript dir and pi's
session dir, newest file wins — a previous run's stale transcripts on one
side never shadow the other agent's live session. Agents that keep no
transcript daedalus can address (opencode, amp, aider) show "no
transcripts yet" even while running; the task log itself is complete for
every agent.

With -cot, prints the run's chain-of-thought logs instead of the task
log: one complete section per jailed round (implementation and review),
read from Temporal history, never truncated. Rounds with a native
thinking channel (claude, amp, pi) show their captured thinking;
openai-wire rounds whose reasoning arrived inline in the answer show the
<think>-fenced part when the model fenced it (tagless inline CoT cannot
be told apart from the answer, so it is not guessed at); rounds with no
CoT at all (aider, opencode) say so per round. Needs the Temporal
service up: it dials the resolved config's host, or the default when no
config is found. Reviewer rounds' native thinking is not in history (the
reviewer activity discards it at capture), so review sections surface
only fenced inline CoT. A retry of a round appears as its own section —
each attempt's CoT is its own.

While a round is in flight, a live section follows the completed ones:
the in-flight round's reasoning and assistant text, read from the agent's
host-side transcript as the agent writes it — claude's and pi's, the same
newest-wins precedence as --status. Agents that keep no host transcript
(aider, opencode, amp) get one line saying their CoT appears when the
round completes. Between rounds there is no live section — the newest
transcript then belongs to the just-finished round, already rendered
above — and a round whose transcript holds no assistant output yet says
so in one line. The live section never truncates, and it does not repeat
a completed section — outside the brief window right after a round
spawns, before the in-flight round's own transcript file appears, when
the just-finished round's transcript is still the newest and can read as
live.

-c/--config is rejected for every log mode: the raw log and --status read
the file by name alone, no config needed; -cot dials the resolved
config's host (or the default when none is found) without taking an
explicit path.

EXAMPLES
  $ daedalus log daedalus-42
  $ daedalus log daedalus-42 --status
  $ daedalus log daedalus-42 -cot
`,
	"worker": `daedalus worker — manage the Temporal worker daemon.

USAGE
  daedalus [-c config.yaml] worker [start|stop|status|restart|foreground|wakeup <workflow-id>]
  daedalus worker restart <worker>            (from any directory)
  daedalus worker restart all | --all         (every worker on record)

ACTIONS
  start         (default) Run the worker as a detached daemon: logs append to
                /tmp/daedalus/worker-<name>.log (pruned to the past week), the
                pid lives in /tmp/daedalus/worker-<name>.pid — one daemon per
                worker name (the config's worker_id, else its task_queue).
  stop          Drain the daemon gracefully (SIGTERM).
  restart       Stop + start with the config the restart is invoked with, re-read
                from disk, so values changed since the worker was started (a
                rotated API key, a new model) take effect on every restart.
  status        List every worker on record — plus any live stray running without
                one, shown as "(no config record)" — with worker name, running
                pid, the binary version that worker is executing (so a daemon
                started before a CLI upgrade is visible as such), live API
                probe, config record, and log path. Also the
                recapture point: a running workflow whose outstanding activity
                attempt is held by a worker identity with no live poller (a
                worker that died mid-round) is recovered automatically — the
                stuck attempt is failed so the workflow reschedules it, and a
                worker is restarted (or started, from the queue's most recent
                config record) only when the attempt's queue has no live
                poller left. Each action prints a [DAEDALUS-ALERT] line here
                and to the worker log; with nothing stuck, status is a
                read-only no-op.
  foreground    Run attached to this terminal — the daemon child's mode, and the
                way to debug a worker that will not start.
  wakeup        Interrupt a RUNNING session's quota heartbeat so the round
                resumes immediately: "daedalus worker wakeup <workflow-id>".
                For when the provider recovered (or its fallback does) but
                the run is still sleeping out its hourly retry — the operator's
                alternative to waiting up to maxQuotaHeartbeats hours. No
                prompt is taken (that is "guide"/"continue"); the run keeps
                its id and worktree. A session that is running but not
                sleeping buffers the wakeup, skipping only its next heartbeat.
                A closed (canceled/failed/parked) or unknown id is a usage
                error — closed sessions resume with "continue", and the id
                can be checked against "daedalus list". The workflow must
                belong to the active config's task queue: a mismatched -c
                fails without acting.

ARGUMENTS
  <action>              One of the actions above (default: start).
  restart <worker>      That one worker, from its recorded config, from any directory.
  restart all | --all   Every worker on record, each with its own recorded config.

FLAGS
  -c, --config <path>   Path to the config file. Accepted by start, stop, bare
                        restart, foreground, and wakeup. Rejected by status,
                        restart <worker>, and restart all — those act from the
                        recorded configs alone, so -c would have no effect.
  -t, --type <type>     Which pollers this daemon starts: dev (pipeline rounds
                        and reviews only — no test-queue poller) or test (test
                        suites and the repro gate only — no pipeline poller).
                        Omit for both pollers, the default. Accepted by start,
                        bare restart, and foreground. Rejected by status,
                        restart <worker>, and restart all — those act from the
                        recorded configs alone and revive the daemon untyped;
                        a run config is never persisted, so pass -t again on
                        a bare restart.
  --all                 Restart target: every worker on record (same as the
                        "all" argument).

The untyped daemon (no -t) also serves the shared test task queue ("test")
that executes native test suites, bounded by config max_concurrent_tests.
("daedalus report" shows the jailed-round slot occupancy, not the test
queue's.)

anthropic.key is optional — if unset, the jailed agent authenticates through
the worker's inherited environment or its own login.

For a local Temporal dev server matching the defaults: temporal server start-dev
`,
	"init": `daedalus init — generate a fully commented config-example.yaml.

Writes config-example.yaml in the current directory: every field with its
default value, fully commented. Copy it to config.yaml and edit. Refuses to
overwrite an existing file.

Configuration is read from the first of ./config.yaml,
./.daedalus/config.yaml, and ~/.config/daedalus/config.yaml (-c/--config
overrides the path); the latter makes the CLI work from any directory. A
fresh "daedalus run <repo-path>" additionally prefers the target repo's
.daedalus/config.yaml over all of the above — see "daedalus run --help".

CONFIGURATION REFERENCE
  agent                 Jailed agent CLI: claude, opencode, amp, pi, or
                        aider (default claude; run -cli/--cli overrides per
                        run)
  thinking              Thinking toggle: an explicit false sends each
                        agent its own off lever (pi --thinking off, aider
                        --thinking-tokens 0, claude MAX_THINKING_TOKENS=0);
                        unset or true leaves every agent on its own default
                        (default true)
  stream                Streaming toggle: an explicit true/false sends
                        --stream/--no-stream on jailed aider rounds (the
                        only agent with a verified lever); absent leaves
                        every agent on its own streaming default
  branch_prefix         Prefix for preserved branches, <prefix>/issue-<id>-<ts>
                        (default daedalus; run -p/--prefix overrides per run)
  worker_id             Name the worker daemon is managed under: keys the
                        pid/log/config-record files and names the target of
                        "worker restart <id>" (default: the task queue)
  tests_timeout         Ceiling for one execution of the repo's native test
                        suite (default 30m)
  agent_run_timeout     Ceiling for one jailed-agent round (default 45m)
  cleanup_timeout       Ceiling for one worktree cleanup (default 30m)
  max_concurrent_agent_runs
                        Jailed-agent rounds this worker runs at once; further
                        rounds queue (default 2; slim: true forces 1)
  slim                  Slim mode for small self-hosted models (aider/pi):
                        one jailed-agent round at a time (no two provider
                        requests in flight) and aider's weak/editor models
                        pinned to AIDER_MODEL under DAEDALUS_SLIM; the
                        per-model limits stay operator-side — see
                        config-example.yaml for the per-agent prerequisites
                        (default false)
  max_concurrent_tests  Native test suites the test worker (shared "test"
                        queue) runs at once; further suites queue (default 2)
  temporal.host         Temporal frontend address (default 127.0.0.1:7233)
  temporal.ui_port      Temporal UI port, shown at worker startup (default 8233)
  temporal.task_queue   Routing key; distinct projects or flows sharing one
                        Temporal server use distinct queues (default daedalus)
  anthropic.url         Anthropic API base URL (default https://api.anthropic.com)
  anthropic.key         Anthropic API key (optional — skipped if unset)
  anthropic.model       Model for the jailed agent (agent default if unset)
  anthropic.heartbeat_model
                        Small/fast model for tiny prompts, exported as
                        ANTHROPIC_DEFAULT_HAIKU_MODEL and used by the
                        "worker status" probe (agent default if unset)
  anthropic.timeout_ms  Agent API timeout in ms, exported as API_TIMEOUT_MS
                        (default 3000000 = 50 minutes)
  anthropic.context_tokens
                        Jailed agent context window in tokens: exported as
                        CLAUDE_CODE_MAX_CONTEXT_TOKENS for claude, and the
                        input side of aider's staged model metadata;
                        ignored by opencode/pi/amp (0 = agent default)
  anthropic.max_output_tokens
                        Jailed agent completion cap in tokens: re-exported
                        as CLAUDE_CODE_MAX_OUTPUT_TOKENS for claude, and
                        both sides of the 8192 constant in aider's staged
                        model metadata; ignored by opencode/pi/amp (0 =
                        aider constant / agent API default)
  openai.top_p/presence_penalty/top_k/min_p/repetition_penalty
                        Sampler knobs for rounds served by the openai
                        section, consumed by aider's staged model settings
                        (top_k/min_p ride extra_body); pi ignores them
                        until its passthrough is probed (0 = omitted from
                        the request entirely)
  openai.url/key/model  Optional OpenAI settings injected into the agent
                        environment
  fallback.enabled/url/key/model/heartbeat_model
                        Independent secondary provider: when a jailed round
                        fails with the primary's quota exhausted, the worker
                        retries it on the fallback and keeps using it until
                        the primary recovers (reset stamp parsed when the
                        provider emits one, else 20m/40m/60m holds, 5h cap)
  fallback.type         Fallback wire style: anthropic (default) or openai;
                        governs the status probe and failover env. A round's
                        wire is chosen by the agent (claude dials ANTHROPIC_*),
                        so openai serves only agents dialing OPENAI_BASE_URL;
                        pi and aider dial whichever provider their selected
                        model uses
`,
	"config": `daedalus config — print the active configuration.

USAGE
  daedalus [-c config.yaml] config

Prints which config file this invocation resolved to — the first of
./config.yaml, ./.daedalus/config.yaml, and ~/.config/daedalus/config.yaml
that exists (an explicit -c/--config wins; named in the "# config:" first
output line, since with three possible sources "the current config" is
ambiguous without it) — followed by every config field as YAML.

The output is the file's own view, not the effective one: fields set in the
file print exactly as written, fields absent from it print empty. Empty
means "the documented default applies", not "off" — the defaults are listed
in the reference table of "daedalus init --help". Durations print as
duration strings ("30m0s"), plain 0 when unset.

Keys print verbatim, unredacted: the command shows the operator their own
file in their own terminal — the same values cat on it would.

A config daedalus cannot load (unknown agent, reserved task queue, negative
timeout, enabled fallback missing url/key/model, bad branch prefix or
worker id) fails with the usual "load config:" diagnostic — that error is
itself the answer to why the current config is not a working one.

EXAMPLE
  $ daedalus config
`,
	"report": `daedalus report — summarize AI provider usage and worker slots.

USAGE
  daedalus [-c config.yaml] report [--json]        (the resolved queue)
  daedalus report -q <queue> [--json]              (one explicit queue)
  daedalus report --all [--json]                   (every queue on record)

Aggregates the raw per-round provider metrics captured in workflow history
(cost, tokens in/out, cache tokens, duration — stamped per round by the
jailed agent CLIs that report them) into per-session, per-day, per-worker,
and total slices, and lists this host's workers with their live slot
occupancy (busy/total) and rounds queued waiting for a slot.

CLIs without a structured result (opencode, aider) report only the worker's
measured wall time: their rounds count toward ROUNDS and TIME with zero
cost and tokens. pi's --mode json events carry usage, so its rounds report
cost and tokens like claude's and amp's. Rounds that ran before usage
capture carry no Usage at all in history and are absent from the slices
entirely, not zeroed.

FLAGS
  -q, --queue <queue>   Scope the usage slices to this task queue instead of
                        the invoking directory's resolved one.
  --all                 Scope the usage slices to every task queue on record
                        (the daemon-dir config records).
  --json                Emit the whole report as JSON instead of a table.
  --table               Table output (the default; accepted for explicitness).

The worker roster and slot occupancy are always this host's, whatever the
usage scope. Without -q/--all, resolving the default queue needs a config
(./config.yaml, ./.daedalus/config.yaml, or ~/.config/daedalus/config.yaml);
with them, the report works from anywhere, falling back to the default
Temporal address.

EXAMPLE
  $ daedalus report --all
`,
	"wipe": `daedalus wipe — erase a session's work on disk entirely.

USAGE
  daedalus [-c config.yaml] wipe <workflow-id> [--yes]

Stops the session if it is still executing (cancel + wait for its cleanup
to close the workflow), then permanently deletes everything daedalus put
on disk for it: the worktree under
~/.daedalus/worktrees/<task-queue>/<segment> (uncommitted changes die
with it), the session's in-flight feat/<segment>-* branches (stale ones
from crashed runs included), every preserved
<branch-prefix>/<segment>-* deliverable branch, the aborted/<segment>
continue-snapshot branch, the per-worktree session-state file under
~/.daedalus/sessions/, and the task log /tmp/daedalus/<workflow-id>.log.
<segment> is "issue-<id>", prefixed "<flow>-" for any flow other than
feature-dev; the interactive confirmation prints the real derived paths
before asking.

After a successful wipe, Temporal's workflow history is the only record
that the session ever happened: "continue" on the wiped id reports
"nothing to continue" (the aborted/ snapshot it builds on is gone), and
"log" reports the task log missing. "attach" still finds the run — it
reads Temporal alone — and reports the run's recorded outcome.

The session must belong to the active config's task queue: a mismatched -c
fails without acting. An unknown id is a usage error (check "daedalus
list"). Artifacts are erased best-effort — each is reported as removed /
already absent / failed, and the exit is non-zero when anything failed.

Agent transcripts (claude's ~/.claude/projects/<slug>/, pi's
~/.pi/agent/sessions/...) are deliberately left in place: they are
host-side agent state, not run deliverables, and the task log already
carries the rounds' full output.

FLAGS
  --yes   Skip the interactive confirmation (scripted use). Without it,
          wipe prints everything it is about to erase — with an explicit
          warning when the target is parked, whose preserved work is what
          "continue" would resume — and requires typing "yes"; a bare
          invocation never wipes. Wiping parked sessions is allowed: that
          is the point.

EXAMPLE
  $ daedalus wipe daedalus-42
`,
	"version": `daedalus version — print the CLI version.

USAGE
  daedalus version | daedalus -v | daedalus --version
`,
}
