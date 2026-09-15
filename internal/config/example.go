// Package config loads Daedalus runtime configuration from a YAML file.
package config

// ExampleYAML is the fully-commented example configuration, every field at
// its default value. `daedalus init` writes it as config-example.yaml in the
// working directory. It is kept in lockstep with the repository's own
// config-example.yaml by TestExampleYAMLMatchesRepoFile, and TestExampleYAML
// pins that loading it yields exactly the default configuration.
const ExampleYAML = `# Daedalus configuration. Copy to config.yaml and edit; every field is
# optional. Provider values that are unset are simply not exported — the
# jailed agent then inherits whatever the worker's environment provides.

# Which jailed agent CLI runs the implementing and reviewer agents:
# claude (Claude Code, the default) or opencode.
agent: claude

# Prefix naming the preserved branch that carries a run's approved, committed
# work: <prefix>/issue-<id>-<unix timestamp>. Independent of
# temporal.task_queue (which routes workflows and scopes worktree paths) —
# this is repo-facing branch naming. "daedalus run --prefix" overrides it
# per run. The in-flight feat/ and aborted/ snapshot branches are internal
# and keep their fixed names, so a branch_prefix starting with feat or
# aborted is rejected. The prefix scopes the run's preserved branch and the
# finalized-deliverable check, but not the aborted/issue-<id> snapshot,
# which is shared per issue across prefixes: a failing run under another
# prefix replaces it and is not suppressed by a deliverable finalized under
# this prefix.
branch_prefix: daedalus

temporal:
  # Temporal frontend address. 7233 is Temporal's own default, so a plain
  # "temporal server start-dev" matches.
  host: 127.0.0.1:7233
  # Temporal UI port. Informational only — never connected to, but shown at
  # worker startup. 8233 is the Temporal UI's own default.
  ui_port: 8233
  # Routing key for this deployment's workflows. Distinct projects or flows
  # sharing one Temporal server use distinct task queues — the queue also
  # scopes workflow IDs and worktree paths, so nothing collides across them.
  task_queue: daedalus

anthropic:
  # API base URL. Empty (the default) means "use whatever the worker's
  # environment already provides" — set it only to override, e.g. a
  # proxy/gateway.
  url: ""
  # API key. Optional: when set it is injected into the jailed agent's
  # environment (never logged or passed via command-line arguments); when
  # unset, the agent authenticates via the worker's inherited environment
  # or its own login.
  key: ""
  # Model for the jailed agent; empty means the agent's own default.
  model: ""
  # API request timeout for the jailed agent, in milliseconds, exported as
  # API_TIMEOUT_MS. Agent rounds run long — builds, whole-repo analyses — so
  # the default is generous: 3000000 = 50 minutes. Zero/unset falls back to
  # this default; there is no inherit-the-environment escape hatch.
  timeout_ms: 3000000

openai:
  # Optional OpenAI settings, injected into the jailed agent's environment
  # (OPENAI_BASE_URL / OPENAI_API_KEY / OPENAI_MODEL) for tooling the agent
  # runs; not consumed by daedalus itself. Same rule as anthropic.url:
  # empty = inherit the environment.
  url: ""
  key: ""
  model: ""
`
