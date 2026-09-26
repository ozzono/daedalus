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
# claude (Claude Code, the default), opencode, amp (Sourcegraph Amp —
# authenticates via AMP_API_KEY in the worker's environment, passed into
# the jail when set; amp's own host login does not reach the jail, and the
# anthropic/openai sections below do not apply to it), pi (Earendil's pi —
# authenticates via the provider env vars below; its host
# ~/.pi/agent/auth.json is bridged into the jail read-write and takes
# priority over them, so a stale host login wins — keep it clean or
# aligned), or aider (authenticates via the provider env vars below; the
# jail masks the repo's .env to empty, so .env files cannot override
# inside jailed rounds).
agent: claude

# Thinking toggle for the jailed agents, independent of slim: defaults to
# true. With the default (or an explicit true) daedalus sends no thinking
# signal at all and every agent follows its own default. Only an explicit
# false makes daedalus send an off signal, translated per agent: pi gains
# "--thinking off" (the knob exists because the host settings.json cannot
# express "off only for daedalus's headless rounds", where a small
# non-reasoning model would burn output budget on thinking blocks), aider
# gains "--thinking-tokens 0", and claude's round env gains
# MAX_THINKING_TOKENS=0. opencode and amp have no verified off lever and
# ignore this setting.
thinking: true

# Streaming toggle for the jailed agents, the same tri-state shape as
# thinking: absent (the default) leaves every agent on its own streaming
# default. An explicit true/false is translated per agent — today only
# aider has a lever ("--stream"/"--no-stream", its documented option
# pair); the other agents ignore the setting.
stream: null

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

# When true, daedalus commits its own work — the approved deliverable and
# the aborted-work snapshot — as author/committer "daedalus
# <daedalus@local>". False (the default) leaves the commits to the
# worker's git config, so they carry whoever runs the worker.
authorship: false

# Name this deployment's worker daemon is managed under: the daemon-dir
# pid/log/config-record files are keyed by it, and "daedalus worker restart
# <id>" addresses it from any directory. Independent of
# temporal.task_queue — two workers may share one queue under different
# ids, each with its own config (e.g. a rotated API key). Empty (the
# default) means the task queue names the worker, so a single-queue
# deployment needs no id.
worker_id: ""

# Ceiling for one execution of the repo's native test suite (a Go
# duration string). Test-command discovery and the suite itself share this
# budget, so it is wider than the 15-minute ceiling the other activities
# use. The jailed agent and worktree cleanup each get their own, wider
# ceilings below; every remaining activity keeps the fixed 15-minute
# StartToClose.
tests_timeout: 30m
# Ceiling for one jailed agent round — the implementation, fix, and
# rebuild rounds (RunJailedClaudeActivity), a Go duration string. Agent
# rounds legitimately run long, so this is wider than the fixed 15-minute
# StartToClose the other activities use.
agent_run_timeout: 45m
# Ceiling for one jailed reviewer round (RunJailedReviewerActivity), a Go
# duration string. The code and test reviewers are jailed rounds like the
# implementer — a whole-repo review legitimately runs long. A review cut
# off at this ceiling retries with its conversation resumed, so
# consecutive windows accumulate instead of restarting.
review_timeout: 15m
# Ceiling for one CleanupWorktreeActivity — committing the aborted-work
# snapshot, unregistering the worktree, and removing its directory — a Go
# duration string. Wider than the shared 15-minute activity ceiling:
# removing a large worktree (e.g. one holding a build tree) is
# filesystem-bound work that has burned the full 15 minutes in practice.
cleanup_timeout: 30m
# How many jailed-agent rounds may run at once on this worker; further
# rounds queue until a slot frees (heartbeating while they wait).
# Concurrent cold agent sessions share one provider account's throughput,
# so unbounded parallelism — many workflows on one task queue — slows
# every run. Rounds also chain into one conversation per run (resuming
# the previous round's session), so this mostly bounds how many runs
# explore a repo cold at the same time. slim: true forces this to 1.
max_concurrent_agent_runs: 2
# Slim mode for limited self-hosted models (small context window, low max
# output tokens) on the aider and pi agents. When true: this worker runs a
# single jailed-agent round at a time — max_concurrent_agent_runs above is
# forced to 1, so no two LLM requests are ever in flight (native test
# suites are unaffected; they dial no LLM) — and jailed aider rounds pin
# aider's weak/editor model wires to the effective AIDER_MODEL (never
# overriding explicit AIDER_WEAK_MODEL/AIDER_EDITOR_MODEL the operator
# exported; no pinning at all when AIDER_MODEL is unset). Applies to the
# deployment regardless of agent, so a "run --cli aider" override works
# without editing this file. The limits themselves stay operator-side:
# for aider, export AIDER_MODEL plus AIDER_MODEL_METADATA_FILE pointing
# outside $HOME (e.g. /etc/aider/models.json, mapping each model to its
# honest max_input_tokens/max_output_tokens) and optionally the budget
# vars AIDER_MAP_TOKENS (0 disables the repo map),
# AIDER_MAX_CHAT_HISTORY_TOKENS, and AIDER_EDIT_FORMAT (diff = small
# output, whole = easy for weak models); for pi, set contextWindow and
# maxTokens per model in ~/.pi/agent/models.json (honest values — pi
# over-stuffs prompts otherwise) plus compaction (reserveTokens/
# keepRecentTokens) in ~/.pi/agent/settings.json. The jail bridges all of
# these: aider reads the worker's exported environment, and the jail's pi
# preset mounts ~/.pi read-write.
slim: false
# How many native test-suite executions may run at once on the test worker
# (the shared "test" task queue); further suites queue until a slot frees
# (heartbeating while they wait). Suites are CPU-bound host work, unlike
# the provider-bound agent rounds above, so this cap is separate.
max_concurrent_tests: 2

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

# Per-agent knob coverage — one agnostic config field per knob, mapped in
# the jailed-round builder to whatever lever the selected agent natively
# supports. Unset always means the agent's own default; "ignored" means no
# wired lever, so the agent proceeds silently on its default (never an
# error):
#
#   knob               claude    aider          opencode  pi        amp
#   -----------------  --------  -------------  --------  --------  ---------
#   context_tokens     env       staged file    ignored   ignored   ignored
#   max_output_tokens  env       staged file    ignored   ignored   ignored
#   thinking: false    env       argv           ignored   argv      ignored
#   stream on/off      —         argv           ignored   ignored   ignored
#   samplers (openai)  (n/a)     staged file    ignored   ignored   (n/a)
#
# Lever detail: claude env = CLAUDE_CODE_MAX_CONTEXT_TOKENS (window) and
# MAX_THINKING_TOKENS=0 (thinking) and CLAUDE_CODE_MAX_OUTPUT_TOKENS
# (completion cap). aider's staged file = the model metadata and settings daedalus
# stages into .daedalus-aider/ (window input side, output cap, sampler
# params); aider argv = --thinking-tokens 0 and --stream/--no-stream (the
# stream toggle). pi argv = --thinking off.
# amp is unprobed: no lever assumed until someone probes one.

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
  # Small/fast model for tiny prompts (session-title generation and the
  # like), exported as ANTHROPIC_DEFAULT_HAIKU_MODEL — the var the jailed
  # claude reads for its small/fast model. "worker status" also probes the
  # provider with it (falling back to model) when set. Unrelated to the
  # workflow layer's quota heartbeats. Empty means the agent's own default.
  heartbeat_model: ""
  # API request timeout for the jailed agent, in milliseconds, exported as
  # API_TIMEOUT_MS. Agent rounds run long — builds, whole-repo analyses — so
  # the default is generous: 3000000 = 50 minutes. Zero/unset falls back to
  # this default; there is no inherit-the-environment escape hatch.
  timeout_ms: 3000000
  # Context window for the jailed agent's model, in tokens. When set, it
  # exports as CLAUDE_CODE_MAX_CONTEXT_TOKENS (which jailed claude honors
  # for its compaction/budget math) and replaces the input side of the
  # model metadata staged for aider rounds; opencode, pi, and amp ignore
  # it (opencode's only mechanism is a config-file pointer whose
  # precedence against a repo-committed opencode.json was never probed).
  # Zero/unset means each agent's own default.
  context_tokens: 0
  # Completion cap for the jailed agent's rounds, in tokens. When set, it
  # replaces both sides of the 8k constant in the model metadata staged
  # for aider rounds (the metadata's max_output_tokens and
  # extra_params.max_tokens) and claude's round env gains
  # CLAUDE_CODE_MAX_OUTPUT_TOKENS (the var claude 2.1.283's own error text
  # names for its request-level max_tokens override). opencode/pi/amp have
  # no wired route. Zero/unset keeps aider's 8192 constant and every other
  # agent's API default.
  max_output_tokens: 0

openai:
  # Optional OpenAI settings, injected into the jailed agent's environment
  # (OPENAI_BASE_URL / OPENAI_API_BASE / OPENAI_API_KEY / OPENAI_MODEL —
  # both URL vars carry the same value, because litellm versions disagree
  # on which they honor) for tooling the agent runs. When set, an aider
  # round also consumes the section directly: the worker selects aider's
  # model with "--model openai/<model>" (the openai/ prefix is what routes
  # aider's litellm layer to the custom endpoint instead of
  # api.openai.com) and stages model metadata sized for self-hosted models
  # into the worktree's .daedalus-aider/ scratch dir. Same rule as
  # anthropic.url: empty = inherit the environment.
  url: ""
  key: ""
  model: ""
  # API request timeout for jailed rounds served by this section, in
  # milliseconds, exported as API_TIMEOUT_MS (it replaces anthropic's
  # whenever url above is set). Zero/unset falls back to anthropic's
  # timeout value, so a ceiling always applies; set it explicitly for
  # self-hosted endpoints whose litellm would otherwise apply only ~600s
  # on its own.
  timeout_ms: 0
  # Sampler knobs for rounds served by this section — sampler behavior is
  # a property of the backend behind url, shared by every agent that dials
  # it. Consumed today only by aider's staged model settings: top_p,
  # presence_penalty, and repetition_penalty as standard litellm params,
  # top_k and min_p under extra_body (litellm's syntax for params it does
  # not map natively). pi's passthrough is unprobed, so pi ignores these
  # until probed; claude and amp are out of scope. Zero/unset means the
  # field is omitted from the request entirely — never sent as a default,
  # so the backend's own sampler values stand (an explicit 0 above is
  # therefore the same as unset).
  top_p: 0
  presence_penalty: 0
  top_k: 0
  min_p: 0
  # Anti-loop penalty re-landed from the 2026-09-25 self-hosted round,
  # where it was the empirically decisive loop fix (its constants were
  # probe-verified live against the tenor litellm proxy but lost before
  # landing).
  repetition_penalty: 0

# Secondary provider for automatic failover — fully independent of the
# primary: the url/key/model may point at a different vendor's
# anthropic-compatible endpoint, sharing nothing with the anthropic:
# section above. When a jailed round fails with the primary API exhausted
# (429 quota), the worker retries the round once against these values and
# keeps using them until the primary's quota window resets (parsed from
# the provider's reset message when present, else 20m/40m/60m escalating
# holds).
# Absent, or enabled: false, disables failover; an enabled fallback needs
# url, key, and model. Values travel the same env-only channel as the
# primary's.
# type selects the endpoint's wire style: anthropic (the default — an
# Anthropic-compatible endpoint) or openai (an OpenAI-compatible
# chat-completions endpoint). It governs the "worker status" probe and
# which environment vars failover values travel on. A jailed round's wire
# is chosen by the agent itself — claude dials ANTHROPIC_*, so an
# openai-style fallback serves only agents that dial OPENAI_BASE_URL.
fallback:
  enabled: false
  type: anthropic
  url: ""
  key: ""
  model: ""
  heartbeat_model: ""
`
