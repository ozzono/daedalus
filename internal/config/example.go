// Package config loads Daedalus runtime configuration from a YAML file.
package config

import "strings"

// The example in its composition pieces: the exampleBase* consts are the
// base configuration — everything outside a profile's blocks — that every
// `daedalus init` invocation writes; exampleSlimBlock/exampleSlimProviders
// are the "slim" profile's slices and examplePromptBlock the "prompt"
// profile's. Their concatenation in file order is ExampleYAML.
const exampleBase1 = `# Daedalus configuration. Copy to config.yaml and edit; every field is
# optional. Provider values that are unset are simply not exported — the
# jailed agent then inherits whatever the worker's environment provides.

# Which jailed agent CLI runs the implementing and reviewer agents:
# claude (Claude Code, the default), opencode, amp (Sourcegraph Amp —
# authenticates via AMP_API_KEY in the worker's environment, passed into
# the jail when set; amp's own host login does not reach the jail, and the
# anthropic/openai sections below do not apply to it), pi (Earendil's pi —
# authenticates via the provider env vars below, except the openai
# section which pi only half-reads over env (key yes, base URL no) and
# which is therefore staged into ~/.pi/agent/models.json per round — see
# the openai section; its host
# ~/.pi/agent/auth.json is bridged into the jail read-write and takes
# priority over them, so a stale host login wins — keep it clean or
# aligned), codex (OpenAI's codex CLI — authenticates like pi via its host
# login state (~/.codex, bridged into the jail read-write) plus the
# provider env vars below, but reads no OPENAI_* var natively and speaks
# only OpenAI wire formats: an openai-section round is bridged per
# invocation through codex's own "-c" config overrides (see the openai
# section), while an anthropic-section round has no codex channel at all),
# or aider (DEPRECATED — it keeps working exactly as it does, but accepts
# no new flags, fixes, or probes; authenticates via the provider env vars
# below; the
# jail masks the repo's .env to empty, so .env files cannot override
# inside jailed rounds). opencode alert: neither the openai nor the
# anthropic base-URL env vars reach it (source-verified 2026-09-26), but
# an openai-section round is bridged regardless: the worker stages a
# round-scoped config file into the worktree (a "daedalus-openai"
# provider entry — baseURL, the key as an env template so it never lands
# on disk, the model's limits) and exposes it via OPENCODE_CONFIG,
# selecting the staged model with -m — no operator-side opencode.json
# work needed. The anthropic section stays unbridged (opencode has no
# ANTHROPIC_* channel), and the jail mounts no opencode config dir, so
# such a round resolves its own config inside the jail: a repo-committed
# opencode.json is honored, the host's global one is not.
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
# This worker-level semaphore is the only concurrency limit on the pi
# path: pi runs one process per round with no internal concurrent-session
# or rate knob of its own (source-verified 2026-09-26), and the slot-wait
# behavior is identical to aider/claude rounds.
max_concurrent_agent_runs: 2
# Cap on provider requests per minute for this deployment: jailed pi
# rounds' chat-completions calls — agent and reviewer alike — are paced
# through the worker's loopback relay to at most this many per sliding
# one-minute window, a request beyond the cap waiting inside the relay
# until the window frees. For a provider whose API tier rate-limits by
# request count (a free 15 req/min tier, say): staying under the cap
# avoids the 429 quota-heartbeat crawl entirely instead of absorbing it.
# Enforcement rides the relay's staging, so it covers exactly the rounds
# the relay serves — requires agent: pi and an openai.url (the relay
# forwards that channel); any other shape is rejected at load, and a
# reviewer.url or openai-fallback override bypasses it. 0 (the default)
# means unthrottled.
max_requests_per_minute: 0
`

// exampleSlimBlock is the slim: section (the mode toggle and the tool-call
// relay's parser model) — the "slim" profile's flow-side slice.
const exampleSlimBlock = `# Slim mode for limited self-hosted models (small context window, low max
# output tokens) on the aider and pi agents. When enabled: this worker runs a
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
# slim also gates the run flow: with slim.enabled: true, a "daedalus run" without
# an explicit -w/--workflow starts the slim flow — the micro-stepped
# atomic loop for limited models (a planner round writes the plan in
# prose and a parse round transcribes it into an ordered queue of 1–2-file
# sub-tasks, then each sub-task runs its own
# implement ↔ review loop with a fresh reviewer session per round and the
# native suite as ground truth) — instead of feature-dev. An explicit -w
# always wins. The flow targets the pi agent (aider is deprecated for
# it).
slim:
  # Turns slim mode on: the single-round concurrency clamp, the
  # DAEDALUS_SLIM=1 export, and the run-flow default described above.
  # Breaking reshape (2026-10-02): the former top-level "slim: true"
  # boolean moved here.
  enabled: false
  # Model the tool-call relay (a worker-hosted loopback reverse proxy)
  # uses to normalize lifted arguments: pi executes only native
  # tool_calls, which text-emitting models like qwen2.5-coder never send —
  # they emit fenced JSON in the message content instead. With a
  # parser_model set (and the openai section above configured), the relay
  # inspects pi's chat-completions traffic, and an answer that is exactly
  # one JSON object naming one of the request's own tools is converted
  # into a native tool call, its arguments normalized by this model via
  # ollama structured output against the tool's own schema. That schema
  # conformance holds on ollama and other format-honoring parser
  # upstreams; an OpenAI-compatible upstream that silently ignores
  # ollama's format field leaves the arguments checked only as a JSON
  # object. Prose is never
  # converted (reviewer verdicts are content), and any relay or parser
  # failure degrades to the old inert-text behavior — never a new run
  # failure. Resolved against the openai-channel upstream (the provider
  # section's url when its type is openai, else the openai section's);
  # create the tag on the model host before setting it. Empty (the
  # default) keeps the relay off.
  parser_model: ""
  # Ceiling on the task input a slim run may embed, in o200k_base tokens,
  # counted after the automatic shrink pass (internal/shrink) has minimized
  # it; a raw input whose biased-high chars/4 estimate is already over the
  # ceiling is refused before the shrink runs (the strict counter is
  # quadratic in one unbroken word, so it never sees oversized text). A run
  # whose input still counts over this fails at submit — no workflow, no
  # worktree, no worker round — and asks for the task to be split. Keep it
  # under the smallest staged model's honest contextWindow minus the output
  # budget it needs. 0 (or the key unset) = 4098; a negative value fails
  # the config load.
  max_input_tokens: 4098
`

// exampleMidBlock is the mid: mode toggle — always part of the example,
// because mid has no init profile: the scaffolded profiles are slim and
// prompt, and mid is one boolean beside them, discoverable here.
const exampleMidBlock = `# Mid-tier mode for capable remote models (large context windows) served on
# the deployment's own provider channel — either wire: the provider: section
# or the legacy anthropic:/openai: sections, exactly as they serve rounds
# today. mid is one boolean beside slim, and slim.enabled and mid are
# mutually exclusive — one model tier per deployment, so setting both fails
# the config load naming both keys. Flagship remains the default: with
# neither knob set, behavior is unchanged.
# What mid changes: with it on, the config must carry verdict_repair (url
# and model — the same load requirement slim has), because a marker-less
# reviewer verdict routes through the decision model's /v1/systemone
# contract (the four markers as named choice criteria, scored, never
# generated) instead of counting a blind marker-less strike. What it
# deliberately does not change: no endpoint fields of its own, no wire
# restriction, no concurrency clamp, no flow reroute, no per-agent branches
# — the deployment's channel and agent wiring serve it as-is, for every
# agent. A wire-vs-agent mismatch (an agent that cannot speak the only
# served wire) fails the run at launch naming both.
# Remote-model readiness checklist for a mid deployment (the audit
# posture): keep keys in the environment, never as disk literals in this
# file (a literal key here lands in the file and every copy of it); size
# the context_tokens / max_output_tokens pair honestly on the serving
# section — the window must cover the prompt plus bootstrap plus reserve
# plus the output budget (provider:'s pi clamp note below applies to any pi
# round); set timeout_ms on the serving section for remote latency, so a
# gateway's own shorter ceiling does not fold long reviewer turns first;
# and expect no slim-only knobs here — the parser_model relay, the
# input-token ceiling, and the single-round clamp above are slim's, not
# mid's.
mid: false
`

// exampleBase2 is the remaining general fields plus the anthropic section
// through timeout_ms.
const exampleBase2 = `# How many native test-suite executions may run at once on the test worker
# (the deployment's suite task queue); further suites queue until a slot
# frees (heartbeating while they wait). Suites are CPU-bound host work,
# unlike the provider-bound agent rounds above, so this cap is separate.
max_concurrent_tests: 2
# Where this deployment's native test suites execute. true (the default,
# and the effect of leaving the key out) schedules them on the one
# Temporal-wide "test" queue that every deployment's workers poll — any
# deployment's worker can run any deployment's suite. false gives the
# deployment its own derived queue ("<task_queue>-test") that only workers
# started from this config schedule onto and poll, so a stale worker of
# another deployment can no longer serve — or fail — this deployment's
# suites; flip it and upgrade this deployment's daemons in the same pass,
# or suites sit Scheduled with no poller on the new queue.
shared_test_queue: true

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
#   knob               claude    aider          opencode    pi            amp         codex
#   -----------------  --------  -------------  ----------  ------------  ----------  ---------
#   context_tokens     env       staged file    ignored     staged file*  ignored     ignored
#   max_output_tokens  env       staged file    ignored     staged file*  ignored     ignored
#   thinking: false    env       argv           ignored     argv          ignored     ignored
#   stream on/off      —         argv           ignored     ignored       ignored     ignored
#   samplers (openai)  (n/a)     staged file    ignored     staged file*  (n/a)       ignored
#   timeout_ms         env       (unprobed)     (unprobed)  staged file*  (unprobed)  (unprobed)
#
# Lever detail: claude env = CLAUDE_CODE_MAX_CONTEXT_TOKENS (window) and
# MAX_THINKING_TOKENS=0 (thinking) and CLAUDE_CODE_MAX_OUTPUT_TOKENS
# (completion cap). aider's staged file = the model metadata and settings daedalus
# stages into .daedalus-aider/ (window input side, output cap, sampler
# params); aider argv = --thinking-tokens 0 and --stream/--no-stream (the
# stream toggle). pi argv = --thinking off. pi's staged file (*) = the
# provider entry daedalus stages into the host's ~/.pi/agent/models.json
# for rounds served by the openai section (model contextWindow/maxTokens
# and samplingParams; see the openai section below), plus — for timeout_ms
# only — both of pi's request-timeout keys merged into the host's
# ~/.pi/agent/settings.json (retry.provider.timeoutMs and top-level
# httpIdleTimeoutMs): pi reads no timeout env var and its provider
# entries carry no timeout it plumbs, so settings.json is its only route
# for them, and pi's own 5-minute defaults fold a slow
# round (reviewer turns at self-hosted pace legitimately run longer)
# before it delivers a verdict. Outside those staged rounds pi has no
# route for these knobs and ignores them. pi's samplers ride
# samplingParams as top-level OpenAI-completions request params with no
# litellm layer in between, so top_k/min_p reach only backends that accept
# non-standard OpenAI params.
# amp is unprobed: no lever assumed until someone probes one. codex is
# likewise unprobed for every knob row — no lever assumed — but note the
# codex column says nothing about provider serving: the openai section's
# url/key/model reach codex rounds regardless, bridged through codex's own
# per-invocation "-c" config overrides (see the openai section below); only
# these knob rows have no codex route.

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
`

// exampleSlimProviders is the "slim" profile's provider sizing: anthropic's
// context_tokens/max_output_tokens and the whole openai: section.
const exampleSlimProviders = `  # Context window for the jailed agent's model, in tokens. When set, it
  # exports as CLAUDE_CODE_MAX_CONTEXT_TOKENS (which jailed claude honors
  # for its compaction/budget math), replaces the input side of the
  # model metadata staged for aider rounds, lands as contextWindow on
  # the model entry staged for openai-served pi rounds, and lands as
  # limit.context on the entry staged for openai-served opencode rounds
  # (that staged file rides OPENCODE_CONFIG, whose precedence against a
  # repo-committed opencode.json was never probed); amp and codex
  # ignore it. Zero/unset means each agent's own default.
  context_tokens: 0
  # Completion cap for the jailed agent's rounds, in tokens. When set, it
  # replaces both sides of the 8k constant in the model metadata staged
  # for aider rounds (the metadata's max_output_tokens and
  # extra_params.max_tokens), claude's round env gains
  # CLAUDE_CODE_MAX_OUTPUT_TOKENS (the var claude 2.1.283's own error text
  # names for its request-level max_tokens override), lands as
  # maxTokens on the model entry staged for openai-served pi rounds, and
  # lands as limit.output on the entry staged for openai-served opencode
  # rounds. amp and codex have
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
  # into the worktree's .daedalus-aider/ scratch dir. A pi round consumes
  # it too: pi honors OPENAI_API_KEY but has no base-URL env channel (a
  # key alone would silently dial api.openai.com), so the worker stages
  # the section as a provider entry in the host's ~/.pi/agent/models.json
  # and selects the model with "--model <id>/<model>", where <id> is a
  # round-unique "daedalus-openai-<token>" entry — concurrent rounds (two
  # workers on one host, or two runs in one worker) each stage and read
  # their own entry, and it is removed when the round ends. The
  # entry carries the base URL, an ${OPENAI_API_KEY} template (the key
  # value itself never lands on disk), and the model with the window/output
  # cap and sampler knobs below; an openai section without url or without
  # model fails a pi round before launch rather than let it dial the cloud
  # default. The guard keys on the exported env vars, not on this section:
  # a foreground run inherits the invoking shell, so an ambient
  # OPENAI_API_KEY with no OPENAI_BASE_URL/OPENAI_MODEL fails pi rounds
  # the same way — unset it or export a full section. A codex round
  # consumes the section too, though codex reads no OPENAI_* var natively:
  # the worker bridges it per invocation through codex's own "-c" config
  # overrides (a staged model_providers entry carrying the base URL, the
  # key's env-var NAME — the key value itself never lands in argv or on
  # disk — and codex's responses wire, plus --model). Same failure posture
  # as pi's: an openai section without url or without model fails a codex
  # round before launch, and the guard keys on the exported env vars, so
  # an ambient partial section fails codex rounds the same way. The
  # section's timeout and sampler knobs have no codex route (a
  # model_providers entry carries no request params) — the backend's own
  # defaults stand. Same rule as
  # anthropic.url: empty = inherit the environment.
  url: ""
  key: ""
  model: ""
  # API request timeout for jailed rounds served by this section, in
  # milliseconds, exported as API_TIMEOUT_MS (it replaces anthropic's
  # whenever url above is set). Zero/unset falls back to anthropic's
  # timeout value, so a ceiling always applies; set it explicitly for
  # self-hosted endpoints whose litellm would otherwise apply only ~600s
  # on its own. pi ignores the exported var; its pi staging bridge merges
  # this value into the host's ~/.pi/agent/settings.json as both
  # retry.provider.timeoutMs (the SDK's time-to-headers ceiling) and the
  # top-level httpIdleTimeoutMs (the undici dispatcher header/body-idle
  # ceiling that actually kills a buffered upstream — pi's 5-minute
  # defaults otherwise), so this one field bounds pi rounds too.
  # Note that merge overwrites whatever values the host user had set for
  # their own interactive pi on these keys — inherent: settings.json is
  # pi's only channel for them — and httpIdleTimeoutMs is pi's global
  # dispatcher ceiling, scoping every HTTP request pi makes (interactive
  # rounds included), not just staged-round provider calls. Keep the
  # value at least as long as one
  # honest reviewer turn at the served model's pace: pi folding first
  # makes rounds exit verdict-less, the run parks after three such rounds
  # in a row, and review_timeout never fires because pi gives up before
  # the round ceiling could.
  timeout_ms: 0
  # Sampler knobs for rounds served by this section — sampler behavior is
  # a property of the backend behind url, shared by every agent that dials
  # it. Consumed today by aider's staged model settings (temperature,
  # top_p, presence_penalty, and repetition_penalty as standard litellm
  # params, top_k and min_p under extra_body — litellm's syntax for params
  # it does not map natively) and by pi's staged models.json entry (all
  # six as top-level OpenAI-completions request params, sent straight to
  # url). claude and amp are out of scope. Zero/unset means the
  # field is omitted from the request entirely — never sent as a default,
  # so the backend's own sampler values stand (an explicit 0 above is
  # therefore the same as unset).
  top_p: 0
  # Sampling temperature for rounds served by this section. Slim's design
  # target is a low-temperature planner (0.2) with top_p 0.9, but samplers
  # are deployment-level — there is no per-phase machinery — so set them
  # here and they apply to every round of every run served by this
  # section.
  temperature: 0
  presence_penalty: 0
  top_k: 0
  min_p: 0
  # Anti-loop penalty re-landed from the 2026-09-25 self-hosted round,
  # where it was the empirically decisive loop fix (its constants were
  # probe-verified live against the tenor litellm proxy but lost before
  # landing).
  repetition_penalty: 0
`

// exampleProviderBlock is the wire-agnostic provider: section — the new
// single home for a model's serving knobs. Base configuration: it appears
// in every profile.
const exampleProviderBlock = `
# The wire-agnostic primary provider: type — not the section name — selects
# the wire format, so one section fully determines how the model behind it
# is served: endpoint, request ceiling, honest sizing, and (openai only) the
# sampler knobs. Its values export on the env channel its type names
# (ANTHROPIC_* or OPENAI_*), so every consumer — jailed claude, the
# aider/pi/codex/opencode staging, "worker status", the slim tool relay —
# serves a provider-served round exactly as it would the equivalent legacy
# section. The legacy anthropic:/openai: sections keep working unchanged
# and indefinitely; the one forbidden mix is this section plus the
# same-wire legacy section (two sources of truth for one channel is a load
# error, never a silent override) — across wires the mix is legal and is
# the incremental migration path (e.g. this section with type: openai for
# a self-hosted model while jailed claude stays on the anthropic: section;
# keep the sizing pair in one place, it is one channel whichever wire
# serves).
provider:
  # Wire format this section serves, and the env channel its values export
  # on: "anthropic" or "openai". Required — a missing or unknown value is a
  # load error, never a silent default.
  type: anthropic
  # API base URL, key, and model, injected into the jailed agent's
  # environment on the channel type selects, with the same
  # unset-means-inherit rule as the legacy sections: an empty field exports
  # nothing. An aider/pi/codex/opencode round consumes them exactly as it
  # would the legacy section of the same wire.
  url: ""
  key: ""
  model: ""
  # Small/fast model for tiny prompts, exported as
  # ANTHROPIC_DEFAULT_HAIKU_MODEL and probed by "worker status" — an
  # anthropic-wire knob (claude's var is its only consumer), so an
  # openai-typed provider carrying it is a load error. Empty means the
  # agent's own default.
  heartbeat_model: ""
  # API request timeout for the jailed agent, in milliseconds, exported as
  # API_TIMEOUT_MS — both wire styles need it. Zero/unset falls back to the
  # same generous default as the legacy sections (3000000 = 50 minutes), so
  # a ceiling is always exported.
  timeout_ms: 3000000
  # Sizing pair, valid on both wire styles and deliberately wire-agnostic:
  # they ride the same exports as their anthropic-section counterparts
  # (CLAUDE_CODE_MAX_CONTEXT_TOKENS / DAEDALUS_MAX_OUTPUT_TOKENS), which is
  # the point — an openai-served model's sizing used to be expressible only
  # under anthropic:, the mis-sectioning this section removes. They reach
  # every staging path that consumes sizing: claude's env re-export,
  # aider's model metadata, the pi and opencode staged model entries. Load
  # rejects negative values. Sizing caveat for pi rounds (the 2026-10-10
  # RCA of three parked wa-termo bootstraps): pi computes each request's
  # max_tokens as the staged contextWindow minus its prompt estimate minus
  # a 4096-token reserve, floored at 1 (pi-ai simple-options.js
  # clampMaxTokensToContext, 0.87.1) — an understaged context_tokens yields
  # silent 1-token replies, so stage the window honestly.
  context_tokens: 0 # both wire styles; see the pi clamp note above
  max_output_tokens: 0
  # Sampler knobs for rounds served by this section — the openai section's
  # fields with the same absent-never-sent discipline (an explicit 0 is
  # indistinguishable from unset, so the backend's own sampler values
  # stand). An openai-wire property — chat-completions request params — so
  # an anthropic-typed provider carrying any of them is a load error (no
  # anthropic-wire staging consumes them; a knob must fail loud, never
  # silently no-op).
  top_p: 0
  temperature: 0
  presence_penalty: 0
  top_k: 0
  min_p: 0
  repetition_penalty: 0
`

// exampleBase3 is the base provider plumbing and toggles: the fallback,
// reviewer, review_skill_list, bug_filing, test_output, dependency, and
// metrics sections.
const exampleBase3 = `
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
# pi can only be failed over by an openai-type fallback (an anthropic-type
# one has no pi channel at all — pi reads no ANTHROPIC_BASE_URL); with an
# anthropic-type fallback a pi round stays pinned to the primary's staged
# endpoint and its "failure" is benched against the fallback side without
# the fallback ever serving it (see
# backlog/bugs/pi-anthropic-fallback-unserveable.md).
fallback:
  enabled: false
  type: anthropic
  url: ""
  key: ""
  model: ""
  heartbeat_model: ""

# Reviewer rounds' own provider endpoint. Absent (the default): the
# reviewer rounds (code reviewer, test reviewer, docs reviewer)
# authenticate exactly like the implementing agent's rounds — the serving
# provider's endpoint and key. Setting url and/or key overrides just those
# values for reviewer rounds: they dial url with key while implementing and
# test rounds keep the primary's (and a failover stint keeps the reviewer
# on its own endpoint too — the override applies on both provider sides).
# The values travel worker env only (never workflow history, activity
# inputs, or argv). Models are not overridable: the reviewer endpoint must
# serve the already-configured model names. pi's staging bridge reads the
# overridden OPENAI_* values, so pi reviewer rounds stage the reviewer
# endpoint; amp authenticates through AMP_API_KEY alone and ignores the
# section.
reviewer:
  url: ""
  key: ""

# Extra skill reviews appended to each review-gated phase of the
# feature-dev flow. Empty (the default): every phase runs the historical
# default-only reviews. With entries, each phase runs the default daedalus
# review ping-pong first, then one ping-pong per entry, in order — the
# code phase routes each skill's feedback through the implementing
# agent's session, the test phase through the tester's — each with its
# own reviewer conversation, the same verdict protocol (APPROVED /
# CHANGES_REQUESTED / NEEDS_MAINTAINER, plus the test phase's REBUILD),
# and the same park handling and loop caps as the default reviewer. A
# skill reviewing the test suite may verdict REBUILD exactly like the
# default reviewer, routing an implementation-level finding back through
# the dev cycle. Each entry is either /name — resolved at round time to
# <name>/SKILL.md under the run worktree's .claude/skills (the project
# wins) or the worker's ~/.claude/skills — or hand-written prompt text,
# passed through verbatim; a skill that resolves nowhere, or to nothing
# but its frontmatter header, fails the round loudly — a skill review
# never silently degrades to the default one. A leading / is what marks a
# skill name, so hand-written text cannot begin with one. The worktree
# copy winning is deliberate (skills may be created after submit), but
# .claude/skills there is the implementing agent's to write mid-run — the
# agent can author or replace the discipline its own skill reviewer
# applies, so the fixed preamble's verdict contract, not that directory,
# is the trust boundary. Whatever the instructions say, they render into
# the reviewer prompt under a fixed preamble restating the verdict marker
# contract and the harness-parsed-output rule, so configured text can
# never reach a reviewer without daedalus's communication rules above it.
# The list is frozen into the run at submit;
# a continued run keeps its attempt's list; blank entries are rejected at
# load.
review_skill_list: []

# Decision-model repair of marker-less review verdicts. When a reviewer
# round's output carries no parseable verdict marker, the review activity
# asks this classify-only model to read the review and name the most likely
# verdict among the four markers (APPROVED, CHANGES_REQUESTED,
# NEEDS_MAINTAINER, REBUILD). A high-confidence APPROVED promotes the round
# to approval; every other outcome only annotates the round — it continues
# as changes requested, and a NEEDS_MAINTAINER or REBUILD classification
# never triggers its lifecycle action. The endpoint is dialed with one
# systemone decision request — the decision model's native /v1/systemone
# surface, which the OpenAI-compat chat paths reject — so url names the
# endpoint's /v1 base (Ollama serving tev1: http://localhost:11434/v1,
# model tev1:latest). No tools, no session, no streaming; the answer is
# the scored choice, never generated text. Any failure — section unset,
# endpoint unreachable, slow, or answering garbage — degrades to the
# historical behavior: the round counts as changes requested with the whole
# output as comments, and the marker-less strike is counted exactly as
# before; three consecutive marker-less rounds park. key is optional (a
# local relay needs none). With slim.enabled: true the section is required
# — url and model must be set or the config fails to load — because slim's
# small models are the marker-less-exit generator and this layer is its
# recovery mechanism.
verdict_repair:
  url: ""
  model: ""
  key: ""

# Out-of-scope-bug filing. Off by default (the section absent, or
# enabled: false): jailed rounds report out-of-scope bugs in their reply or
# review comments only, and no bug files are written into your repos. When
# enabled, the prompts additionally instruct every round to file each
# out-of-scope bug it finds as a file under dir (with the same project-own
# record note as before): dir is worktree-relative and resolves against
# each run's worktree root, so the files are ordinary committed content
# of the branch and reach the real repo on merge. dir empty keeps the
# historical backlog/bugs path. While filing is enabled, load rejects an
# absolute dir, one escaping the worktree root (".."), a ~-prefixed one
# (~ is not expanded for dirs — use mirror for host paths), or one with
# empty or dot path components; any other bytes render verbatim into the
# round prompts.
# mirror, when set, names a host directory each filed bug file lands in:
# absolute, or ~/… expanded against the worker's home (a relative mirror,
# the filesystem root, or a colon is rejected at load). The host dir is
# bind-mounted read-write into each jailed round's sandbox at the
# worktree-relative dir, so the agent's writes land on the host directly —
# one mechanism, no post-round copy — and the files never enter the real
# worktree, so they do not ride the branch. Without the mount (or on an
# ai-jail that rejects it — such a round fails loudly, it never runs
# unmounted), the worker-side copy after the round remains as the fallback
# shape. Bug files already committed to your branches under dir from before
# a mirror was configured stay on the branch while the mount shadows them
# in future rounds. Accepting this trade is yours: a configured mirror
# hands the jailed agent a read-write window onto a host path — repo
# content could steer writes under it — so point it at a dedicated
# directory, never at a shared or sensitive one. mirror empty keeps
# mirroring off.
bug_filing:
  enabled: false
  dir: ""
  mirror: ""

# Native test-suite output dumping. Off by default (the section absent, or
# enabled: false): nothing is written and suite output reaches the run's
# task log only, as before. When enabled, every suite execution writes its
# complete combined output to <dir>/<timestamp>.log in the run's worktree
# (a same-second collision gains a -2, -3, … suffix, never an overwrite)
# and the run's tester and reviewer are told the path — a worktree-relative
# dir puts the dump inside the agent sandbox's only mounted tree, so a fix
# loop holding a transport-bounded tail can read the full record. Files
# accumulate in the worktree; the run's worktree cleanup disposes of them.
# The dump is transient and never part of the deliverable: the activity
# keeps the dir out of "git status" via the repository's .git/info/exclude.
# dir empty keeps the default .daedalus/test-output path. While dumping is
# enabled, load rejects an absolute dir, one escaping the worktree root
# (".."), a ~-prefixed one (~ is not expanded for dirs — use mirror for
# host paths), or one with empty or dot path components; any other bytes
# are used verbatim. mirror, when set, names a host directory the worker
# copies each dump into after the suite runs — cleanup otherwise disposes
# of the dumps. Same accepted shapes and rules as bug_filing's mirror;
# empty keeps mirroring off.
test_output:
  enabled: false
  dir: ""
  mirror: ""

# What a run chained behind another (-dep/--depends) does when its
# dependency stops without approval. The default — every skip flag false,
# no fallback branch — is the historical refusal: the waiting run fails
# before any worktree, branch, or round existed, and re-submitting the
# chain is your decision. Setting fallback_branch turns the gate into a
# switch: a dependency that stops in a skip-listed state releases the
# waiting run, which starts its worktree from that branch instead of
# failing. fallback_branch empty (the default) means the branch you
# submitted the run from, resolved once at submit (a detached HEAD refuses
# and names this key as the fix); an explicitly set branch is verified to
# exist at submit, so a typo fails the run before the dependency gate
# instead of after it; the resolved name is frozen into the run,
# so a config edit cannot retarget an in-flight chain. skip_parked defaults
# true: a parked dependency is the one stopped state whose work survives on
# its aborted branch (resumable with "daedalus continue"), so its
# dependent routinely starts rather than strands. skip_failed, skip_stuck
# (the dependency ran past its timeouts and was killed), and skip_canceled
# (canceled or terminated) default false — a dependency that died those
# ways produced nothing to build on, and releasing past one is an explicit
# choice. A paused dependency is outside every skip set: it has not
# stopped, so the gate waits and logs the unpause line
# ("temporal workflow unpause"). enabled: false restores the unconditional
# refusal whatever the other fields say.
dependency:
  enabled: true
  fallback_branch: ""
  skip_parked: true
  skip_failed: false
  skip_stuck: false
  skip_canceled: false

# The metrics subsystem: a local LLM relay every provider-bound round
# dials, the per-request usage ledger it keeps (tokens in/out, cache
# tokens, latency — parsed from the provider responses), and the
# host-global "daedalus metrics" daemon that hosts both plus the
# dashboard. Usage capture is wire-shaped, not CLI-shaped: an anthropic
# stream always carries usage, but an openai-wire stream carries it only
# when the client asks for it (stream_options.include_usage — most CLIs
# do not), so a streamed openai-wire round can ledger zero tokens while
# the round itself ran fine. Off by default, and never silently on: with metrics enabled
# every round's prompt transits the localhost relay process — that is
# acceptable for a self-hosted relay you own, and it is your call to make.
# What enabling does: "daedalus worker start" and "restart" then require
# the metrics daemon (the worker probes the relay listener and fails fast
# with the "daedalus metrics start" remedy when it is down), and the
# worker stages the relay's URL into each round's provider environment,
# so rounds that would dial the provider dial the relay instead and their
# usage lands in the ledger. A CLI dialing its provider directly (no
# staged URL override) bypasses the relay invisibly — coverage is
# per-CLI-wiring, not per-host. Fallback rounds (failover) and reviewer
# rounds pinned to a reviewer endpoint skip the staging and stay
# invisible to the ledger. The daemon is host-global — one process
# serving every worker on the host — while this per-project toggle gates
# only this project's staging; the daemon's own serving config is read
# from the host context at "daedalus metrics start". One lifecycle note: a
# worker restarted inside a minute of stopping fails its boot registration
# (the dead process's registration lingers for the TTL, and a live name is
# only ever replaced by its own secret) — wait the minute out and restart,
# or clear it immediately with "daedalus metrics restart".
metrics:
  enabled: false
  # The dashboard's port, bound on the LAN (no auth — any LAN peer can
  # read run data and start runs; that is a decided, accepted posture for
  # a localhost operator tool serving your own network). 0 (unset) = 7777.
  port: 7777
  # The relay's port, bound to 127.0.0.1 only — physically unreachable
  # off-host. Must differ from port (one port cannot host both
  # listeners; equality fails the load). 0 (unset) = 7778.
  relay_port: 7778
`

// examplePromptBlock is the prompt: section — the "prompt" profile's slice.
const examplePromptBlock = `
# Project-wise prompt overrides. Empty (the default): every round's prompt
# renders byte-identically to the embedded templates in
# internal/template/prompts. When set, this names a directory of
# replacement prompts: one <prompt-name>.md file per replaced prompt, the
# file stem naming the prompt ("daedalus init prompt <dir>" scaffolds a
# ready-to-edit copy of such a directory — every prompt's embedded source
# plus a README guide). The stems are: implement, implement_fix, continue,
# tests,
# tests_failed, tests_review, review, rebuild, investigate,
# investigate_fix, refactor, refactor_fix, bugfix, bugfix_fix, slim_plan,
# slim_step, slim_parse, slim_parse_reask, or slim_fix (daedalus template
# names, not flow names — there is no dev-session). The path may be
# absolute, ~/…, or relative to this config file's directory, so a
# deployment's replacements travel with its config. Every .md file
# directly inside must name a prompt — a stray stem (e.g. a hoped-for
# dev-session.md) fails the worker's start rather than being ignored. The
# worker resolves and validates the whole directory at startup: a missing
# directory, an unreadable file, a template that does not parse, an empty
# file, a data field the prompt does not take (each template renders a
# fixed struct — copy the field references from the embedded file), a
# {{template}} action (an override runs alone), a {{define}}/{{block}}
# block (a define or block body can never render in an override — the
# sole exception is a define named exactly <prompt-name>.md; don't rely
# on it), or a review override missing the verdict protocol all fail the
# start, never a mid-round render. Every override is also render-tested at
# startup against the prompt's representative data (its zero value; review
# renders further times with a diff handoff attached and in the
# test-review framing, exercising its guarded branches), so a reference
# that parses but only fails at render —
# a nested access, or one whose {{if}} guard an edit stripped — fails the
# start too. Two machine contracts a replacement
# must keep: review
# is validated at startup to keep all four verdict words (APPROVED,
# CHANGES_REQUESTED, NEEDS_MAINTAINER, REBUILD — the reviewer's final line
# protocol the loop parses) in its rendered text — checked in the
# test-review framing, the one shape where the fourth verdict renders —
# as whole words —
# DISAPPROVED cannot vouch for APPROVED — and slim_parse must keep
# instructing the
# parse round to emit the raw SlimSubtask JSON array the plan parser reads
# (unvalidated — check it by hand); slim_plan's replacement carries no
# such contract — it is pure generation, a prose plan with no JSON, which
# the slim_parse round transcribes. Overrides resolve once per worker
# process at startup; rendered prompts are recorded in workflow history,
# so replacement content is not secret and may live beside this config.
# prompt: prompts/
`

// ExampleYAML is the fully-commented example configuration, every field at
// its default value. Bare `daedalus init` writes it as config-example.yaml
// in the working directory. It is kept in lockstep with the repository's own
// config-example.yaml by TestExampleYAMLMatchesRepoFile, and TestExampleYAML
// pins that loading it yields exactly the default configuration.
const ExampleYAML = exampleBase1 + exampleSlimBlock + exampleMidBlock +
	exampleBase2 + exampleSlimProviders + exampleProviderBlock + exampleBase3 + examplePromptBlock

// ExampleYAMLFor renders the example for a profiled `daedalus init`: the
// base pieces always (including the mid: toggle — mid has no profile of
// its own), the slim slices (the slim: section, the openai: section, and
// anthropic's context_tokens/max_output_tokens) and the prompt slice only
// when requested. Both requested reproduces ExampleYAML exactly.
func ExampleYAMLFor(slim, prompt bool) string {
	var b strings.Builder
	b.WriteString(exampleBase1)
	if slim {
		b.WriteString(exampleSlimBlock)
	}
	b.WriteString(exampleMidBlock)
	b.WriteString(exampleBase2)
	if slim {
		b.WriteString(exampleSlimProviders)
	}
	b.WriteString(exampleProviderBlock)
	b.WriteString(exampleBase3)
	if prompt {
		b.WriteString(examplePromptBlock)
	}
	return b.String()
}
