# Slim mode (`slim`)

Slim mode serves a deployment from small self-hosted models — small context windows, low output ceilings, the openai wire — through a flow shaped for them: the micro-stepped atomic loop. The target agent is `pi`; one jailed-agent round runs at a time (no two provider requests in flight). It is config-gated under the `slim:` section, and with `slim.enabled: true` a `daedalus run` without an explicit `-w` starts the slim flow (an explicit `-w` always wins). Slim and `mid` are mutually exclusive — one model tier per deployment; setting both fails the config load naming both keys. Both also require `verdict_repair.url` and `verdict_repair.model` at load, since a small model is the marker-less-verdict generator; the shared repair path is documented in [MID.md](MID.md).

## Configuration

```yaml
slim:
  enabled: true
  parser_model: qwen2.5-coder:3b   # optional; enables the tool-call relay
  max_input_tokens: 4098           # optional; 0/unset = DefaultSlimMaxInputTokens, negative fails the load
```

- **Flow gating:** `slim.enabled: true` reroutes a defaulted `-w` to the slim flow; `max_concurrent_agent_runs` is clamped to 1 under slim (a cold repo cannot be explored concurrently by a small window).
- **Aider under slim:** aider's weak/editor models are pinned via `AIDER_MODEL` under `DAEDALUS_SLIM`.
- **Sampler knob:** the deployment's `temperature` (and the other sampler fields) stage into pi rounds; the planner prompt is written for `temperature: 0.2`.
- **Breaking reshape (2026-10-02):** the former top-level `slim: true` boolean moved into this section — migrate by renaming it `slim.enabled`; the historical boolean still loads (it decodes into `enabled`), so an un-migrated config keeps working.

## The slim flow (`internal/workflows/slim.go`)

A planner round writes the plan **in prose** (no JSON, no code fences — pure generation), and a separate parse round transcribes it into the ordered queue of `SlimSubtask` entries — 1–2-file sub-tasks. A malformed parse gets one deterministic trailing-comma repair first (`stripTrailingCommas` — string-aware, escape-correct, only commas directly before `}`/`]` outside string literals; a still-broken reply keeps its original error) and then exactly one strict re-ask (`slim_parse_reask`, quoted wording pinned by tests); a second failure parks the run naming the failing stage. Each sub-task then runs its own implement ↔ review loop: the worker conversation chains across the run (progressive context) while every review round is a completely fresh reviewer session (no WORKER context leaks into REVIEW), the native suite runs every round as terminal ground truth (a red suite blocks approval), and a sub-task parks after `maxSlimStepRounds` (8) non-converging rounds. A run's flow is fixed at start; `continue` resumes it under the same flow, keeping slim's per-round command and no-suite park on resume.

## Per-role provider overrides

Every slim round resolves to one of three roles, and each role can carry its own provider values — model, endpoint url, key, context window, and wire-level thinking toggle — independently of the serving `anthropic`/`openai` (or `provider`) sections. The resolution is by round, not by flow position: the plan, parse, step, and fix rounds (the implementing agent's rounds, session role `dev`) resolve as **dev**; every sub-task's review loop resolves as **reviewer**; and the last sub-task's review loop resolves as **final-reviewer** — that loop is the run's terminal review, and it is the only review whose worktree diff carries every earlier sub-task's accepted work, i.e. the cumulative diff of the whole plan (the motivating case: a larger window or a different brain for exactly that review, where shared-window runs get context-tight).

Each group is a top-level config section — `dev:`, `reviewer:` (the pre-existing reviewer endpoint group, generalized), `final_reviewer:` — with the same five optional fields: `url`, `key`, `model`, `context_tokens`, `disable_thinking`. Any subset is legal (model without url is the same-endpoint-different-brain split; a window alone resizes just that role's budget arithmetic). The fallback chain is per field, resolved per round: a `final_reviewer` field unset takes the `reviewer` field's value, and an unset `reviewer` field takes the serving value; a `dev` or `reviewer` field unset takes the serving value directly. "Serving value" means what the round stages today — for url/key/model the serving endpoint's, for the window the serving `context_tokens`, and for thinking the openai section's `disable_thinking` (dev rounds' pre-role behavior); the reviewer and final-reviewer thinking knobs key on their own chain alone and send no signal when it is quiet, each review role controlling its own brain. An all-unset set of groups stages every slim round byte-identically to the single-provider behavior — unset means unchanged, with no field introducing a default. Negative `context_tokens` fails the config load; nothing else is constrained.

The values travel the env-only channel — `DAEDALUS_DEV_BASE_URL` / `DAEDALUS_DEV_API_KEY` / `DAEDALUS_DEV_MODEL` / `DAEDALUS_DEV_CONTEXT_TOKENS` / `DAEDALUS_DEV_THINKING`, `DAEDALUS_REVIEWER_CONTEXT_TOKENS`, and the `DAEDALUS_FINAL_REVIEWER_*` five — exported only when set (the absence is load-bearing), unset symmetrically, and scrubbed from the daemon spawn like every other provider-derived value. The jailed-round builder applies the resolved override onto the round env before any staging reads it (both wire families' endpoint/key/model vars plus `CLAUDE_CODE_MAX_CONTEXT_TOKENS`, the var pi's staged `ContextWindow`, aider's staged metadata, opencode's `limit.context`, and claude itself read), so a role's window changes only that role's clamp arithmetic. An endpoint or key override removes the relay vars (the slim tool relay and the metrics relay stage only the primary's upstream — a model-only or window-only override keeps them). The dev group keys on the implementing agent's rounds of any flow the worker serves (they share the one builder); slim is the deployment shape it is documented for.

```yaml
dev:
  model: qwen2.5-coder:7b
final_reviewer:
  model: qwen3:32b
  context_tokens: 131072
  disable_thinking: true
```

With this snippet the plan/parse/step/fix rounds request `qwen2.5-coder:7b` from the serving endpoint and keep the serving window; every sub-task review stages the reviewer chain unchanged (here: the serving values); the last sub-task's review loop requests `qwen3:32b` non-thinking with a 131072-token window, from the serving endpoint (no `final_reviewer.url`, so the reviewer/serving endpoint stands). Size role windows by the same pi clamp rule as the serving `context_tokens` — `contextWindow − prompt − 4096`, floored at 1 — with the terminal review's cumulative diff in mind.

## The input diet (`internal/shrink`)

Slim prompts pass through a pure three-pass composition — collapse structural noise, minify prose (backtick-split code spans, path/flag protection), trim extracts (keep-whitelist veto) — accepted only if the o200k_base token count strictly decreases **and** every reply-contract line survives (the `contractRe` markers: "reply with", "raw json", "no prose", and kin); anything else ships byte-identical. `shrink.Task` is deterministic (same input always re-shrinks to itself), which keeps prompts byte-stable across a chain's rounds. The gate (`slimInputGate`, `cmd/daedalus/pipeline.go`) is estimate-first: a raw `shrink.Estimate` over the `slim.max_input_tokens` ceiling refuses before any work — o200k `bytePairMerge` is O(n²) on one unbroken word (measured 4m01s on 512KB; the estimate-first guard bounds the worst admitted case to hundreds of milliseconds). It fires on both the run path and the continue path. The tiktoken BPE (`o200k_base.tiktoken`) is go:embedded and hash-checked against OpenAI's `expected_hash`, so no runtime download ever happens.

## The tool-call relay (`internal/toolrelay`)

Small coder models often emit tool calls as text — fenced JSON in the message content — which pi only executes in its native `tool_calls` form, so slim rounds would otherwise complete with zero file changes. When `slim.enabled`, `parser_model`, and the openai section's `url` are all set, the worker boots a loopback reverse proxy (stdlib only) that buffers each `POST …/chat/completions`, reassembles the answer across SSE delta fragments, and — when it is exactly one JSON object naming a tool from the request's own `tools` list — asks the parser model (ollama structured output, `format` = the tool's own parameters schema, 60s bound) to normalize the arguments into a synthetic single-delta `tool_calls` stream. Everything else — prose, multi-block, unknown tool, parser failure, any non-200 — replays the upstream bytes verbatim (fail-open, no new workflow failure path), and every inspected response gets one structured worker-log line (lifted/passthrough + reason, tool name only). The relay's URL is staged into pi rounds via the env-only `DAEDALUS_TOOL_RELAY_URL` (symmetric unset + daemon-spawn scrub). The schema conformance holds on ollama and other format-honoring parser upstreams; an upstream that silently ignores `format` leaves arguments checked only as a JSON object.

### Relay rate cap (`max_requests_per_minute`)

A positive value paces the deployment's provider traffic through the same relay: pi rounds' chat-completions requests — agent and reviewer alike — forward only up to this many per sliding one-minute window, a request beyond the cap waiting inside the relay until the window frees. The point is a provider whose API tier rate-limits by request count: staying under the cap avoids the 429 quota-heartbeat crawl instead of absorbing it. Enforcement rides the relay's staging, so it covers exactly the rounds the relay serves (a `reviewer.url` or openai-fallback override bypasses it); Load rejects the key with any other agent or without an openai url, where it would be silently inert. Zero (unset) means unthrottled.

## Operating a slim deployment

The self-hosted readiness checklist:

- **Size `context_tokens` / `max_output_tokens` honestly** on the serving provider section — `provider:` is the preferred home (its fields win over the legacy sections' per field), else the legacy `anthropic:` section, which has always carried the pair whichever wire serves: pi clamps its request to `contextWindow − prompt − 4096` (floored at 1), so a window that does not cover the bootstrap prompt plus reserve silently degrades rounds to 1-token replies that park much later as a parse error that looks like a model failure. The staged numbers are checkable before submitting — size for prompt + bootstrap + reserve + output budget.
- **Pi's request timeouts are staged, not ambient**: pi reads no timeout env var, so daedalus upserts both keys into the host's `~/.pi/agent/settings.json` (`retry.provider.timeoutMs` — the openai SDK's time-to-headers abort — and top-level `httpIdleTimeoutMs`, which feeds undici's dispatcher `headersTimeout`/`bodyTimeout`). At slow-model pace a legitimate long turn must survive both ceilings; a short ambient setting otherwise folds real rounds first.
- **Keys in the environment, never as disk literals** in config.yaml — a literal key lands in the file and every copy of it.
- **One decision model beside the chat model**: tev1-class decision models reject the OpenAI-compat chat paths and score `output_tokens: 0`, so they can never be the slim parser/serving model — `verdict_repair` points at them separately (Ollama serving tev1: `http://localhost:11434/v1`, model `tev1:latest`), on a separate server from the chat model so ollama never weight-swaps mid-round. See [MID.md](MID.md) for the repair contract.

## Prompt surface

Slim owns five embedded templates — `slim_plan` (prose plan), `slim_step` (implement a sub-task), `slim_fix` (fix round), `slim_parse` (transcribe the plan to the raw JSON array), `slim_parse_reask` (the single re-ask) — all written telegraphic per the slim diet, with wording pins in `internal/template` and `internal/workflows` tests. Project overrides follow the general `prompt` rules (see the README row): a `slim_parse` replacement must keep instructing the parse round to emit the raw `SlimSubtask` JSON array, while a `slim_plan` replacement carries no such contract — it is pure generation.

## History

| Date | Round | Outcome |
| --- | --- | --- |
| 2026-10-01 | Slim flow | The micro-stepped atomic loop: prose plan → parse → per-subtask implement↔review, config-gated reroute, `temperature` knob |
| 2026-10-02 | Tool-call relay (#16) | `internal/toolrelay` + `slim:` section reshape + `DAEDALUS_TOOL_RELAY_URL` staging |
| 2026-10-09 | Parse repair | Deterministic trailing-comma repair before the single AI re-ask; bug-policy prompt rework |
| 2026-10-10 | Slim diet (#27) | Telegraphic templates, `internal/shrink` o200k gate, embedded tiktoken, `max_input_tokens` estimate-first gate on run + continue |
| 2026-10-13 | Verdict repair (#30) | Marker-less review verdicts route through the `/v1/systemone` decision wire; `verdict_repair` required under slim (shared with mid — see [MID.md](MID.md)) |
| 2026-10-15 | Relay rate cap + pi timeouts (#32) | `max_requests_per_minute` sliding-window pacing in the relay; pi retry/idle timeout staging |
| 2026-10-15 | Slim prompt trim | Shrink diet applied to each round's final stdin prompt, not only the submit-time input (branch pending merge) |
| 2026-10-16 | Per-role provider overrides | `dev` / `reviewer` / `final_reviewer` groups — per-round model/url/key/window/thinking staging with the final-reviewer → reviewer → serving fallback chain (branch pending merge) |
