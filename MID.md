# Mid-tier mode (`mid`)

Mid-tier mode serves a deployment from capable remote models — large context windows, either wire — on the provider channel the config already has. It is one boolean (`mid: true`) beside `slim`, and it is deliberately structural-free: no endpoint fields of its own, no wire restriction, no concurrency clamp, no flow reroute, no per-agent branches. The deployment's channel and agent wiring serve mid rounds exactly as they serve any other round, for every agent. With neither `mid` nor `slim.enabled` set, behavior is byte-for-byte the pre-mid flagship default.

## Configuration

```yaml
mid: true
```

- **Mutually exclusive with `slim.enabled`** — one model tier per deployment. Setting both fails the config load naming both keys (`internal/config` validate).
- **Requires `verdict_repair`** (`url` and `model`, the same load requirement slim has): marker-less reviewer verdicts route through the decision model's `/v1/systemone` contract instead of counting a blind marker-less strike. The section is inert outside slim/mid — a partial or absent section elsewhere fails nothing.
- The provider channel is whatever the deployment already serves: the `provider:` section or the legacy `anthropic:`/`openai:` sections, on either wire.

### Agent/wire launch gate

A launch whose agent cannot speak the config's only serving endpoint fails at launch — on the start path and the continue path alike, never as a silent mid-run degradation (`Config.AgentWireConflict`). The matrix is the repo's documented wiring: `claude` dials the anthropic wire (a custom `url`, or a `key` alone on the public base) and cannot take an openai-only config; `codex`, `opencode`, and `aider` are openai-wire only; `pi` is staged on the openai channel (it reads no `ANTHROPIC_BASE_URL`, so an anthropic custom URL never reaches it, though a key with no url does); `amp` authenticates through `AMP_API_KEY` and is exempt. An all-unset config is the ambient posture and never trips the gate.

## The review verdict path: verdict repair

Mid's one behavioral change is where marker-less reviewer verdicts go. When a reviewer round's output carries no parseable verdict marker, the review activity runs one classify-only repair pass (`repairReviewVerdict`, `internal/activities/verdictrepair.go`) before the workflow ever sees the round:

- The marker-less review travels as **data in the `state` field** of a `POST <verdict_repair.url>/systemone` request, alongside a named question map. The `verdict` question is a **choice question whose criteria are the four markers** (`verdictMarkers`: APPROVED, CHANGES_REQUESTED, NEEDS_MAINTAINER, REBUILD), 2–26 entries — the endpoint rejects anything else.
- The answer is the decision model's **pointer head's scored choice, never generated text**: a chosen criteria key plus the full probability distribution and a confidence. (`output_tokens` is 0 on scored answers.)
- The OpenAI-compat chat paths reject decision-capable models, so there is **no chat-completions fallback** — the URL must be the `/v1` base of an endpoint speaking the systemone contract. With Ollama serving tev1 that is `http://localhost:11434/v1` and model `tev1:latest`.
- **Promotion:** only a classification of APPROVED at confidence ≥ 0.9 (`repairPromotionThreshold`) promotes the round to approval, and the workflow decides approval *before* its verdictless strike accounting — so a promoted round ends the loop exactly as a parsed approval would (streaks reset, no strike, no strike-budget park discarding the promotion). Every other classification continues as changes-requested, annotated with the most-likely verdict and confidence.
- **Fail-open by contract:** an unconfigured, unreachable, timed-out (60s bound, `repairTimeout`), or garbage-answering endpoint hands back the round byte-identical to the pre-repair behavior — changes-requested, whole output as comments, counted by the workflow's verdictless strike budget exactly as before. The repair outcome never touches strike accounting; only the workflow counts strikes. There is no retry loop — the strike budget is the retry policy.

### `verdict_repair` reference

```yaml
verdict_repair:
  url: http://localhost:11434/v1   # /v1 base of a systemone-contract endpoint
  model: tev1:latest
  key: ""                          # optional (a local relay needs none)
```

The values travel the env channel to the activity (`DAEDALUS_VERDICT_REPAIR_URL` / `_MODEL` / `_KEY`), never through workflow history or argv.

## Operating a mid deployment

The remote-model readiness checklist (also in `config-example.yaml`'s mid block):

- **Keys in the environment, never as disk literals** in config.yaml — a literal key lands in the file and every copy of it.
- **Size `context_tokens` / `max_output_tokens` honestly** on the serving section: the window must cover the prompt plus bootstrap plus reserve plus the output budget. (The provider section's pi clamp note applies to any pi round.)
- **Set `timeout_ms` on the serving section** for remote latency, so a gateway's own shorter ceiling does not fold long reviewer turns first.
- **Expect no slim-only knobs**: the parser_model relay, the input-token ceiling, and the single-round clamp are slim's, not mid's. Mid deployments that do run pi over an openai channel can still use the general worker-side request cap (`max_requests_per_minute`) when the provider's API tier rate-limits by request count — it is not mid-specific.
- Shared flow hardening (the `starved` run status for quota-heartbeat crawls, and the terminal suite-freshness gate at park/approve) is mode-agnostic: it applies to mid rounds exactly as to flagship and slim.

## History

| Date | Round | Outcome |
| --- | --- | --- |
| 2026-10-12 | Structured-verdict design round | Superseded by the repair layer; task file deleted |
| 2026-10-13 | Verdict-repair decision layer | Committed bb40fac on the provider-agnostic stack (merged #29); suite green |
| 2026-10-13 | Tev1 systemone wire | Repair wire swapped to `/v1/systemone` (merged #30); the surviving wire record is `internal/activities/verdictrepair.go` |
| 2026-10-13 | mid-tier round | `mid` flag beside slim: mutual exclusion, verdict_repair requirement, launch gate, stale-worker sweep |
| 2026-10-14 | mid-slim-hardening round | `starved` status, terminal suite-freshness gate, report/tasklog surface (mode-agnostic hardening) |
