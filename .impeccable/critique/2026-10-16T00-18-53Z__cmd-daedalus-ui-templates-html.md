---
target: the htmx dashboard (cmd/daedalus/ui_templates.html)
total_score: 29
max_score: 40
na_heuristics: 
p0_count: 0
p1_count: 4
target_identity: "file:/home/hugo/Projects/daedalus/cmd/daedalus/ui_templates.html"
target_fingerprint: "sha256:2346f1128400295a58391452e3d4ba6d69de4c7e24a42dd802873f2ca536daf4"
target_path: /home/hugo/Projects/daedalus/cmd/daedalus/ui_templates.html
timestamp: 2026-10-16T00-18-53Z
slug: cmd-daedalus-ui-templates-html
---
# Critique snapshot — cmd/daedalus/ui_templates.html (htmx dashboard)

Method: dual-agent (A: design review · B: detector + technical audit). Browser automation unavailable; findings code-verified.

## Scores

Nielsen heuristics: 29/40 (Good). 1:4, 2:3, 3:2, 4:3, 5:3, 6:3, 7:2, 8:3, 9:3, 10:3. No n/a heuristics; applicable max 40. Cognitive load: moderate, leaning high on run detail (4 of 8 checklist failures).

Audit Health Score: 12/20 (Acceptable). Accessibility 2, Performance 2, Responsive 2, Theming 3, Implementation Integrity 3.

Detector: `impeccable detect --json` exit 0, zero findings; all material issues are contextual (beyond pattern rules). No false positives.

## Design specificity verdict

Authored for daedalus — generic skeleton, specific flesh. Default Tailwind admin bones; product-specific verdict badges (incl. "NO VERDICT → likely CHANGES_REQUESTED"), opinionated copy, honest empty states naming CLI remedies. Structural sameness: the rounds rail renders the product's signature loop (implement↔review, REBUILD edge) as an undifferentiated event stream; phase-grouping it is the biggest missed "daedalus" move.

## Priority issues

- P1 Zero label/control associations (systemic; ui_templates.html:163-172, 350-373, 391). WCAG 1.3.1/3.3.2/4.1.2. Fix: ids + `for`, ~15 lines.
- P1 File Path mode radios `class="hidden"` (:366-367) = display:none = unreachable by keyboard. WCAG 2.1.1. Fix: `sr-only` + `has-[:focus-visible]:ring-2` on label.
- P1 Light-mode contrast fails AA: text-emerald-600 links 3.78:1 (:193, :144, :403); text-emerald-500 countdown 2.43:1 (:51). WCAG 1.4.3. Fix: `text-emerald-700 dark:text-emerald-400`.
- P1 5s detail swap destroys reading state (:208 + ui.go:652-653): byte-identical innerHTML swap still replaces nodes — kills text selection and scroll in test excerpt/review body. Fix: trigger gate `[window.getSelection().isCollapsed]` or scope poll to volatile region.
- P2 cluster: (a) countdown permanently reads 0/"stalled" on non-polled pages /metrics and /new; (b) runs table min-w-[820px] no overflow-x-auto wrapper (:175-184; pattern exists :250); (c) no document.hidden gating on three 5s pollers (each /workers/rows = full Temporal walk); (d) Tailwind CDN JIT ~300KB in production (:10-11); (e) no double-submit guard on New Task (:348/:377 + runstart.go:315 ALLOW_DUPLICATE → silent second run); (f) filter selects apply on next 5s tick, no URL state; (g) parked/failed runs show badge but no cause or remedy though the detail pass already walks the history holding both.

## Personas

Alex (power user): 5s filter latency feels broken; copying a test line races the swap ticker; no copy-ID, no URL state, no manual refresh. Sam (a11y): cannot switch input modes by keyboard; ~10 unlabeled fields; tooltip-only countdown explanation; weak default focus outlines. Color-only meaning: handled (badges carry text, probes carry glyphs).

## Strengths

1. Honest liveness instrumentation (countdown clamp stall signal, last-polled clock, probe badges, documented rationale).
2. XSS posture solid: html/template everywhere, zero template.HTML, url.PathEscape on Temporal href.
3. One-snapshot architecture: Workers/Runs/Metrics from a single walk; submissions through the same runstart.Start seam as the CLI (dirty-repo refusal, IssueIDGate); probe caching (uiProbeTTL) keeps the poll from becoming a billed request stream.

## Minor observations

No transport-error surface beyond the countdown; Flow fallback renders guessed "feature-dev" contradicting ui.go:918 "never a guess" (render "-" instead); theme button lacks aria-pressed; no color-scheme meta; scrollbar thumb hard-coded in both themes; body colors raw hex duplicating slate tokens; copy drift (New Task / ＋ New task / Submit Another Task); th without scope.

## Provocative questions

1. If a run parks at 3 a.m., does the dashboard say what happens next — or turn red and wait for you to already know `daedalus continue` exists?
2. Should the polling contract be "don't swap while the operator is reading" rather than "render identically"?
3. Would phase-grouping the rounds rail make it the one view no other tool could render — worth more than the charts the metrics page is missing?

## Browser-dependent (unverified)

Rendered contrast under translucent header; real background-tab throttling; CDN JIT first-paint cost; SR behavior of swapped tbody and sr-only radios; selection/scroll jank with real 64KB excerpt.
