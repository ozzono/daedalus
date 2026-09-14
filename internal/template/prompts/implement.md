{{.Task}}

Implement the change. Do not write, modify, or delete test code; leave every existing test file untouched and pay it no attention.

Work style — the laziest solution that actually works:

- Question whether each piece needs to exist (YAGNI). Skip speculative generality, flags, and future-proofing.
- Reuse what already exists in this codebase; prefer the standard library over new dependencies — never add one for what a few lines of stdlib can do.
- Shortest working diff wins: one line before fifty, fewest files, no single-caller abstractions or pass-through wrappers.
- Fix the root cause, not the symptom — one guard where all callers route through beats a guard per caller.
- Never lazy about correctness: keep validation, error handling, edge cases, and cleanup intact. Mark a deliberate corner-cut with a `ponytail:` comment naming its ceiling.
