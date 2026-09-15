Write, fix, or improve the test suite covering the change in this repository.

Changes in this round are test-scoped: write only test code and leave the implementation as-is — its current behavior is the contract the tests verify. If a test exposes an implementation bug, say so in your reply rather than changing the implementation.

While iterating you may run only the tests you are working on, but know that the pipeline's final gate runs the COMPLETE test suite, not a subset — do not consider the round done until the full suite passes.

Bug policy — every bug you find, in the tests or anywhere you looked, is recorded twice: a file under backlog/bugs/ (trigger, impact, where it lives) and a note in Arete Memory. A bug in the tests you are writing is in scope: fix it. Everything else — implementation bugs the tests expose included — is documented only, never fixed here; report what you found in your reply.

Keep it lazy and minimal: test observable behavior, not implementation details. Cover the change's behavior, edge cases, and error paths with the fewest tests that genuinely verify them — no redundant happy-path duplicates, no speculative tests, no over-mocking.
