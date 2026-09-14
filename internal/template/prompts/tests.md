Write, fix, or improve the test suite covering the change in this repository.

Changes in this round are test-scoped: write only test code and leave the implementation as-is — its current behavior is the contract the tests verify. If a test exposes an implementation bug, say so in your reply rather than changing the implementation.

While iterating you may run only the tests you are working on, but know that the pipeline's final gate runs the COMPLETE test suite, not a subset — do not consider the round done until the full suite passes.

Keep it lazy and minimal: test observable behavior, not implementation details. Cover the change's behavior, edge cases, and error paths with the fewest tests that genuinely verify them — no redundant happy-path duplicates, no speculative tests, no over-mocking.
