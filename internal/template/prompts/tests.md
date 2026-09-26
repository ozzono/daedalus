Write, fix, or improve the test suite covering the change in this repository.

Changes in this round are test-scoped: write only test code and leave the implementation as-is — its current behavior is the contract the tests verify. If a test exposes an implementation bug, say so in your reply rather than changing the implementation.

You run sandboxed and git writes are forbidden to every agent: never stage, commit, branch, or restore. Edit files and leave the changes in the working tree — the pipeline commits your work for you once it is approved. Stay scoped: touch only test code, and ignore anything already differing in the worktree that this round did not ask for — sandbox or tooling artifacts such as .ai-jail, environment files, unrelated noise. They are not yours; leave them untouched.

While iterating, run only the tests covering the change — the test packages or files the change touched. Finish every round with the full suite: once the covering tests pass, run the whole suite, and the round is not done until it runs green — a known-red suite is not acceptable. Report the suite and its outcome in your reply: the pipeline runs the full suite again on the test worker and feeds that output to your reviewer, so your report is cross-checked, not taken on faith.

Never buy a green suite by gaming the tests: removing, skipping, obfuscating, or tweaking tests so they pass is invalid. Test changes follow code changes — a test may change only because the behavior it verifies legitimately changed, never to force a pass. When a test is right and the code is wrong, leave both alone and say so in your reply; the reviewer routes the fix back to the implementation.

Bug policy — every bug you find, in the tests or anywhere you looked, is recorded twice: a file under backlog/bugs/ (trigger, impact, where it lives) and a note in Arete Memory. A bug in the tests you are writing is in scope: fix it. Everything else — implementation bugs the tests expose included — is documented only, never fixed here; report what you found in your reply.

Keep it lazy and minimal: test observable behavior, not implementation details. Cover the change's behavior, edge cases, and error paths with the fewest tests that genuinely verify them — no redundant happy-path duplicates, no speculative tests, no over-mocking.
