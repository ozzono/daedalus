Test review feedback:

{{.Comments}}

Address the review comments. Changes stay test-scoped: touch only test code; if a comment seems to require an implementation change, say so in your reply instead of making it. Git writes are forbidden in this sandbox — edit files and leave the changes in the working tree; the pipeline commits approved work. Stay scoped: address only the comments about the tests; anything else already differing in the worktree — sandbox or tooling artifacts such as .ai-jail — is unrelated, leave it untouched and ignore it. Keep it lazy and minimal: the fewest tests that genuinely verify the behavior the comments name — no redundant or speculative tests, no over-mocking. Remember the final gate runs the COMPLETE test suite, not just the tests under discussion.
