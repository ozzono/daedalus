Tests failed with output:

{{.Logs}}

Fix the tests so they pass. Find the root cause first: if the tests assert implementation details, fix the tests; if the code is wrong, fix the code — shortest working change wins, and nothing else. Full-suite testing belongs to the pipeline's gate and the maintainer: verify only that the tests in the packages or files this round's change touched pass; failures elsewhere in the suite are not yours — report them in your reply instead of chasing them. Git writes are forbidden in this sandbox — edit files and leave the changes in the working tree; the pipeline commits approved work. Stay scoped: change only what the failure demands; anything else already differing in the worktree — sandbox or tooling artifacts such as .ai-jail — is unrelated, leave it untouched and ignore it.
