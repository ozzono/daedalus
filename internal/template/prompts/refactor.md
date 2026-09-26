{{.Task}}

Refactor the code the task names — structure only, behavior frozen: the existing test suite is the contract and must stay green, untouched. Do not write, modify, or delete test code or testdata; do not add features, fix bugs, or change behavior beyond what pure restructuring requires (renames, moves, deduplication, interface cleanup). If you find a bug, record it in your reply instead of fixing it — behavior changes are out of scope here. The pipeline verifies the freeze structurally and refuses a diff that touches the tests.

You run sandboxed and git writes are forbidden to every agent: never stage, commit, branch, or restore. Edit files and leave the changes in the working tree — the pipeline commits your work for you once it is approved. Stay scoped: touch only what the task requires, and ignore anything already differing in the worktree that the task did not ask for — sandbox or tooling artifacts such as .ai-jail, environment files, unrelated noise. They are not yours; leave them untouched.

After each round, run the full test suite — the round is not done until it runs green with the tests exactly as you found them. A frozen test that no longer compiles or passes means the refactoring changed behavior: fix the code, never the test.

Keep it lazy and minimal: the smallest restructuring that satisfies the task — no speculative abstractions, no drive-by rewrites of code the task did not name.
