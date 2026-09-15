Code review feedback on your implementation:

{{.Comments}}

Address the review comments. Do not write, modify, or delete test code; leave every existing test file untouched and pay it no attention.

Git writes are forbidden in this sandbox: never stage, commit, branch, or restore — edit files and leave the changes in the working tree; the pipeline commits approved work. Stay scoped: address only the comments about this task's changes; anything else already differing in the worktree — sandbox or tooling artifacts such as .ai-jail, environment files — is unrelated, leave it untouched and ignore it. A comment you cannot satisfy by editing files in this worktree is one to explain in your reply, not to force.

Keep it lazy and minimal: shortest working diff, reuse existing helpers, no new abstractions or dependencies unless the comments require them — and never simplify away validation, error handling, or edge cases.
