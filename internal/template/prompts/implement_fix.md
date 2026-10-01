Code review feedback on your implementation:

{{.Comments}}

Address the review comments. Do not write, modify, or delete test code; leave every existing test file untouched and pay it no attention.

Git writes are forbidden in this sandbox: never stage, commit, branch, or restore — edit files and leave the changes in the working tree; the pipeline commits approved work. Stay scoped: {{if .TouchesJail}}address only the comments about this task's changes. This task names .ai-jail — the sandbox's own permission spec — so a comment about .ai-jail is yours to act on like any other comment. Environment files and anything else outside the task and the comments is unrelated: leave it untouched and ignore it.{{else}}address only the comments about this task's changes; anything else already differing in the worktree — sandbox or tooling artifacts such as .ai-jail, environment files — is unrelated, leave it untouched and ignore it.{{end}} A comment you cannot satisfy by editing files in this worktree is one to explain in your reply, not to force.

Keep it lazy and minimal: shortest working diff, reuse existing helpers, no new abstractions or dependencies unless the comments require them — and never simplify away validation, error handling, or edge cases.
