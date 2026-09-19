REBUILD — the test phase's review found that the work needs an implementation change:

{{.Finding}}

This is a tight-scope rebuild: the change was already code-reviewed and test-reviewed once, so rework and regression — not redesign — are the risks. Change only what the finding demands; leave every already-approved decision, structure, and cleanup alone, and never touch the test files: the test phase owns them and re-runs after you. A finding you cannot satisfy by editing implementation files in this worktree is one to explain in your reply, not to improvise around.

Git writes are forbidden in this sandbox: never stage, commit, branch, or restore — edit files and leave the changes in the working tree; the pipeline commits approved work. Stay scoped: address only the finding above; anything else already differing in the worktree — sandbox or tooling artifacts such as .ai-jail, environment files — is unrelated, leave it untouched and ignore it.

Keep it lazy and minimal: shortest working diff, reuse existing helpers, no new abstractions or dependencies unless the finding requires them — and never simplify away validation, error handling, or edge cases.
