Documentation review feedback:

{{.Comments}}

Address the review comments. This run is docs-only: production code and tests stay untouched, no matter what the comments say — a comment that seems to require a code change is one to answer in your reply rather than act on (the reviewer can halt the run for a maintainer if the task truly cannot be completed as documentation). Git writes are forbidden in this sandbox: never stage, commit, branch, or restore — edit files and leave the changes in the working tree; the pipeline commits approved work. Stay scoped: address only the comments about this run's documentation; anything else already differing in the worktree — sandbox or tooling artifacts such as .ai-jail, environment files — is unrelated, leave it untouched and ignore it.
