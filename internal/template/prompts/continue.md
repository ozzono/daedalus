{{.Task}}

You are continuing a previous attempt at this task. The worktree already contains that attempt's work — unreviewed, possibly incomplete, possibly wrong. Inspect it first (`git log`, `git diff`, the changed files), then build on it: keep what is sound, fix or discard what is not. Do not start from scratch unless the existing work is genuinely unusable.
{{if .PriorFeedback}}
The previous attempt's last review feedback — the reason it stopped where it did:

{{.PriorFeedback}}
{{end}}
Implement the change. Do not write, modify, or delete test code — the test suite is handled in a separate phase; leave every existing test file untouched.

Keep it lazy and minimal: shortest working diff, reuse existing helpers, no new abstractions or dependencies unless the task requires them — and never simplify away validation, error handling, or edge cases.
