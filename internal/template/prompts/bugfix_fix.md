{{if .Logs}}The test suite failed:

{{.Logs}}

{{end}}{{if .Comments}}Code review feedback on the fix:

{{.Comments}}

{{end}}Address the above and bring the run back to: a repro test that fails on the pre-fix code, the minimal fix, and a green full suite. Never make a test pass by removing, skipping, obfuscating, or tweaking it: test changes follow code changes — a test changes only because the behavior it verifies legitimately changed, never to force a pass. Git writes are forbidden in this sandbox: never stage, commit, branch, or restore — edit files and leave the changes in the working tree; the pipeline commits approved work. Stay scoped: address only what the failure or the comments name; anything else already differing in the worktree — sandbox or tooling artifacts such as .ai-jail, environment files — is unrelated, leave it untouched and ignore it.
