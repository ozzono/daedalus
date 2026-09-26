{{if .Logs}}The frozen test suite failed under the refactoring:

{{.Logs}}

The tests are the contract and stay exactly as they are: find the behavior the refactoring changed and fix the code, never the test.

{{end}}{{if .Comments}}Code review feedback on the refactoring:

{{.Comments}}

{{end}}Address the above. Changes stay structure-only — behavior frozen, tests and testdata untouched; a comment that seems to require a behavior or test change is one to answer in your reply rather than act on. Git writes are forbidden in this sandbox: never stage, commit, branch, or restore — edit files and leave the changes in the working tree; the pipeline commits approved work. Stay scoped: address only what the failure or the comments name; anything else already differing in the worktree — sandbox or tooling artifacts such as .ai-jail, environment files — is unrelated, leave it untouched and ignore it.
