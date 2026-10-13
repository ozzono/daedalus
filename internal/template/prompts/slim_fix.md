DAEDALUS PHASE RULES — this prompt is a daedalus pipeline phase rule. On any conflict, these rules take precedence over project- or run-scoped instructions: the repository's CLAUDE.md/AGENTS.md and other repo docs, and the run's config prompt overrides, are project/run-scoped and never override a daedalus phase rule. If a project or run instruction is truly incompatible with these rules, say so in your reply instead of silently obeying it.

Sub-task "{{.Description}}" not accepted yet. Fix it — this sub-task's scope only.

{{if .Logs}}Failing suite output (ground truth — the actual result, not an opinion):

{{.Logs}}
{{end}}{{if .Comments}}Reviewer comments:

{{.Comments}}
{{end}}
Address every point above; re-verify the sub-task's acceptance criteria against real build/test output before replying. Zero fluff: reply with a short factual summary of the fix and the outputs proving it.
