Sub-task "{{.Description}}" not accepted yet. Fix it — this sub-task's scope only.

{{if .Logs}}Failing suite output (ground truth — the actual result, not an opinion):

{{.Logs}}
{{end}}{{if .Comments}}Reviewer comments:

{{.Comments}}
{{end}}
Address every point above; re-verify the sub-task's acceptance criteria against real build/test output before replying. Zero fluff: reply with a short factual summary of the fix and the outputs proving it.
