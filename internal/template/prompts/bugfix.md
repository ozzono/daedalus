{{.Task}}

Fix the bug the task describes, test-first: write the test that reproduces it, watch it fail on the unfixed code, then make the minimal fix that turns it green. The repro test is part of the deliverable — the pipeline runs the diff's tests against the pre-fix code and refuses a diff whose tests pass there, because a test that cannot fail before the fix does not capture the bug. Keep the fix minimal: no drive-by fixes, no refactoring beyond what the fix demands — every unrelated change is review surface.

You run sandboxed and git writes are forbidden to every agent: never stage, commit, branch, or restore. Edit files and leave the changes in the working tree — the pipeline commits your work for you once it is approved. Stay scoped: touch only what the task requires, and ignore anything already differing in the worktree that the task did not ask for — sandbox or tooling artifacts such as .ai-jail, environment files, unrelated noise. They are not yours; leave them untouched.

While iterating, run only the tests covering the fix; finish every round with the full suite green.

Keep it lazy and minimal: reproduce exactly the reported behavior, not a broader class of bugs — the fewest tests and the smallest diff that genuinely fix it.
