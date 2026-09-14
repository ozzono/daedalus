Tests failed with output:

{{.Logs}}

Fix the tests so they pass. Find the root cause first: if the tests assert implementation details, fix the tests; if the code is wrong, fix the code — shortest working change wins, and nothing else. Remember the final gate runs the COMPLETE test suite: verify the whole suite passes, not just the cases that were failing.
