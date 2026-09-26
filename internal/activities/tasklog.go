package activities

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
)

// TaskLogDir is the daemon directory holding one append-only log per
// pipeline task: /tmp/daedalus/<workflow-id>.log. It is the daemon dir
// itself, not a subdirectory, so the worker startup's pruneOldLogs
// (cmd/daedalus) — which removes any *.log untouched for logRetention —
// prunes task logs for free alongside the worker logs.
var TaskLogDir = "/tmp/daedalus"

// TaskLogPath returns the task log file path for workflowID. The id is
// validated first: it embeds the operator-supplied issue id, so a crafted
// id must not escape the directory.
func TaskLogPath(workflowID string) (string, error) {
	if err := validatePathSegment(workflowID); err != nil {
		return "", err
	}
	return filepath.Join(TaskLogDir, workflowID+".log"), nil
}

// appendTaskLog writes one event block to the running task's log, pulling
// the task identity from the activity context. Outside a real activity
// context (unit tests invoke activities directly) it is a no-op.
// Best-effort by design: the task log is an observability aid and must
// never red a round, so a write failure is only logged.
func appendTaskLog(ctx context.Context, event, body string) {
	workflowID, runID := taskLogIdentity(ctx)
	if workflowID == "" {
		return
	}
	if err := writeTaskLog(workflowID, runID, event, body); err != nil {
		activityLogger(ctx).Warn("task log write failed", "Error", err)
	}
}

// taskLogIdentity returns the running activity's workflow id and run id,
// "" outside a real activity context (the SDK panics there by design; like
// activityLogger, the panic is recovered away).
func taskLogIdentity(ctx context.Context) (workflowID, runID string) {
	defer func() { recover() }()
	info := activity.GetInfo(ctx)
	return info.WorkflowExecution.ID, info.WorkflowExecution.RunID
}

// writeTaskLog appends one block to the task's log file: the entire block
// in a single O_APPEND open + one WriteString, so concurrent activities
// (agent rounds, test suites, parallel workflows) interleave at block
// boundaries, never mid-line.
func writeTaskLog(workflowID, runID, event, body string) error {
	path, err := TaskLogPath(workflowID)
	if err != nil {
		return err
	}
	if len(runID) > 8 {
		runID = runID[:8]
	}
	// Escape body lines that would look like a block header (agent stdout
	// routinely prints "=== " section headers): a parser must only ever
	// see writer-produced headers, or a body line could forge or flip the
	// derived state (see backlog/bugs/tasklog-brief-forged-header-lines.md).
	body = strings.TrimRight(body, "\n")
	if strings.HasPrefix(body, "=== ") {
		body = " " + body
	}
	body = strings.ReplaceAll(body, "\n=== ", "\n === ")
	block := "=== " + time.Now().UTC().Format(time.RFC3339) + " " + event +
		" (run " + runID + ") ===\n" + body + "\n"
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(block)
	return err
}
