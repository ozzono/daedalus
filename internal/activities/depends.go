package activities

import (
	"context"
	"errors"
	"fmt"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// CheckDependencyActivityName is the registered name of the dependency
// probe (NewCheckDependencyActivity), pinned as a string so the workflow's
// ExecuteActivity and the worker's registration cannot drift: the activity
// is a closure over the worker's client, whose reflected name would not
// match on both sides by itself.
const CheckDependencyActivityName = "CheckDependencyActivity"

// NotifyDependentsActivityName is the registered name of the
// dependent-release poke (NewNotifyDependentsActivity), pinned for the
// same closure-name reason as CheckDependencyActivityName.
const NotifyDependentsActivityName = "NotifyDependentsActivity"

// DependencyProbe is one poll of a dependent run's dependency: what state
// the chained workflow is in, and — once approved — the preserved branch
// its work continues from.
type DependencyProbe struct {
	// Terminal reports that the dependency reached a terminal state (or
	// never existed): the chain's fate is decided, approval or break.
	Terminal bool
	// Completed marks a Terminal probe that arrived via workflow
	// completion. Result then decides approval — a preserved branch name —
	// from a park (the PARKED: marker, decoded workflow-side, which owns
	// the marker constant).
	Completed bool
	// Result is a completed workflow's recorded completion result.
	Result string
	// Status is the human-readable state name for messages ("running",
	// "failed", "not found", …).
	Status string
}

// NewCheckDependencyActivity builds the dependency probe over the worker's
// Temporal client: runWorker dials once before registering, so the probe
// reuses that connection — no second dial, no new env channel. It Describes
// the named workflow (by id verbatim, empty run id = latest execution) and
// classifies its execution status; a completed workflow's completion result
// is read so the caller can tell an approval (the preserved branch name)
// from a park.
func NewCheckDependencyActivity(c client.Client) func(context.Context, string) (DependencyProbe, error) {
	return func(ctx context.Context, workflowID string) (DependencyProbe, error) {
		resp, err := c.DescribeWorkflowExecution(ctx, workflowID, "")
		if err != nil {
			var notFound *serviceerror.NotFound
			if errors.As(err, &notFound) {
				// A vanished dependency (wiped, wrong id) is a broken
				// chain, not a probe error: fail the gate, not the probe.
				return DependencyProbe{Terminal: true, Status: "not found"}, nil
			}
			return DependencyProbe{}, fmt.Errorf("describe dependency %s: %w", workflowID, err)
		}
		switch s := resp.GetWorkflowExecutionInfo().GetStatus(); s {
		case enums.WORKFLOW_EXECUTION_STATUS_RUNNING, enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
			return DependencyProbe{Status: "running"}, nil
		case enums.WORKFLOW_EXECUTION_STATUS_PAUSED:
			// Paused is resumable (`temporal workflow unpause`), not
			// terminal — the old default case reported it Terminal, so a
			// maintainer pausing a dependency failed every run chained
			// behind it with "the chain is broken". The gate keeps polling;
			// the status names the hold so the gate can log the unpause
			// line.
			return DependencyProbe{Status: "paused"}, nil
		case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
			var result string
			if err := c.GetWorkflow(ctx, workflowID, "").Get(ctx, &result); err != nil {
				return DependencyProbe{}, fmt.Errorf("read result of dependency %s: %w", workflowID, err)
			}
			return DependencyProbe{Terminal: true, Completed: true, Result: result, Status: "completed"}, nil
		case enums.WORKFLOW_EXECUTION_STATUS_FAILED:
			return DependencyProbe{Terminal: true, Status: "failed"}, nil
		case enums.WORKFLOW_EXECUTION_STATUS_CANCELED:
			return DependencyProbe{Terminal: true, Status: "canceled"}, nil
		case enums.WORKFLOW_EXECUTION_STATUS_TERMINATED:
			return DependencyProbe{Terminal: true, Status: "terminated"}, nil
		case enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
			return DependencyProbe{Terminal: true, Status: "timed out"}, nil
		default:
			return DependencyProbe{Terminal: true, Status: s.String()}, nil
		}
	}
}

// NewNotifyDependentsActivity builds the poke that releases a completed
// run's chain without waiting out the gate's fallback poll: over the
// worker's client (the same dial the probe reuses) it lists the still-open
// executions whose DaedalusDependsOn names this workflow and signals each
// one's "wakeup" — the same channel the quota heartbeat already races —
// so the dependent's gate re-probes at once. Returned before completion on
// purpose: the poke necessarily precedes the completion event, so the
// probe it triggers may still read the dependency running; the gate rides
// a short release grace after a poke to catch the settled state. The count
// of poked dependents is best-effort throughout — a closed-between-listing-
// and-signaling dependent (the chain broke meanwhile) is skipped, not an
// error, and a failed poke leaves the fallback poll as the release path.
func NewNotifyDependentsActivity(c client.Client) func(context.Context, string) (int, error) {
	return func(ctx context.Context, workflowID string) (int, error) {
		poked := 0
		token := []byte(nil)
		for {
			resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace:     "default",
				NextPageToken: token,
				Query:         fmt.Sprintf("DaedalusDependsOn = '%s' AND ExecutionStatus = 'Running'", workflowID),
			})
			if err != nil {
				return 0, fmt.Errorf("list dependents of %s: %w", workflowID, err)
			}
			for _, e := range resp.GetExecutions() {
				if err := c.SignalWorkflow(ctx, e.GetExecution().GetWorkflowId(), e.GetExecution().GetRunId(), "wakeup", ""); err == nil {
					poked++
				}
			}
			token = resp.GetNextPageToken()
			if len(token) == 0 {
				return poked, nil
			}
		}
	}
}
