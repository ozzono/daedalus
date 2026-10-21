package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/client"
)

// The pins for the `d`-keypress detach on an attached run/continue wait:
// the key ends the wait (exit 0, the reattach hint) while the run keeps
// executing server-side, a run that finished first wins over a pending
// keypress, and the raw-mode switch is restored on every exit path.

// blockingWorkflowRun is a client.WorkflowRun whose Get blocks until its
// context is done and then reports that — a running pipeline awaiting
// outcome, as the real client's Get blocks on the wait context.
type blockingWorkflowRun struct{ id string }

func (b blockingWorkflowRun) GetID() string                  { return b.id }
func (b blockingWorkflowRun) GetRunID() string               { return "run-1" }
func (b blockingWorkflowRun) GetFirstExecutionRunID() string { return "run-1" }
func (b blockingWorkflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	<-ctx.Done()
	return ctx.Err()
}
func (b blockingWorkflowRun) GetWithOptions(ctx context.Context, valuePtr interface{}, _ client.WorkflowRunGetOptions) error {
	return b.Get(ctx, valuePtr)
}

// errReader fails every read — the vanished tty a detach watcher must not
// turn into a lost wait.
type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// stubRawStdin swaps the tty seam for f, counting how often awaitPipeline
// restored the terminal; the counter pointer lets a subtest pin the
// restore on each exit path.
func stubRawStdin(t *testing.T, f func() (func(), bool)) *int {
	t.Helper()
	restores := 0
	orig := makeRawStdin
	makeRawStdin = func() (func(), bool) {
		restore, ok := f()
		return func() {
			restores++
			if restore != nil {
				restore()
			}
		}, ok
	}
	t.Cleanup(func() { makeRawStdin = orig })
	return &restores
}

// TestWatchDetachKeys pins the keypress reader: exactly the detach key
// signals the detach and cancels the wait — mid-stream included — while
// any other byte, an end of input, or a read error ends the watch with
// the detach unsignaled (losing the key must never lose the wait), and a
// pre-signaled detach is never closed a second time.
func TestWatchDetachKeys(t *testing.T) {
	t.Run("the key cancels the wait and signals the detach", func(t *testing.T) {
		detached := make(chan struct{})
		canceled := false
		watchDetachKeys(strings.NewReader("d"), func() { canceled = true }, detached)
		select {
		case <-detached:
		default:
			t.Error("the keypress left the detach unsignaled")
		}
		if !canceled {
			t.Error("the keypress did not cancel the wait")
		}
	})

	t.Run("the key is recognized mid-stream", func(t *testing.T) {
		detached := make(chan struct{})
		canceled := false
		watchDetachKeys(strings.NewReader("xxd"), func() { canceled = true }, detached)
		select {
		case <-detached:
		default:
			t.Error("a mid-stream keypress left the detach unsignaled")
		}
		if !canceled {
			t.Error("a mid-stream keypress did not cancel the wait")
		}
	})

	t.Run("other bytes and end of input end the watch without detaching", func(t *testing.T) {
		detached := make(chan struct{})
		canceled := false
		watchDetachKeys(strings.NewReader("ab"), func() { canceled = true }, detached)
		select {
		case <-detached:
			t.Error("non-key bytes detached the wait")
		default:
		}
		if canceled {
			t.Error("non-key bytes canceled the wait")
		}
	})

	t.Run("a read error ends the watch without detaching", func(t *testing.T) {
		detached := make(chan struct{})
		canceled := false
		watchDetachKeys(errReader{err: errors.New("tty gone")}, func() { canceled = true }, detached)
		select {
		case <-detached:
			t.Error("a read error detached the wait")
		default:
		}
		if canceled {
			t.Error("a read error canceled the wait")
		}
	})

	t.Run("an already-signaled detach is not closed twice", func(t *testing.T) {
		// A double close would panic the watcher — the guard is the pin.
		detached := make(chan struct{})
		close(detached)
		watchDetachKeys(strings.NewReader("d"), func() {}, detached)
	})
}

// TestAwaitPipelineDetach pins the wait's detach outcome end to end over
// the stubbed tty seam: a keypress during the wait returns nil with the
// reattach hint and the terminal restored; a run that finished first wins
// over a pending keypress; a plain failure is never labeled detached; and
// without a terminal the wait simply runs without a detach key.
func TestAwaitPipelineDetach(t *testing.T) {
	t.Run("a keypress during the wait ends it with the detach outcome", func(t *testing.T) {
		restores := stubRawStdin(t, func() (func(), bool) { return func() {}, true })
		swapStdin(t, "d")

		out := captureStdout(t, func() {
			if err := awaitPipeline(blockingWorkflowRun{id: "daedalus-issue-42"}); err != nil {
				t.Errorf("awaitPipeline(detached) = %v, want nil (exit 0)", err)
			}
		})
		if !strings.Contains(out, "Detached") ||
			!strings.Contains(out, "reattach with: daedalus attach daedalus-issue-42") {
			t.Errorf("detach output %q should carry the outcome and the reattach hint", out)
		}
		if *restores != 1 {
			t.Errorf("raw mode restored %d times after the detach, want exactly once", *restores)
		}
	})

	t.Run("a run that finished first wins over a pending keypress", func(t *testing.T) {
		restores := stubRawStdin(t, func() (func(), bool) { return func() {}, true })
		swapStdin(t, "d") // sits in the buffer; the outcome is already here

		out := captureStdout(t, func() {
			if err := awaitPipeline(stubWorkflowRun{id: "daedalus-issue-42", getResp: "daedalus/issue-42-1"}); err != nil {
				t.Errorf("awaitPipeline(finished) = %v, want nil", err)
			}
		})
		if !strings.Contains(out, "completed successfully") {
			t.Errorf("finished-run output %q should report the outcome", out)
		}
		if strings.Contains(out, "Detached") {
			t.Errorf("finished-run output %q must not claim a detach", out)
		}
		if *restores != 1 {
			t.Errorf("raw mode restored %d times after the success, want exactly once", *restores)
		}
	})

	t.Run("a plain failure is never labeled detached", func(t *testing.T) {
		restores := stubRawStdin(t, func() (func(), bool) { return func() {}, true })
		swapStdin(t, "") // EOF: no keypress ever lands

		out := captureStdout(t, func() {
			err := awaitPipeline(stubWorkflowRun{id: "daedalus-issue-42", getErr: errors.New("activity timed out")})
			if err == nil || !strings.Contains(err.Error(), "workflow execution") {
				t.Errorf("awaitPipeline(failed) = %v, want a wrapped execution error", err)
			}
		})
		if strings.Contains(out, "Detached") {
			t.Errorf("failed-run output %q must not claim a detach", out)
		}
		if *restores != 1 {
			t.Errorf("raw mode restored %d times after the failure, want exactly once", *restores)
		}
	})

	t.Run("no terminal: the wait runs without a detach key", func(t *testing.T) {
		restores := stubRawStdin(t, func() (func(), bool) { return nil, false })
		swapStdin(t, "d") // unread: no watcher is armed

		out := captureStdout(t, func() {
			if err := awaitPipeline(stubWorkflowRun{id: "daedalus-issue-42", getResp: "daedalus/issue-42-1"}); err != nil {
				t.Errorf("awaitPipeline(no tty) = %v, want nil", err)
			}
		})
		if !strings.Contains(out, "completed successfully") {
			t.Errorf("no-tty output %q should report the outcome", out)
		}
		if *restores != 0 {
			t.Errorf("a tty that never went raw was 'restored' %d times", *restores)
		}
	})
}
