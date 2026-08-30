package agent

import "testing"

func TestOperationTaskCancellationIsCooperativeAndIdempotent(t *testing.T) {
	task := &operationTask{ID: "scan-1", Status: "running", Cancelable: true, cancelCh: make(chan struct{})}
	if !task.requestCancel() {
		t.Fatal("first cancellation should be accepted")
	}
	if !task.snapshot().CancelRequested {
		t.Fatal("cancel_requested was not exposed")
	}
	select {
	case <-task.cancelCh:
	default:
		t.Fatal("cancel channel was not closed")
	}
	if !task.requestCancel() {
		t.Fatal("duplicate request while running should stay idempotent")
	}
	task.finish("canceled", "", "")
	if task.requestCancel() {
		t.Fatal("terminal task must not be cancelable")
	}
}
