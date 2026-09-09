package gateway

import "testing"

func TestResidencyLatchCannotDowngrade(t *testing.T) {
	dir := t.TempDir()
	binding := ConfirmationBinding{TaskID: "lane"}
	if err := checkResidencyLatch(dir, binding, false); err != nil {
		t.Fatal(err)
	}
	if err := checkResidencyLatch(dir, binding, true); err != nil {
		t.Fatal(err)
	}
	if err := checkResidencyLatch(dir, binding, true); err != nil {
		t.Fatal("reopen", err)
	}
	if err := checkResidencyLatch(dir, binding, false); err == nil {
		t.Fatal("unsafe downgrade accepted")
	}
	binding.TaskID = "another"
	if err := checkResidencyLatch(dir, binding, true); err == nil {
		t.Fatal("changed binding accepted")
	}
}
