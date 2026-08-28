package ai

import (
	"strings"
	"testing"
)

// resize_instance 必须被识别为写操作（走 confirm 二次确认，不在对话循环内直接执行）。
func TestResizeInstance_IsWriteTool(t *testing.T) {
	if !IsWriteTool("resize_instance") {
		t.Fatal("resize_instance should be a write tool")
	}
}

// allowWrite 开启时，schemas 应暴露 resize_instance 工具。
func TestResizeInstance_ExposedWhenAllowWrite(t *testing.T) {
	r := &toolRegistry{allowWrite: true}
	found := false
	for _, tool := range r.schemas() {
		if tool.Function.Name == "resize_instance" {
			found = true
			break
		}
	}
	if !found {
		t.Error("resize_instance should be exposed in schemas when allowWrite is true")
	}

	// allowWrite 关闭时不得暴露写工具。
	rRO := &toolRegistry{allowWrite: false}
	for _, tool := range rRO.schemas() {
		if tool.Function.Name == "resize_instance" {
			t.Error("resize_instance must NOT be exposed when allowWrite is false")
		}
	}
}

// 后端生成的可信摘要应反映实际变更维度。
func TestResizeInstance_Summary(t *testing.T) {
	got := writeToolSummary("resize_instance", `{"name":"order-1","memory":"8Gi","cpus":4}`)
	if !strings.Contains(got, "order-1") || !strings.Contains(got, "8Gi") || !strings.Contains(got, "CPU→4") {
		t.Errorf("summary should mention target and changed dims, got: %s", got)
	}
	// 未指定的维度不应出现在摘要里。
	if strings.Contains(got, "磁盘→") {
		t.Errorf("summary should not mention disk when unset, got: %s", got)
	}
}

// 在对话循环内直接执行写工具必须被拒绝（最后一道防线）。
func TestResizeInstance_NotExecInLoop(t *testing.T) {
	r := &toolRegistry{allowWrite: true}
	if _, err := r.exec(nil, "resize_instance", `{"name":"order-1","memory":"8Gi"}`); err == nil {
		t.Error("resize_instance must not be executable in-loop; expected error")
	}
}
