package ai

import (
	"context"
	"encoding/json"
	"testing"
)

// query_incidents 是只读工具，不得进写白名单。
func TestIncidentsTool_IsReadOnly(t *testing.T) {
	if IsWriteTool("query_incidents") {
		t.Fatal("query_incidents must not be a write tool")
	}
}

// query_incidents 应无条件暴露给模型（与 query_audit/query_tasks 一样，非 admin/非 write 也可见）。
func TestIncidentsTool_AlwaysExposed(t *testing.T) {
	has := func(r *toolRegistry) bool {
		for _, tool := range r.schemas() {
			if tool.Function.Name == "query_incidents" {
				return true
			}
		}
		return false
	}
	for _, r := range []*toolRegistry{
		{admin: false, allowWrite: false},
		{admin: true, allowWrite: false},
		{admin: true, allowWrite: true},
	} {
		if !has(r) {
			t.Errorf("query_incidents should always be exposed (admin=%v allowWrite=%v)", r.admin, r.allowWrite)
		}
	}
}

// exec 应把 query_incidents 分发到 PlatformAPI.QueryIncidents 并解析筛选参数。
func TestIncidentsTool_ExecDispatches(t *testing.T) {
	m := &mockPlatform{}
	r := &toolRegistry{api: m}
	out, err := r.exec(context.Background(), "query_incidents",
		`{"status":"open","severity":"critical","instance":"c1","limit":10}`)
	if err != nil {
		t.Fatalf("exec query_incidents: %v", err)
	}
	// mock 记录了一次 incidents 调用
	var found bool
	for _, c := range m.calls {
		if c == "incidents" {
			found = true
		}
	}
	if !found {
		t.Errorf("QueryIncidents was not dispatched; calls=%v", m.calls)
	}
	// 返回值是合法 JSON（records/total 结构）
	var payload struct {
		Records []any `json:"records"`
		Total   int   `json:"total"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		t.Errorf("result not valid records/total JSON: %v", err)
	}
}
