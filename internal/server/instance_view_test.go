package server

import (
	"encoding/json"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// TestInstanceView_OmitsPassword 固定「对外视图不得携带密码」这条约束。
//
// 回归背景：instanceCreate 此前直接返回 *apitypes.Instance（`ok(c, inst)`），而
// Instance 结构体带 Password 字段，导致创建接口的响应体里含 Redis 明文密码——会被
// HTTP 访问日志、反向代理、CLI 调试输出等环节捕获。现已改为 inst.ToView()。
//
// 这里同时用序列化后的 JSON 做断言，而不只是检查结构体字段，这样将来给 InstanceView
// 加回密码类字段也会被抓到。
func TestInstanceView_OmitsPassword(t *testing.T) {
	inst := &apitypes.Instance{
		Group:     "order",
		Role:      "master",
		Server:    "srv-a",
		Port:      6379,
		Password:  "super-secret-password",
		Status:    "running",
		Container: "redis-order-master",
	}

	data, err := json.Marshal(inst.ToView())
	if err != nil {
		t.Fatal(err)
	}
	body := string(data)

	if strings.Contains(body, "super-secret-password") {
		t.Errorf("对外视图泄露了明文密码：%s", body)
	}
	if strings.Contains(strings.ToLower(body), "password") {
		t.Errorf("对外视图出现了 password 字段：%s", body)
	}

	// 正常字段仍应保留，确认不是把整个结构体清空了
	for _, want := range []string{"order", "master", "srv-a", "6379", "running"} {
		if !strings.Contains(body, want) {
			t.Errorf("对外视图缺少预期字段 %q：%s", want, body)
		}
	}
}

// TestInstancesStateView_OmitsPassword 同样约束批量视图。
func TestInstancesStateView_OmitsPassword(t *testing.T) {
	state := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {Type: "replication", CurrentMaster: "order-master"},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master": {Group: "order", Role: "master", Port: 6379, Password: "leak-me-if-you-can"},
		},
	}

	data, err := json.Marshal(state.ToView())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "leak-me-if-you-can") {
		t.Errorf("批量视图泄露了明文密码：%s", data)
	}
}
