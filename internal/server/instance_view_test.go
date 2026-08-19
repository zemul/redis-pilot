package server

import (
	"encoding/json"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// TestInstanceView_NoPasswordField 确认 InstanceView（单实例对外视图）不含 password 字段。
// Instance 结构体已无 Password 字段，ToView() 产出的 JSON 也不应有任何密码相关内容。
func TestInstanceView_NoPasswordField(t *testing.T) {
	inst := &apitypes.Instance{
		Group:        "order",
		Role:         "master",
		Server:       "srv-a",
		Port:         6379,
		DesiredState: "running", ObservedStatus: "running",
		Container: "redis-order-master",
	}

	data, err := json.Marshal(inst.ToView())
	if err != nil {
		t.Fatal(err)
	}
	body := string(data)

	if strings.Contains(strings.ToLower(body), "password") {
		t.Errorf("InstanceView should not contain password field: %s", body)
	}

	// 正常字段仍应保留
	for _, want := range []string{"order", "master", "srv-a", "6379", "running"} {
		if !strings.Contains(body, want) {
			t.Errorf("InstanceView missing expected field %q: %s", want, body)
		}
	}
}

// TestInstancesStateView_GroupPasswordVisible 确认管理接口能看到 group 级别密码。
// 密码已提升到 InstanceGroupState，管理员 API（需 Token 鉴权）展示完整状态是合理的。
func TestInstancesStateView_GroupPasswordVisible(t *testing.T) {
	state := &apitypes.InstancesState{
		Groups: map[string]*apitypes.InstanceGroupState{
			"order": {Type: "replication", CurrentMaster: "order-master", Password: "front-pass"},
		},
		Instances: map[string]*apitypes.Instance{
			"order-master": {Group: "order", Role: "master", Port: 6379},
		},
	}

	data, err := json.Marshal(state.ToView())
	if err != nil {
		t.Fatal(err)
	}
	// group password 对管理员可见
	if !strings.Contains(string(data), "front-pass") {
		t.Errorf("group password should be visible in admin view: %s", data)
	}
}
