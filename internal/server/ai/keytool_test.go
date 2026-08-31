package ai

import (
	"context"
	"strings"
	"testing"
)

// write_key / delete_key 必须是写工具（走 confirm，不在循环内直接执行）。
func TestKeyTools_AreWriteTools(t *testing.T) {
	for _, name := range []string{"write_key", "delete_key"} {
		if !IsWriteTool(name) {
			t.Errorf("%s should be a write tool", name)
		}
	}
	// describe_key / get_key 是只读工具，不得进写白名单。
	for _, name := range []string{"describe_key", "get_key"} {
		if IsWriteTool(name) {
			t.Errorf("%s must not be a write tool", name)
		}
	}
}

// 读工具仅在 admin 时暴露；写 key 工具仅在 allowWrite 时暴露。
func TestKeyTools_Exposure(t *testing.T) {
	has := func(r *toolRegistry, name string) bool {
		for _, tool := range r.schemas() {
			if tool.Function.Name == name {
				return true
			}
		}
		return false
	}

	admin := &toolRegistry{admin: true}
	for _, name := range []string{"describe_key", "get_key"} {
		if !has(admin, name) {
			t.Errorf("%s should be exposed to admin", name)
		}
	}

	nonAdmin := &toolRegistry{admin: false}
	for _, name := range []string{"describe_key", "get_key"} {
		if has(nonAdmin, name) {
			t.Errorf("%s must NOT be exposed to non-admin", name)
		}
	}

	// write_key/delete_key 受 allowWrite 门控（allowWrite 蕴含 admin）。
	writer := &toolRegistry{allowWrite: true, admin: true}
	for _, name := range []string{"write_key", "delete_key"} {
		if !has(writer, name) {
			t.Errorf("%s should be exposed when allowWrite", name)
		}
	}
	roAdmin := &toolRegistry{allowWrite: false, admin: true}
	for _, name := range []string{"write_key", "delete_key"} {
		if has(roAdmin, name) {
			t.Errorf("%s must NOT be exposed when allowWrite is false", name)
		}
	}
}

// 读 key 工具在非 admin 下即便被调用也应被 exec 拒绝（防御纵深）。
func TestKeyReadTools_ExecRequiresAdmin(t *testing.T) {
	r := &toolRegistry{admin: false}
	for _, name := range []string{"describe_key", "get_key"} {
		if _, err := r.exec(context.Background(), name, `{"name":"c1","key":"k"}`); err == nil {
			t.Errorf("%s exec must require admin", name)
		}
	}
}

// 写 key 工具绝不在对话循环内执行。
func TestKeyWriteTools_NotExecInLoop(t *testing.T) {
	r := &toolRegistry{allowWrite: true, admin: true}
	for _, name := range []string{"write_key", "delete_key"} {
		if _, err := r.exec(context.Background(), name, `{"name":"c1","key":"k","op":"set"}`); err == nil {
			t.Errorf("%s must not be executable in-loop", name)
		}
	}
}

// 后端可信摘要应按 op 反映真实动作，并区分整 key 删除与元素删除。
func TestKeyTools_Summary(t *testing.T) {
	cases := []struct {
		name string
		args string
		want []string
	}{
		{"write_key", `{"name":"c1","key":"k","op":"set"}`, []string{"c1", "SET", "k"}},
		{"write_key", `{"name":"c1","key":"h","op":"hdel","field":"f"}`, []string{"HDEL", "f"}},
		{"write_key", `{"name":"c1","key":"z","op":"zadd","member":"m","score":"1.5"}`, []string{"ZADD", "1.5"}},
		{"write_key", `{"name":"c1","key":"l","op":"lset","index":2}`, []string{"LSET", "2"}},
		{"write_key", `{"name":"c1","key":"k","op":"expire","ttl_seconds":-1}`, []string{"PERSIST"}},
		{"delete_key", `{"name":"c1","key":"k"}`, []string{"DEL", "不可恢复"}},
	}
	for _, tc := range cases {
		got := writeToolSummary(tc.name, tc.args)
		for _, w := range tc.want {
			if !strings.Contains(got, w) {
				t.Errorf("%s(%s) summary = %q, want contains %q", tc.name, tc.args, got, w)
			}
		}
	}
}
