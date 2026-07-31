package redisconf

import "testing"

// TestValidateOverrides_RejectsDangerousReservedKeys 验证三个高危配置项被保留字清单拦截。
//
// loadmodule 可加载任意 .so 执行代码；protected-mode no 关闭内置安全防护；
// aclfile 指向外部 ACL 文件可绕过平台密码管理。三者都必须被拒绝。
func TestValidateOverrides_RejectsDangerousReservedKeys(t *testing.T) {
	cases := []struct {
		key   string
		value string
		why   string
	}{
		{"loadmodule", "/tmp/evil.so", "可加载外部模块执行任意代码"},
		{"protected-mode", "no", "关闭 Redis 内置安全防护"},
		{"aclfile", "/tmp/evil.acl", "指向外部 ACL 文件绕过密码管理"},
	}
	for _, c := range cases {
		err := ValidateOverrides(map[string]string{c.key: c.value})
		if err == nil {
			t.Errorf("ValidateOverrides should reject %q (%s)", c.key, c.why)
		}
	}
}

func TestValidateOverrides_AcceptsLegitimateKeys(t *testing.T) {
	err := ValidateOverrides(map[string]string{
		"maxmemory-policy": "allkeys-lru",
		"hz":               "100",
		"timeout":          "300",
	})
	if err != nil {
		t.Fatalf("legitimate overrides should pass: %v", err)
	}
}
