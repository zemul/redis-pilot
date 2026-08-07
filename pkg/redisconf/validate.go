// Package redisconf 定义 Redis/Kvrocks 配置覆盖项的校验规则。
//
// 放在 pkg 下由 Server 与 Agent 共用，而不是各自实现一份：保留字清单一旦分成两份，
// 两边迟早会漂移，届时 Server 放行的项会被 Agent 拒绝（用户看到莫名的失败），
// 或者更糟——Agent 放行了 Server 认为不该放行的项。
package redisconf

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// validConfigKey 限制配置项名称的字符集。Redis/Kvrocks 的配置项名一律是
// 小写字母、数字和连字符（maxmemory-policy、auto-aof-rewrite-min-size 等）。
var validConfigKey = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

// reservedConfigKeys 是平台自己管理、不允许被 config_overrides 覆盖的配置项。
//
// 为什么必须拦住：渲染出的 config_overrides 位于配置模板的最末尾
// （config_template.go 的 `{{ .ConfigOverrides }}`），而 Redis 配置文件是
// 后出现的指令覆盖先出现的。也就是说这里的任何一项都会**静默地**推翻模板里
// 平台刻意设定的值，而且看配置文件开头完全察觉不到。
//
// 逐项说明拦住它的理由（都是实际会造成数据丢失或安全降级的）：
//
//   - replicaof / slaveof：复制关系由 Sentinel 和 Server 拓扑决定。改成指向外部地址，
//     实例会清空自己的数据集去同步对方——一条指令就能抹掉整个实例。
//   - requirepass / masterauth：置空即摘掉认证；改值会让 Server、Agent、Proxy、Sentinel
//     手里的密码全部失效，实例变成谁都连不上。
//   - dir / dbfilename / appendfilename / appenddirname：持久化文件的位置决定备份和恢复
//     能不能找到数据。改掉它，备份链路会静默备份到错误的路径。
//   - port / bind / unixsocket：监听地址由平台分配，改掉直接失联。
//   - replica-announce-ip / replica-announce-port：Sentinel 靠它发现实例的真实地址，
//     改错会让故障转移找不到节点。
//   - min-replicas-to-write / min-replicas-max-lag：这是防脑裂丢写的闸门，
//     调成 0 等于把该保护关掉。
//   - rename-command：模板刻意禁用了 FLUSHDB/FLUSHALL/DEBUG，允许覆盖等于允许重新启用。
//   - daemonize / supervised / pidfile / logfile：容器化部署下改这些会让容器启动即退出。
//   - cluster-enabled：本平台按主从模型管理，开启集群模式会让整套拓扑管理失效。
var reservedConfigKeys = map[string]string{
	"replicaof":             "复制关系由 Sentinel 与 Server 拓扑管理",
	"slaveof":               "复制关系由 Sentinel 与 Server 拓扑管理",
	"requirepass":           "实例密码由 Server 统一管理",
	"masterauth":            "实例密码由 Server 统一管理",
	"dir":                   "数据目录由平台管理，改动会破坏备份与恢复",
	"dbfilename":            "持久化文件名由平台管理，改动会破坏备份与恢复",
	"appendfilename":        "持久化文件名由平台管理，改动会破坏备份与恢复",
	"appenddirname":         "持久化目录名由平台管理，改动会破坏备份与恢复",
	"port":                  "监听端口由 Server 分配",
	"bind":                  "监听地址由平台管理",
	"unixsocket":            "监听方式由平台管理",
	"replica-announce-ip":   "Sentinel 依赖该地址发现实例",
	"replica-announce-port": "Sentinel 依赖该地址发现实例",
	"min-replicas-to-write": "防脑裂丢写的保护，不允许调低",
	"min-replicas-max-lag":  "防脑裂丢写的保护，不允许调低",
	"rename-command":        "危险命令的禁用策略由平台管理",
	"daemonize":             "容器化部署下改动会导致容器启动即退出",
	"supervised":            "容器化部署下改动会导致容器启动即退出",
	"pidfile":               "容器化部署下改动会导致容器启动即退出",
	"logfile":               "日志输出由平台管理",
	"cluster-enabled":       "本平台按主从模型管理，不支持集群模式",
	"loadmodule":            "加载外部模块可执行任意代码，由平台统一管理",
	"protected-mode":        "Redis 内置安全防护由平台管理，关闭会使未认证实例暴露",
	"aclfile":               "ACL 规则文件由平台管理，指向外部文件可绕过密码管理",
}

// ValidateOverrides 校验用户提供的配置覆盖项。
//
// 最要紧的一条是值里不能有换行。configOverridesString 把每一项渲染成
// "<key> <value>\n"，值里塞一个换行就能在配置文件里另起一行写任意指令，
// 且因为 overrides 渲染在模板末尾，注入的指令会覆盖平台设定的同名项。
// 例如 value = "100\nreplicaof 203.0.113.1 6379" 会让实例去同步外部地址，
// 同步的第一步是清空本地数据集。
func ValidateOverrides(overrides map[string]string) error {
	for _, k := range SortedKeys(overrides) {
		v := overrides[k]
		if !validConfigKey.MatchString(k) {
			return fmt.Errorf("invalid config key %q: must match %s", k, validConfigKey.String())
		}
		if reason, reserved := reservedConfigKeys[k]; reserved {
			return fmt.Errorf("config key %q is managed by redis-pilot and cannot be overridden: %s", k, reason)
		}
		if err := validateValue(k, v); err != nil {
			return err
		}
	}
	return nil
}

// validateValue 拒绝会破坏配置文件结构的值。
//
// 空格是合法的（save "3600 1 300 100"、client-output-buffer-limit 都带空格），
// 所以不能简单地禁止空白字符，只拦下真正会改变文件结构的那几类。
func validateValue(k, v string) error {
	if strings.ContainsAny(v, "\r\n") {
		return fmt.Errorf("invalid value for config key %q: must not contain newline "+
			"(a newline would inject an arbitrary directive into the config file)", k)
	}
	if strings.ContainsRune(v, 0) {
		return fmt.Errorf("invalid value for config key %q: must not contain NUL byte", k)
	}
	// '#' 会把行尾变成注释，能让同一行后面的内容失效；单独出现在值开头则整项等于没写。
	if strings.ContainsRune(v, '#') {
		return fmt.Errorf("invalid value for config key %q: must not contain '#' "+
			"(it would comment out the rest of the line)", k)
	}
	if strings.TrimSpace(v) == "" {
		return fmt.Errorf("invalid value for config key %q: must not be empty", k)
	}
	return nil
}

func SortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
