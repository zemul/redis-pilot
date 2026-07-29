// cmd/migrate/main.go
// 一次性迁移工具：将 pool-state.yaml + instances-state.yaml 导入 SQLite (pilot.db)
// 用法：
//
//	redis-pilot-migrate --data-dir /opt/redis-pilot-server/state
//
// 执行后会在 data-dir 下创建 pilot.db，原 YAML 文件重命名为 *.yaml.migrated 备份。
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func main() {
	dataDir := flag.String("data-dir", "/opt/redis-pilot-server/state", "state 目录（含 pool-state.yaml 和 instances-state.yaml）")
	dryRun := flag.Bool("dry-run", false, "只读取 YAML 打印内容，不写入数据库")
	flag.Parse()

	log.Printf("迁移目录: %s", *dataDir)

	// ── 1. 读取 pool-state.yaml ───────────────────────────────────────────
	nodeState, err := readNodeState(*dataDir)
	if err != nil {
		log.Fatalf("读取 pool-state.yaml 失败: %v", err)
	}
	log.Printf("节点数: %d", len(nodeState.Servers))
	for name, srv := range nodeState.Servers {
		log.Printf("  节点 %s  endpoint=%s  status=%s", name, srv.Endpoint, srv.Status)
	}

	// ── 2. 读取 instances-state.yaml ─────────────────────────────────────
	instState, err := readInstancesState(*dataDir)
	if err != nil {
		log.Fatalf("读取 instances-state.yaml 失败: %v", err)
	}
	log.Printf("实例组数: %d", len(instState.Groups))
	log.Printf("实例数: %d", len(instState.Instances))
	for name, inst := range instState.Instances {
		log.Printf("  实例 %s  group=%s  role=%s  desired=%s  observed=%s  server=%s",
			name, inst.Group, inst.Role, inst.DesiredState, inst.ObservedStatus, inst.Server)
	}

	if *dryRun {
		log.Println("dry-run 模式，不写入数据库，退出")
		return
	}

	// ── 3. 检查 pilot.db 是否已存在 ──────────────────────────────────────
	dbPath := filepath.Join(*dataDir, "pilot.db")
	if _, err := os.Stat(dbPath); err == nil {
		log.Fatalf("pilot.db 已存在（%s），请先手动备份或删除再执行迁移", dbPath)
	}

	// ── 4. 打开 SQLite，自动建表 ──────────────────────────────────────────
	db, err := store.Open(*dataDir)
	if err != nil {
		log.Fatalf("初始化 SQLite 失败: %v", err)
	}
	defer db.Close()

	// ── 5. 写入节点 ───────────────────────────────────────────────────────
	if err := db.WriteNodeState(nodeState); err != nil {
		log.Fatalf("写入节点失败: %v", err)
	}
	log.Printf("✓ 写入 %d 个节点", len(nodeState.Servers))

	// ── 6. 写入实例组和实例 ───────────────────────────────────────────────
	if err := db.WithInstances(func(st *apitypes.InstancesState) error {
		st.Groups = instState.Groups
		st.Instances = instState.Instances
		return nil
	}); err != nil {
		log.Fatalf("写入实例失败: %v", err)
	}
	log.Printf("✓ 写入 %d 个实例组，%d 个实例", len(instState.Groups), len(instState.Instances))

	// ── 7. 验证回读 ───────────────────────────────────────────────────────
	gotNode, err := db.ReadNodeState()
	if err != nil {
		log.Fatalf("回读节点失败: %v", err)
	}
	if len(gotNode.Servers) != len(nodeState.Servers) {
		log.Fatalf("节点数不一致: 期望 %d，实际 %d", len(nodeState.Servers), len(gotNode.Servers))
	}

	gotInst, err := db.ReadInstances()
	if err != nil {
		log.Fatalf("回读实例失败: %v", err)
	}
	if len(gotInst.Instances) != len(instState.Instances) {
		log.Fatalf("实例数不一致: 期望 %d，实际 %d", len(instState.Instances), len(gotInst.Instances))
	}
	if len(gotInst.Groups) != len(instState.Groups) {
		log.Fatalf("实例组数不一致: 期望 %d，实际 %d", len(instState.Groups), len(gotInst.Groups))
	}
	log.Println("✓ 回读验证通过")

	// ── 8. 备份原 YAML 文件 ───────────────────────────────────────────────
	for _, name := range []string{"pool-state.yaml", "instances-state.yaml"} {
		src := filepath.Join(*dataDir, name)
		dst := src + ".migrated"
		if err := os.Rename(src, dst); err != nil {
			log.Printf("警告: 备份 %s 失败: %v（不影响迁移结果）", name, err)
		} else {
			log.Printf("✓ 备份 %s → %s", name, dst)
		}
	}

	fmt.Println()
	fmt.Println("========================================")
	fmt.Printf("迁移完成！pilot.db 已就绪: %s\n", dbPath)
	fmt.Println("可以启动新版 redis-pilot-server 了")
	fmt.Println("========================================")
}

func readNodeState(dataDir string) (*apitypes.NodeState, error) {
	path := filepath.Join(dataDir, "pool-state.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取 %s: %w", path, err)
	}
	var state apitypes.NodeState
	if err := yaml.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("解析 %s: %w", path, err)
	}
	if state.Servers == nil {
		state.Servers = make(map[string]*apitypes.NodeServer)
	}
	return &state, nil
}

func readInstancesState(dataDir string) (*apitypes.InstancesState, error) {
	path := filepath.Join(dataDir, "instances-state.yaml")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &apitypes.InstancesState{
			Groups:    make(map[string]*apitypes.InstanceGroupState),
			Instances: make(map[string]*apitypes.Instance),
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("读取 %s: %w", path, err)
	}
	var state apitypes.InstancesState
	if err := yaml.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("解析 %s: %w", path, err)
	}
	if state.Groups == nil {
		state.Groups = make(map[string]*apitypes.InstanceGroupState)
	}
	if state.Instances == nil {
		state.Instances = make(map[string]*apitypes.Instance)
	}
	return &state, nil
}
