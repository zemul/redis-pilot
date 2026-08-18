package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

// --------------------------------------------------------------------------
// 备份产物校验
// --------------------------------------------------------------------------

// TestVerifyBackupSource_RejectsStaleSnapshot 是本轮修复的核心之一。
//
// 回归背景：早期实现的 BGSAVE 等待循环是
//
//	for i := 0; i < 60; i++ {
//	    info, _ := redisCmd(...)                 // 错误被丢弃
//	    if strings.Contains(info, "...:0") { break }
//	    time.Sleep(time.Second)
//	}
//	runShell("tar", "-czf", dst, "-C", src, ".")  // 无条件执行
//
// 循环耗尽和成功退出走同一条路，且错误被丢弃。实例连不上时 info 恒为空串，循环必然
// 跑满 60 次，然后照样打包——打出来的是**上一次 BGSAVE 残留的快照**，而文件名带着
// 崭新的时间戳。运维会看到备份列表里有今天的记录，直到真正需要恢复时才发现内容是旧的。
// 仅校验"目录非空"抓不到这种情况，必须校验"有文件是本次生成的"。
func TestVerifyBackupSource_RejectsStaleSnapshot(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "000001.sst")
	writeFile(t, stale, "old snapshot data")
	// 把文件 mtime 改到 1 小时前，模拟上一次 BGSAVE 的残留
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}

	startedAt := time.Now()
	err := verifyBackupSource(dir, startedAt)
	if err == nil {
		t.Fatal("陈旧快照应当被拒绝：目录非空但没有任何文件是本次 BGSAVE 生成的")
	}
	if !strings.Contains(err.Error(), "stale") {
		t.Errorf("错误信息应当指出快照陈旧，实际：%v", err)
	}
}

func TestVerifyBackupSource_AcceptsFreshSnapshot(t *testing.T) {
	dir := t.TempDir()
	startedAt := time.Now()
	writeFile(t, filepath.Join(dir, "000001.sst"), "fresh snapshot data")

	if err := verifyBackupSource(dir, startedAt); err != nil {
		t.Errorf("本次生成的快照应当通过校验，实际：%v", err)
	}
}

func TestVerifyBackupSource_RejectsEmptyDir(t *testing.T) {
	dir := t.TempDir()
	err := verifyBackupSource(dir, time.Now())
	if err == nil {
		t.Fatal("空目录应当被拒绝：tar 一个空目录会成功，产出格式有效但内容为空的备份包")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("错误信息应当指出目录为空，实际：%v", err)
	}
}

func TestVerifyBackupFile(t *testing.T) {
	dir := t.TempDir()

	t.Run("不存在", func(t *testing.T) {
		if err := verifyBackupFile(filepath.Join(dir, "nope.rdb"), time.Now()); err == nil {
			t.Error("不存在的文件应当被拒绝")
		}
	})

	t.Run("空文件", func(t *testing.T) {
		p := filepath.Join(dir, "empty.rdb")
		writeFile(t, p, "")
		if err := verifyBackupFile(p, time.Now()); err == nil {
			t.Error("空文件应当被拒绝")
		}
	})

	t.Run("陈旧文件", func(t *testing.T) {
		p := filepath.Join(dir, "stale.rdb")
		writeFile(t, p, "data")
		old := time.Now().Add(-time.Hour)
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
		if err := verifyBackupFile(p, time.Now()); err == nil {
			t.Error("mtime 早于本次 BGSAVE 的文件应当被拒绝")
		}
	})

	t.Run("正常文件", func(t *testing.T) {
		startedAt := time.Now()
		p := filepath.Join(dir, "good.rdb")
		writeFile(t, p, "fresh data")
		if err := verifyBackupFile(p, startedAt); err != nil {
			t.Errorf("正常文件应当通过，实际：%v", err)
		}
	})
}

func TestVerifyBackupArchiveExists(t *testing.T) {
	dir := t.TempDir()

	if err := verifyBackupArchiveExists(filepath.Join(dir, "missing.tar.gz")); err == nil {
		t.Error("不存在的备份包应当被拒绝")
	}

	empty := filepath.Join(dir, "empty.tar.gz")
	writeFile(t, empty, "")
	err := verifyBackupArchiveExists(empty)
	if err == nil {
		t.Fatal("空备份包应当被拒绝：用它覆盖现有数据是纯粹的数据损失")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("错误信息应当指出备份为空，实际：%v", err)
	}

	good := filepath.Join(dir, "good.tar.gz")
	writeFile(t, good, "archive bytes")
	if err := verifyBackupArchiveExists(good); err != nil {
		t.Errorf("正常备份包应当通过，实际：%v", err)
	}
}

// --------------------------------------------------------------------------
// 恢复原子性
// --------------------------------------------------------------------------

// TestSwapInPath_PreservesOriginalData 验证替换后原数据被保留而非删除。
//
// 回归背景：早期恢复实现是「先 os.RemoveAll(目标) 再解压」。备份包损坏或磁盘写满时，
// 原始数据已经不可恢复，实例以空库启动。配合备份侧可能产出的空/陈旧备份包，两个缺陷
// 串起来就是「用一个空备份把好数据覆盖掉」。
func TestSwapInPath_PreservesOriginalData(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "db")
	writeFile(t, filepath.Join(target, "original.sst"), "ORIGINAL DATA")

	staged := filepath.Join(root, "staging", "db")
	writeFile(t, filepath.Join(staged, "restored.sst"), "RESTORED DATA")

	preserve := filepath.Join(root, "db.before-restore")
	if err := swapInPath(staged, target, preserve); err != nil {
		t.Fatal(err)
	}

	// 新数据到位
	if got := readFile(t, filepath.Join(target, "restored.sst")); got != "RESTORED DATA" {
		t.Errorf("恢复的数据未到位，得到 %q", got)
	}
	if _, err := os.Stat(filepath.Join(target, "original.sst")); !os.IsNotExist(err) {
		t.Error("目标目录应当被整体替换，不应残留原文件")
	}
	// 原数据被保留，可回退
	if got := readFile(t, filepath.Join(preserve, "original.sst")); got != "ORIGINAL DATA" {
		t.Errorf("原数据应当被保留以便回退，得到 %q", got)
	}
}

func TestSwapInPath_TargetDoesNotExist(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "dump.rdb")
	staged := filepath.Join(root, "staging", "dump.rdb")
	writeFile(t, staged, "RESTORED")

	preserve := filepath.Join(root, "dump.rdb.before-restore")
	if err := swapInPath(staged, target, preserve); err != nil {
		t.Fatalf("目标不存在时应当直接放入新数据，实际报错：%v", err)
	}
	if got := readFile(t, target); got != "RESTORED" {
		t.Errorf("得到 %q", got)
	}
	if _, err := os.Stat(preserve); !os.IsNotExist(err) {
		t.Error("目标原本不存在，不应产生保留目录")
	}
}

func TestRollbackSwappedPath_RestoresWholeDataset(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "data")
	writeFile(t, filepath.Join(target, "old.rdb"), "OLD")
	staged := filepath.Join(root, "staging", "data")
	writeFile(t, filepath.Join(staged, "new.rdb"), "NEW")
	preserve := filepath.Join(root, "data.before-restore")
	if err := swapInPath(staged, target, preserve); err != nil {
		t.Fatal(err)
	}
	if err := rollbackSwappedPath(target, preserve); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(target, "old.rdb")); got != "OLD" {
		t.Fatalf("original dataset not restored, got %q", got)
	}
	if _, err := os.Stat(filepath.Join(target, "new.rdb")); !os.IsNotExist(err) {
		t.Fatal("failed restored dataset must not remain at active target")
	}
}

// TestSwapInPath_RollsBackWhenStagedMissing 验证放入新数据失败时原数据被挪回原位，
// 而不是留下一个空目标。
func TestSwapInPath_RollsBackWhenStagedMissing(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "db")
	writeFile(t, filepath.Join(target, "original.sst"), "ORIGINAL DATA")

	// staged 路径不存在，模拟暂存阶段产物丢失
	staged := filepath.Join(root, "staging", "does-not-exist")
	preserve := filepath.Join(root, "db.before-restore")

	err := swapInPath(staged, target, preserve)
	if err == nil {
		t.Fatal("staged 不存在时应当返回错误")
	}
	// 关键：原数据必须已经回到原位
	if got := readFile(t, filepath.Join(target, "original.sst")); got != "ORIGINAL DATA" {
		t.Errorf("替换失败后原数据应当被回滚到原位，得到 %q", got)
	}
	if !strings.Contains(err.Error(), "original data restored") {
		t.Errorf("错误信息应当说明原数据已回滚，实际：%v", err)
	}
}

func TestDirHasEntries(t *testing.T) {
	dir := t.TempDir()

	nonEmpty, err := dirHasEntries(dir)
	if err != nil {
		t.Fatal(err)
	}
	if nonEmpty {
		t.Error("空目录应当报告为空")
	}

	writeFile(t, filepath.Join(dir, "f"), "x")
	nonEmpty, err = dirHasEntries(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !nonEmpty {
		t.Error("非空目录应当报告非空")
	}

	if _, err := dirHasEntries(filepath.Join(dir, "missing")); err == nil {
		t.Error("不存在的目录应当返回错误")
	}
}

func TestDirHasEntryModifiedSince(t *testing.T) {
	dir := t.TempDir()
	oldFile := filepath.Join(dir, "old")
	writeFile(t, oldFile, "old")
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(oldFile, old, old); err != nil {
		t.Fatal(err)
	}

	since := time.Now()
	fresh, err := dirHasEntryModifiedSince(dir, since)
	if err != nil {
		t.Fatal(err)
	}
	if fresh {
		t.Error("只有陈旧文件时应当报告没有新文件")
	}

	// 嵌套子目录里的新文件也要能被发现（RocksDB 快照有子目录结构）
	writeFile(t, filepath.Join(dir, "sub", "new.sst"), "new")
	fresh, err = dirHasEntryModifiedSince(dir, since)
	if err != nil {
		t.Fatal(err)
	}
	if !fresh {
		t.Error("子目录中的新文件应当被发现")
	}
}

// TestWaitForPersistenceDone_TimesOutInsteadOfSilentlySucceeding 验证等待超时会
// 返回错误，而不是像早期实现那样静默放行让调用方继续打包。
// 返回错误，而不是像早期实现那样静默放行让调用方继续打包。
//
// 这里的实例名不存在，redisCmd 必然失败（podman exec 找不到容器），因此走的是
// "连续失败达到阈值" 的快速失败路径——这正是早期实现最危险的场景：实例连不上时
// info 恒为空串，标记永远匹配不上，循环跑满 60 秒后不报错，照样打包出一个内容
// 陈旧或为空、时间戳崭新的备份包。
func TestWaitForPersistenceDone_TimesOutInsteadOfSilentlySucceeding(t *testing.T) {
	start := time.Now()
	err := waitForPersistenceDone("definitely-not-a-real-instance-xyz", "",
		"rdb_bgsave_in_progress:0", 10*time.Second)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("实例不可达时必须返回错误，不能静默放行让调用方继续打包")
	}
	// 应当在连续失败阈值处快速失败，而不是白等满整个超时窗口
	if elapsed > 8*time.Second {
		t.Errorf("应当在连续失败达到阈值后快速失败，实际等了 %s", elapsed)
	}
	if !strings.Contains(err.Error(), "unreachable") && !strings.Contains(err.Error(), "timed out") {
		t.Errorf("错误信息应当说明是不可达或超时，实际：%v", err)
	}
}

// --------------------------------------------------------------------------
// RESP 错误回复检查
// --------------------------------------------------------------------------

// TestRedisCmdChecked_TreatsErrReplyAsFailure 说明为什么备份流程必须用
// redisCmdChecked：redis-cli 收到服务端的 -ERR 回复时，进程本身仍以退出码 0 结束，
// 错误只体现在标准输出内容里。裸 redisCmd 会把 "BGSAVE 被拒绝" 当成成功，
// 于是后续流程去打包一个从未生成的快照。
//
// 这里只验证判定函数本身，因为 redisCmd 依赖 podman 可执行环境。
func TestRedisCmdChecked_ErrReplyDetection(t *testing.T) {
	cases := map[string]bool{
		"ERR Background save already in progress":   true,
		"-ERR unknown command 'ROCKSDB.CHECKPOINT'": true,
		"Background saving started":                 false,
		"OK":                                        false,
		"":                                          false,
	}
	for out, want := range cases {
		if got := isRedisErrReply(out); got != want {
			t.Errorf("isRedisErrReply(%q) = %v, want %v", out, got, want)
		}
	}
}
