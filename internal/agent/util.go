package agent

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// redisCmd 通过 podman exec 执行 redis-cli 命令
func redisCmd(instanceName, password string, args ...string) (string, error) {
	// 尝试 redis- 和 kvrocks- 前缀
	for _, prefix := range []string{"redis-", "kvrocks-"} {
		container := prefix + instanceName
		cliArgs := []string{"exec", container, "redis-cli"}
		if password != "" {
			cliArgs = append(cliArgs, "-a", password, "--no-auth-warning")
		}
		cliArgs = append(cliArgs, args...)
		out, err := exec.Command("podman", cliArgs...).CombinedOutput()
		if err == nil {
			return strings.TrimSpace(string(out)), nil
		}
	}
	return "", fmt.Errorf("instance %s not found or not running", instanceName)
}

// isRedisErrReply 判断 redis-cli 的输出是否是一条 RESP 错误回复（例如 "ERR ..."）。
// redis-cli 即使收到服务端的 -ERR 错误回复，进程本身也会以退出码 0 正常结束，
// 错误信息体现在标准输出内容里，而不是 exec.Command 返回的 error，调用方必须
// 显式检查输出内容才能感知命令是否真正执行成功。
func isRedisErrReply(out string) bool {
	return strings.HasPrefix(out, "ERR ") || strings.HasPrefix(out, "-ERR ")
}

// redisCmdChecked 在 redisCmd 之上补上 RESP 错误回复检查，把"命令被服务端拒绝"
// 也变成一个 error。备份流程必须用这个版本：BGSAVE 返回 -ERR（例如上一次 BGSAVE
// 仍在进行）时，裸 redisCmd 会当成成功。
func redisCmdChecked(instanceName, password string, args ...string) (string, error) {
	out, err := redisCmd(instanceName, password, args...)
	if err != nil {
		return out, err
	}
	if isRedisErrReply(out) {
		return out, fmt.Errorf("%s rejected: %s", strings.Join(args, " "), out)
	}
	return out, nil
}

// waitForPersistenceDone 轮询 INFO persistence，等待 doneMarker 出现（例如
// "rdb_bgsave_in_progress:0"）。
//
// 与早期实现的三处区别，每一处都对应一个真实的静默数据损坏路径：
//
//  1. 早期写法是 `info, _ := redisCmd(...)`，错误被丢弃。实例连不上时 info 是空串，
//     标记永远匹配不上，循环跑满 60 次后**不返回错误**，调用方继续 tar/copy。
//     现在连续失败达到阈值直接返回错误。
//  2. 早期实现循环耗尽和成功退出走同一条路，没有区分。现在耗尽返回超时错误。
//  3. 早期实现没有对"实例根本不响应"和"BGSAVE 确实还在跑"做区分，两者都只是继续等。
//     现在前者会快速失败，避免白等 60 秒后产出一个内容陈旧、时间戳崭新的备份包。
func waitForPersistenceDone(instanceName, password, doneMarker string, timeout time.Duration) error {
	const maxConsecutiveErrs = 3
	deadline := time.Now().Add(timeout)
	consecutiveErrs := 0
	var lastErr error

	for time.Now().Before(deadline) {
		info, err := redisCmd(instanceName, password, "INFO", "persistence")
		if err != nil {
			lastErr = err
			consecutiveErrs++
			if consecutiveErrs >= maxConsecutiveErrs {
				return fmt.Errorf("instance %s became unreachable while waiting for %s: %w",
					instanceName, doneMarker, lastErr)
			}
			time.Sleep(time.Second)
			continue
		}
		consecutiveErrs = 0
		if strings.Contains(info, doneMarker) {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timed out after %s waiting for %s on instance %s", timeout, doneMarker, instanceName)
}

// runShell 执行 shell 命令
func runShell(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %v: %w\n%s", name, args, err, out)
	}
	return strings.TrimSpace(string(out)), nil
}

// copyFile 复制文件
func copyFile(src, dst string) (retErr error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			out.Close()
			os.Remove(dst)
		}
	}()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	if err = out.Sync(); err != nil {
		return err
	}
	return out.Close()
}

// verifyBackupSource 校验 BGSAVE 的产物目录可用：存在、非空，且至少有一个文件是
// startedAt 之后写入的。
//
// 为什么"非空"不够：一次失败的 BGSAVE 会把上一次的快照文件留在目录里。仅校验非空的话，
// tar 会打包出一个格式完全有效、时间戳崭新、但内容是旧快照的备份包。这种备份包比没有
// 备份更危险——运维会以为数据受保护。
//
// 判据是"至少有一个新文件"而不是"所有文件都是新的"：Kvrocks 的 checkpoint 对未变更的
// SST 用硬链接共享旧文件，它们的 mtime 本来就是旧的，要求全新会把正常备份全部误拒。
// 依赖的是 RocksDB 每次 checkpoint 都会重写 MANIFEST/CURRENT。这一点已在实机验证：
// 对 Kvrocks master 执行 backup exec 正常产出 checkpoint.tar.gz，未触发本函数的误拒。
func verifyBackupSource(dir string, startedAt time.Time) error {
	nonEmpty, err := dirHasEntries(dir)
	if err != nil {
		return fmt.Errorf("snapshot dir %s unusable: %w", dir, err)
	}
	if !nonEmpty {
		return fmt.Errorf("snapshot dir %s is empty", dir)
	}
	fresh, err := dirHasEntryModifiedSince(dir, startedAt)
	if err != nil {
		return fmt.Errorf("inspect snapshot dir %s: %w", dir, err)
	}
	if !fresh {
		return fmt.Errorf("snapshot dir %s contains no file written by this BGSAVE "+
			"(all files predate it, likely a stale snapshot from an earlier run)", dir)
	}
	return nil
}

// verifyBackupFile 校验 BGSAVE 产出的单个文件可用：存在、非空、且是本次写入的。
func verifyBackupFile(path string, startedAt time.Time) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("snapshot file %s unusable: %w", path, err)
	}
	if info.Size() == 0 {
		return fmt.Errorf("snapshot file %s is empty", path)
	}
	if info.ModTime().Before(startedAt.Add(-time.Second)) {
		return fmt.Errorf("snapshot file %s predates this BGSAVE (mtime=%s, started=%s), "+
			"likely a stale snapshot from an earlier run",
			path, info.ModTime().Format(time.RFC3339), startedAt.Format(time.RFC3339))
	}
	return nil
}

// verifyBackupArchiveExists 恢复前校验备份文件存在且非空。空文件说明上一次备份中途
// 失败留下了残骸，用它去覆盖现有数据是纯粹的数据损失。
func verifyBackupArchiveExists(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("backup %s not found: %w", filepath.Base(path), err)
	}
	if info.Size() == 0 {
		return fmt.Errorf("backup %s is empty, refusing to restore from it", filepath.Base(path))
	}
	return nil
}

// ---------------------------------------------------------------------------
// 恢复流程的原子性辅助函数
//
// 早期实现的恢复是「先 os.RemoveAll(目标) 再解压/复制」，一旦备份包损坏、磁盘写满
// 或进程被杀，原始数据已经不可恢复，实例以空库启动。配合备份侧可能产出的空备份包
// （见 waitForPersistenceDone 的注释），两个缺陷串起来就是「用一个空备份把好数据覆盖掉」。
//
// 下面的函数保证：原始数据在替换成功之前始终存在，且替换后仍以 .before-restore-<ts>
// 形式保留一份，便于误恢复后回退。rename 在同一文件系统内是原子操作。
// ---------------------------------------------------------------------------

// dirHasEntries 报告目录是否存在且非空。
func dirHasEntries(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	return len(entries) > 0, nil
}

// dirHasEntryModifiedSince 报告目录下（递归）是否存在 mtime 不早于 since 的文件。
//
// 用途：确认本次 BGSAVE 真的产出了新文件，而不是目录里残留着上一次的快照。仅校验
// "目录非空"不足以区分这两种情况——一个失败的 BGSAVE 会留下上次的文件，打包出来的
// 备份包格式完全有效，只是内容是旧的，而时间戳是新的。
func dirHasEntryModifiedSince(dir string, since time.Time) (bool, error) {
	found := false
	// 文件系统 mtime 粒度可能是秒级，向前留 1 秒容差避免边界误判。
	cutoff := since.Add(-time.Second)
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !info.ModTime().Before(cutoff) {
			found = true
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

// restorePreservePath 为一次恢复操作生成"原数据保留位置"的路径。
func restorePreservePath(target string) string {
	return target + ".before-restore-" + time.Now().Format("20060102T150405")
}

// swapInPath 用 staged 原子替换 target，并把 target 的原内容保留到 preserve
// （而不是删除）。target 不存在时直接把 staged 放到位。
//
// 失败语义：
//   - 第一步（保留原数据）失败 → target 未被触碰，返回错误，调用方可安全放弃
//   - 第二步（放入新数据）失败 → 尝试把原数据挪回来；若连回滚都失败，返回的错误里
//     会带上原数据的保留路径，让运维知道去哪里找
func swapInPath(staged, target, preserve string) error {
	targetExists := true
	if _, err := os.Lstat(target); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("stat %s: %w", target, err)
		}
		targetExists = false
	}

	if targetExists {
		if err := os.Rename(target, preserve); err != nil {
			return fmt.Errorf("preserve existing data at %s: %w (original data untouched)", preserve, err)
		}
	}
	if err := os.Rename(staged, target); err != nil {
		if targetExists {
			if rbErr := os.Rename(preserve, target); rbErr != nil {
				return fmt.Errorf("move restored data into place failed: %w; rollback also failed: %v; "+
					"ORIGINAL DATA IS PRESERVED AT %s", err, rbErr, preserve)
			}
		}
		return fmt.Errorf("move restored data into place failed: %w (original data restored)", err)
	}
	return nil
}

// rollbackSwappedPath 撤销一次已经成功的 swapInPath。恢复后的新数据移到 failed 路径
// 留作排查，再把原数据放回 target。调用方随后可重新启动原实例。
func rollbackSwappedPath(target, preserve string) error {
	failed := target + ".failed-restore-" + time.Now().Format("20060102T150405.000000000")
	if err := os.Rename(target, failed); err != nil {
		return fmt.Errorf("preserve failed restored data at %s: %w", failed, err)
	}
	if err := os.Rename(preserve, target); err != nil {
		// 尽力把新数据放回原位，避免 target 消失。
		_ = os.Rename(failed, target)
		return fmt.Errorf("restore original data from %s: %w", preserve, err)
	}
	return nil
}

// isContainerNotFound 判断 podman 操作错误是否因为容器不存在。
func isContainerNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "no such container") || strings.Contains(msg, "no container with name or ID")
}

// dirSize 递归计算目录总大小（字节）。
func dirSize(path string) int64 {
	var total int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}
