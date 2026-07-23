package store

import (
	"os"
	"path/filepath"
	"testing"
)

// TestOpen_RestrictsFilePermissions 验证 Open 创建的数据目录与 pilot.db 文件
// 权限被收紧为仅属主可访问，防止明文密码/token 被本机其他用户读取（S6）。
func TestOpen_RestrictsFilePermissions(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "state")

	db, err := Open(dataDir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	dirInfo, err := os.Stat(dataDir)
	if err != nil {
		t.Fatalf("stat dataDir: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0700 {
		t.Errorf("dataDir perm = %o, want 0700", perm)
	}

	dbPath := filepath.Join(dataDir, "pilot.db")
	dbInfo, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("stat pilot.db: %v", err)
	}
	if perm := dbInfo.Mode().Perm(); perm != 0600 {
		t.Errorf("pilot.db perm = %o, want 0600", perm)
	}

	// WAL/SHM 伴生文件如果存在也必须是 0600。
	for _, suffix := range []string{"-wal", "-shm"} {
		p := dbPath + suffix
		info, err := os.Stat(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("stat %s: %v", p, err)
		}
		if perm := info.Mode().Perm(); perm != 0600 {
			t.Errorf("%s perm = %o, want 0600", p, perm)
		}
	}
}

// TestOpen_TightensExistingLoosePermissions 验证升级场景：目录/文件已存在且权限过宽时，
// Open 会显式收紧权限，而不是保留旧的宽松权限。
func TestOpen_TightensExistingLoosePermissions(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "state")

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("premkdir: %v", err)
	}

	db, err := Open(dataDir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	dirInfo, err := os.Stat(dataDir)
	if err != nil {
		t.Fatalf("stat dataDir: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0700 {
		t.Errorf("dataDir perm = %o, want 0700 after tightening", perm)
	}
}
