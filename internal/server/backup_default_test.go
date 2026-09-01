package server

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// defaultBackupSchedule 应稳定（同组名多次一致）、落在 02:00–04:00 窗口内、且是合法 5 字段 cron。
func TestDefaultBackupSchedule_StableAndInWindow(t *testing.T) {
	groups := []string{"order", "cache-a", "payments", "session-store", "g", "another-longer-group-name"}
	for _, g := range groups {
		s1 := defaultBackupSchedule(g)
		s2 := defaultBackupSchedule(g)
		if s1 != s2 {
			t.Fatalf("group %q: schedule not stable: %q vs %q", g, s1, s2)
		}
		fields := strings.Fields(s1)
		if len(fields) != 5 {
			t.Fatalf("group %q: expected 5 cron fields, got %q", g, s1)
		}
		minute, err := strconv.Atoi(fields[0])
		if err != nil || minute < 0 || minute > 59 {
			t.Fatalf("group %q: bad minute in %q", g, s1)
		}
		hour, err := strconv.Atoi(fields[1])
		if err != nil || hour < 2 || hour > 3 {
			t.Fatalf("group %q: hour %d outside 02:00-04:00 window in %q", g, hour, s1)
		}
		if fields[2] != "*" || fields[3] != "*" || fields[4] != "*" {
			t.Fatalf("group %q: expected daily schedule, got %q", g, s1)
		}
	}
}

// defaultGroupBackup 只对 persistent 组返回策略；cache 组返回 nil。
func TestDefaultGroupBackup_CategoryGate(t *testing.T) {
	if b := defaultGroupBackup("cache", "g"); b != nil {
		t.Fatalf("cache group should get no default backup, got %+v", b)
	}
	b := defaultGroupBackup("persistent", "g")
	if b == nil {
		t.Fatal("persistent group should get a default backup")
	}
	if b.Retention != 7 {
		t.Fatalf("expected retention 7, got %d", b.Retention)
	}
	if b.Schedule != defaultBackupSchedule("g") {
		t.Fatalf("expected scattered schedule, got %q", b.Schedule)
	}
}

// 打散：足够多的组名应分布到窗口内不同的分钟，不全部撞在同一时刻。
func TestDefaultBackupSchedule_Scatter(t *testing.T) {
	seen := map[string]int{}
	for i := 0; i < 50; i++ {
		seen[defaultBackupSchedule(fmt.Sprintf("group-%d", i))]++
	}
	if len(seen) < 10 {
		t.Fatalf("expected schedules scattered across many slots, got only %d distinct", len(seen))
	}
}
