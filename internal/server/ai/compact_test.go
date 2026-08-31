package ai

import "testing"

func mkHistory(n int) []Message {
	h := make([]Message, n)
	for i := 0; i < n; i++ {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		h[i] = Message{Role: role, Content: "msg"}
	}
	return h
}

func TestSplitHistory(t *testing.T) {
	tests := []struct {
		name       string
		n          int
		threshold  int
		keep       int
		wantOk     bool
		wantOlder  int
		wantRecent int
	}{
		{"compression disabled (threshold<=0)", 100, 0, 6, false, 0, 100},
		{"under threshold no compaction", 20, 20, 6, false, 0, 20},
		{"exactly at threshold no compaction", 20, 20, 6, false, 0, 20},
		{"over threshold triggers compaction", 30, 20, 6, true, 24, 6},
		{"keep>=len no compaction", 30, 5, 30, false, 0, 30},
		{"keep negative clamped to 0", 30, 20, -3, true, 30, 0},
		{"just over threshold", 21, 20, 6, true, 15, 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			older, recent, ok := splitHistory(mkHistory(tt.n), tt.threshold, tt.keep)
			if ok != tt.wantOk {
				t.Fatalf("ok=%v want %v", ok, tt.wantOk)
			}
			if len(older) != tt.wantOlder {
				t.Errorf("older=%d want %d", len(older), tt.wantOlder)
			}
			if len(recent) != tt.wantRecent {
				t.Errorf("recent=%d want %d", len(recent), tt.wantRecent)
			}
			// 不变式：切分不丢消息，older+recent 覆盖全部输入。
			if ok && len(older)+len(recent) != tt.n {
				t.Errorf("older+recent=%d, lost messages (want %d)", len(older)+len(recent), tt.n)
			}
		})
	}
}
