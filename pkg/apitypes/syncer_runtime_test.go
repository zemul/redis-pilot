package apitypes

import (
	"strings"
	"testing"
)

func validSyncerReport() SyncerHeartbeat {
	return SyncerHeartbeat{SyncerID: "worker", NodeID: "node", BootID: "boot", Sequence: 1, BuildVersion: "test", State: "running", LastError: "none", Running: true, Tasks: []SyncTaskReport{{TaskID: "task", IdentityDigest: strings.Repeat("a", 64), State: "running", LastError: "none", Running: true}}}
}

func TestSyncerHeartbeatValidation(t *testing.T) {
	if err := validSyncerReport().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*SyncerHeartbeat){
		"id":             func(v *SyncerHeartbeat) { v.SyncerID = "bad/id" },
		"boot":           func(v *SyncerHeartbeat) { v.BootID = "" },
		"node":           func(v *SyncerHeartbeat) { v.NodeID = strings.Repeat("a", 65) },
		"sequence":       func(v *SyncerHeartbeat) { v.Sequence = 0 },
		"version":        func(v *SyncerHeartbeat) { v.BuildVersion = "secret\nline" },
		"state":          func(v *SyncerHeartbeat) { v.State = "unexpected" },
		"error":          func(v *SyncerHeartbeat) { v.LastError = "raw secret" },
		"empty":          func(v *SyncerHeartbeat) { v.Tasks = nil },
		"too_many":       func(v *SyncerHeartbeat) { v.Tasks = make([]SyncTaskReport, 9) },
		"duplicate":      func(v *SyncerHeartbeat) { v.Tasks = append(v.Tasks, v.Tasks[0]) },
		"unsorted":       func(v *SyncerHeartbeat) { x := v.Tasks[0]; x.TaskID = "aaa"; v.Tasks = append(v.Tasks, x) },
		"digest":         func(v *SyncerHeartbeat) { v.Tasks[0].IdentityDigest = strings.Repeat("A", 64) },
		"task_state":     func(v *SyncerHeartbeat) { v.Tasks[0].State = "SENSITIVE" },
		"task_error":     func(v *SyncerHeartbeat) { v.Tasks[0].LastError = "SENSITIVE" },
		"false_terminal": func(v *SyncerHeartbeat) { v.State = "stopped"; v.Running = false },
	} {
		t.Run(name, func(t *testing.T) {
			v := validSyncerReport()
			mutate(&v)
			err := v.Validate()
			if err == nil || strings.Contains(err.Error(), "SENSITIVE") {
				t.Fatal("accepted or leaked report", err)
			}
		})
	}
}
