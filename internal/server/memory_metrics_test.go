package server

import (
	"encoding/json"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
	"testing"
)

func TestRSSMemoryViewDoesNotReplaceAllocatedMemory(t *testing.T) {
	data := map[string]interface{}{"used_memory": map[string]interface{}{"redis": float64(100)}, "used_memory_rss": map[string]interface{}{"redis": float64(200), "kvrocks": json.Number("300")}}
	s := &Server{memoryUsed: parseUsedMemory(data), memoryRSS: parseMemoryField(data, "used_memory_rss")}
	view := &apitypes.InstancesStateView{Instances: map[string]*apitypes.InstanceView{"redis": {}, "kvrocks": {}}}
	s.injectMemoryUsed(view)
	if view.Instances["redis"].MemoryUsed != 100 || view.Instances["kvrocks"].MemoryUsed != 0 || view.Instances["kvrocks"].MemoryRSS != 300 {
		t.Fatal(view)
	}
	if len(parseMemoryField(map[string]interface{}{}, "used_memory_rss")) != 0 {
		t.Fatal("missing field became a reading")
	}
}
