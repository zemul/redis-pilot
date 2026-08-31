package server

import (
	"net/http"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestNormalizeGroupLabels(t *testing.T) {
	got, err := normalizeGroupLabels([]string{" 交易 ", "生产", "交易"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "交易" || got[1] != "生产" {
		t.Fatalf("unexpected normalized labels: %#v", got)
	}
	if _, err := normalizeGroupLabels([]string{"bad,label"}); err == nil {
		t.Fatal("expected comma to be rejected")
	}
}

func TestInstanceGroupLabels_UpdateClearAndFilter(t *testing.T) {
	s := newTestServer(t, "")
	if err := s.db.UpsertGroup("order", &apitypes.InstanceGroupState{Type: "standalone", Engine: "redis", Category: "cache"}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.UpsertInstance("order-0", &apitypes.Instance{Group: "order", Role: "master", DesiredState: "running"}); err != nil {
		t.Fatal(err)
	}
	r := s.Router()

	w := doAuthRequest(r, http.MethodPost, "/instance/labels", map[string]interface{}{
		"group": "order", "labels": []string{"交易", "生产"},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("update labels: %d %s", w.Code, w.Body.String())
	}
	state, err := s.db.ReadInstances()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Groups["order"].Labels) != 2 || state.Groups["order"].Labels[0] != "交易" {
		t.Fatalf("labels not persisted: %#v", state.Groups["order"].Labels)
	}

	w = doAuthRequest(r, http.MethodGet, "/instance/list?label=%E4%BA%A4%E6%98%93&label=%E7%94%9F%E4%BA%A7", nil)
	if w.Code != http.StatusOK || !containsJSONText(w.Body.Bytes(), "order-0") {
		t.Fatalf("matching filter: %d %s", w.Code, w.Body.String())
	}
	w = doAuthRequest(r, http.MethodGet, "/instance/list?label=%E6%B5%8B%E8%AF%95", nil)
	if w.Code != http.StatusOK || containsJSONText(w.Body.Bytes(), "order-0") {
		t.Fatalf("non-matching filter: %d %s", w.Code, w.Body.String())
	}

	w = doAuthRequest(r, http.MethodPost, "/instance/labels", map[string]interface{}{
		"group": "order", "labels": []string{},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("clear labels: %d %s", w.Code, w.Body.String())
	}
	state, _ = s.db.ReadInstances()
	if len(state.Groups["order"].Labels) != 0 {
		t.Fatalf("labels not cleared: %#v", state.Groups["order"].Labels)
	}
}

func containsJSONText(data []byte, value string) bool {
	for i := 0; i+len(value) <= len(data); i++ {
		if string(data[i:i+len(value)]) == value {
			return true
		}
	}
	return false
}
