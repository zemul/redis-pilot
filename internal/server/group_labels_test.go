package server

import (
	"net/http"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestNormalizeGroupLabels(t *testing.T) {
	got, err := normalizeGroupLabels(map[string]string{" team ": " trade ", "env": "prod"})
	if err != nil {
		t.Fatal(err)
	}
	if got["team"] != "trade" || got["env"] != "prod" {
		t.Fatalf("unexpected normalized labels: %#v", got)
	}
	if _, err := normalizeGroupLabels(map[string]string{"bad key": "value"}); err == nil {
		t.Fatal("expected invalid key to be rejected")
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
		"group": "order", "labels": map[string]string{"team": "trade", "env": "prod"},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("update labels: %d %s", w.Code, w.Body.String())
	}
	state, err := s.db.ReadInstances()
	if err != nil {
		t.Fatal(err)
	}
	if state.Groups["order"].Labels["team"] != "trade" {
		t.Fatalf("labels not persisted: %#v", state.Groups["order"].Labels)
	}

	w = doAuthRequest(r, http.MethodGet, "/instance/list?label=team%3Dtrade&label=env%3Dprod", nil)
	if w.Code != http.StatusOK || !containsJSONText(w.Body.Bytes(), "order-0") {
		t.Fatalf("matching filter: %d %s", w.Code, w.Body.String())
	}
	w = doAuthRequest(r, http.MethodGet, "/instance/list?label=team%3Dother", nil)
	if w.Code != http.StatusOK || containsJSONText(w.Body.Bytes(), "order-0") {
		t.Fatalf("non-matching filter: %d %s", w.Code, w.Body.String())
	}

	w = doAuthRequest(r, http.MethodPost, "/instance/labels", map[string]interface{}{
		"group": "order", "labels": map[string]string{},
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
