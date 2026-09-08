package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestSyncProgressAPIReportsExactPositionsAndRejectsInvalid(t *testing.T) {
	s := newTestServer(t, "")
	const token = "syncer-progress-token-long-enough"
	s.cfg.SyncerTokens = map[string]string{"syncer-1": token}
	r := s.Router()
	v := serverSyncerFixture()
	at := time.Now()
	base := int64(9007199254740993)
	pos := func(n int64) *apitypes.SyncProgressPosition {
		return &apitypes.SyncProgressPosition{History: strings.Repeat("a", 40), Offset: n}
	}
	v.Tasks[0].Progress = (&apitypes.SyncProgress{Local: &apitypes.SyncLocalProgress{SampledAt: at, Captured: pos(base), Applied: pos(base - 100), GCConfirmed: pos(base - 200), JournalLimitBytes: 1000}, Source: apitypes.SyncSourceProgress{SampledAt: at, Position: pos(base + 100), Error: "none"}}).At(at)
	path := "/api/v1/syncer/heartbeat/syncer-1"
	if w := doRequestWithAuth(r, "POST", path, token, v); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := doRequestWithAuth(r, "POST", path, token, v); w.Code != 200 {
		t.Fatal("identical progress retry", w.Code, w.Body.String())
	}
	w := doAuthRequest(r, "GET", "/api/v1/syncer/runtime", nil)
	var reply struct {
		Data apitypes.SyncerRuntime `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || w.Code != 200 {
		t.Fatal(err, w.Code)
	}
	o := reply.Data.Observations[0]
	view := o.TaskProgress["task-1"]
	if view.Condition != "catching_up" || *view.SourceCaptureBytes != 100 || o.Tasks[0].Progress.Local.Captured.Offset != base || !strings.Contains(w.Body.String(), `"offset":"9007199254740993"`) {
		t.Fatal("API lost precision or progress", w.Body.String())
	}
	v.Sequence++
	v.Tasks[0].Progress.Source.Error = "SENSITIVE-response"
	if w := doRequestWithAuth(r, "POST", path, token, v); w.Code != 400 || strings.Contains(w.Body.String(), "SENSITIVE") {
		t.Fatal("invalid progress accepted or leaked", w.Code, w.Body.String())
	}
}
