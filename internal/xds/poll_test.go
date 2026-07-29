package xds

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// newSnapshotServer 起一个假 Server，按需返回快照或 304。
// respond 决定每次请求怎么回应，返回的 handled 为 false 时按默认逻辑回完整快照。
func newSnapshotServer(t *testing.T, hits *atomic.Int32, respond func(w http.ResponseWriter, r *http.Request) bool) *httptest.Server {
	t.Helper()
	snap := apitypes.ProxySnapshot{
		Version:   "v1",
		Listeners: []apitypes.ProxyListener{},
		Clusters:  []apitypes.ProxyCluster{},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if respond != nil && respond(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    snap,
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestService(t *testing.T, endpoint string, poll PollConfig) *Service {
	t.Helper()
	return NewService(&Config{
		Listen: "127.0.0.1:0",
		Server: ServerConfig{Endpoint: endpoint},
		Poll:   poll,
		Envoy:  EnvoyConfig{NodeIDs: []string{"test-node"}},
	})
}

// TestPoll_DoesNotBusyLoopWhenServerIgnoresLongPollParams 是这次改动最大的风险点。
//
// 长轮询模式下拿到"版本未变"的响应后会立刻发起下一次请求，因为正常情况下服务端已经
// 挂起了 Wait 时长。但如果服务端不认识 known_version/wait 参数（版本不匹配的旧 Server，
// 或中间有代理把查询串吃掉），它会立即返回相同版本的完整快照——没有下限保护就变成一个
// 不停打 Server 的忙循环。
//
// 这里的假 Server 完全忽略参数、立即返回同一个版本，验证请求速率被 Poll.Interval 限住。
func TestPoll_DoesNotBusyLoopWhenServerIgnoresLongPollParams(t *testing.T) {
	var hits atomic.Int32
	srv := newSnapshotServer(t, &hits, nil) // 忽略参数，每次都立即回同一版本

	svc := newTestService(t, srv.URL, PollConfig{
		Interval: 100 * time.Millisecond,
		Timeout:  time.Second,
		Wait:     10 * time.Second, // 开了长轮询，但服务端不配合
	})

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	svc.poll(ctx)

	got := hits.Load()
	// 600ms / 100ms ≈ 6 次，留足余量。忙循环会是成百上千次。
	if got > 20 {
		t.Errorf("600ms 内请求了 %d 次，说明退化成忙循环（Interval 下限失效）", got)
	}
	if got == 0 {
		t.Error("一次请求都没发出")
	}
}

// TestPoll_BacksOffOnServerError 服务端不可用时也必须退避，不能忙循环。
func TestPoll_BacksOffOnServerError(t *testing.T) {
	var hits atomic.Int32
	srv := newSnapshotServer(t, &hits, func(w http.ResponseWriter, r *http.Request) bool {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"success":false,"error":"boom"}`))
		return true
	})

	svc := newTestService(t, srv.URL, PollConfig{
		Interval: 100 * time.Millisecond,
		Timeout:  time.Second,
		Wait:     10 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	svc.poll(ctx)

	if got := hits.Load(); got > 20 {
		t.Errorf("服务端持续报错时请求了 %d 次，没有退避", got)
	}
}

// TestPoll_HandlesNotModified 服务端返回 304 时不能当成错误、也不能试图解 JSON。
func TestPoll_HandlesNotModified(t *testing.T) {
	var hits atomic.Int32
	srv := newSnapshotServer(t, &hits, func(w http.ResponseWriter, r *http.Request) bool {
		// 第一次给快照让客户端拿到版本，之后一律 304
		if r.URL.Query().Get("known_version") == "" {
			return false
		}
		w.WriteHeader(http.StatusNotModified)
		return true
	})

	svc := newTestService(t, srv.URL, PollConfig{
		Interval: 50 * time.Millisecond,
		Timeout:  time.Second,
		Wait:     200 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	svc.poll(ctx)

	// 应当拿到了第一版并保持住，没有把 304 当错误反复重置
	svc.mu.Lock()
	version := svc.lastVersion
	svc.mu.Unlock()
	if version != "v1" {
		t.Errorf("lastVersion = %q, want v1（304 被误处理了）", version)
	}
}

// TestPoll_FirstFetchSkipsLongPoll 首次还没有版本可比时不能进长轮询，
// 否则启动阶段会白等一个 Wait 才拿到初始配置。
func TestPoll_FirstFetchSkipsLongPoll(t *testing.T) {
	var hits atomic.Int32
	var sawWaitOnFirst atomic.Bool
	first := true
	srv := newSnapshotServer(t, &hits, func(w http.ResponseWriter, r *http.Request) bool {
		if first {
			first = false
			if r.URL.Query().Get("wait") != "" {
				sawWaitOnFirst.Store(true)
			}
		}
		return false
	})

	svc := newTestService(t, srv.URL, PollConfig{
		Interval: 50 * time.Millisecond,
		Timeout:  time.Second,
		Wait:     30 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	svc.poll(ctx)

	if sawWaitOnFirst.Load() {
		t.Error("首次请求带了 wait 参数，启动时会白等一个 Wait 周期")
	}
	svc.mu.Lock()
	version := svc.lastVersion
	svc.mu.Unlock()
	if version != "v1" {
		t.Errorf("首次拉取没有生效，lastVersion = %q", version)
	}
}
