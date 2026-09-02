package server

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

const (
	proxyOnlineAfter = 30 * time.Second
	proxyStaleAfter  = 60 * time.Second
)

type proxyRuntimeObservation struct {
	heartbeat apitypes.ProxyHeartbeat
	received  time.Time
}

func (s *Server) proxyHeartbeat(c *gin.Context) {
	var heartbeat apitypes.ProxyHeartbeat
	if err := c.ShouldBindJSON(&heartbeat); err != nil {
		fail(c, http.StatusBadRequest, "invalid proxy heartbeat")
		return
	}
	heartbeat.ProxyID = strings.TrimSpace(heartbeat.ProxyID)
	heartbeat.Hostname = strings.TrimSpace(heartbeat.Hostname)
	if heartbeat.ProxyID == "" || len(heartbeat.ProxyID) > 128 {
		fail(c, http.StatusBadRequest, "proxy_id is required and must not exceed 128 characters")
		return
	}
	if len(heartbeat.Hostname) > 255 || heartbeat.ListenerCount < 0 || heartbeat.BackendCount < 0 {
		fail(c, http.StatusBadRequest, "invalid proxy heartbeat fields")
		return
	}

	now := time.Now().UTC()
	s.proxyRuntimeMu.Lock()
	s.proxyRuntime[heartbeat.ProxyID] = proxyRuntimeObservation{heartbeat: heartbeat, received: now}
	s.proxyRuntimeMu.Unlock()
	ok(c, gin.H{"received_at": now.Format(time.RFC3339)})
}

func (s *Server) proxyStatus(c *gin.Context) {
	now := time.Now().UTC()
	s.proxyRuntimeMu.RLock()
	statuses := make([]apitypes.ProxyRuntimeStatus, 0, len(s.proxyRuntime))
	for _, observation := range s.proxyRuntime {
		age := now.Sub(observation.received)
		if age < 0 {
			age = 0
		}
		status := "offline"
		if age < proxyOnlineAfter {
			status = "online"
		} else if age < proxyStaleAfter {
			status = "stale"
		}
		statuses = append(statuses, apitypes.ProxyRuntimeStatus{
			ProxyHeartbeat: observation.heartbeat,
			LastSeen:       observation.received.Format(time.RFC3339),
			AgeSeconds:     int64(age / time.Second),
			Status:         status,
		})
	}
	s.proxyRuntimeMu.RUnlock()

	sort.Slice(statuses, func(i, j int) bool { return statuses[i].ProxyID < statuses[j].ProxyID })
	online := 0
	for _, status := range statuses {
		if status.Status == "online" {
			online++
		}
	}
	ok(c, apitypes.ProxyStatusSummary{Online: online, Total: len(statuses), Proxies: statuses})
}
