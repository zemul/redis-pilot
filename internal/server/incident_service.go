package server

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/store"
)

// 去抖阈值：周期来源(reconcile/topology)连续观测多少次才确认异常/恢复。
// 任务来源(备份/恢复失败)是一次性终态事件，不走去抖，直接 Report。
const (
	incidentConfirmFailures   = 3 // 连续 N 次异常 → 写 open
	incidentConfirmRecoveries = 2 // open 期间连续 N 次正常 → 写 resolved
	incidentEvidenceMaxBytes  = 16 * 1024
)

// Observation 是检测器提交给 IncidentService 的一次观测。
// 检测器不直接碰数据库，统一走 Observe/Report/Resolve。
type Observation struct {
	Type         string         // incident_type，稳定枚举
	Active       bool           // 周期来源用：true=这一轮观测到异常，false=这一轮正常
	Severity     string         // warning|critical
	Source       string         // reconcile|topology|task
	ObjectType   string         // instance|group|node
	ObjectID     string         // 对象标识（fingerprint 的一部分）
	GroupName    string         // 冗余存储，便于按组筛选
	InstanceName string         // 冗余存储，便于按实例筛选
	NodeName     string         // 冗余存储，便于按节点筛选
	Summary      string         // 一句话描述
	Reason       string         // 检测依据（已确认的事实）
	Evidence     map[string]any // 结构化证据（期望值/实际值等），脱敏后存
}

func (o Observation) fingerprint() string {
	return fmt.Sprintf("%s:%s:%s", o.Type, o.ObjectType, o.ObjectID)
}

// IncidentService 记录异常生命周期。去抖计数放内存(counters)，只有确认后才落库。
// Server 重启后计数从 0 重来，已 open 的记录留在库里靠唯一索引不会重复建。
type IncidentService struct {
	db  *store.DB
	log incidentLogger

	mu       sync.Mutex
	counters map[string]*debounceState // key = fingerprint
}

// incidentLogger 是 IncidentService 需要的最小日志接口(便于测试注入)。
type incidentLogger interface {
	Errorf(format string, args ...interface{})
	Infof(format string, args ...interface{})
}

type debounceState struct {
	failCount  int
	recovCount int
}

func newIncidentService(db *store.DB, log incidentLogger) *IncidentService {
	return &IncidentService{
		db:       db,
		log:      log,
		counters: make(map[string]*debounceState),
	}
}

func (s *IncidentService) state(fp string) *debounceState {
	st := s.counters[fp]
	if st == nil {
		st = &debounceState{}
		s.counters[fp] = st
	}
	return st
}

// Observe 处理周期来源(reconcile/topology)的一次观测，带去抖。
//   - obs.Active=true：连续 incidentConfirmFailures 次后写一条 open；已 open 则刷新观测。
//   - obs.Active=false：open 期间连续 incidentConfirmRecoveries 次后标记 resolved。
//
// “这一轮没有可信数据”的情形调用方不应传入 Active=false，否则会被当成恢复。
func (s *IncidentService) Observe(obs Observation) {
	fp := obs.fingerprint()
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.state(fp)

	if obs.Active {
		st.recovCount = 0
		st.failCount++
		now := time.Now().Format(time.RFC3339)
		evidence := s.encodeEvidence(obs.Evidence)

		active, err := s.db.GetActiveIncident(fp)
		if err != nil {
			s.log.Errorf("incident: query active %s: %v", fp, err)
			return
		}
		if active != nil {
			// 已确认异常：刷新最近观测/证据/级别。
			if err := s.db.TouchIncident(fp, now, evidence, obs.Severity); err != nil {
				s.log.Errorf("incident: touch %s: %v", fp, err)
			}
			return
		}
		if st.failCount < incidentConfirmFailures {
			return // 还在去抖窗口内，不落库
		}
		// 达到确认阈值，写一条 open。
		if err := s.db.CreateIncident(s.build(obs, now, evidence)); err != nil {
			// 唯一索引冲突：另一检测器已并发建过，忽略。
			s.log.Errorf("incident: create %s: %v", fp, err)
		}
		return
	}

	// 这一轮正常。
	st.failCount = 0
	active, err := s.db.GetActiveIncident(fp)
	if err != nil {
		s.log.Errorf("incident: query active %s: %v", fp, err)
		return
	}
	if active == nil {
		// 没有 open 记录（异常在确认前就恢复了）：清计数，不进历史。
		st.recovCount = 0
		delete(s.counters, fp)
		return
	}
	st.recovCount++
	if st.recovCount < incidentConfirmRecoveries {
		return
	}
	if _, err := s.db.ResolveIncident(fp, time.Now().Format(time.RFC3339)); err != nil {
		s.log.Errorf("incident: resolve %s: %v", fp, err)
		return
	}
	delete(s.counters, fp)
}

// Report 记录一次任务型异常(备份/恢复失败)，立即 open，不去抖。
// 同 fingerprint 已 open 时只刷新观测。
func (s *IncidentService) Report(obs Observation) {
	fp := obs.fingerprint()
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Format(time.RFC3339)
	evidence := s.encodeEvidence(obs.Evidence)

	active, err := s.db.GetActiveIncident(fp)
	if err != nil {
		s.log.Errorf("incident: query active %s: %v", fp, err)
		return
	}
	if active != nil {
		if err := s.db.TouchIncident(fp, now, evidence, obs.Severity); err != nil {
			s.log.Errorf("incident: touch %s: %v", fp, err)
		}
		return
	}
	if err := s.db.CreateIncident(s.build(obs, now, evidence)); err != nil {
		s.log.Errorf("incident: create %s: %v", fp, err)
	}
}

// Resolve 结束一条任务型异常(后续同类任务成功时调用)。无 open 记录时静默返回。
func (s *IncidentService) Resolve(incidentType, objectType, objectID string) {
	fp := fmt.Sprintf("%s:%s:%s", incidentType, objectType, objectID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.db.ResolveIncident(fp, time.Now().Format(time.RFC3339)); err != nil {
		s.log.Errorf("incident: resolve %s: %v", fp, err)
		return
	}
	delete(s.counters, fp)
}

func (s *IncidentService) build(obs Observation, now, evidence string) store.Incident {
	severity := obs.Severity
	if severity == "" {
		severity = "warning"
	}
	return store.Incident{
		ID:              fmt.Sprintf("incident-%d", time.Now().UnixNano()),
		Fingerprint:     obs.fingerprint(),
		IncidentType:    obs.Type,
		Status:          "open",
		Severity:        severity,
		Source:          obs.Source,
		ObjectType:      obs.ObjectType,
		ObjectID:        obs.ObjectID,
		GroupName:       obs.GroupName,
		InstanceName:    obs.InstanceName,
		NodeName:        obs.NodeName,
		Summary:         obs.Summary,
		Reason:          obs.Reason,
		Evidence:        evidence,
		FirstObservedAt: now,
		LastSeenAt:      now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

// encodeEvidence 把证据序列化为 JSON，超出上限则丢弃(记日志)，避免大对象入库。
func (s *IncidentService) encodeEvidence(ev map[string]any) string {
	if len(ev) == 0 {
		return "{}"
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return "{}"
	}
	if len(b) > incidentEvidenceMaxBytes {
		s.log.Errorf("incident: evidence too large (%d bytes), dropped", len(b))
		return "{}"
	}
	return string(b)
}

// CleanupResolved 删除超过保留期的历史记录，供定时循环调用。
func (s *IncidentService) CleanupResolved(retention time.Duration) {
	if retention <= 0 {
		retention = 180 * 24 * time.Hour
	}
	before := time.Now().Add(-retention).Format(time.RFC3339)
	for {
		n, err := s.db.CleanupResolvedIncidents(before, 500)
		if err != nil {
			s.log.Errorf("incident: cleanup: %v", err)
			return
		}
		if n < 500 {
			return
		}
	}
}
