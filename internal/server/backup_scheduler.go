package server

import (
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/state"
)

type backupScheduler struct {
	s    *Server
	cron *cron.Cron
	mu   sync.Mutex
	jobs map[string]scheduledJob // group name → job info
}

type scheduledJob struct {
	entryID  cron.EntryID
	schedule string
}

func newBackupScheduler(s *Server) *backupScheduler {
	return &backupScheduler{
		s:    s,
		cron: cron.New(),
		jobs: make(map[string]scheduledJob),
	}
}

// Start 启动 cron 调度器和同步 goroutine。
func (bs *backupScheduler) Start() {
	bs.cron.Start()
	go func() {
		// 启动时立即同步一次
		bs.syncJobs()
		for range time.Tick(60 * time.Second) {
			bs.syncJobs()
		}
	}()
}

// syncJobs 读取组状态，对比当前 jobs，增删变更的 cron 任务。
// 备份策略挂在组上（group.Backup）：对配置了 schedule 且组内有可服务实例的组建 job。
func (bs *backupScheduler) syncJobs() {
	instances, err := bs.s.db.ReadInstances()
	if err != nil {
		bs.s.log.Errorf("backup scheduler: read instances: %v", err)
		return
	}

	// 构建期望状态：配置了 schedule 且组内存在可服务实例的组。
	// 判据必须是观测：组内没有任何 serving 实例时，触发备份也会失败（pickBackupExecInstance
	// 会返回空），因此本轮不建 job，等实例恢复后下一轮再补。
	desired := make(map[string]string) // group name → schedule
	for groupName, group := range instances.Groups {
		if group == nil || group.Backup == nil || group.Backup.Schedule == "" {
			continue
		}
		if state.PickBackupExecInstance(groupName, instances) == "" {
			continue
		}
		desired[groupName] = group.Backup.Schedule
	}

	bs.mu.Lock()
	defer bs.mu.Unlock()

	// 删除不再需要的 job
	for name, job := range bs.jobs {
		if _, ok := desired[name]; !ok {
			bs.cron.Remove(job.entryID)
			delete(bs.jobs, name)
			bs.s.log.Infof("backup scheduler: removed job for group %s", name)
		}
	}

	// 新增或更新 job
	for name, schedule := range desired {
		if existing, ok := bs.jobs[name]; ok {
			if existing.schedule == schedule {
				continue // 没变化
			}
			// schedule 变了，先删后加
			bs.cron.Remove(existing.entryID)
			delete(bs.jobs, name)
		}
		groupName := name // capture for closure
		entryID, err := bs.cron.AddFunc(schedule, func() {
			bs.runBackup(groupName)
		})
		if err != nil {
			bs.s.log.Errorf("backup scheduler: invalid cron %q for group %s: %v", schedule, name, err)
			continue
		}
		bs.jobs[name] = scheduledJob{entryID: entryID, schedule: schedule}
		bs.s.log.Infof("backup scheduler: added job for group %s schedule=%s", name, schedule)
	}
}

func (bs *backupScheduler) runBackup(groupName string) {
	taskID, err := bs.s.execBackupForGroup(groupName, "system")
	if err != nil {
		bs.s.log.Errorf("backup scheduler: backup group %s failed: %v", groupName, err)
	} else {
		bs.s.log.Infof("backup scheduler: backup group %s triggered, task_id=%s", groupName, taskID)
	}
}
