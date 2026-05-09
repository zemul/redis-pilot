package server

import (
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

type backupScheduler struct {
	s    *Server
	cron *cron.Cron
	mu   sync.Mutex
	jobs map[string]scheduledJob // instance name → job info
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

// syncJobs 读取实例状态，对比当前 jobs，增删变更的 cron 任务。
func (bs *backupScheduler) syncJobs() {
	instances, err := bs.s.state.ReadInstances()
	if err != nil {
		bs.s.log.Errorf("backup scheduler: read instances: %v", err)
		return
	}

	// 构建期望状态：running 且有 schedule 的实例
	desired := make(map[string]string) // name → schedule
	if instances.Instances != nil {
		for name, inst := range instances.Instances {
			if inst.Status == "running" && inst.Backup != nil && inst.Backup.Schedule != "" {
				desired[name] = inst.Backup.Schedule
			}
		}
	}

	bs.mu.Lock()
	defer bs.mu.Unlock()

	// 删除不再需要的 job
	for name, job := range bs.jobs {
		if _, ok := desired[name]; !ok {
			bs.cron.Remove(job.entryID)
			delete(bs.jobs, name)
			bs.s.log.Infof("backup scheduler: removed job for %s", name)
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
		instName := name // capture for closure
		entryID, err := bs.cron.AddFunc(schedule, func() {
			bs.runBackup(instName)
		})
		if err != nil {
			bs.s.log.Errorf("backup scheduler: invalid cron %q for %s: %v", schedule, name, err)
			continue
		}
		bs.jobs[name] = scheduledJob{entryID: entryID, schedule: schedule}
		bs.s.log.Infof("backup scheduler: added job for %s schedule=%s", name, schedule)
	}
}

func (bs *backupScheduler) runBackup(name string) {
	if err := bs.s.execBackup(name); err != nil {
		bs.s.log.Errorf("backup scheduler: backup %s failed: %v", name, err)
	} else {
		bs.s.log.Infof("backup scheduler: backup %s completed", name)
	}
}
