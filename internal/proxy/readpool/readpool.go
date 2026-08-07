// Package readpool 实现读池的加权随机选择。
//
// 读池成员 = master + 所有健康 replica（§9）。
// 按 weight 加权随机选择；所有 replica 不可用时回落 master（D10）。
package readpool

import (
	"sync"
	"sync/atomic"

	"math/rand/v2"
)

// Role 标识后端角色。
type Role string

const (
	RoleMaster  Role = "master"
	RoleReplica Role = "replica"
)

// Entry 是读池的一个成员。
type Entry struct {
	Addr   string
	Weight int
	Role   Role
}

// Pool 是加权随机选择器。
// 线程安全：Select 和 MarkDown 可并发调用。
type Pool struct {
	entries []Entry
	down    sync.Map // addr -> struct{}{} 标记不可用
	total   atomic.Int64
}

// New 创建读池。master 必须在 entries 中。
func New(entries []Entry) *Pool {
	p := &Pool{entries: entries}
	p.recomputeTotal()
	return p
}

// Select 按权重随机选择一个可用后端。
// 所有 replica 不可用时回落 master（D10）。
func (p *Pool) Select() Entry {
	total := p.total.Load()
	if total <= 0 {
		// 所有权重为零或全部 down，回落 master
		return p.masterEntry()
	}

	n := rand.Int64() % total
	for _, e := range p.entries {
		if p.isDown(e.Addr) || e.Weight <= 0 {
			continue
		}
		n -= int64(e.Weight)
		if n < 0 {
			return e
		}
	}
	// 理论上不会到达（total 与 entries 一致）
	return p.masterEntry()
}

// MarkDown 标记一个后端不可用（replica 故障）。
func (p *Pool) MarkDown(addr string) {
	p.down.Store(addr, struct{}{})
	p.recomputeTotal()
}

// HasAvailableReplica 返回是否存在可用的 replica（weight>0 且未 down）。
func (p *Pool) HasAvailableReplica() bool {
	for _, e := range p.entries {
		if e.Role == RoleReplica && e.Weight > 0 && !p.isDown(e.Addr) {
			return true
		}
	}
	return false
}

func (p *Pool) isDown(addr string) bool {
	_, ok := p.down.Load(addr)
	return ok
}

func (p *Pool) masterEntry() Entry {
	for _, e := range p.entries {
		if e.Role == RoleMaster {
			return e
		}
	}
	// 没有 master 标签，返回第一个（不应发生）
	if len(p.entries) > 0 {
		return p.entries[0]
	}
	return Entry{}
}

func (p *Pool) recomputeTotal() {
	total := int64(0)
	for _, e := range p.entries {
		if !p.isDown(e.Addr) && e.Weight > 0 {
			total += int64(e.Weight)
		}
	}
	p.total.Store(total)
}
