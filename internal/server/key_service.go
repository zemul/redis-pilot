package server

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// 受限 K/V 浏览器的 Server 能力层（单一能力源）。HTTP handler 与后续 AI 工具都调这里，
// 逻辑不重复。读写都路由到实例组的当前 master（绕开 replica READONLY）。
// 设计见 KEY_BROWSER_PLAN.md。

// serverKvWriteOps 是写操作白名单（Server 侧防线，Agent 侧亦校验）。
var serverKvWriteOps = map[string]bool{
	"set": true, "hset": true, "push": true, "sadd": true, "zadd": true, "expire": true,
}

// resolveMaster 把任意实例名/从库名归一到其组的当前 master，并返回 master 所在节点。
// 标准（standalone）组的 CurrentMaster 即其唯一实例；组信息缺失时回退到 name 自身。
func (s *Server) resolveMaster(name string) (*apitypes.Instance, *apitypes.NodeServer, string, error) {
	instances, err := s.db.ReadInstances()
	if err != nil {
		return nil, nil, "", err
	}
	inst, exists := instances.Instances[name]
	if !exists {
		return nil, nil, "", fmt.Errorf("%w: %s", errInstanceNotFound, name)
	}
	masterName := name
	if inst.Group != "" {
		if g := instances.Groups[inst.Group]; g != nil && g.CurrentMaster != "" {
			masterName = g.CurrentMaster
		}
	}
	master := instances.Instances[masterName]
	if master == nil {
		master, masterName = inst, name
	}
	node, err := s.db.ReadNode()
	if err != nil {
		return nil, nil, "", err
	}
	srv := node.Servers[master.Server]
	if srv == nil {
		return nil, nil, "", fmt.Errorf("%w: %s", errServerNotFound, master.Server)
	}
	return master, srv, masterName, nil
}

// keyGet 读一个 key（路由到 master）。meta=true 只取 type/size/ttl。
func (s *Server) keyGet(name, key, cursor string, offset, count int, meta bool) (json.RawMessage, error) {
	_, srv, masterName, err := s.resolveMaster(name)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	q.Set("name", masterName)
	q.Set("key", key)
	if meta {
		q.Set("meta", "true")
	} else {
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		if count > 0 {
			q.Set("count", strconv.Itoa(count))
		}
		if offset > 0 {
			q.Set("offset", strconv.Itoa(offset))
		}
	}
	out, err := newAgentClient(srv).get("/instance/key/get?" + q.Encode())
	if err != nil {
		return nil, err
	}
	return extractAgentData(out), nil
}

// keyMeta 只取 key 的类型/大小/TTL（AI describe_key 与前端探类型用）。
func (s *Server) keyMeta(name, key string) (json.RawMessage, error) {
	return s.keyGet(name, key, "", 0, 0, true)
}

// keyWrite 按 op 写入（路由到 master）。op 白名单在此与 Agent 双重校验。
func (s *Server) keyWrite(name, key, op string, args apitypes.KeyWriteArgs) (json.RawMessage, error) {
	if !serverKvWriteOps[op] {
		return nil, fmt.Errorf("unsupported op: %s", op)
	}
	_, srv, masterName, err := s.resolveMaster(name)
	if err != nil {
		return nil, err
	}
	body := apitypes.KeyWriteRequest{Name: masterName, Key: key, Op: op, Args: args}
	out, err := newAgentClient(srv).post("/instance/key/write", body)
	if err != nil {
		return nil, err
	}
	return extractAgentData(out), nil
}

// keyDelete 删除整个 key（路由到 master）。
func (s *Server) keyDelete(name, key string) (json.RawMessage, error) {
	_, srv, masterName, err := s.resolveMaster(name)
	if err != nil {
		return nil, err
	}
	body := apitypes.KeyDeleteRequest{Name: masterName, Key: key}
	out, err := newAgentClient(srv).post("/instance/key/delete", body)
	if err != nil {
		return nil, err
	}
	return extractAgentData(out), nil
}
