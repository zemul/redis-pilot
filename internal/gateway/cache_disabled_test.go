package gateway

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCacheDisabledConfigAndState(t *testing.T) {
	c := shardConfigForTest(t)
	disabled := false
	c.Cache.Enabled, c.RedisAddress, c.BackendPasswordFile = &disabled, "", ""
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	s, err := NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	endpoints, err := s.cache.shardEndpoints()
	if err != nil {
		t.Fatal(err)
	}
	topology, _ := c.Cache.Sharding.registry()
	if _, err := newCacheShards(s.cache, topology, endpoints); err != nil {
		t.Fatal(err)
	}
	state, err := openShardDisk(context.Background(), c, true)
	if err != nil {
		t.Fatal(err)
	}
	state.close()
	state, err = openShardDisk(context.Background(), c, false)
	if err != nil {
		t.Fatal(err)
	}
	state.close()
	enabled := true
	c.Cache.Enabled, c.RedisAddress = &enabled, "127.0.0.1:17100"
	if state, err := openShardDisk(context.Background(), c, false); err == nil {
		state.close()
		t.Fatal("mode changed existing state")
	}
	c.Cache.Enabled = &disabled
	if c.Validate() == nil {
		t.Fatal("disabled cache accepted hot address")
	}
	c.RedisAddress, c.BackendPasswordFile = "", "/secret"
	if c.Validate() == nil {
		t.Fatal("disabled cache accepted hot credential")
	}
}

func TestCacheDisabledFiveTypes(t *testing.T) {
	disabled := false
	c := shardConfigForTest(t)
	c.RedisAddress, c.BackendPasswordFile = "", ""
	c.Cache.Enabled, c.Cache.Sharding = &disabled, nil
	c.Cache.ColdAddress = os.Getenv("GATEWAY_TEST_CACHE_COLD_ADDRESS")
	password := os.Getenv("GATEWAY_TEST_CACHE_COLD_PASSWORD")
	if c.Cache.ColdAddress == "" {
		c.Cache.ColdAddress, password = migrationCold(t), "g4-backend-test"
	}
	if password != "" {
		c.Cache.ColdPasswordFile = secretFile(t, password)
	}
	c.Cache.LockFile = filepath.Join(filepath.Dir(c.Cache.LockFile), "disabled.lock")
	s, err := NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.cache.start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.cache.close)
	for _, command := range []struct {
		want string
		args []string
	}{
		{"+OK\r\n", []string{"SET", "disabled:s", "new"}},
		{"$3\r\nnew\r\n", []string{"GET", "disabled:s"}},
		{":1\r\n", []string{"HSET", "disabled:h", "f", "v"}},
		{"$1\r\nv\r\n", []string{"HGET", "disabled:h", "f"}},
		{":1\r\n", []string{"RPUSH", "disabled:l", "v"}},
		{"$1\r\nv\r\n", []string{"LPOP", "disabled:l"}},
		{":1\r\n", []string{"SADD", "disabled:set", "v"}},
		{":1\r\n", []string{"SISMEMBER", "disabled:set", "v"}},
		{":1\r\n", []string{"ZADD", "disabled:z", "1", "v"}},
		{"$1\r\n1\r\n", []string{"ZSCORE", "disabled:z", "v"}},
		{"+OK\r\n", []string{"MSET", "disabled:s", "latest", "disabled:s2", "x"}},
		{"*2\r\n$6\r\nlatest\r\n$1\r\nx\r\n", []string{"MGET", "disabled:s", "disabled:s2"}},
	} {
		cacheExec(t, s, command.want, command.args...)
	}
	s.cache.recoverHot(context.Background())
	s.cache.hotFailed()
	if s.cache.hotReady.Load() || s.cache.hotFailures != 0 {
		t.Fatal("disabled cache entered hot recovery")
	}
	if _, err := s.cache.connect(context.Background(), true); err == nil {
		t.Fatal("disabled hot connection allowed")
	}
	cacheExec(t, s, ":5\r\n", "DEL", "disabled:s", "disabled:s2", "disabled:h", "disabled:set", "disabled:z")
}
