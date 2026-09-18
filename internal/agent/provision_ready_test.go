package agent

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/proxy/resp"
)

func TestProvisionDatabaseNativeRedis(t *testing.T) {
	testProvisionNativeRedis(t, "redis-cache")
}

func TestProvisionDatabaseNativeRedisStorage(t *testing.T) {
	testProvisionNativeRedis(t, "redis-storage")
}

func testProvisionNativeRedis(t *testing.T, profile string) {
	t.Helper()
	binary := os.Getenv("PILOT_TEST_REDIS_SERVER")
	if binary == "" {
		t.Skip("set PILOT_TEST_REDIS_SERVER for native engine readiness test")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	aof, policy := "no", "allkeys-lru"
	if profile == "redis-storage" {
		aof, policy = "yes", "noeviction"
	}
	p := exec.Command(binary, "--bind", "127.0.0.1", "--port", strconv.Itoa(port), "--dir", t.TempDir(), "--save", "", "--appendonly", aof, "--appendfsync", "always", "--maxmemory", "16mb", "--maxmemory-policy", policy)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { p.Process.Kill(); p.Wait() }()
	for until := time.Now().Add(5 * time.Second); ; {
		err = probeProvisionDatabase(addr, profile)
		if err == nil {
			break
		}
		if time.Now().After(until) {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	if _, err := fmt.Fprint(c, "*3\r\n$3\r\nSET\r\n$5\r\nowned\r\n$1\r\nx\r\n"); err != nil {
		t.Fatal(err)
	}
	r := resp.NewReader(c)
	frame, err := r.ReadFrame()
	if err != nil || string(frame) != "+OK\r\n" {
		t.Fatalf("%q %v", frame, err)
	}
	if err := probeProvisionDatabase(addr, profile); err == nil {
		t.Fatal("nonempty real database accepted")
	}
	if _, err := fmt.Fprint(c, "*2\r\n$3\r\nGET\r\n$5\r\nowned\r\n"); err != nil {
		t.Fatal(err)
	}
	frame, err = r.ReadFrame()
	if err != nil || string(frame) != "$1\r\nx\r\n" {
		t.Fatal("readiness check modified data", err)
	}
}

func TestProvisionDatabaseReadiness(t *testing.T) {
	for _, mode := range []string{"redis", "kvrocks", "loading", "replica", "nonempty", "other-db", "bad-policy", "wal-off", "noauth", "bad-config", "wrong-engine", "redis-storage", "redis-storage-async", "redis-storage-eviction"} {
		t.Run(mode, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			done := make(chan struct{})
			defer func() { <-done }()
			go func() {
				defer close(done)
				c, err := ln.Accept()
				if err != nil {
					return
				}
				defer c.Close()
				r := resp.NewReader(c)
				r.SetMaxBytes(4096)
				for {
					_, args, err := r.ReadCommand()
					if err != nil {
						return
					}
					a := make([]string, len(args))
					for i := range args {
						a[i] = string(args[i])
					}
					switch a[0] {
					case "PING":
						if mode == "noauth" {
							fmt.Fprint(c, "-NOAUTH required\r\n")
						} else {
							fmt.Fprint(c, "+PONG\r\n")
						}
					case "INFO":
						info := "redis_version:7.2.5\r\nrole:master\r\ncluster_enabled:0\r\nloading:0\r\n"
						if mode == "kvrocks" || mode == "wal-off" || mode == "wrong-engine" {
							info += "kvrocks_version:2.15.0\r\n"
						}
						if mode == "loading" {
							info = strings.ReplaceAll(info, "loading:0", "loading:1")
						}
						if mode == "replica" {
							info = strings.ReplaceAll(info, "role:master", "role:slave")
						}
						if mode == "other-db" {
							info += "db1:keys=1,expires=0\r\n"
						}
						fmt.Fprintf(c, "$%d\r\n%s\r\n", len(info), info)
					case "CONFIG":
						if mode == "bad-config" {
							fmt.Fprint(c, "*0\r\n")
							continue
						}
						values := map[string]string{"save": "", "appendonly": "no", "maxmemory": "1000", "maxmemory-policy": "allkeys-lru", "rocksdb.write_options.sync": "yes", "rocksdb.write_options.disable_wal": "no", "workers": "2", "max-db-size": "0"}
						if strings.HasPrefix(mode, "redis-storage") {
							values["appendonly"], values["appendfsync"], values["no-appendfsync-on-rewrite"], values["maxmemory-policy"] = "yes", "always", "no", "noeviction"
							if mode == "redis-storage-async" {
								values["appendfsync"] = "everysec"
							}
							if mode == "redis-storage-eviction" {
								values["maxmemory-policy"] = "allkeys-lru"
							}
						}
						if mode == "bad-policy" {
							values["maxmemory-policy"] = "noeviction"
						}
						if mode == "wal-off" {
							values["rocksdb.write_options.disable_wal"] = "yes"
						}
						key := a[2]
						value := values[key]
						fmt.Fprintf(c, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
					case "DBSIZE":
						if mode == "nonempty" {
							fmt.Fprint(c, ":1\r\n")
						} else {
							fmt.Fprint(c, ":0\r\n")
						}
					default:
						t.Errorf("unexpected mutation/probe: %v", a)
						return
					}
				}
			}()
			profile := "redis-cache"
			if strings.HasPrefix(mode, "redis-storage") {
				profile = "redis-storage"
			}
			if mode == "kvrocks" || mode == "wal-off" {
				profile = "kvrocks-storage"
			}
			err = probeProvisionDatabase(ln.Addr().String(), profile)
			if (mode == "redis" || mode == "kvrocks" || mode == "redis-storage") != (err == nil) {
				t.Fatalf("unexpected readiness result: %v", err)
			}
		})
	}
}
