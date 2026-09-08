package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSyncMetadataTaskIDValidation(t *testing.T) {
	for _, id := range []string{"a", "A-z_09", strings.Repeat("x", 64)} {
		if err := ValidateIsolatedSyncTaskID(id); err != nil {
			t.Fatalf("valid task ID rejected: %v", err)
		}
	}
	for _, id := range []string{"", strings.Repeat("x", 65), "任务", "a.b", "a:b", "a/b", "a{b}", "a*b", "a b", "a\n", "a\x00", "\xff"} {
		if err := ValidateIsolatedSyncTaskID(id); err == nil {
			t.Fatal("invalid task ID accepted")
		}
		if id != "" {
			// Invalid identity must fail before address/secret validation or dial.
			_, closeClient, err := (syncColdTarget{taskID: id}).open(context.Background())
			if err == nil || closeClient != nil || err.Error() != ValidateIsolatedSyncTaskID(id).Error() {
				t.Fatal("invalid identity reached target setup", err)
			}
		}
	}
	if _, err := (syncColdTarget{taskID: "task-a"}).apply(context.Background(), nil); err == nil {
		t.Fatal("named task used legacy v1 apply")
	}
	if _, err := (syncColdTarget{taskID: "task-a"}).applyPending(context.Background(), nil); err == nil {
		t.Fatal("named task used unbound single-file pending apply")
	}
}

func TestSyncMetadataKeysAndGlobalReservation(t *testing.T) {
	legacy := &syncColdClient{}
	if legacy.pendingCheckpointKey() != syncPendingCPKey || legacy.pendingKey() != syncPendingKey || legacy.anchorKey() != syncAnchorKey {
		t.Fatal("legacy metadata names changed")
	}
	seen := map[string]bool{}
	for _, id := range []string{"", "shard-a", "shard-b", strings.Repeat("x", 64)} {
		c := &syncColdClient{taskID: id}
		for _, suffix := range []string{"v2-checkpoint", "v2-pending", "v2-anchor"} {
			key := c.metadataKey(suffix)
			if seen[key] || !c.ownsMetadataKey(key) {
				t.Fatal("metadata keys overlap or are unowned")
			}
			seen[key] = true
			if id != "" && key != syncMetadataPrefix+"{task:"+id+"}:"+suffix {
				t.Fatal("unexpected task namespace")
			}
			for _, command := range [][]string{{"SET", key, "v"}, {"INCR", key}, {"DEL", key}} {
				if _, err := syncPendingCommand(encodeCommand(command...)); !errors.Is(err, errSyncUnsupported) {
					t.Fatal("business command can modify reserved task metadata", err)
				}
			}
			if err := checkSyncState(map[string]string{key: "v"}); !errors.Is(err, errSyncUnsupported) {
				t.Fatal("snapshot accepted reserved task metadata", err)
			}
		}
	}
}

type syncMetadataExchange struct {
	command []string
	reply   string
}

// Exact command transcripts are intentional: a stray legacy/other-task GET,
// PTTL, WATCH, SET or MSET fails even when it could return plausible values.
func syncMetadataServer(t *testing.T, sessions ...[]syncMetadataExchange) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for sessionIndex, exchanges := range sessions {
			conn, err := listener.Accept()
			if err != nil {
				done <- err
				return
			}
			reader, _ := NewReader(conn, DefaultConfig().Limits)
			conn.SetDeadline(time.Now().Add(3 * time.Second))
			for index, exchange := range exchanges {
				_, args, err := reader.ReadCommand()
				if err != nil {
					conn.Close()
					done <- fmt.Errorf("session %d command %d: %w", sessionIndex, index, err)
					return
				}
				got := make([]string, len(args))
				for i, arg := range args {
					got[i] = string(arg)
				}
				if !reflect.DeepEqual(got, exchange.command) {
					conn.Close()
					done <- fmt.Errorf("session %d command %d: got %q, want %q", sessionIndex, index, got, exchange.command)
					return
				}
				if err := writeAll(conn, []byte(exchange.reply)); err != nil {
					conn.Close()
					done <- err
					return
				}
			}
			conn.Close()
		}
		done <- nil
	}()
	t.Cleanup(func() {
		listener.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(4 * time.Second):
			t.Error("metadata server did not join")
		}
	})
	return listener.Addr().String()
}

func syncMetadataBulk(value string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value) }

func syncMetadataScanPage(keys ...string) string {
	reply := fmt.Sprintf("*2\r\n$1\r\n0\r\n*%d\r\n", len(keys))
	for _, key := range keys {
		reply += syncMetadataBulk(key)
	}
	return reply
}

func syncMetadataHandshake(c *syncColdClient, keys ...string) []syncMetadataExchange {
	info := "\r\nredis_version:7.0.0\r\ncluster_enabled:0\r\nrole:master\r\nkvrocks_version:2.15.0\r\n"
	exchanges := []syncMetadataExchange{{[]string{"INFO", "all"}, syncMetadataBulk(info)}}
	for _, setting := range []struct{ key, value string }{{"rocksdb.write_options.sync", "yes"}, {"rocksdb.write_options.disable_wal", "no"}} {
		exchanges = append(exchanges, syncMetadataExchange{[]string{"CONFIG", "GET", setting.key}, "*2\r\n" + syncMetadataBulk(setting.key) + syncMetadataBulk(setting.value)})
	}
	if c.taskID != "" {
		exchanges = append(exchanges, syncMetadataExchange{[]string{"SCAN", "0", "COUNT", "64", "MATCH", syncMetadataPrefix + "*"}, syncMetadataScanPage(keys...)})
	}
	return exchanges
}

func syncMetadataReads(c *syncColdClient, meta syncPendingMeta) []syncMetadataExchange {
	values := []string{"", "", ""}
	found := []bool{meta.checkpoint.Version != 0, meta.anchor != "", meta.pending != nil}
	if found[0] {
		values[0] = meta.checkpoint.encode()
	}
	values[1] = meta.anchor
	if found[2] {
		values[2] = meta.pending.encode()
	}
	var exchanges []syncMetadataExchange
	for i, key := range []string{c.pendingCheckpointKey(), c.anchorKey(), c.pendingKey()} {
		value, ttl := "$-1\r\n", ":-2\r\n"
		if found[i] {
			value, ttl = syncMetadataBulk(values[i]), ":-1\r\n"
		}
		exchanges = append(exchanges, syncMetadataExchange{[]string{"GET", key}, value}, syncMetadataExchange{[]string{"PTTL", key}, ttl})
	}
	return exchanges
}

func syncMetadataWatch(c *syncColdClient, key string) syncMetadataExchange {
	command := []string{"WATCH", c.pendingCheckpointKey(), c.pendingKey(), c.anchorKey()}
	if key != "" {
		command = append(command, key)
	}
	return syncMetadataExchange{command, "+OK\r\n"}
}

func syncMetadataTxn(reply string, commands ...[]string) []syncMetadataExchange {
	exchanges := []syncMetadataExchange{{[]string{"MULTI"}, "+OK\r\n"}}
	for _, command := range commands {
		exchanges = append(exchanges, syncMetadataExchange{command, "+QUEUED\r\n"})
	}
	return append(exchanges, syncMetadataExchange{[]string{"EXEC"}, reply})
}

func syncMetadataNamedFixture(t *testing.T, f *syncSegmentFixture, target syncColdTarget) {
	t.Helper()
	if target.taskID == "" {
		return
	}
	// Reuse the synthetic record sequence, not the legacy journal directory:
	// named journals must be created with identity, never silently adopted.
	identity := &syncTaskIdentity{TaskID: target.taskID, Source: f.records[0].Position.Source, Cold: target.address}
	store, err := openSyncSegmentsForTask(filepath.Join(t.TempDir(), "named"), 1024, 64<<10, defaultSyncResourcePolicy(), identity)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range f.records {
		if err := store.append(record); err != nil {
			store.close()
			t.Fatal(err)
		}
	}
	f.store.close()
	f.store = store
}

func TestSyncMetadataSegmentBootstrapResumeAndPruneCommands(t *testing.T) {
	for _, id := range []string{"", "shard-a", "shard-b"} {
		t.Run("task_"+id, func(t *testing.T) {
			f := newSyncSegmentFixture(t, 1024, 64<<10)
			f.append(t, 3)
			c := &syncColdClient{taskID: id}
			base := syncPendingMeta{checkpoint: f.points[0], anchor: f.points[0].encode()}
			committed := f.meta(t, 1)
			pending := committed
			pending.checkpoint = base.checkpoint
			key := "segment-key-0001"
			missing := []syncMetadataExchange{{[]string{"GET", key}, "$-1\r\n"}, {[]string{"PTTL", key}, ":-2\r\n"}}
			present := []syncMetadataExchange{{[]string{"GET", key}, syncMetadataBulk("vvv")}, {[]string{"PTTL", key}, ":-1\r\n"}}
			owned := []string{c.pendingCheckpointKey(), c.anchorKey(), c.pendingKey()}

			bootstrap := syncMetadataHandshake(c)
			bootstrap = append(bootstrap, syncMetadataWatch(c, ""))
			bootstrap = append(bootstrap, syncMetadataReads(c, syncPendingMeta{})...)
			bootstrap = append(bootstrap, syncMetadataExchange{[]string{"SCAN", "0", "COUNT", "64"}, syncMetadataScanPage()})
			bootstrap = append(bootstrap, syncMetadataTxn("*1\r\n+OK\r\n", []string{"MSET", c.pendingCheckpointKey(), base.checkpoint.encode(), c.anchorKey(), base.anchor})...)
			bootstrap = append(bootstrap, syncMetadataReads(c, base)...)
			bootstrap = append(bootstrap, syncMetadataWatch(c, key))
			bootstrap = append(bootstrap, syncMetadataReads(c, base)...)
			bootstrap = append(bootstrap, missing...)
			bootstrap = append(bootstrap, syncMetadataTxn("*1\r\n+OK\r\n", []string{"SET", c.pendingKey(), pending.pending.encode()})...)
			bootstrap = append(bootstrap, syncMetadataWatch(c, key))
			bootstrap = append(bootstrap, syncMetadataReads(c, pending)...)
			bootstrap = append(bootstrap, missing...)
			bootstrap = append(bootstrap, syncMetadataTxn("*2\r\n+OK\r\n+OK\r\n", []string{"SET", key, "vvv"}, []string{"SET", c.pendingCheckpointKey(), committed.checkpoint.encode()})...)
			bootstrap = append(bootstrap, syncMetadataReads(c, committed)...)
			bootstrap = append(bootstrap, present...)

			resume := syncMetadataHandshake(c, owned...)
			resume = append(resume, syncMetadataWatch(c, ""))
			resume = append(resume, syncMetadataReads(c, committed)...)
			resume = append(resume, syncMetadataExchange{[]string{"UNWATCH"}, "+OK\r\n"}, syncMetadataWatch(c, key))
			resume = append(resume, syncMetadataReads(c, committed)...)
			resume = append(resume, present...)
			resume = append(resume, syncMetadataExchange{[]string{"UNWATCH"}, "+OK\r\n"})

			prune := syncMetadataHandshake(c, owned...)
			prune = append(prune, syncMetadataReads(c, committed)...)
			prune = append(prune, syncMetadataWatch(c, key))
			prune = append(prune, syncMetadataReads(c, committed)...)
			prune = append(prune, present...)
			prune = append(prune, syncMetadataTxn("*1\r\n+PONG\r\n", []string{"PING"})...)

			target := syncColdTarget{address: syncMetadataServer(t, bootstrap, resume, prune), timeout: time.Second, taskID: id}
			syncMetadataNamedFixture(t, f, target)
			for pass := 0; pass < 2; pass++ {
				position, err := target.applySegments(context.Background(), f.store)
				if err != nil || position != committed.checkpoint.Position {
					t.Fatalf("apply pass %d: %v", pass, err)
				}
			}
			if err := target.pruneSegments(context.Background(), f.store); err != nil {
				t.Fatal(err)
			}
			if f.store.manifest.Confirmed != committed.checkpoint {
				t.Fatal("prune did not confirm the named checkpoint")
			}
		})
	}
}

func TestSyncMetadataForeignNamespaceRefusesConnectionAndBootstrap(t *testing.T) {
	c := &syncColdClient{taskID: "shard-a"}
	for _, key := range []string{syncPendingCPKey, syncCheckpointKey, (&syncColdClient{taskID: "shard-b"}).pendingKey(), syncMetadataPrefix + "unknown"} {
		t.Run(key, func(t *testing.T) {
			address := syncMetadataServer(t, syncMetadataHandshake(c, key))
			client, closeClient, err := (syncColdTarget{address: address, timeout: time.Second, taskID: c.taskID}).open(context.Background())
			if !errors.Is(err, errSyncTargetDrift) || client != nil || closeClient != nil {
				t.Fatal("foreign namespace accepted", err)
			}
		})
	}
	// A namespace-filtered metadata check cannot authorize bootstrapping over
	// unrelated business keys; the original unfiltered DB scan must remain.
	exchanges := syncMetadataHandshake(c)
	exchanges = append(exchanges, syncMetadataWatch(c, ""))
	exchanges = append(exchanges, syncMetadataReads(c, syncPendingMeta{})...)
	exchanges = append(exchanges, syncMetadataExchange{[]string{"SCAN", "0", "COUNT", "64"}, syncMetadataScanPage("unrelated-business-key")})
	f := newSyncSegmentFixture(t, 1024, 64<<10)
	target := syncColdTarget{address: syncMetadataServer(t, exchanges), timeout: time.Second, taskID: c.taskID}
	syncMetadataNamedFixture(t, f, target)
	if _, err := target.applySegments(context.Background(), f.store); !errors.Is(err, errSyncTargetDrift) {
		t.Fatal("metadata namespace bypassed the whole-DB empty gate", err)
	}
}

func TestSyncMetadataScanLimitsIncludeLongNamedKeys(t *testing.T) {
	c := &syncColdClient{taskID: strings.Repeat("x", 64)}
	keys := []string{c.pendingCheckpointKey(), c.pendingKey(), c.anchorKey()}
	remaining := syncStateBytes
	for i := 0; i < syncStateKeys; i++ {
		key := fmt.Sprintf("%03d", i) + strings.Repeat("k", remaining/(syncStateKeys-i)-3)
		keys = append(keys, key)
		remaining -= len(key)
	}
	for _, extra := range []bool{false, true} {
		t.Run(fmt.Sprintf("over_limit_%v", extra), func(t *testing.T) {
			page := append([]string{}, keys...)
			if extra {
				page = append(page, "one-extra-business-key")
			}
			address := syncMetadataServer(t, []syncMetadataExchange{{[]string{"SCAN", "0", "COUNT", "64"}, syncMetadataScanPage(page...)}})
			conn, err := net.Dial("tcp", address)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			reader, _ := NewReader(conn, DefaultConfig().Limits)
			client := &syncColdClient{conn: conn, reader: reader, timeout: time.Second, taskID: c.taskID}
			got, err := client.keys()
			if extra {
				if !errors.Is(err, errSyncTargetDrift) {
					t.Fatal("scan key-count limit was not enforced", err)
				}
			} else if err != nil || len(got) != syncStateKeys+3 {
				t.Fatal("bounded named metadata reduced the permitted business-key budget", err)
			}
		})
	}
}
