package gateway

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway/sharding"
)

func shardTestTopology(t *testing.T, count int, revision uint64) *sharding.Topology {
	t.Helper()
	c := sharding.Config{Algorithm: sharding.Algorithm, Revision: revision}
	for i := 0; i < count; i++ {
		c.Nodes = append(c.Nodes, sharding.Node{ID: fmt.Sprint(i), Address: fmt.Sprintf("127.0.0.1:%d", 16000+i)})
	}
	x, err := sharding.New(c)
	if err != nil {
		t.Fatal(err)
	}
	return x
}

func shardTestDifferentKeys(t *testing.T, x *sharding.Topology) (string, string) {
	t.Helper()
	a := "key-0"
	for i := 1; i < 1000; i++ {
		b := fmt.Sprintf("key-%d", i)
		if x.Owner([]byte(a)).ID != x.Owner([]byte(b)).ID {
			return a, b
		}
	}
	t.Fatal("could not find two owners")
	return "", ""
}

func TestShardCommandPlans(t *testing.T) {
	r := &cacheRuntime{}
	x := shardTestTopology(t, 3, 1)
	a, b := shardTestDifferentKeys(t, x)
	for _, args := range [][]string{
		{"MGET", a, b, a}, {"EXISTS", a, b, a}, {"DEL", a, b, a}, {"UNLINK", a, b, a}, {"MSET", a, "first", b, "second", a, "last"},
	} {
		plan, err := r.planCacheCommand(x, nil, cacheArgs(args...))
		if err != nil || plan.moves || len(plan.groups) != 2 || len(plan.keys) != 3 {
			t.Fatalf("%v: %+v %v", args, plan, err)
		}
		if !reflect.DeepEqual(plan.groups[0].positions, []int{0, 2}) || !reflect.DeepEqual(plan.groups[1].positions, []int{1}) {
			t.Fatal("operand order lost", plan)
		}
		if plan.fromRevision != 1 || plan.toRevision != 1 {
			t.Fatal("wrong revisions")
		}
	}
	for _, args := range [][]string{
		{"GET", a}, {"HSET", a, "field", "value"}, {"LPUSH", a, "item"}, {"SADD", a, "member"}, {"ZADD", a, "1", "member"},
		{"HMGET", a, b, "other-field"}, {"HSCAN", a, "0"},
	} {
		plan, err := r.planCacheCommand(x, nil, cacheArgs(args...))
		if err != nil || len(plan.keys) != 1 || len(plan.groups) != 1 || plan.keys[0].key != a {
			t.Fatalf("single key %v: %v", args, err)
		}
	}
	for _, args := range [][]string{
		{"MSETNX", a, "1", b, "2"}, {"RENAME", a, b}, {"RENAMENX", a, b}, {"SMOVE", a, b, "x"},
		{"LMOVE", a, b, "LEFT", "RIGHT"}, {"RPOPLPUSH", a, b}, {"SUNION", a, b},
		{"ZINTER", "2", a, b}, {"SINTERCARD", "2", a, b}, {"SUNIONSTORE", a, b},
		{"ZUNIONSTORE", a, "1", b}, {"ZRANGESTORE", a, b, "0", "-1"},
		{"EVAL", "return 1", "2", a, b}, {"EVALSHA", strings.Repeat("a", 40), "2", a, b},
	} {
		if _, err := r.planCacheCommand(x, nil, cacheArgs(args...)); !errors.Is(err, errShardCrossNode) {
			t.Fatalf("cross-node %v: %v", args, err)
		}
		// The same operation remains allowed if all key operands co-locate.
		copyArgs := append([]string(nil), args...)
		for i := 1; i < len(copyArgs); i++ {
			if copyArgs[i] == a {
				copyArgs[i] = "a{account}:x"
			}
			if copyArgs[i] == b {
				copyArgs[i] = "b{account}:y"
			}
		}
		if p, err := r.planCacheCommand(x, nil, cacheArgs(copyArgs...)); err != nil || len(p.groups) != 1 {
			t.Fatalf("same-tag %v: %v", copyArgs, err)
		}
	}
}

func TestShardPlanPreflight(t *testing.T) {
	r := &cacheRuntime{}
	x := shardTestTopology(t, 3, 1)
	for _, args := range [][]string{
		{}, {"UNKNOWN", "key"}, {"MSET", "a", "1", "b"}, {"MSET", "a", "1", cachePrefix + "owner", "2"},
		{"MGET", "a", strings.Repeat("k", 4097)}, {"EVAL", "return 1", "-1"}, {"EVAL", "return 1", "1", cachePrefix + "owner"},
		{"ZUNIONSTORE", "dest", "99", "a"}, {"BLPOP", "queue:a", "1"},
	} {
		p, err := r.planCacheCommand(x, nil, cacheArgs(args...))
		if !errors.Is(err, errShardCommand) || len(p.keys) != 0 || len(p.groups) != 0 {
			t.Fatalf("invalid preflight %v: %+v %v", args, p, err)
		}
	}
	for _, args := range [][]string{{"SCAN", "0"}, {"KEYS", "*"}, {"EVAL", "return 1", "0"}, {"PING"}, {"ECHO", "hello"}} {
		if _, err := r.planCacheCommand(x, nil, cacheArgs(args...)); !errors.Is(err, errShardGlobal) {
			t.Fatalf("global/keyless %v: %v", args, err)
		}
	}
	if _, err := r.planCacheCommand(nil, nil, cacheArgs("GET", "key")); err == nil {
		t.Fatal("nil topology accepted")
	}
	if _, err := r.planCacheCommand(&sharding.Topology{}, nil, cacheArgs("GET", "key")); err == nil {
		t.Fatal("unvalidated topology accepted")
	}
	if _, err := r.planCacheCommand(x, &sharding.Transition{}, cacheArgs("GET", "key")); err == nil {
		t.Fatal("unvalidated transition accepted")
	}
	args := cacheArgs("MGET", "\x00\xff", "")
	p, err := r.planCacheCommand(x, nil, args)
	if err != nil || p.keys[0].key != "\x00\xff" || p.keys[1].key != "" {
		t.Fatal("binary/empty key", err)
	}
	args[1][0] = 'x'
	if p.keys[0].key != "\x00\xff" {
		t.Fatal("plan retained mutable key input")
	}
}

func TestShardTransactionPlans(t *testing.T) {
	r := &cacheRuntime{}
	x := shardTestTopology(t, 3, 1)
	a, b := shardTestDifferentKeys(t, x)
	for _, queue := range [][][][]byte{
		{cacheArgs("SET", a, "1"), cacheArgs("SET", b, "2")},
		{cacheArgs("MSET", a, "1", b, "2")},
		{cacheArgs("MGET", a, b)},
	} {
		if _, err := r.planCacheTransaction(x, nil, queue, nil); !errors.Is(err, errShardCrossNode) {
			t.Fatal("transaction split across nodes", err)
		}
	}
	queue := [][][]byte{cacheArgs("SET", a, "1")}
	if _, err := r.planCacheTransaction(x, nil, queue, cacheArgs(b)); !errors.Is(err, errShardCrossNode) {
		t.Fatal("WATCH key omitted", err)
	}
	if _, err := r.planCacheTransaction(x, nil, queue, cacheArgs(cachePrefix+"owner")); !errors.Is(err, errShardCommand) {
		t.Fatal("reserved WATCH key", err)
	}
	for _, args := range [][]string{{"KEYS", "*"}, {"SCAN", "0"}} {
		if _, err := r.planCacheTransaction(x, nil, append(queue, cacheArgs(args...)), nil); !errors.Is(err, errShardGlobal) {
			t.Fatal("global scan misrouted in transaction", err)
		}
	}
	queue = [][][]byte{cacheArgs("PING"), cacheArgs("HSET", "u{100}:profile", "field", "v"), cacheArgs("EVAL", "return 1", "1", "u{100}:balance"), cacheArgs("UNWATCH")}
	if p, err := r.planCacheTransaction(x, nil, queue, cacheArgs("u{100}:watched")); err != nil || len(p.groups) != 1 || len(p.keys) != 3 {
		t.Fatal("same-tag transaction", err)
	}
	if _, err := r.planCacheTransaction(x, nil, [][][]byte{nil}, nil); !errors.Is(err, errShardCommand) {
		t.Fatal("empty command", err)
	}
}

func TestShardPlansDuringMigration(t *testing.T) {
	r := &cacheRuntime{}
	from, to := shardTestTopology(t, 3, 10), shardTestTopology(t, 4, 11)
	change, err := sharding.NewTransition(from, to)
	if err != nil {
		t.Fatal(err)
	}
	var moved, still string
	for i := 0; i < 1000 && (moved == "" || still == ""); i++ {
		key := fmt.Sprintf("key-%d", i)
		if change.DesiredPlacement([]byte(key)).Moves() {
			moved = key
		} else {
			still = key
		}
	}
	if moved == "" || still == "" {
		t.Fatal("missing test keys")
	}
	p, err := r.planCacheCommand(from, change, cacheArgs("MGET", moved, still))
	if err != nil || !p.moves || !p.keys[0].placement.Moves() || p.keys[1].placement.Moves() || p.fromRevision != 10 || p.toRevision != 11 {
		t.Fatalf("placement lost migration boundary: %+v %v", p, err)
	}
	if _, err := r.planCacheCommand(to, change, cacheArgs("GET", moved)); err == nil {
		t.Fatal("wrong revision accepted")
	}
	c := from.Config()
	c.Nodes[0].Address = "10.0.0.1:17000"
	wrong, _ := sharding.New(c)
	if _, err := r.planCacheCommand(wrong, change, cacheArgs("GET", moved)); err == nil {
		t.Fatal("different snapshot with same revision accepted")
	}
	// Same-tag operations are planned toward one target, but remain marked
	// pending until an executor has resolved every key's actual handoff state.
	for i := 0; i < 1000; i++ {
		a, b := fmt.Sprintf("a{%d}", i), fmt.Sprintf("b{%d}", i)
		if !change.DesiredPlacement([]byte(a)).Moves() {
			continue
		}
		p, err := r.planCacheCommand(from, change, cacheArgs("RENAME", a, b))
		if err != nil || !p.moves || len(p.groups) != 1 {
			t.Fatal("same-tag handoff", err)
		}
		return
	}
	t.Fatal("no moved tag found")
}
