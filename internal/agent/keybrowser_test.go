package agent

import (
	"reflect"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestKvBuildWriteArgs(t *testing.T) {
	cases := []struct {
		name    string
		op      string
		args    apitypes.KeyWriteArgs
		want    []string
		wantErr bool
	}{
		{"set", "set", apitypes.KeyWriteArgs{Value: "v"}, []string{"SET", "k", "v"}, false},
		{"set empty ok", "set", apitypes.KeyWriteArgs{Value: ""}, []string{"SET", "k", ""}, false},
		{"hset", "hset", apitypes.KeyWriteArgs{Field: "f", Value: "v"}, []string{"HSET", "k", "f", "v"}, false},
		{"hset missing field", "hset", apitypes.KeyWriteArgs{Value: "v"}, nil, true},
		{"push right default", "push", apitypes.KeyWriteArgs{Value: "v"}, []string{"RPUSH", "k", "v"}, false},
		{"push left", "push", apitypes.KeyWriteArgs{Value: "v", Side: "left"}, []string{"LPUSH", "k", "v"}, false},
		{"sadd", "sadd", apitypes.KeyWriteArgs{Member: "m"}, []string{"SADD", "k", "m"}, false},
		{"sadd missing member", "sadd", apitypes.KeyWriteArgs{}, nil, true},
		{"zadd", "zadd", apitypes.KeyWriteArgs{Member: "m", Score: "1.5"}, []string{"ZADD", "k", "1.5", "m"}, false},
		{"zadd bad score", "zadd", apitypes.KeyWriteArgs{Member: "m", Score: "abc"}, nil, true},
		{"expire", "expire", apitypes.KeyWriteArgs{TTLSeconds: 60}, []string{"EXPIRE", "k", "60"}, false},
		{"persist", "expire", apitypes.KeyWriteArgs{TTLSeconds: -1}, []string{"PERSIST", "k"}, false},
		{"unknown op", "flushall", apitypes.KeyWriteArgs{}, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := kvBuildWriteArgs("k", tc.op, tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got args %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("args = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestKvBuildWriteArgsRejectsOversizedValue(t *testing.T) {
	big := strings.Repeat("x", kvMaxWriteBytes+1)
	if _, err := kvBuildWriteArgs("k", "set", apitypes.KeyWriteArgs{Value: big}); err == nil {
		t.Fatalf("expected oversized value to be rejected")
	}
}

func TestKvWriteOpsWhitelist(t *testing.T) {
	for _, op := range []string{"set", "hset", "push", "sadd", "zadd", "expire"} {
		if !kvWriteOps[op] {
			t.Errorf("op %q should be whitelisted", op)
		}
	}
	for _, op := range []string{"del", "flushall", "config", "keys", "eval"} {
		if kvWriteOps[op] {
			t.Errorf("op %q must not be whitelisted", op)
		}
	}
}

func TestKvSplit(t *testing.T) {
	if got := kvSplit(""); got != nil {
		t.Errorf("empty should be nil, got %v", got)
	}
	if got := kvSplit("a\nb\nc"); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Errorf("split = %v", got)
	}
}
