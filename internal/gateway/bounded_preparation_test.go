package gateway

import (
	"fmt"
	"strings"
	"testing"
)

func TestBoundedPreparationIntent(t *testing.T) {
	i := evictionTestIntent("key", "value")
	i.Version, i.PreviousOperationID = 2, strings.Repeat("0", 32)
	if !i.valid() || preparationScript(i) != boundedPreparationScript {
		t.Fatal("valid bounded intent rejected")
	}
	key := i.prepareKey()
	i.PreviousOperationID, i.OperationID = i.OperationID, strings.Repeat("2", 32)
	if !i.valid() || i.prepareKey() != key {
		t.Fatal("marker grew with operation ID")
	}
	i.PreviousOperationID = i.OperationID
	if i.valid() {
		t.Fatal("non-increasing operation accepted")
	}
	i.Version = 1
	if i.valid() {
		t.Fatal("legacy intent accepted bounded fields")
	}
}

func TestBoundedPreparationRejectsLegacyDirectory(t *testing.T) {
	f := newHotPreparationFixture(t)
	req := hotPreparationRequest{strings.Repeat("1", 32), []byte("key")}
	if w := hotPreparationPost(t, f.h, hotPreparationPath, req); w.Code != 200 {
		t.Fatal(w.Code)
	}
	f.h.Close()
	s, err := NewService(f.h.service.cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.admission = testKeyBarrierSet(t, 8, 1024, 8)
	f.o.BoundedMarkers = true
	if h, err := openHotPreparation(s, f.o); err == nil {
		h.Close()
		t.Fatal("legacy operation silently adopted into bounded protocol")
	}
}

// Opt-in real Redis test; owns only its two explicit keys. No cold replay or
// Gateway is implied by this script-level stress check.
func TestBoundedPreparationRedis(t *testing.T) {
	hot, _ := evictionColdEnvironment(t)
	c := kvCommitConnect(t, hot)
	defer c.c.Close()
	c.expect(t, ":0\r\n", "DBSIZE")
	i := evictionTestIntent("bounded-business", "value")
	i.Version, i.PreviousOperationID = 2, strings.Repeat("0", 32)
	defer c.request("DEL", "bounded-business", i.prepareKey())
	i.Epoch = uint64(1) << 53
	c.expect(t, "+OK\r\n", "SET", "bounded-business", "value")
	var first evictionIntent
	for n := uint64(0); n < 100; n++ {
		// IDs above 2^53 would lose precision if converted to Lua numbers.
		i.OperationID = fmt.Sprintf("%032x", (uint64(1)<<53)+n)
		c.expect(t, ":1\r\n", "EVAL", boundedPreparationScript, "2", "bounded-business", i.prepareKey(), "value", "prepare:"+i.encode())
		c.expect(t, ":2\r\n", "EVAL", boundedPreparationScript, "2", "bounded-business", i.prepareKey(), "value", "prepare:"+i.encode())
		if n == 0 {
			first = i
		}
		if n != 99 {
			i.PreviousOperationID = i.OperationID
		}
	}
	c.expect(t, ":2\r\n", "DBSIZE")
	wrongEpoch := i
	wrongEpoch.PreviousOperationID = i.OperationID
	wrongEpoch.OperationID = fmt.Sprintf("%032x", (uint64(1)<<53)+100)
	wrongEpoch.Epoch++
	c.expect(t, "-ERR prepare predecessor conflict\r\n", "EVAL", boundedPreparationScript, "2", "bounded-business", wrongEpoch.prepareKey(), "value", "prepare:"+wrongEpoch.encode())
	c.expect(t, "-ERR prepare predecessor conflict\r\n", "EVAL", boundedPreparationScript, "2", "bounded-business", first.prepareKey(), "value", "prepare:"+first.encode())
	c.expect(t, "+OK\r\n", "SET", "bounded-business", "changed")
	c.expect(t, ":2\r\n", "EVAL", boundedPreparationScript, "2", "bounded-business", i.prepareKey(), "value", "prepare:"+i.encode())
	kvCommitGet(t, c, "bounded-business", "changed")
	c.expect(t, ":1\r\n", "DEL", i.prepareKey()) // inject lost source metadata
	i.PreviousOperationID = i.OperationID
	i.OperationID = fmt.Sprintf("%032x", (uint64(1)<<53)+100)
	c.expect(t, "-ERR prepare predecessor missing\r\n", "EVAL", boundedPreparationScript, "2", "bounded-business", i.prepareKey(), "changed", "prepare:"+i.encode())
}
