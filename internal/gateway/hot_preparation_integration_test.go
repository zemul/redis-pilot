package gateway

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Disposable real Redis, explicitly enabled via GATEWAY_TEST_REDIS_SERVER.
// No real Kvrocks is involved here; this tests hot preparation, not cold commit.
func TestHotPreparationRedis(t *testing.T) {
	testHotPreparationRedis(t, false)
}

func TestHotPreparationRedisLostReply(t *testing.T) {
	testHotPreparationRedis(t, true)
}

func testHotPreparationRedis(t *testing.T, dropReply bool) {
	addr := syncTestRedis(t)
	c, r, err := authenticated(addr, "g4-backend-test")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	backend := &kvCommitClient{c, r}
	backend.expect(t, "+OK\r\n", "CONFIG", "SET", "maxmemory-policy", "noeviction")
	info, err := backend.request("INFO", "replication")
	if err != nil {
		t.Fatal(err)
	}
	pos, code := parseSyncSourceInfo(info)
	if code != "none" {
		t.Fatal(code)
	}
	if dropReply {
		// A RESP relay executes against the real disposable Redis, then drops
		// the INCR reply before Gateway sees it. No fake Redis mutation here.
		realAddress := addr
		addr = fakeBackend(t, func(client net.Conn, args [][]byte) bool {
			upstream, replies, err := authenticated(realAddress, "g4-backend-test")
			if err != nil {
				t.Error(err)
				return false
			}
			defer upstream.Close()
			command := make([]string, len(args))
			for i := range args {
				command[i] = string(args[i])
			}
			raw, err := (&kvCommitClient{upstream, replies}).request(command...)
			if err != nil {
				t.Error(err)
				return false
			}
			if command[0] == "INCR" {
				return false
			}
			return writeAll(client, raw) == nil
		})
	}
	id, intent, _ := evictionOperationFixtureData()
	id.Source = addr
	intent.History = pos.History
	binding := confirmationTestBinding(id, intent)
	syncer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer syncer.Close()
	o := hotPreparationOptions{IsolatedConfirmationGatewayOptions{binding, syncer.URL, secretFile(t, strings.Repeat("s", 32)), confirmationPrivateDir(t), 2 * time.Second}, secretFile(t, strings.Repeat("a", 32)), true}
	cfg := passConfig(addr)
	cfg.GatewayID = binding.GatewayID
	cfg.BackendPasswordFile = secretFile(t, "g4-backend-test")
	if dropReply {
		// Authentication is handled by the local test relay's upstream link.
		cfg.BackendPasswordFile = ""
	}
	cfg.RequestTimeout = time.Second
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.admission = testKeyBarrierSet(t, 8, 1024, 8)
	h, err := openHotPreparation(s, o)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	s.preparation = h
	client, replies, _ := startSession(t, s)
	key := "hot:\x00\xffkey"
	if err := writeAll(client, encodeCommand("SET", key, "41")); err != nil {
		t.Fatal(err)
	}
	expect(t, replies, "+OK\r\n")
	if err := writeAll(client, encodeCommand("INCR", key)); err != nil {
		t.Fatal(err)
	}
	if dropReply {
		if _, err := replies.ReadFrame(); err == nil {
			t.Fatal("fabricated INCR reply")
		}
		client, replies, _ = startSession(t, s)
		if err := writeAll(client, encodeCommand("GET", key)); err != nil {
			t.Fatal(err)
		}
		expect(t, replies, "$2\r\n42\r\n")
	} else {
		expect(t, replies, ":42\r\n")
	}
	req := hotPreparationRequest{strings.Repeat("1", 32), []byte(key)}
	for attempt := 0; attempt < 2; attempt++ {
		if w := hotPreparationPost(t, h, hotPreparationPath, req); w.Code != 200 {
			t.Fatal("prepare", w.Code, w.Body.String())
		}
	}
	state, err := h.confirmation.op.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	backend.expect(t, "$2\r\n42\r\n", "GET", key)
	backend.expect(t, ":-1\r\n", "PTTL", key)
	marker, err := backend.request("GET", state.Intent.prepareKey())
	if err != nil {
		t.Fatal(err)
	}
	value, err := hotPreparationValue(marker)
	if err != nil || string(value) != "prepare:"+state.Intent.encode() {
		t.Fatal("marker mismatch", err)
	}
	if err := writeAll(client, encodeCommand("INCR", key)); err != nil {
		t.Fatal(err)
	}
	expect(t, replies, "-TRYAGAIN hot key admission unavailable; command not sent\r\n")
	if w := hotPreparationPost(t, h, hotPreparationPath+"/confirm", req); w.Code != 503 {
		t.Fatal("unavailable cold confirmation accepted", w.Code)
	}
	backend.expect(t, "$2\r\n42\r\n", "GET", key)
	t.Log("real Redis hot value retained, identical preparation retry accepted, business INCR blocked, unavailable confirmation retained barrier")
	if dropReply {
		t.Log("real Redis executed INCR once with lost backend reply; new preparation still succeeded without manual reset")
	}
}
