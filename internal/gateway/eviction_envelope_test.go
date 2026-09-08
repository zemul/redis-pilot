package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"testing"
)

func evictionTestIntent(key, value string) evictionIntent {
	h := sha256.Sum256([]byte(value))
	return evictionIntent{Version: 1, TaskID: "probe", OperationID: strings.Repeat("1", 32), Epoch: 1,
		History: strings.Repeat("a", 40), Key: []byte(key), ValueSHA256: hex.EncodeToString(h[:])}
}

func evictionTestRecords(before syncPosition, frames [][]byte) []syncRecord {
	var result []syncRecord
	for _, frame := range frames {
		before.Offset += int64(len(frame))
		result = append(result, syncRecord{Kind: "command", Position: before, Data: frame})
	}
	return result
}

func TestEvictionEnvelopeExactMatch(t *testing.T) {
	for _, key := range []string{"", "key", "\x00\xff\r\n", strings.Repeat("k", 64<<10)} {
		i := evictionTestIntent(key, "\x00\xffvalue")
		before := syncPosition{Source: "127.0.0.1:12345", History: i.History, Offset: 100}
		records := evictionTestRecords(before, i.frames())
		e, err := inspectEvictionEnvelope(before, records, i)
		if err != nil || e.Before != before || e.End != records[4].Position || len(e.WireSHA256) != 64 || len(e.IntentSHA256) != 64 {
			t.Fatalf("evidence: %+v %v", e, err)
		}
		again, err := inspectEvictionEnvelope(before, records, i)
		if err != nil || again != e {
			t.Fatal("not deterministic", err)
		}
		// Existing source replayer remains fail-closed on ALL envelope frames.
		for _, r := range records {
			if _, err := syncPendingCommand(r.Data); !errors.Is(err, errSyncUnsupported) {
				t.Fatal("probe accidentally enabled live replay", err)
			}
		}
		shifted := before
		shifted.Offset++
		other, err := inspectEvictionEnvelope(shifted, evictionTestRecords(shifted, i.frames()), i)
		if err != nil || other.WireSHA256 == e.WireSHA256 {
			t.Fatal("digest did not bind source position", err)
		}
	}
}

func TestEvictionEnvelopeRejectsPartialAndAlteredRecords(t *testing.T) {
	i := evictionTestIntent("key", "value")
	before := syncPosition{Source: "127.0.0.1:12345", History: i.History, Offset: 100}
	mutations := map[string]func([]syncRecord) []syncRecord{
		"missing_begin": func(r []syncRecord) []syncRecord { return r[1:] },
		"extra_command": func(r []syncRecord) []syncRecord { return append(r, r[4]) },
		"reordered":     func(r []syncRecord) []syncRecord { r[1], r[3] = r[3], r[1]; return r },
		"business_del":  func(r []syncRecord) []syncRecord { r[2].Data = encodeCommand("DEL", "key"); return r },
		"wrong_key":     func(r []syncRecord) []syncRecord { r[2].Data = encodeCommand("UNLINK", "other"); return r },
		"two_keys":      func(r []syncRecord) []syncRecord { r[2].Data = encodeCommand("UNLINK", "key", "other"); return r },
		"expired_marker": func(r []syncRecord) []syncRecord {
			r[1].Data = encodeCommand("SET", i.markerKey(), "begin:"+i.encode(), "PX", "100")
			return r
		},
		"wrong_completion": func(r []syncRecord) []syncRecord { r[3].Data = i.frames()[1]; return r },
		"nested_multi":     func(r []syncRecord) []syncRecord { r[2].Data = encodeCommand("MULTI"); return r },
		"embedded_exec":    func(r []syncRecord) []syncRecord { r[2].Data = append(r[2].Data, encodeCommand("EXEC")...); return r },
		"gap":              func(r []syncRecord) []syncRecord { r[2].Position.Offset++; return r },
		"duplicate_offset": func(r []syncRecord) []syncRecord { r[2].Position = r[1].Position; return r },
		"wrong_history":    func(r []syncRecord) []syncRecord { r[2].Position.History = strings.Repeat("b", 40); return r },
		"wrong_source":     func(r []syncRecord) []syncRecord { r[2].Position.Source = "127.0.0.1:54321"; return r },
		"snapshot":         func(r []syncRecord) []syncRecord { r[2].Kind = "snapshot"; return r },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			e, err := inspectEvictionEnvelope(before, mutate(evictionTestRecords(before, i.frames())), i)
			if err != errEvictionEnvelope || e != (evictionEnvelopeEvidence{}) {
				t.Fatalf("invalid envelope yielded evidence: %+v %v", e, err)
			}
		})
	}
	for n := 0; n < 5; n++ {
		if _, err := inspectEvictionEnvelope(before, evictionTestRecords(before, i.frames())[:n], i); err != errEvictionEnvelope {
			t.Fatal("prefix accepted", n, err)
		}
	}
}

func TestEvictionEnvelopeRequiresExpectedIdentity(t *testing.T) {
	i := evictionTestIntent("key", "value")
	before := syncPosition{Source: "127.0.0.1:12345", History: i.History, Offset: 100}
	for name, mutate := range map[string]func(*evictionIntent){
		"different_operation": func(p *evictionIntent) { p.OperationID = strings.Repeat("2", 32) },
		"different_epoch":     func(p *evictionIntent) { p.Epoch++ },
		"different_task":      func(p *evictionIntent) { p.TaskID = "other" },
		"different_value":     func(p *evictionIntent) { p.ValueSHA256 = strings.Repeat("2", 64) },
		"zero_epoch":          func(p *evictionIntent) { p.Epoch = 0 },
		"version":             func(p *evictionIntent) { p.Version = 2 },
		"task_path":           func(p *evictionIntent) { p.TaskID = "../other" },
		"nil_key":             func(p *evictionIntent) { p.Key = nil },
		"metadata_key":        func(p *evictionIntent) { p.Key = []byte(syncMetadataPrefix + "forbidden") },
		"huge_key":            func(p *evictionIntent) { p.Key = []byte(strings.Repeat("k", (64<<10)+1)) },
		"invalid_hash":        func(p *evictionIntent) { p.ValueSHA256 = "not-a-hash" },
		"zero_history":        func(p *evictionIntent) { p.History = strings.Repeat("0", 40) },
		"uppercase_history":   func(p *evictionIntent) { p.History = strings.Repeat("A", 40) },
		"zero_operation":      func(p *evictionIntent) { p.OperationID = strings.Repeat("0", 32) },
		"huge_operation":      func(p *evictionIntent) { p.OperationID = strings.Repeat("1", 1<<20) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := i
			mutate(&changed)
			if _, err := inspectEvictionEnvelope(before, evictionTestRecords(before, i.frames()), changed); err != errEvictionEnvelope {
				t.Fatal("unexpected intent accepted", err)
			}
		})
	}
	for _, offset := range []int64{-1, math.MaxInt64, math.MaxInt64 - 1} {
		bad := before
		bad.Offset = offset
		if _, err := inspectEvictionEnvelope(bad, evictionTestRecords(bad, i.frames()), i); err != errEvictionEnvelope {
			t.Fatal("invalid/overflowing offset accepted", offset, err)
		}
	}
	for _, source := range []string{"", "localhost:12345", "127.0.0.1:012345", strings.Repeat("x", 257)} {
		bad := before
		bad.Source = source
		if _, err := inspectEvictionEnvelope(bad, evictionTestRecords(bad, i.frames()), i); err != errEvictionEnvelope {
			t.Fatal("invalid source accepted", err)
		}
	}
	// A plain business delete is never an internal removal, irrespective of key.
	cmd, err := syncPendingCommand(encodeCommand("DEL", "key"))
	if err != nil || len(cmd) != 2 || cmd[0] != "DEL" {
		t.Fatal("business deletion semantics changed", cmd, err)
	}
}

func TestEvictionEnvelopeSelectZeroInsideTransaction(t *testing.T) {
	i := evictionTestIntent("key", "value")
	before := syncPosition{Source: "127.0.0.1:12345", History: i.History, Offset: 100}
	base := i.frames()
	for _, db := range []string{"0", "1", "00", "-1"} {
		frames := append([][]byte{base[0], encodeCommand("SELECT", db)}, base[1:]...)
		records := evictionTestRecords(before, frames)
		_, err := inspectEvictionEnvelope(before, records, i)
		if (db == "0" && err != nil) || (db != "0" && err != errEvictionEnvelope) {
			t.Fatal("SELECT context", db, err)
		}
	}
	frames := append([][]byte{base[0], base[1], encodeCommand("SELECT", "0")}, base[2:]...)
	if _, err := inspectEvictionEnvelope(before, evictionTestRecords(before, frames), i); err != errEvictionEnvelope {
		t.Fatal("misplaced SELECT accepted", err)
	}
}
