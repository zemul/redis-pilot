package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestResidencyEnvelopePending(t *testing.T) {
	for _, warm := range []bool{false, true} {
		for _, selectDB := range []bool{false, true} {
			f := newSyncSegmentFixture(t, 2048, 4<<20)
			f.append(t, 500)
			base := f.points[len(f.points)-1]
			hash := sha256.Sum256([]byte("42"))
			i := evictionIntent{Version: 2, TaskID: "lane", OperationID: strings.Repeat("1", 32), PreviousOperationID: strings.Repeat("0", 32), Epoch: 1, History: base.Position.History, Key: []byte("counter"), ValueSHA256: hex.EncodeToString(hash[:])}
			frames := [][]byte{encodeCommand("MULTI"), encodeCommand("SET", i.residencyKey(), "cold:"+i.encode()), encodeCommand("UNLINK", string(i.Key)), encodeCommand("EXEC")}
			if warm {
				frames[1], frames[2] = encodeCommand("SET", string(i.Key), "42"), encodeCommand("SET", i.residencyKey(), "hot:"+i.encode())
			}
			if selectDB {
				frames = append([][]byte{frames[0], encodeCommand("SELECT", "0")}, frames[1:]...)
			}
			previous := base
			for n, raw := range frames {
				pos := previous.Position
				pos.Offset += int64(len(raw))
				r := syncRecord{Kind: "command", Position: pos, Data: raw}
				if err := f.store.append(r); err != nil {
					t.Fatal(err)
				}
				previous = nextPendingCheckpoint(previous, r)
				v, err := f.store.pendingView()
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				end, err := v.walkResidency(context.Background(), base, v.end, func(group syncRecord, before, after syncApplyCheckpoint) error {
					count++
					p := syncPending{Version: 3, Previous: before, Next: after, Record: group}
					if err := validatePending(p); err != nil {
						t.Fatal(err)
					}
					if err := v.proveResidencyPending(p); err != nil {
						t.Fatal(err)
					}
					meta := syncPendingMeta{checkpoint: after, anchor: f.points[0].encode(), pending: &p}
					if err := f.store.prune(v, meta); err != nil {
						t.Fatal("group prune", err)
					}
					nextView, err := f.store.pendingView()
					if err != nil {
						t.Fatal(err)
					}
					if err := nextView.provePending(meta); err != nil {
						t.Fatal("proof after group prune", err)
					}
					nextView.close()
					bad := p
					bad.Next = nextPendingCheckpoint(before, group)
					if validatePending(bad) == nil {
						t.Fatal("aggregate hash substituted for per-frame chain")
					}
					bad = p
					bad.After.Exists = true
					if validatePending(bad) == nil {
						t.Fatal("control group gained business effect")
					}
					return nil
				})
				v.close()
				if err != nil {
					t.Fatal(err)
				}
				if n < len(frames)-1 && (count != 0 || end != base) {
					t.Fatal("partial transaction advanced")
				}
				if n == len(frames)-1 && (count != 1 || end != previous) {
					t.Fatal("complete transaction not coalesced")
				}
			}
			raw := bytes.Join(frames, nil)
			if _, err := parseResidencyEnvelope(append(raw, encodeCommand("DEL", "counter")...)); err == nil {
				t.Fatal("extra command accepted")
			}
			if _, err := parseResidencyEnvelope(bytes.Replace(raw, []byte("counter"), []byte("another"), 1)); err == nil {
				t.Fatal("changed key accepted")
			}
		}
	}
}

func TestResidencyUnknownTransactionRejected(t *testing.T) {
	for _, raw := range [][]byte{
		bytes.Join([][]byte{encodeCommand("MULTI"), encodeCommand("DEL", "counter"), encodeCommand("EXEC")}, nil),
		bytes.Join([][]byte{encodeCommand("MULTI"), encodeCommand("SELECT", "1"), encodeCommand("EXEC")}, nil),
	} {
		if _, err := parseResidencyEnvelope(raw); err == nil {
			t.Fatal("arbitrary transaction accepted")
		}
	}
}
