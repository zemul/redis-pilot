package gateway

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestSyncPendingCommand(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want []string
	}{
		{"set", []string{"set", "key", "value"}, []string{"SET", "key", "value"}},
		{"incr", []string{"InCr", "key"}, []string{"INCR", "key"}},
		{"del", []string{"del", "key"}, []string{"DEL", "key"}},
		{"empty_key_and_value", []string{"SET", "", ""}, []string{"SET", "", ""}},
		{"binary", []string{"SET", "\xff\x00\xfe", "\xfe\r\n\xff"}, []string{"SET", "\xff\x00\xfe", "\xfe\r\n\xff"}},
		{"select", []string{"SELECT", "0"}, nil},
		{"ping", []string{"PING"}, nil},
		{"getack", []string{"replconf", "getack", "*"}, nil},
		{"key_limit", []string{"DEL", strings.Repeat("k", syncPendingValueLimit)}, []string{"DEL", strings.Repeat("k", syncPendingValueLimit)}},
		{"value_limit", []string{"SET", "k", strings.Repeat("v", syncPendingValueLimit)}, []string{"SET", "k", strings.Repeat("v", syncPendingValueLimit)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := syncPendingCommand(encodeCommand(tc.args...))
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("command mismatch: got %q, err %v", got, err)
			}
		})
	}
	for name, args := range map[string][]string{
		"unknown":           {"HSET", "k", "f", "v"},
		"ttl_command":       {"PEXPIRE", "k", "100"},
		"set_ttl":           {"SET", "k", "v", "PX", "100"},
		"set_conditional":   {"SET", "k", "v", "NX"},
		"set_short":         {"SET", "k"},
		"del_multiple":      {"DEL", "a", "b"},
		"incr_missing_key":  {"INCR"},
		"select_other_db":   {"SELECT", "1"},
		"select_nondecimal": {"SELECT", "00"},
		"ping_argument":     {"PING", "hello"},
		"replconf_ack":      {"REPLCONF", "ACK", "100"},
		"getack_argument":   {"REPLCONF", "GETACK", "1"},
		"metadata_set":      {"SET", syncPendingKey, "value"},
		"metadata_incr":     {"INCR", syncPendingCPKey},
		"metadata_del":      {"DEL", syncAnchorKey},
		"key_over_limit":    {"DEL", strings.Repeat("k", syncPendingValueLimit+1)},
		"value_over_limit":  {"SET", "k", strings.Repeat("v", syncPendingValueLimit+1)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := syncPendingCommand(encodeCommand(args...)); !errors.Is(err, errSyncUnsupported) {
				t.Fatalf("unsupported command: got %v", err)
			}
		})
	}
	valid := encodeCommand("SET", "k", "v")
	for name, raw := range map[string][]byte{
		"empty":         nil,
		"null_array":    []byte("*-1\r\n"),
		"empty_array":   []byte("*0\r\n"),
		"null_key":      []byte("*2\r\n$4\r\nINCR\r\n$-1\r\n"),
		"truncated":     valid[:len(valid)-1],
		"inline":        []byte("INCR k\r\n"),
		"trailing_byte": append(bytes.Clone(valid), 'x'),
		"trailing_frame": append(bytes.Clone(valid),
			encodeCommand("DEL", "k")...),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := syncPendingCommand(raw); !errors.Is(err, errSyncUnsupported) {
				t.Fatalf("invalid frame: got %v", err)
			}
		})
	}
}

func TestSyncPendingAfter(t *testing.T) {
	missing := syncPendingValue{}
	value := func(s string) syncPendingValue { return syncPendingValue{true, []byte(s)} }
	for _, tc := range []struct {
		name   string
		cmd    []string
		before syncPendingValue
		after  syncPendingValue
	}{
		{"create_empty", []string{"SET", "", ""}, missing, value("")},
		{"replace_binary", []string{"SET", "k", "\xff\xfe"}, value("\xfe\xff"), value("\xff\xfe")},
		{"delete_missing", []string{"DEL", ""}, missing, missing},
		{"delete_empty", []string{"DEL", ""}, value(""), missing},
		{"incr_missing", []string{"INCR", ""}, missing, value("1")},
		{"incr_zero", []string{"INCR", "k"}, value("0"), value("1")},
		{"incr_negative", []string{"INCR", "k"}, value("-1"), value("0")},
		{"incr_min", []string{"INCR", "k"}, value("-9223372036854775808"), value("-9223372036854775807")},
		{"incr_to_max", []string{"INCR", "k"}, value("9223372036854775806"), value("9223372036854775807")},
		{"control", nil, missing, missing},
		{"control_empty_slice", nil, syncPendingValue{false, []byte{}}, missing},
		{"before_limit", []string{"DEL", "k"}, value(strings.Repeat("v", syncPendingValueLimit)), missing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := syncPendingAfter(tc.cmd, tc.before)
			if err != nil || !equalPendingValue(got, tc.after) {
				t.Fatalf("after mismatch: got %#v, err %v", got, err)
			}
		})
	}
	for _, old := range []string{"", "abc", "+1", "01", "-0", " 1", "1 ", "1.0", "\xff", "9223372036854775807", "9223372036854775808", "-9223372036854775809"} {
		t.Run("invalid_integer_"+strconv.Quote(old), func(t *testing.T) {
			if _, err := syncPendingAfter([]string{"INCR", "k"}, value(old)); !errors.Is(err, errSyncTargetDrift) {
				t.Fatalf("invalid integer accepted: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		cmd    []string
		before syncPendingValue
		want   error
	}{
		{"missing_with_data", []string{"DEL", "k"}, syncPendingValue{false, []byte("x")}, errSyncUnsupported},
		{"before_over_limit", []string{"DEL", "k"}, value(strings.Repeat("v", syncPendingValueLimit+1)), errSyncUnsupported},
		{"control_existing_empty", nil, value(""), errSyncTargetDrift},
		{"unknown", []string{"GET", "k"}, missing, errSyncUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := syncPendingAfter(tc.cmd, tc.before); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func pendingUnitRecord(t *testing.T, args []string, before syncPendingValue) syncPending {
	t.Helper()
	snap := captureSnapshot("127.0.0.1:12345")
	snap.Data = syncTestRDB()
	prev := nextPendingCheckpoint(syncApplyCheckpoint{}, snap)
	raw := encodeCommand(args...)
	pos := prev.Position
	pos.Offset += int64(len(raw))
	record := syncRecord{"command", pos, raw}
	cmd, err := syncPendingCommand(raw)
	if err != nil {
		t.Fatal(err)
	}
	after, err := syncPendingAfter(cmd, before)
	if err != nil {
		t.Fatal(err)
	}
	return syncPending{1, prev, nextPendingCheckpoint(prev, record), record, before, after}
}

func TestSyncPendingValidateAndBinaryRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   []string
		before syncPendingValue
	}{
		{"binary", []string{"SET", "\xff\xfe", "\xfe\xff"}, syncPendingValue{true, []byte{0xff, 0xfe}}},
		{"empty_present", []string{"SET", "", ""}, syncPendingValue{true, []byte{}}},
		{"empty_missing", []string{"SET", "", ""}, syncPendingValue{}},
		{"delete_empty", []string{"DEL", ""}, syncPendingValue{true, []byte{}}},
		{"control", []string{"PING"}, syncPendingValue{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := pendingUnitRecord(t, tc.args, tc.before)
			if err := validatePending(p); err != nil {
				t.Fatal(err)
			}
			var decoded syncPending
			if err := json.Unmarshal([]byte(p.encode()), &decoded); err != nil {
				t.Fatal(err)
			}
			if err := validatePending(decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.encode() != p.encode() || !equalPendingValue(decoded.Before, p.Before) || !equalPendingValue(decoded.After, p.After) || !bytes.Equal(decoded.Record.Data, p.Record.Data) {
				t.Fatal("pending JSON altered binary or presence information")
			}
			cmd, err := syncPendingCommand(decoded.Record.Data)
			if err != nil || (len(cmd) > 0 && !reflect.DeepEqual(cmd, tc.args)) {
				t.Fatal("binary key/value was altered", err)
			}
		})
	}
}

func TestSyncPendingValidateRejectsMismatch(t *testing.T) {
	for name, mutate := range map[string]func(*syncPending){
		"pending_version":    func(p *syncPending) { p.Version++ },
		"previous_version":   func(p *syncPending) { p.Previous.Version++ },
		"next_version":       func(p *syncPending) { p.Next.Version++ },
		"record_kind":        func(p *syncPending) { p.Record.Kind = "snapshot" },
		"record_offset":      func(p *syncPending) { p.Record.Position.Offset++ },
		"record_history":     func(p *syncPending) { p.Record.Position.History = strings.Repeat("b", 40) },
		"record_source":      func(p *syncPending) { p.Record.Position.Source = "127.0.0.1:12346" },
		"next_chain":         func(p *syncPending) { p.Next.Chain = strings.Repeat("b", 64) },
		"previous_chain":     func(p *syncPending) { p.Previous.Chain = strings.Repeat("b", 64) },
		"next_offset":        func(p *syncPending) { p.Next.Position.Offset++ },
		"before_value":       func(p *syncPending) { p.Before.Data = []byte("40") },
		"before_missing":     func(p *syncPending) { p.Before = syncPendingValue{} },
		"before_bad_integer": func(p *syncPending) { p.Before.Data = []byte("not-an-integer") },
		"after_value":        func(p *syncPending) { p.After.Data = []byte("43") },
		"after_missing":      func(p *syncPending) { p.After.Exists = false },
		"encoded_limit":      func(p *syncPending) { p.After.Data = bytes.Repeat([]byte{'a'}, syncPendingEncodedLimit) },
	} {
		t.Run(name, func(t *testing.T) {
			p := pendingUnitRecord(t, []string{"INCR", "k"}, syncPendingValue{true, []byte("41")})
			mutate(&p)
			if err := validatePending(p); !errors.Is(err, errSyncTargetDrift) {
				t.Fatalf("mismatch accepted: %v", err)
			}
		})
	}
	for _, args := range [][]string{{"PEXPIRE", "k", "100"}, {"HSET", "k", "f", "v"}} {
		p := pendingUnitRecord(t, []string{"INCR", "k"}, syncPendingValue{true, []byte("41")})
		p.Record.Data = encodeCommand(args...)
		p.Record.Position.Offset = p.Previous.Position.Offset + int64(len(p.Record.Data))
		p.Next = nextPendingCheckpoint(p.Previous, p.Record)
		if err := validatePending(p); !errors.Is(err, errSyncUnsupported) {
			t.Fatalf("valid chain must not permit unsupported %q: %v", args[0], err)
		}
	}
}

func TestSyncPendingViewBoundaryAndClose(t *testing.T) {
	j, _ := captureJournal(t, 1<<20)
	if _, err := j.pendingView(); !errors.Is(err, errSyncJournalCorrupt) {
		t.Fatalf("empty journal view accepted: %v", err)
	}
	snap := captureSnapshot("127.0.0.1:12345")
	snap.Data = syncTestRDB()
	if err := j.append(snap); err != nil {
		t.Fatal(err)
	}
	appendSyncTestCommand(t, j, "SET", "k", "41")
	v, err := j.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer v.f.Close()
	walk := func(view *syncPendingView, want int) {
		t.Helper()
		count := 0
		var previous syncApplyCheckpoint
		lastAt := int64(len(syncJournalMagic))
		err := view.walk(func(r syncRecord, prev, next syncApplyCheckpoint, at int64) error {
			if prev != previous || next != nextPendingCheckpoint(prev, r) || at <= lastAt || at > view.size {
				t.Fatal("view chain or byte boundary mismatch")
			}
			count++
			previous, lastAt = next, at
			return nil
		})
		if err != nil || count != want || previous.Position != view.end || lastAt != view.size {
			t.Fatalf("view walk count=%d want=%d, err=%v", count, want, err)
		}
	}
	walk(v, 2)
	// walk uses ReadAt through a section reader, leaving append's file offset intact.
	appendSyncTestCommand(t, j, "INCR", "k")
	walk(v, 2)
	newView, err := j.pendingView()
	if err != nil {
		t.Fatal(err)
	}
	defer newView.f.Close()
	walk(newView, 3)
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	walk(v, 2)
	walk(newView, 3)
	if _, err := j.pendingView(); !errors.Is(err, errSyncJournalCorrupt) {
		t.Fatalf("closed journal view accepted: %v", err)
	}
	sentinel := errors.New("stop walking")
	called := 0
	err = v.walk(func(syncRecord, syncApplyCheckpoint, syncApplyCheckpoint, int64) error {
		called++
		return sentinel
	})
	if !errors.Is(err, sentinel) || called != 1 {
		t.Fatalf("callback error not preserved: calls=%d err=%v", called, err)
	}
}

func TestSyncPendingViewRejectsCorruption(t *testing.T) {
	for _, mode := range []string{"crc", "offset_with_valid_crc", "truncated", "end_mismatch"} {
		t.Run(mode, func(t *testing.T) {
			j, _ := captureJournal(t, 1<<20)
			if err := j.append(captureSnapshot("127.0.0.1:12345")); err != nil {
				t.Fatal(err)
			}
			commandAt := j.size
			appendSyncTestCommand(t, j, "INCR", "k")
			v, err := j.pendingView()
			if err != nil {
				t.Fatal(err)
			}
			defer v.f.Close()
			switch mode {
			case "crc":
				var b [1]byte
				if _, err := j.f.ReadAt(b[:], commandAt+4); err != nil {
					t.Fatal(err)
				}
				b[0] ^= 1
				if _, err := j.f.WriteAt(b[:], commandAt+4); err != nil {
					t.Fatal(err)
				}
			case "offset_with_valid_crc":
				record, err := readSyncRecord(io.NewSectionReader(j.f, commandAt, v.size-commandAt))
				if err != nil {
					t.Fatal(err)
				}
				record.Position.Offset++
				payload, err := json.Marshal(record)
				if err != nil {
					t.Fatal(err)
				}
				frame := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
				frame = binary.BigEndian.AppendUint32(frame, crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)))
				frame = append(frame, payload...)
				if int64(len(frame)) != v.size-commandAt {
					t.Fatal("fixture changed frame length")
				}
				if _, err := j.f.WriteAt(frame, commandAt); err != nil {
					t.Fatal(err)
				}
			case "truncated":
				if err := j.f.Truncate(v.size - 1); err != nil {
					t.Fatal(err)
				}
			case "end_mismatch":
				v.end.Offset++
			}
			called := 0
			err = v.walk(func(syncRecord, syncApplyCheckpoint, syncApplyCheckpoint, int64) error {
				called++
				return nil
			})
			if !errors.Is(err, errSyncJournalCorrupt) {
				t.Fatalf("invalid view accepted: %v", err)
			}
			wantCalls := 1
			if mode == "end_mismatch" {
				wantCalls = 2
			}
			if called != wantCalls {
				t.Fatalf("callback saw corrupt record: got %d calls, want %d", called, wantCalls)
			}
		})
	}
}
