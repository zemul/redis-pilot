package gateway

// Bounded RDB v11 String decoder for the isolated no-TTL applier prototype.
// No compressed values, expiry, modules, other DBs or non-String objects.
// Format reference: Redis 7.2 src/rdb.h; this is not a general RDB importer.

import (
	"encoding/binary"
	"errors"
	"strconv"
)

const syncStateKeys = 256
const syncStateBytes = 512 << 10
const syncMetadataPrefix = "__pilot_sync_v1__:"
const syncCheckpointKey = syncMetadataPrefix + "{isolated}:checkpoint"

var errSyncUnsupported = errors.New("unsupported record in isolated no-TTL sync prototype")

// Reflected CRC64/Jones, initial zero and no final complement (Redis RDB).
func syncRDBCRC(b []byte) uint64 {
	var crc uint64
	for _, v := range b {
		crc ^= uint64(v)
		for bit := 0; bit < 8; bit++ {
			if crc&1 != 0 {
				crc = crc>>1 ^ 0x95ac9329ac4bc9b5
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}

type syncRDBReader struct {
	b []byte
	i int
}

func (r *syncRDBReader) take(n int) ([]byte, error) {
	if n < 0 || n > len(r.b)-r.i {
		return nil, errSyncJournalCorrupt
	}
	b := r.b[r.i : r.i+n]
	r.i += n
	return b, nil
}

func (r *syncRDBReader) length() (uint64, bool, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, false, err
	}
	switch b[0] >> 6 {
	case 0:
		return uint64(b[0]), false, nil
	case 1:
		v, err := r.take(1)
		if err != nil {
			return 0, false, err
		}
		return uint64(b[0]&63)<<8 | uint64(v[0]), false, nil
	case 3:
		return uint64(b[0] & 63), true, nil
	default:
		switch b[0] {
		case 0x80:
			v, err := r.take(4)
			if err != nil {
				return 0, false, err
			}
			return uint64(binary.BigEndian.Uint32(v)), false, nil
		case 0x81:
			v, err := r.take(8)
			if err != nil {
				return 0, false, err
			}
			return binary.BigEndian.Uint64(v), false, nil
		}
	}
	return 0, false, errSyncJournalCorrupt
}

func (r *syncRDBReader) str() (string, error) {
	n, encoded, err := r.length()
	if err != nil {
		return "", err
	}
	if !encoded {
		if n > syncStateBytes {
			return "", errSyncUnsupported
		}
		b, err := r.take(int(n))
		return string(b), err
	}
	if n > 2 {
		return "", errSyncUnsupported
	} // includes LZF
	b, err := r.take(1 << n)
	if err != nil {
		return "", err
	}
	var v int64
	switch n {
	case 0:
		v = int64(int8(b[0]))
	case 1:
		v = int64(int16(binary.LittleEndian.Uint16(b)))
	case 2:
		v = int64(int32(binary.LittleEndian.Uint32(b)))
	}
	return strconv.FormatInt(v, 10), nil
}

func decodeSyncRDB(b []byte) (map[string]string, error) {
	if len(b) < 18 || len(b) > 8<<20 || string(b[:9]) != "REDIS0011" {
		return nil, errSyncUnsupported
	}
	checksum := binary.LittleEndian.Uint64(b[len(b)-8:])
	if checksum == 0 || checksum != syncRDBCRC(b[:len(b)-8]) {
		return nil, errSyncJournalCorrupt
	}
	r := syncRDBReader{b: b[9 : len(b)-8]}
	state := make(map[string]string)
	selected := false
	for {
		op, err := r.take(1)
		if err != nil {
			return nil, err
		}
		switch op[0] {
		case 255:
			if r.i != len(r.b) {
				return nil, errSyncJournalCorrupt
			}
			return state, checkSyncState(state)
		case 250:
			key, err := r.str()
			if err != nil {
				return nil, err
			}
			value, err := r.str()
			if err != nil {
				return nil, err
			}
			if key == "repl-stream-db" && value != "0" {
				return nil, errSyncUnsupported
			}
		case 254:
			db, enc, err := r.length()
			if err != nil || enc || db != 0 || selected {
				return nil, errSyncUnsupported
			}
			selected = true
		case 251:
			keys, enc, err := r.length()
			if err != nil || enc || !selected || keys > syncStateKeys {
				return nil, errSyncUnsupported
			}
			expires, enc, err := r.length()
			if err != nil || enc || expires != 0 {
				return nil, errSyncUnsupported
			}
		case 0:
			if !selected {
				return nil, errSyncJournalCorrupt
			}
			key, err := r.str()
			if err != nil {
				return nil, err
			}
			value, err := r.str()
			if err != nil {
				return nil, err
			}
			if _, duplicate := state[key]; duplicate {
				return nil, errSyncJournalCorrupt
			}
			state[key] = value
			if err := checkSyncState(state); err != nil {
				return nil, err
			}
		default:
			return nil, errSyncUnsupported
		}
	}
}
