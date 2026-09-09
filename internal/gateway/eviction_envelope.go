package gateway

// G6b-2a: isolated wire-format experiment, not a cold applier or an eviction
// executor. The caller must supply an independently trusted intent. Matching
// source bytes is NOT authorization, cold commit evidence, or crash recovery.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
)

const evictionProbePrefix = syncMetadataPrefix + "{g6b-probe}:evict:"

var errEvictionEnvelope = errors.New("eviction envelope does not match expected operation")

type evictionIntent struct {
	Version             int    `json:"version"`
	TaskID              string `json:"task_id"`
	OperationID         string `json:"operation_id"`
	Epoch               uint64 `json:"epoch"`
	History             string `json:"history"`
	Key                 []byte `json:"key"` // base64 in JSON; preserves non-UTF-8/empty keys
	ValueSHA256         string `json:"value_sha256"`
	PreviousOperationID string `json:"previous_operation_id,omitempty"`
}

func canonicalEvictionHex(s string, size int) bool {
	if len(s) != size*2 {
		return false
	}
	decoded, err := hex.DecodeString(s)
	return err == nil && len(decoded) == size && hex.EncodeToString(decoded) == s
}

func (i evictionIntent) valid() bool {
	versionOK := i.Version == 1 && i.PreviousOperationID == ""
	if i.Version == 2 {
		versionOK = canonicalEvictionHex(i.PreviousOperationID, 16) && i.PreviousOperationID < i.OperationID
	}
	return versionOK && ValidateIsolatedSyncTaskID(i.TaskID) == nil &&
		canonicalEvictionHex(i.OperationID, 16) && i.OperationID != strings.Repeat("0", 32) &&
		i.Epoch > 0 && canonicalEvictionHex(i.History, 20) && i.History != strings.Repeat("0", 40) &&
		i.Key != nil && len(i.Key) <= 64<<10 && !bytes.HasPrefix(i.Key, []byte(syncMetadataPrefix)) &&
		canonicalEvictionHex(i.ValueSHA256, sha256.Size)
}

func (i evictionIntent) encode() string {
	b, _ := json.Marshal(i)
	return string(b)
}

func (i evictionIntent) markerKey() string {
	// Probe-only namespace. Arbitrary Cluster business keys are NOT guaranteed
	// to share this slot; production slot-local metadata layout is undecided.
	return evictionProbePrefix + i.TaskID + ":" + i.OperationID
}

func (i evictionIntent) frames() [][]byte {
	payload := i.encode()
	return [][]byte{
		encodeCommand("MULTI"),
		encodeCommand("SET", i.markerKey(), "begin:"+payload),
		encodeCommand("UNLINK", string(i.Key)),
		encodeCommand("SET", i.markerKey(), "complete:"+payload),
		encodeCommand("EXEC"),
	}
}

// This records a match of an exact source frame group, not cold-layer progress.
// No source record or mutable key buffer is retained in the result.
type evictionEnvelopeEvidence struct {
	Before       syncPosition
	End          syncPosition
	IntentSHA256 string
	WireSHA256   string
}

func inspectEvictionEnvelope(before syncPosition, records []syncRecord, expected evictionIntent) (evictionEnvelopeEvidence, error) {
	bad := evictionEnvelopeEvidence{}
	if !expected.valid() || (len(records) != 5 && len(records) != 6) || before.History != expected.History ||
		before.Source == "" || len(before.Source) > 256 || syncCanonicalEndpoint(before.Source) != before.Source ||
		before.Offset < 0 || before.Offset == math.MaxInt64 {
		return bad, errEvictionEnvelope
	}
	want := expected.frames()
	// Real Redis 7.2.5 inserts SELECT 0 just after MULTI when the replication
	// stream has not selected a DB yet. Permit only that exact location/value.
	// Without SELECT, the caller must already have established DB 0 context.
	if len(records) == 6 {
		want = append([][]byte{want[0], encodeCommand("SELECT", "0")}, want[1:]...)
	}
	previous := before
	h := sha256.New()
	// Bind the digest to the origin, history and starting byte offset too.
	binding, _ := json.Marshal(before)
	h.Write(binding)
	for index, r := range records {
		if r.Kind != "command" || !bytes.Equal(r.Data, want[index]) ||
			r.Position.Source != before.Source || r.Position.History != before.History ||
			previous.Offset > math.MaxInt64-int64(len(r.Data))-1 ||
			r.Position.Offset != previous.Offset+int64(len(r.Data)) {
			return bad, errEvictionEnvelope
		}
		h.Write(r.Data)
		previous = r.Position
	}
	intentHash := sha256.Sum256([]byte(expected.encode()))
	return evictionEnvelopeEvidence{
		Before: before, End: previous,
		IntentSHA256: hex.EncodeToString(intentHash[:]), WireSHA256: hex.EncodeToString(h.Sum(nil)),
	}, nil
}
