package gateway

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

func nextOperationID(previous string) (string, error) {
	if !canonicalEvictionHex(previous, 16) {
		return "", errEvictionOperation
	}
	id, _ := hex.DecodeString(previous)
	for n := len(id) - 1; n >= 0; n-- {
		id[n]++
		if id[n] != 0 {
			return hex.EncodeToString(id), nil
		}
	}
	return "", errEvictionOperation // Never wrap back to a reusable identity.
}

// h.gate serializes allocation; prepare persists the new intent before EVAL.
// No clock/random UUID and no separate per-key allocator database are needed.
func (h *hotPreparation) assignOperationID(ctx context.Context, req *hotPreparationRequest) (bool, error) {
	if !h.options.ManualResidency || !h.options.BoundedMarkers {
		return false, errConfirmationRPC
	}
	check := *req
	check.OperationID = strings.Repeat("0", 31) + "1"
	if !check.valid() {
		return false, errConfirmationRPC
	}
	if h.request != nil {
		if !bytes.Equal(h.request.Key, req.Key) {
			return false, errKeyBarrierBusy
		}
		req.OperationID = h.request.OperationID
		return false, nil
	}
	// An already-cold key needs no new preparation, including when a different
	// key has since used the task's fixed last-operation file.
	c, r, err := h.service.openBackend(ctx)
	if err != nil {
		return false, err
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	key := residencyKey(h.options.Confirmation.Binding.TaskID, req.Key)
	if err := writeAll(c, encodeCommand("GET", key)); err != nil {
		return false, err
	}
	raw, err := r.ReadFrame()
	if err != nil {
		return false, err
	}
	if string(raw) != "$-1\r\n" {
		value, err := residencyBulk(raw)
		if err != nil {
			return false, err
		}
		prefix := "hot:"
		cold := bytes.HasPrefix(value, []byte("cold:"))
		if cold {
			prefix = "cold:"
		}
		payload, ok := strings.CutPrefix(string(value), prefix)
		var intent evictionIntent
		if !ok || decodeConfirmation(strings.NewReader(payload), &intent) != nil || !h.options.Confirmation.Binding.accepts(intent) || intent.Version != 2 || intent.encode() != payload || !bytes.Equal(intent.Key, req.Key) {
			return false, errConfirmationRPC
		}
		if err := writeAll(c, encodeCommand("PTTL", key)); err != nil {
			return false, err
		}
		raw, err = r.ReadFrame()
		if err != nil || string(raw) != ":-1\r\n" {
			return false, errConfirmationRPC
		}
		if cold {
			req.OperationID = intent.OperationID
			return true, nil
		}
	}
	path := filepath.Join(h.options.Confirmation.OperationDir, "confirmation-operation.log")
	previous := strings.Repeat("0", 32)
	if _, err := os.Lstat(path); err == nil {
		op, err := openEvictionOperation(path, h.options.Confirmation.Binding.identity(), nil, nil)
		if err != nil {
			return false, err
		}
		defer op.close()
		state, err := op.snapshot()
		if err != nil || !state.Finished || state.Intent.Version != 2 {
			return false, errEvictionOperation
		}
		previous = state.Intent.OperationID
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	req.OperationID, err = nextOperationID(previous)
	return false, err
}
