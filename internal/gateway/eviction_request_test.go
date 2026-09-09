package gateway

import (
	"strings"
	"testing"
)

func TestDecodeEvictionRequest(t *testing.T) {
	for _, raw := range []string{`{"key": "aw=="}`, ` { "key":"aw==", "operation_id":"" } `} {
		var req hotPreparationRequest
		if err := decodeEvictionRequest(strings.NewReader(raw), &req); err != nil || string(req.Key) != "k" || req.OperationID != "" {
			t.Fatal(req, err)
		}
	}
	for _, raw := range []string{`{}`, `{"key":null}`, `{"key":"aw==","key":"bA=="}`, `{"key":"aw==","extra":1}`, `{"key":"aw=="} {}`, `{"key":[1,2]}`, `{"key":"not-base64"}`} {
		var req hotPreparationRequest
		if decodeEvictionRequest(strings.NewReader(raw), &req) == nil {
			t.Fatal("accepted", raw)
		}
	}
}
