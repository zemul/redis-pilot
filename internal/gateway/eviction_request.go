package gateway

import (
	"encoding/base64"
	"encoding/json"
	"io"
)

// Human-facing JSON permits whitespace, field order and an omitted ID. Keep
// the canonical, hash-bound decoder for internal receipts/intents unchanged.
func decodeEvictionRequest(r io.Reader, req *hotPreparationRequest) error {
	d := json.NewDecoder(io.LimitReader(r, confirmationRPCMaxBytes+1))
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		return errConfirmationRPC
	}
	seen := map[string]bool{}
	for d.More() {
		tok, err := d.Token()
		if err != nil {
			return errConfirmationRPC
		}
		name, ok := tok.(string)
		if !ok || seen[name] || (name != "key" && name != "operation_id") {
			return errConfirmationRPC
		}
		seen[name] = true
		tok, err = d.Token()
		if err != nil {
			return errConfirmationRPC
		}
		value, ok := tok.(string)
		if !ok {
			return errConfirmationRPC
		}
		if name == "operation_id" {
			req.OperationID = value
		} else {
			key, err := base64.StdEncoding.DecodeString(value)
			if err != nil || base64.StdEncoding.EncodeToString(key) != value || len(key) > 64<<10 {
				return errConfirmationRPC
			}
			req.Key = key
		}
	}
	tok, err = d.Token()
	if err != nil || tok != json.Delim('}') || !seen["key"] {
		return errConfirmationRPC
	}
	if _, err = d.Token(); err != io.EOF {
		return errConfirmationRPC
	}
	return nil
}
