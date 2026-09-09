package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// The atomic guard closes the gap between checking residency and executing a
// business command, including a late eviction whose reply was lost. Only the
// explicit NOT EXECUTED response can trigger a business retry.
const residencyBusinessScript = `
redis.replicate_commands()
local m = redis.call('GET', KEYS[2])
if m then
  local cold = string.sub(m, 1, 5) == 'cold:'
  if not cold and string.sub(m, 1, 4) ~= 'hot:' then return redis.error_reply('ERR residency metadata invalid') end
  local payload = string.sub(m, cold and 6 or 5)
  local ok, i = pcall(cjson.decode, payload)
  if not ok or i.version ~= 2 or i.key ~= ARGV[1] or i.history ~= ARGV[2] or
     string.match(payload, '"epoch":(%d+)') ~= ARGV[3] or redis.call('PTTL', KEYS[2]) ~= -1 then
    return redis.error_reply('ERR residency metadata invalid')
  end
  if cold then return {0, m} end
end
local args = {}
for j = 4, #ARGV do args[#args+1] = ARGV[j] end
return {1, redis.pcall(unpack(args))}
`

const residencyEvictScript = `
redis.replicate_commands()
if #KEYS ~= 3 or #ARGV ~= 2 then return redis.error_reply('ERR residency arguments') end
local payload = ARGV[2]
local ok, i = pcall(cjson.decode, payload)
if not ok then return redis.error_reply('ERR residency intent') end
local old = redis.call('GET', KEYS[3])
if old == 'cold:' .. payload then return 2 end
if old == 'hot:' .. payload then return 3 end
if old then
  if string.sub(old, 1, 4) ~= 'hot:' then return redis.error_reply('ERR another key is cold') end
  local good, prior = pcall(cjson.decode, string.sub(old, 5))
  if not good or prior.operation_id >= i.operation_id then return redis.error_reply('ERR stale residency operation') end
end
if redis.call('GET', KEYS[2]) ~= 'prepare:' .. payload or redis.call('PTTL', KEYS[2]) ~= -1 then
  return redis.error_reply('ERR residency preparation changed')
end
if redis.call('TYPE', KEYS[1]).ok ~= 'string' or redis.call('PTTL', KEYS[1]) ~= -1 or redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return redis.error_reply('ERR residency value changed')
end
redis.call('SET', KEYS[3], 'cold:' .. payload)
redis.call('UNLINK', KEYS[1])
return 1
`

const residencyWarmScript = `
redis.replicate_commands()
if #KEYS ~= 2 or #ARGV ~= 2 then return redis.error_reply('ERR residency arguments') end
local old = redis.call('GET', KEYS[2])
local hot = 'hot:' .. string.sub(ARGV[1], 6)
if old == hot then return 2 end
if old ~= ARGV[1] or redis.call('PTTL', KEYS[2]) ~= -1 then
  return redis.error_reply('ERR residency changed')
end
if redis.call('EXISTS', KEYS[1]) ~= 0 and
   (redis.call('TYPE', KEYS[1]).ok ~= 'string' or redis.call('PTTL', KEYS[1]) ~= -1 or redis.call('GET', KEYS[1]) ~= ARGV[2]) then
  return redis.error_reply('ERR residency value conflict')
end
redis.call('SET', KEYS[1], ARGV[2])
redis.call('SET', KEYS[2], hot)
return 1
`

func (s *Service) guardResidency(q *request) {
	if s.preparation == nil || !s.preparation.options.ManualResidency || q.local != nil {
		return
	}
	keys, err := requestKeys(q.args)
	if err != nil || len(keys) != 1 {
		return
	}
	binding := s.preparation.options.Confirmation.Binding
	args := []string{"EVAL", residencyBusinessScript, "2", string(keys[0]), residencyKey(binding.TaskID, keys[0]), base64.StdEncoding.EncodeToString(keys[0]), binding.History, strconv.FormatUint(binding.Epoch, 10)}
	for _, arg := range q.args {
		args = append(args, string(arg))
	}
	q.raw, q.guarded = encodeCommand(args...), true
	if len(q.raw) > s.cfg.Limits.MaxBytes {
		q.local = []byte("-ERR guarded command exceeds request limit\r\n")
	}
}

func (s *Service) residencyReply(ctx context.Context, c net.Conn, r *Reader, q request, raw []byte) ([]byte, error) {
	if bytes.HasPrefix(raw, []byte("*2\r\n:1\r\n")) {
		return raw[len("*2\r\n:1\r\n"):], nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	for attempt := 0; ; attempt++ {
		if bytes.HasPrefix(raw, []byte("*2\r\n:1\r\n")) {
			return raw[len("*2\r\n:1\r\n"):], nil
		}
		if !bytes.HasPrefix(raw, []byte("*2\r\n:0\r\n")) {
			return raw, nil
		}
		if attempt == 3 {
			return []byte("-TRYAGAIN residency changed; command not executed\r\n"), nil
		}
		marker, err := residencyBulk(raw[len("*2\r\n:0\r\n"):])
		if err != nil {
			return []byte("-TRYAGAIN cold key unavailable; command not executed\r\n"), nil
		}
		warmErr := s.preparation.warmResidency(ctx, q.args[1], string(marker))
		if ctx.Err() != nil {
			return []byte("-TRYAGAIN cold key unavailable; command not executed\r\n"), nil
		}
		c.SetDeadline(time.Now().Add(s.cfg.RequestTimeout))
		if err := writeAll(c, q.raw); err != nil {
			return nil, err
		}
		raw, err = r.ReadFrame()
		if err != nil {
			return nil, err
		} // Never replay an uncertain INCR/SET.
		// Another request may already have warmed and modified the key while
		// our cold read failed or observed a newer cold value. Recheck via the
		// atomic guard before reporting an outage; never restore that old value.
		if warmErr != nil && bytes.HasPrefix(raw, []byte("*2\r\n:0\r\n")) {
			return []byte("-TRYAGAIN cold key unavailable; command not executed\r\n"), nil
		}
	}
}

func residencyBulk(raw []byte) ([]byte, error) {
	// Intents contain a base64 key and can exceed the business-value limit.
	r, _ := NewReader(bytes.NewReader(raw), DefaultConfig().Limits)
	frame, err := r.ReadFrame()
	if err != nil || len(frame) != len(raw) || len(raw) < 6 || raw[0] != '$' {
		return nil, errConfirmationRPC
	}
	at := bytes.Index(raw, []byte("\r\n"))
	if at < 2 || string(raw) == "$-1\r\n" {
		return nil, errConfirmationRPC
	}
	return raw[at+2 : len(raw)-2], nil
}

func (h *hotPreparation) warmResidency(ctx context.Context, key []byte, marker string) error {
	payload, ok := strings.CutPrefix(marker, "cold:")
	var intent evictionIntent
	if !ok || decodeConfirmation(strings.NewReader(payload), &intent) != nil || !h.options.Confirmation.Binding.accepts(intent) || intent.Version != 2 || intent.encode() != payload || !bytes.Equal(key, intent.Key) {
		return errConfirmationRPC
	}
	target := syncColdTarget{address: h.options.Confirmation.Binding.Cold, passwordFile: h.options.ColdPasswordFile, timeout: h.options.Confirmation.Timeout, taskID: intent.TaskID, evictionReceipt: true}
	target.testDial = h.options.coldTestDial
	cold, closeCold, err := target.open(ctx)
	if err != nil {
		return err
	}
	defer closeCold()
	value, err := cold.pendingValue([]string{"GET", string(key)})
	hash := sha256.Sum256(value.Data)
	if err != nil || !value.Exists || hex.EncodeToString(hash[:]) != intent.ValueSHA256 {
		return errSyncTargetDrift
	}
	return h.residencyCall(ctx, "EVAL", residencyWarmScript, "2", string(key), intent.residencyKey(), marker, string(value.Data))
}

func (h *hotPreparation) residencyCall(ctx context.Context, args ...string) error {
	c, r, err := h.service.openBackend(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	c.SetDeadline(time.Now().Add(h.service.cfg.RequestTimeout))
	if err := writeAll(c, encodeCommand(args...)); err != nil {
		return err
	}
	raw, err := r.ReadFrame()
	if err != nil {
		return err
	}
	if string(raw) != ":1\r\n" && string(raw) != ":2\r\n" && string(raw) != ":3\r\n" {
		return errConfirmationRPC
	}
	return nil
}

// Caller owns h.gate. A failed request leaves hot data available; retries use
// the same durable preparation ID, never issue a second non-idempotent write.
func (h *hotPreparation) manualEvict(ctx context.Context, req hotPreparationRequest) error {
	if !h.options.ManualResidency || !h.options.BoundedMarkers {
		return errConfirmationRPC
	}
	var state evictionOperationState
	load := func() bool {
		op, err := openEvictionOperation(filepath.Join(h.options.Confirmation.OperationDir, "confirmation-operation.log"), h.options.Confirmation.Binding.identity(), nil, nil)
		if err != nil {
			return false
		}
		defer op.close()
		state, err = op.snapshot()
		return err == nil && state.Finished && state.Intent.OperationID == req.OperationID && bytes.Equal(state.Intent.Key, req.Key)
	}
	if h.confirmation != nil || !load() {
		if !h.finishing {
			if err := h.prepare(ctx, req); err != nil {
				return err
			}
		}
		for {
			if err := h.requestFinish(ctx, req); err == nil {
				break
			}
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		if !load() {
			return errConfirmationRPC
		}
	}
	if err := h.verifyRetainedMarker(ctx, state.Intent); err != nil {
		return err
	}
	// Briefly drain this key. The atomic source guard remains authoritative even
	// after an uncertain EVAL or Gateway crash, so the barrier need not stay stuck.
	b, err := h.service.admission.reserve(string(req.Key))
	if err != nil {
		return err
	}
	defer func() {
		g := b.set
		g.mu.Lock()
		defer g.mu.Unlock()
		if b.entry.barrier == b {
			b.entry.barrier = nil
			b.signalLocked()
			g.removeIdleLocked(b.entry)
		}
	}()
	select {
	case <-b.wake:
	case <-ctx.Done():
		return ctx.Err()
	}
	b.set.mu.Lock()
	ready := !b.set.closed && b.entry.barrier == b && b.entry.active == 0 && !b.entry.uncertain
	b.set.mu.Unlock()
	if !ready {
		return errKeyBarrierBusy
	}
	// Read the original confirmed value from cold; Lua compares the exact hot
	// bytes before deleting. A concurrent newer value causes a harmless refusal.
	target := syncColdTarget{address: h.options.Confirmation.Binding.Cold, passwordFile: h.options.ColdPasswordFile, timeout: h.options.Confirmation.Timeout, taskID: state.Intent.TaskID, evictionReceipt: true}
	c, closeCold, err := target.open(ctx)
	if err != nil {
		return err
	}
	defer closeCold()
	v, err := c.pendingValue([]string{"GET", string(req.Key)})
	hash := sha256.Sum256(v.Data)
	if err != nil || !v.Exists || hex.EncodeToString(hash[:]) != state.Intent.ValueSHA256 {
		return errSyncTargetDrift
	}
	return h.residencyCall(ctx, "EVAL", residencyEvictScript, "3", string(req.Key), state.Intent.prepareKey(), state.Intent.residencyKey(), string(v.Data), state.Intent.encode())
}
