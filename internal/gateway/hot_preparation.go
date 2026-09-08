package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
)

const hotPreparationPath = "/internal/v1/hot-preparation"

// This script only adds the reserved preparation marker. It never changes the
// business key, removes hot data, or treats the marker as a cold acknowledgement.
const hotPreparationScript = `
redis.replicate_commands()
if #KEYS ~= 2 or #ARGV ~= 2 then return redis.error_reply('ERR prepare arguments') end
if redis.call('TYPE', KEYS[1]).ok ~= 'string'
   or redis.call('PTTL', KEYS[1]) ~= -1
   or redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return redis.error_reply('ERR prepare key changed')
end
if redis.call('EXISTS', KEYS[2]) ~= 0 then
  if redis.call('TYPE', KEYS[2]).ok == 'string'
     and redis.call('PTTL', KEYS[2]) == -1
     and redis.call('GET', KEYS[2]) == ARGV[2] then return 2 end
  return redis.error_reply('ERR prepare marker conflict')
end
redis.call('SET', KEYS[2], ARGV[2])
return 1
`

type hotPreparationOptions struct {
	Confirmation   IsolatedConfirmationGatewayOptions
	AdminTokenFile string
	ExclusiveHot   bool // Operator prerequisite, NOT distributed fencing.
}

type hotPreparationRequest struct {
	OperationID string `json:"operation_id"`
	Key         []byte `json:"key"`
}

func (r hotPreparationRequest) valid() bool {
	return canonicalEvictionHex(r.OperationID, 16) && r.OperationID != strings.Repeat("0", 32) &&
		r.Key != nil && len(r.Key) <= 64<<10 && !bytes.HasPrefix(r.Key, []byte(syncMetadataPrefix))
}

type hotPreparation struct {
	service            *Service
	options            hotPreparationOptions
	token, syncerToken string
	lock               *os.File
	gate               chan struct{}
	ctx                context.Context
	cancel             context.CancelFunc
	stopping           atomic.Bool
	closed, poisoned   bool // gate-protected
	barrier            *keyBarrier
	request            *hotPreparationRequest
	confirmation       *IsolatedConfirmationGateway
	prepared           bool // Online observation only; never recovered from a local receipt.
	workspace          int64
}

// Called before Service starts any workers. Existing operations restore their
// barrier before the returned controller can process management requests.
// The caller must also hold the durable boot latch before admitting traffic.
func openHotPreparation(s *Service, o hotPreparationOptions) (*hotPreparation, error) {
	if s.admission == nil || !o.ExclusiveHot || o.Confirmation.Binding.GatewayID != s.cfg.GatewayID ||
		o.Confirmation.Binding.Source != s.cfg.RedisAddress {
		return nil, errConfirmationRPC
	}
	syncerToken, err := validateConfirmationGateway(o.Confirmation)
	if err != nil {
		return nil, err
	}
	token, err := confirmationToken(o.AdminTokenFile)
	if err != nil || token == syncerToken || token == s.frontendPassword || token == s.backendPassword {
		return nil, errConfirmationRPC
	}
	lock, err := openConfirmationBinding(o.Confirmation.OperationDir, o.Confirmation.Binding)
	if err != nil {
		return nil, err
	}
	workspace := s.cfg.workspaceBytes()
	if !s.acquireWorkspace(workspace) {
		lock.Close()
		return nil, errConfirmationRPC
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &hotPreparation{service: s, options: o, token: token, syncerToken: syncerToken, lock: lock, gate: make(chan struct{}, 1), ctx: ctx, cancel: cancel, workspace: workspace}
	path := filepath.Join(o.Confirmation.OperationDir, "confirmation-operation.log")
	if _, err := os.Lstat(path); err == nil {
		op, err := openEvictionOperation(path, o.Confirmation.Binding.identity(), nil, nil)
		if err != nil {
			h.Close()
			return nil, err
		}
		state, err := op.snapshot()
		if err != nil || !o.Confirmation.Binding.accepts(state.Intent) {
			op.close()
			h.Close()
			return nil, errConfirmationRPC
		}
		b, err := s.admission.restore(string(state.Intent.Key))
		if err != nil {
			op.close()
			h.Close()
			return nil, err
		}
		h.barrier = b
		h.request = &hotPreparationRequest{state.Intent.OperationID, bytes.Clone(state.Intent.Key)}
		h.attach(op)
	} else if !errors.Is(err, os.ErrNotExist) {
		h.Close()
		return nil, errConfirmationRPC
	}
	return h, nil
}

func (h *hotPreparation) attach(op *evictionOperation) {
	h.confirmation = &IsolatedConfirmationGateway{options: h.options.Confirmation, token: h.syncerToken,
		client: confirmationHTTPClient(h.options.Confirmation.Timeout), op: op, barriers: h.service.admission,
		barrier: h.barrier, gate: make(chan struct{}, 1), borrowed: true}
}

// Caller holds gate. Failures retain the same operation/barrier. There is no
// cancel/reset endpoint and no path that treats a lost EVAL reply as not sent.
func (h *hotPreparation) prepare(ctx context.Context, req hotPreparationRequest) error {
	if !req.valid() || h.poisoned || h.stopping.Load() {
		return errConfirmationRPC
	}
	if h.request != nil && (req.OperationID != h.request.OperationID || !bytes.Equal(req.Key, h.request.Key)) {
		return errConfirmationRPC
	}
	if h.request == nil {
		b, err := h.service.admission.reserve(string(req.Key))
		if err != nil {
			return err
		}
		h.barrier = b
		h.request = &hotPreparationRequest{req.OperationID, append([]byte{}, req.Key...)}
	}
	select {
	case <-h.barrier.wake:
	case <-ctx.Done():
		return errConfirmationRPC
	}
	g := h.service.admission
	g.mu.Lock()
	ready := !g.closed && !h.barrier.entry.uncertain && h.barrier.entry.active == 0 && h.barrier.entry.barrier == h.barrier
	g.mu.Unlock()
	if !ready {
		return errConfirmationRPC
	}
	h.prepared = false
	c, r, err := h.service.openBackend(ctx)
	if err != nil {
		return errConfirmationRPC
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		c.SetDeadline(deadline)
	}
	call := func(args ...string) ([]byte, error) {
		if err := writeAll(c, encodeCommand(args...)); err != nil {
			return nil, err
		}
		return r.ReadFrame()
	}
	check := func() bool {
		raw, err := call("INFO", "replication")
		if err != nil {
			return false
		}
		p, code := parseSyncSourceInfo(raw)
		if code != "none" || p.History != h.options.Confirmation.Binding.History {
			return false
		}
		raw, err = call("CONFIG", "GET", "maxmemory-policy")
		return err == nil && string(raw) == "*2\r\n$16\r\nmaxmemory-policy\r\n$10\r\nnoeviction\r\n"
	}
	if !check() {
		return errConfirmationRPC
	}
	raw, err := call("GET", string(req.Key))
	if err != nil {
		return errConfirmationRPC
	}
	value, err := hotPreparationValue(raw)
	if err != nil {
		return err
	}
	raw, err = call("PTTL", string(req.Key))
	if err != nil || string(raw) != ":-1\r\n" {
		return errConfirmationRPC
	}
	if h.confirmation == nil {
		i, err := newEvictionIntent(h.options.Confirmation.Binding.TaskID, h.options.Confirmation.Binding.Epoch,
			h.options.Confirmation.Binding.History, req.Key, value)
		if err != nil {
			return err
		}
		i.OperationID = req.OperationID
		// From this point an I/O error requires recovery; never create a new
		// intent over a possibly durable file, or release the protected key.
		h.poisoned = true
		op, err := openEvictionOperation(filepath.Join(h.options.Confirmation.OperationDir, "confirmation-operation.log"),
			h.options.Confirmation.Binding.identity(), &i, nil)
		if err != nil {
			return err
		}
		h.attach(op)
		h.poisoned = false
	}
	state, err := h.confirmation.op.snapshot()
	digest := sha256.Sum256(value)
	if err != nil || state.Intent.ValueSHA256 != hex.EncodeToString(digest[:]) {
		return errConfirmationRPC
	}
	g.mu.Lock()
	ready = !g.closed && !h.barrier.entry.uncertain && h.barrier.entry.active == 0 && !h.stopping.Load()
	if ready {
		h.barrier.dispatched = true
	}
	g.mu.Unlock()
	if !ready || ctx.Err() != nil {
		return errConfirmationRPC
	}
	raw, err = call("EVAL", hotPreparationScript, "2", string(req.Key), state.Intent.prepareKey(), string(value), "prepare:"+state.Intent.encode())
	if err != nil || (string(raw) != ":1\r\n" && string(raw) != ":2\r\n") || !check() || ctx.Err() != nil || h.stopping.Load() {
		return errConfirmationRPC
	}
	h.prepared = true
	return nil
}

func hotPreparationValue(raw []byte) ([]byte, error) {
	i := bytes.Index(raw, []byte("\r\n"))
	if i < 2 || raw[0] != '$' {
		return nil, errConfirmationRPC
	}
	n, err := strconv.Atoi(string(raw[1:i]))
	if err != nil || n < 0 || n > syncPendingValueLimit || len(raw)-i-4 != n || !bytes.HasSuffix(raw, []byte("\r\n")) {
		return nil, errConfirmationRPC
	}
	return raw[i+2 : i+2+n], nil
}

func (h *hotPreparation) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !confirmationAuthorized(r, h.token) {
		http.Error(w, "unauthorized", 401)
		return
	}
	if h.stopping.Load() {
		http.Error(w, "stopping", 503)
		return
	}
	select {
	case h.gate <- struct{}{}:
		defer func() { <-h.gate }()
	default:
		http.Error(w, "operation busy", 409)
		return
	}
	if h.closed || h.stopping.Load() {
		http.Error(w, "stopping", 503)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodGet && r.URL.Path == hotPreparationPath {
		state := "idle"
		if h.request != nil {
			state = "held"
		}
		if h.confirmation != nil {
			state = "intent_recorded_requires_verification"
		}
		if h.prepared {
			state = "prepare_observed_held"
		}
		if h.poisoned {
			state = "persistence_uncertain_held"
		}
		json.NewEncoder(w).Encode(struct {
			State string `json:"state"`
		}{state})
		return
	}
	if r.Method != http.MethodPost || (r.URL.Path != hotPreparationPath && r.URL.Path != hotPreparationPath+"/confirm") {
		http.Error(w, "unsupported operation", 405)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.options.Confirmation.Timeout)
	defer cancel()
	stop := context.AfterFunc(h.ctx, cancel)
	defer stop()
	var req hotPreparationRequest
	if decodeConfirmation(http.MaxBytesReader(w, r.Body, confirmationRPCMaxBytes), &req) != nil || !req.valid() {
		http.Error(w, "invalid operation", 400)
		return
	}
	var err error
	if r.URL.Path == hotPreparationPath {
		err = h.prepare(ctx, req)
	} else if h.confirmation == nil || h.request.OperationID != req.OperationID || !bytes.Equal(h.request.Key, req.Key) {
		err = errConfirmationRPC
	} else {
		err = h.confirmation.Confirm(ctx)
	}
	if err != nil || ctx.Err() != nil || h.stopping.Load() {
		http.Error(w, "operation unavailable; protection retained", 503)
		return
	}
	w.Write([]byte("{\"state\":\"held\"}\n"))
}

func (h *hotPreparation) Close() {
	h.stopping.Store(true)
	h.cancel()
	h.gate <- struct{}{}
	defer func() { <-h.gate }()
	if h.closed {
		return
	}
	h.closed = true
	if h.confirmation != nil {
		h.confirmation.Close()
	}
	h.lock.Close()
	h.service.releaseWorkspace(h.workspace)
}

// This is deliberately narrower than passthrough key extraction. Writes must
// fit the existing cold replay implementation; EVAL remains internal-only.
func hotPreparationCommand(args [][]byte) bool {
	if len(args) == 0 {
		return false
	}
	switch strings.ToUpper(string(args[0])) {
	case "PING", "ECHO":
		return true
	case "GET", "TYPE", "TTL", "PTTL", "EXISTS", "INCR", "DEL":
		return len(args) == 2 && len(args[1]) <= 64<<10
	case "SET":
		return len(args) == 3 && len(args[1]) <= 64<<10 && len(args[2]) <= syncPendingValueLimit
	default:
		return false
	}
}
