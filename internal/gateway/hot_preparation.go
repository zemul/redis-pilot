package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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
	Confirmation       IsolatedConfirmationGatewayOptions
	AdminTokenFile     string
	ExclusiveHot       bool          // Operator prerequisite, NOT distributed fencing.
	PreparationTimeout time.Duration // Internal inactivity fallback; zero uses 30s.
	BoundedMarkers     bool          // Opt-in v2; ordered operation IDs, no mixed-format migration.
	ManualResidency    bool          // Internal single-key operations; not a production cluster switch.
	ColdPasswordFile   string
	coldTestDial       func(context.Context, string) (net.Conn, error) // test-only transport fault injection
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
	finishing          bool // gate-protected; persisted before requesting remote completion
	wakeFinish         chan struct{}
	finishDone         chan struct{}
	autoFinishTimer    *time.Timer
	autoFinishAt       time.Time // fixed per operation; duplicate requests cannot extend it
}

// Called before Service starts any workers. Existing operations restore their
// barrier before the returned controller can process management requests.
// The caller must also hold the durable boot latch before admitting traffic.
func openHotPreparation(s *Service, o hotPreparationOptions) (*hotPreparation, error) {
	if o.ManualResidency && !o.BoundedMarkers {
		return nil, errConfirmationRPC
	}
	if o.PreparationTimeout == 0 {
		o.PreparationTimeout = 30 * time.Second
	}
	if o.PreparationTimeout < 0 {
		return nil, errConfirmationRPC
	}
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
	if err := checkResidencyLatch(o.Confirmation.OperationDir, o.Confirmation.Binding, o.ManualResidency); err != nil {
		lock.Close()
		return nil, err
	}
	workspace := s.cfg.workspaceBytes()
	if !s.acquireWorkspace(workspace) {
		lock.Close()
		return nil, errConfirmationRPC
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &hotPreparation{service: s, options: o, token: token, syncerToken: syncerToken, lock: lock, gate: make(chan struct{}, 1), ctx: ctx, cancel: cancel, workspace: workspace}
	h.wakeFinish = make(chan struct{}, 1)
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
		if (state.Intent.Version == 2) != o.BoundedMarkers {
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
	if err := h.restoreFinishing(); err != nil {
		h.Close()
		return nil, err
	}
	h.finishDone = make(chan struct{})
	go h.runFinishing(h.finishing)
	return h, nil
}

func (h *hotPreparation) attach(op *evictionOperation) {
	h.confirmation = &IsolatedConfirmationGateway{options: h.options.Confirmation, token: h.syncerToken,
		client: confirmationHTTPClient(h.options.Confirmation.Timeout), op: op, barriers: h.service.admission,
		barrier: h.barrier, gate: make(chan struct{}, 1), borrowed: true}
	h.autoFinishAt = time.Now().Add(h.options.PreparationTimeout)
	wake := h.wakeFinish
	h.autoFinishTimer = time.AfterFunc(h.options.PreparationTimeout, func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	})
}

// Caller holds gate. An attempt with no durable intent and no possible prepare
// dispatch is automatically abandoned. Once intent persistence starts, errors
// retain protection; a lost EVAL reply is never treated as proof of no dispatch.
func (h *hotPreparation) prepare(ctx context.Context, req hotPreparationRequest) error {
	if !req.valid() || h.poisoned || h.stopping.Load() {
		return errConfirmationRPC
	}
	if h.request != nil && (req.OperationID != h.request.OperationID || !bytes.Equal(req.Key, h.request.Key)) {
		return errConfirmationRPC
	}
	if h.request == nil {
		// A terminal operation ID is never adopted as a new operation.
		path := filepath.Join(h.options.Confirmation.OperationDir, "confirmation-operation.log")
		if _, err := os.Lstat(path); err == nil {
			op, err := openEvictionOperation(path, h.options.Confirmation.Binding.identity(), nil, nil)
			if err == nil {
				state, err := op.snapshot()
				op.close()
				if err != nil || !state.Finished || state.Intent.OperationID == req.OperationID {
					return errConfirmationRPC
				}
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		b, err := h.service.admission.reserve(string(req.Key))
		if err != nil {
			return err
		}
		h.barrier = b
		h.request = &hotPreparationRequest{req.OperationID, append([]byte{}, req.Key...)}
	}
	defer h.abandonUnrecorded()
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
		if h.options.BoundedMarkers {
			if err := h.configureBoundedIntent(&i); err != nil {
				return err
			}
		}
		// Retained source markers make delayed old EVALs idempotent. Refuse
		// reusing an older completed ID before creating a new local intent.
		if !h.options.BoundedMarkers {
			raw, err := call("EXISTS", i.prepareKey())
			if err != nil || string(raw) != ":0\r\n" {
				return errConfirmationRPC
			}
		}
		// From this point an I/O error requires recovery; never create a new
		// intent over a possibly durable file, or release the protected key.
		h.poisoned = true
		path := filepath.Join(h.options.Confirmation.OperationDir, "confirmation-operation.log")
		var op *evictionOperation
		if _, statErr := os.Lstat(path); statErr == nil {
			op, err = replaceFinishedOperation(path, h.options.Confirmation.Binding.identity(), i)
		} else if errors.Is(statErr, os.ErrNotExist) {
			op, err = openEvictionOperation(path, h.options.Confirmation.Binding.identity(), &i, nil)
		} else {
			err = statErr
		}
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
	raw, err = call("EVAL", preparationScript(state.Intent), "2", string(req.Key), state.Intent.prepareKey(), string(value), "prepare:"+state.Intent.encode())
	if err != nil || (string(raw) != ":1\r\n" && string(raw) != ":2\r\n") || !check() || ctx.Err() != nil || h.stopping.Load() {
		return errConfirmationRPC
	}
	h.prepared = true
	return nil
}

// Caller holds the controller gate, so no prepare/confirm can start while this
// check and release execute. Only read-only backend requests may have been sent
// without an intent. Keep business request accounting: abandoning a preparation
// does not assert that an unknown business write failed or finished remotely.
func (h *hotPreparation) abandonUnrecorded() {
	if h.barrier == nil || h.confirmation != nil || h.poisoned {
		return
	}
	b := h.barrier
	g := b.set
	g.mu.Lock()
	defer g.mu.Unlock()
	if b.entry.barrier != b || b.dispatched {
		return
	}
	b.entry.barrier = nil
	b.entry.uncertain = false // There is no longer a preparation awaiting this outcome.
	b.signalLocked()
	g.removeIdleLocked(b.entry)
	h.barrier, h.request, h.prepared = nil, nil, false
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
		if h.finishing {
			state = "finishing_hot_retained"
			if h.barrier == nil {
				state = "hot_available_sync_pending"
			}
		}
		json.NewEncoder(w).Encode(struct {
			State string `json:"state"`
		}{state})
		return
	}
	if r.Method != http.MethodPost || (r.URL.Path != hotPreparationPath && r.URL.Path != hotPreparationPath+"/confirm" && r.URL.Path != hotPreparationPath+"/finish" && r.URL.Path != hotPreparationPath+"/evict") {
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
	if r.URL.Path == hotPreparationPath+"/evict" {
		err = h.manualEvict(ctx, req)
	} else if r.URL.Path == hotPreparationPath+"/finish" {
		err = h.requestFinish(ctx, req)
	} else if h.finishing {
		err = errConfirmationRPC
	} else if r.URL.Path == hotPreparationPath {
		err = h.prepare(ctx, req)
	} else if h.confirmation == nil || h.request.OperationID != req.OperationID || !bytes.Equal(h.request.Key, req.Key) {
		err = errConfirmationRPC
	} else {
		err = h.confirmation.Confirm(ctx)
	}
	if err != nil || ctx.Err() != nil || h.stopping.Load() {
		message := "operation unavailable; protection retained"
		if h.finishing && h.barrier == nil {
			message = "hot data available; cold completion pending"
		}
		if h.request == nil {
			message = "preparation not started; no operation held"
		}
		if r.URL.Path == hotPreparationPath+"/evict" {
			message = "eviction not confirmed; retry the same operation ID; key access remains guarded"
		}
		http.Error(w, message, 503)
		return
	}
	if r.URL.Path == hotPreparationPath+"/evict" {
		w.Write([]byte("{\"state\":\"eviction_completed_or_already_rewarmed\"}\n"))
	} else if r.URL.Path == hotPreparationPath+"/finish" {
		w.Write([]byte("{\"state\":\"finished_hot_retained\"}\n"))
	} else {
		w.Write([]byte("{\"state\":\"held\"}\n"))
	}
}

func (h *hotPreparation) Close() {
	h.stopping.Store(true)
	h.cancel()
	if h.finishDone != nil {
		<-h.finishDone
	}
	h.gate <- struct{}{}
	defer func() { <-h.gate }()
	if h.closed {
		return
	}
	h.closed = true
	if h.autoFinishTimer != nil {
		h.autoFinishTimer.Stop()
	}
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
