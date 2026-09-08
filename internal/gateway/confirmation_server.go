package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// IsolatedConfirmationOptions opens an EXISTING captured journal exclusively.
// Capture/normal replay must already have stopped. This endpoint neither runs
// PSYNC nor prunes logs, and is not an automatic handoff from the live runner.
type IsolatedConfirmationOptions struct {
	Binding              ConfirmationBinding `yaml:"binding"`
	JournalDir           string              `yaml:"journal_dir"`
	OperationDir         string              `yaml:"operation_dir"`
	TokenFile            string              `yaml:"token_file"`
	SourcePasswordFile   string              `yaml:"source_password_file"`
	ColdPasswordFile     string              `yaml:"cold_password_file"`
	AllowUnauthenticated bool                `yaml:"allow_unauthenticated"`
	SegmentBytes         int64               `yaml:"segment_bytes"`
	JournalBytes         int64               `yaml:"journal_bytes"`
	Timeout              time.Duration       `yaml:"timeout"`
}

func (o IsolatedConfirmationOptions) syncOptions() IsolatedSyncOptions {
	return IsolatedSyncOptions{TaskID: o.Binding.TaskID, SourceAddress: o.Binding.Source, ColdAddress: o.Binding.Cold,
		SourcePasswordFile: o.SourcePasswordFile, ColdPasswordFile: o.ColdPasswordFile, AllowUnauthenticated: o.AllowUnauthenticated,
		JournalDir: o.JournalDir, IOTimeout: o.Timeout, ShutdownTimeout: o.Timeout, SegmentBytes: o.SegmentBytes, JournalBytes: o.JournalBytes}
}

func (o IsolatedConfirmationOptions) Validate() error {
	if o.Binding.Validate() != nil || !syncAbsolutePath(o.OperationDir) || !syncAbsolutePath(o.TokenFile) ||
		o.Timeout <= 0 || o.Timeout > 10*time.Second || ValidateIsolatedSyncOptions(o.syncOptions()) != nil {
		return errConfirmationRPC
	}
	// Separate recovery files from capture storage and its cleanup namespace.
	if o.OperationDir == o.JournalDir || stringsPathWithin(o.OperationDir, o.JournalDir) || stringsPathWithin(o.JournalDir, o.OperationDir) {
		return errConfirmationRPC
	}
	return nil
}

func stringsPathWithin(path, dir string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

func (o IsolatedConfirmationOptions) CheckCredentials() error {
	if o.Validate() != nil {
		return errConfirmationRPC
	}
	if _, err := confirmationToken(o.TokenFile); err != nil {
		return err
	}
	for _, path := range []string{o.SourcePasswordFile, o.ColdPasswordFile} {
		if _, err := syncReadPassword(path); err != nil {
			return errConfirmationRPC
		}
	}
	return nil
}

type IsolatedConfirmationServer struct {
	live        *confirmationLive // immutable pointer, mutable fields under gate
	options     IsolatedConfirmationOptions
	session     confirmationSession
	token       string
	gate        chan struct{}
	closed      bool // gate protects all mutable state and lifetime
	bindingFile *os.File
	journal     *syncSegments
	probe       *IsolatedSyncTask
	target      syncColdTarget
}

func NewIsolatedConfirmationServer(o IsolatedConfirmationOptions) (*IsolatedConfirmationServer, error) {
	if o.CheckCredentials() != nil {
		return nil, errConfirmationRPC
	}
	journalPath, journalErr := filepath.EvalSymlinks(o.JournalDir)
	operationPath, operationErr := filepath.EvalSymlinks(o.OperationDir)
	if journalErr != nil || operationErr != nil || stringsPathWithin(journalPath, operationPath) || stringsPathWithin(operationPath, journalPath) {
		return nil, errConfirmationRPC
	}
	// Never synthesize an empty journal as a replacement for lost source proof.
	info, err := os.Lstat(filepath.Join(o.JournalDir, "manifest"))
	if err != nil || !info.Mode().IsRegular() {
		return nil, errConfirmationRPC
	}
	binding, err := openConfirmationBinding(o.OperationDir, o.Binding)
	if err != nil {
		return nil, err
	}
	identity := o.Binding.identity()
	j, err := openSyncSegmentsForTask(o.JournalDir, o.SegmentBytes, o.JournalBytes, defaultSyncResourcePolicy(), &identity)
	if err != nil {
		binding.Close()
		return nil, errConfirmationRPC
	}
	ok := false
	defer func() {
		if !ok {
			j.close()
			binding.Close()
		}
	}()
	if j.durablePosition().History != o.Binding.History {
		return nil, errConfirmationRPC
	}
	boot, err := confirmationBoot()
	if err != nil {
		return nil, err
	}
	token, err := confirmationToken(o.TokenFile)
	if err != nil {
		return nil, err
	}
	probe, err := NewIsolatedSyncTask(o.syncOptions())
	if err != nil {
		return nil, errConfirmationRPC
	}
	s := &IsolatedConfirmationServer{options: o, session: confirmationSession{1, evictionHash(o.Binding), boot}, token: token,
		gate: make(chan struct{}, 1), bindingFile: binding, journal: j, probe: probe, target: probe.runner.target}
	ok = true
	return s, nil
}

func (s *IsolatedConfirmationServer) Close() error {
	// Cancel and join outside gate: the runner may be finishing a gated drain.
	s.gate <- struct{}{}
	var done chan struct{}
	if s.live != nil && s.live.started {
		s.live.cancel()
		done = s.live.done
	}
	<-s.gate
	if done != nil {
		<-done
	}
	s.gate <- struct{}{}
	defer func() { <-s.gate }()
	if s.closed {
		return nil
	}
	s.closed = true
	err := s.journal.close()
	if closeErr := s.bindingFile.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (s *IsolatedConfirmationServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	fail := func(status int) { http.Error(w, "confirmation unavailable", status) }
	if !confirmationAuthorized(r, s.token) {
		fail(http.StatusUnauthorized)
		return
	}
	statusRequest := s.live != nil && r.URL.Path == confirmationRPCPath+"/status" && r.Method == http.MethodGet
	if (r.URL.Path != confirmationRPCPath && !statusRequest) || r.URL.RawQuery != "" {
		fail(http.StatusNotFound)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		fail(http.StatusMethodNotAllowed)
		return
	}
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	default:
		fail(http.StatusServiceUnavailable)
		return
	}
	if s.closed {
		fail(http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if statusRequest {
		json.NewEncoder(w).Encode(s.liveStatus())
		return
	}
	if s.live != nil && (!s.live.active || !s.live.ready || s.live.blocked || s.live.stopping.Load()) {
		fail(http.StatusServiceUnavailable)
		return
	}
	if r.Method == http.MethodGet {
		json.NewEncoder(w).Encode(s.session)
		return
	}
	var req confirmationRequest
	if decodeConfirmation(r.Body, &req) != nil {
		fail(http.StatusBadRequest)
		return
	}
	if req.Session != s.session || !s.options.Binding.accepts(req.Intent) {
		fail(http.StatusConflict)
		return
	}
	if s.live != nil && (s.live.held == nil || evictionHash(s.live.held.Intent) != evictionHash(req.Intent)) {
		fail(http.StatusConflict)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.options.Timeout)
	defer cancel()
	if s.live != nil {
		s.live.verified = false
		s.live.verifiedAt = time.Time{}
	}
	result, err := s.confirm(ctx, req)
	if err != nil || (s.live != nil && s.live.stopping.Load()) {
		fail(http.StatusConflict)
		return
	}
	if s.live != nil {
		s.live.verified = true
		s.live.verifiedAt = time.Now()
		s.live.held.Receipt = &result.Receipt
	}
	json.NewEncoder(w).Encode(result)
}

// The caller submits only intent, never source bytes or a claimed checkpoint.
func (s *IsolatedConfirmationServer) preparation(i evictionIntent) (evictionPreparation, error) {
	if s.live != nil && s.live.held != nil && evictionHash(s.live.held.Intent) == evictionHash(i) {
		// The live lane already identified the first marker. confirmEviction
		// will re-prove it from the source log; do not scan the held suffix.
		p := *s.live.held.Prepare
		p.Record.Data = bytes.Clone(p.Record.Data)
		return p, nil
	}
	v, err := s.journal.pendingView()
	if err != nil {
		return evictionPreparation{}, err
	}
	defer v.close()
	var result evictionPreparation
	count := 0
	err = v.walkFrom(v.floor, func(r syncRecord, prev, _ syncApplyCheckpoint) error {
		if bytes.Equal(r.Data, i.prepareFrame()) {
			count++
			result = evictionPreparation{Before: prev.Position, Record: r}
		}
		return nil
	})
	if err != nil || count != 1 {
		return evictionPreparation{}, errConfirmationRPC
	}
	return result, nil
}

func (s *IsolatedConfirmationServer) checkSource(ctx context.Context, offset int64) error {
	probe := s.probe.sampleSourceProgress(ctx, min(s.options.Timeout, 2*time.Second))
	if probe.Error != "none" || probe.Position == nil || probe.Position.History != s.options.Binding.History || probe.Position.Offset < offset {
		return errConfirmationRPC
	}
	return nil // point-in-time check, not source fencing
}

func (s *IsolatedConfirmationServer) confirm(ctx context.Context, req confirmationRequest) (confirmationResponse, error) {
	var zero confirmationResponse
	p, err := s.preparation(req.Intent)
	if err != nil || s.checkSource(ctx, p.Record.Position.Offset) != nil {
		return zero, errConfirmationRPC
	}
	path := filepath.Join(s.options.OperationDir, "confirmation-operation.log")
	var create *evictionIntent
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		create = &req.Intent
	} else if err != nil {
		return zero, errConfirmationRPC
	}
	op, err := openEvictionOperation(path, s.options.Binding.identity(), create, nil)
	if err != nil {
		return zero, errConfirmationRPC
	}
	defer op.close()
	state, err := op.snapshot()
	if err != nil || evictionHash(state.Intent) != evictionHash(req.Intent) {
		return zero, errConfirmationRPC
	}
	if err := op.recordPreparation(p); err != nil {
		return zero, errConfirmationRPC
	}
	ack, err := s.target.confirmEviction(ctx, s.journal, op)
	if err != nil || s.checkSource(ctx, p.Record.Position.Offset) != nil || ctx.Err() != nil {
		return zero, errConfirmationRPC
	}
	return confirmationResponse{Session: s.session, RequestSHA256: evictionHash(req), Prepare: p, Receipt: ack}, nil
}
