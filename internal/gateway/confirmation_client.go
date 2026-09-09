package gateway

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// The Gateway owns ONLY its intent/receipt files, not the syncer's source log.
// The endpoint is local-only until authenticated cross-host TLS and actual
// execution ownership are implemented. There is no environment proxy fallback.
type IsolatedConfirmationGatewayOptions struct {
	Binding                           ConfirmationBinding
	Endpoint, TokenFile, OperationDir string
	Timeout                           time.Duration
}

type IsolatedConfirmationGateway struct {
	options     IsolatedConfirmationGatewayOptions
	client      *http.Client
	token       string
	lock        *os.File
	op          *evictionOperation
	barriers    *keyBarrierSet
	barrier     *keyBarrier
	gate        chan struct{}
	closed      bool
	borrowed    bool // Preparation controller owns the binding lock and barrier set.
	hotRetained bool // Internal controller verified the retained source marker; no deletion authority.
}

// OpenIsolatedConfirmationGateway restores protection BEFORE permitting any
// confirmation. Missing intent is an error, never proof that a key is hot.
func OpenIsolatedConfirmationGateway(o IsolatedConfirmationGatewayOptions) (*IsolatedConfirmationGateway, error) {
	return openConfirmationGateway(o, nil, nil)
}

// Internal construction supports isolated tests starting an operation. The
// live Service does not call this until all relevant requests use the barrier.
func openConfirmationGateway(o IsolatedConfirmationGatewayOptions, intent *evictionIntent, barriers *keyBarrierSet) (*IsolatedConfirmationGateway, error) {
	token, err := validateConfirmationGateway(o)
	if err != nil {
		return nil, err
	}
	return openConfirmationGatewayValidated(o, intent, barriers, token)
}

func validateConfirmationGateway(o IsolatedConfirmationGatewayOptions) (string, error) {
	u, err := url.Parse(o.Endpoint)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" ||
		syncCanonicalEndpoint(u.Host) != u.Host || u.Host == "" || u.Host == o.Binding.Source || u.Host == o.Binding.Cold || o.Binding.Validate() != nil ||
		!syncAbsolutePath(o.OperationDir) || !syncAbsolutePath(o.TokenFile) || o.Timeout <= 0 || o.Timeout > 15*time.Second {
		return "", errConfirmationRPC
	}
	return confirmationToken(o.TokenFile)
}

func openConfirmationGatewayValidated(o IsolatedConfirmationGatewayOptions, intent *evictionIntent, barriers *keyBarrierSet, token string) (*IsolatedConfirmationGateway, error) {
	lock, err := openConfirmationBinding(o.OperationDir, o.Binding)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			lock.Close()
		}
	}()
	if intent != nil && !o.Binding.accepts(*intent) {
		return nil, errConfirmationRPC
	}
	op, err := openEvictionOperation(filepath.Join(o.OperationDir, "confirmation-operation.log"), o.Binding.identity(), intent, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !ok {
			op.close()
		}
	}()
	s, err := op.snapshot()
	if err != nil || !o.Binding.accepts(s.Intent) {
		return nil, errConfirmationRPC
	}
	if barriers == nil {
		barriers, _ = newKeyBarrierSet(1, 64<<10, 1)
	}
	b, err := barriers.reserve(string(s.Intent.Key))
	if err != nil {
		return nil, err
	}
	g := &IsolatedConfirmationGateway{options: o, client: confirmationHTTPClient(o.Timeout), token: token, lock: lock,
		op: op, barriers: barriers, barrier: b, gate: make(chan struct{}, 1)}
	ok = true
	return g, nil
}

// Confirm never returns deletion authority and never reopens admission, even
// on success. Retrying/reopening always revalidates through the remote service.
func (g *IsolatedConfirmationGateway) Confirm(ctx context.Context) error {
	return g.exchange(ctx, false)
}

func (g *IsolatedConfirmationGateway) Finish(ctx context.Context) error {
	return g.exchange(ctx, true)
}

func (g *IsolatedConfirmationGateway) exchange(ctx context.Context, finish bool) error {
	if ctx == nil {
		return errConfirmationRPC
	}
	ctx, cancel := context.WithTimeout(ctx, g.options.Timeout)
	defer cancel()
	select {
	case g.gate <- struct{}{}:
		defer func() { <-g.gate }()
	case <-ctx.Done():
		return errConfirmationRPC
	}
	if g.closed {
		return errConfirmationRPC
	}
	if !g.hotRetained {
		select {
		case <-g.barrier.wake:
		case <-ctx.Done():
			return errConfirmationRPC
		}
		g.barriers.mu.Lock()
		b := g.barrier
		valid := !g.barriers.closed && b.entry.barrier == b && !b.entry.uncertain && b.entry.active == 0
		if valid {
			b.dispatched = true
		}
		g.barriers.mu.Unlock()
		if !valid {
			return errConfirmationRPC
		}
	}
	s, err := g.op.snapshot()
	if err != nil {
		return errConfirmationRPC
	}
	if finish && s.Receipt == nil {
		return errConfirmationRPC
	}
	if !finish && s.Finished {
		return errConfirmationRPC
	}
	var session confirmationSession
	if confirmationCall(ctx, g.client, g.options.Endpoint, g.token, http.MethodGet, nil, &session) != nil ||
		session.Version != 1 || session.BindingSHA256 != evictionHash(g.options.Binding) || !canonicalEvictionHex(session.BootID, 16) {
		return errConfirmationRPC
	}
	req := confirmationRequest{Session: session, Intent: s.Intent, Finish: finish}
	var reply confirmationResponse
	if confirmationCall(ctx, g.client, g.options.Endpoint, g.token, http.MethodPost, req, &reply) != nil {
		return errConfirmationRPC
	}
	if reply.Finished != finish || reply.Session != session || reply.RequestSHA256 != evictionHash(req) || !validEvictionPreparation(s, reply.Prepare) {
		return errConfirmationRPC
	}
	checked := copyEvictionOperationState(s)
	checked.Prepare = &reply.Prepare
	if !validEvictionReceipt(checked, reply.Receipt) || (s.Receipt != nil && *s.Receipt != reply.Receipt) {
		return errConfirmationRPC
	}
	// Detect a restart observed during the round trip. Still not a lease: a
	// later ownership change must be checked by the future deletion executor.
	var current confirmationSession
	if confirmationCall(ctx, g.client, g.options.Endpoint, g.token, http.MethodGet, nil, &current) != nil || current != session {
		return errConfirmationRPC
	}
	if g.op.recordPreparation(reply.Prepare) != nil {
		return errConfirmationRPC
	}
	g.op.mu.Lock()
	err = g.op.recordReceiptLocked(reply.Receipt)
	g.op.mu.Unlock()
	if err != nil || ctx.Err() != nil {
		return errConfirmationRPC
	}
	if finish {
		return g.op.recordFinished()
	}
	return nil
}

func (g *IsolatedConfirmationGateway) Close() {
	g.gate <- struct{}{}
	defer func() { <-g.gate }()
	if g.closed {
		return
	}
	g.closed = true
	if !g.borrowed {
		g.barriers.close()
	}
	g.op.close()
	if !g.borrowed {
		g.lock.Close()
	}
	g.client.CloseIdleConnections()
}
