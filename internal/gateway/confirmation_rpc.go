package gateway

// The confirmation transport is deliberately separate from business admission.
// A pinned binding is operator configuration, not a Server heartbeat or lease.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const confirmationRPCPath = "/internal/v1/cold-confirmation"
const confirmationRPCMaxBytes = 512 << 10

var errConfirmationRPC = errors.New("confirmation unavailable or mismatched; keep operation protected")

// ConfirmationBinding must match on both processes. Changes require explicit
// reconciliation, not overwriting the file or accepting a new heartbeat.
type ConfirmationBinding struct {
	SyncerID  string `json:"syncer_id" yaml:"syncer_id"`
	GatewayID string `json:"gateway_id" yaml:"gateway_id"`
	TaskID    string `json:"task_id" yaml:"task_id"`
	Source    string `json:"source" yaml:"source"`
	Cold      string `json:"cold" yaml:"cold"`
	History   string `json:"history" yaml:"history"`
	Epoch     uint64 `json:"epoch" yaml:"epoch"`
}

func (b ConfirmationBinding) identity() syncTaskIdentity {
	return syncTaskIdentity{TaskID: b.TaskID, Source: b.Source, Cold: b.Cold}
}

func (b ConfirmationBinding) Validate() error {
	i := b.identity()
	if ValidateIsolatedSyncTaskID(b.SyncerID) != nil || ValidateIsolatedSyncTaskID(b.GatewayID) != nil ||
		!validSyncTaskIdentity(&i) || b.Epoch == 0 || !canonicalEvictionHex(b.History, 20) || b.History == strings.Repeat("0", 40) {
		return errConfirmationRPC
	}
	return nil
}

func (b ConfirmationBinding) accepts(i evictionIntent) bool {
	return i.valid() && i.TaskID == b.TaskID && i.Epoch == b.Epoch && i.History == b.History
}

type confirmationSession struct {
	Version       int    `json:"version"`
	BindingSHA256 string `json:"binding_sha256"`
	BootID        string `json:"boot_id"`
}

type confirmationRequest struct {
	Session confirmationSession `json:"session"`
	Intent  evictionIntent      `json:"intent"`
}

type confirmationResponse struct {
	Session       confirmationSession `json:"session"`
	RequestSHA256 string              `json:"request_sha256"`
	Prepare       evictionPreparation `json:"prepare"`
	Receipt       evictionColdReceipt `json:"receipt"`
}

// Canonical bounded JSON rejects duplicate/unknown fields as well as extra
// documents. Both peers use json.Marshal; no raw error or key is logged.
func decodeConfirmation(r io.Reader, dst any) error {
	b, err := io.ReadAll(io.LimitReader(r, confirmationRPCMaxBytes+1))
	if err != nil || len(b) > confirmationRPCMaxBytes {
		return errConfirmationRPC
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(dst) != nil {
		return errConfirmationRPC
	}
	canonical, err := json.Marshal(dst)
	if err != nil || !bytes.Equal(bytes.TrimSpace(b), canonical) {
		return errConfirmationRPC
	}
	return nil
}

func confirmationToken(path string) (string, error) {
	token, err := syncReadPassword(path)
	if err != nil || len(token) < 32 || len(token) > 4096 {
		return "", errConfirmationRPC
	}
	for _, ch := range token {
		if ch < 33 || ch > 126 {
			return "", errConfirmationRPC
		}
	}
	return token, nil
}

// Open a private fixed binding before admitting ANY key. The lock lives for
// the endpoint lifetime; a missing operation is not interpreted as completed.
func openConfirmationBinding(dir string, binding ConfirmationBinding) (*os.File, error) {
	if !syncAbsolutePath(dir) || binding.Validate() != nil {
		return nil, errConfirmationRPC
	}
	dfd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errConfirmationRPC
	}
	defer unix.Close(dfd)
	var st unix.Stat_t
	if unix.Fstat(dfd, &st) != nil || st.Uid != uint32(os.Geteuid()) || st.Mode&0077 != 0 {
		return nil, errConfirmationRPC
	}
	flags := unix.O_RDWR | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_CLOEXEC
	fd, err := unix.Openat(dfd, "confirmation-binding.json", flags|unix.O_CREAT|unix.O_EXCL, 0600)
	created := err == nil
	if errors.Is(err, unix.EEXIST) {
		fd, err = unix.Openat(dfd, "confirmation-binding.json", flags, 0)
	}
	if err != nil {
		return nil, errConfirmationRPC
	}
	f := os.NewFile(uintptr(fd), filepath.Join(dir, "confirmation-binding.json"))
	ok := false
	defer func() {
		if !ok {
			f.Close()
		}
	}()
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0077 != 0 ||
		st.Uid != uint32(os.Geteuid()) || st.Nlink != 1 || st.Size > 4096 || unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB) != nil {
		return nil, errConfirmationRPC
	}
	data, _ := json.Marshal(binding)
	if created {
		// Losing binding state must not allow an existing operation to be
		// silently adopted by a different configured syncer/Gateway identity.
		var operation unix.Stat_t
		if err := unix.Fstatat(dfd, "confirmation-operation.log", &operation, unix.AT_SYMLINK_NOFOLLOW); !errors.Is(err, unix.ENOENT) {
			return nil, errConfirmationRPC
		}
		if n, err := f.Write(data); err != nil || n != len(data) {
			return nil, errConfirmationRPC
		}
	} else {
		old, err := io.ReadAll(io.LimitReader(f, 4097))
		if err != nil || !bytes.Equal(data, old) {
			return nil, errConfirmationRPC
		}
	}
	if f.Sync() != nil || unix.Fsync(dfd) != nil {
		return nil, errConfirmationRPC
	}
	ok = true
	return f, nil
}

func confirmationBoot() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", errConfirmationRPC
	}
	return hex.EncodeToString(b[:]), nil
}

func confirmationAuthorized(r *http.Request, token string) bool {
	values := r.Header.Values("Authorization")
	return len(values) == 1 && subtle.ConstantTimeCompare([]byte(values[0]), []byte("Bearer "+token)) == 1
}

func confirmationHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: timeout}).DialContext,
			DisableKeepAlives: true, MaxResponseHeaderBytes: 8192, ResponseHeaderTimeout: timeout}}
}

func confirmationCall(ctx context.Context, client *http.Client, endpoint, token, method string, body any, result any) error {
	var data []byte
	if body != nil {
		data, _ = json.Marshal(body)
	}
	if len(data) > confirmationRPCMaxBytes {
		return errConfirmationRPC
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint+confirmationRPCPath, bytes.NewReader(data))
	if err != nil {
		return errConfirmationRPC
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return errConfirmationRPC
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || decodeConfirmation(resp.Body, result) != nil {
		return errConfirmationRPC
	}
	return nil
}
