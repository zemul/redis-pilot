package syncer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/gateway"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

// Explicit maintenance/experiment mode of the SAME syncer executable. It
// cannot be combined with normal capture configuration or run concurrently
// against its journal. It serves one fixed task and one unresolved operation.
type ConfirmationConfig struct {
	Mode                 string                              `yaml:"mode"`
	Listen               string                              `yaml:"listen"`
	ConfirmExclusiveCold bool                                `yaml:"confirm_exclusive_cold"`
	Options              gateway.IsolatedConfirmationOptions `yaml:"confirmation"`
}

func LoadConfirmationConfig(path string) (ConfirmationConfig, error) {
	var c ConfirmationConfig
	invalid := errors.New("invalid isolated confirmation configuration")
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return c, invalid
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() > maxConfigBytes {
		return c, invalid
	}
	b, err := io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	if err != nil || len(b) > maxConfigBytes {
		return c, invalid
	}
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	if d.Decode(&c) != nil {
		return ConfirmationConfig{}, invalid
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return ConfirmationConfig{}, invalid
	}
	var node yaml.Node
	if yaml.Unmarshal(b, &node) != nil || hasYAMLAliasOrMerge(&node) || c.Validate() != nil {
		return ConfirmationConfig{}, invalid
	}
	return c, nil
}

func (c ConfirmationConfig) Validate() error {
	listen, ok := loopbackEndpoint(c.Listen, true)
	source, _ := loopbackEndpoint(c.Options.Binding.Source, false)
	cold, _ := loopbackEndpoint(c.Options.Binding.Cold, false)
	if (c.Mode != "isolated-confirmation" && c.Mode != "isolated-confirmation-live") || !c.ConfirmExclusiveCold || !ok || listen == source || listen == cold || c.Options.Validate() != nil {
		return errors.New("invalid isolated confirmation configuration")
	}
	return nil
}

func (c ConfirmationConfig) CheckCredentials() error {
	if c.Validate() != nil {
		return errors.New("invalid isolated confirmation configuration")
	}
	return c.Options.CheckCredentials()
}

// RunConfirmation binds first, then acquires existing source storage. No
// goroutines, requests or journal owners survive its return.
func RunConfirmation(ctx context.Context, c ConfirmationConfig) error {
	if ctx == nil || c.CheckCredentials() != nil {
		return errors.New("confirmation configuration unavailable")
	}
	if ctx.Err() != nil {
		return nil
	}
	listener, err := net.Listen("tcp", c.Listen)
	if err != nil {
		return errors.New("confirmation listener unavailable")
	}
	defer listener.Close()
	var handler *gateway.IsolatedConfirmationServer
	if c.Mode == "isolated-confirmation-live" {
		handler, err = gateway.NewLiveConfirmationServer(c.Options)
	} else {
		handler, err = gateway.NewIsolatedConfirmationServer(c.Options)
	}
	if err != nil {
		return errors.New("confirmation storage unavailable")
	}
	defer handler.Close()
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second,
		WriteTimeout: c.Options.Timeout + 4*time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 8192,
		ErrorLog: log.New(io.Discard, "", 0), BaseContext: func(net.Listener) context.Context { return workCtx }}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	var runnerDone chan error
	if c.Mode == "isolated-confirmation-live" {
		runnerDone = make(chan error, 1)
		go func() { runnerDone <- handler.RunLive(workCtx) }()
	}
	var result error
	httpJoined, runnerJoined := false, runnerDone == nil
	select {
	case err := <-done:
		httpJoined = true
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			result = errors.New("confirmation listener stopped")
		}
	case err := <-runnerDone:
		runnerJoined = true
		if err != nil || workCtx.Err() == nil {
			result = errors.New("live confirmation runner stopped")
		}
	case <-ctx.Done():
	}
	cancelWork()
	shutdown, cancel := context.WithTimeout(context.Background(), c.Options.Timeout+4*time.Second)
	defer cancel()
	err = server.Shutdown(shutdown)
	if err != nil {
		server.Close()
	}
	if !httpJoined {
		<-done
	}
	if !runnerJoined {
		if err := <-runnerDone; err != nil {
			result = errors.New("live confirmation runner failed")
		}
	}
	if err != nil {
		return errors.New("confirmation shutdown incomplete")
	}
	return result
}
