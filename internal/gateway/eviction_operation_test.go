package gateway

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func evictionOperationFixture(t *testing.T) (string, syncTaskIdentity, evictionIntent, evictionPreparation) {
	t.Helper()
	identity, i, p := evictionOperationFixtureData()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "operation.log"), identity, i, p
}

func evictionOperationFixtureData() (syncTaskIdentity, evictionIntent, evictionPreparation) {
	i := evictionTestIntent("\x00\xffkey", "value")
	identity := syncTaskIdentity{TaskID: i.TaskID, Source: "127.0.0.1:12345", Cold: "127.0.0.1:23456"}
	before := syncPosition{Source: identity.Source, History: i.History, Offset: 100}
	p := evictionPreparation{Before: before, Record: evictionTestRecords(before, [][]byte{i.prepareFrame()})[0]}
	return identity, i, p
}

func TestEvictionOperationRecoverAndCopies(t *testing.T) {
	path, identity, i, p := evictionOperationFixture(t)
	g, err := openEvictionOperation(path, identity, &i, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer g.close()
	if other, err := openEvictionOperation(path, identity, nil, nil); err == nil {
		other.close()
		t.Fatal("concurrent owner allowed")
	}
	if other, err := openEvictionOperation(path, identity, &i, nil); err == nil {
		other.close()
		t.Fatal("create overwrote existing file")
	}
	i.Key[0] = 'x'
	s, err := g.snapshot()
	if err != nil || s.Intent.Key[0] != 0 || s.Prepare != nil {
		t.Fatal("intent copy/state", err)
	}
	s.Intent.Key[0] = 'y'
	if err := g.recordPreparation(p); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(path)
	if err := g.recordPreparation(p); err != nil {
		t.Fatal("idempotent prepare", err)
	}
	after, _ := os.Stat(path)
	if after.Size() != info.Size() {
		t.Fatal("duplicate grew journal")
	}
	p.Record.Data[0] = '!'
	s, err = g.snapshot()
	if err != nil || s.Prepare.Record.Data[0] != '*' {
		t.Fatal("prepare alias", err)
	}
	s.Prepare.Record.Data[0] = '!'
	g.close()
	if _, err := g.snapshot(); err != errEvictionOperationClosed {
		t.Fatal(err)
	}
	g, err = openEvictionOperation(path, identity, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer g.close()
	s, err = g.snapshot()
	if err != nil || s.Prepare == nil || s.Prepare.Record.Data[0] != '*' || s.Intent.Key[0] != 0 {
		t.Fatal("recovery", err)
	}
	changed := *s.Prepare
	changed.Before.Offset++
	changed.Record.Position.Offset++
	if err := g.recordPreparation(changed); err != errEvictionOperation {
		t.Fatal("different occurrence accepted", err)
	}
}

func TestEvictionOperationRejectsUnsafeRecovery(t *testing.T) {
	for _, kind := range []string{"missing", "truncated", "checksum", "extra", "permissions", "symlink", "hardlink", "identity", "parent_permissions", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			path, identity, i, p := evictionOperationFixture(t)
			if kind != "missing" {
				g, err := openEvictionOperation(path, identity, &i, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := g.recordPreparation(p); err != nil {
					t.Fatal(err)
				}
				g.close()
			}
			switch kind {
			case "truncated", "checksum", "extra":
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "truncated" {
					data = data[:len(data)-1]
				}
				if kind == "checksum" {
					data[len(data)-1] ^= 1
				}
				if kind == "extra" {
					data = append(data, 0)
				}
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			case "permissions":
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			case "parent_permissions":
				if err := os.Chmod(filepath.Dir(path), 0755); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				link := path + ".link"
				if err := os.Symlink(path, link); err != nil {
					t.Fatal(err)
				}
				path = link
			case "hardlink":
				if err := os.Link(path, path+".link"); err != nil {
					t.Fatal(err)
				}
			case "identity":
				identity.Cold = "127.0.0.1:23457"
			case "oversized":
				if err := os.Truncate(path, evictionOperationFileLimit+1); err != nil {
					t.Fatal(err)
				}
			}
			if g, err := openEvictionOperation(path, identity, nil, nil); err == nil {
				g.close()
				t.Fatal("unsafe recovery accepted")
			}
			if kind == "missing" {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("recovery created missing file", err)
				}
			}
		})
	}
}

func TestEvictionOperationUncertainPersistence(t *testing.T) {
	for _, stage := range []string{"prepare_before_write", "prepare_after_write", "prepare_after_sync"} {
		t.Run(stage, func(t *testing.T) {
			path, identity, i, p := evictionOperationFixture(t)
			g, err := openEvictionOperation(path, identity, &i, func(at string) error {
				if at == stage {
					return errors.New("injected")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.recordPreparation(p); err != errEvictionOperationIO {
				t.Fatal(err)
			}
			if _, err := g.snapshot(); err != errEvictionOperationIO {
				t.Fatal("uncertain state exposed", err)
			}
			if err := g.recordPreparation(p); err != errEvictionOperationIO {
				t.Fatal("poisoned handle reused", err)
			}
			g.close()
			g, err = openEvictionOperation(path, identity, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer g.close()
			s, err := g.snapshot()
			if err != nil || (s.Prepare != nil) != (stage != "prepare_before_write") {
				t.Fatal("recovered wrong stage", err)
			}
		})
	}
	for _, stage := range []string{"create_before_write", "create_after_write", "create_after_sync", "after_directory_sync"} {
		t.Run(stage, func(t *testing.T) {
			path, identity, i, _ := evictionOperationFixture(t)
			g, err := openEvictionOperation(path, identity, &i, func(at string) error {
				if at == stage {
					return errors.New("injected")
				}
				return nil
			})
			if g != nil || err != errEvictionOperationIO {
				t.Fatal("uncertain create returned usable state", err)
			}
			g, err = openEvictionOperation(path, identity, nil, nil)
			if stage == "create_before_write" {
				if err == nil {
					g.close()
					t.Fatal("empty file treated as intent")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				g.close()
			}
		})
	}
}

func TestEvictionOperationValidationAndConcurrency(t *testing.T) {
	path, identity, i, p := evictionOperationFixture(t)
	g, err := openEvictionOperation(path, identity, &i, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer g.close()
	for _, mutate := range []func(*evictionPreparation){
		func(p *evictionPreparation) { p.Before.History = strings.Repeat("b", 40) },
		func(p *evictionPreparation) { p.Record.Position.Offset++ },
		func(p *evictionPreparation) { p.Record.Data = encodeCommand("DEL", string(i.Key)) },
		func(p *evictionPreparation) { p.Record.Data = encodeCommand("SET", i.prepareKey(), "forged") },
	} {
		bad := p
		mutate(&bad)
		if err := g.recordPreparation(bad); err != errEvictionOperation {
			t.Fatal("bad prepare accepted", err)
		}
	}
	var wg sync.WaitGroup
	for n := 0; n < 16; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.recordPreparation(p); err != nil {
				t.Error(err)
			}
			if _, err := g.snapshot(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	s, _ := g.snapshot()
	if s.Prepare == nil {
		t.Fatal("lost preparation")
	}
}

func TestEvictionOperationNewIntentBounds(t *testing.T) {
	i, err := newEvictionIntent("task", 1, strings.Repeat("a", 40), nil, []byte("value"))
	if err != nil || len(i.Key) != 0 || i.Key == nil {
		t.Fatal("empty binary key", err)
	}
	i2, err := newEvictionIntent("task", 1, i.History, nil, []byte("value"))
	if err != nil || i.OperationID == i2.OperationID {
		t.Fatal("operation ID reuse", err)
	}
	if _, err := newEvictionIntent("task", 1, i.History, nil, bytes.Repeat([]byte("v"), syncPendingValueLimit+1)); err == nil {
		t.Fatal("oversized value")
	}
	path, identity, _, _ := evictionOperationFixture(t)
	i, err = newEvictionIntent(identity.TaskID, 1, i.History, bytes.Repeat([]byte{0xff}, 64<<10), nil)
	if err != nil {
		t.Fatal(err)
	}
	g, err := openEvictionOperation(path, identity, &i, nil)
	if err != nil {
		t.Fatal(err)
	}
	before := syncPosition{Source: identity.Source, History: i.History, Offset: 100}
	p := evictionPreparation{before, evictionTestRecords(before, [][]byte{i.prepareFrame()})[0]}
	if err := g.recordPreparation(p); err != nil {
		t.Fatal("maximum key", err)
	}
	g.close()
	g, err = openEvictionOperation(path, identity, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	g.close()
}

func TestEvictionOperationKilledProcess(t *testing.T) {
	if path := os.Getenv("GATEWAY_EVICTION_OPERATION_CHILD_PATH"); path != "" {
		identity, i, p := evictionOperationFixtureData()
		g, err := openEvictionOperation(path, identity, &i, nil)
		if err != nil {
			t.Fatal(err)
		}
		if os.Getenv("GATEWAY_EVICTION_OPERATION_CHILD_STAGE") == "prepare" {
			if err := g.recordPreparation(p); err != nil {
				t.Fatal(err)
			}
		}
		fmt.Println("durable")
		select {}
	}
	for _, stage := range []string{"intent", "prepare"} {
		t.Run(stage, func(t *testing.T) {
			path, identity, _, _ := evictionOperationFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEvictionOperationKilledProcess$", "-test.timeout=15s")
			cmd.Env = append(os.Environ(), "GATEWAY_EVICTION_OPERATION_CHILD_PATH="+path, "GATEWAY_EVICTION_OPERATION_CHILD_STAGE="+stage)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			line, readErr := bufio.NewReader(stdout).ReadString('\n')
			killErr := cmd.Process.Kill()
			cmd.Wait()
			if readErr != nil || line != "durable\n" || killErr != nil {
				t.Fatal("child boundary", readErr, killErr, line)
			}
			g, err := openEvictionOperation(path, identity, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer g.close()
			s, err := g.snapshot()
			if err != nil || (s.Prepare != nil) != (stage == "prepare") {
				t.Fatal("killed-process recovery", err)
			}
		})
	}
}
