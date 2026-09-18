package agent

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/podman"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

type provisionRuntime struct {
	fakeRuntime
	owner      string
	id         string
	creates    int
	loseReply  bool
	failBefore bool
	listError  bool
	stopped    bool
}

func (f *provisionRuntime) CreateOwned(engine, name, image string, port int, memory string, cpus float64, dir, owner string) (string, error) {
	f.creates++
	if f.failBefore {
		return "", fmt.Errorf("create not dispatched")
	}
	f.owner, f.id = owner, "owned-id"
	f.containers = []podman.ContainerStatus{{Name: name, Running: true}}
	if f.loseReply {
		return "", fmt.Errorf("response lost")
	}
	return f.id, nil
}
func (f *provisionRuntime) ListAll() ([]podman.ContainerStatus, error) {
	if f.listError {
		return nil, fmt.Errorf("runtime unavailable")
	}
	return f.containers, nil
}
func (f *provisionRuntime) Run(args ...string) (string, error) {
	b, _ := json.Marshal([]any{map[string]any{"Id": f.id, "Config": map[string]any{"Labels": map[string]string{"redis-pilot.provision": f.owner}}, "State": map[string]any{"Running": !f.stopped}}})
	return string(b), nil
}
func provisioningFixture(t *testing.T) (*Agent, *provisionRuntime, ensureProfileRequest) {
	t.Helper()
	a := newTestAgent(t)
	f := &provisionRuntime{}
	a.runtime = f
	r := ensureProfileRequest{OperationID: "operation-1", CreateInstanceRequest: apitypes.CreateInstanceRequest{Name: "cache", Engine: "redis", Profile: "redis-cache", Category: "cache", Type: "standalone", Memory: "256Mi", CPUs: 1, Port: 6379, EngineImage: "redis:7", EngineVersion: "7", DisableProxy: true}}
	return a, f, r
}

func TestProvisionLostReplyAndAgentRestart(t *testing.T) {
	a, f, r := provisioningFixture(t)
	f.loseReply = true
	if _, err := a.ensureProfile(r); err == nil {
		t.Fatal("expected unknown result")
	}
	a = &Agent{cfg: a.cfg, runtime: f}
	id, err := a.ensureProfile(r)
	if err != nil || id != "owned-id" || f.creates != 1 {
		t.Fatalf("%s %v creates=%d", id, err, f.creates)
	}
	if _, err := a.ensureProfile(r); err != nil || f.creates != 1 {
		t.Fatal("repeat was not idempotent", err)
	}
	r.Memory = "512Mi"
	if _, err := a.ensureProfile(r); err == nil {
		t.Fatal("changed parameters accepted")
	}
}

func TestProvisionResumeBeforeContainerCreation(t *testing.T) {
	a, f, r := provisioningFixture(t)
	f.failBefore = true
	if _, err := a.ensureProfile(r); err == nil {
		t.Fatal("expected create failure")
	}
	f.failBefore = false
	a = &Agent{cfg: a.cfg, runtime: f}
	if _, err := a.ensureProfile(r); err != nil {
		t.Fatal(err)
	}
	if len(f.containers) != 1 {
		t.Fatal("missing resumed container")
	}
}

func TestProvisionRefusesUnknownOrChangedResources(t *testing.T) {
	for _, mode := range []string{"directory", "container", "list-error", "owner", "stopped", "missing", "replaced", "operation"} {
		t.Run(mode, func(t *testing.T) {
			a, f, r := provisioningFixture(t)
			switch mode {
			case "directory":
				os.MkdirAll(filepath.Join(a.cfg.DataDir, r.Name), 0755)
			case "container":
				f.containers = []podman.ContainerStatus{{Name: "redis-cache", Running: true}}
			case "list-error":
				f.listError = true
			default:
				if _, err := a.ensureProfile(r); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "owner":
					f.owner = "another-operation"
				case "stopped":
					f.stopped = true
				case "missing":
					f.containers = nil
				case "replaced":
					f.id = "replacement"
				case "operation":
					r.OperationID = "operation-2"
				}
			}
			n := f.creates
			if _, err := a.ensureProfile(r); err == nil {
				t.Fatal("unsafe recovery accepted")
			}
			if f.creates != n {
				t.Fatal("unexpected container creation")
			}
		})
	}
}

func TestProvisionConcurrentHTTPAndLegacyCreate(t *testing.T) {
	a, f, r := provisioningFixture(t)
	router := a.Router()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := doAgentRequest(router, "POST", "/instance/ensure-profile", r)
			if w.Code != 200 {
				t.Errorf("%d %s", w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()
	if f.creates != 1 {
		t.Fatalf("created %d containers", f.creates)
	}
	w := doAgentRequest(router, "POST", "/instance/create-profile", r.CreateInstanceRequest)
	if w.Code != 409 {
		t.Fatalf("legacy create overwrote owned provisioning: %d %s", w.Code, w.Body.String())
	}
}

func TestProvisionDeleteFencesDelayedRetryAcrossRestart(t *testing.T) {
	a, f, r := provisioningFixture(t)
	if _, err := a.ensureProfile(r); err != nil {
		t.Fatal(err)
	}
	w := doAgentRequest(a.Router(), "POST", "/instance/delete", map[string]any{"name": r.Name, "engine": r.Engine, "clean_data": true})
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	f.containers = nil
	a = &Agent{cfg: a.cfg, runtime: f}
	if _, err := a.ensureProfile(r); err == nil {
		t.Fatal("delayed operation recreated deleted resource")
	}
	if f.creates != 1 {
		t.Fatal("unexpected create")
	}
	r.OperationID = "operation-new"
	if _, err := a.ensureProfile(r); err != nil {
		t.Fatal("new operation cannot reuse released name", err)
	}
}
