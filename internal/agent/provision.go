package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/gin-gonic/gin"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/redisconf"
)

type ensureProfileRequest = apitypes.EnsureProfileRequest

type provisionReceipt struct {
	OperationID string `json:"operation_id"`
	Digest      string `json:"digest"`
	ContainerID string `json:"container_id,omitempty"`
}

type ownedCreator interface {
	CreateOwned(engine, name, image string, port int, memory string, cpus float64, dataDir, owner string) (string, error)
}

var receiptTemplate = template.Must(template.New("receipt").Parse("{{.}}\n"))

// Internal endpoint for a future durable Server coordinator. Neither ordinary
// create nor a resource with unproven ownership is an acceptable retry target.
func (a *Agent) instanceEnsureProfile(c *gin.Context) {
	var req ensureProfileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, 400, err.Error())
		return
	}
	if validateInstanceName(req.Name) != nil || validateInstanceName(req.OperationID) != nil || req.Profile == "" || req.EngineImage == "" || req.Password != "" || !req.DisableProxy || req.Port < 1 || req.Port > 65535 || req.CPUs <= 0 {
		fail(c, 400, "invalid dedicated provisioning request")
		return
	}
	if err := redisconf.ValidateProfile(req.Profile, req.Engine, req.Category, req.Type, req.ReplicaOf, req.ConfigOverrides); err != nil {
		fail(c, 400, err.Error())
		return
	}
	unlock := a.lockInstance(req.Name)
	defer unlock()
	id, err := a.ensureProfile(req)
	if err != nil {
		fail(c, 409, err.Error())
		return
	}
	ok(c, gin.H{"operation_id": req.OperationID, "container_id": id})
}

func (a *Agent) ensureProfile(req ensureProfileRequest) (string, error) {
	return a.ensureProfileAttempt(req, true)
}

func (a *Agent) ensureProfileAttempt(req ensureProfileRequest, allowCreate bool) (string, error) {
	if _, err := os.Stat(filepath.Join(a.cfg.DataDir, ".provision-retired", req.OperationID)); !os.IsNotExist(err) {
		return "", fmt.Errorf("provisioning operation is retired or its retirement state cannot be checked")
	}
	creator, ok := a.runtime.(ownedCreator)
	if !ok {
		return "", fmt.Errorf("runtime does not support owned provisioning")
	}
	b, err := json.Marshal(req.CreateInstanceRequest)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	want := provisionReceipt{OperationID: req.OperationID, Digest: hex.EncodeToString(h[:])}
	dir := filepath.Join(a.cfg.DataDir, req.Name)
	path := filepath.Join(dir, "conf", "provision.json")
	b, err = os.ReadFile(path)
	fresh := os.IsNotExist(err)
	if err != nil && !fresh {
		return "", err
	}
	if !fresh {
		var old provisionReceipt
		if json.Unmarshal(b, &old) != nil || old.OperationID != want.OperationID || old.Digest != want.Digest {
			return "", fmt.Errorf("provisioning ownership or parameters differ")
		}
		want = old
	} else {
		// Never adopt any pre-existing instance directory, including an empty one.
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			return "", fmt.Errorf("unowned instance directory already exists or cannot be checked")
		}
	}
	containers, err := a.runtime.ListAll()
	if err != nil {
		return "", err
	}
	name := req.Engine + "-" + req.Name
	for _, ctr := range containers {
		if ctr.Name != name {
			continue
		}
		if fresh {
			return "", fmt.Errorf("unowned container already exists")
		}
		raw, err := a.runtime.Run("inspect", name)
		if err != nil {
			return "", err
		}
		var rows []struct {
			ID     string `json:"Id"`
			Config struct {
				Labels map[string]string `json:"Labels"`
			} `json:"Config"`
			State struct {
				Running bool `json:"Running"`
			} `json:"State"`
		}
		if json.Unmarshal([]byte(raw), &rows) != nil || len(rows) != 1 || rows[0].ID == "" || rows[0].Config.Labels["redis-pilot.provision"] != want.OperationID+":"+want.Digest {
			return "", fmt.Errorf("existing container ownership cannot be verified")
		}
		if want.ContainerID != "" && want.ContainerID != rows[0].ID {
			return "", fmt.Errorf("provisioned container was replaced")
		}
		if !rows[0].State.Running {
			return "", fmt.Errorf("owned container is stopped; not restarting it implicitly")
		}
		want.ContainerID = rows[0].ID
		return want.ContainerID, saveProvisionReceipt(dir, want)
	}
	if want.ContainerID != "" {
		return "", fmt.Errorf("completed container is missing; refusing to recreate")
	}
	if !allowCreate {
		return "", fmt.Errorf("creation returned but container is not observable yet")
	}
	if err := saveProvisionReceipt(dir, want); err != nil {
		return "", err
	}
	for _, sub := range []string{"data", "backup"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0755); err != nil {
			return "", err
		}
	}
	err = a.writeInstanceConfig(&instanceConfigRequest{
		Name: req.Name, Profile: req.Profile, Engine: req.Engine, Type: req.Type,
		Category: req.Category, Memory: req.Memory, Port: req.Port,
		EngineVersion: req.EngineVersion, ConfigOverrides: req.ConfigOverrides,
	})
	if err != nil {
		return "", err
	}
	_, err = creator.CreateOwned(req.Engine, name, req.EngineImage, req.Port, req.Memory, req.CPUs, dir, want.OperationID+":"+want.Digest)
	if err != nil {
		return "", err
	}
	// Re-inspect, including Running and ownership. A successful `run -d` alone
	// is not evidence that the engine stayed alive. No automatic deletion.
	return a.ensureProfileAttempt(req, false)
}

// A delayed retry must not recreate an instance after an acknowledged delete.
// Keep this tombstone outside the instance directory that clean_data removes.
func (a *Agent) retireProvision(name string) error {
	b, err := os.ReadFile(filepath.Join(a.cfg.DataDir, name, "conf", "provision.json"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var r provisionReceipt
	if json.Unmarshal(b, &r) != nil || validateInstanceName(r.OperationID) != nil {
		return fmt.Errorf("invalid provisioning receipt; refusing unprotected deletion")
	}
	if err := saveProvisionReceipt(filepath.Join(a.cfg.DataDir, ".provision-retired", r.OperationID), r); err != nil {
		return err
	}
	f, err := os.Open(a.cfg.DataDir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func saveProvisionReceipt(dir string, r provisionReceipt) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if err := writeConfig(dir, "provision.json", receiptTemplate, string(b)); err != nil {
		return err
	}
	for _, path := range []string{dir, filepath.Dir(dir)} {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		err = f.Sync()
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
