package agent

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/gin-gonic/gin"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

const defaultSentinelPort = 26379

var sentinelTmpl = template.Must(template.New("sentinel").Parse(`port {{ .Port }}
bind 0.0.0.0
dir /data

sentinel resolve-hostnames yes
{{- if .Announce }}
sentinel announce-ip {{ .Announce }}
sentinel announce-port {{ .Port }}
{{- end }}
{{ range .Masters }}
sentinel monitor {{ .Group }} {{ .Host }} {{ .Port }} {{ $.Quorum }}
{{- if .Password }}
sentinel auth-pass {{ .Group }} {{ .Password }}
{{- end }}
sentinel down-after-milliseconds {{ .Group }} {{ .DownAfterMilliseconds }}
sentinel failover-timeout {{ .Group }} {{ .FailoverTimeout }}
sentinel parallel-syncs {{ .Group }} {{ .ParallelSyncs }}
{{ end }}`))

func (a *Agent) sentinelSync(c *gin.Context) {
	var req apitypes.SentinelSyncRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if req.Port == 0 {
		req.Port = defaultSentinelPort
	}
	if req.Quorum == 0 {
		req.Quorum = 2
	}
	normalizeSentinelMasters(req.Masters)

	existingMasters := a.sentinelMastersFromConfig()
	running, err := a.sentinelContainerRunning()
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if !running {
		fail(c, http.StatusConflict, "redis-sentinel container is not running; deploy sentinel before syncing masters")
		return
	}

	confPath, err := a.writeSentinelConfig(req)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if err := a.applySentinelRuntimeConfig(req, existingMasters); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, gin.H{"config": confPath, "masters": len(req.Masters)})
}

func normalizeSentinelMasters(masters []apitypes.SentinelMaster) {
	for i := range masters {
		if masters[i].DownAfterMilliseconds == 0 {
			masters[i].DownAfterMilliseconds = 5000
		}
		if masters[i].FailoverTimeout == 0 {
			masters[i].FailoverTimeout = 30000
		}
		if masters[i].ParallelSyncs == 0 {
			masters[i].ParallelSyncs = 1
		}
	}
}

func (a *Agent) writeSentinelConfig(req apitypes.SentinelSyncRequest) (string, error) {
	confDir := filepath.Join(a.cfg.SentinelDir, "conf")
	dataDir := filepath.Join(a.cfg.SentinelDir, "data")
	if err := os.MkdirAll(confDir, 0755); err != nil {
		return "", fmt.Errorf("mkdir sentinel conf: %w", err)
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return "", fmt.Errorf("mkdir sentinel data: %w", err)
	}
	var buf bytes.Buffer
	if err := sentinelTmpl.Execute(&buf, req); err != nil {
		return "", fmt.Errorf("render sentinel config: %w", err)
	}
	path := filepath.Join(confDir, "sentinel.conf")
	if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		return "", fmt.Errorf("write sentinel config: %w", err)
	}
	return path, nil
}

func (a *Agent) sentinelContainerRunning() (bool, error) {
	containers, err := a.runtime.ListAll()
	if err != nil {
		return false, err
	}
	for _, container := range containers {
		if container.Name == "redis-sentinel" {
			return container.Running, nil
		}
	}
	return false, nil
}

func (a *Agent) applySentinelRuntimeConfig(req apitypes.SentinelSyncRequest, existing []string) error {
	desired := map[string]apitypes.SentinelMaster{}
	for _, master := range req.Masters {
		desired[master.Group] = master
	}
	for _, group := range existing {
		if _, ok := desired[group]; !ok {
			if _, err := a.sentinelCLI(req.Port, "SENTINEL", "REMOVE", group); err != nil {
				return err
			}
		}
	}
	for _, master := range req.Masters {
		_, _ = a.sentinelCLI(req.Port, "SENTINEL", "REMOVE", master.Group)
		if _, err := a.sentinelCLI(req.Port, "SENTINEL", "MONITOR", master.Group, master.Host, fmt.Sprintf("%d", master.Port), fmt.Sprintf("%d", req.Quorum)); err != nil {
			return err
		}
		if master.Password != "" {
			if _, err := a.sentinelCLI(req.Port, "SENTINEL", "SET", master.Group, "auth-pass", master.Password); err != nil {
				return err
			}
		}
		if _, err := a.sentinelCLI(req.Port, "SENTINEL", "SET", master.Group, "down-after-milliseconds", fmt.Sprintf("%d", master.DownAfterMilliseconds)); err != nil {
			return err
		}
		if _, err := a.sentinelCLI(req.Port, "SENTINEL", "SET", master.Group, "failover-timeout", fmt.Sprintf("%d", master.FailoverTimeout)); err != nil {
			return err
		}
		if _, err := a.sentinelCLI(req.Port, "SENTINEL", "SET", master.Group, "parallel-syncs", fmt.Sprintf("%d", master.ParallelSyncs)); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) sentinelCLI(port int, args ...string) (string, error) {
	cliArgs := append([]string{"exec", "redis-sentinel", "redis-cli", "-p", fmt.Sprintf("%d", port)}, args...)
	return a.runtime.Run(cliArgs...)
}

func (a *Agent) sentinelRemoveMaster(c *gin.Context) {
	var req apitypes.SentinelRemoveMasterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if req.Port == 0 {
		req.Port = defaultSentinelPort
	}
	if _, err := a.sentinelCLI(req.Port, "SENTINEL", "REMOVE", req.Group); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if err := a.removeMasterFromSentinelConfig(req.Group); err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	ok(c, nil)
}

func (a *Agent) removeMasterFromSentinelConfig(group string) error {
	path := filepath.Join(a.cfg.SentinelDir, "conf", "sentinel.conf")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var kept []string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == "sentinel" && fields[2] == group {
			continue
		}
		kept = append(kept, line)
	}
	return os.WriteFile(path, []byte(strings.Join(kept, "\n")), 0644)
}

func (a *Agent) sentinelStatus(c *gin.Context) {
	port := defaultSentinelPort
	status := apitypes.SentinelStatus{
		Port:    port,
		Config:  filepath.Join(a.cfg.SentinelDir, "conf", "sentinel.conf"),
		Masters: a.sentinelMastersFromConfig(),
	}
	if st, err := os.Stat(status.Config); err == nil {
		status.UpdatedAt = st.ModTime().Format(time.RFC3339)
	}
	if containers, err := a.runtime.ListAll(); err == nil {
		for _, container := range containers {
			if container.Name == "redis-sentinel" {
				status.Running = container.Running
				break
			}
		}
	}
	ok(c, status)
}

func (a *Agent) sentinelMastersFromConfig() []string {
	path := filepath.Join(a.cfg.SentinelDir, "conf", "sentinel.conf")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var masters []string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == "sentinel" && fields[1] == "monitor" {
			masters = append(masters, fields[2])
		}
	}
	return masters
}
