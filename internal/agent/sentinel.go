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

func (a *Agent) sentinelEnsure(c *gin.Context) {
	var req apitypes.SentinelEnsureRequest
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

	confPath, err := a.writeSentinelConfig(req)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	if err := a.recreateSentinelContainer(req.Port); err != nil {
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

func (a *Agent) writeSentinelConfig(req apitypes.SentinelEnsureRequest) (string, error) {
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

func (a *Agent) recreateSentinelContainer(port int) error {
	_, _ = a.runtime.Run("rm", "-f", "redis-sentinel")
	_, err := a.runtime.Run("run", "-d",
		"--name", "redis-sentinel",
		"--network", "host",
		"--restart", "on-failure:5",
		"-v", fmt.Sprintf("%s/conf/sentinel.conf:/etc/redis/sentinel.conf:Z", a.cfg.SentinelDir),
		"-v", fmt.Sprintf("%s/data:/data:Z", a.cfg.SentinelDir),
		"docker.io/redis:7",
		"redis-sentinel", "/etc/redis/sentinel.conf",
	)
	return err
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
	if _, err := a.runtime.Run("exec", "redis-sentinel", "redis-cli", "-p", fmt.Sprintf("%d", req.Port), "SENTINEL", "REMOVE", req.Group); err != nil {
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
