package agent

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
)

func convertMemoryToBytes(mem string) string {
	mem = strings.TrimSpace(mem)
	if mem == "" {
		return "0"
	}
	suffixes := []struct {
		suffix     string
		multiplier int64
	}{
		{"Gi", 1024 * 1024 * 1024},
		{"Mi", 1024 * 1024},
		{"Ki", 1024},
		{"GB", 1000 * 1000 * 1000},
		{"MB", 1000 * 1000},
		{"KB", 1000},
	}
	for _, s := range suffixes {
		if strings.HasSuffix(mem, s.suffix) {
			numStr := strings.TrimSuffix(mem, s.suffix)
			num, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return mem
			}
			return strconv.FormatInt(int64(num*float64(s.multiplier)), 10)
		}
	}
	return mem
}

var redisTmpl = template.Must(template.New("redis").Parse(`
port 6379
bind 0.0.0.0
{{ if .Password }}requirepass {{ .Password }}
{{ end }}timeout 300
tcp-keepalive 60
loglevel notice
databases 16

maxmemory {{ .Memory }}
maxmemory-policy {{ .MaxmemoryPolicy }}

save 3600 1 300 100 60 10000
rdbcompression yes
rdbchecksum yes
dbfilename dump.rdb
dir /data
stop-writes-on-bgsave-error yes

appendonly {{ .Appendonly }}
appendfilename "appendonly.aof"
appendfsync everysec
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb

{{ if .ReplicaOf }}
replicaof {{ .ReplicaOf }}
replica-read-only yes
replica-serve-stale-data no
replica-priority 100
{{ end }}

min-replicas-to-write {{ .MinReplicasToWrite }}
min-replicas-max-lag 10

rename-command FLUSHDB ""
rename-command FLUSHALL ""
rename-command DEBUG ""

slowlog-log-slower-than 10000
slowlog-max-len 128
maxclients 10000

{{ .ConfigOverrides }}
`))

var kvrocksTmpl = template.Must(template.New("kvrocks").Parse(`
bind 0.0.0.0
port 6666
{{ if .Password }}requirepass {{ .Password }}
{{ end }}timeout 300
log-level info

dir /data

max-db-size {{ .Memory }}

rocksdb.compression snappy
rocksdb.block_size 16384
rocksdb.max_open_files 4096
rocksdb.write_buffer_size 64
rocksdb.max_write_buffer_number 4
rocksdb.target_file_size_base 128
rocksdb.max_bytes_for_level_base 268435456
rocksdb.level0_slowdown_writes_trigger 20
rocksdb.level0_stop_writes_trigger 40
rocksdb.enable_pipelined_write yes
rocksdb.max_subcompactions 2

{{ if .ReplicaOf }}
slaveof {{ .ReplicaOf }}
slave-read-only yes
slave-priority 100
{{ end }}

slowlog-log-slower-than 10000
slowlog-max-len 128
maxclients 10000

{{ .ConfigOverrides }}
`))

type RedisConfigParams struct {
	Password           string
	Memory             string
	MaxmemoryPolicy    string
	Appendonly         string
	ReplicaOf          string
	ConfigOverrides    string
	MinReplicasToWrite int
}

type KvrocksConfigParams struct {
	Password           string
	Memory             string
	ReplicaOf          string
	ConfigOverrides    string
	MinReplicasToWrite int
}

func writeRedisConfig(dataDir string, params RedisConfigParams) error {
	params.Memory = convertMemoryToBytes(params.Memory)
	params.ReplicaOf = strings.Replace(params.ReplicaOf, ":", " ", 1)
	return writeConfig(dataDir, "redis.conf", redisTmpl, params)
}

func writeKvrocksConfig(dataDir string, params KvrocksConfigParams) error {
	params.Memory = convertMemoryToBytes(params.Memory)
	params.ReplicaOf = strings.Replace(params.ReplicaOf, ":", " ", 1)
	return writeConfig(dataDir, "kvrocks.conf", kvrocksTmpl, params)
}

func writeConfig(dataDir, filename string, tmpl *template.Template, params interface{}) error {
	confDir := filepath.Join(dataDir, "conf")
	if err := os.MkdirAll(confDir, 0755); err != nil {
		return fmt.Errorf("mkdir conf: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, params); err != nil {
		return fmt.Errorf("render template: %w", err)
	}
	return os.WriteFile(filepath.Join(confDir, filename), buf.Bytes(), 0644)
}
