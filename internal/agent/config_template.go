package agent

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"text/template"
)

var redisTmpl = template.Must(template.New("redis").Parse(`
port 6379
bind 0.0.0.0
requirepass {{ .Password }}
timeout 300
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

min-replicas-to-write 1
min-replicas-max-lag 10

proto 2

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
requirepass {{ .Password }}
timeout 300
tcp-keepalive 60
loglevel info
databases 16

dir /data

rocksdb.compression snappy
rocksdb.block_size 16384
rocksdb.max_open_files 4096
rocksdb.write_buffer_size 64MB
rocksdb.max_write_buffer_number 4
rocksdb.target_file_size_base 64MB
rocksdb.max_bytes_for_level_base 256MB
rocksdb.level0_slowdown_writes_trigger 20
rocksdb.level0_stop_writes_trigger 40
rocksdb.enable_pipelined_write yes
rocksdb.max_sub_compactions 2

{{ if .ReplicaOf }}
replicaof {{ .ReplicaOf }}
replica-read-only yes
replica-priority 100
{{ end }}

min-replicas-to-write 1
min-replicas-max-lag 10

slowlog-log-slower-than 10000
slowlog-max-len 128
maxclients 10000

checkpoint-dir /backup

{{ .ConfigOverrides }}
`))

type RedisConfigParams struct {
	Password        string
	Memory          string
	MaxmemoryPolicy string
	Appendonly      string
	ReplicaOf       string
	ConfigOverrides string
}

type KvrocksConfigParams struct {
	Password        string
	ReplicaOf       string
	ConfigOverrides string
}

func writeRedisConfig(dataDir string, params RedisConfigParams) error {
	return writeConfig(dataDir, "redis.conf", redisTmpl, params)
}

func writeKvrocksConfig(dataDir string, params KvrocksConfigParams) error {
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
