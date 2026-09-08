package gateway

import (
	"context"
	"net"
	"strconv"
	"strings"
	"time"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

// Published only with a complete cached local observation, not on unrelated
// retry/lifecycle updates. HTTP reads never touch the journal or the network.
type syncProgressSnapshot struct {
	Captured, Applied, Confirmed syncPosition
	Resources                    syncResourceStatus
	At                           time.Time
}

func recordSyncProgress(s *syncRunnerStatus, captured, applied, confirmed syncPosition, resources syncResourceStatus, at time.Time) {
	s.Progress = syncProgressSnapshot{captured, applied, confirmed, resources, at}
}

func publicSyncPosition(p syncPosition, source string) *apitypes.SyncProgressPosition {
	if !validSyncMetricPosition(p) || syncCanonicalEndpoint(p.Source) != syncCanonicalEndpoint(source) || syncCanonicalEndpoint(source) == "" {
		return nil
	}
	return &apitypes.SyncProgressPosition{History: strings.ToLower(p.History), Offset: p.Offset}
}

func (t *IsolatedSyncTask) localProgress() *apitypes.SyncLocalProgress {
	p := t.runner.snapshot().Progress
	if p.At.IsZero() {
		return nil
	}
	l := &apitypes.SyncLocalProgress{SampledAt: p.At, Captured: publicSyncPosition(p.Captured, t.options.SourceAddress), Applied: publicSyncPosition(p.Applied, t.options.SourceAddress), GCConfirmed: publicSyncPosition(p.Confirmed, t.options.SourceAddress), LiveJournalBytes: p.Resources.LiveBytes, JournalLimitBytes: t.options.JournalBytes, DiskPaused: p.Resources.DiskPaused, BacklogPaused: p.Resources.BacklogPaused}
	if p.Resources.AvailableKnown {
		v := p.Resources.AvailableBytes
		l.AvailableBytes = &v
	}
	return l
}

// SampleProgress performs only AUTH (when configured) and INFO replication on
// a separate bounded connection. No PSYNC, data writes, cold probes or retries.
// Snapshot local state first so a healthy unchanged lineage has source >= local.
func (t *IsolatedSyncTask) SampleProgress(ctx context.Context, timeout time.Duration) apitypes.SyncProgress {
	p := apitypes.SyncProgress{Local: t.localProgress()}
	p.Source = t.sampleSourceProgress(ctx, timeout)
	return p
}

func (t *IsolatedSyncTask) sampleSourceProgress(ctx context.Context, timeout time.Duration) (sample apitypes.SyncSourceProgress) {
	sample.Error = "source_unavailable"
	defer func() { sample.SampledAt = time.Now() }()
	if ctx == nil || timeout <= 0 || timeout > 2*time.Second {
		return sample
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if ctx.Err() != nil {
		sample.Error = "canceled"
		return sample
	}
	if _, _, ok := syncLiteralEndpoint(t.options.SourceAddress); !ok {
		sample.Error = "invalid_response"
		return sample
	}
	password, err := syncReadPassword(t.options.SourcePasswordFile)
	if err != nil {
		sample.Error = "credential_file"
		return sample
	}
	conn, err := (&net.Dialer{Timeout: timeout}).DialContext(ctx, "tcp", t.options.SourceAddress)
	if err != nil {
		return sample
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if conn.SetDeadline(deadline) != nil {
		return sample
	}
	r, _ := NewReader(conn, Limits{MaxBytes: 16 << 10, MaxElements: 8, MaxDepth: 2})
	request := func(args ...string) ([]byte, error) {
		if err := writeAll(conn, encodeCommand(args...)); err != nil {
			return nil, err
		}
		return r.ReadFrame()
	}
	if password != "" {
		reply, err := request("AUTH", password)
		if err != nil {
			return sample
		}
		if string(reply) != "+OK\r\n" {
			sample.Error = "source_auth"
			return sample
		}
	}
	raw, err := request("INFO", "replication")
	if err != nil {
		if !syncProbeTransportError(ctx, err) {
			sample.Error = "invalid_response"
		}
		return sample
	}
	position, reason := parseSyncSourceInfo(raw)
	sample.Position, sample.Error = position, reason
	return sample
}

func syncProbeTransportError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}
	return syncTransportFailure(ctx, errSyncSourceTransport, "observation", err) != nil
}

func parseSyncSourceInfo(raw []byte) (*apitypes.SyncProgressPosition, string) {
	invalid := "invalid_response"
	if len(raw) > 16<<10 {
		return nil, invalid
	}
	header, body, ok := strings.Cut(string(raw), "\r\n")
	if !ok || !strings.HasPrefix(header, "$") {
		return nil, invalid
	}
	n, err := strconv.Atoi(header[1:])
	if err != nil || n < 0 || n > len(body) || len(body)-n != 2 || body[n:] != "\r\n" {
		return nil, invalid
	}
	fields := map[string]string{}
	for _, line := range strings.Split(body[:n], "\r\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			return nil, invalid
		}
		if key == "role" || key == "master_replid" || key == "master_repl_offset" {
			if _, duplicate := fields[key]; duplicate {
				return nil, invalid
			}
			fields[key] = value
		}
	}
	if fields["role"] != "master" {
		if fields["role"] == "slave" || fields["role"] == "replica" {
			return nil, "not_master"
		}
		return nil, invalid
	}
	offset, err := strconv.ParseInt(fields["master_repl_offset"], 10, 64)
	position := syncPosition{Source: "source", History: fields["master_replid"], Offset: offset}
	if err != nil || !validSyncMetricPosition(position) || position.History == strings.Repeat("0", 40) || strconv.FormatInt(offset, 10) != fields["master_repl_offset"] {
		return nil, invalid
	}
	return &apitypes.SyncProgressPosition{History: strings.ToLower(position.History), Offset: offset}, "none"
}
