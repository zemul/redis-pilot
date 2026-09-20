package gateway

import (
	"context"
	"errors"
	"net/netip"
	"strconv"
	"time"
)

// Absolute expirations require synchronized cold clocks. This admission check
// detects gross skew/uncertainty; operators must still maintain NTP and avoid
// wall-clock steps. It is not a distributed clock or a zero-skew guarantee.
const migrationClockTolerance = 250 * time.Millisecond

func checkBackendClock(ctx context.Context, c migrationCaller) error {
	start := time.Now()
	b, err := c.call(ctx, "TIME")
	end := time.Now()
	if err != nil {
		return err
	}
	p, err := frameParts(b)
	if err != nil || len(p) != 2 {
		return errors.New("invalid backend clock")
	}
	seconds, ok1 := frameString(p[0])
	micros, ok2 := frameString(p[1])
	s, e1 := strconv.ParseInt(seconds, 10, 64)
	u, e2 := strconv.ParseInt(micros, 10, 64)
	if !ok1 || !ok2 || e1 != nil || e2 != nil || s < 0 || s > 1<<40 || u < 0 || u >= 1000000 {
		return errors.New("invalid backend clock")
	}
	remote := time.Unix(s, u*1000)
	if !clockIntervalSafe(start, end, remote) {
		return errors.New("cold clock skew or uncertainty exceeds 250ms; migration will retry")
	}
	return nil
}

func clockIntervalSafe(start, end, remote time.Time) bool {
	return end.Sub(start) <= migrationClockTolerance &&
		!remote.Before(end.Add(-migrationClockTolerance)) && !remote.After(start.Add(migrationClockTolerance))
}

func checkMigrationClocks(ctx context.Context, source, target *migrationFence) error {
	a, _ := netip.ParseAddrPort(source.address)
	b, _ := netip.ParseAddrPort(target.address)
	if a.Addr() == b.Addr() {
		return nil
	}
	if err := checkBackendClock(ctx, source.c); err != nil {
		return err
	}
	return checkBackendClock(ctx, target.c)
}
