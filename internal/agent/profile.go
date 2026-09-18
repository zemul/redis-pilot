package agent

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/redisconf"
)

// The rendered file retains purpose across Agent restarts. Missing purpose in
// an old request must not silently render a dedicated cache as ordinary Redis.
func (a *Agent) checkProfile(name, profile, engine, category, kind, replica string, overrides map[string]string) error {
	if err := redisconf.ValidateProfile(profile, engine, category, kind, replica, overrides); err != nil {
		return err
	}
	file := "redis.conf"
	if engine == "kvrocks" {
		file = "kvrocks.conf"
	}
	b, err := os.ReadFile(filepath.Join(a.cfg.DataDir, name, "conf", file))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	stored := ""
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "# redis-pilot profile: ") {
			stored = strings.TrimSpace(strings.TrimPrefix(line, "# redis-pilot profile: "))
			break
		}
	}
	if stored != profile {
		return errors.New("instance profile differs from existing config; in-place conversion is not supported")
	}
	return nil
}
