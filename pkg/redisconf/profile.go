package redisconf

import "fmt"

const RedisCacheProfile = "redis-cache"
const KvrocksStorageProfile = "kvrocks-storage"

// Profiles are platform-managed, immutable purposes, not arbitrary templates.
func ValidateProfile(profile, engine, category, kind, replica string, overrides map[string]string) error {
	if err := ValidateOverrides(overrides); err != nil {
		return err
	}
	if profile == "" {
		return nil
	}
	if kind != "standalone" || replica != "" {
		return fmt.Errorf("dedicated profile requires standalone without replicas")
	}
	switch profile {
	case RedisCacheProfile:
		if engine != "redis" || category != "cache" {
			return fmt.Errorf("redis-cache requires Redis cache category")
		}
	case KvrocksStorageProfile:
		if engine != "kvrocks" || category != "persistent" {
			return fmt.Errorf("kvrocks-storage requires persistent Kvrocks")
		}
	default:
		return fmt.Errorf("unknown instance profile %q", profile)
	}
	for key := range overrides {
		if key == "maxmemory" || key == "maxmemory-policy" || key == "save" || key == "appendonly" || key == "max-db-size" {
			return fmt.Errorf("%s is managed by dedicated profile", key)
		}
	}
	return nil
}
