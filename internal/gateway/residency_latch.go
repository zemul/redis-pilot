package gateway

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

// Once hot keys can be absent, reopening as ordinary passthrough is unsafe.
// The controller's binding lock must already be held. No automatic downgrade.
func checkResidencyLatch(dir string, binding ConfirmationBinding, enabled bool) error {
	path := filepath.Join(dir, "manual-residency.enabled")
	f, created, err := openFinishingFile(path, enabled)
	if errors.Is(err, os.ErrNotExist) && !enabled {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	if !enabled {
		return errConfirmationRPC
	}
	want := "redis-pilot-manual-residency-v1:" + evictionHash(binding) + "\n"
	if created {
		if _, err := f.WriteString(want); err != nil {
			return err
		}
	} else {
		raw, err := io.ReadAll(io.LimitReader(f, int64(len(want)+1)))
		if err != nil || string(raw) != want {
			return errConfirmationRPC
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
