package gateway

import (
	"context"
	"errors"
	"time"
)

// Explicit development mode, not enrollment or multi-Gateway ownership.
type HotColdConfig struct {
	AutoEviction        *AutoEvictionConfig `yaml:"auto_eviction"`
	Binding             ConfirmationBinding `yaml:"binding"`
	SyncerEndpoint      string              `yaml:"syncer_endpoint"`
	SyncerTokenFile     string              `yaml:"syncer_token_file"`
	AdminTokenFile      string              `yaml:"admin_token_file"`
	ColdPasswordFile    string              `yaml:"cold_password_file"`
	ConfirmExclusiveHot bool                `yaml:"confirm_exclusive_hot"`
	Timeout             time.Duration       `yaml:"timeout"`
}

func (c Config) hotColdOptions() hotPreparationOptions {
	h := c.HotCold
	return hotPreparationOptions{
		Confirmation:   IsolatedConfirmationGatewayOptions{Binding: h.Binding, Endpoint: h.SyncerEndpoint, TokenFile: h.SyncerTokenFile, OperationDir: c.StateDir, Timeout: h.Timeout},
		AdminTokenFile: h.AdminTokenFile, ColdPasswordFile: h.ColdPasswordFile,
		ExclusiveHot: h.ConfirmExclusiveHot, BoundedMarkers: true, ManualResidency: true,
	}
}

func (c Config) validateHotCold() error {
	if c.HotCold == nil {
		if c.StateDir != "" {
			return errors.New("state_dir requires hot_cold; do not downgrade a partial hot dataset")
		}
		return nil
	}
	h := c.HotCold
	if c.Mode != "passthrough" || !syncAbsolutePath(c.StateDir) || !h.ConfirmExclusiveHot ||
		h.Binding.GatewayID != c.GatewayID || h.Binding.Source != c.RedisAddress || h.Binding.Validate() != nil ||
		h.Timeout <= 0 || h.Timeout > 4*time.Second || !syncAbsolutePath(h.AdminTokenFile) ||
		(h.ColdPasswordFile != "" && !syncAbsolutePath(h.ColdPasswordFile)) ||
		(h.ColdPasswordFile == "" && !c.AllowUnauthenticated) ||
		(c.BackendPasswordFile == "" && !c.AllowUnauthenticated) ||
		c.Limits.MaxDepth < 2 || c.Limits.MaxBytes < 256<<10 || c.BufferBudgetBytes < 2*c.workspaceBytes()+c.probeWorkspaceBytes() {
		return errors.New("invalid hot_cold identity, ownership, credentials, timeout or buffer budget")
	}
	if err := h.AutoEviction.validate(); err != nil {
		return err
	}
	if h.AutoEviction != nil && h.AutoEviction.Enabled && c.BufferBudgetBytes < 3*c.workspaceBytes()+c.probeWorkspaceBytes() {
		return errors.New("auto_eviction requires an additional background workspace")
	}
	return validateConfirmationGatewayShape(c.hotColdOptions().Confirmation)
}

func (s *Service) configureHotCold() error {
	if s.cfg.HotCold == nil {
		return nil
	}
	o := s.cfg.hotColdOptions()
	syncToken, err := validateConfirmationGateway(o.Confirmation)
	if err != nil {
		return err
	}
	adminToken, err := confirmationToken(o.AdminTokenFile)
	if err != nil || adminToken == syncToken || adminToken == s.frontendPassword || adminToken == s.backendPassword {
		return errors.New("hot_cold requires readable, independent management credentials")
	}
	if _, err := syncReadPassword(o.ColdPasswordFile); err != nil {
		return err
	}
	s.preparationOptions = &o
	s.admissionBoot = &admissionRunOptions{Dir: s.cfg.StateDir, Binding: o.Confirmation.Binding, MaxKeys: 1024, MaxBytes: 1 << 20, MaxActive: 1024}
	return nil
}

// One-time provisioning into an existing EMPTY private directory. Normal
// startup resumes and never reconstructs missing state. No listeners/backends.
func (s *Service) InitializeHotCold() error {
	if s.cfg.HotCold == nil || s.admissionBoot == nil {
		return errors.New("init-state requires hot_cold configuration")
	}
	o := *s.admissionBoot
	o.Create = true
	boot, err := openAdmissionRun(o, nil)
	if err != nil {
		return err
	}
	defer boot.close()
	s.admission = boot.barriers
	defer func() { s.admission = nil }()
	h, err := openHotPreparation(s, *s.preparationOptions)
	if err != nil {
		return err
	}
	h.Close()
	return boot.finish()
}

func (s *Service) verifyHotColdSource(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.ConnectTimeout+s.cfg.RequestTimeout)
	defer cancel()
	c, r, err := s.openBackend(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	if err := writeAll(c, encodeCommand("INFO", "replication")); err != nil {
		return err
	}
	raw, err := r.ReadFrame()
	p, code := parseSyncSourceInfo(raw)
	if err != nil || code != "none" || p.History != s.cfg.HotCold.Binding.History {
		return errors.New("hot_cold source history mismatch or unavailable")
	}
	if err := writeAll(c, encodeCommand("CONFIG", "GET", "maxmemory-policy")); err != nil {
		return err
	}
	raw, err = r.ReadFrame()
	if err != nil || string(raw) != "*2\r\n$16\r\nmaxmemory-policy\r\n$10\r\nnoeviction\r\n" {
		return errors.New("hot_cold requires Redis noeviction")
	}
	return nil
}
