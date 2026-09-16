package gateway

import (
	"errors"
	"net/netip"
)

// Remote passwordless engines must be explicitly admitted by the operator.
// This is an outbound allowlist, not a substitute for a backend firewall.
func (c CacheConfig) validateBackendNetworks() error {
	if len(c.BackendNetworks) > 32 {
		return errors.New("at most 32 backend networks are allowed")
	}
	for _, raw := range c.BackendNetworks {
		p, err := netip.ParsePrefix(raw)
		if err != nil || p != p.Masked() || p.Addr().Is4In6() || !p.Addr().IsPrivate() ||
			(p.Addr().Is4() && p.Bits() < 8) || (p.Addr().Is6() && p.Bits() < 7) {
			return errors.New("backend_networks requires canonical private CIDRs")
		}
		// Require the entire prefix to fit an RFC1918/ULA private range.
		valid := false
		for _, block := range []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7"} {
			b := netip.MustParsePrefix(block)
			valid = valid || (b.Contains(p.Addr()) && p.Bits() >= b.Bits())
		}
		if !valid {
			return errors.New("backend network includes non-private addresses")
		}
	}
	return nil
}

func (c CacheConfig) allowsBackend(address string) bool {
	a, err := netip.ParseAddrPort(address)
	ip := a.Addr().Unmap()
	if err != nil || a.Port() == 0 || ip.Zone() != "" {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	if !ip.IsPrivate() {
		return false
	}
	for _, raw := range c.BackendNetworks {
		p, err := netip.ParsePrefix(raw)
		if err == nil && p.Contains(ip) {
			return true
		}
	}
	return false
}
