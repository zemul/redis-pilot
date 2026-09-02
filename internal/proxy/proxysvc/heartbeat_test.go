package proxysvc

import "testing"

func TestProxyIDUsesConfiguredValueOrHostname(t *testing.T) {
	configured := New(Config{ProxyID: "  proxy-a  "})
	if configured.cfg.ProxyID != "proxy-a" {
		t.Fatalf("configured proxy id = %q", configured.cfg.ProxyID)
	}

	fallback := New(Config{})
	if fallback.cfg.ProxyID == "" {
		t.Fatal("fallback proxy id must not be empty")
	}
}
