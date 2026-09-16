package store

import (
	"encoding/json"
	"strings"
	"testing"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func TestColdIngressReservationAndSecrets(t *testing.T) {
	d := openTestDB(t)
	spec := coldFixture(t, d)
	if _, err := d.CreateColdCluster(spec); err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertGroup("ordinary", &apitypes.InstanceGroupState{Proxy: &apitypes.ProxyConfig{MasterPort: 18000}}); err != nil {
		t.Fatal(err)
	}
	req := apitypes.ColdIngressRequest{GatewayAddress: "127.0.0.1:19000", Password: "business-secret"}
	generation := d.Generation()
	v, err := d.CreateColdIngress(spec.ID, req, 18000, 18002)
	if err != nil || v.Port != 18001 || v.Ready || !v.HasPassword || d.Generation() == generation {
		t.Fatal(v, err)
	}
	if _, err := d.CreateColdIngress(spec.ID, req, 18000, 18002); err != nil {
		t.Fatal(err)
	}
	changed := req
	changed.Password = "other"
	if _, err := d.CreateColdIngress(spec.ID, changed, 18000, 18002); err == nil {
		t.Fatal("changed live credential")
	}
	if err := d.ConfirmColdIngress("wrong", spec.ID, req.GatewayAddress); err == nil {
		t.Fatal("wrong Gateway confirmed")
	}
	if err := d.ConfirmColdIngress(spec.GatewayID, spec.ID, req.GatewayAddress); err != nil {
		t.Fatal(err)
	}
	if v, err := d.CreateColdIngress(spec.ID, req, 18000, 18002); err != nil || !v.Ready {
		t.Fatal(v, err)
	}
	c, err := d.GetColdCluster(spec.ID)
	if err != nil || c.Ingress == nil || !c.Ingress.Ready {
		t.Fatal(c, err)
	}
	b, _ := json.Marshal(c)
	if strings.Contains(string(b), req.Password) {
		t.Fatal("secret leaked")
	}
	routes, err := d.ColdIngressRoutes(spec.NodeID)
	if err != nil || len(routes) != 1 || routes[0].Password != req.Password {
		t.Fatal(routes, err)
	}
	routes, err = d.ColdIngressRoutes("another")
	if err != nil || len(routes) != 0 {
		t.Fatal(routes, err)
	}
	if err := d.UpsertGroup("collision", &apitypes.InstanceGroupState{Proxy: &apitypes.ProxyConfig{MasterPort: v.Port}}); err == nil {
		t.Fatal("port collision")
	}
	if err := d.UpsertGroup("cold:"+spec.ID, &apitypes.InstanceGroupState{}); err == nil {
		t.Fatal("name collision")
	}
	if err := d.WithInstances(func(st *apitypes.InstancesState) error {
		if len(st.ReservedProxyPorts) != 1 || st.ReservedProxyPorts[0] != v.Port {
			t.Fatal(st.ReservedProxyPorts)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
