package server

import (
	"net"
	"testing"
)

func TestSentinelPrimaryVersion(t *testing.T) {
	agent := newFakeAgentWithContainers(nil)
	defer agent.Close()
	s := newTestServer(t, "")
	host := setupReplicationGroup(t, s, agent.Listener.Addr().String(), "order-master")
	master, before, err := s.db.GroupPrimaryVersion("order")
	if err != nil || master != "order-master" || before == 0 {
		t.Fatal(master, before, err)
	}
	if err = s.handleSentinelFailover("order", net.JoinHostPort(host, "6380"), "test", "test"); err != nil {
		t.Fatal(err)
	}
	master, after, err := s.db.GroupPrimaryVersion("order")
	if err != nil || master != "order-replica" || after != before+1 {
		t.Fatal(master, after, err)
	}
	if err = s.handleSentinelFailover("order", net.JoinHostPort(host, "6380"), "test", "test"); err != nil {
		t.Fatal(err)
	}
	_, replay, err := s.db.GroupPrimaryVersion("order")
	if err != nil || replay != after {
		t.Fatal("duplicate notification changed generation", replay, err)
	}
	if err = s.handleSentinelFailover("order", net.JoinHostPort(host, "6379"), "test", "test"); err != nil {
		t.Fatal(err)
	}
	master, returned, err := s.db.GroupPrimaryVersion("order")
	if err != nil || master != "order-master" || returned != after+1 {
		t.Fatal(master, returned, err)
	}
}
