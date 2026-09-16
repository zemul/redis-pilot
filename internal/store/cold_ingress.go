package store

import (
	"database/sql"
	"net/netip"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

type ColdIngressRoute struct {
	apitypes.ColdIngress
	Password string `json:"-"`
}

func coldIngressPorts(q interface {
	Query(string, ...any) (*sql.Rows, error)
}) ([]int, error) {
	rows, err := q.Query(`SELECT port FROM cold_ingress`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ports []int
	for rows.Next() {
		var p int
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		ports = append(ports, p)
	}
	return ports, rows.Err()
}

// Immutable registration: retries must match, including password. Changing
// live routes/credentials requires a separate drain protocol, not blind upsert.
func (d *DB) CreateColdIngress(id string, req apitypes.ColdIngressRequest, start, end int) (*apitypes.ColdIngress, error) {
	a, err := netip.ParseAddrPort(req.GatewayAddress)
	if err != nil || !a.Addr().IsLoopback() || a.Port() == 0 || start < 1 || end > 65535 || end < start || len(req.Password) > 4096 {
		return nil, coldInvalid("valid loopback Gateway address and Proxy port range required")
	}
	req.GatewayAddress = netip.AddrPortFrom(a.Addr().Unmap(), a.Port()).String()
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	c, err := loadColdCluster(tx, id)
	if err != nil {
		return nil, err
	}
	var oldAddress, oldPass string
	var oldPort int
	err = tx.QueryRow(`SELECT address,port,password FROM cold_ingress WHERE cluster_id=?`, id).Scan(&oldAddress, &oldPort, &oldPass)
	if err == nil {
		if oldAddress != req.GatewayAddress || oldPass != req.Password {
			return nil, ErrColdConflict
		}
		return c.Ingress, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}
	st, err := loadInstancesStateTx(tx)
	if err != nil {
		return nil, err
	}
	used := map[int]bool{}
	used[int(a.Port())] = true // Proxy and its local Gateway cannot bind the same port
	for _, p := range st.ReservedProxyPorts {
		used[p] = true
	}
	for _, g := range st.Groups {
		if g.Proxy != nil {
			used[g.Proxy.MasterPort], used[g.Proxy.AutoPort] = true, true
		}
	}
	if st.Groups["cold:"+id] != nil {
		return nil, ErrColdConflict
	}
	for _, i := range st.Instances {
		if i.Server == c.NodeID {
			used[i.Port] = true
		}
		if i.Server == c.NodeID && i.Port == int(a.Port()) {
			return nil, coldInvalid("Gateway address points to a database instance")
		}
	}
	var duplicate int
	if err := tx.QueryRow(`SELECT count(*) FROM cold_ingress WHERE node_id=? AND address=?`, c.NodeID, req.GatewayAddress).Scan(&duplicate); err != nil {
		return nil, err
	}
	if duplicate != 0 {
		return nil, ErrColdConflict
	}
	port := 0
	for p := start; p <= end; p++ {
		if !used[p] {
			port = p
			break
		}
	}
	if port == 0 {
		return nil, ErrColdConflict
	}
	if _, err := tx.Exec(`INSERT INTO cold_ingress(cluster_id,node_id,address,port,password) VALUES(?,?,?,?,?)`, id, c.NodeID, req.GatewayAddress, port, req.Password); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	d.bumpGeneration()
	return &apitypes.ColdIngress{ClusterID: id, NodeID: c.NodeID, GatewayAddress: req.GatewayAddress, Port: port, HasPassword: req.Password != ""}, nil
}

// Secrets are available only for machine Proxy snapshots, not human responses.
func (d *DB) ColdIngressRoutes(node string) ([]ColdIngressRoute, error) {
	rows, err := d.sql.Query(`SELECT cluster_id,node_id,address,port,password,ready FROM cold_ingress WHERE node_id=? ORDER BY cluster_id`, node)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ColdIngressRoute
	for rows.Next() {
		var v ColdIngressRoute
		if err := rows.Scan(&v.ClusterID, &v.NodeID, &v.GatewayAddress, &v.Port, &v.Password, &v.Ready); err != nil {
			return nil, err
		}
		v.HasPassword = v.Password != ""
		out = append(out, v)
	}
	return out, rows.Err()
}

func (d *DB) ConfirmColdIngress(gateway, id, address string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	c, err := d.GetColdCluster(id)
	if err != nil {
		return err
	}
	if c.GatewayID != gateway || c.Ingress == nil || c.Ingress.GatewayAddress != address {
		return ErrColdConflict
	}
	if c.Ingress.Ready {
		return nil
	}
	return d.execWrite(`UPDATE cold_ingress SET ready=1 WHERE cluster_id=?`, id)
}
