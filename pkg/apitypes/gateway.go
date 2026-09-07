package apitypes

// GatewayDraftSpec is planning metadata, not a deployable runtime configuration.
// Ports are proposals only; secrets and arbitrary backend URLs are not accepted.
type GatewayDraftSpec struct {
	Node      string `json:"node"`
	Group     string `json:"group"`
	Mode      string `json:"mode"`
	DataPort  int    `json:"data_port"`
	AdminPort int    `json:"admin_port"`
}

type GatewayDraft struct {
	Name      string           `json:"name"`
	Spec      GatewayDraftSpec `json:"spec"`
	Revision  string           `json:"revision"` // Opaque CAS token; changes even after delete/recreate.
	Status    string           `json:"status"`   // Always draft; never implies deployment or readiness.
	CreatedAt string           `json:"created_at"`
	UpdatedAt string           `json:"updated_at"`
}
