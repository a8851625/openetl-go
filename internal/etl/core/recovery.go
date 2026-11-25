package core

// RecoveryCaptureContext contains runtime-owned identity, not inferred cause.
// Empty context is explicitly unassociated (legacy/direct callers).
type RecoveryCaptureContext struct {
	Endpoint        string `json:"endpoint,omitempty"`
	NodeID          string `json:"node_id,omitempty"`
	ConnectionName  string `json:"connection_name,omitempty"`
	ConnectionKnown bool   `json:"connection_known"`
	SpecVersion     int    `json:"spec_version,omitempty"`
}
