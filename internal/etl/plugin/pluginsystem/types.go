package pluginsystem

// PluginKind classifies plugin types.
type PluginKind string

const (
	KindTransform PluginKind = "transform"
	KindSource    PluginKind = "source"
	KindSink      PluginKind = "sink"
)

// PluginMeta describes an installed plugin.
type PluginMeta struct {
	Name              string          `json:"name"`
	Kind              PluginKind      `json:"kind"`
	Version           string          `json:"version"`
	ABI               string          `json:"abi,omitempty"`
	MinRuntimeVersion string          `json:"min_runtime_version,omitempty"`
	ManifestValidated bool            `json:"manifest_validated"`
	Manifest          *PluginManifest `json:"manifest,omitempty"`
	Enabled           bool            `json:"enabled"`
	Path              string          `json:"path"`
}
