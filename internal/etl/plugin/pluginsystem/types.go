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
	Name    string     `json:"name"`
	Kind    PluginKind `json:"kind"`
	Version string     `json:"version"`
	Enabled bool       `json:"enabled"`
	Path    string     `json:"path"`
}
