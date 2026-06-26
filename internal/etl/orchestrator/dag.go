package orchestrator

import (
	"fmt"
	"strings"
)

// NodeKind classifies the role of a DAG node.
type NodeKind string

const (
	KindSource      NodeKind = "source"
	KindTransform   NodeKind = "transform"
	KindSink        NodeKind = "sink"
	KindFanout      NodeKind = "fanout"       // 1-to-N broadcast (no data change)
	KindRouter      NodeKind = "router"       // conditional 1-to-1 routing
	KindTap         NodeKind = "tap"          // pass-through side-tap (metrics/alerts)
	KindRateLimiter NodeKind = "rate_limiter" // throttle / token bucket
	KindEnricher    NodeKind = "enricher"     // HTTP/DB field enrichment
	KindLookup      NodeKind = "lookup"       // in-memory dimension table join
)

// Node represents a single processing unit in the pipeline DAG.
type Node struct {
	ID            string                 `yaml:"id" json:"id"`
	Kind          NodeKind               `yaml:"kind" json:"kind"`
	Plugin        string                 `yaml:"plugin" json:"plugin"`
	Connection    string                 `yaml:"connection,omitempty" json:"connection,omitempty"`
	ConnectionRef string                 `yaml:"connection_ref,omitempty" json:"connection_ref,omitempty"`
	Config        map[string]interface{} `yaml:"config,omitempty" json:"config,omitempty"`
	X             float64                `yaml:"x,omitempty" json:"x,omitempty"`
	Y             float64                `yaml:"y,omitempty" json:"y,omitempty"`
}

// ConditionOp defines comparison operators for edge routing.
type ConditionOp string

const (
	OpEq       ConditionOp = "eq"
	OpNe       ConditionOp = "ne"
	OpGt       ConditionOp = "gt"
	OpLt       ConditionOp = "lt"
	OpGe       ConditionOp = "ge"
	OpLe       ConditionOp = "le"
	OpContains ConditionOp = "contains"
	OpRegex    ConditionOp = "regex"
)

// Condition defines optional routing criteria on an edge.
// Only records matching the condition traverse this edge.
// If Condition is nil, all records from the source traverse.
type Condition struct {
	Field    string      `yaml:"field" json:"field"`
	Operator ConditionOp `yaml:"operator" json:"operator"`
	Value    interface{} `yaml:"value" json:"value"`
}

// Edge connects two nodes in the DAG. Data flows from From to To.
type Edge struct {
	ID        string     `yaml:"id,omitempty" json:"id,omitempty"`
	From      string     `yaml:"from" json:"from"`
	To        string     `yaml:"to" json:"to"`
	Condition *Condition `yaml:"condition,omitempty" json:"condition,omitempty"`
}

// DAG is the graph structure for a pipeline.
type DAG struct {
	Nodes []*Node `yaml:"nodes" json:"nodes"`
	Edges []*Edge `yaml:"edges" json:"edges"`
}

// NodeIDs returns the set of all node IDs.
func (d *DAG) NodeIDs() []string {
	ids := make([]string, len(d.Nodes))
	for i, n := range d.Nodes {
		ids[i] = n.ID
	}
	return ids
}

// GetNode returns the node with the given ID, or nil if not found.
func (d *DAG) GetNode(id string) *Node {
	for _, n := range d.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// Sources returns all nodes with KindSource.
func (d *DAG) Sources() []*Node {
	var result []*Node
	for _, n := range d.Nodes {
		if n.Kind == KindSource {
			result = append(result, n)
		}
	}
	return result
}

// Sinks returns all nodes with KindSink.
func (d *DAG) Sinks() []*Node {
	var result []*Node
	for _, n := range d.Nodes {
		if n.Kind == KindSink {
			result = append(result, n)
		}
	}
	return result
}

// Downstream returns all nodes that are immediate successors of the given node.
func (d *DAG) Downstream(nodeID string) []*Node {
	var result []*Node
	for _, e := range d.Edges {
		if e.From == nodeID {
			if n := d.GetNode(e.To); n != nil {
				result = append(result, n)
			}
		}
	}
	return result
}

// Upstream returns all nodes that are immediate predecessors of the given node.
func (d *DAG) Upstream(nodeID string) []*Node {
	var result []*Node
	for _, e := range d.Edges {
		if e.To == nodeID {
			if n := d.GetNode(e.From); n != nil {
				result = append(result, n)
			}
		}
	}
	return result
}

// Validate checks that the DAG is well-formed:
//   - has at least one source and one sink
//   - all edge endpoints reference existing nodes
//   - no cycles (DAG = directed acyclic graph)
//   - node IDs are unique
func (d *DAG) Validate() error {
	if len(d.Nodes) == 0 {
		return fmt.Errorf("dag has no nodes")
	}

	// Check for duplicate node IDs
	seen := map[string]bool{}
	for _, n := range d.Nodes {
		if n.ID == "" {
			return fmt.Errorf("node missing ID")
		}
		if seen[n.ID] {
			return fmt.Errorf("duplicate node ID: %s", n.ID)
		}
		seen[n.ID] = true
		if n.Plugin == "" {
			return fmt.Errorf("node %s missing plugin", n.ID)
		}
		switch n.Kind {
		case KindSource, KindTransform, KindSink,
			KindFanout, KindRouter, KindTap, KindRateLimiter, KindEnricher, KindLookup:
		default:
			return fmt.Errorf("node %s has invalid kind %q", n.ID, n.Kind)
		}
	}

	// Check edges reference existing nodes
	for _, e := range d.Edges {
		if !seen[e.From] {
			return fmt.Errorf("edge from unknown node %s", e.From)
		}
		if !seen[e.To] {
			return fmt.Errorf("edge to unknown node %s", e.To)
		}
	}

	// Check at least one source and one sink
	if len(d.Sources()) == 0 {
		return fmt.Errorf("dag has no source nodes")
	}
	if len(d.Sinks()) == 0 {
		return fmt.Errorf("dag has no sink nodes")
	}

	// Check for cycles using topological sort
	if err := d.detectCycle(); err != nil {
		return err
	}

	return nil
}

// detectCycle returns an error if the DAG contains a cycle.
func (d *DAG) detectCycle() error {
	inDegree := map[string]int{}
	adj := map[string][]string{}
	for _, n := range d.Nodes {
		inDegree[n.ID] = 0
	}
	for _, e := range d.Edges {
		inDegree[e.To]++
		adj[e.From] = append(adj[e.From], e.To)
	}

	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	visited := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++
		for _, next := range adj[node] {
			inDegree[next]--
			if inDegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}

	if visited != len(d.Nodes) {
		return fmt.Errorf("dag contains a cycle (%d of %d nodes reachable from sources)", visited, len(d.Nodes))
	}
	return nil
}

// TopoSort returns nodes in topological order (sources first, sinks last).
func (d *DAG) TopoSort() ([]string, error) {
	if err := d.detectCycle(); err != nil {
		return nil, err
	}
	inDegree := map[string]int{}
	adj := map[string][]string{}
	for _, n := range d.Nodes {
		inDegree[n.ID] = 0
	}
	for _, e := range d.Edges {
		inDegree[e.To]++
		adj[e.From] = append(adj[e.From], e.To)
	}
	var result []string
	var queue []string
	for _, n := range d.Nodes {
		if inDegree[n.ID] == 0 {
			queue = append(queue, n.ID)
		}
	}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		result = append(result, node)
		for _, next := range adj[node] {
			inDegree[next]--
			if inDegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	return result, nil
}

// String returns a human-readable representation of the DAG.
func (d *DAG) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("DAG(%d nodes, %d edges):\n", len(d.Nodes), len(d.Edges)))
	for _, n := range d.Nodes {
		b.WriteString(fmt.Sprintf("  [%s] %s/%s\n", n.Kind, n.ID, n.Plugin))
	}
	for _, e := range d.Edges {
		if e.Condition != nil {
			b.WriteString(fmt.Sprintf("  %s -> %s (if %s %s %v)\n", e.From, e.To, e.Condition.Field, e.Condition.Operator, e.Condition.Value))
		} else {
			b.WriteString(fmt.Sprintf("  %s -> %s\n", e.From, e.To))
		}
	}
	return b.String()
}
