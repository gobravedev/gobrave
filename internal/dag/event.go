package dag

import "time"

const (
	EventDagStarted      = "dag.started"
	EventDagCompleted    = "dag.completed"
	EventDagFailed       = "dag.failed"
	EventNodeSubmitted   = "dag.node.submitted"
	EventNodeRunning     = "dag.node.running"
	EventNodeCompleted   = "dag.node.completed"
	EventNodeFailed      = "dag.node.failed"
	EventNodeStateChange = "dag.node.state_changed"
)

type RuntimeEvent struct {
	Name           string                 `json:"name"`
	AnalysisID     int64                  `json:"analysis_id,string"`
	AnalysisNodeID int64                  `json:"analysis_node_id,string,omitempty"`
	NodeID         string                 `json:"node_id,omitempty"`
	OccurredAt     time.Time              `json:"occurred_at"`
	Payload        map[string]interface{} `json:"payload,omitempty"`
}
