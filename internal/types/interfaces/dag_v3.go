package interfaces

import "context"

// DataflowDagOrchestrator defines the V3 orchestration contract.
//
// V3 follows a dataflow-first model (Nextflow-like):
// - Channels carry values/events between processes.
// - Process instances are materialized when channel inputs are available.
// - Existing persistence schema and executors are reused.
type DataflowDagOrchestrator interface {
	StartAsyncV3(ctx context.Context, projectID int64, analysisID string, parseAnalysisResult map[string]any, dagDefinition map[string]any) error
}
