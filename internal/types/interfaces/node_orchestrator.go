package interfaces

import "context"

// NodeOrchestrator dispatches a single analysis node without requiring DAG analysis scheduling.
type NodeOrchestrator interface {
	StartAsync(ctx context.Context, analysisNodeID int64) error
	StopAsync(ctx context.Context, analysisNodeID int64) error
}
