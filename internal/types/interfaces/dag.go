package interfaces

import "context"

type DagOrchestrator interface {
	StartAsync(ctx context.Context, analysisID string) error
}
