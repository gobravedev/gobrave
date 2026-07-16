package interfaces

import "context"

type DagOrchestrator interface {
	StartAsync(ctx context.Context, analysisID string) error
	StopAsync(ctx context.Context, analysisID string) error
	RecoverRunningAnalyses(ctx context.Context) (int, error)
}
