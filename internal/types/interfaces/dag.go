package interfaces

import "context"

type DagOrchestrator interface {
	EnsureCompletionCoordinatorStarted(ctx context.Context)
	StartAsync(ctx context.Context, analysisID string) error
	StopAsync(ctx context.Context, analysisID string) error
	RecoverRunningAnalyses(ctx context.Context) (int, error)
}
