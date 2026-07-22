package interfaces

import "context"

type DagOrchestrator interface {
	StartAsync(ctx context.Context, analysisID int64) error
	StopAsync(ctx context.Context, analysisID int64) error
	RecoverRunningAnalyses(ctx context.Context) (int, error)
}
