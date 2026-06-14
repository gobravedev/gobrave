package interfaces

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
)

type AnalysisService interface {
	GetAnalysisByAnalysisID(ctx context.Context, analysisID string) (*types.Analysis, error)
	GetAnalysisNodeByAnalysisNodeID(ctx context.Context, analysisNodeID string) (*types.AnalysisNode, error)
}

type AnalysisRepository interface {
	GetAnalysisByAnalysisID(ctx context.Context, analysisID string) (*types.Analysis, error)
	GetAnalysisNodeByAnalysisNodeID(ctx context.Context, analysisNodeID string) (*types.AnalysisNode, error)
}
