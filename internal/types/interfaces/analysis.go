package interfaces

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
)

type AnalysisService interface {
	GetAnalysisByAnalysisID(ctx context.Context, analysisID string) (*types.Analysis, error)
}

type AnalysisRepository interface {
	GetAnalysisByAnalysisID(ctx context.Context, analysisID string) (*types.Analysis, error)
}
