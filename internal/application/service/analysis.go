package service

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type analysisService struct {
	analysisRepo interfaces.AnalysisRepository
}

func NewAnalysisService(analysisRepo interfaces.AnalysisRepository) interfaces.AnalysisService {
	return &analysisService{analysisRepo: analysisRepo}
}

func (s *analysisService) GetAnalysisByAnalysisID(ctx context.Context, analysisID string) (*types.Analysis, error) {
	return s.analysisRepo.GetAnalysisByAnalysisID(ctx, analysisID)
}
