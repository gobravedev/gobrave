package repository

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/gorm"
)

type analysisRepository struct {
	db *gorm.DB
}

func NewAnalysisRepository(db *gorm.DB) interfaces.AnalysisRepository {
	return &analysisRepository{db: db}
}

func (r *analysisRepository) GetAnalysisByAnalysisID(ctx context.Context, analysisID string) (*types.Analysis, error) {
	item := &types.Analysis{}
	if err := r.db.WithContext(ctx).Where("analysis_id = ?", analysisID).Take(item).Error; err != nil {
		return nil, err
	}
	return item, nil
}
