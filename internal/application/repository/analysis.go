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

func (r *analysisRepository) WithTransaction(ctx context.Context, fn func(interfaces.AnalysisRepository) error) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := &analysisRepository{db: tx}
		return fn(txRepo)
	})
}

func (r *analysisRepository) CreateAnalysis(ctx context.Context, item *types.Analysis) error {
	return r.db.WithContext(ctx).Create(item).Error
}

func (r *analysisRepository) UpdateAnalysisByAnalysisID(ctx context.Context, analysisID string, values map[string]any) error {
	if len(values) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Model(&types.Analysis{}).Where("analysis_id = ?", analysisID).Updates(values).Error
}

func (r *analysisRepository) GetAnalysisByAnalysisID(ctx context.Context, analysisID string) (*types.Analysis, error) {
	item := &types.Analysis{}
	if err := r.db.WithContext(ctx).Where("analysis_id = ?", analysisID).Take(item).Error; err != nil {
		return nil, err
	}
	return item, nil
}

func (r *analysisRepository) GetAnalysisNodeByID(ctx context.Context, id int64) (*types.AnalysisNode, error) {
	item := &types.AnalysisNode{}
	if err := r.db.WithContext(ctx).Where("id = ?", id).Take(item).Error; err != nil {
		return nil, err
	}
	return item, nil
}

func (r *analysisRepository) GetAnalysisNodeByAnalysisNodeID(ctx context.Context, analysisNodeID string) (*types.AnalysisNode, error) {
	item := &types.AnalysisNode{}
	if err := r.db.WithContext(ctx).Where("analysis_node_id = ?", analysisNodeID).Take(item).Error; err != nil {
		return nil, err
	}
	return item, nil
}

func (r *analysisRepository) ListAnalysisNodesByAnalysisID(ctx context.Context, analysisID string) ([]*types.AnalysisNode, error) {
	items := make([]*types.AnalysisNode, 0)
	err := r.db.WithContext(ctx).Where("analysis_id = ?", analysisID).Find(&items).Error
	if err != nil {
		return nil, err
	}
	return items, nil
}

func (r *analysisRepository) DeleteAnalysisNodesByAnalysisID(ctx context.Context, analysisID string) error {
	return r.db.WithContext(ctx).Where("analysis_id = ?", analysisID).Delete(&types.AnalysisNode{}).Error
}

func (r *analysisRepository) CreateAnalysisNodes(ctx context.Context, items []*types.AnalysisNode) error {
	if len(items) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Create(&items).Error
}

func (r *analysisRepository) DeleteAnalysisEdgesByAnalysisID(ctx context.Context, analysisID string) error {
	return r.db.WithContext(ctx).Where("analysis_id = ?", analysisID).Delete(&types.AnalysisEdge{}).Error
}

func (r *analysisRepository) CreateAnalysisEdges(ctx context.Context, items []*types.AnalysisEdge) error {
	if len(items) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Create(&items).Error
}
