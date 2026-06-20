package repository

import (
	"context"
	"time"

	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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

func (r *analysisRepository) TryMarkAnalysisRunning(ctx context.Context, analysisID string, now time.Time, staleBefore time.Time) (bool, error) {
	result := r.db.WithContext(ctx).
		Model(&types.Analysis{}).
		Where("analysis_id = ? AND (job_status IS NULL OR job_status <> ? OR updated_at IS NULL OR updated_at < ?)", analysisID, "running", staleBefore).
		Updates(map[string]any{
			"job_status": "running",
			"updated_at": now,
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
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

func (r *analysisRepository) GetAnalysisNodeByNodeID(ctx context.Context, analysisID string, nodeID string) (*types.AnalysisNode, error) {
	item := &types.AnalysisNode{}
	if err := r.db.WithContext(ctx).Where("analysis_id = ? AND node_id = ?", analysisID, nodeID).Take(item).Error; err != nil {
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

func (r *analysisRepository) ListAnalysisEdgesByAnalysisID(ctx context.Context, analysisID string) ([]*types.AnalysisEdge, error) {
	items := make([]*types.AnalysisEdge, 0)
	err := r.db.WithContext(ctx).Where("analysis_id = ?", analysisID).Find(&items).Error
	if err != nil {
		return nil, err
	}
	return items, nil
}

func (r *analysisRepository) UpdateAnalysisNodeByAnalysisNodeID(ctx context.Context, analysisNodeID string, values map[string]any) error {
	if len(values) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Model(&types.AnalysisNode{}).Where("analysis_node_id = ?", analysisNodeID).Updates(values).Error
}

func (r *analysisRepository) ClaimNextReadyNode(ctx context.Context, analysisID string, fromStatus string, toStatus string) (*types.AnalysisNode, error) {
	var claimed *types.AnalysisNode
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		node := &types.AnalysisNode{}
		if err := tx.
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("analysis_id = ? AND status = ?", analysisID, fromStatus).
			Order("id ASC").
			Take(node).Error; err != nil {
			return err
		}

		result := tx.Model(&types.AnalysisNode{}).
			Where("id = ? AND status = ?", node.ID, fromStatus).
			Updates(map[string]any{"status": toStatus})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}

		node.Status = toStatus
		claimed = node
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
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
