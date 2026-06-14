package repository

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/gorm"
)

type workflowRepository struct {
	db *gorm.DB
}

func NewWorkflowRepository(db *gorm.DB) interfaces.WorkflowRepository {
	return &workflowRepository{db: db}
}

func (r *workflowRepository) GetWorkflowByWorkflowID(ctx context.Context, workflowID string) (*types.Workflow, error) {
	item := &types.Workflow{}
	if err := r.db.WithContext(ctx).Where("relation_id = ?", workflowID).Take(item).Error; err != nil {
		return nil, err
	}
	return item, nil
}

func (r *workflowRepository) GetModuleByModuleID(ctx context.Context, moduleID string) (*types.Module, error) {
	item := &types.Module{}
	if err := r.db.WithContext(ctx).Where("component_id = ?", moduleID).Take(item).Error; err != nil {
		return nil, err
	}
	return item, nil
}

func (r *workflowRepository) FindModulesByModuleIDs(ctx context.Context, moduleIDs []string) ([]*types.Module, error) {
	items := make([]*types.Module, 0)
	if len(moduleIDs) == 0 {
		return items, nil
	}
	err := r.db.WithContext(ctx).Where("component_id IN ?", moduleIDs).Find(&items).Error
	if err != nil {
		return nil, err
	}
	return items, nil
}
