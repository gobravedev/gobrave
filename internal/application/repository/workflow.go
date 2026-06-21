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

func (r *workflowRepository) GetScriptByScriptID(ctx context.Context, scriptID string) (*types.Script, error) {
	item := &types.Script{}
	if err := r.db.WithContext(ctx).Where("component_id = ?", scriptID).Take(item).Error; err != nil {
		return nil, err
	}
	return item, nil
}

func (r *workflowRepository) FindScriptsByScriptIDs(ctx context.Context, scriptIDs []string) ([]*types.Script, error) {
	items := make([]*types.Script, 0)
	if len(scriptIDs) == 0 {
		return items, nil
	}
	err := r.db.WithContext(ctx).Where("component_id IN ?", scriptIDs).Find(&items).Error
	if err != nil {
		return nil, err
	}
	return items, nil
}

func (r *workflowRepository) GetScriptContainerSnapshotByScriptID(ctx context.Context, scriptID string) (*types.ScriptContainerSnapshot, error) {
	item := &types.ScriptContainerSnapshot{}

	err := r.db.WithContext(ctx).
		Table("pipeline_components AS pc").
		Select(`
			pc.component_id AS script_id,
			pc.container_template_id AS container_template_id,
			ct.name AS container_name,
			ct.image_id AS image_id,
			ci.full_name AS container_image,
			ci.name AS image_name,
			ci.tag AS image_tag,
			ci.status AS image_status
		`).
		Joins("LEFT JOIN go_container_template AS ct ON pc.container_template_id = ct.id").
		Joins("LEFT JOIN go_container_image AS ci ON ct.image_id = ci.id").
		Where("pc.component_id = ?", scriptID).
		Limit(1).
		Scan(item).Error
	if err != nil {
		return nil, err
	}

	if item.ScriptID == "" {
		return nil, gorm.ErrRecordNotFound
	}

	return item, nil
}
