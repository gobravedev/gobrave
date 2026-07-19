package repository

import (
	"context"
	"fmt"

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

func (r *workflowRepository) PageScript(ctx context.Context, pagination *types.Pagination, query *types.ScriptPageQuery) ([]*types.Script, int64, error) {
	if pagination == nil {
		pagination = &types.Pagination{}
	}

	items := make([]*types.Script, 0)
	var total int64

	buildQuery := func() *gorm.DB {
		return r.db.WithContext(ctx).
			Model(&types.Script{}).
			Where("component_type = ?", "script")
	}

	applyFilters := func(db *gorm.DB) *gorm.DB {
		if query == nil {
			return db
		}

		if query.ID != nil {
			db = db.Where("id = ?", *query.ID)
		}

		if len(query.IDs) > 0 {
			db = db.Where("id IN ?", query.IDs)
		}

		if scriptID := query.GetScriptID(); scriptID != "" {
			db = db.Where("component_id = ?", scriptID)
		}

		if componentName := query.GetComponentName(); componentName != "" {
			db = db.Where("component_name LIKE ?", "%"+componentName+"%")
		}

		if scriptType := query.GetScriptType(); scriptType != "" {
			db = db.Where("script_type = ?", scriptType)
		}

		if category := query.GetCategory(); category != "" {
			db = db.Where("category = ?", category)
		}

		if installKey := query.GetInstallKey(); installKey != "" {
			db = db.Where("install_key = ?", installKey)
		}

		if tags := query.GetTags(); tags != "" {
			db = db.Where("tags LIKE ?", "%"+tags+"%")
		}

		if query.ContainerTemplateID != nil {
			db = db.Where("container_template_id = ?", *query.ContainerTemplateID)
		}

		if keywords := query.GetKeywords(); keywords != "" {
			like := "%" + keywords + "%"
			db = db.Where(
				r.db.WithContext(ctx).Where("component_name LIKE ?", like).
					Or("description LIKE ?", like).
					Or("tags LIKE ?", like).
					Or("component_id LIKE ?", like),
			)
		}

		return db
	}

	if err := applyFilters(buildQuery()).Count(&total).Error; err != nil {
		return nil, 0, err
	}

	sortColumn := "id"
	sortOrder := "DESC"
	if query != nil {
		sortColumn = query.GetSortColumn()
		sortOrder = query.GetSortOrder()
	}

	err := applyFilters(buildQuery()).
		Order(fmt.Sprintf("%s %s", sortColumn, sortOrder)).
		Offset(pagination.Offset()).
		Limit(pagination.Limit()).
		Find(&items).Error
	if err != nil {
		return nil, 0, err
	}

	if len(items) == 0 {
		return []*types.Script{}, total, nil
	}

	return items, total, nil
}

func (r *workflowRepository) GetScriptByID(ctx context.Context, id int64) (*types.Script, error) {
	item := &types.Script{}
	if err := r.db.WithContext(ctx).Where("id = ?", id).Take(item).Error; err != nil {
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

func (r *workflowRepository) CreateScript(ctx context.Context, script *types.Script) error {
	return r.db.WithContext(ctx).Create(script).Error
}

func (r *workflowRepository) UpdateScript(ctx context.Context, script *types.Script) error {
	if script == nil || script.ID == 0 {
		return gorm.ErrRecordNotFound
	}

	updates := map[string]any{
		"component_id":          script.ScriptID,
		"install_key":           script.InstallKey,
		"component_type":        script.ComponentType,
		"component_name":        script.ComponentName,
		"description":           script.Description,
		"component_ids":         script.ComponentIDs,
		"img":                   script.Img,
		"container_template_id": script.ContainerTemplateID,
		"tools_container_id":    script.ToolsContainerID,
		"prompt":                script.Prompt,
		"io_schema":             script.IOSchema,
		"sub_container_id":      script.SubContainerID,
		"tags":                  script.Tags,
		"file_type":             script.FileType,
		"script_type":           script.ScriptType,
		"category":              script.Category,
		"content":               script.Content,
		"order_index":           script.OrderIndex,
		"position":              script.Position,
		"edges":                 script.Edges,
	}

	result := r.db.WithContext(ctx).Model(&types.Script{}).Where("id = ?", script.ID).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}
