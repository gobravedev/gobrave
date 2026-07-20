package types

import "strings"

// WorkflowPageQuery defines optional filters and sorting for workflow pagination.
type WorkflowPageQuery struct {
	IDs []uint `json:"ids,omitempty"`

	ID *uint `json:"id,omitempty"`

	ProjectID int64 `json:"project_id,string,omitempty"`

	WorkflowID string `json:"workflow_id,omitempty"`

	Name string `json:"name,omitempty"`

	Category string `json:"category,omitempty"`

	InstallKey string `json:"install_key,omitempty"`

	ModuleID string `json:"module_id,omitempty"`

	RelationType string `json:"relation_type,omitempty"`

	Tags string `json:"tags,omitempty"`

	Keywords string `json:"keywords,omitempty"`

	SortBy string `json:"sort_by,omitempty"`

	SortOrder string `json:"sort_order,omitempty"`
}

func (q *WorkflowPageQuery) GetWorkflowID() string {
	if q == nil {
		return ""
	}
	return strings.TrimSpace(q.WorkflowID)
}

func (q *WorkflowPageQuery) GetName() string {
	if q == nil {
		return ""
	}
	return strings.TrimSpace(q.Name)
}

func (q *WorkflowPageQuery) GetCategory() string {
	if q == nil {
		return ""
	}
	return strings.TrimSpace(q.Category)
}

func (q *WorkflowPageQuery) GetInstallKey() string {
	if q == nil {
		return ""
	}
	return strings.TrimSpace(q.InstallKey)
}

func (q *WorkflowPageQuery) GetModuleID() string {
	if q == nil {
		return ""
	}
	return strings.TrimSpace(q.ModuleID)
}

func (q *WorkflowPageQuery) GetRelationType() string {
	if q == nil {
		return ""
	}
	return strings.TrimSpace(q.RelationType)
}

func (q *WorkflowPageQuery) GetTags() string {
	if q == nil {
		return ""
	}
	return strings.TrimSpace(q.Tags)
}

func (q *WorkflowPageQuery) GetKeywords() string {
	if q == nil {
		return ""
	}
	return strings.TrimSpace(q.Keywords)
}

func (q *WorkflowPageQuery) GetSortColumn() string {
	if q == nil {
		return "id"
	}

	switch strings.ToLower(strings.TrimSpace(q.SortBy)) {
	case "id":
		return "id"
	case "workflow_id", "relation_id":
		return "relation_id"
	case "name":
		return "name"
	case "category":
		return "category"
	case "install_key":
		return "install_key"
	case "relation_type":
		return "relation_type"
	case "module_id", "component_id":
		return "component_id"
	case "order_index":
		return "order_index"
	case "created_at":
		return "created_at"
	case "updated_at":
		return "updated_at"
	default:
		return "id"
	}
}

func (q *WorkflowPageQuery) GetSortOrder() string {
	if q == nil {
		return "DESC"
	}

	sortOrder := strings.ToUpper(strings.TrimSpace(q.SortOrder))
	if sortOrder == "ASC" {
		return "ASC"
	}
	return "DESC"
}
