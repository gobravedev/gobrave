package types

import "strings"

// StorePageQuery defines required filters for store pagination.
type StorePageQuery struct {
	StoreType string `json:"store_type"`
	Keywords  string `json:"keywords,omitempty"`
	SortBy    string `json:"sort_by,omitempty"`
	SortOrder string `json:"sort_order,omitempty"`
}

func (q *StorePageQuery) GetStoreType() string {
	if q == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(q.StoreType))
}

func (q *StorePageQuery) GetKeywords() string {
	if q == nil {
		return ""
	}
	return strings.TrimSpace(q.Keywords)
}

func (q *StorePageQuery) GetSortColumn() string {
	if q == nil {
		return "store.id"
	}

	switch strings.ToLower(strings.TrimSpace(q.SortBy)) {
	case "id":
		return "store.id"
	case "store_id":
		return "store.store_id"
	case "name":
		return "store.name"
	case "category":
		return "store.category"
	case "status":
		return "store.status"
	case "version":
		return "store.version"
	case "created_at":
		return "store.created_at"
	case "updated_at":
		return "store.updated_at"
	default:
		return "store.id"
	}
}

func (q *StorePageQuery) GetSortOrder() string {
	if q == nil {
		return "DESC"
	}

	sortOrder := strings.ToUpper(strings.TrimSpace(q.SortOrder))
	if sortOrder == "ASC" {
		return "ASC"
	}
	return "DESC"
}
