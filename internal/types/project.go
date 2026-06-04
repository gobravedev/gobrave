package types

import "time"

// Project maps to Python brave's t_project table.
type Project struct {
	ID           uint      `json:"id" gorm:"primaryKey;autoIncrement"`
	ProjectID    string    `json:"project_id" gorm:"type:varchar(255);uniqueIndex;not null"`
	ProjectName  string    `json:"project_name" gorm:"type:varchar(255)"`
	MetadataForm string    `json:"metadata_form" gorm:"type:text"`
	Research     string    `json:"research" gorm:"type:text"`
	Parameter    string    `json:"parameter" gorm:"type:text"`
	Description  string    `json:"description" gorm:"type:text"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (Project) TableName() string {
	return "t_project"
}

// UserProject is a manual many-to-many mapping table between users and projects.
// We intentionally do not use GORM association tags/relations.
type UserProject struct {
	ID        uint      `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID    string    `json:"user_id" gorm:"type:varchar(36);not null;index:idx_user_project_user;uniqueIndex:idx_user_project_unique,priority:1"`
	ProjectID string    `json:"project_id" gorm:"type:varchar(255);not null;index:idx_user_project_project;uniqueIndex:idx_user_project_unique,priority:2"`
	IsActive  bool      `json:"is_active" gorm:"default:false"`
	CreatedAt time.Time `json:"created_at"`
}

func (UserProject) TableName() string {
	return "user_project"
}
