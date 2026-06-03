package repository

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/gorm"
)

type projectRepository struct {
	db *gorm.DB
}

func NewProjectRepository(db *gorm.DB) interfaces.ProjectRepository {
	return &projectRepository{db: db}
}

func (r *projectRepository) AddUserProject(ctx context.Context, up *types.UserProject) error {
	return r.db.WithContext(ctx).Create(up).Error
}

func (r *projectRepository) ExistsUserProject(ctx context.Context, userID, projectID string) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&types.UserProject{}).
		Where("user_id = ? AND project_id = ?", userID, projectID).
		Count(&count).Error
	return count > 0, err
}

func (r *projectRepository) ListProjectByUserID(ctx context.Context, userID string) ([]*types.Project, error) {
	projects := make([]*types.Project, 0)
	err := r.db.WithContext(ctx).
		Table("t_project AS p").
		Select("p.*").
		Joins("INNER JOIN user_project up ON up.project_id = p.project_id").
		Where("up.user_id = ?", userID).
		Order("p.id DESC").
		Scan(&projects).Error
	if err != nil {
		return nil, err
	}

	return projects, nil
}
