package service

import (
	"context"
	"errors"
	"time"

	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type projectService struct {
	projectRepo interfaces.ProjectRepository
}

func NewProjectService(projectRepo interfaces.ProjectRepository) interfaces.ProjectService {
	return &projectService{projectRepo: projectRepo}
}

func (s *projectService) ListProjectByUserID(ctx context.Context, userID string) ([]*types.Project, error) {
	return s.projectRepo.ListProjectByUserID(ctx, userID)
}

func (s *projectService) AddUserProject(ctx context.Context, userID, projectID string) error {
	exists, err := s.projectRepo.ExistsUserProject(ctx, userID, projectID)
	if err != nil {
		return err
	}
	if exists {
		return errors.New("user already has access to this project")
	}
	return s.projectRepo.AddUserProject(ctx, &types.UserProject{
		UserID:    userID,
		ProjectID: projectID,
		CreatedAt: time.Now(),
	})
}
