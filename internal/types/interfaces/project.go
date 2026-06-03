package interfaces

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
)

// ProjectService defines project business capabilities.
type ProjectService interface {
	ListProjectByUserID(ctx context.Context, userID string) ([]*types.Project, error)
	AddUserProject(ctx context.Context, userID, projectID string) error
}

// ProjectRepository defines project data access methods.
type ProjectRepository interface {
	ListProjectByUserID(ctx context.Context, userID string) ([]*types.Project, error)
	AddUserProject(ctx context.Context, up *types.UserProject) error
	ExistsUserProject(ctx context.Context, userID, projectID string) (bool, error)
}
