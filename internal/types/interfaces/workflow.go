package interfaces

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
)

type WorkflowService interface {
	GetFormJSONByWorkflowID(ctx context.Context, workflowID string) ([]any, error)
}

type WorkflowRepository interface {
	GetWorkflowByWorkflowID(ctx context.Context, workflowID string) (*types.Workflow, error)
	FindModulesByModuleIDs(ctx context.Context, moduleIDs []string) ([]*types.Module, error)
}
