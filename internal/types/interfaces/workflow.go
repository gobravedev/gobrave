package interfaces

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
)

type WorkflowService interface {
	GetFormJSONByWorkflowID(ctx context.Context, workflowID string) ([]any, error)
	GetWorkflowByWorkflowID(ctx context.Context, workflowID string) (*types.Workflow, error)
	GetModuleByModuleID(ctx context.Context, moduleID string) (*types.Module, error)
}

type WorkflowRepository interface {
	GetWorkflowByWorkflowID(ctx context.Context, workflowID string) (*types.Workflow, error)
	GetModuleByModuleID(ctx context.Context, moduleID string) (*types.Module, error)
	FindModulesByModuleIDs(ctx context.Context, moduleIDs []string) ([]*types.Module, error)
}
