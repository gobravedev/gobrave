package interfaces

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
)

type WorkflowService interface {
	GetFormJSONByWorkflowID(ctx context.Context, workflowID string) ([]any, error)
	GetWorkflowVisByWorkflowID(ctx context.Context, workflowID string) (map[string]any, error)
	GetWorkflowByWorkflowID(ctx context.Context, workflowID string) (*types.Workflow, error)
	GetScriptByScriptID(ctx context.Context, scriptID string) (*types.Script, error)
	GetScriptMainFileByScriptID(ctx context.Context, scriptID string) (string, string, error)
	GetScriptContainerSnapshotByScriptID(ctx context.Context, scriptID string) (*types.ScriptContainerSnapshot, error)
	CreateScript(ctx context.Context, script *types.Script) error
}

type WorkflowRepository interface {
	GetWorkflowByWorkflowID(ctx context.Context, workflowID string) (*types.Workflow, error)
	GetScriptByScriptID(ctx context.Context, scriptID string) (*types.Script, error)
	FindScriptsByScriptIDs(ctx context.Context, scriptIDs []string) ([]*types.Script, error)
	GetScriptContainerSnapshotByScriptID(ctx context.Context, scriptID string) (*types.ScriptContainerSnapshot, error)
	CreateScript(ctx context.Context, script *types.Script) error
}
