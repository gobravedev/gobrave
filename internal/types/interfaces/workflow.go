package interfaces

import (
	"context"

	"github.com/gobravedev/gobrave/internal/types"
)

type WorkflowService interface {
	GetFormJSONByWorkflowID(ctx context.Context, workflowID string) ([]any, error)
	GetScriptFormJSONByID(ctx context.Context, scriptID int64) ([]any, error)
	// 后续废除
	GetFormJSONByScriptID(ctx context.Context, scriptID string) ([]any, error)
	GetWorkflowVisByWorkflowID(ctx context.Context, workflowID string) (map[string]any, error)
	GetWorkflowByWorkflowID(ctx context.Context, workflowID string) (*types.Workflow, error)
	PageScript(ctx context.Context, pagination *types.Pagination, query *types.ScriptPageQuery) ([]*types.Script, int64, error)
	GetScriptByID(ctx context.Context, id int64) (*types.Script, error)
	GetScriptByScriptID(ctx context.Context, scriptID string) (*types.Script, error)
	// 后续废除
	GetScriptMainFileByScriptID(ctx context.Context, scriptID string) (string, string, error)
	GetScriptFileByScriptID(ctx context.Context, scriptID int64) (string, string, error)
	GetScriptContainerSnapshotByScriptID(ctx context.Context, scriptID int64) (*types.ScriptContainerSnapshot, error)
	CreateScript(ctx context.Context, script *types.Script) error
	UpdateScript(ctx context.Context, script *types.Script) error
}

type WorkflowRepository interface {
	GetWorkflowByWorkflowID(ctx context.Context, workflowID string) (*types.Workflow, error)
	PageScript(ctx context.Context, pagination *types.Pagination, query *types.ScriptPageQuery) ([]*types.Script, int64, error)
	GetScriptByID(ctx context.Context, id int64) (*types.Script, error)
	GetScriptByScriptID(ctx context.Context, scriptID string) (*types.Script, error)
	FindScriptsByScriptIDs(ctx context.Context, scriptIDs []string) ([]*types.Script, error)
	GetScriptContainerSnapshotByScriptID(ctx context.Context, scriptID int64) (*types.ScriptContainerSnapshot, error)
	CreateScript(ctx context.Context, script *types.Script) error
	UpdateScript(ctx context.Context, script *types.Script) error
}
