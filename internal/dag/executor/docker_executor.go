package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/gobravedev/gobrave/internal/manager"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type DockerExecutor struct {
	fallback     Executor
	workflowRepo interfaces.WorkflowRepository
	containerMgr *manager.ContainerManager
}

func NewDockerExecutor(
	fallback Executor,
	workflowRepo interfaces.WorkflowRepository,
	containerMgr *manager.ContainerManager,
) *DockerExecutor {
	return &DockerExecutor{
		fallback:     fallback,
		workflowRepo: workflowRepo,
		containerMgr: containerMgr,
	}
}

func (e *DockerExecutor) Execute(ctx context.Context, node *types.AnalysisNode) (*Result, error) {
	if e.containerMgr == nil || e.workflowRepo == nil {
		return e.fallback.Execute(ctx, node)
	}

	scriptID := strings.TrimSpace(node.ScriptID)
	if scriptID == "" {
		return nil, fmt.Errorf("docker executor requires script_id")
	}

	scriptItem, err := e.workflowRepo.GetScriptByScriptID(ctx, scriptID)
	if err != nil {
		return nil, fmt.Errorf("load script failed: %w", err)
	}
	if scriptItem.ContainerTemplateID == 0 {
		return nil, fmt.Errorf("script container_template_id is required: script_id=%s", scriptID)
	}

	if node.ID == 0 {
		return nil, fmt.Errorf("docker executor requires persisted analysis node id")
	}

	instanceName := fmt.Sprintf("dag-node-%s-%s", node.AnalysisID, node.NodeID)
	inst, err := e.containerMgr.CreateByTemplate(
		ctx,
		"docker",
		scriptItem.ContainerTemplateID,
		types.ContainerOwnerDagNode,
		int64(node.ID),
		instanceName,
	)
	if err != nil {
		return nil, fmt.Errorf("create container instance by dag node failed: %w", err)
	}

	resolvedOutputs := map[string]any{
		"container_instance_id": inst.ID,
		"container_runtime_id":  inst.RuntimeID,
		"container_ip":          inst.IPAddress,
		"container_status":      string(inst.Status),
		"container_owner_type":  string(inst.OwnerType),
		"container_owner_id":    inst.OwnerID,
	}

	return &Result{
		Status:          "running",
		ResolvedOutputs: resolvedOutputs,
		ExitCode:        0,
		Deferred:        true,
	}, nil
}
