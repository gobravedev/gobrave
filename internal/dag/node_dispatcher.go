package dag

import (
	"context"
	"fmt"
	"time"

	"github.com/gobravedev/gobrave/internal/dag/executor"
	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type NodeFailureCleanupFunc func(ctx context.Context, node *types.AnalysisNode)

type NodeDispatcher struct {
	runtime *RuntimeEngine
	repo    interfaces.AnalysisRepository
	bus     event.Bus
	factory *executor.Factory
	cleanup NodeFailureCleanupFunc
}

func NewNodeDispatcher(
	runtime *RuntimeEngine,
	repo interfaces.AnalysisRepository,
	bus event.Bus,
	factory *executor.Factory,
	cleanup NodeFailureCleanupFunc,
) *NodeDispatcher {
	return &NodeDispatcher{runtime: runtime, repo: repo, bus: bus, factory: factory, cleanup: cleanup}
}

func (d *NodeDispatcher) Dispatch(ctx context.Context, analysisID string, analysisNodeID string) error {
	node, err := d.runtime.MarkNodeRunning(ctx, analysisNodeID)
	if err != nil {
		return err
	}
	d.publish(RuntimeEvent{
		Name:       EventNodeRunning,
		AnalysisID: analysisID,
		NodeID:     node.NodeID,
		OccurredAt: time.Now().UTC(),
	})

	ex := d.factory.Resolve(node.Executor)
	result, execErr := ex.Execute(ctx, node)
	if execErr != nil {
		_, _ = d.runtime.CompleteNode(ctx, analysisID, node.NodeID, StatusFailed, nil, 1, execErr.Error())
		d.runCleanup(ctx, node)
		d.publish(RuntimeEvent{
			Name:       EventNodeFailed,
			AnalysisID: analysisID,
			NodeID:     node.NodeID,
			OccurredAt: time.Now().UTC(),
			Payload: map[string]any{
				"error": execErr.Error(),
			},
		})
		return execErr
	}

	if result == nil {
		result = &executor.Result{Status: StatusDone, ExitCode: 0}
	}
	if result.Deferred {
		d.publish(RuntimeEvent{
			Name:       EventNodeStateChange,
			AnalysisID: analysisID,
			NodeID:     node.NodeID,
			OccurredAt: time.Now().UTC(),
			Payload: map[string]any{
				"status":   StatusRunning,
				"deferred": true,
			},
		})
		return nil
	}
	status := result.Status
	if status == "" {
		status = StatusDone
	}
	_, err = d.runtime.CompleteNode(
		ctx,
		analysisID,
		node.NodeID,
		status,
		result.ResolvedOutputs,
		result.ExitCode,
		result.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("complete node failed: %w", err)
	}

	eventName := EventNodeCompleted
	if status == StatusFailed {
		eventName = EventNodeFailed
		d.runCleanup(ctx, node)
	}
	d.publish(RuntimeEvent{
		Name:       eventName,
		AnalysisID: analysisID,
		NodeID:     node.NodeID,
		OccurredAt: time.Now().UTC(),
		Payload: map[string]any{
			"status": status,
		},
	})
	return nil
}

func (d *NodeDispatcher) publish(evt RuntimeEvent) {
	if d.bus == nil {
		return
	}
	d.bus.Publish(evt)
}

func (d *NodeDispatcher) runCleanup(ctx context.Context, node *types.AnalysisNode) {
	if d.cleanup == nil || node == nil {
		return
	}
	d.cleanup(ctx, node)
}
