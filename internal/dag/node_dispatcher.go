package dag

import (
	"context"
	"fmt"
	"time"

	"github.com/gobravedev/gobrave/internal/dag/executor"
	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type NodeDispatcher struct {
	runtime *RuntimeEngine
	repo    interfaces.AnalysisRepository
	bus     event.Bus
	factory *executor.Factory
}

func NewNodeDispatcher(runtime *RuntimeEngine, repo interfaces.AnalysisRepository, bus event.Bus, factory *executor.Factory) *NodeDispatcher {
	return &NodeDispatcher{runtime: runtime, repo: repo, bus: bus, factory: factory}
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
