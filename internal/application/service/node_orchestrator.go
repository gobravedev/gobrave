package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	dagruntime "github.com/gobravedev/gobrave/internal/dag"
	"github.com/gobravedev/gobrave/internal/dag/executor"
	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/manager"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type nodeOrchestrator struct {
	repo         interfaces.AnalysisRepository
	workflowRepo interfaces.WorkflowRepository
	containerMgr *manager.ContainerManager
	bus          event.Bus
}

func NewNodeOrchestrator(
	repo interfaces.AnalysisRepository,
	workflowRepo interfaces.WorkflowRepository,
	containerMgr *manager.ContainerManager,
	bus event.Bus,
) interfaces.NodeOrchestrator {
	return &nodeOrchestrator{
		repo:         repo,
		workflowRepo: workflowRepo,
		containerMgr: containerMgr,
		bus:          bus,
	}
}

// StartAsync submits one existing analysis_node to NodeDispatcher directly.
// This is a bootstrap path that intentionally does not require analysis DAG scheduler wiring.
func (o *nodeOrchestrator) StartAsync(ctx context.Context, analysisNodeID string) error {
	analysisNodeID = strings.TrimSpace(analysisNodeID)
	if analysisNodeID == "" {
		return fmt.Errorf("analysis_node_id is required")
	}

	node, err := o.repo.GetAnalysisNodeByAnalysisNodeID(ctx, analysisNodeID)
	if err != nil {
		return err
	}
	if node == nil || node.ID == 0 {
		return fmt.Errorf("analysis node not found")
	}

	status := strings.ToLower(strings.TrimSpace(node.Status))
	shouldPublishSubmitted := false
	switch status {
	case dagruntime.StatusRunning, dagruntime.StatusDone, dagruntime.StatusFailed, dagruntime.StatusSkipped, dagruntime.StatusCached:
		return nil
	case dagruntime.StatusSubmitted:
		// keep status as submitted
		shouldPublishSubmitted = true
	case dagruntime.StatusReady:
		if err := o.repo.UpdateAnalysisNodeByAnalysisNodeID(ctx, node.AnalysisNodeID, map[string]any{
			"status":     dagruntime.StatusSubmitted,
			"updated_at": time.Now().UTC(),
		}); err != nil {
			return err
		}
		shouldPublishSubmitted = true
	default:
		return fmt.Errorf("analysis node status must be ready/submitted, current=%s", node.Status)
	}

	if shouldPublishSubmitted && o.bus != nil {
		o.bus.Publish(dagruntime.RuntimeEvent{
			Name:           dagruntime.EventNodeSubmitted,
			AnalysisID:     node.AnalysisID,
			AnalysisNodeID: node.ID,
			NodeID:         node.NodeID,
			OccurredAt:     time.Now().UTC(),
			Payload: map[string]any{
				"status":           dagruntime.StatusSubmitted,
				"analysis_node_id": node.AnalysisNodeID,
			},
		})
	}

	runtime := dagruntime.NewRuntimeEngine(o.repo)
	preparer := dagruntime.NoopNodeRuntimePreparer{}
	dispatcher := dagruntime.NewNodeDispatcher(
		runtime,
		o.repo,
		o.bus,
		executor.NewFactory(executor.FactoryDeps{
			WorkflowRepository: o.workflowRepo,
			ContainerManager:   o.containerMgr,
		}),
		nil,
		preparer,
	)

	go func(nodeID int64, relationID string, runtimeNodeID string) {
		if dispatchErr := dispatcher.Dispatch(context.Background(), nodeID); dispatchErr != nil {
			logger.Warnf(context.Background(), "[NodeOrchestrator] dispatch failed, analysis_node_id=%s node_id=%s analysis_id=%s err=%v", analysisNodeID, runtimeNodeID, relationID, dispatchErr)
		}
	}(node.ID, node.AnalysisID, node.NodeID)

	return nil
}

// StopAsync requests stop for one analysis node.
// It first marks node status as stopping, then delegates executor/container stop.
// Final status transition to stopped is completed by NodeCompletionCoordinator.
func (o *nodeOrchestrator) StopAsync(ctx context.Context, analysisNodeID string) error {
	analysisNodeID = strings.TrimSpace(analysisNodeID)
	if analysisNodeID == "" {
		return fmt.Errorf("analysis_node_id is required")
	}

	node, err := o.repo.GetAnalysisNodeByAnalysisNodeID(ctx, analysisNodeID)
	if err != nil {
		return err
	}
	if node == nil || node.ID == 0 {
		return fmt.Errorf("analysis node not found")
	}

	status := strings.ToLower(strings.TrimSpace(node.Status))
	if status == dagruntime.StatusStopping {
		return nil
	}
	if status == dagruntime.StatusStopped || status == dagruntime.StatusDone || status == dagruntime.StatusFailed || status == dagruntime.StatusSkipped || status == dagruntime.StatusCached {
		return nil
	}
	if status != dagruntime.StatusReady && status != dagruntime.StatusSubmitted && status != dagruntime.StatusRunning {
		return fmt.Errorf("analysis node status must be ready/submitted/running, current=%s", node.Status)
	}

	values := map[string]any{
		"status":        dagruntime.StatusStopping,
		"server_status": "stopping",
		"error_message": "node stop requested by user",
		"updated_at":    time.Now().UTC(),
	}
	if err := o.repo.UpdateAnalysisNodeByAnalysisNodeID(ctx, node.AnalysisNodeID, values); err != nil {
		return err
	}
	// if o.bus != nil {
	// 	o.bus.Publish(dagruntime.RuntimeEvent{
	// 		Name:           dagruntime.EventNodeStateChange,
	// 		AnalysisID:     node.AnalysisID,
	// 		AnalysisNodeID: node.ID,
	// 		NodeID:         node.NodeID,
	// 		OccurredAt:     time.Now().UTC(),
	// 		Payload: map[string]any{
	// 			"status":           dagruntime.StatusStopping,
	// 			"analysis_node_id": node.AnalysisNodeID,
	// 		},
	// 	})
	// }

	runtime := dagruntime.NewRuntimeEngine(o.repo)
	preparer := dagruntime.NoopNodeRuntimePreparer{}
	dispatcher := dagruntime.NewNodeDispatcher(
		runtime,
		o.repo,
		o.bus,
		executor.NewFactory(executor.FactoryDeps{
			WorkflowRepository: o.workflowRepo,
			ContainerManager:   o.containerMgr,
		}),
		nil,
		preparer,
	)

	if _, execErr := dispatcher.Stop(ctx, node); execErr != nil {
		_ = o.repo.UpdateAnalysisNodeByAnalysisNodeID(context.Background(), node.AnalysisNodeID, map[string]any{
			"status":        dagruntime.StatusFailed,
			"server_status": "stopped",
			"error_message": fmt.Sprintf("node stop failed: %v", execErr),
			"finished_at":   time.Now().UTC(),
			"updated_at":    time.Now().UTC(),
		})
		return execErr
	}

	return nil
}
