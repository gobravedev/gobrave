package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gobravedev/gobrave/internal/config"
	dagruntime "github.com/gobravedev/gobrave/internal/dag"
	"github.com/gobravedev/gobrave/internal/dag/executor"
	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/manager"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

const (
	cleanupPolicyNone    = "none"
	cleanupPolicyStop    = "stop"
	cleanupPolicyDelete  = "delete"
	analysisRunLeaseTTL  = 90 * time.Second
	analysisRunHeartbeat = 15 * time.Second
)

type dagOrchestrator struct {
	repo           interfaces.AnalysisRepository
	workflowRepo   interfaces.WorkflowRepository
	containerRepo  interfaces.ContainerRepository
	containerMgr   *manager.ContainerManager
	cfg            *config.Config
	bus            event.Bus
	registry       *dagruntime.RunningRegistry
	completion     *dagruntime.NodeCompletionCoordinator
	completionOnce sync.Once
}

func NewDagOrchestrator(
	repo interfaces.AnalysisRepository,
	workflowRepo interfaces.WorkflowRepository,
	containerRepo interfaces.ContainerRepository,
	containerMgr *manager.ContainerManager,
	cfg *config.Config,
	bus event.Bus,
) interfaces.DagOrchestrator {
	o := &dagOrchestrator{
		repo:          repo,
		workflowRepo:  workflowRepo,
		containerRepo: containerRepo,
		containerMgr:  containerMgr,
		cfg:           cfg,
		bus:           bus,
		registry:      dagruntime.NewRunningRegistry(),
	}
	o.completion = dagruntime.NewNodeCompletionCoordinator(
		o.repo,
		o.containerRepo,
		nil,
		o.bus,
		func(cleanupCtx context.Context, node *types.AnalysisNode) {
			o.cleanupDagNodeContainer(cleanupCtx, node, o.cleanupPolicyOnNodeFailed())
		},
		2*time.Second,
	)
	return o
}

func (o *dagOrchestrator) StartAsync(ctx context.Context, analysisID string) error {
	if analysisID == "" {
		return fmt.Errorf("analysis_id is required")
	}
	if o.registry.IsRunning(analysisID) {
		return nil
	}
	now := time.Now().UTC()
	locked, err := o.repo.TryMarkAnalysisRunning(ctx, analysisID, now, now.Add(-analysisRunLeaseTTL))
	if err != nil {
		return err
	}
	if !locked {
		return nil
	}

	if err := o.prepareNodesForResume(ctx, analysisID); err != nil {
		_ = o.repo.UpdateAnalysisByAnalysisID(context.Background(), analysisID, map[string]any{
			"job_status": "failed",
			"updated_at": time.Now().UTC(),
		})
		return fmt.Errorf("prepare nodes for resume failed: %w", err)
	}

	heartbeatStop := make(chan struct{})
	go o.renewAnalysisRunningLease(analysisID, heartbeatStop)

	o.completionOnce.Do(func() {
		if o.completion == nil {
			return
		}
		if o.bus != nil {
			o.bus.Subscribe(o.completion)
		}
		go o.completion.Start(context.Background())
	})

	runtime := dagruntime.NewRuntimeEngine(o.repo)
	onNodeFailedCleanupPolicy := o.cleanupPolicyOnNodeFailed()
	onDagFinishedCleanupPolicy := o.cleanupPolicyOnDagFinished()
	dispatcher := dagruntime.NewNodeDispatcher(runtime, o.repo, o.bus, executor.NewFactory(executor.FactoryDeps{
		WorkflowRepository: o.workflowRepo,
		ContainerManager:   o.containerMgr,
	}), func(cleanupCtx context.Context, node *types.AnalysisNode) {
		o.cleanupDagNodeContainer(cleanupCtx, node, onNodeFailedCleanupPolicy)
	})
	scheduler := dagruntime.NewDagScheduler(
		analysisID,
		runtime,
		dispatcher,
		o.bus,
		dagruntime.SchedulerConfig{
			MaxSteps:       10000,
			MaxConcurrency: 1,
			QueueSize:      64,
			PollInterval:   500 * time.Millisecond,
		},
	)

	o.registry.Register(&dagruntime.RunningEntry{
		AnalysisID:     analysisID,
		TaskName:       "dag-run-" + analysisID,
		MaxConcurrency: 1,
		QueueSize:      64,
		PollIntervalMs: 500,
		Status:         "running",
	})

	go func() {
		defer close(heartbeatStop)
		result, err := scheduler.Run(context.Background())
		if err != nil {
			o.cleanupByAnalysisID(context.Background(), analysisID, onDagFinishedCleanupPolicy)
			o.registry.MarkFinished(analysisID, "failed")
			_ = o.repo.UpdateAnalysisByAnalysisID(context.Background(), analysisID, map[string]any{
				"job_status": "failed",
				"updated_at": time.Now().UTC(),
			})
			return
		}

		o.cleanupByAnalysisID(context.Background(), analysisID, onDagFinishedCleanupPolicy)

		finalStatus := "finished"
		if result == nil || result.Snapshot == nil {
			finalStatus = "failed"
		} else if failedCount := result.Snapshot.StatusCount[dagruntime.StatusFailed]; failedCount > 0 {
			finalStatus = "failed"
		}

		o.registry.MarkFinished(analysisID, finalStatus)
		_ = o.repo.UpdateAnalysisByAnalysisID(context.Background(), analysisID, map[string]any{
			"job_status": finalStatus,
			"updated_at": time.Now().UTC(),
		})
	}()

	return nil
}

func (o *dagOrchestrator) RecoverRunningAnalyses(ctx context.Context) (int, error) {
	if o == nil || o.repo == nil {
		return 0, nil
	}

	items, err := o.repo.ListAnalysisByJobStatus(ctx, "running")
	if err != nil {
		return 0, err
	}

	recovered := 0
	for _, item := range items {
		if item == nil || strings.TrimSpace(item.AnalysisID) == "" {
			continue
		}
		wasRunning := o.registry != nil && o.registry.IsRunning(item.AnalysisID)
		if err := o.StartAsync(ctx, item.AnalysisID); err != nil {
			logger.Warnf(ctx, "[DagOrchestrator] recover running analysis failed, analysis_id=%s err=%v", item.AnalysisID, err)
			continue
		}
		isRunning := o.registry != nil && o.registry.IsRunning(item.AnalysisID)
		if !wasRunning && isRunning {
			recovered++
		}
	}

	return recovered, nil
}

func (o *dagOrchestrator) prepareNodesForResume(ctx context.Context, analysisID string) error {
	if o == nil || o.repo == nil || strings.TrimSpace(analysisID) == "" {
		return nil
	}

	nodes, err := o.repo.ListAnalysisNodesByAnalysisID(ctx, analysisID)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}

	instancesByOwner := map[int64]*types.ContainerInstance{}
	if o.containerRepo != nil {
		instances, listErr := o.containerRepo.ListContainerInstance(ctx)
		if listErr != nil {
			return listErr
		}
		for _, inst := range instances {
			if inst == nil || inst.OwnerType != types.ContainerOwnerDagNode || inst.OwnerID <= 0 {
				continue
			}
			existing := instancesByOwner[inst.OwnerID]
			if existing == nil || inst.ID > existing.ID {
				instancesByOwner[inst.OwnerID] = inst
			}
		}
	}

	for _, node := range nodes {
		if node == nil || strings.TrimSpace(node.AnalysisNodeID) == "" {
			continue
		}

		status := strings.TrimSpace(strings.ToLower(node.Status))
		hasContainer := instancesByOwner[int64(node.ID)] != nil
		targetStatus, shouldReset := resumeNodeStatusForRestart(status, hasContainer)
		if !shouldReset {
			continue
		}
		if err := o.repo.UpdateAnalysisNodeByAnalysisNodeID(ctx, node.AnalysisNodeID, map[string]any{
			"status":        targetStatus,
			"started_at":    nil,
			"finished_at":   nil,
			"error_message": nil,
			"exit_code":     0,
		}); err != nil {
			return err
		}
	}

	return nil
}

func resumeNodeStatusForRestart(status string, hasContainer bool) (string, bool) {
	status = strings.TrimSpace(strings.ToLower(status))
	switch status {
	case dagruntime.StatusSubmitted:
		return dagruntime.StatusReady, true
	case dagruntime.StatusRunning:
		if hasContainer {
			return "", false
		}
		return dagruntime.StatusReady, true
	default:
		return "", false
	}
}

func (o *dagOrchestrator) renewAnalysisRunningLease(analysisID string, stop <-chan struct{}) {
	if analysisID == "" || o.repo == nil {
		return
	}
	ticker := time.NewTicker(analysisRunHeartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			err := o.repo.UpdateAnalysisByAnalysisID(context.Background(), analysisID, map[string]any{
				"updated_at": time.Now().UTC(),
			})
			if err != nil {
				logger.Warnf(context.Background(), "[DagOrchestrator] renew analysis running lease failed, analysis_id=%s err=%v", analysisID, err)
			}
		}
	}
}

func (o *dagOrchestrator) cleanupPolicyOnNodeFailed() string {
	if o.cfg != nil && o.cfg.Container != nil {
		return normalizeCleanupPolicy(o.cfg.Container.DagNodeCleanupOnFailed, cleanupPolicyStop)
	}
	return cleanupPolicyStop
}

func (o *dagOrchestrator) cleanupPolicyOnDagFinished() string {
	if o.cfg != nil && o.cfg.Container != nil {
		return normalizeCleanupPolicy(o.cfg.Container.DagNodeCleanupOnDagFinished, cleanupPolicyDelete)
	}
	return cleanupPolicyDelete
}

func normalizeCleanupPolicy(value string, defaultValue string) string {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case cleanupPolicyNone, cleanupPolicyStop, cleanupPolicyDelete:
		return strings.TrimSpace(strings.ToLower(value))
	default:
		return defaultValue
	}
}

func (o *dagOrchestrator) cleanupByAnalysisID(ctx context.Context, analysisID string, policy string) {
	if policy == cleanupPolicyNone || o.containerRepo == nil || o.containerMgr == nil || analysisID == "" {
		return
	}

	nodes, err := o.repo.ListAnalysisNodesByAnalysisID(ctx, analysisID)
	if err != nil {
		logger.Warnf(ctx, "[DagOrchestrator] list analysis nodes for cleanup failed, analysis_id=%s err=%v", analysisID, err)
		return
	}
	if len(nodes) == 0 {
		return
	}

	ownerSet := make(map[int64]struct{}, len(nodes))
	for _, node := range nodes {
		if node != nil && node.ID > 0 {
			ownerSet[int64(node.ID)] = struct{}{}
		}
	}

	instances, err := o.containerRepo.ListContainerInstance(ctx)
	if err != nil {
		logger.Warnf(ctx, "[DagOrchestrator] list container instances for cleanup failed, analysis_id=%s err=%v", analysisID, err)
		return
	}

	for _, inst := range instances {
		if inst == nil || inst.OwnerType != types.ContainerOwnerDagNode {
			continue
		}
		if _, ok := ownerSet[inst.OwnerID]; !ok {
			continue
		}
		o.cleanupContainerInstance(ctx, inst, policy)
	}
}

func (o *dagOrchestrator) cleanupDagNodeContainer(ctx context.Context, node *types.AnalysisNode, policy string) {
	if policy == cleanupPolicyNone || o.containerRepo == nil || o.containerMgr == nil || node == nil || node.ID == 0 {
		return
	}

	instances, err := o.containerRepo.ListContainerInstance(ctx)
	if err != nil {
		logger.Warnf(ctx, "[DagOrchestrator] list container instances for node cleanup failed, node_id=%s err=%v", node.NodeID, err)
		return
	}

	for _, inst := range instances {
		if inst == nil {
			continue
		}
		if inst.OwnerType == types.ContainerOwnerDagNode && inst.OwnerID == int64(node.ID) {
			o.cleanupContainerInstance(ctx, inst, policy)
		}
	}
}

func (o *dagOrchestrator) cleanupContainerInstance(ctx context.Context, inst *types.ContainerInstance, policy string) {
	if inst == nil || o.containerMgr == nil {
		return
	}

	var err error
	switch policy {
	case cleanupPolicyDelete:
		err = o.containerMgr.Delete(ctx, inst.ID)
	case cleanupPolicyStop:
		err = o.containerMgr.Stop(ctx, inst.ID)
	default:
		return
	}

	if err != nil {
		logger.Warnf(ctx, "[DagOrchestrator] dag node container cleanup failed, policy=%s instance_id=%d runtime_id=%s err=%v", policy, inst.ID, inst.RuntimeID, err)
	}
}
