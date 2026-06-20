package service

import (
	"context"
	"fmt"
	"time"

	dagruntime "github.com/gobravedev/gobrave/internal/dag"
	"github.com/gobravedev/gobrave/internal/dag/executor"
	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/manager"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type dagOrchestrator struct {
	repo         interfaces.AnalysisRepository
	workflowRepo interfaces.WorkflowRepository
	containerMgr *manager.ContainerManager
	bus          event.Bus
	registry     *dagruntime.RunningRegistry
}

func NewDagOrchestrator(
	repo interfaces.AnalysisRepository,
	workflowRepo interfaces.WorkflowRepository,
	containerMgr *manager.ContainerManager,
	bus event.Bus,
) interfaces.DagOrchestrator {
	return &dagOrchestrator{
		repo:         repo,
		workflowRepo: workflowRepo,
		containerMgr: containerMgr,
		bus:          bus,
		registry:     dagruntime.NewRunningRegistry(),
	}
}

func (o *dagOrchestrator) StartAsync(ctx context.Context, analysisID string) error {
	if analysisID == "" {
		return fmt.Errorf("analysis_id is required")
	}
	if o.registry.IsRunning(analysisID) {
		return nil
	}

	if err := o.repo.UpdateAnalysisByAnalysisID(ctx, analysisID, map[string]any{
		"job_status": "running",
		"updated_at": time.Now().UTC(),
	}); err != nil {
		return err
	}

	runtime := dagruntime.NewRuntimeEngine(o.repo)
	dispatcher := dagruntime.NewNodeDispatcher(runtime, o.repo, o.bus, executor.NewFactory(executor.FactoryDeps{
		WorkflowRepository: o.workflowRepo,
		ContainerManager:   o.containerMgr,
	}))
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
		result, err := scheduler.Run(context.Background())
		if err != nil {
			o.registry.MarkFinished(analysisID, "failed")
			_ = o.repo.UpdateAnalysisByAnalysisID(context.Background(), analysisID, map[string]any{
				"job_status": "failed",
				"updated_at": time.Now().UTC(),
			})
			return
		}

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
