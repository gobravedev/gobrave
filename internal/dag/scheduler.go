package dag

import (
	"context"
	"time"

	"github.com/gobravedev/gobrave/internal/event"
)

type SchedulerConfig struct {
	MaxSteps       int
	MaxConcurrency int
	QueueSize      int
	PollInterval   time.Duration
	Timeout        time.Duration
}

type SchedulerResult struct {
	AnalysisID          string           `json:"analysis_id"`
	SubmittedCount      int              `json:"submitted_count"`
	FailedToSubmitCount int              `json:"failed_to_submit_count"`
	TimedOut            bool             `json:"timed_out"`
	Snapshot            *RuntimeSnapshot `json:"snapshot"`
}

type DagScheduler struct {
	analysisID string
	runtime    *RuntimeEngine
	dispatcher *NodeDispatcher
	bus        event.Bus
	cfg        SchedulerConfig
}

func NewDagScheduler(analysisID string, runtime *RuntimeEngine, dispatcher *NodeDispatcher, bus event.Bus, cfg SchedulerConfig) *DagScheduler {
	if cfg.MaxSteps <= 0 {
		cfg.MaxSteps = 10000
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 1
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 64
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	return &DagScheduler{
		analysisID: analysisID,
		runtime:    runtime,
		dispatcher: dispatcher,
		bus:        bus,
		cfg:        cfg,
	}
}

func (s *DagScheduler) Run(ctx context.Context) (*SchedulerResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if s.cfg.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.cfg.Timeout)
		defer cancel()
	}

	pool := NewWorkerPool(s.analysisID, s.dispatcher, s.cfg.MaxConcurrency, s.cfg.QueueSize)
	pool.Start(ctx)
	defer pool.Stop()

	s.publish(RuntimeEvent{Name: EventDagStarted, AnalysisID: s.analysisID, OccurredAt: time.Now().UTC()})

	submitted := 0
	failedToSubmit := 0
	timedOut := false

	for submitted < s.cfg.MaxSteps {
		if err := s.runtime.RefreshReadyStatus(ctx, s.analysisID); err != nil {
			return nil, err
		}

		for submitted < s.cfg.MaxSteps && pool.QueueLen() < s.cfg.QueueSize {
			node, err := s.runtime.ClaimNextReadyNode(ctx, s.analysisID)
			if err != nil {
				failedToSubmit++
				break
			}
			if node == nil {
				break
			}
			if ok := pool.Enqueue(node.AnalysisNodeID); !ok {
				failedToSubmit++
				break
			}
			submitted++
			s.publish(RuntimeEvent{
				Name:       EventNodeSubmitted,
				AnalysisID: s.analysisID,
				NodeID:     node.NodeID,
				OccurredAt: time.Now().UTC(),
			})
		}

		snapshot, err := s.runtime.GetSnapshot(ctx, s.analysisID)
		if err != nil {
			return nil, err
		}

		if snapshot.IsFinished && pool.QueueLen() == 0 {
			s.publish(RuntimeEvent{Name: EventDagCompleted, AnalysisID: s.analysisID, OccurredAt: time.Now().UTC()})
			return &SchedulerResult{
				AnalysisID:          s.analysisID,
				SubmittedCount:      submitted,
				FailedToSubmitCount: failedToSubmit,
				TimedOut:            false,
				Snapshot:            snapshot,
			}, nil
		}

		select {
		case <-ctx.Done():
			timedOut = true
			finalSnapshot, _ := s.runtime.GetSnapshot(context.Background(), s.analysisID)
			s.publish(RuntimeEvent{Name: EventDagFailed, AnalysisID: s.analysisID, OccurredAt: time.Now().UTC(), Payload: map[string]any{"reason": ctx.Err().Error()}})
			return &SchedulerResult{
				AnalysisID:          s.analysisID,
				SubmittedCount:      submitted,
				FailedToSubmitCount: failedToSubmit,
				TimedOut:            timedOut,
				Snapshot:            finalSnapshot,
			}, nil
		case <-time.After(s.cfg.PollInterval):
		}
	}

	snapshot, _ := s.runtime.GetSnapshot(context.Background(), s.analysisID)
	s.publish(RuntimeEvent{Name: EventDagCompleted, AnalysisID: s.analysisID, OccurredAt: time.Now().UTC(), Payload: map[string]any{"max_steps_reached": true}})
	return &SchedulerResult{
		AnalysisID:          s.analysisID,
		SubmittedCount:      submitted,
		FailedToSubmitCount: failedToSubmit,
		TimedOut:            false,
		Snapshot:            snapshot,
	}, nil
}

func (s *DagScheduler) publish(evt RuntimeEvent) {
	if s.bus == nil {
		return
	}
	s.bus.Publish(evt)
}
