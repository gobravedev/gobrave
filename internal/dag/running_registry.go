package dag

import (
	"context"
	"sync"
	"time"
)

type RunningEntry struct {
	AnalysisID     string
	TaskName       string
	StartedAt      time.Time
	UpdatedAt      time.Time
	MaxConcurrency int
	QueueSize      int
	PollIntervalMs int64
	TimeoutSeconds int64
	Status         string
	StopRequested  bool
	Cancel         context.CancelFunc
}

type RunningRegistry struct {
	mu      sync.RWMutex
	running map[string]*RunningEntry
}

func NewRunningRegistry() *RunningRegistry {
	return &RunningRegistry{running: map[string]*RunningEntry{}}
}

func (r *RunningRegistry) Register(entry *RunningEntry) {
	if entry == nil {
		return
	}
	now := time.Now().UTC()
	if entry.StartedAt.IsZero() {
		entry.StartedAt = now
	}
	entry.UpdatedAt = now
	if entry.Status == "" {
		entry.Status = "running"
	}
	r.mu.Lock()
	r.running[entry.AnalysisID] = entry
	r.mu.Unlock()
}

func (r *RunningRegistry) MarkFinished(analysisID string, status string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.running[analysisID]
	if entry == nil {
		return
	}
	entry.Status = status
	entry.UpdatedAt = time.Now().UTC()
	delete(r.running, analysisID)
}

func (r *RunningRegistry) IsRunning(analysisID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.running[analysisID]
	return ok
}

func (r *RunningRegistry) RequestStop(analysisID string) bool {
	r.mu.Lock()
	entry := r.running[analysisID]
	if entry == nil {
		r.mu.Unlock()
		return false
	}
	entry.StopRequested = true
	entry.Status = "stopping"
	entry.UpdatedAt = time.Now().UTC()
	cancel := entry.Cancel
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	return true
}

func (r *RunningRegistry) IsStopping(analysisID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry := r.running[analysisID]
	return entry != nil && entry.StopRequested
}
