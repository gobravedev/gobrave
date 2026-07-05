package dag

import (
	"context"
	"sync"
)

type queuedNode struct {
	analysisNodeID int64
}

type WorkerPool struct {
	dispatcher *NodeDispatcher
	jobs       chan queuedNode
	workers    int
	wg         sync.WaitGroup
}

func NewWorkerPool(dispatcher *NodeDispatcher, workers int, queueSize int) *WorkerPool {
	if workers <= 0 {
		workers = 1
	}
	if queueSize <= 0 {
		queueSize = 1
	}
	return &WorkerPool{
		dispatcher: dispatcher,
		jobs:       make(chan queuedNode, queueSize),
		workers:    workers,
	}
}

func (p *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-p.jobs:
					if !ok {
						return
					}
					_ = p.dispatcher.Dispatch(ctx, job.analysisNodeID)
				}
			}
		}()
	}
}

func (p *WorkerPool) Enqueue(nodeID int64) bool {
	select {
	case p.jobs <- queuedNode{analysisNodeID: nodeID}:
		return true
	default:
		return false
	}
}

func (p *WorkerPool) QueueLen() int {
	return len(p.jobs)
}

func (p *WorkerPool) Stop() {
	close(p.jobs)
	p.wg.Wait()
}
