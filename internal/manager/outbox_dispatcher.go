package manager

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type OutboxDispatcher struct {
	repo         interfaces.ContainerRepository
	bus          event.Bus
	batchSize    int
	pollInterval time.Duration
}

func NewOutboxDispatcher(repo interfaces.ContainerRepository, bus event.Bus) *OutboxDispatcher {
	return &OutboxDispatcher{
		repo:         repo,
		bus:          bus,
		batchSize:    100,
		pollInterval: time.Second,
	}
}

func RunOutboxDispatcher(dispatcher *OutboxDispatcher) {
	go dispatcher.Start(context.Background())
}

func (d *OutboxDispatcher) Start(ctx context.Context) {
	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.dispatchOnce(ctx)
		}
	}
}

func (d *OutboxDispatcher) dispatchOnce(ctx context.Context) {
	items, err := d.repo.ListPendingOutboxEvent(ctx, d.batchSize)
	if err != nil {
		logger.Errorf(ctx, "[OutboxDispatcher] list pending outbox failed: %v", err)
		return
	}

	for _, item := range items {
		evt := &types.ContainerEvent{}
		if err := json.Unmarshal(item.Payload, evt); err != nil {
			logger.Errorf(ctx, "[OutboxDispatcher] unmarshal outbox payload failed, id=%d err=%v", item.ID, err)
			continue
		}

		d.bus.Publish(*evt)

		if err := d.repo.MarkOutboxEventSent(ctx, item.ID); err != nil {
			logger.Errorf(ctx, "[OutboxDispatcher] mark outbox sent failed, id=%d err=%v", item.ID, err)
		}
	}
}
