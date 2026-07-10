package manager

import (
	"context"
	"strings"
	"time"

	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

var _ event.Handler = (*AppSessionEventHandler)(nil)

type AppSessionEventHandler struct {
	repo interfaces.ContainerRepository
}

func NewAppSessionEventHandler(repo interfaces.ContainerRepository) *AppSessionEventHandler {
	return &AppSessionEventHandler{repo: repo}
}

func (h *AppSessionEventHandler) Handle(evt event.Event) {
	ce, ok := evt.(types.ContainerEvent)
	if !ok {
		return
	}

	ctx := context.Background()
	inst, err := h.repo.GetContainerInstanceByID(ctx, ce.ContainerInstanceID)
	if err != nil || inst == nil {
		return
	}
	if inst.OwnerType != types.ContainerOwnerAppSession {
		return
	}

	session, err := h.repo.GetAppSessionByID(ctx, inst.OwnerID)
	if err != nil || session == nil {
		return
	}

	now := time.Now()
	switch normalizeContainerEvent(ce.Event) {
	case "running":
		session.Status = "RUNNING"
		if session.StartedAt == nil {
			session.StartedAt = &now
		}
		session.StoppedAt = nil
	case "creating":
		session.Status = "CREATING"
		session.StoppedAt = nil
	case "stopped":
		session.Status = "STOPPED"
		session.StoppedAt = &now
	case "failed":
		session.Status = "FAILED"
		session.StoppedAt = &now
	default:
		return
	}

	if err := h.repo.UpdateAppSession(ctx, session); err != nil {
		logger.Warnf(ctx, "[AppSessionEventHandler] update app session failed, session_id=%d event=%s err=%v", session.ID, ce.Event, err)
	}
}

func normalizeContainerEvent(eventName string) string {
	eventName = strings.TrimSpace(eventName)
	switch eventName {
	case "ContainerCreating":
		return "creating"
	case "ContainerStarted", "ContainerResumed":
		return "running"
	case "ContainerStopped", "ContainerExited", "ContainerDeleted":
		return "stopped"
	}

	if strings.Contains(strings.ToLower(eventName), "failed") {
		return "failed"
	}

	return ""
}
