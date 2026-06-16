package route

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/datatypes"
)

var _ event.Handler = (*RouteRegistryHandler)(nil)

type RouteRegistryHandler struct {
	repo     interfaces.ContainerRepository
	registry RouteRegistry
}

func NewRouteRegistryHandler(repo interfaces.ContainerRepository, registry RouteRegistry) *RouteRegistryHandler {
	return &RouteRegistryHandler{repo: repo, registry: registry}
}

func (h *RouteRegistryHandler) Handle(evt event.Event) {
	ce, ok := evt.(types.ContainerEvent)
	if !ok {
		return
	}

	ctx := context.Background()
	routeKey, inst, appSession, tpl, err := h.loadContext(ctx, ce.ContainerInstanceID)
	if err != nil {
		logger.Warnf(ctx, "[RouteRegistryHandler] skip event=%s instance_id=%d: %v", ce.Event, ce.ContainerInstanceID, err)
		return
	}

	switch ce.Event {
	case "ContainerStarted", "ContainerResumed":
		port := resolveBackendPort(tpl)
		if port == 0 {
			logger.Warnf(ctx, "[RouteRegistryHandler] skip register route key=%s: unresolved backend port", routeKey)
			return
		}
		if strings.TrimSpace(inst.IPAddress) == "" {
			logger.Warnf(ctx, "[RouteRegistryHandler] skip register route key=%s: empty container ip", routeKey)
			return
		}

		reg := Registration{
			RouteKey:   routeKey,
			PathPrefix: fmt.Sprintf("/apps/%d", appSession.ID),
			Backend: Backend{
				Host: strings.TrimSpace(inst.IPAddress),
				Port: port,
			},
			Metadata: map[string]string{
				"owner_type":            string(inst.OwnerType),
				"container_instance_id": strconv.FormatInt(inst.ID, 10),
				"app_session_id":        strconv.FormatInt(appSession.ID, 10),
				"container_template_id": strconv.FormatInt(tpl.ID, 10),
			},
		}
		if err := h.registry.UpsertRoute(ctx, reg); err != nil {
			logger.Errorf(ctx, "[RouteRegistryHandler] upsert route failed key=%s err=%v", reg.RouteKey, err)
			return
		}
		logger.Infof(ctx, "[RouteRegistryHandler] route upserted key=%s event=%s", reg.RouteKey, ce.Event)

	case "ContainerStopped", "ContainerDeleted", "ContainerFailed":
		if err := h.registry.DeleteRoute(ctx, routeKey); err != nil {
			logger.Errorf(ctx, "[RouteRegistryHandler] delete route failed key=%s err=%v", routeKey, err)
			return
		}
		logger.Infof(ctx, "[RouteRegistryHandler] route deleted key=%s event=%s", routeKey, ce.Event)
	}
}

func (h *RouteRegistryHandler) loadContext(ctx context.Context, containerInstanceID int64) (string, *types.ContainerInstance, *types.AppSession, *types.ContainerTemplate, error) {
	inst, err := h.repo.GetContainerInstanceByID(ctx, containerInstanceID)
	if err != nil {
		return "", nil, nil, nil, err
	}
	if inst.OwnerType != types.ContainerOwnerAppSession {
		return "", nil, nil, nil, fmt.Errorf("owner type is not app_session: %s", inst.OwnerType)
	}

	appSession, err := h.repo.GetAppSessionByID(ctx, inst.OwnerID)
	if err != nil {
		return "", nil, nil, nil, err
	}

	tpl, err := h.repo.GetContainerTemplateByID(ctx, inst.TemplateID)
	if err != nil {
		return "", nil, nil, nil, err
	}

	routeKey := fmt.Sprintf("app-session-%d", appSession.ID)
	return routeKey, inst, appSession, tpl, nil
}

func resolveBackendPort(tpl *types.ContainerTemplate) int {
	if tpl == nil {
		return 0
	}

	keys := []string{"route_port", "app_port", "target_port", "container_port", "service_port", "port", "PORT", "APP_PORT"}
	if p := pickPortFromJSON(tpl.Labels, keys...); p > 0 {
		return p
	}
	if p := pickPortFromJSON(tpl.Env, keys...); p > 0 {
		return p
	}
	return 8787
}

func pickPortFromJSON(raw datatypes.JSON, keys ...string) int {
	if len(raw) == 0 {
		return 0
	}

	obj := map[string]string{}
	if err := json.Unmarshal(raw, &obj); err == nil {
		for _, key := range keys {
			if p := parsePort(obj[key]); p > 0 {
				return p
			}
		}
	}

	list := []map[string]string{}
	if err := json.Unmarshal(raw, &list); err == nil {
		for _, item := range list {
			for _, key := range keys {
				if p := parsePort(item[key]); p > 0 {
					return p
				}
			}
		}
	}

	return 0
}

func parsePort(v string) int {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	p, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	if p <= 0 || p > 65535 {
		return 0
	}
	return p
}
