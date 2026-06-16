package route

import (
	"context"
	"fmt"
	"strings"

	"github.com/gobravedev/gobrave/internal/logger"
)

// GatewayRegistry is a lightweight placeholder implementation.
// It keeps integration points stable before concrete gateway/traefik adapters land.
type GatewayRegistry struct{}

func NewGatewayRegistry() *GatewayRegistry {
	return &GatewayRegistry{}
}

func (r *GatewayRegistry) UpsertRoute(ctx context.Context, route Registration) error {
	if strings.TrimSpace(route.RouteKey) == "" {
		return fmt.Errorf("route key is required")
	}
	if strings.TrimSpace(route.PathPrefix) == "" {
		return fmt.Errorf("path prefix is required")
	}
	if strings.TrimSpace(route.Backend.Host) == "" {
		return fmt.Errorf("backend host is required")
	}
	if route.Backend.Port <= 0 || route.Backend.Port > 65535 {
		return fmt.Errorf("invalid backend port: %d", route.Backend.Port)
	}

	logger.Infof(ctx, "[GatewayRegistry] upsert route key=%s prefix=%s backend=%s:%d", route.RouteKey, route.PathPrefix, route.Backend.Host, route.Backend.Port)
	return nil
}

func (r *GatewayRegistry) DeleteRoute(ctx context.Context, routeKey string) error {
	if strings.TrimSpace(routeKey) == "" {
		return fmt.Errorf("route key is required")
	}

	logger.Infof(ctx, "[GatewayRegistry] delete route key=%s", routeKey)
	return nil
}
