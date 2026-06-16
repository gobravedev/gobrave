package route

import "context"

// Registration describes one app-session route bound to a concrete backend.
type Registration struct {
	RouteKey   string
	PathPrefix string
	Backend    Backend
	Metadata   map[string]string
}

type Backend struct {
	Host string
	Port int
}

// RouteRegistry is the abstraction for route management backends such as
// Traefik API/File, in-process gateway, or a composite implementation.
type RouteRegistry interface {
	UpsertRoute(ctx context.Context, route Registration) error
	DeleteRoute(ctx context.Context, routeKey string) error
}
