package route

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/goccy/go-yaml"
)

func TestNewTraefikRegistryFileProviderValidation(t *testing.T) {
	t.Parallel()

	_, err := NewTraefikRegistry(TraefikRegistryConfig{Provider: "file"})
	if err == nil {
		t.Fatalf("expected error when file path is empty")
	}
}

func TestTraefikRegistryFileProviderUpsertAndDelete(t *testing.T) {
	t.Parallel()

	filePath := filepath.Join(t.TempDir(), "dynamic", "routes.yaml")
	reg, err := NewTraefikRegistry(TraefikRegistryConfig{
		Provider: "file",
		FilePath: filePath,
	})
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}

	route := Registration{
		RouteKey:   "app-session-42",
		PathPrefix: "/apps/42",
		Backend: Backend{
			Host: "172.18.0.42",
			Port: 8787,
		},
		Metadata: map[string]string{
			"traefik_profile": "rstudio",
		},
	}

	if err := reg.UpsertRoute(context.Background(), route); err != nil {
		t.Fatalf("upsert route: %v", err)
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read file config: %v", err)
	}

	cfg := &traefikFileConfig{}
	if err := yaml.Unmarshal(content, cfg); err != nil {
		t.Fatalf("unmarshal file config: %v", err)
	}

	router, ok := cfg.HTTP.Routers["app-session-42"]
	if !ok {
		t.Fatalf("router not found in file config")
	}
	if router.Rule != "PathPrefix(`/apps/42`)" {
		t.Fatalf("unexpected router rule: %s", router.Rule)
	}
	if router.Service != "app-session-42-svc" {
		t.Fatalf("unexpected router service: %s", router.Service)
	}
	if len(router.Middlewares) != 2 {
		t.Fatalf("unexpected router middlewares length: %d", len(router.Middlewares))
	}
	if router.Middlewares[0] != "app-session-42-strip" {
		t.Fatalf("unexpected first middleware: %s", router.Middlewares[0])
	}
	if router.Middlewares[1] != "app-session-42-server-root-path-header" {
		t.Fatalf("unexpected second middleware: %s", router.Middlewares[1])
	}

	service, ok := cfg.HTTP.Services["app-session-42-svc"]
	if !ok {
		t.Fatalf("service not found in file config")
	}
	if len(service.LoadBalancer.Servers) != 1 {
		t.Fatalf("unexpected servers length: %d", len(service.LoadBalancer.Servers))
	}
	if service.LoadBalancer.Servers[0].URL != "http://172.18.0.42:8787" {
		t.Fatalf("unexpected backend url: %s", service.LoadBalancer.Servers[0].URL)
	}

	stripMiddleware, ok := cfg.HTTP.Middlewares["app-session-42-strip"]
	if !ok {
		t.Fatalf("strip middleware not found in file config")
	}
	if stripMiddleware.StripPrefix == nil {
		t.Fatalf("strip middleware config is nil")
	}
	if len(stripMiddleware.StripPrefix.Prefixes) != 1 || stripMiddleware.StripPrefix.Prefixes[0] != "/apps/42" {
		t.Fatalf("unexpected strip prefixes: %+v", stripMiddleware.StripPrefix.Prefixes)
	}
	if stripMiddleware.StripPrefix.ForceSlash {
		t.Fatalf("unexpected strip forceSlash: true")
	}

	headerMiddleware, ok := cfg.HTTP.Middlewares["app-session-42-server-root-path-header"]
	if !ok {
		t.Fatalf("header middleware not found in file config")
	}
	if headerMiddleware.Headers == nil {
		t.Fatalf("header middleware config is nil")
	}
	if headerMiddleware.Headers.CustomRequestHeaders["X-RStudio-Root-Path"] != "/apps/42" {
		t.Fatalf("unexpected X-RStudio-Root-Path header: %s", headerMiddleware.Headers.CustomRequestHeaders["X-RStudio-Root-Path"])
	}

	if err := reg.DeleteRoute(context.Background(), "app-session-42"); err != nil {
		t.Fatalf("delete route: %v", err)
	}

	content, err = os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read file config after delete: %v", err)
	}
	cfg = &traefikFileConfig{}
	if err := yaml.Unmarshal(content, cfg); err != nil {
		t.Fatalf("unmarshal file config after delete: %v", err)
	}
	if _, ok := cfg.HTTP.Routers["app-session-42"]; ok {
		t.Fatalf("router still exists after delete")
	}
	if _, ok := cfg.HTTP.Services["app-session-42-svc"]; ok {
		t.Fatalf("service still exists after delete")
	}
	if _, ok := cfg.HTTP.Middlewares["app-session-42-strip"]; ok {
		t.Fatalf("strip middleware still exists after delete")
	}
	if _, ok := cfg.HTTP.Middlewares["app-session-42-server-root-path-header"]; ok {
		t.Fatalf("header middleware still exists after delete")
	}
}

func TestTraefikRegistryFileProviderSCodeMiddlewares(t *testing.T) {
	t.Parallel()

	filePath := filepath.Join(t.TempDir(), "dynamic", "routes.yaml")
	reg, err := NewTraefikRegistry(TraefikRegistryConfig{
		Provider: "file",
		FilePath: filePath,
	})
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}

	route := Registration{
		RouteKey:   "app-session-99",
		PathPrefix: "/apps/99",
		Backend: Backend{
			Host: "172.18.0.99",
			Port: 8443,
		},
		Metadata: map[string]string{
			"traefik_profile": "scode",
		},
	}

	if err := reg.UpsertRoute(context.Background(), route); err != nil {
		t.Fatalf("upsert route: %v", err)
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read file config: %v", err)
	}

	cfg := &traefikFileConfig{}
	if err := yaml.Unmarshal(content, cfg); err != nil {
		t.Fatalf("unmarshal file config: %v", err)
	}

	router, ok := cfg.HTTP.Routers["app-session-99"]
	if !ok {
		t.Fatalf("router not found in file config")
	}
	if len(router.Middlewares) != 1 {
		t.Fatalf("unexpected router middlewares length: %d", len(router.Middlewares))
	}
	if router.Middlewares[0] != "app-session-99-strip" {
		t.Fatalf("unexpected middleware: %s", router.Middlewares[0])
	}

	if _, ok := cfg.HTTP.Middlewares["app-session-99-strip"]; !ok {
		t.Fatalf("strip middleware not found in file config")
	}
	if _, ok := cfg.HTTP.Middlewares["app-session-99-server-root-path-header"]; ok {
		t.Fatalf("rstudio header middleware should not exist")
	}
}

// func TestResolveTraefikProfile(t *testing.T) {
// 	t.Parallel()

// 	cases := []struct {
// 		name  string
// 		route Registration
// 		want  string
// 	}{
// 		{
// 			name: "metadata overrides port",
// 			route: Registration{
// 				Backend:  Backend{Port: 8787},
// 				Metadata: map[string]string{"traefik_profile": "scode"},
// 			},
// 			want: traefikProfileSCode,
// 		},
// 		{
// 			name:  "infer rstudio by port",
// 			route: Registration{Backend: Backend{Port: 8787}},
// 			want:  traefikProfileRStudio,
// 		},
// 		{
// 			name:  "infer notebook by port",
// 			route: Registration{Backend: Backend{Port: 8888}},
// 			want:  traefikProfileNotebook,
// 		},
// 		{
// 			name:  "fallback default",
// 			route: Registration{Backend: Backend{Port: 9999}},
// 			want:  traefikProfileDefault,
// 		},
// 	}

// 	for _, tc := range cases {
// 		tc := tc
// 		t.Run(tc.name, func(t *testing.T) {
// 			t.Parallel()
// 			if got := resolveTraefikProfile(tc.route); got != tc.want {
// 				t.Fatalf("resolveTraefikProfile() = %s, want %s", got, tc.want)
// 			}
// 		})
// 	}
// }
