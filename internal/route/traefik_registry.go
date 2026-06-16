package route

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gobravedev/gobrave/internal/logger"
)

type TraefikRegistryConfig struct {
	BaseURL    string
	UpsertPath string
	DeletePath string
	AuthToken  string
	Timeout    time.Duration
}

type TraefikRegistry struct {
	baseURL    string
	upsertPath string
	deletePath string
	authToken  string
	client     *http.Client
}

type traefikRoutePayload struct {
	RouteKey   string            `json:"route_key"`
	PathPrefix string            `json:"path_prefix"`
	BackendURL string            `json:"backend_url"`
	Backend    Backend           `json:"backend"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	Traefik    traefikHTTPConfig `json:"traefik"`
}

type traefikHTTPConfig struct {
	Router  traefikRouterConfig  `json:"router"`
	Service traefikServiceConfig `json:"service"`
}

type traefikRouterConfig struct {
	Name        string   `json:"name"`
	Rule        string   `json:"rule"`
	Service     string   `json:"service"`
	EntryPoints []string `json:"entry_points"`
}

type traefikServiceConfig struct {
	Name         string                    `json:"name"`
	LoadBalancer traefikLoadBalancerConfig `json:"load_balancer"`
}

type traefikLoadBalancerConfig struct {
	Servers []traefikServer `json:"servers"`
}

type traefikServer struct {
	URL string `json:"url"`
}

func NewTraefikRegistry(cfg TraefikRegistryConfig) (*TraefikRegistry, error) {
	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		return nil, fmt.Errorf("traefik base url is required")
	}

	upsertPath := strings.TrimSpace(cfg.UpsertPath)
	if upsertPath == "" {
		upsertPath = "/api/providers/http/routes/{route_key}"
	}
	deletePath := strings.TrimSpace(cfg.DeletePath)
	if deletePath == "" {
		deletePath = "/api/providers/http/routes/{route_key}"
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	return &TraefikRegistry{
		baseURL:    strings.TrimRight(baseURL, "/"),
		upsertPath: upsertPath,
		deletePath: deletePath,
		authToken:  strings.TrimSpace(cfg.AuthToken),
		client:     &http.Client{Timeout: timeout},
	}, nil
}

func (r *TraefikRegistry) UpsertRoute(ctx context.Context, route Registration) error {
	if err := validateRegistration(route); err != nil {
		return err
	}

	payload := buildTraefikRoutePayload(route)
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	endpoint := r.resolvePath(r.upsertPath, route.RouteKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	r.attachAuth(req)

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("traefik upsert route failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	logger.Infof(ctx, "[TraefikRegistry] upsert route key=%s prefix=%s backend=%s:%d", route.RouteKey, route.PathPrefix, route.Backend.Host, route.Backend.Port)
	return nil
}

func (r *TraefikRegistry) DeleteRoute(ctx context.Context, routeKey string) error {
	if strings.TrimSpace(routeKey) == "" {
		return fmt.Errorf("route key is required")
	}

	endpoint := r.resolvePath(r.deletePath, routeKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	r.attachAuth(req)

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("traefik delete route failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	logger.Infof(ctx, "[TraefikRegistry] delete route key=%s", routeKey)
	return nil
}

func (r *TraefikRegistry) resolvePath(pathTemplate string, routeKey string) string {
	path := strings.TrimSpace(pathTemplate)
	if path == "" {
		path = "/api/providers/http/routes/{route_key}"
	}
	path = strings.ReplaceAll(path, "{route_key}", routeKey)
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return r.baseURL + path
}

func (r *TraefikRegistry) attachAuth(req *http.Request) {
	if req == nil || r.authToken == "" {
		return
	}
	req.Header.Set("Authorization", "Bearer "+r.authToken)
}

func buildTraefikRoutePayload(route Registration) *traefikRoutePayload {
	serviceName := route.RouteKey + "-svc"
	backendURL := "http://" + route.Backend.Host + ":" + strconv.Itoa(route.Backend.Port)
	return &traefikRoutePayload{
		RouteKey:   route.RouteKey,
		PathPrefix: route.PathPrefix,
		BackendURL: backendURL,
		Backend:    route.Backend,
		Metadata:   route.Metadata,
		Traefik: traefikHTTPConfig{
			Router: traefikRouterConfig{
				Name:        route.RouteKey,
				Rule:        "PathPrefix(`" + route.PathPrefix + "`)",
				Service:     serviceName,
				EntryPoints: []string{"web"},
			},
			Service: traefikServiceConfig{
				Name: serviceName,
				LoadBalancer: traefikLoadBalancerConfig{
					Servers: []traefikServer{{URL: backendURL}},
				},
			},
		},
	}
}

func validateRegistration(route Registration) error {
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
	return nil
}
