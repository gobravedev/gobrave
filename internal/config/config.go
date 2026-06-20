package config

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/utils"
	"github.com/goccy/go-yaml"
)

type Config struct {
	Server    *ServerConfig    `yaml:"server"   json:"server"`
	Database  *DatabaseConfig  `yaml:"database" json:"database"`
	Feed      *FeedConfig      `yaml:"feed"     json:"feed"`
	Proxy     *ProxyConfig     `yaml:"proxy"    json:"proxy"`
	Route     *RouteConfig     `yaml:"route"    json:"route"`
	Storage   *StorageConfig   `yaml:"storage"  json:"storage"`
	Container *ContainerConfig `yaml:"container" json:"container"`
	// Ingest   *IngestConfig   `yaml:"ingest"   json:"ingest"`
	Tenant *TenantConfig `yaml:"tenant"   json:"tenant"`
	// Audio  *AudioConfig  `yaml`
}

type ContainerConfig struct {
	RefreshImageStatusOnStart   bool   `yaml:"refresh_image_status_on_start" json:"refresh_image_status_on_start"`
	RecoverRunningDagOnStart    bool   `yaml:"recover_running_dag_on_start" json:"recover_running_dag_on_start"`
	DagNodeCleanupOnFailed      string `yaml:"dag_node_cleanup_on_failed" json:"dag_node_cleanup_on_failed"`
	DagNodeCleanupOnDagFinished string `yaml:"dag_node_cleanup_on_dag_finished" json:"dag_node_cleanup_on_dag_finished"`
}

type StorageConfig struct {
	ImageDir string `yaml:"image_dir" json:"image_dir"`
	BaseDir  string `yaml:"base_dir" json:"base_dir"`
}

type ProxyConfig struct {
	BraveAPI   string `yaml:"brave_api" json:"brave_api"`
	Container  string `yaml:"container" json:"container"`
	OnlyOffice string `yaml:"onlyoffice" json:"onlyoffice"`
}

type RouteConfig struct {
	Registry   string              `yaml:"registry"     json:"registry"`
	AppsPrefix string              `yaml:"apps_prefix"  json:"apps_prefix"`
	Traefik    *TraefikRouteConfig `yaml:"traefik"      json:"traefik"`
}

const defaultAppsPrefix = "/apps"

type TraefikRouteConfig struct {
	Provider      string `yaml:"provider"        json:"provider"`
	BaseURL       string `yaml:"base_url"        json:"base_url"`
	UpsertPath    string `yaml:"upsert_path"     json:"upsert_path"`
	DeletePath    string `yaml:"delete_path"     json:"delete_path"`
	AuthToken     string `yaml:"auth_token"      json:"auth_token"`
	TimeoutSecond int    `yaml:"timeout_second"  json:"timeout_second"`
	FilePath      string `yaml:"file_path"       json:"file_path"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Port int `yaml:"port"             json:"port"`
	// GRPCPort        int           `yaml:"grpc_port"        json:"grpc_port"`
	Host            string        `yaml:"host"             json:"host"`
	LogPath         string        `yaml:"log_path"         json:"log_path"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" json:"shutdown_timeout" default:"30s"`
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	Driver   string `yaml:"driver"   json:"driver"`
	Host     string `yaml:"host"     json:"host"`
	Port     string `yaml:"port"     json:"port"`
	User     string `yaml:"user"     json:"user"`
	Password string `yaml:"password" json:"password"`
	Name     string `yaml:"name"     json:"name"`
	SSLMode  string `yaml:"ssl_mode" json:"ssl_mode"`
	Path     string `yaml:"path"     json:"path"`
}

type AudioConfig struct {
	Dir string
}

// FeedConfig feed 异步构建配置
type FeedConfig struct {
	WorkerCount      int  `yaml:"worker_count"      json:"worker_count"`
	QueueSize        int  `yaml:"queue_size"        json:"queue_size"`
	BackfillEnabled  bool `yaml:"backfill_enabled"  json:"backfill_enabled"`
	BackfillBatch    int  `yaml:"backfill_batch"    json:"backfill_batch"`
	RetryMaxAttempts int  `yaml:"retry_max_attempts" json:"retry_max_attempts"`
	RetryBaseDelayMs int  `yaml:"retry_base_delay_ms" json:"retry_base_delay_ms"`
	RetryMaxDelayMs  int  `yaml:"retry_max_delay_ms"  json:"retry_max_delay_ms"`
}

// type IngestConfig struct {
// 	Enabled                 bool   `yaml:"enabled" json:"enabled"`
// 	FetchIntervalSec        int    `yaml:"fetch_interval_sec" json:"fetch_interval_sec"`
// 	HTTPTimeoutSec          int    `yaml:"http_timeout_sec" json:"http_timeout_sec"`
// 	FetchWorkers            int    `yaml:"fetch_workers" json:"fetch_workers"`
// 	ParserGRPCAddr          string `yaml:"parser_grpc_addr" json:"parser_grpc_addr"`
// 	ParserGRPCInsecure      *bool  `yaml:"parser_grpc_insecure" json:"parser_grpc_insecure"`
// 	ParserGRPCTimeoutSec    int    `yaml:"parser_grpc_timeout_sec" json:"parser_grpc_timeout_sec"`
// 	ParserDispatchBatchSize int    `yaml:"parser_dispatch_batch_size" json:"parser_dispatch_batch_size"`
// 	ParserCallbackSecret    string `yaml:"parser_callback_secret" json:"parser_callback_secret"`
// }

type TenantConfig struct {
	AesKey string `yaml:"aes_key" json:"aes_key"`
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		Server: &ServerConfig{
			Port: 8082,
			// GRPCPort:        9092,
			Host:            "0.0.0.0",
			LogPath:         "logs/server.log",
			ShutdownTimeout: 30 * time.Second,
		},
		Database: &DatabaseConfig{
			Driver:  "sqlite",
			Host:    "127.0.0.1",
			Port:    "5432",
			User:    "postgres",
			Name:    "postgres",
			SSLMode: "disable",
			Path:    "",
		},
		Feed: &FeedConfig{
			WorkerCount:      4,
			QueueSize:        2048,
			BackfillEnabled:  false,
			BackfillBatch:    500,
			RetryMaxAttempts: 5,
			RetryBaseDelayMs: 100,
			RetryMaxDelayMs:  5000,
		},
		Proxy: &ProxyConfig{
			BraveAPI:   "http://localhost:5000",
			Container:  "http://localhost:8089",
			OnlyOffice: "http://localhost:8080",
		},
		Route: &RouteConfig{
			Registry:   "gateway",
			AppsPrefix: defaultAppsPrefix,
			Traefik: &TraefikRouteConfig{
				Provider:      "api",
				BaseURL:       "",
				UpsertPath:    "/api/providers/http/routes/{route_key}",
				DeletePath:    "/api/providers/http/routes/{route_key}",
				AuthToken:     "",
				TimeoutSecond: 5,
				FilePath:      "",
			},
		},
		Storage: &StorageConfig{
			ImageDir: "",
			BaseDir:  "",
		},
		Container: &ContainerConfig{
			RefreshImageStatusOnStart:   true,
			RecoverRunningDagOnStart:    true,
			DagNodeCleanupOnFailed:      "stop",
			DagNodeCleanupOnDagFinished: "delete",
		},
		// Ingest: &IngestConfig{
		// 	Enabled:                 true,
		// 	FetchIntervalSec:        300,
		// 	HTTPTimeoutSec:          15,
		// 	FetchWorkers:            1,
		// 	ParserGRPCAddr:          "127.0.0.1:50051",
		// 	ParserGRPCTimeoutSec:    8,
		// 	ParserDispatchBatchSize: 100,
		// 	ParserCallbackSecret:    "",
		// },
		Tenant: &TenantConfig{
			AesKey: "your-aes-key-here",
		},
	}

	configPath, err := utils.ResolveExternalPath("config.yml")
	logger.Infof(context.Background(), "Resolved config.yml path: %s", configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve config path: %w", err)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
	}

	if cfg.Storage == nil {
		cfg.Storage = &StorageConfig{ImageDir: ""}
	} else if strings.TrimSpace(cfg.Storage.ImageDir) == "" {
		cfg.Storage.ImageDir = ""
	}
	if cfg.Container == nil {
		cfg.Container = &ContainerConfig{
			RefreshImageStatusOnStart:   true,
			RecoverRunningDagOnStart:    true,
			DagNodeCleanupOnFailed:      "stop",
			DagNodeCleanupOnDagFinished: "delete",
		}
	}

	cfg.Container.DagNodeCleanupOnFailed = normalizeContainerCleanupPolicy(cfg.Container.DagNodeCleanupOnFailed, "stop")
	cfg.Container.DagNodeCleanupOnDagFinished = normalizeContainerCleanupPolicy(cfg.Container.DagNodeCleanupOnDagFinished, "delete")

	if cfg.Route == nil {
		cfg.Route = &RouteConfig{Registry: "gateway"}
	}
	if strings.TrimSpace(cfg.Route.Registry) == "" {
		cfg.Route.Registry = "gateway"
	}
	cfg.Route.AppsPrefix = normalizePathPrefix(cfg.Route.AppsPrefix, defaultAppsPrefix)
	if cfg.Route.Traefik == nil {
		cfg.Route.Traefik = &TraefikRouteConfig{}
	}
	if strings.TrimSpace(cfg.Route.Traefik.Provider) == "" {
		cfg.Route.Traefik.Provider = "api"
	}
	if cfg.Route.Traefik.TimeoutSecond <= 0 {
		cfg.Route.Traefik.TimeoutSecond = 5
	}
	if strings.TrimSpace(cfg.Route.Traefik.UpsertPath) == "" {
		cfg.Route.Traefik.UpsertPath = "/api/providers/http/routes/{route_key}"
	}
	if strings.TrimSpace(cfg.Route.Traefik.DeletePath) == "" {
		cfg.Route.Traefik.DeletePath = "/api/providers/http/routes/{route_key}"
	}

	TENANT_AES_KEY := cfg.Tenant.AesKey
	os.Setenv("TENANT_AES_KEY", TENANT_AES_KEY)

	return cfg, nil
}

func ResolveAppsPathPrefix(cfg *Config) string {
	if cfg == nil || cfg.Route == nil {
		return defaultAppsPrefix
	}
	return normalizePathPrefix(cfg.Route.AppsPrefix, defaultAppsPrefix)
}

func normalizePathPrefix(value, fallback string) string {
	prefix := strings.TrimSpace(value)
	if prefix == "" {
		prefix = fallback
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	prefix = strings.TrimRight(prefix, "/")
	if prefix == "" {
		return fallback
	}
	return prefix
}

func normalizeContainerCleanupPolicy(value string, fallback string) string {
	v := strings.TrimSpace(strings.ToLower(value))
	switch v {
	case "none", "stop", "delete":
		return v
	default:
		return fallback
	}
}
