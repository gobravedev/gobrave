package container

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	// sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	// "github.com/minebiome/ai-agent-go/internal/application/repository"
	// "github.com/minebiome/ai-agent-go/internal/application/service"
	// "github.com/minebiome/ai-agent-go/internal/config"
	// "github.com/minebiome/ai-agent-go/internal/grpcserver"
	// "github.com/minebiome/ai-agent-go/internal/handler"
	"github.com/gobravedev/gobrave/internal/application/repository"
	"github.com/gobravedev/gobrave/internal/application/service"
	"github.com/gobravedev/gobrave/internal/config"
	"github.com/gobravedev/gobrave/internal/handler"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/router"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"github.com/gobravedev/gobrave/internal/utils"

	// "github.com/minebiome/ai-agent-go/internal/router"
	// "github.com/minebiome/ai-agent-go/internal/types"
	// "github.com/minebiome/ai-agent-go/internal/utils"

	"go.uber.org/dig"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// must is a helper function for error handling
// Panics if the error is not nil, useful for configuration steps that must succeed
// Parameters:
//   - err: Error to check
func must(err error) {
	if err != nil {
		panic(err)
	}
}

func BuildContainer(container *dig.Container) *dig.Container {
	ctx := context.Background()
	logger.Debugf(ctx, "[Container] Starting container initialization...")

	logger.Debugf(ctx, "[Container] Registering core infrastructure...")
	must(container.Provide(config.LoadConfig))
	must(container.Provide(initDatabase))

	logger.Debugf(ctx, "[Container] Registering timeline repository...")
	// must(container.Provide(repository.NewTimelineRepository))
	// must(container.Provide(repository.NewArticleRepository))
	// must(container.Provide(repository.NewArticleTranslationRepository))
	// must(container.Provide(repository.NewArticleAudioRepository))
	// must(container.Provide(repository.NewEntityRepository))
	// must(container.Provide(repository.NewTopicRepository))
	// must(container.Provide(repository.NewTopicArticleRepository))
	// must(container.Provide(repository.NewEntityTranslationRepository))
	// must(container.Provide(repository.NewArticleEntityRepository))
	// must(container.Provide(repository.NewArticleFeedRepository))
	// must(container.Provide(repository.NewSystemSettingRepository))
	must(container.Provide(repository.NewUserRepository))
	must(container.Provide(repository.NewAuthTokenRepository))
	must(container.Provide(repository.NewProjectRepository))
	// must(container.Provide(repository.NewTenantRepository))
	// must(container.Provide(repository.NewTraceRepository))
	// must(container.Provide(repository.NewRSSSourceRepository))

	logger.Debugf(ctx, "[Container] Registering timeline services...")
	// must(container.Provide(service.NewWeightedRankingStrategy))
	// must(container.Provide(service.NewFeedBuilder))
	// must(container.Provide(service.NewFeedDispatcher))
	// must(container.Provide(service.NewFeedEventProducer))
	// // must(container.Provide(service.NewFeedBackfillRunner))
	// // must(container.Provide(service.NewIngestionOrchestrator))
	// must(container.Provide(service.NewTimelineService))
	// must(container.Provide(service.NewArticleService))
	// must(container.Provide(service.NewArticleTranslationService))
	// must(container.Provide(service.NewArticleAudioService))
	// must(container.Provide(service.NewEntityService))
	// must(container.Provide(service.NewTopicService))
	// must(container.Provide(service.NewTopicArticleService))
	// must(container.Provide(service.NewEntityTranslationService))
	// must(container.Provide(service.NewArticleEntityService))
	// must(container.Provide(service.NewTenantService))
	must(container.Provide(service.NewUserService))
	must(container.Provide(service.NewProjectService))
	// must(container.Provide(service.NewTraceService))
	// must(container.Provide(service.NewAuthService))
	// must(container.Provide(service.NewRSSSourceService))

	// HTTP handlers layer
	logger.Debugf(ctx, "[Container] Registering HTTP handlers...")
	// must(container.Provide(handler.NewTimelineHandler))
	// // must(container.Provide(handler.NewParserCallbackHandler))
	// must(container.Provide(handler.NewArticleHandler))
	// must(container.Provide(handler.NewArticleTranslationHandler))
	// must(container.Provide(handler.NewArticleAudioHandler))
	// must(container.Provide(handler.NewEntityHandler))
	// must(container.Provide(handler.NewTopicHandler))
	// must(container.Provide(handler.NewTopicArticleHandler))
	// must(container.Provide(handler.NewEntityTranslationHandler))
	// must(container.Provide(handler.NewArticleEntityHandler))
	must(container.Provide(handler.NewAuthHandler))
	must(container.Provide(handler.NewProjectHandler))
	must(container.Provide(handler.NewProxyHandler))
	// must(container.Provide(handler.NewTraceHandler))

	// must(container.Provide(grpcserver.NewTraceServer))
	// must(container.Provide(grpcserver.NewArticleServer))
	// must(container.Provide(grpcserver.NewServer))
	logger.Debugf(ctx, "[Container] Registering task enqueuer...")
	redisAvailable := os.Getenv("REDIS_ADDR") != ""
	if redisAvailable {
		// 当有人需要 *TaskEnqueuer  时，请调用 NewAsyncqClient() 创建
		// 遵循依赖倒置原则
		// 不要依赖 client 而依赖 TaskEnqueuer task interfaces.TaskEnqueuer
		must(container.Provide(router.NewAsyncqClient, dig.As(new(interfaces.TaskEnqueuer))))
		must(container.Provide(router.NewAsynqServer))
	} else {
		syncExec := router.NewSyncTaskExecutor()
		must(container.Provide(func() interfaces.TaskEnqueuer { return syncExec }))
		must(container.Provide(func() *router.SyncTaskExecutor { return syncExec }))
	}

	// Router configuration
	logger.Debugf(ctx, "[Container] Registering router and starting task server...")
	must(container.Provide(router.NewRouter))
	if redisAvailable {
		must(container.Invoke(router.RunAsynqServer))
	} else {
		must(container.Invoke(router.RegisterSyncHandlers))
	}

	return container
}

func initDatabase(cfg *config.Config) (*gorm.DB, error) {
	dbCfg := cfg.Database
	if dbCfg == nil {
		dbCfg = &config.DatabaseConfig{Driver: "sqlite", SSLMode: "disable"}
	}
	driver := dbCfg.Driver
	if driver == "" {
		driver = "sqlite"
	}
	driver = strings.ToLower(driver)

	var dialector gorm.Dialector
	switch driver {
	case "postgres":
		sslMode := dbCfg.SSLMode
		if sslMode == "" {
			sslMode = "disable"
		}
		port := dbCfg.Port
		if port == "" {
			port = "5432"
		}
		gormDSN := fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s TimeZone=UTC",
			dbCfg.Host,
			port,
			dbCfg.User,
			dbCfg.Password,
			dbCfg.Name,
			sslMode,
		)
		dialector = postgres.Open(gormDSN)

		logger.Infof(context.Background(), "DB Config: user=%s host=%s port=%s dbname=%s",
			dbCfg.User,
			dbCfg.Host,
			port,
			dbCfg.Name,
		)
	case "mysql":
		host := dbCfg.Host
		if host == "" {
			host = "127.0.0.1"
		}
		port := dbCfg.Port
		if port == "" {
			port = "3306"
		}
		gormDSN := fmt.Sprintf(
			"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=UTC",
			dbCfg.User,
			dbCfg.Password,
			host,
			port,
			dbCfg.Name,
		)
		dialector = mysql.Open(gormDSN)

		logger.Infof(context.Background(), "DB Config: driver=mysql user=%s host=%s port=%s dbname=%s",
			dbCfg.User,
			host,
			port,
			dbCfg.Name,
		)
	case "sqlite":
		dbPath := dbCfg.Path
		if dbPath == "" {
			dbPath = filepath.Join("data", "ai-agent-go.db")
		}
		resolvedDBPath, err := utils.ResolveExternalPath(dbPath)
		logger.Infof(context.Background(), "Resolved SQLite DB path: %s", resolvedDBPath)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve sqlite db path: %w", err)
		}
		dbPath = resolvedDBPath
		if dir := filepath.Dir(dbPath); dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("failed to create SQLite data directory %s: %w", dir, err)
			}
		}
		// sqlite_vec.Auto()
		dsn := dbPath + "?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on"
		dialector = sqlite.Open(dsn)
		logger.Infof(context.Background(), "DB Config: driver=sqlite path=%s", dbPath)
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", driver)
	}
	db, err := gorm.Open(dialector, &gorm.Config{
		NowFunc: func() time.Time {
			return time.Now().UTC()
		},
	})
	if err != nil {
		return nil, err
	}

	if driver == "sqlite" {
		sqlDB, err := db.DB()
		if err != nil {
			return nil, fmt.Errorf("failed to get underlying sql.DB: %w", err)
		}
		if err := sqlDB.Ping(); err != nil {
			return nil, fmt.Errorf("failed to ping SQLite database: %w", err)
		}
	}

	if err := db.AutoMigrate(
		// &types.Timeline{},
		// &types.Article{},
		// &types.ArticleTranslation{},
		// &types.ArticleAudio{},
		// &types.Entity{},
		// &types.Topic{},
		// &types.TopicArticle{},
		// &types.EntityTranslation{},
		// &types.ArticleEntity{},
		// &types.ArticleFeed{},
		// &types.SystemSetting{},
		&types.User{},
		// &types.Tenant{},
		&types.Project{},
		&types.UserProject{},
		&types.ProjectReport{},
		&types.AuthToken{},
	// &types.Trace{},
	// &types.RSSSource{},
	); err != nil {
		return nil, fmt.Errorf("failed to auto migrate tables: %w", err)
	}

	// Get underlying SQL DB object
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	// Configure connection pool parameters
	if driver == "sqlite" {
		// SQLite only supports one concurrent writer even in WAL mode.
		// Limiting to a single open connection serialises all DB access and
		// prevents "database is locked" errors from concurrent goroutines.
		sqlDB.SetMaxOpenConns(1)
	} else {
		sqlDB.SetMaxIdleConns(10)
	}
	sqlDB.SetConnMaxLifetime(time.Duration(10) * time.Minute)

	return db, nil
}
