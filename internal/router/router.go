package router

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	_ "github.com/gobravedev/gobrave/docs" // IGNORE

	"github.com/gobravedev/gobrave/internal/config"
	"github.com/gobravedev/gobrave/internal/handler"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/middleware"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"github.com/gobravedev/gobrave/internal/utils"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/dig"
)

type RouterParams struct {
	dig.In
	Config           *config.Config
	UserService      interfaces.UserService
	AuthHandler      *handler.AuthHandler
	ProjectHandler   *handler.ProjectHandler
	DataHandler      *handler.DataHandler
	ContainerHandler *handler.ContainerHandler
	AnalysisHandler  *handler.AnalysisHandler
	WorkflowHandler  *handler.WorkflowHandler
	SheetHandler     *handler.SheetHandler
	UploadHandler    *handler.UploadHandler
	ProxyHandler     *handler.ProxyHandler
}

func NewRouter(params RouterParams) *gin.Engine {
	r := gin.New()
	r.ContextWithFallback = true
	// CORS 中间件应放在最前面
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization", "X-API-Key", "X-Request-ID"},
		ExposeHeaders:    []string{"Content-Length", "Access-Control-Allow-Origin"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// 基础中间件（不需要认证）
	r.Use(middleware.RequestID())
	// r.Use(middleware.Language())
	// r.Use(middleware.Logger())
	r.Use(middleware.Recovery())
	r.Use(middleware.ErrorHandler())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// Swagger API 文档（仅在非生产环境下启用）
	// 通过 GIN_MODE 环境变量判断：release 模式下禁用 Swagger
	if gin.Mode() != gin.ReleaseMode {
		r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler,
			ginSwagger.DefaultModelsExpandDepth(-1), // 默认折叠 Models
			ginSwagger.DocExpansion("list"),         // 展开模式: "list"(展开标签), "full"(全部展开), "none"(全部折叠)
			ginSwagger.DeepLinking(true),            // 启用深度链接
			ginSwagger.PersistAuthorization(true),   // 持久化认证信息
		))
	}
	analysisAppsPrefix := config.ResolveAppsPathPrefix(params.Config)
	serveFrontendStatic(r, params.Config)
	handler.RegisterOnlyOfficeRoutes(r, params.ProxyHandler)

	r.Use(middleware.Auth(params.UserService, params.Config))
	serveImageStatic(r, params.Config)
	v1 := r.Group("/api/v1")
	{
		RegisterAuthRoutes(v1, params.AuthHandler)
		RegisterProjectRoutes(v1, params.ProjectHandler, params.UploadHandler)
		RegisterDataRoutes(v1, params.DataHandler)
		RegisterContainerRoutes(v1, params.ContainerHandler)
		RegisterAnalysisRoutes(v1, params.AnalysisHandler)
		RegisterWorkflowRoutes(v1, params.WorkflowHandler)
		RegisterSheetRoutes(v1, params.SheetHandler)
	}

	r.Any("/brave-api", params.ProxyHandler.BraveAPIProxy)
	r.Any("/brave-api/*proxyPath", params.ProxyHandler.BraveAPIProxy)
	r.Any("/container", params.ProxyHandler.ContainerProxy)
	r.Any("/container/*proxyPath", params.ProxyHandler.ContainerProxy)

	// r.Any("/apps", params.ProxyHandler.ContainerProxy)
	// r.Any("/apps/*proxyPath", params.ProxyHandler.ContainerProxy)
	r.Any(analysisAppsPrefix, params.ProxyHandler.ContainerProxy)
	r.Any(analysisAppsPrefix+"/*proxyPath", params.ProxyHandler.ContainerProxy)

	// r.Any("/apps", params.ProxyHandler.AppSessionProxy)
	// r.Any("/apps/*proxyPath", params.ProxyHandler.AppSessionProxy)
	r.NoRoute(params.ProxyHandler.FallbackProxy)
	return r
}

func RegisterAuthRoutes(r *gin.RouterGroup, handler *handler.AuthHandler) {
	r.POST("/auth/register", handler.Register)
	r.POST("/auth/login", handler.Login)
	// r.POST("/auth/auto-setup", handler.AutoSetup)
	// r.GET("/auth/oidc/config", handler.GetOIDCConfig)
	// r.GET("/auth/oidc/url", handler.GetOIDCAuthorizationURL)
	// r.GET("/auth/oidc/callback", handler.OIDCRedirectCallback)
	r.POST("/auth/refresh", handler.RefreshToken)
	// r.GET("/auth/validate", handler.ValidateToken)
	r.POST("/auth/logout", handler.Logout)
	r.GET("/auth/me", handler.GetCurrentUser)
	// r.POST("/auth/change-password", handler.ChangePassword)
}

func RegisterProjectRoutes(r *gin.RouterGroup, handler *handler.ProjectHandler, uploadHandler *handler.UploadHandler) {
	r.GET("/project/list-project", handler.ListProject)
	r.GET("/project/active-project", handler.GetActiveProject)
	r.POST("/project/add-user-project", handler.AddUserProject)
	r.POST("/project/activate-project", handler.ActivateProject)
	r.POST("/project/add-project-report", handler.AddProjectReport)
	r.POST("/project/update-project-report", handler.UpdateProjectReport)
	r.POST("/project/delete-project-report", handler.DeleteProjectReport)
	r.GET("/project/list-project-report", handler.ListProjectReport)
	r.GET("/project/project-report-detail", handler.GetProjectReportDetail)
	r.POST("/project/upload-image", uploadHandler.UploadImage)
}

func RegisterSheetRoutes(r *gin.RouterGroup, handler *handler.SheetHandler) {
	r.GET("/sheet/workbook", handler.ReadWorkbook)
	r.POST("/sheet/workbook/save", handler.WriteWorkbook)
	r.GET("/sheet/workbook/by-file-id", handler.ReadWorkbookByFileID)
	r.POST("/sheet/workbook/save/by-file-id", handler.WriteWorkbookByFileID)
}

func RegisterDataRoutes(r *gin.RouterGroup, handler *handler.DataHandler) {
	r.POST("/data/dataset/create", handler.CreateDataset)
	r.GET("/data/dataset/get", handler.GetDataset)
	r.POST("/data/dataset/update", handler.UpdateDataset)
	r.POST("/data/dataset/delete", handler.DeleteDataset)
	r.GET("/data/dataset/list", handler.ListDataset)
	r.POST("/data/dataset/list-by-project-page", handler.PageDatasetByProjectID)

	r.POST("/data/project-dataset/create", handler.CreateProjectDataset)
	r.GET("/data/project-dataset/get", handler.GetProjectDataset)
	r.POST("/data/project-dataset/update", handler.UpdateProjectDataset)
	r.POST("/data/project-dataset/delete", handler.DeleteProjectDataset)
	r.GET("/data/project-dataset/list", handler.ListProjectDataset)

	r.POST("/data/file/create", handler.CreateFile)
	r.GET("/data/file/get", handler.GetFile)
	r.POST("/data/file/update", handler.UpdateFile)
	r.POST("/data/file/delete", handler.DeleteFile)
	r.GET("/data/file/list", handler.ListFile)
	r.GET("/data/file/list-by-project", handler.ListFileByProjectID)
	r.POST("/data/file/list-by-project-page", handler.PageFileByProjectID)
	r.GET("/data/file/list-by-project-group", handler.ListFileByProjectIDGroupByRole)

	r.POST("/data/dataset-file/create", handler.CreateDatasetFile)
	r.POST("/data/dataset-file/add-file", handler.AddFileToDataset)
	r.GET("/data/dataset-file/get", handler.GetDatasetFile)
	r.POST("/data/dataset-file/update", handler.UpdateDatasetFile)
	r.POST("/data/dataset-file/delete", handler.DeleteDatasetFile)
	r.GET("/data/dataset-file/list", handler.ListDatasetFile)

	r.POST("/data/sample/create", handler.CreateSample)
	r.GET("/data/sample/get", handler.GetSample)
	r.POST("/data/sample/update", handler.UpdateSample)
	r.POST("/data/sample/delete", handler.DeleteSample)
	r.GET("/data/sample/list", handler.ListSample)
	r.GET("/data/sample/list-by-project", handler.ListSampleByProjectID)
	r.POST("/data/sample/list-by-project-page", handler.PageSampleByProjectID)

	r.POST("/data/sample-file/create", handler.CreateSampleFile)
	r.GET("/data/sample-file/get", handler.GetSampleFile)
	r.POST("/data/sample-file/update", handler.UpdateSampleFile)
	r.POST("/data/sample-file/delete", handler.DeleteSampleFile)
	r.GET("/data/sample-file/list", handler.ListSampleFile)

	r.POST("/data/dataset-sample/create", handler.CreateDatasetSample)
	r.GET("/data/dataset-sample/get", handler.GetDatasetSample)
	r.POST("/data/dataset-sample/update", handler.UpdateDatasetSample)
	r.POST("/data/dataset-sample/delete", handler.DeleteDatasetSample)
	r.GET("/data/dataset-sample/list", handler.ListDatasetSample)
}

func RegisterContainerRoutes(r *gin.RouterGroup, handler *handler.ContainerHandler) {
	r.POST("/container/image/create", handler.CreateContainerImage)
	r.GET("/container/image/get", handler.GetContainerImage)
	r.POST("/container/image/update", handler.UpdateContainerImage)
	r.POST("/container/image/delete", handler.DeleteContainerImage)
	r.GET("/container/image/list", handler.ListContainerImage)
	r.POST("/container/image/list-by-page", handler.PageContainerImage)

	r.POST("/container/template/create", handler.CreateContainerTemplate)
	r.GET("/container/template/get", handler.GetContainerTemplate)
	r.POST("/container/template/update", handler.UpdateContainerTemplate)
	r.POST("/container/template/delete", handler.DeleteContainerTemplate)
	r.GET("/container/template/list", handler.ListContainerTemplate)
	r.POST("/container/template/list-by-page", handler.PageContainerTemplate)

	r.POST("/container/app-session/create", handler.CreateAppSession)
	r.POST("/container/app-session/start", handler.StartAppSession)
	r.POST("/container/app-session/stop", handler.StopAppSession)
	r.POST("/container/app-session/delete", handler.DeleteAppSession)
	r.GET("/container/app-session/get", handler.GetAppSession)
	r.GET("/container/app-session/list", handler.ListAppSession)
	r.POST("/container/app-session/list-by-page", handler.PageAppSession)

	r.POST("/container/instance/list-by-page", handler.PageContainerInstance)
	r.POST("/container/event/list-by-page", handler.PageContainerEvent)
	r.POST("/container/outbox/list-by-page", handler.PageOutboxEvent)
}

func RegisterWorkflowRoutes(r *gin.RouterGroup, handler *handler.WorkflowHandler) {
	r.GET("/workflow/tools/get-from-json/:workflowId", handler.GetFromJSONByWorlflow)
	r.GET("/workflows/:workflowId/form", handler.GetWorkflowForm)
}

func RegisterAnalysisRoutes(r *gin.RouterGroup, handler *handler.AnalysisHandler) {
	r.POST("/analysis/edit-params-v2/:analysisId", handler.EditParamsV2)
	r.POST("/analysis/edit-node-params/:analysisNodeId", handler.EditNodeParams)
}

// serveImageStatic maps local image resources under /images.
func serveImageStatic(r *gin.Engine, cfg *config.Config) {
	configuredDir := ""
	baseDir := ""
	if cfg != nil && cfg.Storage != nil {
		configuredDir = cfg.Storage.ImageDir
		baseDir = cfg.Storage.BaseDir
	}

	imageDir, err := utils.ResolveConfiguredPath(configuredDir, "images")
	if err != nil {
		return
	}
	if err := os.MkdirAll(imageDir, 0o755); err != nil {
		return
	}

	logger.Infof(context.Background(), "[Router] Serving image files from %s at /images", imageDir)
	r.StaticFS("/images", http.Dir(imageDir))

	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		return
	}

	dataDir, err := utils.SafePathUnderBase(baseDir, filepath.Join(baseDir, "data"))
	if err != nil {
		logger.Warnf(context.Background(), "[Router] Skip serving /images-data: invalid base_dir/data path: base_dir=%s err=%v", baseDir, err)
		return
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		logger.Warnf(context.Background(), "[Router] Skip serving /images-data: create dir failed: data_dir=%s err=%v", dataDir, err)
		return
	}

	logger.Infof(context.Background(), "[Router] Serving data files from %s at /images-data", dataDir)
	r.StaticFS("/images-data", http.Dir(dataDir))
}

func serveFrontendStatic(r *gin.Engine, cfg *config.Config) {
	absDir, err := utils.ResolveExternalPath("web")
	if err != nil {
		return
	}
	indexPath := filepath.Join(absDir, "index.html")
	if _, err := os.Stat(indexPath); err != nil {
		return
	}

	logger.Infof(context.Background(), "[Router] Serving frontend static files from %s", absDir)
	fs := http.Dir(absDir)
	fileServer := http.FileServer(fs)
	analysisAppsPrefix := config.ResolveAppsPathPrefix(cfg)

	r.Use(func(c *gin.Context) {
		if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead {
			c.Next()
			return
		}
		path := c.Request.URL.Path
		if path == "/api" ||
			strings.HasPrefix(path, "/api/") ||
			path == "/health" ||
			strings.HasPrefix(path, "/health/") ||
			path == "/swagger" ||
			strings.HasPrefix(path, "/swagger/") ||
			path == "/audio" ||
			strings.HasPrefix(path, "/audio/") ||
			path == "/images" ||
			strings.HasPrefix(path, "/images/") ||
			path == "/images-data" ||
			strings.HasPrefix(path, "/images-data/") ||
			path == "/brave-api" ||
			strings.HasPrefix(path, "/brave-api/") ||
			path == "/container" ||
			strings.HasPrefix(path, "/container/") ||
			path == "/apps" ||
			strings.HasPrefix(path, "/apps/") ||
			path == analysisAppsPrefix ||
			strings.HasPrefix(path, analysisAppsPrefix+"/") ||
			path == "/onlyoffice" ||
			strings.HasPrefix(path, "/onlyoffice/") ||
			path == "/go-onlyoffice" ||
			strings.HasPrefix(path, "/go-onlyoffice/") {
			c.Next()
			return
		}
		fullPath := filepath.Join(absDir, path)
		if info, err := os.Stat(fullPath); err == nil && !info.IsDir() {
			setFrontendCacheHeaders(c.Writer, path)
			fileServer.ServeHTTP(c.Writer, c.Request)
			c.Abort()
			return
		}

		// BrowserRouter fallback: route-like paths should return index.html,
		// but missing static assets (e.g. *.js, *.css) should stay 404.
		if ext := filepath.Ext(path); ext != "" {
			c.Next()
			return
		}

		setFrontendCacheHeaders(c.Writer, "/index.html")
		c.File(indexPath)
		c.Abort()
	})
}

// setFrontendCacheHeaders sets Cache-Control headers for frontend static resources.
// Vite 构建产物中 /assets/* 的文件名带 hash，可长期缓存；其余（index.html、config.js、favicon 等）
// 每次都需 revalidate，避免前端升级后用户看到旧版本。
func setFrontendCacheHeaders(w http.ResponseWriter, path string) {
	if strings.HasPrefix(path, "/assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		return
	}
	w.Header().Set("Cache-Control", "no-cache, must-revalidate")
}
