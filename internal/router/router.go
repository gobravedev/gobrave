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
	Config         *config.Config
	UserService    interfaces.UserService
	AuthHandler    *handler.AuthHandler
	ProjectHandler *handler.ProjectHandler
	ProxyHandler   *handler.ProxyHandler
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
	serveFrontendStatic(r)

	r.Use(middleware.Auth(params.UserService, params.Config))
	v1 := r.Group("/api/v1")
	{
		RegisterAuthRoutes(v1, params.AuthHandler)
		RegisterProjectRoutes(v1, params.ProjectHandler)
	}

	r.Any("/brave-api", params.ProxyHandler.BraveAPIProxy)
	r.Any("/brave-api/*proxyPath", params.ProxyHandler.BraveAPIProxy)
	r.Any("/container", params.ProxyHandler.ContainerProxy)
	r.Any("/container/*proxyPath", params.ProxyHandler.ContainerProxy)
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
	// r.POST("/auth/refresh", handler.RefreshToken)
	// r.GET("/auth/validate", handler.ValidateToken)
	// r.POST("/auth/logout", handler.Logout)
	// r.GET("/auth/me", handler.GetCurrentUser)
	// r.POST("/auth/change-password", handler.ChangePassword)
}

func RegisterProjectRoutes(r *gin.RouterGroup, handler *handler.ProjectHandler) {
	r.GET("/project/list-project", handler.ListProject)
	r.GET("/project/active-project", handler.GetActiveProject)
	r.POST("/project/add-user-project", handler.AddUserProject)
	r.POST("/project/activate-project", handler.ActivateProject)
}

func serveFrontendStatic(r *gin.Engine) {
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

	r.Use(func(c *gin.Context) {
		if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead {
			c.Next()
			return
		}
		path := c.Request.URL.Path
		if path == "/api" || strings.HasPrefix(path, "/api/") || path == "/health" || strings.HasPrefix(path, "/health/") || path == "/swagger" || strings.HasPrefix(path, "/swagger/") || path == "/audio" || strings.HasPrefix(path, "/audio/") || path == "/brave-api" || strings.HasPrefix(path, "/brave-api/") || path == "/container" || strings.HasPrefix(path, "/container/") {
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
