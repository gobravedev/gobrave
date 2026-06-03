package router

import (
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	_ "github.com/gobravedev/gobrave/docs" // IGNORE

	"github.com/gobravedev/gobrave/internal/config"
	"github.com/gobravedev/gobrave/internal/handler"
	"github.com/gobravedev/gobrave/internal/middleware"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/dig"
)

type RouterParams struct {
	dig.In
	Config      *config.Config
	UserService interfaces.UserService
	AuthHandler *handler.AuthHandler
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
	// r.Use(middleware.Auth(params.TenantService, params.UserService, params.Config))
	v1 := r.Group("/api/v1")
	{
		RegisterAuthRoutes(v1, params.AuthHandler)

	}
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
