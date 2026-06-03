package handler

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gobravedev/gobrave/internal/errors"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type ProjectHandler struct {
	projectService interfaces.ProjectService
}

func NewProjectHandler(projectService interfaces.ProjectService) *ProjectHandler {
	return &ProjectHandler{projectService: projectService}
}

// ProjectListItem godoc
type projectListItem struct {
	ID           uint        `json:"id"`
	ProjectID    string      `json:"project_id"`
	ProjectName  string      `json:"project_name"`
	MetadataForm interface{} `json:"metadata_form"`
	Research     string      `json:"research"`
	Parameter    string      `json:"parameter"`
	Description  string      `json:"description"`
}

// ListProject godoc
// @Summary      获取当前用户的项目列表
// @Description  返回与当前登录用户关联的所有项目（通过 user_project 关系表过滤）
// @Tags         项目
// @Produce      json
// @Success      200  {array}   projectListItem  "项目列表"
// @Failure      401  {object}  errors.AppError  "未认证"
// @Failure      500  {object}  errors.AppError  "服务器错误"
// @Security     Bearer
// @Router       /project/list-project [get]
func (h *ProjectHandler) ListProject(c *gin.Context) {
	ctx := c.Request.Context()

	userIDVal, exists := c.Get(types.UserIDContextKey.String())
	if !exists {
		c.Error(errors.NewUnauthorizedError("missing user identity"))
		return
	}

	userID, ok := userIDVal.(string)
	if !ok || userID == "" {
		c.Error(errors.NewUnauthorizedError("invalid user identity"))
		return
	}

	projects, err := h.projectService.ListProjectByUserID(ctx, userID)
	if err != nil {
		c.Error(errors.NewInternalServerError("failed to list projects").WithDetails(err.Error()))
		return
	}

	result := make([]projectListItem, 0, len(projects))
	for _, p := range projects {
		metadata := parseMetadataForm(p.MetadataForm)
		result = append(result, projectListItem{
			ID:           p.ID,
			ProjectID:    p.ProjectID,
			ProjectName:  p.ProjectName,
			MetadataForm: metadata,
			Research:     p.Research,
			Parameter:    p.Parameter,
			Description:  p.Description,
		})
	}

	c.JSON(http.StatusOK, result)
}

// addUserProjectRequest is the request body for AddUserProject.
type addUserProjectRequest struct {
	ProjectID string `json:"project_id" binding:"required"`
}

// AddUserProject godoc
// @Summary      关联用户与项目
// @Description  将当前登录用户与指定项目关联（写入 user_project 中间表）
// @Tags         项目
// @Accept       json
// @Produce      json
// @Param        request  body      addUserProjectRequest  true  "请求参数"
// @Success      200      {object}  map[string]string      "关联成功"
// @Failure      400      {object}  errors.AppError        "参数错误或已存在关联"
// @Failure      401      {object}  errors.AppError        "未认证"
// @Failure      500      {object}  errors.AppError        "服务器错误"
// @Security     Bearer
// @Router       /project/add-user-project [post]
func (h *ProjectHandler) AddUserProject(c *gin.Context) {
	ctx := c.Request.Context()

	userIDVal, exists := c.Get(types.UserIDContextKey.String())
	if !exists {
		c.Error(errors.NewUnauthorizedError("missing user identity"))
		return
	}
	userID, ok := userIDVal.(string)
	if !ok || userID == "" {
		c.Error(errors.NewUnauthorizedError("invalid user identity"))
		return
	}

	var req addUserProjectRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.projectService.AddUserProject(ctx, userID, req.ProjectID); err != nil {
		c.Error(errors.NewBadRequestError(err.Error()))
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "user project added successfully"})
}

func parseMetadataForm(raw string) interface{} {
	if raw == "" {
		return []interface{}{}
	}

	var decoded interface{}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return []interface{}{}
	}

	return decoded
}
