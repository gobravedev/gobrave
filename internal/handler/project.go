package handler

import (
	"encoding/json"
	stderrs "errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gobravedev/gobrave/internal/errors"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/gorm"
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

	userID, ok := getCurrentUserID(c)
	if !ok {
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

// GetActiveProject godoc
// @Summary      获取当前用户激活项目
// @Description  返回当前登录用户唯一激活的项目
// @Tags         项目
// @Produce      json
// @Success      200  {object}  projectListItem  "激活项目"
// @Failure      401  {object}  errors.AppError  "未认证"
// @Failure      404  {object}  errors.AppError  "未找到激活项目"
// @Failure      500  {object}  errors.AppError  "服务器错误"
// @Security     Bearer
// @Router       /project/active-project [get]
func (h *ProjectHandler) GetActiveProject(c *gin.Context) {
	ctx := c.Request.Context()

	userID, ok := getCurrentUserID(c)
	if !ok {
		return
	}

	project, err := h.projectService.GetActiveProjectByUserID(ctx, userID)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("active project not found"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to get active project").WithDetails(err.Error()))
		return
	}

	c.JSON(http.StatusOK, projectListItem{
		ID:           project.ID,
		ProjectID:    project.ProjectID,
		ProjectName:  project.ProjectName,
		MetadataForm: parseMetadataForm(project.MetadataForm),
		Research:     project.Research,
		Parameter:    project.Parameter,
		Description:  project.Description,
	})
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

	userID, ok := getCurrentUserID(c)
	if !ok {
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

type activateProjectRequest struct {
	ProjectID string `json:"project_id" binding:"required"`
}

// ActivateProject godoc
// @Summary      激活当前用户项目
// @Description  按项目ID激活当前用户的项目，并将该用户其他项目全部置为未激活
// @Tags         项目
// @Accept       json
// @Produce      json
// @Param        request  body      activateProjectRequest  true  "请求参数"
// @Success      200      {object}  map[string]string       "激活成功"
// @Failure      400      {object}  errors.AppError         "参数错误"
// @Failure      401      {object}  errors.AppError         "未认证"
// @Failure      404      {object}  errors.AppError         "项目未关联当前用户"
// @Failure      500      {object}  errors.AppError         "服务器错误"
// @Security     Bearer
// @Router       /project/activate-project [post]
func (h *ProjectHandler) ActivateProject(c *gin.Context) {
	ctx := c.Request.Context()

	userID, ok := getCurrentUserID(c)
	if !ok {
		return
	}

	var req activateProjectRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.projectService.ActivateUserProject(ctx, userID, req.ProjectID); err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("project is not bound to current user"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to activate project").WithDetails(err.Error()))
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "project activated successfully"})
}

func getCurrentUserID(c *gin.Context) (string, bool) {
	userIDVal, exists := c.Get(types.UserIDContextKey.String())
	if !exists {
		c.Error(errors.NewUnauthorizedError("missing user identity"))
		return "", false
	}

	userID, ok := userIDVal.(string)
	if !ok || userID == "" {
		c.Error(errors.NewUnauthorizedError("invalid user identity"))
		return "", false
	}

	return userID, true
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
