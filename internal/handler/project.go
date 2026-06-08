package handler

import (
	"encoding/json"
	stderrs "errors"
	"net/http"
	"strconv"

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

type addProjectReportRequest struct {
	ProjectID string `json:"project_id" binding:"required"`
	Title     string `json:"title" binding:"required"`
	Content   string `json:"content"`
	SortOrder int    `json:"sort_order"`
}

// AddProjectReport godoc
// @Summary      添加项目报告
// @Description  向指定项目添加报告记录
// @Tags         项目
// @Accept       json
// @Produce      json
// @Param        request  body      addProjectReportRequest  true  "请求参数"
// @Success      200      {object}  types.ProjectReport      "创建成功"
// @Failure      400      {object}  errors.AppError          "参数错误"
// @Failure      401      {object}  errors.AppError          "未认证"
// @Failure      404      {object}  errors.AppError          "项目未关联当前用户"
// @Failure      500      {object}  errors.AppError          "服务器错误"
// @Security     Bearer
// @Router       /project/add-project-report [post]
func (h *ProjectHandler) AddProjectReport(c *gin.Context) {
	ctx := c.Request.Context()

	userID, ok := getCurrentUserID(c)
	if !ok {
		return
	}

	var req addProjectReportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	report := &types.ProjectReport{
		ProjectID: req.ProjectID,
		Title:     req.Title,
		Content:   req.Content,
		SortOrder: req.SortOrder,
	}

	if err := h.projectService.AddProjectReport(ctx, userID, report); err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("project is not bound to current user"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to add project report").WithDetails(err.Error()))
		return
	}

	c.JSON(http.StatusOK, report)
}

type updateProjectReportRequest struct {
	ID        int64  `json:"id,string" binding:"required"`
	ProjectID string `json:"project_id" binding:"required"`
	Title     string `json:"title" binding:"required"`
	Content   string `json:"content"`
	SortOrder int    `json:"sort_order"`
}

// UpdateProjectReport godoc
// @Summary      更新项目报告
// @Description  按报告ID更新指定项目下的报告
// @Tags         项目
// @Accept       json
// @Produce      json
// @Param        request  body      updateProjectReportRequest  true  "请求参数"
// @Success      200      {object}  map[string]string           "更新成功"
// @Failure      400      {object}  errors.AppError             "参数错误"
// @Failure      401      {object}  errors.AppError             "未认证"
// @Failure      404      {object}  errors.AppError             "项目或报告不存在"
// @Failure      500      {object}  errors.AppError             "服务器错误"
// @Security     Bearer
// @Router       /project/update-project-report [post]
func (h *ProjectHandler) UpdateProjectReport(c *gin.Context) {
	ctx := c.Request.Context()

	userID, ok := getCurrentUserID(c)
	if !ok {
		return
	}

	var req updateProjectReportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.projectService.UpdateProjectReport(ctx, userID, &types.ProjectReport{
		ID:        req.ID,
		ProjectID: req.ProjectID,
		Title:     req.Title,
		Content:   req.Content,
		SortOrder: req.SortOrder,
	}); err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("project report not found"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to update project report").WithDetails(err.Error()))
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "project report updated successfully"})
}

type deleteProjectReportRequest struct {
	ID int64 `json:"id,string" binding:"required"`
}

// DeleteProjectReport godoc
// @Summary      删除项目报告
// @Description  按报告ID删除指定项目下的报告
// @Tags         项目
// @Accept       json
// @Produce      json
// @Param        request  body      deleteProjectReportRequest  true  "请求参数"
// @Success      200      {object}  map[string]string           "删除成功"
// @Failure      400      {object}  errors.AppError             "参数错误"
// @Failure      401      {object}  errors.AppError             "未认证"
// @Failure      404      {object}  errors.AppError             "项目或报告不存在"
// @Failure      500      {object}  errors.AppError             "服务器错误"
// @Security     Bearer
// @Router       /project/delete-project-report [post]
func (h *ProjectHandler) DeleteProjectReport(c *gin.Context) {
	ctx := c.Request.Context()

	userID, ok := getCurrentUserID(c)
	if !ok {
		return
	}

	var req deleteProjectReportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.projectService.DeleteProjectReport(ctx, userID, req.ID); err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("project report not found"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to delete project report").WithDetails(err.Error()))
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "project report deleted successfully"})
}

type listProjectReportRequest struct {
	ProjectID string `form:"project_id" binding:"required"`
}

type projectReportDetailRequest struct {
	ID int64 `form:"id" binding:"required"`
}

type projectReportListItem struct {
	ID        string `json:"id"`
	ProjectID string `json:"project_id"`
	Title     string `json:"title"`
	SortOrder int    `json:"sort_order"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// ListProjectReport godoc
// @Summary      查询项目报告列表
// @Description  根据 project_id 查询报告列表，不返回 content 字段，按 sort_order、created_at 升序排序
// @Tags         项目
// @Produce      json
// @Param        project_id  query     string                  true  "项目ID"
// @Success      200         {array}   projectReportListItem   "报告列表"
// @Failure      400         {object}  errors.AppError         "参数错误"
// @Failure      401         {object}  errors.AppError         "未认证"
// @Failure      404         {object}  errors.AppError         "项目未关联当前用户"
// @Failure      500         {object}  errors.AppError         "服务器错误"
// @Security     Bearer
// @Router       /project/list-project-report [get]
func (h *ProjectHandler) ListProjectReport(c *gin.Context) {
	ctx := c.Request.Context()

	userID, ok := getCurrentUserID(c)
	if !ok {
		return
	}

	var req listProjectReportRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	reports, err := h.projectService.ListProjectReportByProjectID(ctx, userID, req.ProjectID)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("project is not bound to current user"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to list project report").WithDetails(err.Error()))
		return
	}

	result := make([]projectReportListItem, 0, len(reports))
	for _, report := range reports {
		result = append(result, projectReportListItem{
			ID:        strconv.FormatInt(report.ID, 10),
			ProjectID: report.ProjectID,
			Title:     report.Title,
			SortOrder: report.SortOrder,
			CreatedAt: report.CreatedAt.Format("2006-01-02 15:04:05"),
			UpdatedAt: report.UpdatedAt.Format("2006-01-02 15:04:05"),
		})
	}

	c.JSON(http.StatusOK, result)
}

// GetProjectReportDetail godoc
// @Summary      查询项目报告详情
// @Description  根据 id 查询报告详情（包含 content）
// @Tags         项目
// @Produce      json
// @Param        id          query     integer                 true  "报告ID"
// @Success      200         {object}  types.ProjectReport     "报告详情"
// @Failure      400         {object}  errors.AppError         "参数错误"
// @Failure      401         {object}  errors.AppError         "未认证"
// @Failure      404         {object}  errors.AppError         "项目未关联当前用户"
// @Failure      500         {object}  errors.AppError         "服务器错误"
// @Security     Bearer
// @Router       /project/project-report-detail [get]
func (h *ProjectHandler) GetProjectReportDetail(c *gin.Context) {
	ctx := c.Request.Context()

	userID, ok := getCurrentUserID(c)
	if !ok {
		return
	}

	var req projectReportDetailRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	report, err := h.projectService.GetProjectReportDetailByID(ctx, userID, req.ID)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("project report is not bound to current user"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to get project report detail").WithDetails(err.Error()))
		return
	}

	c.JSON(http.StatusOK, report)
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
