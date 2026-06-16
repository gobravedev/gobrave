package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gobravedev/gobrave/internal/errors"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type ContainerHandler struct {
	containerService interfaces.ContainerService
}

func NewContainerHandler(containerService interfaces.ContainerService) *ContainerHandler {
	return &ContainerHandler{containerService: containerService}
}

// CreateContainerImage godoc
// @Summary      创建容器镜像
// @Description  创建 ContainerImage 记录
// @Tags         容器管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.ContainerImage  true  "请求参数"
// @Success      200      {object}  types.ContainerImage
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /container/image/create [post]
func (h *ContainerHandler) CreateContainerImage(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.ContainerImage
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.containerService.CreateContainerImage(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to create container image")
		return
	}

	c.JSON(http.StatusOK, req)
}

// GetContainerImage godoc
// @Summary      获取容器镜像
// @Description  按 ID 查询 ContainerImage 详情
// @Tags         容器管理
// @Produce      json
// @Param        id       query     integer               true  "主键 ID"
// @Success      200      {object}  types.ContainerImage
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /container/image/get [get]
func (h *ContainerHandler) GetContainerImage(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idQuery
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid query parameters").WithDetails(err.Error()))
		return
	}

	item, err := h.containerService.GetContainerImageByID(c.Request.Context(), req.ID)
	if err != nil {
		handleDataError(c, err, "failed to get container image")
		return
	}

	c.JSON(http.StatusOK, item)
}

// UpdateContainerImage godoc
// @Summary      更新容器镜像
// @Description  按 ID 更新 ContainerImage 记录
// @Tags         容器管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.ContainerImage  true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /container/image/update [post]
func (h *ContainerHandler) UpdateContainerImage(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.ContainerImage
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}
	if req.ID == 0 {
		c.Error(errors.NewValidationError("id is required"))
		return
	}

	if err := h.containerService.UpdateContainerImage(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to update container image")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "container image updated successfully"})
}

// DeleteContainerImage godoc
// @Summary      删除容器镜像
// @Description  按 ID 删除 ContainerImage 记录
// @Tags         容器管理
// @Accept       json
// @Produce      json
// @Param        request  body      idBody               true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /container/image/delete [post]
func (h *ContainerHandler) DeleteContainerImage(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idBody
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.containerService.DeleteContainerImage(c.Request.Context(), req.ID); err != nil {
		handleDataError(c, err, "failed to delete container image")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "container image deleted successfully"})
}

// ListContainerImage godoc
// @Summary      容器镜像列表
// @Description  查询 ContainerImage 列表
// @Tags         容器管理
// @Produce      json
// @Success      200      {array}   types.ContainerImage
// @Failure      401      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /container/image/list [get]
func (h *ContainerHandler) ListContainerImage(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	items, err := h.containerService.ListContainerImage(c.Request.Context())
	if err != nil {
		handleDataError(c, err, "failed to list container image")
		return
	}

	c.JSON(http.StatusOK, items)
}

// CreateContainerTemplate godoc
// @Summary      创建容器模板
// @Description  创建 ContainerTemplate 记录（手动校验 ImageID，不使用 GORM 关系维护）
// @Tags         容器管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.ContainerTemplate  true  "请求参数"
// @Success      200      {object}  types.ContainerTemplate
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /container/template/create [post]
func (h *ContainerHandler) CreateContainerTemplate(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.ContainerTemplate
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.containerService.CreateContainerTemplate(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to create container template")
		return
	}

	c.JSON(http.StatusOK, req)
}

// GetContainerTemplate godoc
// @Summary      获取容器模板
// @Description  按 ID 查询 ContainerTemplate 详情
// @Tags         容器管理
// @Produce      json
// @Param        id       query     integer                  true  "主键 ID"
// @Success      200      {object}  types.ContainerTemplate
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /container/template/get [get]
func (h *ContainerHandler) GetContainerTemplate(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idQuery
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid query parameters").WithDetails(err.Error()))
		return
	}

	item, err := h.containerService.GetContainerTemplateByID(c.Request.Context(), req.ID)
	if err != nil {
		handleDataError(c, err, "failed to get container template")
		return
	}

	c.JSON(http.StatusOK, item)
}

// UpdateContainerTemplate godoc
// @Summary      更新容器模板
// @Description  按 ID 更新 ContainerTemplate 记录（手动校验 ImageID，不使用 GORM 关系维护）
// @Tags         容器管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.ContainerTemplate  true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /container/template/update [post]
func (h *ContainerHandler) UpdateContainerTemplate(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.ContainerTemplate
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}
	if req.ID == 0 {
		c.Error(errors.NewValidationError("id is required"))
		return
	}

	if err := h.containerService.UpdateContainerTemplate(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to update container template")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "container template updated successfully"})
}

// DeleteContainerTemplate godoc
// @Summary      删除容器模板
// @Description  按 ID 删除 ContainerTemplate 记录
// @Tags         容器管理
// @Accept       json
// @Produce      json
// @Param        request  body      idBody                 true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /container/template/delete [post]
func (h *ContainerHandler) DeleteContainerTemplate(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idBody
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.containerService.DeleteContainerTemplate(c.Request.Context(), req.ID); err != nil {
		handleDataError(c, err, "failed to delete container template")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "container template deleted successfully"})
}

// ListContainerTemplate godoc
// @Summary      容器模板列表
// @Description  查询 ContainerTemplate 列表
// @Tags         容器管理
// @Produce      json
// @Success      200      {array}   types.ContainerTemplate
// @Failure      401      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /container/template/list [get]
func (h *ContainerHandler) ListContainerTemplate(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	items, err := h.containerService.ListContainerTemplate(c.Request.Context())
	if err != nil {
		handleDataError(c, err, "failed to list container template")
		return
	}

	c.JSON(http.StatusOK, items)
}
