package handler

import (
	"encoding/csv"
	"encoding/json"
	stderrs "errors"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/gobravedev/gobrave/internal/errors"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/gorm"
)

type DataHandler struct {
	dataService interfaces.DataService
}

func NewDataHandler(dataService interfaces.DataService) *DataHandler {
	return &DataHandler{dataService: dataService}
}

type idQuery struct {
	ID int64 `form:"id" binding:"required"`
}

type idBody struct {
	ID int64 `json:"id,string" binding:"required"`
}

type projectIDQuery struct {
	ProjectID string `form:"project_id" binding:"required"`
}

func handleDataError(c *gin.Context, err error, internalMsg string) {
	if stderrs.Is(err, gorm.ErrRecordNotFound) {
		c.Error(errors.NewNotFoundError("record not found"))
		return
	}
	c.Error(errors.NewInternalServerError(internalMsg).WithDetails(err.Error()))
}

// buildFileColumns reads the TSV header at path and returns a column list
// compatible with the Python build_collected_analysis_result output.
func buildFileColumns(path string, fileID int64) ([]map[string]interface{}, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = '\t'
	r.LazyQuotes = true
	r.FieldsPerRecord = -1

	header, err := r.Read()
	if err != nil {
		return nil, err
	}

	columns := make([]map[string]interface{}, 0, len(header))
	for _, col := range header {
		columns = append(columns, map[string]interface{}{
			"id":                 fileID,
			"analysis_result_id": fileID,
			"sample_name":        col,
			"columns_name":       col,
		})
	}
	return columns, nil
}

func buildCompatFileItem(item *types.FileWithDatasetInfo) (map[string]interface{}, error) {
	b, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, err
	}

	if v, ok := result["path"]; ok {
		result["content"] = v
		result["label"] = result["file_name"] // 保持 label 字段兼容
		result["value"] = result["id"]        // 保持 value 字段兼容
		delete(result, "path")
	}

	// For EXP/TABLE roles, read TSV header and attach columns (mirrors Python build_collected_analysis_result).
	if (item.Role == "EXP" || item.Role == "TABLE") && item.Path != "" {
		if cols, err := buildFileColumns(item.Path, item.ID); err == nil {
			result["columns"] = cols
		}
		// silently skip if file is missing or unreadable
	}

	return result, nil
}

// CreateDataset godoc
// @Summary      创建数据集
// @Description  创建 Dataset 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.Dataset     true  "请求参数"
// @Success      200      {object}  types.Dataset
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset/create [post]
func (h *DataHandler) CreateDataset(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.Dataset
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.dataService.CreateDataset(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to create dataset")
		return
	}

	c.JSON(http.StatusOK, req)
}

// GetDataset godoc
// @Summary      获取数据集
// @Description  按 ID 查询 Dataset 详情
// @Tags         数据管理
// @Produce      json
// @Param        id       query     integer           true  "主键 ID"
// @Success      200      {object}  types.Dataset
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset/get [get]
func (h *DataHandler) GetDataset(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idQuery
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid query parameters").WithDetails(err.Error()))
		return
	}

	item, err := h.dataService.GetDatasetByID(c.Request.Context(), req.ID)
	if err != nil {
		handleDataError(c, err, "failed to get dataset")
		return
	}

	c.JSON(http.StatusOK, item)
}

// UpdateDataset godoc
// @Summary      更新数据集
// @Description  按 ID 更新 Dataset 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.Dataset     true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset/update [post]
func (h *DataHandler) UpdateDataset(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.Dataset
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}
	if req.ID == 0 {
		c.Error(errors.NewValidationError("id is required"))
		return
	}

	if err := h.dataService.UpdateDataset(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to update dataset")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "dataset updated successfully"})
}

// DeleteDataset godoc
// @Summary      删除数据集
// @Description  按 ID 删除 Dataset，并手动清理关联映射关系
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      idBody            true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset/delete [post]
func (h *DataHandler) DeleteDataset(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idBody
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.dataService.DeleteDataset(c.Request.Context(), req.ID); err != nil {
		handleDataError(c, err, "failed to delete dataset")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "dataset deleted successfully"})
}

// ListDataset godoc
// @Summary      数据集列表
// @Description  查询 Dataset 列表
// @Tags         数据管理
// @Produce      json
// @Success      200      {array}   types.Dataset
// @Failure      401      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset/list [get]
func (h *DataHandler) ListDataset(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	items, err := h.dataService.ListDataset(c.Request.Context())
	if err != nil {
		handleDataError(c, err, "failed to list dataset")
		return
	}

	c.JSON(http.StatusOK, items)
}

// CreateProjectDataset godoc
// @Summary      创建项目-数据集映射
// @Description  创建 ProjectDataset 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.ProjectDataset  true  "请求参数"
// @Success      200      {object}  types.ProjectDataset
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/project-dataset/create [post]
func (h *DataHandler) CreateProjectDataset(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.ProjectDataset
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.dataService.CreateProjectDataset(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to create project dataset")
		return
	}

	c.JSON(http.StatusOK, req)
}

// GetProjectDataset godoc
// @Summary      获取项目-数据集映射
// @Description  按 ID 查询 ProjectDataset 详情
// @Tags         数据管理
// @Produce      json
// @Param        id       query     integer               true  "主键 ID"
// @Success      200      {object}  types.ProjectDataset
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/project-dataset/get [get]
func (h *DataHandler) GetProjectDataset(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idQuery
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid query parameters").WithDetails(err.Error()))
		return
	}

	item, err := h.dataService.GetProjectDatasetByID(c.Request.Context(), req.ID)
	if err != nil {
		handleDataError(c, err, "failed to get project dataset")
		return
	}

	c.JSON(http.StatusOK, item)
}

// UpdateProjectDataset godoc
// @Summary      更新项目-数据集映射
// @Description  按 ID 更新 ProjectDataset 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.ProjectDataset  true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/project-dataset/update [post]
func (h *DataHandler) UpdateProjectDataset(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.ProjectDataset
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}
	if req.ID == 0 {
		c.Error(errors.NewValidationError("id is required"))
		return
	}

	if err := h.dataService.UpdateProjectDataset(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to update project dataset")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "project dataset updated successfully"})
}

// DeleteProjectDataset godoc
// @Summary      删除项目-数据集映射
// @Description  按 ID 删除 ProjectDataset 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      idBody                true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/project-dataset/delete [post]
func (h *DataHandler) DeleteProjectDataset(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idBody
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.dataService.DeleteProjectDataset(c.Request.Context(), req.ID); err != nil {
		handleDataError(c, err, "failed to delete project dataset")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "project dataset deleted successfully"})
}

// ListProjectDataset godoc
// @Summary      项目-数据集映射列表
// @Description  查询 ProjectDataset 列表
// @Tags         数据管理
// @Produce      json
// @Success      200      {array}   types.ProjectDataset
// @Failure      401      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/project-dataset/list [get]
func (h *DataHandler) ListProjectDataset(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	items, err := h.dataService.ListProjectDataset(c.Request.Context())
	if err != nil {
		handleDataError(c, err, "failed to list project dataset")
		return
	}

	c.JSON(http.StatusOK, items)
}

// CreateFile godoc
// @Summary      创建文件
// @Description  创建 File 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.File        true  "请求参数"
// @Success      200      {object}  types.File
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/file/create [post]
func (h *DataHandler) CreateFile(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.File
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.dataService.CreateFile(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to create file")
		return
	}

	c.JSON(http.StatusOK, req)
}

// GetFile godoc
// @Summary      获取文件
// @Description  按 ID 查询 File 详情
// @Tags         数据管理
// @Produce      json
// @Param        id       query     integer           true  "主键 ID"
// @Success      200      {object}  types.File
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/file/get [get]
func (h *DataHandler) GetFile(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idQuery
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid query parameters").WithDetails(err.Error()))
		return
	}

	item, err := h.dataService.GetFileByID(c.Request.Context(), req.ID)
	if err != nil {
		handleDataError(c, err, "failed to get file")
		return
	}

	c.JSON(http.StatusOK, item)
}

// UpdateFile godoc
// @Summary      更新文件
// @Description  按 ID 更新 File 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.File        true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/file/update [post]
func (h *DataHandler) UpdateFile(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.File
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}
	if req.ID == 0 {
		c.Error(errors.NewValidationError("id is required"))
		return
	}

	if err := h.dataService.UpdateFile(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to update file")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "file updated successfully"})
}

// DeleteFile godoc
// @Summary      删除文件
// @Description  按 ID 删除 File，并手动清理关联映射关系
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      idBody            true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/file/delete [post]
func (h *DataHandler) DeleteFile(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idBody
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.dataService.DeleteFile(c.Request.Context(), req.ID); err != nil {
		handleDataError(c, err, "failed to delete file")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "file deleted successfully"})
}

// ListFile godoc
// @Summary      文件列表
// @Description  查询 File 列表
// @Tags         数据管理
// @Produce      json
// @Success      200      {array}   types.File
// @Failure      401      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/file/list [get]
func (h *DataHandler) ListFile(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	items, err := h.dataService.ListFile(c.Request.Context())
	if err != nil {
		handleDataError(c, err, "failed to list file")
		return
	}

	c.JSON(http.StatusOK, items)
}

// ListFileByProjectID godoc
// @Summary      按项目查询文件列表
// @Description  根据 project_id 查询关联的所有文件
// @Tags         数据管理
// @Produce      json
// @Param        project_id  query     string           true  "项目业务ID"
// @Success      200         {array}   types.FileWithDatasetInfo
// @Failure      400         {object}  errors.AppError
// @Failure      401         {object}  errors.AppError
// @Failure      500         {object}  errors.AppError
// @Security     Bearer
// @Router       /data/file/list-by-project [get]
func (h *DataHandler) ListFileByProjectID(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req projectIDQuery
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid query parameters").WithDetails(err.Error()))
		return
	}

	items, err := h.dataService.ListFileByProjectID(c.Request.Context(), req.ProjectID)
	if err != nil {
		handleDataError(c, err, "failed to list file by project id")
		return
	}

	result := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		compatItem, err := buildCompatFileItem(item)
		if err != nil {
			handleDataError(c, err, "failed to build file response")
			return
		}
		result = append(result, compatItem)
	}

	c.JSON(http.StatusOK, result)
}

// ListFileByProjectIDGroupByRole godoc
// @Summary      按项目查询文件列表并按角色分组
// @Description  根据 project_id 查询关联文件，并按 role 分组
// @Tags         数据管理
// @Produce      json
// @Param        project_id  query     string                      true  "项目业务ID"
// @Success      200         {object}  map[string][]types.FileWithDatasetInfo
// @Failure      400         {object}  errors.AppError
// @Failure      401         {object}  errors.AppError
// @Failure      500         {object}  errors.AppError
// @Security     Bearer
// @Router       /data/file/list-by-project-group [get]
func (h *DataHandler) ListFileByProjectIDGroupByRole(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req projectIDQuery
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid query parameters").WithDetails(err.Error()))
		return
	}

	items, err := h.dataService.ListFileByProjectIDGroupByRole(c.Request.Context(), req.ProjectID)
	if err != nil {
		handleDataError(c, err, "failed to list grouped file by project id")
		return
	}

	result := make(map[string]interface{}, len(items))
	for _, group := range items {
		groupItems := make([]map[string]interface{}, 0, len(group.Items))
		for _, item := range group.Items {
			compatItem, err := buildCompatFileItem(item)
			if err != nil {
				handleDataError(c, err, "failed to build grouped file response")
				return
			}
			groupItems = append(groupItems, compatItem)
		}
		result[group.Role] = groupItems
	}

	c.JSON(http.StatusOK, result)
}

// CreateDatasetFile godoc
// @Summary      创建数据集-文件映射
// @Description  创建 DatasetFile 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.DatasetFile  true  "请求参数"
// @Success      200      {object}  types.DatasetFile
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset-file/create [post]
func (h *DataHandler) CreateDatasetFile(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.DatasetFile
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.dataService.CreateDatasetFile(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to create dataset file")
		return
	}

	c.JSON(http.StatusOK, req)
}

// GetDatasetFile godoc
// @Summary      获取数据集-文件映射
// @Description  按 ID 查询 DatasetFile 详情
// @Tags         数据管理
// @Produce      json
// @Param        id       query     integer             true  "主键 ID"
// @Success      200      {object}  types.DatasetFile
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset-file/get [get]
func (h *DataHandler) GetDatasetFile(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idQuery
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid query parameters").WithDetails(err.Error()))
		return
	}

	item, err := h.dataService.GetDatasetFileByID(c.Request.Context(), req.ID)
	if err != nil {
		handleDataError(c, err, "failed to get dataset file")
		return
	}

	c.JSON(http.StatusOK, item)
}

// UpdateDatasetFile godoc
// @Summary      更新数据集-文件映射
// @Description  按 ID 更新 DatasetFile 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.DatasetFile   true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset-file/update [post]
func (h *DataHandler) UpdateDatasetFile(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.DatasetFile
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}
	if req.ID == 0 {
		c.Error(errors.NewValidationError("id is required"))
		return
	}

	if err := h.dataService.UpdateDatasetFile(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to update dataset file")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "dataset file updated successfully"})
}

// DeleteDatasetFile godoc
// @Summary      删除数据集-文件映射
// @Description  按 ID 删除 DatasetFile 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      idBody              true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset-file/delete [post]
func (h *DataHandler) DeleteDatasetFile(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idBody
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.dataService.DeleteDatasetFile(c.Request.Context(), req.ID); err != nil {
		handleDataError(c, err, "failed to delete dataset file")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "dataset file deleted successfully"})
}

// ListDatasetFile godoc
// @Summary      数据集-文件映射列表
// @Description  查询 DatasetFile 列表
// @Tags         数据管理
// @Produce      json
// @Success      200      {array}   types.DatasetFile
// @Failure      401      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset-file/list [get]
func (h *DataHandler) ListDatasetFile(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	items, err := h.dataService.ListDatasetFile(c.Request.Context())
	if err != nil {
		handleDataError(c, err, "failed to list dataset file")
		return
	}

	c.JSON(http.StatusOK, items)
}

// CreateSample godoc
// @Summary      创建样本
// @Description  创建 Sample 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.Sample      true  "请求参数"
// @Success      200      {object}  types.Sample
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/sample/create [post]
func (h *DataHandler) CreateSample(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.Sample
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.dataService.CreateSample(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to create sample")
		return
	}

	c.JSON(http.StatusOK, req)
}

// GetSample godoc
// @Summary      获取样本
// @Description  按 ID 查询 Sample 详情
// @Tags         数据管理
// @Produce      json
// @Param        id       query     integer           true  "主键 ID"
// @Success      200      {object}  types.Sample
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/sample/get [get]
func (h *DataHandler) GetSample(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idQuery
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid query parameters").WithDetails(err.Error()))
		return
	}

	item, err := h.dataService.GetSampleByID(c.Request.Context(), req.ID)
	if err != nil {
		handleDataError(c, err, "failed to get sample")
		return
	}

	c.JSON(http.StatusOK, item)
}

// UpdateSample godoc
// @Summary      更新样本
// @Description  按 ID 更新 Sample 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.Sample      true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/sample/update [post]
func (h *DataHandler) UpdateSample(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.Sample
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}
	if req.ID == 0 {
		c.Error(errors.NewValidationError("id is required"))
		return
	}

	if err := h.dataService.UpdateSample(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to update sample")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "sample updated successfully"})
}

// DeleteSample godoc
// @Summary      删除样本
// @Description  按 ID 删除 Sample，并手动清理关联映射关系
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      idBody            true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/sample/delete [post]
func (h *DataHandler) DeleteSample(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idBody
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.dataService.DeleteSample(c.Request.Context(), req.ID); err != nil {
		handleDataError(c, err, "failed to delete sample")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "sample deleted successfully"})
}

// ListSample godoc
// @Summary      样本列表
// @Description  查询 Sample 列表
// @Tags         数据管理
// @Produce      json
// @Success      200      {array}   types.Sample
// @Failure      401      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/sample/list [get]
func (h *DataHandler) ListSample(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	items, err := h.dataService.ListSample(c.Request.Context())
	if err != nil {
		handleDataError(c, err, "failed to list sample")
		return
	}

	c.JSON(http.StatusOK, items)
}

// CreateDatasetSample godoc
// @Summary      创建数据集-样本映射
// @Description  创建 DatasetSample 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.DatasetSample  true  "请求参数"
// @Success      200      {object}  types.DatasetSample
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset-sample/create [post]
func (h *DataHandler) CreateDatasetSample(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.DatasetSample
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.dataService.CreateDatasetSample(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to create dataset sample")
		return
	}

	c.JSON(http.StatusOK, req)
}

// GetDatasetSample godoc
// @Summary      获取数据集-样本映射
// @Description  按 ID 查询 DatasetSample 详情
// @Tags         数据管理
// @Produce      json
// @Param        id       query     integer               true  "主键 ID"
// @Success      200      {object}  types.DatasetSample
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset-sample/get [get]
func (h *DataHandler) GetDatasetSample(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idQuery
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid query parameters").WithDetails(err.Error()))
		return
	}

	item, err := h.dataService.GetDatasetSampleByID(c.Request.Context(), req.ID)
	if err != nil {
		handleDataError(c, err, "failed to get dataset sample")
		return
	}

	c.JSON(http.StatusOK, item)
}

// UpdateDatasetSample godoc
// @Summary      更新数据集-样本映射
// @Description  按 ID 更新 DatasetSample 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      types.DatasetSample   true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset-sample/update [post]
func (h *DataHandler) UpdateDatasetSample(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req types.DatasetSample
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}
	if req.ID == 0 {
		c.Error(errors.NewValidationError("id is required"))
		return
	}

	if err := h.dataService.UpdateDatasetSample(c.Request.Context(), &req); err != nil {
		handleDataError(c, err, "failed to update dataset sample")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "dataset sample updated successfully"})
}

// DeleteDatasetSample godoc
// @Summary      删除数据集-样本映射
// @Description  按 ID 删除 DatasetSample 记录
// @Tags         数据管理
// @Accept       json
// @Produce      json
// @Param        request  body      idBody                true  "请求参数"
// @Success      200      {object}  map[string]string
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      404      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset-sample/delete [post]
func (h *DataHandler) DeleteDatasetSample(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req idBody
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if err := h.dataService.DeleteDatasetSample(c.Request.Context(), req.ID); err != nil {
		handleDataError(c, err, "failed to delete dataset sample")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "dataset sample deleted successfully"})
}

// ListDatasetSample godoc
// @Summary      数据集-样本映射列表
// @Description  查询 DatasetSample 列表
// @Tags         数据管理
// @Produce      json
// @Success      200      {array}   types.DatasetSample
// @Failure      401      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /data/dataset-sample/list [get]
func (h *DataHandler) ListDatasetSample(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	items, err := h.dataService.ListDatasetSample(c.Request.Context())
	if err != nil {
		handleDataError(c, err, "failed to list dataset sample")
		return
	}

	c.JSON(http.StatusOK, items)
}
