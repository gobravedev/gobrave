package handler

import (
	"encoding/json"
	stderrs "errors"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"github.com/gobravedev/gobrave/internal/config"
	"github.com/gobravedev/gobrave/internal/errors"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"github.com/gobravedev/gobrave/internal/utils"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type WorkflowHandler struct {
	workflowService interfaces.WorkflowService
	dataService     interfaces.DataService
	cfg             *config.Config
}

type WorkflowFormJSONResponse struct {
	Type     string        `json:"type"`
	FormJSON []interface{} `json:"formJson"`
}

type WorkflowFormResponse struct {
	Type           string                 `json:"type"`
	FormJSON       []interface{}          `json:"formJson"`
	AnalysisResult map[string]interface{} `json:"analysis_result"`
}

type workflowProjectQuery struct {
	ProjectID string `form:"projectId" binding:"required"`
}

type createScriptRequest struct {
	ScriptID            string `json:"component_id"`
	InstallKey          string `json:"install_key"`
	ComponentName       string `json:"component_name"`
	Description         string `json:"description"`
	ComponentIDs        string `json:"component_ids"`
	Img                 string `json:"img"`
	ContainerTemplateID int64  `json:"container_template_id,string"`
	ToolsContainerID    string `json:"tools_container_id"`
	Prompt              string `json:"prompt"`
	IOSchema            string `json:"io_schema"`
	SubContainerID      string `json:"sub_container_id"`
	Tags                string `json:"tags"`
	FileType            string `json:"file_type"`
	ScriptType          string `json:"script_type"`
	Category            string `json:"category"`
	Content             string `json:"content"`
	OrderIndex          int    `json:"order_index"`
	Position            string `json:"position"`
	Edges               string `json:"edges"`
}

func NewWorkflowHandler(workflowService interfaces.WorkflowService,
	dataService interfaces.DataService, cfg *config.Config) *WorkflowHandler {
	return &WorkflowHandler{workflowService: workflowService, dataService: dataService, cfg: cfg}
}

// CreateScript godoc
// @Summary      创建脚本组件
// @Description  在 pipeline_components 中创建 script 组件（兼容 Python save-pipeline 的 script 语义）
// @Tags         工作流
// @Accept       json
// @Produce      json
// @Param        request  body      handler.createScriptRequest  true  "请求参数"
// @Success      200      {object}  types.Script
// @Failure      400      {object}  errors.AppError
// @Failure      401      {object}  errors.AppError
// @Failure      500      {object}  errors.AppError
// @Security     Bearer
// @Router       /workflow/createscript [post]
func (h *WorkflowHandler) CreateScript(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req createScriptRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request parameters").WithDetails(err.Error()))
		return
	}

	if req.Content != "" && !json.Valid([]byte(req.Content)) {
		c.Error(errors.NewValidationError("content is not valid JSON format"))
		return
	}
	if req.IOSchema != "" && !json.Valid([]byte(req.IOSchema)) {
		c.Error(errors.NewValidationError("io_schema is not valid JSON format"))
		return
	}

	scriptID := req.ScriptID
	if scriptID == "" {
		scriptID = uuid.NewString()
	}

	item := &types.Script{
		ScriptID:            scriptID,
		InstallKey:          req.InstallKey,
		ComponentType:       "script",
		ComponentName:       req.ComponentName,
		Description:         req.Description,
		ComponentIDs:        req.ComponentIDs,
		Img:                 req.Img,
		ContainerTemplateID: req.ContainerTemplateID,
		ToolsContainerID:    req.ToolsContainerID,
		Prompt:              req.Prompt,
		IOSchema:            req.IOSchema,
		SubContainerID:      req.SubContainerID,
		Tags:                req.Tags,
		FileType:            req.FileType,
		ScriptType:          req.ScriptType,
		Category:            req.Category,
		Content:             req.Content,
		OrderIndex:          req.OrderIndex,
		Position:            req.Position,
		Edges:               req.Edges,
	}

	if err := h.workflowService.CreateScript(c.Request.Context(), item); err != nil {
		c.Error(errors.NewInternalServerError("failed to create script").WithDetails(err.Error()))
		return
	}

	scriptDir, _, _ := utils.GetScriptFile(item.ScriptType, item.ScriptID)
	ioSchemaFile := filepath.Join(h.cfg.Storage.BaseDir, scriptDir, "io_schema.json")
	// Write io_schema.json file if IOSchema is provided
	if item.IOSchema != "" {
		if err := os.MkdirAll(filepath.Dir(ioSchemaFile), 0o755); err != nil {
			c.Error(errors.NewInternalServerError("failed to prepare script directory").WithDetails(err.Error()))
			return
		}
		if err := os.WriteFile(ioSchemaFile, []byte(item.IOSchema), 0o644); err != nil {
			c.Error(errors.NewInternalServerError("failed to write io_schema file").WithDetails(err.Error()))
			return
		}
	}

	c.JSON(http.StatusOK, item)
}

// GetFromJSONByRelationID godoc
// @Summary      获取工作流表单配置
// @Description  根据 workflowId 解析工作流 DAG，聚合输入/参数/formJson 配置
// @Tags         工作流
// @Produce      json
// @Param        workflowId  path      string                    true  "工作流 ID"
// @Success      200         {object}  handler.WorkflowFormJSONResponse
// @Failure      400         {object}  errors.AppError
// @Failure      401         {object}  errors.AppError
// @Failure      404         {object}  errors.AppError
// @Failure      500         {object}  errors.AppError
// @Security     Bearer
// @Router       /workflow/tools/get-from-json/{workflowId} [get]
func (h *WorkflowHandler) GetFromJSONByWorlflow(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	workflowID := c.Param("workflowId")
	if workflowID == "" {
		c.Error(errors.NewValidationError("workflowId is required"))
		return
	}

	formJSONWrap, err := h.workflowService.GetFormJSONByWorkflowID(c.Request.Context(), workflowID)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("workflow not found"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to get form json by workflow id").WithDetails(err.Error()))
		return
	}

	c.JSON(http.StatusOK, WorkflowFormJSONResponse{
		Type:     "tools",
		FormJSON: formJSONWrap,
	})
}

// GetWorkflowForm godoc
// @Summary      获取工作流表单与分析数据
// @Description  基于 workflowId 返回 formJson，并按 input_type 自动补充 analysis_result（sample 与按 role 分组的文件）
// @Tags         工作流
// @Produce      json
// @Param        workflowId  path      string                       true  "工作流 ID"
// @Param        projectId   query     string                       true  "项目业务ID"
// @Success      200         {object}  handler.WorkflowFormResponse
// @Failure      400         {object}  errors.AppError
// @Failure      401         {object}  errors.AppError
// @Failure      404         {object}  errors.AppError
// @Failure      500         {object}  errors.AppError
// @Security     Bearer
// @Router       /workflows/{workflowId}/form [get]
func (h *WorkflowHandler) GetWorkflowForm(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	workflowID := c.Param("workflowId")
	if workflowID == "" {
		c.Error(errors.NewValidationError("workflowId is required"))
		return
	}

	var req workflowProjectQuery
	if err := c.ShouldBindQuery(&req); err != nil {
		c.Error(errors.NewValidationError("invalid query parameters").WithDetails(err.Error()))
		return
	}

	formJSONWrap, analysisResult, err := buildWorkflowFormData(c.Request.Context(), h.workflowService, h.dataService, workflowID, req.ProjectID)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("workflow not found"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to get workflow form").WithDetails(err.Error()))
		return
	}

	c.JSON(http.StatusOK, WorkflowFormResponse{
		Type:           "tools",
		FormJSON:       formJSONWrap,
		AnalysisResult: analysisResult,
	})
}

func buildCompatSampleItem(item interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, err
	}

	result["label"] = result["sample_name"]
	result["value"] = result["id"]

	return result, nil
}

func extractStringList(v interface{}) []string {
	if v == nil {
		return nil
	}

	if values, ok := v.([]string); ok {
		return values
	}

	items, ok := v.([]interface{})
	if !ok {
		return nil
	}

	result := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok && s != "" {
			result = append(result, s)
		}
	}
	return result
}
