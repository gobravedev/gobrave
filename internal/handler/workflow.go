package handler

import (
	"encoding/json"
	stderrs "errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gobravedev/gobrave/internal/errors"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/gorm"
)

type WorkflowHandler struct {
	workflowService interfaces.WorkflowService
	dataService     interfaces.DataService
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

func NewWorkflowHandler(workflowService interfaces.WorkflowService, dataService interfaces.DataService) *WorkflowHandler {
	return &WorkflowHandler{workflowService: workflowService, dataService: dataService}
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
