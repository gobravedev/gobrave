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

type AnalysisHandler struct {
	analysisService interfaces.AnalysisService
	workflowService interfaces.WorkflowService
	dataService     interfaces.DataService
}

type EditParamsV2Response struct {
	AnalysisName   string                 `json:"analysis_name"`
	IsReport       bool                   `json:"is_report"`
	IsCache        bool                   `json:"is_cache"`
	AnalysisID     string                 `json:"analysis_id"`
	Status         string                 `json:"status"`
	ServerStatus   string                 `json:"server_status"`
	RequestParam   map[string]interface{} `json:"request_param"`
	AnalysisResult map[string]interface{} `json:"analysis_result"`
	FormJSON       []interface{}          `json:"formJson"`
}

type EditNodeParamsResponse struct {
	AnalysisName string                 `json:"analysis_name"`
	IsReport     bool                   `json:"is_report"`
	IsCache      bool                   `json:"is_cache"`
	AnalysisID   string                 `json:"analysis_id"`
	Status       string                 `json:"status"`
	ServerStatus string                 `json:"server_status"`
	RequestParam map[string]interface{} `json:"request_param"`
	FormJSON     []interface{}          `json:"formJson"`
}

func NewAnalysisHandler(analysisService interfaces.AnalysisService, workflowService interfaces.WorkflowService, dataService interfaces.DataService) *AnalysisHandler {
	return &AnalysisHandler{
		analysisService: analysisService,
		workflowService: workflowService,
		dataService:     dataService,
	}
}

// EditParamsV2 godoc
// @Summary      编辑分析参数（V2）
// @Description  查询 analysis 基础信息，并按 workflowId 复用工作流表单逻辑返回 formJson 与 analysis_result
// @Tags         分析
// @Produce      json
// @Param        analysisId  path      string                       true  "分析 ID"
// @Success      200         {object}  handler.EditParamsV2Response
// @Failure      400         {object}  errors.AppError
// @Failure      401         {object}  errors.AppError
// @Failure      404         {object}  errors.AppError
// @Failure      500         {object}  errors.AppError
// @Security     Bearer
// @Router       /analysis/edit-params-v2/{analysisId} [post]
func (h *AnalysisHandler) EditParamsV2(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	analysisID := c.Param("analysisId")
	if analysisID == "" {
		c.Error(errors.NewValidationError("analysisId is required"))
		return
	}

	analysisItem, err := h.analysisService.GetAnalysisByAnalysisID(c.Request.Context(), analysisID)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("analysis not found"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to get analysis").WithDetails(err.Error()))
		return
	}

	requestParam := make(map[string]interface{})
	if analysisItem.RequestParam != "" {
		if err := json.Unmarshal([]byte(analysisItem.RequestParam), &requestParam); err != nil {
			c.Error(errors.NewInternalServerError("failed to parse request_param").WithDetails(err.Error()))
			return
		}
	}

	formJSONWrap, analysisResult, err := buildWorkflowFormData(
		c.Request.Context(),
		h.workflowService,
		h.dataService,
		analysisItem.WorkflowID,
		analysisItem.Project,
	)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("workflow not found"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to build workflow form data").WithDetails(err.Error()))
		return
	}

	c.JSON(http.StatusOK, EditParamsV2Response{
		AnalysisName:   analysisItem.AnalysisName,
		IsReport:       analysisItem.IsReport,
		IsCache:        analysisItem.IsCache,
		AnalysisID:     analysisItem.AnalysisID,
		Status:         analysisItem.JobStatus,
		ServerStatus:   analysisItem.ServerStatus,
		RequestParam:   requestParam,
		AnalysisResult: analysisResult,
		FormJSON:       formJSONWrap,
	})
}

// EditNodeParams godoc
// @Summary      编辑分析节点参数
// @Description  依次查询 AnalysisNode、Analysis、Workflow(dag_definition)、Module(io_schema) 后组装 formJson
// @Tags         分析
// @Produce      json
// @Param        analysisNodeId  path      string                             true  "分析节点 ID"
// @Success      200             {object}  handler.EditNodeParamsResponse
// @Failure      400             {object}  errors.AppError
// @Failure      401             {object}  errors.AppError
// @Failure      404             {object}  errors.AppError
// @Failure      500             {object}  errors.AppError
// @Security     Bearer
// @Router       /analysis/edit-node-params/{analysisNodeId} [post]
func (h *AnalysisHandler) EditNodeParams(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	analysisNodeID := c.Param("analysisNodeId")
	if analysisNodeID == "" {
		c.Error(errors.NewValidationError("analysisNodeId is required"))
		return
	}

	analysisNode, err := h.analysisService.GetAnalysisNodeByAnalysisNodeID(c.Request.Context(), analysisNodeID)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("analysis node not found"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to get analysis node").WithDetails(err.Error()))
		return
	}

	analysisItem, err := h.analysisService.GetAnalysisByAnalysisID(c.Request.Context(), analysisNode.AnalysisID)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("analysis not found"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to get analysis").WithDetails(err.Error()))
		return
	}

	workflowItem, err := h.workflowService.GetWorkflowByWorkflowID(c.Request.Context(), analysisItem.WorkflowID)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("workflow not found"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to get workflow").WithDetails(err.Error()))
		return
	}

	moduleItem, err := h.workflowService.GetModuleByModuleID(c.Request.Context(), analysisNode.ScriptID)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("module not found"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to get module").WithDetails(err.Error()))
		return
	}

	formJSON, err := buildNodeFormJSON(workflowItem.DagDefinition, moduleItem, analysisNode.ScriptID)
	if err != nil {
		c.Error(errors.NewInternalServerError("failed to build node form json").WithDetails(err.Error()))
		return
	}

	requestParam := make(map[string]interface{})
	if analysisItem.RequestParam != "" {
		if err := json.Unmarshal([]byte(analysisItem.RequestParam), &requestParam); err != nil {
			c.Error(errors.NewInternalServerError("failed to parse request_param").WithDetails(err.Error()))
			return
		}
	}

	c.JSON(http.StatusOK, EditNodeParamsResponse{
		AnalysisName: analysisItem.AnalysisName,
		IsReport:     analysisItem.IsReport,
		IsCache:      analysisItem.IsCache,
		AnalysisID:   analysisItem.AnalysisID,
		Status:       analysisNode.Status,
		ServerStatus: analysisItem.ServerStatus,
		RequestParam: requestParam,
		FormJSON:     formJSON,
	})
}
