package handler

import (
	"encoding/csv"
	"encoding/json"
	stderrs "errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gobravedev/gobrave/internal/compiler"
	"github.com/gobravedev/gobrave/internal/config"
	"github.com/gobravedev/gobrave/internal/errors"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type AnalysisHandler struct {
	analysisService  interfaces.AnalysisService
	workflowService  interfaces.WorkflowService
	dataService      interfaces.DataService
	containerService interfaces.ContainerService
	config           *config.Config
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

type VisualizationResultResponse struct {
	Images []map[string]interface{} `json:"images"`
	Tables []map[string]interface{} `json:"tables"`
	HTMLs  []map[string]interface{} `json:"htmls"`
}

type VisualizationNodeFileResponse struct {
	Node         map[string]interface{}      `json:"node"`
	Result       VisualizationResultResponse `json:"result"`
	Status       string                      `json:"status"`
	ServerStatus string                      `json:"server_status"`
}

type analysisControllerRequest struct {
	RequestParam   map[string]interface{} `json:"request_param" binding:"required"`
	AnalysisNodeID string                 `json:"analysis_node_id"`
	Save           bool                   `json:"save"`
	IsSubmit       bool                   `json:"is_submit"`
	IsReport       bool                   `json:"is_report"`
}

func NewAnalysisHandler(analysisService interfaces.AnalysisService, workflowService interfaces.WorkflowService, dataService interfaces.DataService, containerService interfaces.ContainerService, cfg *config.Config) *AnalysisHandler {
	return &AnalysisHandler{
		analysisService:  analysisService,
		workflowService:  workflowService,
		dataService:      dataService,
		containerService: containerService,
		config:           cfg,
	}
}

func (h *AnalysisHandler) ParseParams(c *gin.Context) {
	params := make(map[string]interface{})
	if err := c.ShouldBindJSON(&params); err != nil {
		c.Error(errors.NewValidationError("invalid request body").WithDetails(err.Error()))
		return
	}
	requestParam, ok := params["request_param"].(map[string]interface{})
	if !ok {
		c.Error(errors.NewValidationError("request_param is required and must be an object"))
		return
	}
	workflowID, ok := requestParam["relation_id"].(string)
	if !ok || strings.TrimSpace(workflowID) == "" {
		c.Error(errors.NewValidationError("request_param.relation_id is required and must be a string"))
		return
	}
	formJSONWrap, err := h.workflowService.GetFormJSONByWorkflowID(c.Request.Context(), workflowID)
	if err != nil {
		c.Error(errors.NewInternalServerError("failed to get form JSON").WithDetails(err.Error()))
		return
	}
	parseAnalysisResult, err := buildParseAnalysisResult(c.Request.Context(), h.dataService, requestParam, formJSONWrap)
	if err != nil {
		c.Error(errors.NewInternalServerError("failed to build parse analysis result").WithDetails(err.Error()))
		return
	}
	dagDefinition, err := h.workflowService.GetWorkflowVisByWorkflowID(c.Request.Context(), workflowID)
	if err != nil {
		c.Error(errors.NewInternalServerError("failed to get workflow visualization").WithDetails(err.Error()))
		return
	}

	analysisID, _ := requestParam["analysis_id"].(string)
	if strings.TrimSpace(analysisID) == "" {
		analysisID = "preview"
	}

	runtimeDAG, err := compiler.BuildRuntimeTasks(analysisID, parseAnalysisResult, dagDefinition)
	if err != nil {
		c.Error(errors.NewInternalServerError("failed to compile runtime dag").WithDetails(err.Error()))
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"form_json":             formJSONWrap,
		"dag_definition":        dagDefinition,
		"parse_analysis_result": parseAnalysisResult,
		"params":                parseAnalysisResult,
		"analysis_nodes":        runtimeDAG["analysis_nodes"],
		"analysis_edges":        runtimeDAG["analysis_edges"],
	})
}

func (h *AnalysisHandler) SaveAnalysisController(c *gin.Context) {
	if _, ok := getCurrentUserID(c); !ok {
		return
	}

	var req analysisControllerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(errors.NewValidationError("invalid request body").WithDetails(err.Error()))
		return
	}

	workflowID, ok := req.RequestParam["relation_id"].(string)
	if !ok || strings.TrimSpace(workflowID) == "" {
		c.Error(errors.NewValidationError("request_param.relation_id is required and must be a string"))
		return
	}

	formJSONWrap, err := h.workflowService.GetFormJSONByWorkflowID(c.Request.Context(), workflowID)
	if err != nil {
		c.Error(errors.NewInternalServerError("failed to get form JSON").WithDetails(err.Error()))
		return
	}

	parseAnalysisResult, err := buildParseAnalysisResult(c.Request.Context(), h.dataService, req.RequestParam, formJSONWrap)
	if err != nil {
		c.Error(errors.NewInternalServerError("failed to build parse analysis result").WithDetails(err.Error()))
		return
	}

	dagDefinition, err := h.workflowService.GetWorkflowVisByWorkflowID(c.Request.Context(), workflowID)
	if err != nil {
		c.Error(errors.NewInternalServerError("failed to get workflow visualization").WithDetails(err.Error()))
		return
	}

	// isRunNode := strings.TrimSpace(req.AnalysisNodeID) != ""
	// dagRuntime := map[string]any{}
	// if !isRunNode {
	// 	analysisID, _ := req.RequestParam["analysis_id"].(string)
	// 	if strings.TrimSpace(analysisID) == "" {
	// 		analysisID = "preview"
	// 	}
	// 	dagRuntime, err = compiler.BuildRuntimeTasks(analysisID, parseAnalysisResult, dagDefinition)
	// 	if err != nil {
	// 		c.Error(errors.NewInternalServerError("failed to compile runtime dag").WithDetails(err.Error()))
	// 		return
	// 	}
	// }
	analysisID := strings.TrimSpace(fmt.Sprintf("%v", req.RequestParam["analysis_id"]))
	if analysisID == "" || analysisID == "<nil>" {
		if req.Save {
			analysisID = uuid.NewString()
			req.RequestParam["analysis_id"] = analysisID
		} else {
			analysisID = "preview"
		}
	}

	dagRuntime, err := compiler.BuildRuntimeTasks(analysisID, parseAnalysisResult, dagDefinition)
	if err != nil {
		c.Error(errors.NewInternalServerError("failed to compile runtime dag").WithDetails(err.Error()))
		return
	}
	if !req.Save {
		c.JSON(http.StatusOK, gin.H{
			"params":      parseAnalysisResult,
			"dag_runtime": dagRuntime,
		})
		return
	}

	saved, err := h.analysisService.SaveAnalysisController(c.Request.Context(), &types.AnalysisControllerSaveInput{
		RequestParam:        req.RequestParam,
		ParseAnalysisResult: parseAnalysisResult,
		DagRuntime:          dagRuntime,
		IsRunNode:           req.IsSubmit, // 只有提交才会真正执行节点，否则仅保存参数和编译结果
		IsReport:            req.IsReport,
	})
	if err != nil {
		c.Error(errors.NewInternalServerError("failed to save analysis").WithDetails(err.Error()))
		return
	}

	response := gin.H{
		"analysis_id":           saved.AnalysisID,
		"dag_definition":        dagDefinition,
		"parse_analysis_result": parseAnalysisResult,
		"params":                parseAnalysisResult,
		"dag_runtime":           dagRuntime,
	}

	if req.IsSubmit {
		response["submit_skipped"] = true
	}

	c.JSON(http.StatusOK, response)
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
		analysisItem.ProjectID,
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

	scriptItem, err := h.workflowService.GetScriptByScriptID(c.Request.Context(), analysisNode.ScriptID)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			c.Error(errors.NewNotFoundError("script not found"))
			return
		}
		c.Error(errors.NewInternalServerError("failed to get script").WithDetails(err.Error()))
		return
	}

	formJSON, err := buildNodeFormJSON(workflowItem.DagDefinition, scriptItem, analysisNode.ScriptID)
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

// VisualizationNodeFile godoc
// @Summary      分析节点结果文件可视化
// @Description  查询 AnalysisNode 并补充容器模板与镜像信息，同时返回 output_dir 下可视化资源列表
// @Tags         分析
// @Produce      json
// @Param        analysisNodeId  path      string                                 true  "分析节点 ID"
// @Success      200             {object}  handler.VisualizationNodeFileResponse
// @Failure      400             {object}  errors.AppError
// @Failure      401             {object}  errors.AppError
// @Failure      404             {object}  errors.AppError
// @Failure      500             {object}  errors.AppError
// @Security     Bearer
// @Router       /analysis/visualization-node-file/{analysisNodeId} [get]
func (h *AnalysisHandler) VisualizationNodeFile(c *gin.Context) {
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

	nodePayload, err := h.buildVisualizationNodePayload(c, analysisNode)
	if err != nil {
		c.Error(errors.NewInternalServerError("failed to build visualization node payload").WithDetails(err.Error()))
		return
	}

	result, err := visualizationResultsPath(analysisNode.OutputDir, h.config)
	if err != nil {
		c.Error(errors.NewInternalServerError("failed to list visualization files").WithDetails(err.Error()))
		return
	}

	c.JSON(http.StatusOK, VisualizationNodeFileResponse{
		Node:         nodePayload,
		Result:       result,
		Status:       analysisNode.Status,
		ServerStatus: analysisNode.ServerStatus,
	})
}

func (h *AnalysisHandler) buildVisualizationNodePayload(c *gin.Context, analysisNode *types.AnalysisNode) (map[string]interface{}, error) {
	b, err := json.Marshal(analysisNode)
	if err != nil {
		return nil, err
	}

	nodeMap := make(map[string]interface{})
	if err := json.Unmarshal(b, &nodeMap); err != nil {
		return nil, err
	}

	nodeMap["status"] = analysisNode.Status
	nodeMap["server_status"] = analysisNode.ServerStatus
	nodeMap["analysis_id"] = analysisNode.AnalysisID
	nodeMap["script_id"] = analysisNode.ScriptID

	return h.attachContainerInfoToNode(c, nodeMap)
}

func (h *AnalysisHandler) attachContainerInfoToNode(c *gin.Context, node map[string]interface{}) (map[string]interface{}, error) {
	scriptID, _ := node["script_id"].(string)
	if scriptID == "" {
		return node, nil
	}

	snapshot, err := h.workflowService.GetScriptContainerSnapshotByScriptID(c.Request.Context(), scriptID)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			return node, nil
		}
		return nil, err
	}

	containerID := snapshot.ContainerID
	node["container_id"] = strconv.FormatInt(containerID, 10)
	node["container_name"] = ""
	node["container_image"] = ""
	node["image_id"] = ""
	node["image_status"] = "pending"

	node["container_name"] = strings.TrimSpace(snapshot.ContainerName)
	if snapshot.ImageID > 0 {
		node["image_id"] = snapshot.ImageID
	}

	containerImage := strings.TrimSpace(snapshot.ContainerImage)
	if containerImage == "" {
		if strings.TrimSpace(snapshot.ImageTag) != "" {
			containerImage = snapshot.ImageName + ":" + snapshot.ImageTag
		} else {
			containerImage = snapshot.ImageName
		}
	}

	status := strings.TrimSpace(snapshot.ImageStatus)
	if strings.EqualFold(status, "ready") {
		status = "exist"
	}
	if status == "" {
		status = "pending"
	}

	node["container_image"] = containerImage
	node["image_status"] = status

	return node, nil
}

func visualizationResultsPath(path string, cfg *config.Config) (VisualizationResultResponse, error) {
	result := VisualizationResultResponse{
		Images: make([]map[string]interface{}, 0),
		Tables: make([]map[string]interface{}, 0),
		HTMLs:  make([]map[string]interface{}, 0),
	}

	path = strings.TrimSpace(path)
	if path == "" {
		return result, nil
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return result, nil
		}
		return result, err
	}

	imageGroups := make(map[string][]map[string]interface{})
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		fullPath := filepath.Join(path, filename)
		lowerName := strings.ToLower(filename)

		switch {
		case isImageFile(lowerName):
			imageItem := buildImageOutput(fullPath, cfg)
			baseName := imageGroupName(filename)
			imageGroups[baseName] = append(imageGroups[baseName], imageItem)
		case strings.HasSuffix(lowerName, ".html"):
			htmlItem := buildHTMLOutput(fullPath, cfg)
			result.HTMLs = append(result.HTMLs, htmlItem)
		case isTableFile(lowerName):
			tableItem, err := buildTableOutput(fullPath, cfg, 10)
			if err != nil {
				return result, err
			}
			result.Tables = append(result.Tables, tableItem)
		}
	}

	groupNames := make([]string, 0, len(imageGroups))
	for groupName := range imageGroups {
		groupNames = append(groupNames, groupName)
	}
	sort.Strings(groupNames)

	for _, groupName := range groupNames {
		group := imageGroups[groupName]
		if len(group) == 0 {
			continue
		}

		urls := make([]map[string]string, 0, len(group))
		for _, img := range group {
			url, _ := img["url"].(string)
			format := strings.TrimPrefix(strings.ToLower(filepath.Ext(url)), ".")
			if strings.HasSuffix(strings.ToLower(url), ".download.pdf") {
				format = "pdf"
			}
			urls = append(urls, map[string]string{
				"format": format,
				"url":    url,
			})
		}

		merged := map[string]interface{}{}
		for k, v := range group[0] {
			merged[k] = v
		}
		merged["urls"] = urls
		result.Images = append(result.Images, merged)
	}

	sort.Slice(result.Tables, func(i, j int) bool {
		a, _ := result.Tables[i]["order"].(int)
		b, _ := result.Tables[j]["order"].(int)
		return a > b
	})

	return result, nil
}

func isImageFile(name string) bool {
	return strings.HasSuffix(name, ".png") ||
		strings.HasSuffix(name, ".jpg") ||
		strings.HasSuffix(name, ".jpeg") ||
		strings.HasSuffix(name, ".pdf") ||
		strings.HasSuffix(name, ".download.pdf")
}

func isTableFile(name string) bool {
	return strings.HasSuffix(name, ".csv") ||
		strings.HasSuffix(name, ".md") ||
		strings.HasSuffix(name, ".tsv") ||
		strings.HasSuffix(name, ".txt") ||
		strings.HasSuffix(name, ".xlsx") ||
		strings.HasSuffix(name, ".info") ||
		strings.HasSuffix(name, ".vis") ||
		strings.HasSuffix(name, ".feature.list") ||
		strings.HasSuffix(name, ".diff") ||
		strings.HasSuffix(name, ".log") ||
		strings.HasSuffix(name, ".download.tsv")
}

func imageGroupName(filename string) string {
	name := strings.TrimSuffix(filename, filepath.Ext(filename))
	if strings.HasSuffix(strings.ToLower(filename), ".download.pdf") {
		name = strings.TrimSuffix(filename, ".download.pdf")
	}
	return name
}

func buildImageOutput(path string, cfg *config.Config) map[string]interface{} {
	url := buildAnalysisFileURL(path, cfg)
	filename := filepath.Base(path)
	data := url

	lowerName := strings.ToLower(path)
	if strings.HasSuffix(lowerName, ".download.pdf") {
		filename = strings.TrimSuffix(filename, ".download.pdf")
		data = strings.TrimSuffix(url, ".download.pdf") + ".png"
	} else if strings.HasSuffix(lowerName, ".pdf") {
		filename = strings.TrimSuffix(filename, ".pdf")
	} else {
		filename = strings.TrimSuffix(filename, filepath.Ext(filename))
	}

	return map[string]interface{}{
		"data":     data,
		"type":     "img",
		"filename": filename,
		"url":      url,
	}
}

func buildHTMLOutput(path string, cfg *config.Config) map[string]interface{} {
	url := buildAnalysisFileURL(path, cfg)
	return map[string]interface{}{
		"data":     url,
		"type":     "img",
		"filename": filepath.Base(path),
		"url":      url,
	}
}

func buildTableOutput(path string, cfg *config.Config, rowLimit int) (map[string]interface{}, error) {
	item := map[string]interface{}{
		"data":     "",
		"order":    0,
		"type":     "table",
		"filename": filepath.Base(path),
		"url":      buildAnalysisFileURL(path, cfg),
	}

	name := strings.ToLower(path)
	switch {
	case strings.HasSuffix(name, ".download.tsv"):
		item["data"] = []interface{}{}
		item["type"] = "download"
		return item, nil
	case strings.HasSuffix(name, ".csv"):
		tableData, err := readDelimitedTable(path, ',', rowLimit)
		if err != nil {
			return nil, err
		}
		item["data"] = tableData
		return item, nil
	case strings.HasSuffix(name, ".tsv"):
		tableData, err := readDelimitedTable(path, '\t', rowLimit)
		if err != nil {
			return nil, err
		}
		item["data"] = tableData
		return item, nil
	case strings.HasSuffix(name, ".vis"):
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var data interface{}
		if err := json.Unmarshal(raw, &data); err != nil {
			item["data"] = string(raw)
		} else {
			item["data"] = data
		}
		item["type"] = strings.TrimSuffix(filepath.Base(path), ".vis")
		return item, nil
	case strings.HasSuffix(name, ".diff"):
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var data interface{}
		if err := json.Unmarshal(raw, &data); err != nil {
			item["data"] = string(raw)
		} else {
			item["data"] = data
		}
		item["type"] = "diff"
		item["order"] = 11
		return item, nil
	case strings.HasSuffix(name, ".feature.list"):
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		item["data"] = string(content)
		item["type"] = "feature_list"
		item["order"] = 9
		return item, nil
	case strings.HasSuffix(name, ".info"):
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		item["data"] = string(content)
		item["type"] = "info"
		item["order"] = 10
		return item, nil
	case strings.HasSuffix(name, ".md"):
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		item["data"] = string(content)
		item["type"] = "md"
		item["order"] = 10
		return item, nil
	case strings.HasSuffix(name, ".xlsx"):
		item["data"] = []interface{}{}
		item["type"] = "download"
		return item, nil
	default:
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		item["data"] = string(content)
		item["type"] = "string"
		return item, nil
	}
}

func readDelimitedTable(path string, sep rune, rowLimit int) (map[string]interface{}, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = sep
	r.FieldsPerRecord = -1

	records := make([][]string, 0, rowLimit+1)
	for {
		rec, err := r.Read()
		if err != nil {
			if stderrs.Is(err, os.ErrClosed) {
				return nil, err
			}
			if stderrs.Is(err, io.EOF) {
				break
			}
			if stderrs.Is(err, csv.ErrFieldCount) {
				records = append(records, rec)
				if len(records) > rowLimit {
					break
				}
				continue
			}
			return nil, err
		}
		records = append(records, rec)
		if rowLimit > 0 && len(records) > rowLimit {
			break
		}
	}

	nrow := 0
	ncol := 0
	if len(records) > 0 {
		ncol = len(records[0])
		nrow = len(records) - 1
	}

	tables := make([][]string, 0, len(records))
	tables = append(tables, records...)

	return map[string]interface{}{
		"nrow":   nrow,
		"ncol":   ncol,
		"tables": tables,
	}, nil
}

func buildAnalysisFileURL(path string, cfg *config.Config) string {
	p := filepath.Clean(path)
	p = filepath.ToSlash(p)

	base := ""
	if cfg != nil && cfg.Storage != nil {
		base = strings.TrimSpace(cfg.Storage.BaseDir)
	}

	if base != "" {
		base = filepath.Clean(base)
		base = filepath.ToSlash(base)
		base = fmt.Sprintf("%s/analysis", base)
		if strings.HasPrefix(p, base) {
			rel := strings.TrimPrefix(p, base)
			if !strings.HasPrefix(rel, "/") {
				rel = "/" + rel
			}
			return "/images-analysis" + rel
		}
	}

	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return "/images-analysis" + p
}
