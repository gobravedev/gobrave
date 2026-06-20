package service

import (
	"context"
	"encoding/json"
	stderrs "errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gobravedev/gobrave/internal/config"
	dagruntime "github.com/gobravedev/gobrave/internal/dag"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type analysisService struct {
	analysisRepo interfaces.AnalysisRepository
	cfg          *config.Config
}

func NewAnalysisService(analysisRepo interfaces.AnalysisRepository, cfg *config.Config) interfaces.AnalysisService {
	return &analysisService{analysisRepo: analysisRepo, cfg: cfg}
}

func (s *analysisService) GetAnalysisByAnalysisID(ctx context.Context, analysisID string) (*types.Analysis, error) {
	return s.analysisRepo.GetAnalysisByAnalysisID(ctx, analysisID)
}

func (s *analysisService) GetAnalysisNodeByID(ctx context.Context, id int64) (*types.AnalysisNode, error) {
	return s.analysisRepo.GetAnalysisNodeByID(ctx, id)
}

func (s *analysisService) GetAnalysisNodeByAnalysisNodeID(ctx context.Context, analysisNodeID string) (*types.AnalysisNode, error) {
	return s.analysisRepo.GetAnalysisNodeByAnalysisNodeID(ctx, analysisNodeID)
}

func (s *analysisService) SaveAnalysisController(ctx context.Context, input *types.AnalysisControllerSaveInput) (*types.Analysis, error) {
	if input == nil || input.RequestParam == nil {
		return nil, fmt.Errorf("request_param is required")
	}

	workflowID := strings.TrimSpace(toString(input.RequestParam["relation_id"]))
	if workflowID == "" {
		return nil, fmt.Errorf("request_param.relation_id is required")
	}
	projectID := strings.TrimSpace(toString(input.RequestParam["project"]))
	if projectID == "" {
		return nil, fmt.Errorf("request_param.project is required")
	}
	analysisName := strings.TrimSpace(toString(input.RequestParam["analysis_name"]))
	if analysisName == "" {
		return nil, fmt.Errorf("request_param.analysis_name is required")
	}

	parseResult := input.ParseAnalysisResult
	if parseResult == nil {
		parseResult = map[string]any{}
	}

	analysisID := strings.TrimSpace(toString(input.RequestParam["analysis_id"]))
	isCache := toBool(input.RequestParam["is_cache"])
	dataComponentIDs := toString(input.RequestParam["data_component_ids"])

	var saved *types.Analysis
	err := s.analysisRepo.WithTransaction(ctx, func(tx interfaces.AnalysisRepository) error {
		var existing *types.Analysis
		if analysisID != "" {
			item, err := tx.GetAnalysisByAnalysisID(ctx, analysisID)
			if err != nil && !stderrs.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if err == nil {
				existing = item
			}
		}

		baseDir := s.resolveStorageBaseDir()
		if existing == nil && analysisID == "" {
			analysisID = uuid.NewString()
		}

		outputDir := filepath.Join(baseDir, "analysis", projectID, analysisID)
		workDir := filepath.Join(baseDir, "work", projectID, workflowID, analysisID)
		if existing != nil {
			analysisID = existing.AnalysisID
			if strings.TrimSpace(existing.OutputDir) != "" {
				outputDir = existing.OutputDir
			}
			if strings.TrimSpace(existing.WorkDir) != "" {
				workDir = existing.WorkDir
			}
		}

		if err := os.MkdirAll(outputDir, 0o755); err != nil {
			return err
		}
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			return err
		}

		paramsPath := filepath.Join(outputDir, "params.json")
		commandPath := filepath.Join(outputDir, "run.sh")
		commandLogPath := filepath.Join(outputDir, "run.log")
		traceFile := filepath.Join(outputDir, "trace.log")
		workflowLogFile := filepath.Join(outputDir, "workflow.log")
		executorLogFile := filepath.Join(outputDir, ".nextflow.log")

		parseResult["tools_output_dir"] = outputDir
		input.RequestParam["analysis_id"] = analysisID

		if err := writeJSONFile(paramsPath, parseResult); err != nil {
			return err
		}

		requestParamJSON, err := json.Marshal(input.RequestParam)
		if err != nil {
			return err
		}

		if existing != nil {
			updateValues := map[string]any{
				"project":            projectID,
				"analysis_name":      analysisName,
				"request_param":      string(requestParamJSON),
				"is_cache":           isCache,
				"relation_id":        workflowID,
				"is_report":          input.IsReport,
				"data_component_ids": dataComponentIDs,
				"output_dir":         outputDir,
				"work_dir":           workDir,
				"params_path":        paramsPath,
				"command_path":       commandPath,
				"command_log_path":   commandLogPath,
				"updated_at":         time.Now().UTC(),
			}
			if strings.TrimSpace(existing.JobStatus) != "running" {
				updateValues["job_status"] = "updated"
			}
			if err := tx.UpdateAnalysisByAnalysisID(ctx, analysisID, updateValues); err != nil {
				return err
			}
		} else {
			newAnalysis := &types.Analysis{
				ProjectID:        projectID,
				AnalysisID:       analysisID,
				WorkflowID:       workflowID,
				AnalysisName:     analysisName,
				WorkDir:          workDir,
				ParamsPath:       paramsPath,
				CommandPath:      commandPath,
				RequestParam:     string(requestParamJSON),
				OutputDir:        outputDir,
				TraceFile:        traceFile,
				WorkflowLogFile:  workflowLogFile,
				ExecutorLogFile:  executorLogFile,
				JobStatus:        "created",
				ServerStatus:     "",
				CommandLogPath:   commandLogPath,
				IsReport:         input.IsReport,
				IsCache:          isCache,
				DataComponentIDs: dataComponentIDs,
				Used:             true,
			}
			if err := tx.CreateAnalysis(ctx, newAnalysis); err != nil {
				return err
			}
		}

		savedAnalysis, err := tx.GetAnalysisByAnalysisID(ctx, analysisID)
		if err != nil {
			return err
		}

		if input.DagRuntime != nil {
			if err := s.persistDagRuntime(ctx, tx, savedAnalysis, input.DagRuntime); err != nil {
				return err
			}
		}

		saved = savedAnalysis
		return nil
	})
	if err != nil {
		return nil, err
	}

	return saved, nil
}

func (s *analysisService) persistDagRuntime(ctx context.Context, repo interfaces.AnalysisRepository, analysis *types.Analysis, dagRuntime map[string]any) error {
	nodeRows := toMapSlice(dagRuntime["analysis_nodes"])
	edgeRows := toMapSlice(dagRuntime["analysis_edges"])
	useCache := analysis != nil && analysis.IsCache

	existingNodes, err := repo.ListAnalysisNodesByAnalysisID(ctx, analysis.AnalysisID)
	if err != nil {
		return err
	}
	existingByNodeID := map[string]*types.AnalysisNode{}
	for _, item := range existingNodes {
		existingByNodeID[item.NodeID] = item
	}

	nodes := make([]*types.AnalysisNode, 0, len(nodeRows))
	for _, row := range nodeRows {
		nodeID := strings.TrimSpace(toString(row["node_id"]))
		if nodeID == "" {
			continue
		}

		existing := existingByNodeID[nodeID]
		analysisNodeID := strings.TrimSpace(toString(row["analysis_node_id"]))
		if existing != nil && strings.TrimSpace(existing.AnalysisNodeID) != "" {
			analysisNodeID = existing.AnalysisNodeID
		}
		if analysisNodeID == "" {
			analysisNodeID = "node-" + uuid.NewString()
		}

		workspaceDir := filepath.Join(analysis.OutputDir, analysisNodeID)
		outputDir := filepath.Join(workspaceDir, "output")
		paramsPath := filepath.Join(workspaceDir, "params.json")
		commandPath := filepath.Join(workspaceDir, "run.sh")
		logPath := filepath.Join(workspaceDir, "command.log")

		if existing != nil {
			if strings.TrimSpace(existing.WorkspaceDir) != "" {
				workspaceDir = existing.WorkspaceDir
			}
			if strings.TrimSpace(existing.OutputDir) != "" {
				outputDir = existing.OutputDir
			}
			if strings.TrimSpace(existing.ParamsPath) != "" {
				paramsPath = existing.ParamsPath
			}
			if strings.TrimSpace(existing.CommandPath) != "" {
				commandPath = existing.CommandPath
			}
			if strings.TrimSpace(existing.LogPath) != "" {
				logPath = existing.LogPath
			}
		}

		if err := os.MkdirAll(outputDir, 0o755); err != nil {
			return err
		}

		status := fallbackString(toString(row["status"]), dagruntime.StatusPending)
		cacheHit := toBool(row["cache_hit"])

		if useCache {
			if existing != nil && shouldKeepNodeStatusForCache(existing.Status) {
				status = strings.TrimSpace(existing.Status)
				cacheHit = true
			} else {
				status = dagruntime.StatusPending
				cacheHit = false
			}
		} else {
			status = dagruntime.StatusPending
			cacheHit = false
		}

		node := &types.AnalysisNode{
			AnalysisNodeID:         analysisNodeID,
			AnalysisID:             analysis.AnalysisID,
			NodeID:                 nodeID,
			NodeName:               toString(row["node_name"]),
			SampleID:               toString(row["sample_id"]),
			ScriptID:               toString(row["script_id"]),
			InputsPatterns:         toJSONMap(row["inputs_patterns"]),
			ResolvedInputs:         toJSONMap(row["resolved_inputs"]),
			OutputPatterns:         toJSONMap(row["output_patterns"]),
			ResolvedOutputs:        toJSONMap(row["resolved_outputs"]),
			Params:                 toJSONMap(row["params"]),
			Status:                 status,
			Executor:               toString(row["executor"]),
			Retry:                  intFromAny(row["retry"], 0),
			MaxRetry:               intFromAny(row["max_retry"], 3),
			CacheHit:               cacheHit,
			UpstreamIDs:            toJSONSlice(row["upstream_ids"]),
			DownstreamIDs:          toJSONSlice(row["downstream_ids"]),
			InputValidationErrors:  toJSONSlice(row["input_validation_errors"]),
			OutputValidationErrors: toJSONSlice(row["output_validation_errors"]),
			LogPath:                logPath,
			WorkspaceDir:           workspaceDir,
			OutputDir:              outputDir,
			CommandPath:            commandPath,
			ParamsPath:             paramsPath,
		}
		if strings.TrimSpace(node.Status) == "" {
			node.Status = dagruntime.StatusPending
		}
		nodes = append(nodes, node)
	}

	edges := make([]*types.AnalysisEdge, 0, len(edgeRows))
	for _, row := range edgeRows {
		edge := &types.AnalysisEdge{
			AnalysisEdgeID: fallbackString(toString(row["analysis_edge_id"]), uuid.NewString()),
			AnalysisID:     analysis.AnalysisID,
			SourceNode:     toString(row["source_node"]),
			TargetNode:     toString(row["target_node"]),
			SourceHandle:   toString(row["source_handle"]),
			TargetHandle:   toString(row["target_handle"]),
		}
		edges = append(edges, edge)
	}

	if err := repo.DeleteAnalysisNodesByAnalysisID(ctx, analysis.AnalysisID); err != nil {
		return err
	}
	if err := repo.CreateAnalysisNodes(ctx, nodes); err != nil {
		return err
	}
	if err := repo.DeleteAnalysisEdgesByAnalysisID(ctx, analysis.AnalysisID); err != nil {
		return err
	}
	if err := repo.CreateAnalysisEdges(ctx, edges); err != nil {
		return err
	}

	return nil
}

func (s *analysisService) resolveStorageBaseDir() string {
	if s.cfg != nil && s.cfg.Storage != nil {
		if base := strings.TrimSpace(s.cfg.Storage.BaseDir); base != "" {
			return base
		}
	}
	return "."
}

func writeJSONFile(path string, value any) error {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, content, 0o644)
}

func toMapSlice(value any) []map[string]any {
	if value == nil {
		return []map[string]any{}
	}
	if rows, ok := value.([]map[string]any); ok {
		return rows
	}
	raw, ok := value.([]any)
	if !ok {
		return []map[string]any{}
	}
	rows := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			rows = append(rows, m)
		}
	}
	return rows
}

func toString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func fallbackString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func toBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return strings.EqualFold(strings.TrimSpace(t), "true") || strings.TrimSpace(t) == "1"
	case int:
		return t != 0
	case int64:
		return t != 0
	case float64:
		return t != 0
	default:
		return false
	}
}

func intFromAny(v any, fallback int) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(t))
		if err == nil {
			return n
		}
	}
	return fallback
}

func toJSONMap(value any) types.JSONMap {
	if value == nil {
		return types.JSONMap{}
	}
	switch v := value.(type) {
	case types.JSONMap:
		return v
	case map[string]any:
		return types.JSONMap(v)
	case string:
		return parseJSONMap([]byte(v))
	case []byte:
		return parseJSONMap(v)
	default:
		buf, err := json.Marshal(v)
		if err != nil {
			return types.JSONMap{}
		}
		return parseJSONMap(buf)
	}
}

func parseJSONMap(raw []byte) types.JSONMap {
	out := types.JSONMap{}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return out
	}
	if err := json.Unmarshal([]byte(trimmed), &out); err != nil {
		return types.JSONMap{}
	}
	return out
}

func toJSONSlice(value any) types.JSONSlice {
	if value == nil {
		return types.JSONSlice{}
	}
	switch v := value.(type) {
	case types.JSONSlice:
		return v
	case []any:
		return types.JSONSlice(v)
	case []string:
		out := make(types.JSONSlice, 0, len(v))
		for _, item := range v {
			out = append(out, item)
		}
		return out
	case string:
		return parseJSONSlice([]byte(v))
	case []byte:
		return parseJSONSlice(v)
	default:
		buf, err := json.Marshal(v)
		if err != nil {
			return types.JSONSlice{}
		}
		return parseJSONSlice(buf)
	}
}

func parseJSONSlice(raw []byte) types.JSONSlice {
	out := make(types.JSONSlice, 0)
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return out
	}
	if err := json.Unmarshal([]byte(trimmed), &out); err != nil {
		return types.JSONSlice{}
	}
	return out
}

func shouldKeepNodeStatusForCache(status string) bool {
	normalized := strings.TrimSpace(strings.ToLower(status))
	switch normalized {
	case dagruntime.StatusReady, dagruntime.StatusDone, dagruntime.StatusCached:
		return true
	default:
		return false
	}
}
