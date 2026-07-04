package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gobravedev/gobrave/internal/compiler"
	"github.com/gobravedev/gobrave/internal/config"
	dagruntime "github.com/gobravedev/gobrave/internal/dag"
	"github.com/gobravedev/gobrave/internal/dag/executor"
	"github.com/gobravedev/gobrave/internal/event"
	"github.com/gobravedev/gobrave/internal/logger"
	"github.com/gobravedev/gobrave/internal/manager"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"github.com/gobravedev/gobrave/internal/utils"
	"github.com/google/uuid"
)

const (
	dynamicV2LeaseTTL  = 90 * time.Second
	dynamicV2Heartbeat = 15 * time.Second
)

type dynamicDagOrchestratorV2 struct {
	repo          interfaces.AnalysisRepository
	workflowRepo  interfaces.WorkflowRepository
	containerRepo interfaces.ContainerRepository
	containerMgr  *manager.ContainerManager
	cfg           *config.Config
	bus           event.Bus

	registry *dagruntime.RunningRegistry
	mu       sync.Mutex
}

func NewDynamicDagOrchestratorV2(
	repo interfaces.AnalysisRepository,
	workflowRepo interfaces.WorkflowRepository,
	containerRepo interfaces.ContainerRepository,
	containerMgr *manager.ContainerManager,
	cfg *config.Config,
	bus event.Bus,
) interfaces.DynamicDagOrchestrator {
	return &dynamicDagOrchestratorV2{
		repo:          repo,
		workflowRepo:  workflowRepo,
		containerRepo: containerRepo,
		containerMgr:  containerMgr,
		cfg:           cfg,
		bus:           bus,
		registry:      dagruntime.NewRunningRegistry(),
	}
}

func (o *dynamicDagOrchestratorV2) StartAsyncV2(ctx context.Context, analysisID string, parseAnalysisResult map[string]any, dagDefinition map[string]any) error {
	analysisID = strings.TrimSpace(analysisID)
	if analysisID == "" {
		return fmt.Errorf("analysis_id is required")
	}
	if o.registry.IsRunning(analysisID) {
		return nil
	}

	now := time.Now().UTC()
	locked, err := o.repo.TryMarkAnalysisRunning(ctx, analysisID, now, now.Add(-dynamicV2LeaseTTL))
	if err != nil {
		return err
	}
	if !locked {
		return nil
	}

	compiled, err := compiler.BuildRuntimeTasks(analysisID, dynamicCloneAnyMap(parseAnalysisResult), dynamicCloneAnyMap(dagDefinition))
	if err != nil {
		_ = o.repo.UpdateAnalysisByAnalysisID(context.Background(), analysisID, map[string]any{
			"job_status": "failed",
			"updated_at": time.Now().UTC(),
		})
		return fmt.Errorf("compile runtime dag for dynamic v2 failed: %w", err)
	}

	nodeTemplates := dynamicToMapSlice(compiled["analysis_nodes"])
	edgeRows := dynamicToMapSlice(compiled["analysis_edges"])

	runCtx, runCancel := context.WithCancel(context.Background())
	o.registry.Register(&dagruntime.RunningEntry{
		AnalysisID:     analysisID,
		TaskName:       "dag-v2-run-" + analysisID,
		MaxConcurrency: 1,
		QueueSize:      64,
		PollIntervalMs: 500,
		Status:         "running",
		Cancel:         runCancel,
	})

	heartbeatStop := make(chan struct{})
	go o.renewRunningLease(analysisID, heartbeatStop)

	go func() {
		defer close(heartbeatStop)
		defer runCancel()
		finalStatus := "finished"
		if err := o.runDynamicLoop(runCtx, analysisID, nodeTemplates, edgeRows); err != nil {
			finalStatus = "failed"
			logger.Warnf(context.Background(), "[DynamicDagOrchestratorV2] run failed, analysis_id=%s err=%v", analysisID, err)
		}
		if o.registry.IsStopping(analysisID) {
			finalStatus = "stopped"
		}
		o.registry.MarkFinished(analysisID, finalStatus)
		_ = o.repo.UpdateAnalysisByAnalysisID(context.Background(), analysisID, map[string]any{
			"job_status": finalStatus,
			"updated_at": time.Now().UTC(),
		})
	}()

	return nil
}

func (o *dynamicDagOrchestratorV2) runDynamicLoop(ctx context.Context, analysisID string, nodeTemplates []map[string]any, edgeRows []map[string]any) error {
	analysis, err := o.repo.GetAnalysisByAnalysisID(ctx, analysisID)
	if err != nil {
		return err
	}

	edges := buildAnalysisEdges(analysisID, edgeRows)
	if err := o.repo.DeleteAnalysisEdgesByAnalysisID(ctx, analysisID); err != nil {
		return err
	}
	if err := o.repo.CreateAnalysisEdges(ctx, edges); err != nil {
		return err
	}

	runtime := dagruntime.NewRuntimeEngine(o.repo)
	storageBase := ""
	if o.cfg != nil && o.cfg.Storage != nil {
		storageBase = strings.TrimSpace(o.cfg.Storage.BaseDir)
	}
	preparer := dagruntime.NewFileSystemNodeRuntimePreparer(o.repo, o.workflowRepo, storageBase)
	dispatcher := dagruntime.NewNodeDispatcher(
		runtime,
		o.repo,
		o.bus,
		executor.NewFactory(executor.FactoryDeps{
			WorkflowRepository: o.workflowRepo,
			ContainerManager:   o.containerMgr,
		}),
		nil,
		preparer,
	)

	pool := dagruntime.NewWorkerPool(analysisID, dispatcher, 1, 64)
	pool.Start(ctx)
	defer pool.Stop()

	incoming := buildIncomingEdgeMap(edges)
	nodeTemplateByID := make(map[string]map[string]any, len(nodeTemplates))
	for _, row := range nodeTemplates {
		nid := strings.TrimSpace(dynamicToString(row["node_id"]))
		if nid == "" {
			continue
		}
		nodeTemplateByID[nid] = row
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		if shouldStop, stopErr := o.shouldStopByJobStatus(ctx, analysisID); stopErr == nil && shouldStop {
			return nil
		}

		if err := o.materializeDynamicNodes(ctx, analysis, nodeTemplateByID, incoming); err != nil {
			return err
		}
		if err := runtime.RefreshReadyStatus(ctx, analysisID); err != nil {
			return err
		}

		queueLen := pool.QueueLen()
		if queueLen < 64 {
			slots := 64 - queueLen
			for i := 0; i < slots; i++ {
				node, claimErr := runtime.ClaimNextReadyNode(ctx, analysisID)
				if claimErr != nil {
					return claimErr
				}
				if node == nil {
					break
				}
				if ok := pool.Enqueue(node.AnalysisNodeID); !ok {
					break
				}
			}
		}

		snapshot, err := runtime.GetSnapshot(ctx, analysisID)
		if err != nil {
			return err
		}

		allCreated, err := o.allCandidateNodesCreated(ctx, analysisID, len(nodeTemplateByID))
		if err != nil {
			return err
		}
		if allCreated && snapshot.IsFinished && pool.QueueLen() == 0 {
			if snapshot.StatusCount[dagruntime.StatusFailed] > 0 {
				return fmt.Errorf("one or more dynamic dag nodes failed")
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (o *dynamicDagOrchestratorV2) materializeDynamicNodes(
	ctx context.Context,
	analysis *types.Analysis,
	nodeTemplateByID map[string]map[string]any,
	incoming map[string][]*types.AnalysisEdge,
) error {
	existingNodes, err := o.repo.ListAnalysisNodesByAnalysisID(ctx, analysis.AnalysisID)
	if err != nil {
		return err
	}
	existingByNodeID := make(map[string]*types.AnalysisNode, len(existingNodes))
	for _, n := range existingNodes {
		existingByNodeID[n.NodeID] = n
	}

	keys := make([]string, 0, len(nodeTemplateByID))
	for nodeID := range nodeTemplateByID {
		keys = append(keys, nodeID)
	}
	sort.Strings(keys)

	newItems := make([]*types.AnalysisNode, 0)
	for _, nodeID := range keys {
		if _, exists := existingByNodeID[nodeID]; exists {
			continue
		}

		row := nodeTemplateByID[nodeID]
		upstreamIDs := dynamicToStringSlice(row["upstream_ids"])
		if !allUpstreamSuccess(upstreamIDs, existingByNodeID) {
			continue
		}

		mergedParams := dynamicToJSONMap(row["params"])
		mergedInputs := dynamicToJSONMap(row["resolved_inputs"])
		bootstrapInputsFromUpstream(row, mergedParams, mergedInputs, incoming[nodeID], existingByNodeID)

		analysisNodeID := strings.TrimSpace(dynamicToString(row["analysis_node_id"]))
		if analysisNodeID == "" {
			analysisNodeID = "node-" + uuid.NewString()
		}
		workspaceDir := filepath.Join(analysis.OutputDir, analysisNodeID)
		outputDir := filepath.Join(workspaceDir, "output")
		paramsPath := filepath.Join(workspaceDir, "params.json")
		commandPath := filepath.Join(workspaceDir, "run.sh")
		logPath := filepath.Join(workspaceDir, "command.log")
		if err := os.MkdirAll(outputDir, 0o755); err != nil {
			return err
		}

		newItems = append(newItems, &types.AnalysisNode{
			ID:                     utils.GenerateID(),
			AnalysisNodeID:         analysisNodeID,
			AnalysisID:             analysis.AnalysisID,
			NodeID:                 nodeID,
			NodeName:               dynamicToString(row["node_name"]),
			SampleID:               dynamicToString(row["sample_id"]),
			ScriptID:               dynamicToString(row["script_id"]),
			InputsPatterns:         dynamicToJSONMap(row["inputs_patterns"]),
			ResolvedInputs:         mergedInputs,
			OutputPatterns:         dynamicToJSONMap(row["output_patterns"]),
			ResolvedOutputs:        dynamicToJSONMap(row["resolved_outputs"]),
			Params:                 mergedParams,
			Status:                 dagruntime.StatusPending,
			Executor:               dynamicToString(row["executor"]),
			Retry:                  dynamicIntFromAny(row["retry"], 0),
			MaxRetry:               dynamicIntFromAny(row["max_retry"], 3),
			CacheHit:               false,
			UpstreamIDs:            types.JSONSlice(dynamicStringSliceToAny(upstreamIDs)),
			DownstreamIDs:          dynamicToJSONSlice(row["downstream_ids"]),
			InputValidationErrors:  dynamicToJSONSlice(row["input_validation_errors"]),
			OutputValidationErrors: types.JSONSlice{},
			LogPath:                logPath,
			WorkspaceDir:           workspaceDir,
			OutputDir:              outputDir,
			CommandPath:            commandPath,
			ParamsPath:             paramsPath,
		})
	}

	if len(newItems) == 0 {
		return nil
	}
	return o.repo.CreateAnalysisNodes(ctx, newItems)
}

func (o *dynamicDagOrchestratorV2) allCandidateNodesCreated(ctx context.Context, analysisID string, total int) (bool, error) {
	nodes, err := o.repo.ListAnalysisNodesByAnalysisID(ctx, analysisID)
	if err != nil {
		return false, err
	}
	return len(nodes) >= total, nil
}

func (o *dynamicDagOrchestratorV2) shouldStopByJobStatus(ctx context.Context, analysisID string) (bool, error) {
	analysis, err := o.repo.GetAnalysisByAnalysisID(ctx, analysisID)
	if err != nil {
		return false, err
	}
	status := strings.ToLower(strings.TrimSpace(analysis.JobStatus))
	return status == "stopping" || status == "stopped", nil
}

func (o *dynamicDagOrchestratorV2) renewRunningLease(analysisID string, stop <-chan struct{}) {
	ticker := time.NewTicker(dynamicV2Heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			_ = o.repo.UpdateAnalysisByAnalysisID(context.Background(), analysisID, map[string]any{
				"updated_at": time.Now().UTC(),
			})
		}
	}
}

func buildAnalysisEdges(analysisID string, edgeRows []map[string]any) []*types.AnalysisEdge {
	out := make([]*types.AnalysisEdge, 0, len(edgeRows))
	for _, row := range edgeRows {
		out = append(out, &types.AnalysisEdge{
			AnalysisEdgeID: dynamicFallbackString(dynamicToString(row["analysis_edge_id"]), uuid.NewString()),
			AnalysisID:     analysisID,
			SourceNode:     dynamicToString(row["source_node"]),
			TargetNode:     dynamicToString(row["target_node"]),
			SourceHandle:   dynamicToString(row["source_handle"]),
			TargetHandle:   dynamicToString(row["target_handle"]),
		})
	}
	return out
}

func buildIncomingEdgeMap(edges []*types.AnalysisEdge) map[string][]*types.AnalysisEdge {
	incoming := make(map[string][]*types.AnalysisEdge)
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		incoming[edge.TargetNode] = append(incoming[edge.TargetNode], edge)
	}
	return incoming
}

func bootstrapInputsFromUpstream(
	row map[string]any,
	params types.JSONMap,
	resolvedInputs types.JSONMap,
	edges []*types.AnalysisEdge,
	existing map[string]*types.AnalysisNode,
) {
	patterns := dynamicToJSONMap(row["inputs_patterns"])
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		source := existing[edge.SourceNode]
		if !isSuccessNode(source) {
			continue
		}
		value, ok := source.ResolvedOutputs[edge.SourceHandle]
		if !ok {
			continue
		}

		cfg := dynamicAsMap(patterns[edge.TargetHandle])
		multiple := dynamicAsBool(cfg["multiple"])
		if multiple || dynamicIsListValue(params[edge.TargetHandle]) || dynamicIsListValue(resolvedInputs[edge.TargetHandle]) {
			params[edge.TargetHandle] = dynamicAppendToList(params[edge.TargetHandle], value)
			resolvedInputs[edge.TargetHandle] = dynamicAppendToList(resolvedInputs[edge.TargetHandle], value)
			continue
		}
		params[edge.TargetHandle] = value
		resolvedInputs[edge.TargetHandle] = value
	}
}

func allUpstreamSuccess(upstream []string, existing map[string]*types.AnalysisNode) bool {
	for _, id := range upstream {
		node := existing[id]
		if !isSuccessNode(node) {
			return false
		}
	}
	return true
}

func isSuccessNode(node *types.AnalysisNode) bool {
	if node == nil {
		return false
	}
	status := strings.ToLower(strings.TrimSpace(node.Status))
	if status == dagruntime.StatusReady && node.CacheHit {
		return true
	}
	return dagruntime.IsSuccessStatus(status)
}

func dynamicToMapSlice(value any) []map[string]any {
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

func dynamicToString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func dynamicFallbackString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func dynamicIntFromAny(v any, fallback int) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case string:
		n, err := strconvAtoi(strings.TrimSpace(t))
		if err == nil {
			return n
		}
	}
	return fallback
}

func dynamicToJSONMap(v any) types.JSONMap {
	if v == nil {
		return types.JSONMap{}
	}
	if m, ok := v.(types.JSONMap); ok {
		return m
	}
	if m, ok := v.(map[string]any); ok {
		return types.JSONMap(dynamicCloneAnyMap(m))
	}
	if m, ok := v.(map[string]interface{}); ok {
		out := make(map[string]any, len(m))
		for k, val := range m {
			out[k] = val
		}
		return types.JSONMap(out)
	}
	return types.JSONMap{}
}

func dynamicToJSONSlice(v any) types.JSONSlice {
	if v == nil {
		return types.JSONSlice{}
	}
	if s, ok := v.(types.JSONSlice); ok {
		return s
	}
	if s, ok := v.([]any); ok {
		return types.JSONSlice(s)
	}
	rv := reflect.ValueOf(v)
	if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
		out := make([]any, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out = append(out, rv.Index(i).Interface())
		}
		return types.JSONSlice(out)
	}
	return types.JSONSlice{}
}

func dynamicCloneAnyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func dynamicToStringSlice(v any) []string {
	if v == nil {
		return []string{}
	}
	if arr, ok := v.([]string); ok {
		out := make([]string, 0, len(arr))
		for _, s := range arr {
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	if arr, ok := v.([]any); ok {
		out := make([]string, 0, len(arr))
		for _, item := range arr {
			s := strings.TrimSpace(dynamicToString(item))
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return []string{}
}

func dynamicStringSliceToAny(items []string) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	return out
}

func dynamicAsMap(v any) map[string]any {
	if v == nil {
		return map[string]any{}
	}
	if m, ok := v.(map[string]any); ok {
		return m
	}
	if m, ok := v.(types.JSONMap); ok {
		return map[string]any(m)
	}
	return map[string]any{}
}

func dynamicAsBool(v any) bool {
	switch val := v.(type) {
	case bool:
		return val
	case string:
		normalized := strings.TrimSpace(strings.ToLower(val))
		return normalized == "true" || normalized == "1" || normalized == "yes" || normalized == "y"
	case int:
		return val != 0
	case int64:
		return val != 0
	case float64:
		return val != 0
	default:
		return false
	}
}

func dynamicIsListValue(v any) bool {
	if v == nil {
		return false
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array
}

func dynamicAppendToList(current any, value any) []any {
	if current == nil {
		return []any{value}
	}
	if arr, ok := current.([]any); ok {
		return append(arr, value)
	}
	rv := reflect.ValueOf(current)
	if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
		out := make([]any, 0, rv.Len()+1)
		for i := 0; i < rv.Len(); i++ {
			out = append(out, rv.Index(i).Interface())
		}
		out = append(out, value)
		return out
	}
	return []any{current, value}
}

func strconvAtoi(v string) (int, error) {
	neg := false
	if strings.HasPrefix(v, "-") {
		neg = true
		v = strings.TrimPrefix(v, "-")
	}
	if v == "" {
		return 0, fmt.Errorf("empty")
	}
	n := 0
	for _, ch := range v {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid")
		}
		n = n*10 + int(ch-'0')
	}
	if neg {
		n = -n
	}
	return n, nil
}
