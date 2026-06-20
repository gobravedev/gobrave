package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/gorm"
)

type RuntimeSnapshot struct {
	AnalysisID        string         `json:"analysis_id"`
	TotalNodes        int            `json:"total_nodes"`
	StatusCount       map[string]int `json:"status_count"`
	CompletedCount    int            `json:"completed_count"`
	CompletionPercent int            `json:"completion_percent"`
	ReadyCount        int            `json:"ready_count"`
	RunningCount      int            `json:"running_count"`
	IsFinished        bool           `json:"is_finished"`
}

type RuntimeEngine struct {
	repo interfaces.AnalysisRepository
}

func NewRuntimeEngine(repo interfaces.AnalysisRepository) *RuntimeEngine {
	return &RuntimeEngine{repo: repo}
}

func (e *RuntimeEngine) GetSnapshot(ctx context.Context, analysisID string) (*RuntimeSnapshot, error) {
	nodes, err := e.repo.ListAnalysisNodesByAnalysisID(ctx, analysisID)
	if err != nil {
		return nil, err
	}

	statusCount := map[string]int{}
	completed := 0
	readyCount := 0
	runningCount := 0
	isFinished := true
	for _, node := range nodes {
		status := strings.TrimSpace(strings.ToLower(node.Status))
		if status == "" {
			status = StatusPending
		}
		statusCount[status]++
		if isTerminalNodeStatus(node) {
			completed++
		} else {
			isFinished = false
		}
		if status == StatusReady && !node.CacheHit {
			readyCount++
		}
		if status == StatusRunning || status == StatusSubmitted {
			runningCount++
		}
	}

	percent := 100
	if len(nodes) > 0 {
		percent = int(float64(completed) * 100.0 / float64(len(nodes)))
	}

	return &RuntimeSnapshot{
		AnalysisID:        analysisID,
		TotalNodes:        len(nodes),
		StatusCount:       statusCount,
		CompletedCount:    completed,
		CompletionPercent: percent,
		ReadyCount:        readyCount,
		RunningCount:      runningCount,
		IsFinished:        isFinished,
	}, nil
}

func (e *RuntimeEngine) RefreshReadyStatus(ctx context.Context, analysisID string) error {
	nodes, err := e.repo.ListAnalysisNodesByAnalysisID(ctx, analysisID)
	if err != nil {
		return err
	}
	edges, err := e.repo.ListAnalysisEdgesByAnalysisID(ctx, analysisID)
	if err != nil {
		return err
	}

	nodeByID := make(map[string]*types.AnalysisNode, len(nodes))
	for _, node := range nodes {
		nodeByID[node.NodeID] = node
	}

	incoming := map[string][]*types.AnalysisEdge{}
	for _, edge := range edges {
		incoming[edge.TargetNode] = append(incoming[edge.TargetNode], edge)
	}

	for _, node := range nodes {
		status := strings.TrimSpace(strings.ToLower(node.Status))
		if status == "" {
			status = StatusPending
		}
		if status != StatusPending {
			continue
		}

		canRun := true
		for _, edge := range incoming[node.NodeID] {
			upstream := nodeByID[edge.SourceNode]
			if upstream == nil {
				continue
			}
			upstreamStatus := strings.TrimSpace(strings.ToLower(upstream.Status))
			if !isTerminalNodeStatus(upstream) {
				canRun = false
				break
			}
			if !isSuccessNodeStatus(upstream) {
				canRun = false
				_ = e.repo.UpdateAnalysisNodeByAnalysisNodeID(ctx, node.AnalysisNodeID, map[string]any{
					"status":        StatusSkipped,
					"finished_at":   time.Now().UTC(),
					"error_message": fmt.Sprintf("upstream node %s ended with status %s", upstream.NodeID, upstreamStatus),
				})
				break
			}
		}
		if canRun {
			if err := e.repo.UpdateAnalysisNodeByAnalysisNodeID(ctx, node.AnalysisNodeID, map[string]any{"status": StatusReady}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *RuntimeEngine) ClaimNextReadyNode(ctx context.Context, analysisID string) (*types.AnalysisNode, error) {
	node, err := e.repo.ClaimNextReadyNode(ctx, analysisID, StatusReady, StatusSubmitted)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return node, nil
}

func (e *RuntimeEngine) MarkNodeRunning(ctx context.Context, analysisNodeID string) (*types.AnalysisNode, error) {
	node, err := e.repo.GetAnalysisNodeByAnalysisNodeID(ctx, analysisNodeID)
	if err != nil {
		return nil, err
	}
	if err := EnsureTransition(node.Status, StatusRunning); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if err := e.repo.UpdateAnalysisNodeByAnalysisNodeID(ctx, analysisNodeID, map[string]any{
		"status":     StatusRunning,
		"started_at": &now,
	}); err != nil {
		return nil, err
	}
	node.Status = StatusRunning
	node.StartedAt = &now
	return node, nil
}

func (e *RuntimeEngine) CompleteNode(
	ctx context.Context,
	analysisID string,
	nodeID string,
	status string,
	resolvedOutputs map[string]any,
	exitCode int,
	errorMessage string,
) (*types.AnalysisNode, error) {
	node, err := e.repo.GetAnalysisNodeByNodeID(ctx, analysisID, nodeID)
	if err != nil {
		return nil, err
	}
	status = strings.TrimSpace(strings.ToLower(status))
	if status == "" {
		status = StatusDone
	}
	if err := EnsureTransition(node.Status, status); err != nil {
		return nil, err
	}

	updates := map[string]any{
		"status":        status,
		"exit_code":     exitCode,
		"finished_at":   time.Now().UTC(),
		"error_message": nil,
	}
	if status == StatusFailed {
		if strings.TrimSpace(errorMessage) == "" {
			errorMessage = "node execution failed"
		}
		updates["error_message"] = errorMessage
	}

	if IsSuccessStatus(status) {
		payload := toJSON(resolvedOutputs)
		updates["resolved_outputs"] = payload
		updates["output_validation_errors"] = "[]"
	}

	if err := e.repo.UpdateAnalysisNodeByAnalysisNodeID(ctx, node.AnalysisNodeID, updates); err != nil {
		return nil, err
	}

	if IsSuccessStatus(status) {
		if err := e.propagateOutputs(ctx, analysisID, nodeID, resolvedOutputs); err != nil {
			return nil, err
		}
	}
	if err := e.RefreshReadyStatus(ctx, analysisID); err != nil {
		return nil, err
	}
	return e.repo.GetAnalysisNodeByNodeID(ctx, analysisID, nodeID)
}

func (e *RuntimeEngine) propagateOutputs(ctx context.Context, analysisID string, sourceNodeID string, outputs map[string]any) error {
	if len(outputs) == 0 {
		return nil
	}
	edges, err := e.repo.ListAnalysisEdgesByAnalysisID(ctx, analysisID)
	if err != nil {
		return err
	}

	for _, edge := range edges {
		if edge.SourceNode != sourceNodeID {
			continue
		}
		sourceHandle := strings.TrimSpace(edge.SourceHandle)
		targetHandle := strings.TrimSpace(edge.TargetHandle)
		if sourceHandle == "" || targetHandle == "" {
			continue
		}
		value, ok := outputs[sourceHandle]
		if !ok {
			continue
		}

		targetNode, err := e.repo.GetAnalysisNodeByNodeID(ctx, analysisID, edge.TargetNode)
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				continue
			}
			return err
		}

		params := fromJSONMap(targetNode.Params)
		resolvedInputs := fromJSONMap(targetNode.ResolvedInputs)
		inputPatterns := fromJSONMap(targetNode.InputsPatterns)
		inputCfg := asMap(inputPatterns[targetHandle])
		multiple := asBool(inputCfg["multiple"])

		if multiple || isListValue(params[targetHandle]) || isListValue(resolvedInputs[targetHandle]) {
			params[targetHandle] = appendToList(params[targetHandle], value)
			resolvedInputs[targetHandle] = appendToList(resolvedInputs[targetHandle], value)
		} else {
			params[targetHandle] = value
			resolvedInputs[targetHandle] = value
		}

		if err := e.repo.UpdateAnalysisNodeByAnalysisNodeID(ctx, targetNode.AnalysisNodeID, map[string]any{
			"params":          toJSON(params),
			"resolved_inputs": toJSON(resolvedInputs),
		}); err != nil {
			return err
		}
	}

	return nil
}

func toJSON(v any) string {
	if v == nil {
		return "{}"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func fromJSONMap(raw string) map[string]any {
	out := map[string]any{}
	if strings.TrimSpace(raw) == "" {
		return out
	}
	_ = json.Unmarshal([]byte(raw), &out)
	return out
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	if m, ok := v.(map[string]interface{}); ok {
		return map[string]any(m)
	}
	return map[string]any{}
}

func asBool(v any) bool {
	b, ok := v.(bool)
	return ok && b
}

func isListValue(v any) bool {
	switch v.(type) {
	case []any:
		return true
	default:
		return false
	}
}

func appendToList(current any, value any) []any {
	if current == nil {
		return []any{value}
	}
	if arr, ok := current.([]any); ok {
		return append(arr, value)
	}
	if arr, ok := current.([]interface{}); ok {
		return append(arr, value)
	}
	return []any{current, value}
}

func isTerminalNodeStatus(node *types.AnalysisNode) bool {
	if node == nil {
		return false
	}
	status := strings.TrimSpace(strings.ToLower(node.Status))
	if status == StatusReady && node.CacheHit {
		return true
	}
	return IsTerminalStatus(status)
}

func isSuccessNodeStatus(node *types.AnalysisNode) bool {
	if node == nil {
		return false
	}
	status := strings.TrimSpace(strings.ToLower(node.Status))
	if status == StatusReady && node.CacheHit {
		return true
	}
	return IsSuccessStatus(status)
}

func SortNodesByStartedAt(nodes []*types.AnalysisNode) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].CreatedAt.Before(nodes[j].CreatedAt)
	})
}
