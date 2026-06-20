package compiler

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

type DecorateStage struct{}

func (s *DecorateStage) Name() string { return "DecorateStage" }

func (s *DecorateStage) Run(ctx *CompileContext) error {
	upstream := map[string]map[string]struct{}{}
	downstream := map[string]map[string]struct{}{}

	for _, edge := range ctx.AnalysisEdges {
		sourceNode := fmt.Sprintf("%v", firstNonNil(edge["source_node"], ""))
		targetNode := fmt.Sprintf("%v", firstNonNil(edge["target_node"], ""))

		edge["analysis_id"] = ctx.AnalysisID
		if edge["analysis_edge_id"] == nil || fmt.Sprintf("%v", edge["analysis_edge_id"]) == "" {
			edge["analysis_edge_id"] = uuid.NewString()
		}

		sourceHandle := edge["source_handle"]
		if sourceHandle == nil {
			sourceHandle = edge["sourceHandle"]
		}
		targetHandle := edge["target_handle"]
		if targetHandle == nil {
			targetHandle = edge["targetHandle"]
		}
		edge["source_handle"] = sourceHandle
		edge["target_handle"] = targetHandle
		delete(edge, "sourceHandle")
		delete(edge, "targetHandle")

		if sourceNode != "" && targetNode != "" {
			if _, ok := upstream[targetNode]; !ok {
				upstream[targetNode] = map[string]struct{}{}
			}
			if _, ok := downstream[sourceNode]; !ok {
				downstream[sourceNode] = map[string]struct{}{}
			}
			upstream[targetNode][sourceNode] = struct{}{}
			downstream[sourceNode][targetNode] = struct{}{}
		}
	}

	for _, node := range ctx.AnalysisNodes {
		nodeID := fmt.Sprintf("%v", firstNonNil(node["node_id"], ""))
		node["analysis_id"] = ctx.AnalysisID
		if node["analysis_node_id"] == nil || fmt.Sprintf("%v", node["analysis_node_id"]) == "" {
			node["analysis_node_id"] = uuid.NewString()
		}

		if node["status"] == nil {
			node["status"] = "pending"
		}
		if node["retry"] == nil {
			node["retry"] = 0
		}
		if node["max_retry"] == nil {
			node["max_retry"] = 3
		}
		if node["cache_hit"] == nil {
			node["cache_hit"] = false
		}
		if node["input_validation_errors"] == nil {
			node["input_validation_errors"] = []any{}
		}

		node["upstream_ids"] = sortedSetKeys(upstream[nodeID])
		node["downstream_ids"] = sortedSetKeys(downstream[nodeID])
	}

	return nil
}

func sortedSetKeys(values map[string]struct{}) []string {
	if len(values) == 0 {
		return []string{}
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
