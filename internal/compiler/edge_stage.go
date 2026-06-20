package compiler

import "fmt"

type EdgeStage struct {
	sampleStrategy    EdgeStrategy
	singletonStrategy EdgeStrategy
	builder           *RuntimeEdgeBuilder
}

type EdgeStrategy interface {
	Build(ctx *CompileContext, nid string, node map[string]any, states []*NodeRuntimeState, builder *RuntimeEdgeBuilder) []map[string]any
}

type RuntimeEdgeBuilder struct{}

func (s *EdgeStage) Name() string { return "EdgeStage" }

func (s *EdgeStage) Run(ctx *CompileContext) error {
	if s.sampleStrategy == nil {
		s.sampleStrategy = &SampleEdgeStrategy{}
	}
	if s.singletonStrategy == nil {
		s.singletonStrategy = &SingletonEdgeStrategy{}
	}
	if s.builder == nil {
		s.builder = &RuntimeEdgeBuilder{}
	}

	ctx.AnalysisEdges = make([]map[string]any, 0)
	for _, nid := range ctx.Order {
		node := ctx.NodeMap[nid]
		states := ctx.NodeStates[nid]
		if ctx.NodeKind[nid] == "sample" {
			ctx.AnalysisEdges = append(ctx.AnalysisEdges, s.sampleStrategy.Build(ctx, nid, node, states, s.builder)...)
			continue
		}
		ctx.AnalysisEdges = append(ctx.AnalysisEdges, s.singletonStrategy.Build(ctx, nid, node, states, s.builder)...)
	}
	return nil
}

func (b *RuntimeEdgeBuilder) Build(analysisID string, sourceNode string, targetNode string, sourceHandle string, targetHandle string) map[string]any {
	return map[string]any{
		"analysis_id":   analysisID,
		"source_node":   sourceNode,
		"target_node":   targetNode,
		"source_handle": sourceHandle,
		"target_handle": targetHandle,
	}
}

type SampleEdgeStrategy struct{}

func (s *SampleEdgeStrategy) Build(ctx *CompileContext, nid string, node map[string]any, states []*NodeRuntimeState, builder *RuntimeEdgeBuilder) []map[string]any {
	res := make([]map[string]any, 0)
	sourceIDBase := nodeIDBase(node)
	currentLabels := append([]string(nil), ctx.NodeLabel[nid]...)
	if len(currentLabels) == 0 {
		currentLabels = []string{""}
	}

	for _, edge := range ctx.Outgoing[nid] {
		target := fmt.Sprintf("%v", firstNonNil(edge["target"], ""))
		targetNode := ctx.NodeMap[target]
		targetIDBase := nodeIDBase(targetNode)
		targetKind := ctx.NodeKind[target]
		sourceHandle := edgeValue(edge, "sourceHandle", "source_handle")
		targetHandle := edgeValue(edge, "targetHandle", "target_handle")
		targetScatterField := scatterField(targetNode)

		targetLabels := make([]string, 0)
		if targetScatterField != "" {
			if raw, ok := ctx.Params[targetScatterField].([]any); ok {
				for i, item := range raw {
					sample, ok := item.(map[string]any)
					if !ok {
						continue
					}
					targetLabels = append(targetLabels, sampleLabel(sample, i))
				}
			}
		}

		if targetKind == "sample" {
			for _, srcLabel := range currentLabels {
				sourceNodeID := buildNodeID(sourceIDBase, srcLabel)
				if targetScatterField != "" {
					candidateLabels := targetLabels
					if srcLabel != "" && containsLabel(targetLabels, srcLabel) {
						candidateLabels = []string{srcLabel}
					}
					for _, dstLabel := range candidateLabels {
						res = append(res, builder.Build(ctx.AnalysisID, sourceNodeID, buildNodeID(targetIDBase, dstLabel), sourceHandle, targetHandle))
					}
				} else {
					res = append(res, builder.Build(ctx.AnalysisID, sourceNodeID, buildNodeID(targetIDBase, srcLabel), sourceHandle, targetHandle))
				}
			}
			continue
		}

		for _, srcLabel := range currentLabels {
			sourceNodeID := buildNodeID(sourceIDBase, srcLabel)
			res = append(res, builder.Build(ctx.AnalysisID, sourceNodeID, targetIDBase, sourceHandle, targetHandle))
		}
	}
	return res
}

type SingletonEdgeStrategy struct{}

func (s *SingletonEdgeStrategy) Build(ctx *CompileContext, nid string, node map[string]any, states []*NodeRuntimeState, builder *RuntimeEdgeBuilder) []map[string]any {
	if len(states) == 0 {
		return nil
	}
	nodeInstance := states[0].NodeID
	res := make([]map[string]any, 0)

	for _, edge := range ctx.Outgoing[nid] {
		target := fmt.Sprintf("%v", firstNonNil(edge["target"], ""))
		targetNode := ctx.NodeMap[target]
		targetIDBase := nodeIDBase(targetNode)
		targetKind := ctx.NodeKind[target]
		sourceHandle := edgeValue(edge, "sourceHandle", "source_handle")
		targetHandle := edgeValue(edge, "targetHandle", "target_handle")
		targetScatterField := scatterField(targetNode)

		if targetKind == "sample" {
			targetLabels := make([]string, 0)
			if targetScatterField != "" {
				if raw, ok := ctx.Params[targetScatterField].([]any); ok {
					for i, item := range raw {
						sample, ok := item.(map[string]any)
						if !ok {
							continue
						}
						targetLabels = append(targetLabels, sampleLabel(sample, i))
					}
				}
			} else {
				targetLabels = deriveUpstreamSampleLabels(ctx, target)
			}
			for _, dstLabel := range targetLabels {
				res = append(res, builder.Build(ctx.AnalysisID, nodeInstance, buildNodeID(targetIDBase, dstLabel), sourceHandle, targetHandle))
			}
			continue
		}

		res = append(res, builder.Build(ctx.AnalysisID, nodeInstance, targetIDBase, sourceHandle, targetHandle))
	}
	return res
}

func containsLabel(labels []string, target string) bool {
	for _, label := range labels {
		if label == target {
			return true
		}
	}
	return false
}
