package compiler

import "fmt"

type RuntimeCompiler struct {
	stages []Stage
}

func NewRuntimeCompiler() *RuntimeCompiler {
	return &RuntimeCompiler{
		stages: []Stage{
			&TopologyStage{},
			&ClassifyStage{},
			&ExpandStage{},
			&ResolveStage{},
			&OutputStage{},
			&EdgeStage{},
			&DecorateStage{},
		},
	}
}

func (c *RuntimeCompiler) Compile(analysisID int64, params map[string]any, dagDefinition map[string]any) (map[string]any, error) {
	ctx := NewCompileContext(analysisID, params, dagDefinition)
	for _, stage := range c.stages {
		if ctx.Abort {
			break
		}
		if err := stage.Run(ctx); err != nil {
			return nil, fmt.Errorf("%s failed: %w", stage.Name(), err)
		}
	}
	return map[string]any{
		"analysis_nodes": ctx.AnalysisNodes,
		"analysis_edges": ctx.AnalysisEdges,
	}, nil
}

func BuildRuntimeTasks(analysisID int64, params map[string]any, dagDefinition map[string]any) (map[string]any, error) {
	return NewRuntimeCompiler().Compile(analysisID, params, dagDefinition)
}

// func build_runtime_tasks(analysisID string, params map[string]any, dagDefinition map[string]any) (map[string]any, error) {
// 	return BuildRuntimeTasks(analysisID, params, dagDefinition)
// }
