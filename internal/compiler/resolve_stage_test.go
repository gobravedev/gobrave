package compiler

import "testing"

func TestBuildRuntimeTasks_ScatterEachMapsPluralFieldToSingularInput(t *testing.T) {
	runtimeDAG, err := BuildRuntimeTasks("analysis-1", map[string]any{
		"seuratObjects": []any{
			map[string]any{"file_name": "sample-1.rds", "sample_id": "s1"},
			map[string]any{"file_name": "sample-2.rds", "sample_id": "s2"},
		},
	}, map[string]any{
		"nodes": []any{
			map[string]any{
				"id":   "node-1",
				"name": "Seurat",
				"inputs": map[string]any{
					"seuratObject": map[string]any{
						"type":     "NestSelectSample",
						"required": true,
					},
				},
				"outputs": map[string]any{
					"summary": map[string]any{"pattern": "output.md", "required": true, "type": "file"},
				},
				"scatter": map[string]any{
					"mode":  "each",
					"field": "seuratObjects",
				},
			},
		},
		"edges": []any{},
	})
	if err != nil {
		t.Fatalf("BuildRuntimeTasks failed: %v", err)
	}

	nodesAny, ok := runtimeDAG["analysis_nodes"].([]map[string]any)
	if !ok {
		t.Fatalf("analysis_nodes type mismatch: %T", runtimeDAG["analysis_nodes"])
	}
	if len(nodesAny) != 2 {
		t.Fatalf("expected 2 analysis nodes, got %d", len(nodesAny))
	}

	for i, node := range nodesAny {
		resolvedInputs, ok := node["resolved_inputs"].(map[string]any)
		if !ok {
			t.Fatalf("node %d resolved_inputs type mismatch: %T", i, node["resolved_inputs"])
		}
		if _, exists := resolvedInputs["seuratObject"]; !exists {
			t.Fatalf("node %d missing resolved_inputs.seuratObject", i)
		}
	}
}
