package compiler

import "testing"

func TestTopologyStage_NormalizesEdgeNodeAliases(t *testing.T) {
	ctx := NewCompileContext("analysis-1", map[string]any{}, map[string]any{
		"nodes": []any{
			map[string]any{
				"id":      "A",
				"node_id": "A_1",
				"name":    "node-a",
			},
			map[string]any{
				"id":      "B",
				"node_id": "B_1",
				"name":    "node-b",
			},
		},
		"edges": []any{
			map[string]any{
				"source":       "A_1",
				"target":       "B_1",
				"sourceHandle": "tsv",
				"targetHandle": "tsv",
			},
		},
	})

	stage := &TopologyStage{}
	if err := stage.Run(ctx); err != nil {
		t.Fatalf("TopologyStage.Run failed: %v", err)
	}

	if len(ctx.Outgoing["A"]) != 1 {
		t.Fatalf("expected 1 outgoing edge for A, got %d", len(ctx.Outgoing["A"]))
	}
	if len(ctx.Incoming["B"]) != 1 {
		t.Fatalf("expected 1 incoming edge for B, got %d", len(ctx.Incoming["B"]))
	}

	edge := ctx.Outgoing["A"][0]
	if got := edge["source"]; got != "A" {
		t.Fatalf("expected normalized source=A, got %v", got)
	}
	if got := edge["target"]; got != "B" {
		t.Fatalf("expected normalized target=B, got %v", got)
	}
}
