package compiler

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

const defaultDebugAnalysisID = "bbec2bee-973b-4dd9-956a-98ef5fd09c9e"

type debugParamsPayload struct {
	AnalysisID          string         `json:"analysis_id"`
	ParseAnalysisResult map[string]any `json:"parse_analysis_result"`
	DagDefinition       map[string]any `json:"dag_definition"`
}

func TestBuildRuntimeTasks_DebugFromLocalParams(t *testing.T) {
	analysisID := os.Getenv("BRAVE_DAG_ANALYSIS_ID")
	if analysisID == "" {
		analysisID = defaultDebugAnalysisID
	}

	baseDir := os.Getenv("BRAVE_DAG_BASE_DIR")
	if baseDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatalf("resolve home dir: %v", err)
		}
		baseDir = filepath.Join(home, ".brave", "dag")
	}

	paramsPath := filepath.Join(baseDir, analysisID, "params.json")
	raw, err := os.ReadFile(paramsPath)
	if err != nil {
		t.Skipf("skip debug test: cannot read params file %s: %v", paramsPath, err)
	}

	var payload debugParamsPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal %s: %v", paramsPath, err)
	}

	if payload.AnalysisID != "" {
		analysisID = payload.AnalysisID
	}

	runtimeDAG, err := BuildRuntimeTasks(analysisID, payload.ParseAnalysisResult, payload.DagDefinition)
	if err != nil {
		t.Fatalf("BuildRuntimeTasks failed: %v", err)
	}

	nodes, _ := runtimeDAG["analysis_nodes"].([]map[string]any)
	if len(nodes) == 0 {
		t.Fatalf("analysis_nodes is empty, runtimeDAG=%v", runtimeDAG)
	}

	edges, _ := runtimeDAG["analysis_edges"].([]map[string]any)
	if len(edges) == 0 {
		t.Fatalf("analysis_edges is empty, runtimeDAG=%v", runtimeDAG)
	}
}
