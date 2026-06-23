package main

import (
  "encoding/json"
  "fmt"
  "os"

  "github.com/gobravedev/gobrave/internal/compiler"
)

type payload struct {
  AnalysisID string `json:"analysis_id"`
  Parse map[string]any `json:"parse_analysis_result"`
  Dag map[string]any `json:"dag_definition"`
}

func main() {
  raw, err := os.ReadFile("/home/admin/.brave/debug/dag/bbec2bee-973b-4dd9-956a-98ef5fd09c9e/params.json")
  if err != nil { panic(err) }
  var p payload
  if err := json.Unmarshal(raw, &p); err != nil { panic(err) }
  out, err := compiler.BuildRuntimeTasks(p.AnalysisID, p.Parse, p.Dag)
  if err != nil { panic(err) }
  nodes, _ := out["analysis_nodes"].([]map[string]any)
  edges, _ := out["analysis_edges"].([]map[string]any)
  fmt.Printf("analysis_id=%s nodes=%d edges=%d\n", p.AnalysisID, len(nodes), len(edges))
}
