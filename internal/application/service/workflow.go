package service

import (
	"context"
	"encoding/json"

	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type workflowService struct {
	workflowRepo interfaces.WorkflowRepository
}

func NewWorkflowService(workflowRepo interfaces.WorkflowRepository) interfaces.WorkflowService {
	return &workflowService{workflowRepo: workflowRepo}
}

func (s *workflowService) GetFormJSONByWorkflowID(ctx context.Context, workflowID string) ([]any, error) {
	findWorkflow, err := s.workflowRepo.GetWorkflowByWorkflowID(ctx, workflowID)
	if err != nil {
		return nil, err
	}

	formJSONWrap := make([]any, 0)
	if findWorkflow.DagDefinition == "" {
		return formJSONWrap, nil
	}

	var dagDefinition map[string]any
	if err := json.Unmarshal([]byte(findWorkflow.DagDefinition), &dagDefinition); err != nil {
		return formJSONWrap, nil
	}

	nodesRaw, ok := dagDefinition["nodes"].([]any)
	if !ok {
		return formJSONWrap, nil
	}

	nodesMap := make(map[string]map[string]any)
	inputScriptIDs := make(map[string]struct{})
	targetNodeIDs := make(map[string]struct{})

	edgesRaw, _ := dagDefinition["edges"].([]any)
	for _, edgeAny := range edgesRaw {
		edge, ok := edgeAny.(map[string]any)
		if !ok {
			continue
		}
		target, _ := edge["target"].(string)
		if target != "" {
			targetNodeIDs[target] = struct{}{}
		}
	}

	moduleIDs := make([]string, 0)
	for _, nodeAny := range nodesRaw {
		node, ok := nodeAny.(map[string]any)
		if !ok {
			continue
		}
		moduleID, _ := node["script_id"].(string)
		if moduleID == "" {
			continue
		}
		nodesMap[moduleID] = node
		moduleIDs = append(moduleIDs, moduleID)
		if _, exists := targetNodeIDs[moduleID]; !exists {
			inputScriptIDs[moduleID] = struct{}{}
		}
	}

	if len(moduleIDs) == 0 {
		return formJSONWrap, nil
	}

	scripts, err := s.workflowRepo.FindModulesByModuleIDs(ctx, moduleIDs)
	if err != nil {
		return nil, err
	}

	for _, script := range scripts {
		moduleID := script.ModuleID
		ioSchema := make(map[string]any)
		if script.IOSchema != "" {
			_ = json.Unmarshal([]byte(script.IOSchema), &ioSchema)
		}

		if _, isInputScript := inputScriptIDs[moduleID]; isInputScript && script.IOSchema != "" {
			merged := make(map[string]any, len(ioSchema)+4)
			for k, v := range ioSchema {
				merged[k] = v
			}
			if node, exists := nodesMap[moduleID]; exists {
				for k, v := range node {
					merged[k] = v
				}
			}
			buildInputScriptFormJSON(merged, &formJSONWrap)
		}

		if params, ok := ioSchema["params"].([]any); ok {
			formJSONWrap = append(formJSONWrap, params...)
		}

		if script.Content != "" {
			var content map[string]any
			if err := json.Unmarshal([]byte(script.Content), &content); err == nil {
				if contentFormJSON, ok := content["formJson"].([]any); ok {
					formJSONWrap = append(formJSONWrap, contentFormJSON...)
				}
			}
		}
	}

	return formJSONWrap, nil
}

func buildInputScriptFormJSON(ioSchema map[string]any, formJSONWrap *[]any) {
	if scatterAny, ok := ioSchema["scatter"]; ok {
		scatter, ok := scatterAny.(map[string]any)
		if !ok {
			return
		}
		if mode, _ := scatter["mode"].(string); mode == "each" {
			if workflow, ok := ioSchema["workflow"].([]any); ok {
				*formJSONWrap = append(*formJSONWrap, workflow...)
			}
			return
		}
		if inputs, ok := ioSchema["inputs"].([]any); ok {
			*formJSONWrap = append(*formJSONWrap, inputs...)
		}
		return
	}

	if inputs, ok := ioSchema["inputs"].([]any); ok {
		*formJSONWrap = append(*formJSONWrap, inputs...)
	}
}
