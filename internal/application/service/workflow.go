package service

import (
	"context"
	"encoding/json"

	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
)

type workflowService struct {
	workflowRepo interfaces.WorkflowRepository
}

func NewWorkflowService(workflowRepo interfaces.WorkflowRepository) interfaces.WorkflowService {
	return &workflowService{workflowRepo: workflowRepo}
}

func (s *workflowService) GetWorkflowByWorkflowID(ctx context.Context, workflowID string) (*types.Workflow, error) {
	return s.workflowRepo.GetWorkflowByWorkflowID(ctx, workflowID)
}

func (s *workflowService) GetModuleByModuleID(ctx context.Context, moduleID string) (*types.Module, error) {
	return s.workflowRepo.GetModuleByModuleID(ctx, moduleID)
}

func (s *workflowService) GetModuleContainerSnapshotByModuleID(ctx context.Context, moduleID string) (*types.ModuleContainerSnapshot, error) {
	return s.workflowRepo.GetModuleContainerSnapshotByModuleID(ctx, moduleID)
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
	nodeIncomingHandles := make(map[string]map[string]struct{})
	nodeIDsByModuleID := make(map[string][]string)

	edgesRaw, _ := dagDefinition["edges"].([]any)
	for _, edgeAny := range edgesRaw {
		edge, ok := edgeAny.(map[string]any)
		if !ok {
			continue
		}
		targetNodeID, _ := edge["target"].(string)
		targetHandle, _ := edge["targetHandle"].(string)
		if targetNodeID == "" || targetHandle == "" {
			continue
		}
		if _, exists := nodeIncomingHandles[targetNodeID]; !exists {
			nodeIncomingHandles[targetNodeID] = make(map[string]struct{})
		}
		nodeIncomingHandles[targetNodeID][targetHandle] = struct{}{}
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
		nodeID, _ := node["node_id"].(string)

		nodesMap[moduleID] = node
		moduleIDs = append(moduleIDs, moduleID)
		if nodeID != "" {
			nodeIDsByModuleID[moduleID] = append(nodeIDsByModuleID[moduleID], nodeID)
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

		inputNames := getInputNames(ioSchema)
		nodeIDs := nodeIDsByModuleID[moduleID]
		missingInputNames := make(map[string]struct{})
		for _, nodeID := range nodeIDs {
			incomingHandles := nodeIncomingHandles[nodeID]
			for inputName := range inputNames {
				if _, ok := incomingHandles[inputName]; !ok {
					missingInputNames[inputName] = struct{}{}
				}
			}
		}
		if len(missingInputNames) > 0 {
			inputScriptIDs[moduleID] = struct{}{}
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
			buildInputScriptFormJSON(merged, &formJSONWrap, missingInputNames)
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

func getInputNames(ioSchema map[string]any) map[string]struct{} {
	result := make(map[string]struct{})
	inputs, ok := ioSchema["inputs"].([]any)
	if !ok {
		return result
	}

	for _, itemAny := range inputs {
		item, ok := itemAny.(map[string]any)
		if !ok {
			continue
		}
		name, _ := item["name"].(string)
		if name == "" {
			continue
		}
		result[name] = struct{}{}
	}

	return result
}

func filterInputs(items []any, inputNames map[string]struct{}) []any {
	if inputNames == nil {
		return items
	}

	filtered := make([]any, 0, len(items))
	for _, itemAny := range items {
		item, ok := itemAny.(map[string]any)
		if !ok {
			continue
		}
		name, _ := item["name"].(string)
		if name == "" {
			continue
		}
		if _, exists := inputNames[name]; exists {
			filtered = append(filtered, itemAny)
		}
	}

	return filtered
}

func buildInputScriptFormJSON(ioSchema map[string]any, formJSONWrap *[]any, inputNames map[string]struct{}) {
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
			*formJSONWrap = append(*formJSONWrap, filterInputs(inputs, inputNames)...)
		}
		return
	}

	if inputs, ok := ioSchema["inputs"].([]any); ok {
		*formJSONWrap = append(*formJSONWrap, filterInputs(inputs, inputNames)...)
	}
}
