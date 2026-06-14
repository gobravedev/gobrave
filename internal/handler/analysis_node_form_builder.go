package handler

import (
	"encoding/json"

	"github.com/gobravedev/gobrave/internal/types"
)

func buildNodeFormJSON(dagDefinitionRaw string, module *types.Module, scriptID string) ([]interface{}, error) {
	formJSON := make([]interface{}, 0)

	nodeInDag := make(map[string]interface{})
	if dagDefinitionRaw != "" {
		dagDefinition := make(map[string]interface{})
		if err := json.Unmarshal([]byte(dagDefinitionRaw), &dagDefinition); err != nil {
			return nil, err
		}

		nodes, _ := dagDefinition["nodes"].([]interface{})
		for _, nodeAny := range nodes {
			node, ok := nodeAny.(map[string]interface{})
			if !ok {
				continue
			}
			nodeScriptID, _ := node["script_id"].(string)
			if nodeScriptID == scriptID {
				nodeInDag = node
				break
			}
		}
	}

	ioSchema := make(map[string]interface{})
	if module.IOSchema != "" {
		if err := json.Unmarshal([]byte(module.IOSchema), &ioSchema); err != nil {
			return nil, err
		}
	}
	for k, v := range nodeInDag {
		ioSchema[k] = v
	}

	if module.Content != "" {
		content := make(map[string]interface{})
		if err := json.Unmarshal([]byte(module.Content), &content); err != nil {
			return nil, err
		}
		if contentFormJSON, ok := content["formJson"].([]interface{}); ok {
			formJSON = append(formJSON, contentFormJSON...)
		}
	}

	if params, ok := ioSchema["params"].([]interface{}); ok {
		formJSON = append(formJSON, params...)
	}

	return formJSON, nil
}
