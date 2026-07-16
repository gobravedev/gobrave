package utils

import "strings"

func GetScriptFile(scriptType, scriptID string) (string, string, error) {
	resolvedScriptID := strings.TrimSpace(scriptID)
	if resolvedScriptID == "" {
		resolvedScriptID = strings.TrimSpace(scriptID)
	}

	return "pipeline/script/" + resolvedScriptID, mainFileByScriptType(scriptType), nil
}

func mainFileByScriptType(scriptType string) string {
	switch strings.ToLower(strings.TrimSpace(scriptType)) {
	case "r":
		return "main.R"
	case "python":
		return "main.py"
	case "shell":
		return "main.sh"
	case "jupyter":
		return "main.ipynb"
	default:
		return "main.R"
	}
}
