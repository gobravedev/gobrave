package utils

import (
	"path/filepath"
	"strings"
)

func GetScriptFile(baseDir, projectID, scriptType, scriptID string) (string, string, error) {
	resolvedScriptID := strings.TrimSpace(scriptID)
	if resolvedScriptID == "" {
		resolvedScriptID = strings.TrimSpace(scriptID)
	}

	// return "pipeline/script/" + resolvedScriptID, mainFileByScriptType(scriptType), nil
	scriptDir := GetScriptDir(baseDir, projectID)
	return filepath.Join(scriptDir, scriptID), mainFileByScriptType(scriptType), nil
}

func GetScriptDir(baseDir, projectId string) string {
	return filepath.Join(baseDir, "data", projectId, "pipeline", "script")
}
func GetWorkflowDir(baseDir, projectId string) string {
	return filepath.Join(baseDir, "data", projectId, "pipeline", "workflow")
}
func GetAnalysisDir(baseDir, projectId string) string {
	return filepath.Join(baseDir, "data", projectId, "analysis")
}
func GetAnalysisNodeDir(baseDir, projectId string) string {
	return filepath.Join(baseDir, "data", projectId, "analysis_node")
}

func GetProjectDir(baseDir, projectId string) string {
	return filepath.Join(baseDir, "data", projectId)
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
	case "qmd":
		return "main.qmd"
	default:
		return "main.R"
	}
}
