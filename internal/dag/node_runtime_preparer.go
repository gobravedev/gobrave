package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/flosch/pongo2/v6"
	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"github.com/gobravedev/gobrave/internal/utils"
)

type NodeRuntimePreparer interface {
	Prepare(ctx context.Context, node *types.AnalysisNode) error
}

type NoopNodeRuntimePreparer struct{}

func (NoopNodeRuntimePreparer) Prepare(context.Context, *types.AnalysisNode) error {
	return nil
}

type RunScriptBuilder interface {
	Build(node *types.AnalysisNode, scriptPath string, scriptContent string, params map[string]any) (string, error)
}

type RScriptBuilder struct{}

func (RScriptBuilder) Build(node *types.AnalysisNode, scriptPath string, _ string, _ map[string]any) (string, error) {
	return fmt.Sprintf("#!/usr/bin/env bash\nset -euo pipefail\nRscript %q %q %q\n", scriptPath, node.ParamsPath, node.OutputDir), nil
}

type PythonScriptBuilder struct{}

func (PythonScriptBuilder) Build(node *types.AnalysisNode, scriptPath string, _ string, _ map[string]any) (string, error) {
	return fmt.Sprintf("#!/usr/bin/env bash\nset -euo pipefail\npython %q %q %q\n", scriptPath, node.ParamsPath, node.OutputDir), nil
}

type ShellScriptBuilder struct{}

func (ShellScriptBuilder) Build(_ *types.AnalysisNode, scriptPath string, scriptContent string, params map[string]any) (string, error) {
	rendered, err := renderShellTemplate(scriptContent, params)
	if err != nil {
		return "", err
	}
	return rendered + "\n\n#" + scriptPath + "\n", nil
}

type FileSystemNodeRuntimePreparer struct {
	analysisRepo interfaces.AnalysisRepository
	workflowRepo interfaces.WorkflowRepository
	projectRepo  interfaces.ProjectRepository
	storageBase  string
	builders     map[string]RunScriptBuilder
}

func NewFileSystemNodeRuntimePreparer(
	analysisRepo interfaces.AnalysisRepository,
	workflowRepo interfaces.WorkflowRepository,
	projectRepo interfaces.ProjectRepository,
	storageBase string,
) *FileSystemNodeRuntimePreparer {
	return &FileSystemNodeRuntimePreparer{
		analysisRepo: analysisRepo,
		workflowRepo: workflowRepo,
		projectRepo:  projectRepo,
		storageBase:  strings.TrimSpace(storageBase),
		builders: map[string]RunScriptBuilder{
			"r":      RScriptBuilder{},
			"python": PythonScriptBuilder{},
			"shell":  ShellScriptBuilder{},
		},
	}
}

func (p *FileSystemNodeRuntimePreparer) Prepare(ctx context.Context, node *types.AnalysisNode) error {
	if node == nil {
		return fmt.Errorf("analysis node is nil")
	}
	if strings.TrimSpace(node.AnalysisID) == "" {
		return fmt.Errorf("analysis_id is required")
	}
	if strings.TrimSpace(node.AnalysisNodeID) == "" {
		return fmt.Errorf("analysis_node_id is required")
	}
	if node.ScriptID == 0 {
		return fmt.Errorf("script_id is required")
	}

	analysis, err := p.analysisRepo.GetAnalysisByAnalysisID(ctx, node.AnalysisID)
	if err != nil {
		return fmt.Errorf("load analysis failed: %w", err)
	}

	if err := p.ensureNodePaths(node, analysis); err != nil {
		return err
	}
	if err := os.MkdirAll(node.WorkspaceDir, 0o755); err != nil {
		return fmt.Errorf("create workspace dir failed: %w", err)
	}
	if err := os.MkdirAll(node.OutputDir, 0o755); err != nil {
		return fmt.Errorf("create output dir failed: %w", err)
	}
	if err := cleanDirContents(node.OutputDir); err != nil {
		return fmt.Errorf("clean output dir failed: %w", err)
	}

	params, err := p.buildNodeParams(node, analysis)
	if err != nil {
		return err
	}
	if err := writeJSONAtomic(node.ParamsPath, params, 0o644); err != nil {
		return fmt.Errorf("write params json failed: %w", err)
	}

	script, err := p.workflowRepo.GetScriptByID(ctx, node.ScriptID)
	if err != nil {
		return fmt.Errorf("load script failed: %w", err)
	}
	scriptType := normalizeScriptType(script.ScriptType)
	builder := p.builders[scriptType]
	if builder == nil {
		builder = p.builders["shell"]
	}

	// scriptPath := p.resolveScriptPath(script.ScriptID, scriptType)
	project, err := p.projectRepo.GetProjectByID(ctx, script.ProjectID)
	if err != nil {
		return err
	}
	scriptDir, scriptFile, _ := utils.GetScriptFile(p.storageBase, project.ProjectID, script.ScriptType, script.ScriptID)
	scriptPath := filepath.Join(scriptDir, scriptFile)
	scriptContent, err := os.ReadFile(scriptPath)
	if err != nil {
		return fmt.Errorf("read script file failed: %w", err)
	}

	runScript, err := builder.Build(node, scriptPath, string(scriptContent), params)
	if err != nil {
		return fmt.Errorf("build run script failed: %w", err)
	}
	if err := writeTextAtomic(node.CommandPath, runScript, 0o755); err != nil {
		return fmt.Errorf("write run.sh failed: %w", err)
	}

	return nil
}

func (p *FileSystemNodeRuntimePreparer) ensureNodePaths(node *types.AnalysisNode, analysis *types.Analysis) error {
	baseWorkspace := strings.TrimSpace(node.WorkspaceDir)
	if baseWorkspace == "" {
		analysisOutputDir := ""
		if analysis != nil {
			analysisOutputDir = strings.TrimSpace(analysis.OutputDir)
		}
		if analysisOutputDir == "" {
			return fmt.Errorf("node workspace_dir is empty and analysis output_dir is empty")
		}
		baseWorkspace = filepath.Join(analysisOutputDir, node.AnalysisNodeID)
		node.WorkspaceDir = baseWorkspace
	}

	if strings.TrimSpace(node.OutputDir) == "" {
		node.OutputDir = filepath.Join(baseWorkspace, "output")
	}
	if strings.TrimSpace(node.ParamsPath) == "" {
		node.ParamsPath = filepath.Join(baseWorkspace, "params.json")
	}
	if strings.TrimSpace(node.CommandPath) == "" {
		node.CommandPath = filepath.Join(baseWorkspace, "run.sh")
	}

	if err := os.MkdirAll(filepath.Dir(node.ParamsPath), 0o755); err != nil {
		return fmt.Errorf("create params dir failed: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(node.CommandPath), 0o755); err != nil {
		return fmt.Errorf("create command dir failed: %w", err)
	}
	return nil
}

func (p *FileSystemNodeRuntimePreparer) buildNodeParams(node *types.AnalysisNode, analysis *types.Analysis) (map[string]any, error) {
	baseParams := map[string]any{}
	if analysis != nil && strings.TrimSpace(analysis.ParamsPath) != "" {
		raw, err := os.ReadFile(analysis.ParamsPath)
		if err != nil {
			return nil, fmt.Errorf("read analysis params failed: %w", err)
		}
		if len(strings.TrimSpace(string(raw))) > 0 {
			if err := json.Unmarshal(raw, &baseParams); err != nil {
				return nil, fmt.Errorf("parse analysis params failed: %w", err)
			}
		}
	}

	resolvedInputs := map[string]any(node.ResolvedInputs)

	merged := map[string]any{}
	for k, v := range baseParams {
		merged[k] = v
	}
	for k, v := range resolvedInputs {
		merged[k] = v
	}
	merged["output_dir"] = node.OutputDir

	return merged, nil
}

func (p *FileSystemNodeRuntimePreparer) resolveScriptPath(scriptID string, scriptType string) string {
	mainFile := mainFileByScriptType(scriptType)
	if strings.TrimSpace(p.storageBase) == "" {
		return filepath.Join("pipeline", "script", scriptID, mainFile)
	}
	return filepath.Join(p.storageBase, "pipeline", "script", scriptID, mainFile)
}

func normalizeScriptType(scriptType string) string {
	typeName := strings.ToLower(strings.TrimSpace(scriptType))
	switch typeName {
	case "", "jupyter", "bash", "sh":
		return "shell"
	default:
		return typeName
	}
}

func mainFileByScriptType(scriptType string) string {
	switch normalizeScriptType(scriptType) {
	case "r":
		return "main.R"
	case "python":
		return "main.py"
	case "shell":
		return "main.sh"
	default:
		return "main.sh"
	}
}

func cleanDirContents(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func writeJSONAtomic(path string, value any, mode os.FileMode) error {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	content = append(content, '\n')
	return writeBytesAtomic(path, content, mode)
}

func writeTextAtomic(path string, content string, mode os.FileMode) error {
	return writeBytesAtomic(path, []byte(content), mode)
}

func writeBytesAtomic(path string, content []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}

	if _, err := tmp.Write(content); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Chmod(tmpPath, mode); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func renderShellTemplate(content string, params map[string]any) (string, error) {
	tpl, err := pongo2.FromString(content)
	if err != nil {
		return "", fmt.Errorf("parse shell template failed: %w", err)
	}

	ctx := pongo2.Context{}
	for k, v := range params {
		ctx[k] = v
	}

	if meta, ok := templateAsMap(ctx["meta"]); ok {
		if _, exists := ctx["meta_file_name"]; !exists {
			if fileName, ok := meta["file_name"]; ok {
				ctx["meta_file_name"] = fileName
			}
		}
	}

	rendered, err := tpl.Execute(ctx)
	if err != nil {
		return "", fmt.Errorf("render shell template failed: %w", err)
	}
	return rendered, nil
}

func templateAsMap(v any) (map[string]any, bool) {
	if v == nil {
		return nil, false
	}
	m, ok := v.(map[string]any)
	return m, ok
}
