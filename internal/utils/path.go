package utils

import (
	"os"
	"path/filepath"
)

const projectDirEnv = "AI_AGENT_GO_DIR"

// ResolveExternalPath resolves project external file paths.
// If AI_AGENT_GO_DIR is set, the path is resolved relative to it;
// otherwise, it is resolved relative to current working directory.
func ResolveExternalPath(relativePath string) (string, error) {
	if filepath.IsAbs(relativePath) {
		return filepath.Abs(relativePath)
	}

	baseDir := os.Getenv(projectDirEnv)
	if baseDir != "" {
		return filepath.Abs(filepath.Join(baseDir, relativePath))
	}

	return filepath.Abs(relativePath)
}
