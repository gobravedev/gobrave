package utils

import (
	"io"
	"os"
	"path/filepath"
)

// CopyDir copies the contents of srcDir into dstDir, preserving the directory structure.
// dstDir is created if it does not exist. Existing files in dstDir may be overwritten.
func CopyDir(srcDir, dstDir string) error {
	// dstDir 不存在则创建
	if _, err := os.Stat(dstDir); os.IsNotExist(err) {
		if err := os.MkdirAll(dstDir, 0o755); err != nil {
			return err
		}
	}

	// Walk the source directory and copy each file/directory to the destination.
	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Compute the relative path from the source root.
		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}

		targetPath := filepath.Join(dstDir, relPath)

		if info.IsDir() {
			return os.MkdirAll(targetPath, info.Mode())
		}

		return copyFile(path, targetPath, info.Mode())
	})
}

// CopyDirectory copies the entire srcDir directory (not just its contents) into dstParentDir.
// For example, CopyDirectory("/a/foo", "/b") creates /b/foo/ with all files/dirs from /a/foo.
func CopyDirectory(srcDir, dstParentDir string) error {
	srcName := filepath.Base(srcDir)
	dstDir := filepath.Join(dstParentDir, srcName)

	if _, err := os.Stat(dstParentDir); os.IsNotExist(err) {
		if err := os.MkdirAll(dstParentDir, 0o755); err != nil {
			return err
		}
	}

	return CopyDir(srcDir, dstDir)
}

func copyFile(src, dst string, perm os.FileMode) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	// Ensure the parent directory exists (in case Walk visited a file before its directory).
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
