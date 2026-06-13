package interfaces

import "context"

type WorkbookReadResult struct {
	FilePath     string         `json:"file_path"`
	Format       string         `json:"format"`
	WorkbookData map[string]any `json:"workbook_data"`
}

type WorkbookWriteResult struct {
	FilePath string `json:"file_path"`
	Format   string `json:"format"`
}

// SheetFileService defines local sheet file read/write capabilities.
// Current implementation supports Excel (xlsx), while csv/tsv are reserved for future extension.
type SheetFileService interface {
	ReadWorkbook(ctx context.Context, filePath, format string) (*WorkbookReadResult, error)
	WriteWorkbook(ctx context.Context, filePath, format string, workbookData map[string]any) (*WorkbookWriteResult, error)
}
