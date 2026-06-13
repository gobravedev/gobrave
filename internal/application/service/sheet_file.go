package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/gobravedev/gobrave/internal/config"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"github.com/gobravedev/gobrave/internal/utils"
	"github.com/xuri/excelize/v2"
)

var (
	ErrUnsupportedSheetFormat = errors.New("unsupported sheet format")
	ErrInvalidWorkbookData    = errors.New("invalid workbook data")
)

type sheetFileService struct {
	baseDir string
}

type workbookSnapshot struct {
	ID         string                       `json:"id"`
	Name       string                       `json:"name"`
	SheetOrder []string                     `json:"sheetOrder"`
	Sheets     map[string]worksheetSnapshot `json:"sheets"`
}

type worksheetSnapshot struct {
	ID       string                             `json:"id"`
	Name     string                             `json:"name"`
	CellData map[string]map[string]cellSnapshot `json:"cellData"`
}

type cellSnapshot struct {
	V any    `json:"v"`
	F string `json:"f"`
}

func NewSheetFileService(cfg *config.Config) interfaces.SheetFileService {
	baseDir := ""
	if cfg != nil && cfg.Storage != nil {
		baseDir = strings.TrimSpace(cfg.Storage.BaseDir)
	}

	resolvedBase, err := utils.ResolveConfiguredPath(baseDir, "sheets")
	if err != nil {
		resolvedBase = "sheets"
	}

	return &sheetFileService{baseDir: resolvedBase}
}

func (s *sheetFileService) ReadWorkbook(ctx context.Context, filePath, format string) (*interfaces.WorkbookReadResult, error) {
	_ = ctx
	resolvedPath, normalizedFormat, err := s.resolveTargetPath(filePath, format)
	if err != nil {
		return nil, err
	}

	switch normalizedFormat {
	case "xlsx":
		workbookData, err := s.readExcelWorkbook(resolvedPath)
		if err != nil {
			return nil, err
		}
		return &interfaces.WorkbookReadResult{
			FilePath:     resolvedPath,
			Format:       normalizedFormat,
			WorkbookData: workbookData,
		}, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedSheetFormat, normalizedFormat)
	}
}

func (s *sheetFileService) WriteWorkbook(ctx context.Context, filePath, format string, workbookData map[string]any) (*interfaces.WorkbookWriteResult, error) {
	_ = ctx
	resolvedPath, normalizedFormat, err := s.resolveTargetPath(filePath, format)

	if err != nil {
		return nil, err
	}

	switch normalizedFormat {
	case "xlsx":
		if err := s.writeExcelWorkbook(resolvedPath, workbookData); err != nil {
			return nil, err
		}
		return &interfaces.WorkbookWriteResult{
			FilePath: resolvedPath,
			Format:   normalizedFormat,
		}, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedSheetFormat, normalizedFormat)
	}
}

func (s *sheetFileService) resolveTargetPath(filePath, format string) (string, string, error) {
	return filePath, format, nil
	// filePath = strings.TrimSpace(filePath)
	// if filePath == "" {
	// 	return "", "", fmt.Errorf("file path is required")
	// }

	// baseDir, err := filepath.Abs(filepath.Clean(s.baseDir))
	// if err != nil {
	// 	return "", "", err
	// }
	// if err := os.MkdirAll(baseDir, 0o755); err != nil {
	// 	return "", "", err
	// }

	// candidate := filePath
	// if !filepath.IsAbs(candidate) {
	// 	candidate = filepath.Join(baseDir, candidate)
	// }

	// resolvedPath, err := utils.SafePathUnderBase(baseDir, candidate)
	// if err != nil {
	// 	return "", "", err
	// }

	// resolvedFormat, err := normalizeSheetFormat(format, resolvedPath)
	// if err != nil {
	// 	return "", "", err
	// }

	// return resolvedPath, resolvedFormat, nil
}

func (s *sheetFileService) readExcelWorkbook(filePath string) (map[string]any, error) {
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sheetNames := f.GetSheetList()
	if len(sheetNames) == 0 {
		sheetNames = []string{"Sheet1"}
	}

	workbookName := strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath))
	if workbookName == "" {
		workbookName = "Workbook"
	}

	workbookID := sanitizeIdentifier(workbookName, "workbook")
	sheets := make(map[string]any, len(sheetNames))
	sheetOrder := make([]string, 0, len(sheetNames))

	for idx, name := range sheetNames {
		sheetID := fmt.Sprintf("sheet-%02d", idx+1)
		sheetOrder = append(sheetOrder, sheetID)

		rows, err := f.GetRows(name)
		if err != nil {
			return nil, err
		}

		cellData := make(map[string]any)
		maxColCount := 0
		for rowIndex, row := range rows {
			if len(row) > maxColCount {
				maxColCount = len(row)
			}

			rowData := make(map[string]any)
			for colIndex, cellValue := range row {
				if strings.TrimSpace(cellValue) == "" {
					continue
				}
				rowData[strconv.Itoa(colIndex)] = map[string]any{"v": cellValue}
			}

			if len(rowData) > 0 {
				cellData[strconv.Itoa(rowIndex)] = rowData
			}
		}

		rowCount := len(rows)
		if rowCount < 200 {
			rowCount = 200
		}
		columnCount := maxColCount
		if columnCount < 26 {
			columnCount = 26
		}

		sheets[sheetID] = map[string]any{
			"id":          sheetID,
			"name":        name,
			"rowCount":    rowCount,
			"columnCount": columnCount,
			"cellData":    cellData,
		}
	}

	return map[string]any{
		"id":         workbookID,
		"name":       workbookName,
		"sheetOrder": sheetOrder,
		"sheets":     sheets,
	}, nil
}

func (s *sheetFileService) writeExcelWorkbook(filePath string, workbookData map[string]any) error {
	if len(workbookData) == 0 {
		return ErrInvalidWorkbookData
	}

	var snapshot workbookSnapshot
	raw, err := json.Marshal(workbookData)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidWorkbookData, err)
	}
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidWorkbookData, err)
	}
	if len(snapshot.Sheets) == 0 {
		return fmt.Errorf("%w: no sheets", ErrInvalidWorkbookData)
	}

	order := snapshot.SheetOrder
	if len(order) == 0 {
		order = make([]string, 0, len(snapshot.Sheets))
		for sheetID := range snapshot.Sheets {
			order = append(order, sheetID)
		}
		sort.Strings(order)
	}

	f := excelize.NewFile()
	createdAnySheet := false

	for _, sheetID := range order {
		sheet, ok := snapshot.Sheets[sheetID]
		if !ok {
			continue
		}

		sheetName := strings.TrimSpace(sheet.Name)
		if sheetName == "" {
			sheetName = sheetID
		}

		if !createdAnySheet {
			f.SetSheetName("Sheet1", sheetName)
			createdAnySheet = true
		} else {
			if _, err := f.NewSheet(sheetName); err != nil {
				return err
			}
		}

		for rowKey, row := range sheet.CellData {
			rowIndex, err := strconv.Atoi(rowKey)
			if err != nil || rowIndex < 0 {
				continue
			}

			for colKey, cell := range row {
				colIndex, err := strconv.Atoi(colKey)
				if err != nil || colIndex < 0 {
					continue
				}

				cellAddress, err := excelize.CoordinatesToCellName(colIndex+1, rowIndex+1)
				if err != nil {
					continue
				}

				if strings.TrimSpace(cell.F) != "" {
					formula := strings.TrimPrefix(strings.TrimSpace(cell.F), "=")
					if formula != "" {
						if err := f.SetCellFormula(sheetName, cellAddress, formula); err != nil {
							return err
						}
					}
					continue
				}

				if cell.V == nil {
					continue
				}

				if err := f.SetCellValue(sheetName, cellAddress, normalizeCellValue(cell.V)); err != nil {
					return err
				}
			}
		}
	}

	if !createdAnySheet {
		return fmt.Errorf("%w: no valid sheet in workbook", ErrInvalidWorkbookData)
	}

	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return err
	}
	return f.SaveAs(filePath)
}

func normalizeSheetFormat(format, filePath string) (string, error) {
	fmtNormalized := strings.TrimSpace(strings.ToLower(format))
	if fmtNormalized == "" {
		ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(filePath), "."))
		fmtNormalized = ext
	}

	switch fmtNormalized {
	case "xlsx":
		return fmtNormalized, nil
	case "csv", "tsv":
		return "", fmt.Errorf("%w: %s", ErrUnsupportedSheetFormat, fmtNormalized)
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedSheetFormat, fmtNormalized)
	}
}

func sanitizeIdentifier(name, fallback string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return fallback
	}

	var b strings.Builder
	for _, ch := range trimmed {
		switch {
		case ch >= 'a' && ch <= 'z':
			b.WriteRune(ch)
		case ch >= 'A' && ch <= 'Z':
			b.WriteRune(ch + 32)
		case ch >= '0' && ch <= '9':
			b.WriteRune(ch)
		case ch == '-' || ch == '_':
			b.WriteRune(ch)
		default:
			b.WriteRune('-')
		}
	}

	result := strings.Trim(b.String(), "-_")
	if result == "" {
		return fallback
	}
	return result
}

func normalizeCellValue(v any) any {
	switch val := v.(type) {
	case string, float64, bool, int, int32, int64, uint, uint32, uint64:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}
