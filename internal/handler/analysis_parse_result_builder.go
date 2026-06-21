package handler

import (
	"context"
	"encoding/json"
	stderrs "errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/gobravedev/gobrave/internal/types"
	"github.com/gobravedev/gobrave/internal/types/interfaces"
	"gorm.io/gorm"
)

func buildParseAnalysisResult(ctx context.Context, dataService interfaces.DataService, requestParam map[string]interface{}, formJSONWrap []interface{}) (map[string]interface{}, error) {
	queryNameDict := getQueryDBField(formJSONWrap)
	queryNames := make([]string, 0, len(queryNameDict))
	for _, item := range formJSONWrap {
		formItem, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := formItem["name"].(string)
		if name == "" {
			continue
		}
		if _, ok := queryNameDict[name]; ok {
			queryNames = append(queryNames, name)
		}
	}

	analysisDict, err := buildAnalysisDictFromDB(ctx, dataService, requestParam, queryNames, queryNameDict)
	if err != nil {
		return nil, err
	}

	groupsName := make(map[string]interface{})
	reGroupsName := make(map[string]interface{})
	colors := make(map[string]interface{})
	for _, key := range queryNames {
		value, exists := requestParam[key]
		if !exists {
			continue
		}
		groupsName[key] = getGroupName(value)
		reGroupsName[key] = getReGroupName(value)
		colors[key] = getColor(value)
	}

	formNames := make(map[string]struct{})
	for _, item := range formJSONWrap {
		formItem, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		if itemType, _ := formItem["type"].(string); itemType == "Divider" {
			continue
		}
		name, _ := formItem["name"].(string)
		if name == "" {
			continue
		}
		formNames[name] = struct{}{}
	}

	extraDict := make(map[string]interface{})
	for key, value := range requestParam {
		if _, ok := formNames[key]; ok {
			extraDict[key] = value
			continue
		}
		if strings.HasPrefix(key, "__") {
			extraDict[key] = value
		}
	}

	result := make(map[string]interface{}, len(extraDict)+len(analysisDict)+6)
	for k, v := range extraDict {
		result[k] = v
	}
	for k, v := range analysisDict {
		result[k] = v
	}
	result["analysis_name"] = requestParam["analysis_name"]
	result["colors"] = colors
	result["groups_name"] = groupsName
	result["re_groups_name"] = reGroupsName
	result["groups"] = queryNames
	result["pipeline_dir"] = strings.TrimSpace(os.Getenv("PIPELINE_DIR"))

	return result, nil
}

func buildAnalysisDictFromDB(
	ctx context.Context,
	dataService interfaces.DataService,
	requestParam map[string]interface{},
	queryNames []string,
	queryNameDict map[string]map[string]interface{},
) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	samplesByName := loadSamplesByName(ctx, dataService, requestParam)

	for _, key := range queryNames {
		formItem := queryNameDict[key]
		if strings.TrimSpace(anyToString(formItem["input_type"])) != "file" {
			continue
		}

		rawValue, exists := requestParam[key]
		if !exists {
			continue
		}

		ids, isSingle := extractIDs(rawValue)
		if len(ids) == 0 {
			continue
		}

		items, err := findCompatAnalysisResultByIDs(ctx, dataService, ids)
		if err != nil {
			return nil, fmt.Errorf("resolve %s ids failed: %w", key, err)
		}

		formType := anyToString(formItem["type"])
		selectedGroupName := getGroupName(rawValue)
		reGroupName := getReGroupName(rawValue)

		if formType == "CollectedGroupSelectSampleButton" {
			analysisResult := copyAnyMap(items[0])
			columns := toInterfaceSlice(formItem["columns"])
			analysisResult["form_type"] = formType
			analysisResult["groups"] = columns

			requestMap, _ := rawValue.(map[string]interface{})
			selectedGroupMap, _ := selectedGroupName.(map[string]string)
			reGroupMap, _ := reGroupName.(map[string]interface{})

			for _, groupAny := range columns {
				groupName := anyToString(groupAny)
				if groupName == "" {
					continue
				}

				groupValue := requestMap[groupName]
				switch v := groupValue.(type) {
				case []interface{}:
					groupItems := make([]interface{}, 0, len(v))
					for _, col := range v {
						built := buildCollectedAnalysisResult(col, analysisResult, samplesByName)
						built["selcted_group_name"] = selectedGroupMap[groupName]
						built["re_groups_name"] = reGroupMap[groupName]
						groupItems = append(groupItems, built)
					}
					analysisResult[groupName] = groupItems
				default:
					analysisResult[groupName] = buildCollectedAnalysisResult(v, analysisResult, samplesByName)
				}
			}

			result[key] = analysisResult
			continue
		}

		if formType == "CollectedSampleSelect" {
			analysisResult := copyAnyMap(items[0])
			if requestMap, ok := rawValue.(map[string]interface{}); ok {
				for rk, rv := range requestMap {
					analysisResult[rk] = rv
				}
			}
			analysisResult["form_type"] = formType
			analysisResult["groups"] = toInterfaceSlice(formItem["columns"])
			result[key] = analysisResult
			continue
		}

		if formType == "NestCollectedSampleSelect" {
			itemByID := make(map[string]map[string]interface{}, len(items))
			for _, item := range items {
				itemByID[anyToString(item["id"])] = item
			}

			requestList, _ := rawValue.([]interface{})
			mergedList := make([]interface{}, 0, len(requestList))
			for _, one := range requestList {
				requestOne, ok := one.(map[string]interface{})
				if !ok {
					continue
				}
				merged := copyAnyMap(requestOne)
				fileID := anyToString(requestOne["file"])
				if dbItem, ok := itemByID[fileID]; ok {
					for dk, dv := range dbItem {
						merged[dk] = dv
					}
				}
				mergedList = append(mergedList, merged)
			}
			result[key] = mergedList
			continue
		}

		if isSingle {
			one := copyAnyMap(items[0])
			one["form_type"] = formType
			one["selcted_group_name"] = selectedGroupName
			one["re_groups_name"] = reGroupName
			result[key] = one
			continue
		}

		list := make([]interface{}, 0, len(items))
		for _, item := range items {
			e := copyAnyMap(item)
			e["form_type"] = formType
			e["selcted_group_name"] = selectedGroupName
			e["re_groups_name"] = reGroupName
			list = append(list, e)
		}
		result[key] = list
	}

	return result, nil
}

func findCompatAnalysisResultByIDs(ctx context.Context, dataService interfaces.DataService, ids []string) ([]map[string]interface{}, error) {
	result := make([]map[string]interface{}, 0, len(ids))
	seen := make(map[string]struct{})

	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}

		file, err := findFileByIDOrFileID(ctx, dataService, id)
		if err != nil {
			return nil, err
		}
		if file == nil {
			return nil, fmt.Errorf("data inconsistency: file id %s not found", id)
		}

		result = append(result, buildCompatAnalysisResultItem(file))
	}

	if len(result) != len(seen) {
		return nil, fmt.Errorf("data inconsistency: expected %d files, got %d", len(seen), len(result))
	}

	return result, nil
}

func findFileByIDOrFileID(ctx context.Context, dataService interfaces.DataService, rawID string) (*types.File, error) {
	if n, err := strconv.ParseInt(rawID, 10, 64); err == nil {
		file, err := dataService.GetFileByID(ctx, n)
		if err == nil {
			return file, nil
		}
		if !stderrs.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}

	file, err := dataService.GetFileByFileID(ctx, rawID)
	if err != nil {
		if stderrs.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return file, nil
}

func buildCompatAnalysisResultItem(file *types.File) map[string]interface{} {
	id := strconv.FormatInt(file.ID, 10)
	return map[string]interface{}{
		"id":                 id,
		"analysis_result_id": id,
		"sample_id":          "",
		"file_name":          file.FileName,
		"component_id":       "",
		"file_type":          file.Format,
		"path":               file.Path,
		"format":             file.Format,
		"size":               file.Size,
		"md5":                file.MD5,
		"storage":            file.Storage,
		"description":        file.Description,
	}
}

func loadSamplesByName(ctx context.Context, dataService interfaces.DataService, requestParam map[string]interface{}) map[string]map[string]interface{} {
	projectID := strings.TrimSpace(anyToString(requestParam["project"]))
	if projectID == "" {
		return map[string]map[string]interface{}{}
	}

	samples, err := dataService.ListSampleByProjectID(ctx, projectID)
	if err != nil {
		return map[string]map[string]interface{}{}
	}

	result := make(map[string]map[string]interface{}, len(samples))
	for _, sample := range samples {
		metadata := parseMetadataJSON(sample.Metadata)
		item := map[string]interface{}{
			"sample_id":   sample.SampleID,
			"sample_name": sample.SampleName,
			"subject_id":  sample.SubjectID,
			"group_name":  sample.GroupName,
			"phenotype":   sample.Phenotype,
		}
		for k, v := range metadata {
			item[k] = v
		}
		result[sample.SampleName] = item
	}

	return result
}

func parseMetadataJSON(raw string) map[string]interface{} {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]interface{}{}
	}
	meta := make(map[string]interface{})
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return map[string]interface{}{}
	}
	result := make(map[string]interface{}, len(meta))
	for k, v := range meta {
		if v == nil {
			continue
		}
		result[k] = fmt.Sprint(v)
	}
	return result
}

func buildCollectedAnalysisResult(column interface{}, analysisResult map[string]interface{}, samplesByName map[string]map[string]interface{}) map[string]interface{} {
	columnName := anyToString(column)
	if sample, ok := samplesByName[columnName]; ok {
		result := copyAnyMap(sample)
		result["id"] = analysisResult["id"]
		result["analysis_result_id"] = analysisResult["analysis_result_id"]
		result["columns_name"] = columnName
		return result
	}

	return map[string]interface{}{
		"id":                 analysisResult["id"],
		"sample_name":        columnName,
		"analysis_result_id": analysisResult["analysis_result_id"],
		"columns_name":       columnName,
	}
}

func toInterfaceSlice(v interface{}) []interface{} {
	if items, ok := v.([]interface{}); ok {
		return items
	}
	return []interface{}{}
}

func getQueryDBField(formJSONWrap []interface{}) map[string]map[string]interface{} {
	merged := mergeColumnsByName(formJSONWrap)
	result := make(map[string]map[string]interface{})
	for _, item := range merged {
		name, _ := item["name"].(string)
		if name == "" {
			continue
		}
		dbRaw, exists := item["db"]
		if !exists {
			continue
		}
		dbFlag, ok := dbRaw.(bool)
		if !ok || !dbFlag {
			continue
		}
		result[name] = item
	}
	return result
}

func mergeColumnsByName(formJSONWrap []interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(formJSONWrap))
	sampleMap := make(map[string]map[string]interface{})
	columnsMap := make(map[string][][]interface{})

	for _, item := range formJSONWrap {
		formItem, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		copied := copyAnyMap(formItem)
		result = append(result, copied)
		name, _ := copied["name"].(string)
		itemType, _ := copied["type"].(string)

		switch itemType {
		case "CollectedSampleSelect":
			if name != "" {
				sampleMap[name] = copied
			}
		case "CollectedColumnsSelect":
			if name == "" {
				continue
			}
			if cols, ok := copied["columns"].([]interface{}); ok {
				columnsMap[name] = append(columnsMap[name], cols)
			}
		}
	}

	for name, sample := range sampleMap {
		current, _ := sample["columns"].([]interface{})
		merged := make([]interface{}, 0, len(current)+8)
		merged = append(merged, current...)
		for _, cols := range columnsMap[name] {
			merged = append(merged, cols...)
		}
		sample["columns"] = merged
	}

	return result
}

func getGroupName(value interface{}) interface{} {
	m, ok := value.(map[string]interface{})
	if !ok {
		return "-"
	}

	group, exists := m["group"]
	if !exists {
		return "-"
	}

	if groupDict, ok := group.(map[string]interface{}); ok {
		result := make(map[string]string)
		for k, v := range groupDict {
			items := toStringSlice(v)
			if len(items) == 0 {
				continue
			}
			result[k] = strings.Join(items, "-")
		}
		return result
	}

	items := toStringSlice(group)
	if len(items) == 0 {
		return "-"
	}
	if len(items) == 1 {
		return items[0]
	}
	return strings.Join(items, "-")
}

func getReGroupName(value interface{}) interface{} {
	m, ok := value.(map[string]interface{})
	if !ok {
		return "-"
	}
	groupName, exists := m["group_name"]
	if !exists {
		return "-"
	}
	if groupMap, ok := groupName.(map[string]interface{}); ok {
		result := make(map[string]interface{})
		for k, v := range groupMap {
			s, ok := v.(string)
			if !ok || strings.TrimSpace(s) == "" {
				continue
			}
			result[k] = s
		}
		return result
	}
	return groupName
}

func getColor(value interface{}) interface{} {
	m, ok := value.(map[string]interface{})
	if !ok {
		return "-"
	}
	if color, ok := m["color"]; ok {
		return color
	}
	return "-"
}

func toStringSlice(value interface{}) []string {
	switch v := value.(type) {
	case []string:
		return v
	case []interface{}:
		result := make([]string, 0, len(v))
		for _, item := range v {
			s, ok := item.(string)
			if !ok || strings.TrimSpace(s) == "" {
				continue
			}
			result = append(result, s)
		}
		return result
	default:
		return nil
	}
}

func copyAnyMap(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func extractIDs(value interface{}) ([]string, bool) {
	switch v := value.(type) {
	case map[string]interface{}:
		if fileID, ok := v["file"]; ok {
			id := strings.TrimSpace(anyToString(fileID))
			if id == "" {
				return nil, true
			}
			return []string{id}, true
		}
		if sample, ok := v["sample"]; ok {
			ids := extractIDList(sample)
			if len(ids) == 1 {
				return ids, true
			}
			return ids, false
		}
		if val, ok := v["value"]; ok {
			id := strings.TrimSpace(anyToString(val))
			if id == "" {
				return nil, true
			}
			return []string{id}, true
		}
		return nil, false
	case []interface{}:
		ids := make([]string, 0, len(v))
		for _, item := range v {
			m, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			if fileID, ok := m["file"]; ok {
				id := strings.TrimSpace(anyToString(fileID))
				if id != "" {
					ids = append(ids, id)
				}
				continue
			}
			if sample, ok := m["sample"]; ok {
				ids = append(ids, extractIDList(sample)...)
				continue
			}
			if val, ok := m["value"]; ok {
				id := strings.TrimSpace(anyToString(val))
				if id != "" {
					ids = append(ids, id)
				}
			}
		}
		return ids, false
	default:
		id := strings.TrimSpace(anyToString(value))
		if id == "" {
			return nil, true
		}
		return []string{id}, true
	}
}

func extractIDList(value interface{}) []string {
	switch v := value.(type) {
	case []interface{}:
		result := make([]string, 0, len(v))
		for _, item := range v {
			id := strings.TrimSpace(anyToString(item))
			if id == "" {
				continue
			}
			result = append(result, id)
		}
		return result
	default:
		id := strings.TrimSpace(anyToString(v))
		if id == "" {
			return nil
		}
		return []string{id}
	}
}

func anyToString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatInt(int64(x), 10)
	default:
		if v == nil {
			return ""
		}
		return fmt.Sprint(v)
	}
}
