package types

import "time"

// Analysis maps to Python brave's nextflow table.
type Analysis struct {
	ID                  uint      `json:"id" gorm:"primaryKey;autoIncrement"`
	ProjectID           string    `json:"project" gorm:"column:project;type:varchar(255)"`
	AnalysisID          string    `json:"analysis_id" gorm:"column:analysis_id;type:varchar(255)"`
	ComponentID         string    `json:"component_id" gorm:"column:component_id;type:varchar(255)"`
	WorkflowID          string    `json:"relation_id" gorm:"column:relation_id;type:varchar(255)"`
	AnalysisName        string    `json:"analysis_name" gorm:"column:analysis_name;type:varchar(255)"`
	InputFile           string    `json:"input_file" gorm:"column:input_file;type:varchar(255)"`
	AnalysisMethod      string    `json:"analysis_method" gorm:"column:analysis_method;type:varchar(255)"`
	WorkDir             string    `json:"work_dir" gorm:"column:work_dir;type:varchar(255)"`
	ParamsPath          string    `json:"params_path" gorm:"column:params_path;type:varchar(255)"`
	CommandPath         string    `json:"command_path" gorm:"column:command_path;type:varchar(255)"`
	RequestParam        string    `json:"request_param" gorm:"column:request_param;type:longtext"`
	OutputFormat        string    `json:"output_format" gorm:"column:output_format;type:longtext"`
	OutputDir           string    `json:"output_dir" gorm:"column:output_dir;type:varchar(255)"`
	PipelineScript      string    `json:"pipeline_script" gorm:"column:pipeline_script;type:varchar(255)"`
	ParseAnalysisModule string    `json:"parse_analysis_module" gorm:"column:parse_analysis_module;type:varchar(255)"`
	TraceFile           string    `json:"trace_file" gorm:"column:trace_file;type:varchar(255)"`
	WorkflowLogFile     string    `json:"workflow_log_file" gorm:"column:workflow_log_file;type:varchar(255)"`
	ExecutorLogFile     string    `json:"executor_log_file" gorm:"column:executor_log_file;type:varchar(255)"`
	ProcessID           string    `json:"process_id" gorm:"column:process_id;type:varchar(255)"`
	ScriptConfigFile    string    `json:"script_config_file" gorm:"column:script_config_file;type:varchar(255)"`
	JobID               string    `json:"job_id" gorm:"column:job_id;type:varchar(255)"`
	Ports               string    `json:"ports" gorm:"column:ports;type:varchar(255)"`
	URL                 string    `json:"url" gorm:"column:url;type:varchar(255)"`
	JobStatus           string    `json:"job_status" gorm:"column:job_status;type:varchar(255)"`
	ServerStatus        string    `json:"server_status" gorm:"column:server_status;type:varchar(255)"`
	CommandLogPath      string    `json:"command_log_path" gorm:"column:command_log_path;type:varchar(255)"`
	IsReport            bool      `json:"is_report" gorm:"column:is_report;default:false"`
	IsCache             bool      `json:"is_cache" gorm:"column:is_cache;default:false"`
	Used                bool      `json:"used" gorm:"column:used;default:true"`
	DataComponentIDs    string    `json:"data_component_ids" gorm:"column:data_component_ids;type:text"`
	ExtraProjectIDs     string    `json:"extra_project_ids" gorm:"column:extra_project_ids;type:longtext"`
	CreatedAt           time.Time `json:"created_at" gorm:"column:created_at"`
	UpdatedAt           time.Time `json:"updated_at" gorm:"column:updated_at"`
}

func (Analysis) TableName() string {
	return "nextflow"
}

// AnalysisNode maps to Python brave's analysis_nodes table.
type AnalysisNode struct {
	ID                     uint       `json:"id" gorm:"primaryKey;autoIncrement"`
	AnalysisNodeID         string     `json:"analysis_node_id" gorm:"column:analysis_node_id;type:varchar(255)"`
	AnalysisID             string     `json:"analysis_id" gorm:"column:analysis_id;type:varchar(255);index:idx_analysis_nodes_analysis_id;index:idx_analysis_nodes_analysis_id_status,priority:1;index:idx_analysis_nodes_analysis_id_node_id,priority:1"`
	NodeID                 string     `json:"node_id" gorm:"column:node_id;type:varchar(255);index:idx_analysis_nodes_analysis_id_node_id,priority:2"`
	NodeName               string     `json:"node_name" gorm:"column:node_name;type:varchar(255)"`
	SampleID               string     `json:"sample_id" gorm:"column:sample_id;type:varchar(255)"`
	ScriptID               string     `json:"script_id" gorm:"column:script_id;type:varchar(255)"`
	InputsPatterns         string     `json:"inputs_patterns" gorm:"column:inputs_patterns;type:json"`
	ResolvedInputs         string     `json:"resolved_inputs" gorm:"column:resolved_inputs;type:json"`
	OutputPatterns         string     `json:"output_patterns" gorm:"column:output_patterns;type:json"`
	ResolvedOutputs        string     `json:"resolved_outputs" gorm:"column:resolved_outputs;type:json"`
	Params                 string     `json:"params" gorm:"column:params;type:json"`
	CPU                    int        `json:"cpu" gorm:"column:cpu"`
	Memory                 string     `json:"memory" gorm:"column:memory;type:varchar(64)"`
	Disk                   string     `json:"disk" gorm:"column:disk;type:varchar(64)"`
	GPU                    int        `json:"gpu" gorm:"column:gpu"`
	Status                 string     `json:"status" gorm:"column:status;type:varchar(64);index:idx_analysis_nodes_analysis_id_status,priority:2"`
	ServerStatus           string     `json:"server_status" gorm:"column:server_status;type:varchar(64)"`
	PID                    int        `json:"pid" gorm:"column:pid"`
	JobID                  string     `json:"job_id" gorm:"column:job_id;type:varchar(255)"`
	Executor               string     `json:"executor" gorm:"column:executor;type:varchar(64)"`
	Retry                  int        `json:"retry" gorm:"column:retry;default:0"`
	MaxRetry               int        `json:"max_retry" gorm:"column:max_retry;default:3"`
	ExitCode               int        `json:"exit_code" gorm:"column:exit_code"`
	ErrorMessage           string     `json:"error_message" gorm:"column:error_message;type:text"`
	InputHash              string     `json:"input_hash" gorm:"column:input_hash;type:varchar(255)"`
	CacheHit               bool       `json:"cache_hit" gorm:"column:cache_hit"`
	UpstreamIDs            string     `json:"upstream_ids" gorm:"column:upstream_ids;type:json"`
	DownstreamIDs          string     `json:"downstream_ids" gorm:"column:downstream_ids;type:json"`
	InputValidationErrors  string     `json:"input_validation_errors" gorm:"column:input_validation_errors;type:json"`
	OutputValidationErrors string     `json:"output_validation_errors" gorm:"column:output_validation_errors;type:json"`
	LogPath                string     `json:"log_path" gorm:"column:log_path;type:varchar(255)"`
	WorkspaceDir           string     `json:"workspace_dir" gorm:"column:workspace_dir;type:varchar(255)"`
	OutputDir              string     `json:"output_dir" gorm:"column:output_dir;type:varchar(255)"`
	CommandPath            string     `json:"command_path" gorm:"column:command_path;type:varchar(255)"`
	ParamsPath             string     `json:"params_path" gorm:"column:params_path;type:varchar(255)"`
	StartedAt              *time.Time `json:"started_at" gorm:"column:started_at"`
	FinishedAt             *time.Time `json:"finished_at" gorm:"column:finished_at"`
	CreatedAt              time.Time  `json:"created_at" gorm:"column:created_at"`
	UpdatedAt              time.Time  `json:"updated_at" gorm:"column:updated_at"`
}

func (AnalysisNode) TableName() string {
	return "analysis_nodes"
}

// AnalysisEdge maps to Python brave's analysis_edges table.
type AnalysisEdge struct {
	ID             uint      `json:"id" gorm:"primaryKey;autoIncrement"`
	AnalysisEdgeID string    `json:"analysis_edge_id" gorm:"column:analysis_edge_id;type:varchar(255)"`
	AnalysisID     string    `json:"analysis_id" gorm:"column:analysis_id;type:varchar(255);index:idx_analysis_edges_analysis_id"`
	SourceNode     string    `json:"source_node" gorm:"column:source_node;type:varchar(255)"`
	TargetNode     string    `json:"target_node" gorm:"column:target_node;type:varchar(255)"`
	SourceHandle   string    `json:"source_handle" gorm:"column:source_handle;type:varchar(255)"`
	TargetHandle   string    `json:"target_handle" gorm:"column:target_handle;type:varchar(255)"`
	CreatedAt      time.Time `json:"created_at" gorm:"column:created_at"`
	UpdatedAt      time.Time `json:"updated_at" gorm:"column:updated_at"`
}

func (AnalysisEdge) TableName() string {
	return "analysis_edges"
}

type AnalysisControllerSaveInput struct {
	RequestParam        map[string]any
	ParseAnalysisResult map[string]any
	DagRuntime          map[string]any
	IsRunNode           bool
	IsReport            bool
}
