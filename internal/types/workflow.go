package types

import "time"

type Module struct {
	ID               uint      `json:"id" gorm:"primaryKey;autoIncrement"`
	ComponentID      string    `json:"component_id" gorm:"type:varchar(255)"`
	InstallKey       string    `json:"install_key" gorm:"type:varchar(255)"`
	ComponentType    string    `json:"component_type" gorm:"type:varchar(255)"`
	ComponentName    string    `json:"component_name" gorm:"type:varchar(255)"`
	Description      string    `json:"description" gorm:"type:longtext"`
	ComponentIDs     string    `json:"component_ids" gorm:"type:longtext"`
	Img              string    `json:"img" gorm:"type:varchar(255)"`
	ContainerID      string    `json:"container_id" gorm:"type:varchar(255)"`
	ToolsContainerID string    `json:"tools_container_id" gorm:"type:text"`
	Prompt           string    `json:"prompt" gorm:"type:longtext"`
	IOSchema         string    `json:"io_schema" gorm:"column:io_schema;type:longtext"`
	SubContainerID   string    `json:"sub_container_id" gorm:"type:varchar(255)"`
	Tags             string    `json:"tags" gorm:"type:varchar(255)"`
	FileType         string    `json:"file_type" gorm:"type:varchar(255)"`
	ScriptType       string    `json:"script_type" gorm:"type:varchar(255)"`
	Category         string    `json:"category" gorm:"type:varchar(255);default:default"`
	Content          string    `json:"content" gorm:"type:text"`
	OrderIndex       int       `json:"order_index"`
	Position         string    `json:"position" gorm:"type:text"`
	Edges            string    `json:"edges" gorm:"type:text"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func (Module) TableName() string {
	return "pipeline_components"
}

type Workflow struct {
	ID                 uint      `json:"id" gorm:"primaryKey;autoIncrement"`
	Name               string    `json:"name" gorm:"type:varchar(255)"`
	Img                string    `json:"img" gorm:"type:varchar(255)"`
	Tags               string    `json:"tags" gorm:"type:json"`
	URL                string    `json:"url" gorm:"column:url;type:varchar(255)"`
	Category           string    `json:"category" gorm:"type:varchar(255);default:default"`
	Description        string    `json:"description" gorm:"type:longtext"`
	Prompt             string    `json:"prompt" gorm:"type:longtext"`
	DagDefinition      string    `json:"dag_definition" gorm:"column:dag_definition;type:longtext"`
	RelationID         string    `json:"relation_id" gorm:"type:varchar(255)"`
	RelationType       string    `json:"relation_type" gorm:"type:varchar(255)"`
	InstallKey         string    `json:"install_key" gorm:"type:varchar(255)"`
	ComponentID        string    `json:"component_id" gorm:"type:varchar(255)"`
	ContainerID        string    `json:"container_id" gorm:"type:varchar(255)"`
	ParentComponentID  string    `json:"parent_component_id" gorm:"type:varchar(255)"`
	InputComponentIDs  string    `json:"input_component_ids" gorm:"type:json"`
	OutputComponentIDs string    `json:"output_component_ids" gorm:"type:json"`
	OrderIndex         int       `json:"order_index"`
	Version            string    `json:"version" gorm:"type:varchar(255)"`
	UpdateInfo         string    `json:"update_info" gorm:"type:longtext"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

func (Workflow) TableName() string {
	return "pipeline_components_relation"
}
