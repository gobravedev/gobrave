```
                ContainerTemplate
                 (启动模板)
                      │
        ┌─────────────┴─────────────┐
        │                           │
        ▼                           ▼
    Workflow DAG               AppSession
        │                           │
        └─────────────┬─────────────┘
                      ▼
              ContainerInstance
                      │
                      ▼
               Docker / K8s Pod
```

Workflow：直接由 dag_node 创建 ContainerInstance。
用户应用：由 ContainerTemplate 创建 AppSession，再由 AppSession 管理当前运行的 ContainerInstance





// ContainerRuntime maps to Python brave's t_container table.
// type ContainerRuntime struct {
// 	ID           uint      `json:"id" gorm:"primaryKey;autoIncrement"`
// 	ContainerID  string    `json:"container_id" gorm:"column:container_id;type:varchar(255)"`
// 	UserID       string    `json:"user_id" gorm:"column:user_id;type:varchar(255);index"`
// 	ContainerKey string    `json:"container_key" gorm:"column:container_key;type:varchar(255)"`
// 	Name         string    `json:"name" gorm:"column:name;type:varchar(255)"`
// 	Image        string    `json:"image" gorm:"column:image;type:varchar(255)"`
// 	Img          string    `json:"img" gorm:"column:img;type:longtext"`
// 	ImageID      string    `json:"image_id" gorm:"column:image_id;type:varchar(255)"`
// 	ImageStatus  string    `json:"image_status" gorm:"column:image_status;type:varchar(255)"`
// 	Description  string    `json:"description" gorm:"column:description;type:varchar(255)"`
// 	Version      string    `json:"version" gorm:"column:version;type:varchar(255)"`
// 	Environment  string    `json:"envionment" gorm:"column:envionment;type:varchar(255)"`
// 	Volumes      string    `json:"volumes" gorm:"column:volumes;type:longtext"`
// 	Command      string    `json:"command" gorm:"column:command;type:varchar(255)"`
// 	Port         string    `json:"port" gorm:"column:port;type:varchar(255)"`
// 	Labels       string    `json:"labels" gorm:"column:labels;type:longtext"`
// 	ChangeUID    bool      `json:"change_uid" gorm:"column:change_uid;default:true"`
// 	CreatedAt    time.Time `json:"created_at" gorm:"column:created_at"`
// 	UpdatedAt    time.Time `json:"updated_at" gorm:"column:updated_at"`
// }

//	func (ContainerRuntime) TableName() string {
//		return "container"
//	}