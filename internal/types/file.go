package types

import (
	"time"

	"github.com/gobravedev/gobrave/internal/utils"
	"gorm.io/gorm"
)

type File struct {
	ID int64 `json:"id,string" gorm:"primaryKey;type:bigint;autoIncrement:false"`

	FileID string `json:"file_id" gorm:"type:varchar(255);uniqueIndex;not null"`

	FileName string `json:"file_name" gorm:"type:varchar(255)"`

	Path string `json:"path" gorm:"type:text;not null"`

	Format string `json:"format" gorm:"type:varchar(64)"`

	Size int64 `json:"size"`

	MD5 string `json:"md5" gorm:"type:varchar(64);index"`

	Storage string `json:"storage" gorm:"type:varchar(32);default:LOCAL"`

	Description string `json:"description" gorm:"type:text"`

	CreatedAt time.Time `json:"created_at"`

	UpdatedAt time.Time `json:"updated_at"`
}

func (File) TableName() string {
	return "t_file"
}
func (t *File) BeforeCreate(_ *gorm.DB) error {
	if t.ID == 0 {
		t.ID = utils.GenerateID()
	}
	return nil
}

type DatasetFile struct {
	ID int64 `json:"id,string" gorm:"primaryKey;type:bigint;autoIncrement:false"`

	DatasetID int64 `json:"dataset_id,string" gorm:"index;not null"`

	FileID int64 `json:"file_id,string" gorm:"index;not null"`

	Role string `json:"role" gorm:"type:varchar(64)"`

	CreatedAt time.Time `json:"created_at"`
}

func (DatasetFile) TableName() string {
	return "go_dataset_file"
}

func (t *DatasetFile) BeforeCreate(_ *gorm.DB) error {
	if t.ID == 0 {
		t.ID = utils.GenerateID()
	}
	return nil
}
