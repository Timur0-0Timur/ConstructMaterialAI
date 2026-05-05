package models

import (
	"database/sql/driver"
	"fmt"
	"time"
)

// JSONB реализует Valuer и Scanner для работы GORM с типом JSONB
type JSONB []byte

func (j JSONB) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return string(j), nil
}

func (j *JSONB) Scan(value interface{}) error {
	if value == nil {
		*j = nil
		return nil
	}
	switch v := value.(type) {
	case []byte:
		*j = make([]byte, len(v))
		copy(*j, v)
	case string:
		*j = JSONB(v)
	default:
		return fmt.Errorf("JSONB: неподдерживаемый тип %T", value)
	}
	return nil
}

type User struct {
	ID           uint      `gorm:"primarykey;column:id"`
	Email        string    `gorm:"uniqueIndex;not null;column:email"`
	PasswordHash string    `gorm:"not null;column:password_hash"`
	CreatedAt    time.Time `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt    time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

type Project struct {
	ID             uint            `gorm:"primarykey;column:id"`
	UserID         uint            `gorm:"not null;index;column:user_id"`
	Name           string          `gorm:"not null;column:name"`
	Description    string          `gorm:"column:description"`
	EquipmentItems []EquipmentItem `gorm:"foreignKey:ProjectID"`
	CreatedAt      time.Time       `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt      time.Time       `gorm:"column:updated_at;autoUpdateTime"`
}

type EquipmentItem struct {
	ID            uint      `gorm:"primarykey;column:id"`
	ProjectID     uint      `gorm:"not null;index;column:project_id"`
	EquipmentType string    `gorm:"not null;column:equipment_type"`
	Parameters    JSONB     `gorm:"column:parameters;type:jsonb"`
	WeightResult  *float64  `gorm:"column:weight_result"`
	CreatedAt     time.Time `gorm:"column:created_at;autoCreateTime"`
}
