package models

import (
	"postgresus-backend/internal/features/backups/backups"
	"postgresus-backend/internal/features/databases/databases/postgresql"
	"postgresus-backend/internal/features/restores/enums"
	"time"

	"github.com/google/uuid"
)

type Restore struct {
	ID     uuid.UUID           `json:"id"     gorm:"column:id;type:uuid;primaryKey"`
	Status enums.RestoreStatus `json:"status" gorm:"column:status;type:text;not null"`

	BackupID uuid.UUID `json:"backupId" gorm:"column:backup_id;type:uuid;not null"`
	Backup   *backups.Backup

	Postgresql *postgresql.PostgresqlDatabase `json:"postgresql,omitempty" gorm:"foreignKey:RestoreID"`

	FailMessage *string `json:"failMessage" gorm:"column:fail_message"`

	RestoreDurationMs int64     `json:"restoreDurationMs" gorm:"column:restore_duration_ms;default:0"`
	CreatedAt         time.Time `json:"createdAt"         gorm:"column:created_at;default:now()"`
}
