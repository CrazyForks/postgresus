package databases

import (
	"errors"
	"log/slog"
	"postgresus-backend/internal/features/databases/databases/postgresql"
	"postgresus-backend/internal/features/notifiers"
	"time"

	"github.com/google/uuid"
)

type Database struct {
	ID     uuid.UUID    `json:"id"     gorm:"column:id;primaryKey;type:uuid;default:gen_random_uuid()"`
	UserID uuid.UUID    `json:"userId" gorm:"column:user_id;type:uuid;not null"`
	Name   string       `json:"name"   gorm:"column:name;type:text;not null"`
	Type   DatabaseType `json:"type"   gorm:"column:type;type:text;not null"`

	Postgresql *postgresql.PostgresqlDatabase `json:"postgresql,omitempty" gorm:"foreignKey:DatabaseID"`

	Notifiers []notifiers.Notifier `json:"notifiers" gorm:"many2many:database_notifiers;"`

	// these fields are not reliable, but
	// they are used for pretty UI
	LastBackupTime         *time.Time `json:"lastBackupTime,omitempty"         gorm:"column:last_backup_time;type:timestamp with time zone"`
	LastBackupErrorMessage *string    `json:"lastBackupErrorMessage,omitempty" gorm:"column:last_backup_error_message;type:text"`

	HealthStatus *HealthStatus `json:"healthStatus" gorm:"column:health_status;type:text;not null"`
}

func (d *Database) Validate() error {
	if d.Name == "" {
		return errors.New("name is required")
	}

	switch d.Type {
	case DatabaseTypePostgres:
		return d.Postgresql.Validate()
	default:
		return errors.New("invalid database type: " + string(d.Type))
	}
}

func (d *Database) ValidateUpdate(old, new Database) error {
	if old.Type != new.Type {
		return errors.New("database type is not allowed to change")
	}

	return nil
}

func (d *Database) TestConnection(logger *slog.Logger) error {
	return d.getSpecificDatabase().TestConnection(logger)
}

func (d *Database) getSpecificDatabase() DatabaseConnector {
	switch d.Type {
	case DatabaseTypePostgres:
		return d.Postgresql
	}

	panic("invalid database type: " + string(d.Type))
}
