package postgres_monitoring_settings

import (
	"errors"
	"postgresus-backend/internal/storage"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type PostgresMonitoringSettingsRepository struct{}

func (r *PostgresMonitoringSettingsRepository) Save(settings *PostgresMonitoringSettings) error {
	return storage.GetDb().Save(settings).Error
}

func (r *PostgresMonitoringSettingsRepository) GetByDbID(
	dbID uuid.UUID,
) (*PostgresMonitoringSettings, error) {
	var settings PostgresMonitoringSettings

	if err := storage.
		GetDb().
		Where("database_id = ?", dbID).
		First(&settings).Error; err != nil {
		return nil, err
	}

	return &settings, nil
}

func (r *PostgresMonitoringSettingsRepository) GetByDbIDWithRelations(
	dbID uuid.UUID,
) (*PostgresMonitoringSettings, error) {
	var settings PostgresMonitoringSettings

	if err := storage.
		GetDb().
		Preload("Database").
		Where("database_id = ?", dbID).
		First(&settings).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		return nil, err
	}

	return &settings, nil
}
