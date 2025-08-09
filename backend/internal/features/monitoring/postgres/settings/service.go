package postgres_monitoring_settings

import (
	"errors"
	"postgresus-backend/internal/features/databases"
	users_models "postgresus-backend/internal/features/users/models"
	"postgresus-backend/internal/util/logger"
	"postgresus-backend/internal/util/tools"

	"github.com/google/uuid"
)

var log = logger.GetLogger()

type PostgresMonitoringSettingsService struct {
	databaseService                      *databases.DatabaseService
	postgresMonitoringSettingsRepository *PostgresMonitoringSettingsRepository
}

func (s *PostgresMonitoringSettingsService) OnDatabaseCreated(dbID uuid.UUID) {
	db, err := s.databaseService.GetDatabaseByID(dbID)
	if err != nil {
		return
	}

	if db.Type != databases.DatabaseTypePostgres {
		return
	}

	settings := &PostgresMonitoringSettings{
		DatabaseID:                         dbID,
		IsSystemResourcesMonitoringEnabled: true,
		IsDbResourcesMonitoringEnabled:     true,
		MonitoringIntervalSeconds:          15,
	}

	installedExtensions, err := s.ensureSystemMonitoringExtensionsInstalled(dbID)
	if err != nil {
		settings.IsSystemResourcesMonitoringEnabled = false
	} else {
		settings.AddInstalledExtensions(installedExtensions)
	}

	err = s.postgresMonitoringSettingsRepository.Save(settings)
	if err != nil {
		log.Error("failed to save postgres monitoring settings", "error", err)
	}
}

func (s *PostgresMonitoringSettingsService) Save(
	user *users_models.User,
	settings *PostgresMonitoringSettings,
) error {
	db, err := s.databaseService.GetDatabaseByID(settings.DatabaseID)
	if err != nil {
		return err
	}

	if db.UserID != user.ID {
		return errors.New("user does not have access to this database")
	}

	existingSettings, err := s.postgresMonitoringSettingsRepository.GetByDbID(settings.DatabaseID)
	if err != nil {
		return err
	}

	if existingSettings != nil &&
		settings.IsSystemResourcesMonitoringEnabled &&
		!existingSettings.IsSystemResourcesMonitoringEnabled {
		extensions, err := s.ensureSystemMonitoringExtensionsInstalled(settings.DatabaseID)
		if err != nil {
			return err
		}

		settings.AddInstalledExtensions(extensions)
	}

	return s.postgresMonitoringSettingsRepository.Save(settings)
}

func (s *PostgresMonitoringSettingsService) GetByDbID(
	user *users_models.User,
	dbID uuid.UUID,
) (*PostgresMonitoringSettings, error) {
	dbSettings, err := s.postgresMonitoringSettingsRepository.GetByDbIDWithRelations(dbID)
	if err != nil {
		return nil, err
	}

	if dbSettings == nil {
		dbSettings = &PostgresMonitoringSettings{
			DatabaseID: dbID,

			IsSystemResourcesMonitoringEnabled: false,
			IsDbResourcesMonitoringEnabled:     false,
			MonitoringIntervalSeconds:          15,

			InstalledExtensions:    []tools.PostgresqlExtension{},
			InstalledExtensionsRaw: "",
		}

		err = s.Save(user, dbSettings)
		if err != nil {
			return nil, err
		}

		return s.GetByDbID(user, dbID)
	}

	if dbSettings.Database.UserID != user.ID {
		return nil, errors.New("user does not have access to this database")
	}

	return dbSettings, nil
}

func (s *PostgresMonitoringSettingsService) ensureSystemMonitoringExtensionsInstalled(
	dbID uuid.UUID,
) ([]tools.PostgresqlExtension, error) {
	database, err := s.databaseService.GetDatabaseByID(dbID)
	if err != nil {
		return nil, err
	}

	if database.Type != databases.DatabaseTypePostgres {
		return nil, errors.New("database is not a postgres database")
	}

	if database.Postgresql == nil {
		return nil, errors.New("database is not a postgres database")
	}

	if database.Postgresql.Version < tools.PostgresqlVersion16 {
		return nil, errors.New("system monitoring extensions supported for postgres 16+")
	}

	extensions := []tools.PostgresqlExtension{
		tools.PostgresqlExtensionPgProctab,
	}

	err = database.Postgresql.InstallExtensions(extensions)
	if err != nil {
		return nil, err
	}

	return extensions, nil
}
