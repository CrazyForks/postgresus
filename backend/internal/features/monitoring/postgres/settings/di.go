package postgres_monitoring_settings

import (
	"postgresus-backend/internal/features/databases"
	"postgresus-backend/internal/features/users"
)

var postgresMonitoringSettingsRepository = &PostgresMonitoringSettingsRepository{}
var postgresMonitoringSettingsService = &PostgresMonitoringSettingsService{
	databases.GetDatabaseService(),
	postgresMonitoringSettingsRepository,
}
var postgresMonitoringSettingsController = &PostgresMonitoringSettingsController{
	postgresMonitoringSettingsService,
	users.GetUserService(),
}

func GetPostgresMonitoringSettingsController() *PostgresMonitoringSettingsController {
	return postgresMonitoringSettingsController
}

func GetPostgresMonitoringSettingsService() *PostgresMonitoringSettingsService {
	return postgresMonitoringSettingsService
}

func GetPostgresMonitoringSettingsRepository() *PostgresMonitoringSettingsRepository {
	return postgresMonitoringSettingsRepository
}
