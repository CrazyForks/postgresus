package postgres_monitoring_metrics

import (
	"postgresus-backend/internal/features/databases"
	"postgresus-backend/internal/features/users"
)

var metricsRepository = &PostgresMonitoringMetricRepository{}
var metricsService = &PostgresMonitoringMetricService{
	metricsRepository,
	databases.GetDatabaseService(),
}
var metricsController = &PostgresMonitoringMetricsController{
	metricsService,
	users.GetUserService(),
}
var metricsBackgroundService = &PostgresMonitoringMetricsBackgroundService{
	metricsRepository,
}

func GetPostgresMonitoringMetricsController() *PostgresMonitoringMetricsController {
	return metricsController
}

func GetPostgresMonitoringMetricsService() *PostgresMonitoringMetricService {
	return metricsService
}

func GetPostgresMonitoringMetricsRepository() *PostgresMonitoringMetricRepository {
	return metricsRepository
}

func GetPostgresMonitoringMetricsBackgroundService() *PostgresMonitoringMetricsBackgroundService {
	return metricsBackgroundService
}
