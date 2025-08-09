package postgres_monitoring_metrics

import (
	"errors"
	"postgresus-backend/internal/features/databases"
	users_models "postgresus-backend/internal/features/users/models"
	"time"

	"github.com/google/uuid"
)

type PostgresMonitoringMetricService struct {
	metricsRepository *PostgresMonitoringMetricRepository
	databaseService   *databases.DatabaseService
}

func (s *PostgresMonitoringMetricService) Insert(metrics []PostgresMonitoringMetric) error {
	if len(metrics) == 0 {
		return nil
	}

	return s.metricsRepository.Insert(metrics)
}

func (s *PostgresMonitoringMetricService) GetMetrics(
	user *users_models.User,
	databaseID uuid.UUID,
	metricType PostgresMonitoringMetricType,
	from time.Time,
	to time.Time,
) ([]PostgresMonitoringMetric, error) {
	database, err := s.databaseService.GetDatabaseByID(databaseID)
	if err != nil {
		return nil, err
	}

	if database.UserID != user.ID {
		return nil, errors.New("database not found")
	}

	return s.metricsRepository.GetByMetrics(databaseID, metricType, from, to)
}
