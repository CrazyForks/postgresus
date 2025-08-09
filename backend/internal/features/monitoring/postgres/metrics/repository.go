package postgres_monitoring_metrics

import (
	"postgresus-backend/internal/storage"
	"time"

	"github.com/google/uuid"
)

type PostgresMonitoringMetricRepository struct{}

func (r *PostgresMonitoringMetricRepository) Insert(metrics []PostgresMonitoringMetric) error {
	return storage.GetDb().Create(&metrics).Error
}

func (r *PostgresMonitoringMetricRepository) GetByMetrics(
	databaseID uuid.UUID,
	metricType PostgresMonitoringMetricType,
	from time.Time,
	to time.Time,
) ([]PostgresMonitoringMetric, error) {
	var metrics []PostgresMonitoringMetric

	query := storage.GetDb().
		Where("database_id = ?", databaseID).
		Where("created_at >= ?", from).
		Where("created_at <= ?", to).
		Where("metric = ?", metricType)

	if err := query.
		Order("created_at DESC").
		Find(&metrics).Error; err != nil {
		return nil, err
	}

	return metrics, nil
}

func (r *PostgresMonitoringMetricRepository) RemoveOlderThan(
	olderThan time.Time,
) error {
	return storage.GetDb().
		Where("created_at < ?", olderThan).
		Delete(&PostgresMonitoringMetric{}).Error
}