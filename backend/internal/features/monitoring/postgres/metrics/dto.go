package postgres_monitoring_metrics

import (
	"time"

	"github.com/google/uuid"
)

type GetMetricsRequest struct {
	DatabaseID uuid.UUID                    `json:"databaseId" binding:"required"`
	MetricType PostgresMonitoringMetricType `json:"metricType"`
	From       time.Time                    `json:"from" binding:"required"`
	To         time.Time                    `json:"to" binding:"required"`
}