package postgres_monitoring_metrics

import (
	"postgresus-backend/internal/config"
	"postgresus-backend/internal/util/logger"
	"time"
)

var log = logger.GetLogger()

type PostgresMonitoringMetricsBackgroundService struct {
	metricsRepository *PostgresMonitoringMetricRepository
}

func (s *PostgresMonitoringMetricsBackgroundService) Run() {
	for {
		if config.IsShouldShutdown() {
			return
		}

		s.RemoveOldMetrics()

		time.Sleep(5 * time.Minute)
	}
}

func (s *PostgresMonitoringMetricsBackgroundService) RemoveOldMetrics() {
	monthAgo := time.Now().UTC().Add(-3 * 30 * 24 * time.Hour)

	if err := s.metricsRepository.RemoveOlderThan(monthAgo); err != nil {
		log.Error("Failed to remove old metrics", "error", err)
	}
}
