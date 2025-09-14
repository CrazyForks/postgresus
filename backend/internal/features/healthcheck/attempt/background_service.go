package healthcheck_attempt

import (
	"log/slog"
	"postgresus-backend/internal/config"
	healthcheck_config "postgresus-backend/internal/features/healthcheck/config"
	"time"
)

type HealthcheckAttemptBackgroundService struct {
	healthcheckConfigService *healthcheck_config.HealthcheckConfigService
	checkPgHealthUseCase     *CheckPgHealthUseCase
	logger                   *slog.Logger
}

func (s *HealthcheckAttemptBackgroundService) Run() {
	// first healthcheck immediately
	s.checkDatabases()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		if config.IsShouldShutdown() {
			break
		}

		s.checkDatabases()
	}
}

func (s *HealthcheckAttemptBackgroundService) checkDatabases() {
	now := time.Now().UTC()

	healthcheckConfigs, err := s.healthcheckConfigService.GetDatabasesWithEnabledHealthcheck()
	if err != nil {
		s.logger.Error("failed to get databases with enabled healthcheck", "error", err)
		return
	}

	for _, healthcheckConfig := range healthcheckConfigs {
		go func(healthcheckConfig *healthcheck_config.HealthcheckConfig) {
			err := s.checkPgHealthUseCase.Execute(now, healthcheckConfig)
			if err != nil {
				s.logger.Error("failed to check pg health", "error", err)
			}
		}(&healthcheckConfig)
	}
}
