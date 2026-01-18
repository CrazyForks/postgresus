package healthcheck_attempt

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	healthcheck_config "databasus-backend/internal/features/healthcheck/config"
)

type HealthcheckAttemptBackgroundService struct {
	healthcheckConfigService   *healthcheck_config.HealthcheckConfigService
	checkDatabaseHealthUseCase *CheckDatabaseHealthUseCase
	logger                     *slog.Logger

	runOnce sync.Once
	hasRun  atomic.Bool
}

func (s *HealthcheckAttemptBackgroundService) Run(ctx context.Context) {
	wasAlreadyRun := s.hasRun.Load()

	s.runOnce.Do(func() {
		s.hasRun.Store(true)

		// first healthcheck immediately
		s.checkDatabases()

		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.checkDatabases()
			}
		}
	})

	if wasAlreadyRun {
		panic(fmt.Sprintf("%T.Run() called multiple times", s))
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
			err := s.checkDatabaseHealthUseCase.Execute(now, healthcheckConfig)
			if err != nil {
				s.logger.Error("failed to check database health", "error", err)
			}
		}(&healthcheckConfig)
	}
}
