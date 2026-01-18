package audit_logs

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type AuditLogBackgroundService struct {
	auditLogService *AuditLogService
	logger          *slog.Logger

	runOnce sync.Once
	hasRun  atomic.Bool
}

func (s *AuditLogBackgroundService) Run(ctx context.Context) {
	wasAlreadyRun := s.hasRun.Load()

	s.runOnce.Do(func() {
		s.hasRun.Store(true)

		s.logger.Info("Starting audit log cleanup background service")

		if ctx.Err() != nil {
			return
		}

		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.cleanOldAuditLogs(); err != nil {
					s.logger.Error("Failed to clean old audit logs", "error", err)
				}
			}
		}
	})

	if wasAlreadyRun {
		panic(fmt.Sprintf("%T.Run() called multiple times", s))
	}
}

func (s *AuditLogBackgroundService) cleanOldAuditLogs() error {
	return s.auditLogService.CleanOldAuditLogs()
}
