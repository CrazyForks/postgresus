package audit_logs

import (
	"context"
	"log/slog"
	"time"
)

type AuditLogBackgroundService struct {
	auditLogService *AuditLogService
	logger          *slog.Logger
}

func (s *AuditLogBackgroundService) Run(ctx context.Context) {
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
}

func (s *AuditLogBackgroundService) cleanOldAuditLogs() error {
	return s.auditLogService.CleanOldAuditLogs()
}
