package backups_download

import (
	"context"
	"log/slog"
	"time"
)

type DownloadTokenBackgroundService struct {
	downloadTokenService *DownloadTokenService
	logger               *slog.Logger
}

func (s *DownloadTokenBackgroundService) Run(ctx context.Context) {
	s.logger.Info("Starting download token cleanup background service")

	if ctx.Err() != nil {
		return
	}

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.downloadTokenService.CleanExpiredTokens(); err != nil {
				s.logger.Error("Failed to clean expired download tokens", "error", err)
			}
		}
	}
}
