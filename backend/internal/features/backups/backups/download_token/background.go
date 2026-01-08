package download_token

import (
	"databasus-backend/internal/config"
	"log/slog"
	"time"
)

type DownloadTokenBackgroundService struct {
	downloadTokenService *DownloadTokenService
	logger               *slog.Logger
}

func (s *DownloadTokenBackgroundService) Run() {
	s.logger.Info("Starting download token cleanup background service")

	if config.IsShouldShutdown() {
		return
	}

	for {
		if config.IsShouldShutdown() {
			return
		}

		if err := s.downloadTokenService.CleanExpiredTokens(); err != nil {
			s.logger.Error("Failed to clean expired download tokens", "error", err)
		}

		time.Sleep(1 * time.Minute)
	}
}
