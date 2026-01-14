package backups_download

import (
	"databasus-backend/internal/util/logger"
)

var downloadTokenRepository = &DownloadTokenRepository{}

var downloadTokenService = &DownloadTokenService{
	downloadTokenRepository,
	logger.GetLogger(),
}

var downloadTokenBackgroundService = &DownloadTokenBackgroundService{
	downloadTokenService,
	logger.GetLogger(),
}

func GetDownloadTokenService() *DownloadTokenService {
	return downloadTokenService
}

func GetDownloadTokenBackgroundService() *DownloadTokenBackgroundService {
	return downloadTokenBackgroundService
}
