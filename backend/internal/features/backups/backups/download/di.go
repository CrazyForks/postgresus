package backups_download

import (
	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/logger"
)

var downloadTokenRepository = &DownloadTokenRepository{}

var downloadTracker = NewDownloadTracker(cache_utils.GetValkeyClient())

var downloadTokenService = &DownloadTokenService{
	downloadTokenRepository,
	logger.GetLogger(),
	downloadTracker,
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
