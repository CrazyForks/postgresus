package backups_cancellation

import (
	"context"
	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/logger"
	"sync"

	"github.com/google/uuid"
)

var backupCancelManager = &BackupCancelManager{
	sync.RWMutex{},
	make(map[uuid.UUID]context.CancelFunc),
	cache_utils.NewPubSubManager(),
	logger.GetLogger(),
}

func GetBackupCancelManager() *BackupCancelManager {
	return backupCancelManager
}

func SetupDependencies() {
	backupCancelManager.StartSubscription()
}
