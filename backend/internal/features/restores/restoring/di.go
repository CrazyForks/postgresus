package restoring

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"databasus-backend/internal/features/backups/backups"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	restores_core "databasus-backend/internal/features/restores/core"
	"databasus-backend/internal/features/restores/usecases"
	"databasus-backend/internal/features/storages"
	tasks_cancellation "databasus-backend/internal/features/tasks/cancellation"
	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
)

var restoreRepository = &restores_core.RestoreRepository{}

var restoreNodesRegistry = &RestoreNodesRegistry{
	client:            cache_utils.GetValkeyClient(),
	logger:            logger.GetLogger(),
	timeout:           cache_utils.DefaultCacheTimeout,
	pubsubRestores:    cache_utils.NewPubSubManager(),
	pubsubCompletions: cache_utils.NewPubSubManager(),
	runOnce:           sync.Once{},
	hasRun:            atomic.Bool{},
}

var restoreDatabaseCache = cache_utils.NewCacheUtil[RestoreDatabaseCache](
	cache_utils.GetValkeyClient(),
	"restore_db:",
)

var restoreCancelManager = tasks_cancellation.GetTaskCancelManager()

var restorerNode = &RestorerNode{
	nodeID:               uuid.New(),
	databaseService:      databases.GetDatabaseService(),
	backupService:        backups.GetBackupService(),
	fieldEncryptor:       encryption.GetFieldEncryptor(),
	restoreRepository:    restoreRepository,
	backupConfigService:  backups_config.GetBackupConfigService(),
	storageService:       storages.GetStorageService(),
	restoreNodesRegistry: restoreNodesRegistry,
	logger:               logger.GetLogger(),
	restoreBackupUsecase: usecases.GetRestoreBackupUsecase(),
	cacheUtil:            restoreDatabaseCache,
	restoreCancelManager: restoreCancelManager,
	lastHeartbeat:        time.Time{},
	runOnce:              sync.Once{},
	hasRun:               atomic.Bool{},
}

var restoresScheduler = &RestoresScheduler{
	restoreRepository:        restoreRepository,
	backupService:            backups.GetBackupService(),
	storageService:           storages.GetStorageService(),
	backupConfigService:      backups_config.GetBackupConfigService(),
	restoreNodesRegistry:     restoreNodesRegistry,
	lastCheckTime:            time.Now().UTC(),
	logger:                   logger.GetLogger(),
	restoreToNodeRelations:   make(map[uuid.UUID]RestoreToNodeRelation),
	restorerNode:             restorerNode,
	cacheUtil:                restoreDatabaseCache,
	completionSubscriptionID: uuid.Nil,
	runOnce:                  sync.Once{},
	hasRun:                   atomic.Bool{},
}

func GetRestoresScheduler() *RestoresScheduler {
	return restoresScheduler
}

func GetRestorerNode() *RestorerNode {
	return restorerNode
}

func GetRestoreNodesRegistry() *RestoreNodesRegistry {
	return restoreNodesRegistry
}
