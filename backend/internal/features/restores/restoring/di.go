package restoring

import (
	"time"

	"github.com/google/uuid"

	"databasus-backend/internal/features/backups/backups"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	restores_core "databasus-backend/internal/features/restores/core"
	"databasus-backend/internal/features/restores/usecases"
	"databasus-backend/internal/features/storages"
	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
)

var restoreRepository = &restores_core.RestoreRepository{}

var restoreNodesRegistry = &RestoreNodesRegistry{
	cache_utils.GetValkeyClient(),
	logger.GetLogger(),
	cache_utils.DefaultCacheTimeout,
	cache_utils.NewPubSubManager(),
	cache_utils.NewPubSubManager(),
}

var restoreDatabaseCache = cache_utils.NewCacheUtil[RestoreDatabaseCache](
	cache_utils.GetValkeyClient(),
	"restore_db:",
)

var restorerNode = &RestorerNode{
	uuid.New(),
	databases.GetDatabaseService(),
	backups.GetBackupService(),
	encryption.GetFieldEncryptor(),
	restoreRepository,
	backups_config.GetBackupConfigService(),
	storages.GetStorageService(),
	restoreNodesRegistry,
	logger.GetLogger(),
	usecases.GetRestoreBackupUsecase(),
	restoreDatabaseCache,
	time.Time{},
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
