package backuping

import (
	"databasus-backend/internal/config"
	backups_cancellation "databasus-backend/internal/features/backups/backups/cancellation"
	backups_core "databasus-backend/internal/features/backups/backups/core"
	"databasus-backend/internal/features/backups/backups/usecases"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/notifiers"
	"databasus-backend/internal/features/storages"
	workspaces_services "databasus-backend/internal/features/workspaces/services"
	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
	"time"

	"github.com/google/uuid"
)

var backupRepository = &backups_core.BackupRepository{}

var backupCancelManager = backups_cancellation.GetBackupCancelManager()

var nodesRegistry = &BackupNodesRegistry{
	cache_utils.GetValkeyClient(),
	logger.GetLogger(),
	cache_utils.DefaultCacheTimeout,
	cache_utils.NewPubSubManager(),
	cache_utils.NewPubSubManager(),
}

func getNodeID() uuid.UUID {
	nodeIDStr := config.GetEnv().NodeID
	nodeID, err := uuid.Parse(nodeIDStr)
	if err != nil {
		logger.GetLogger().Error("Failed to parse node ID from config", "error", err)
		panic(err)
	}
	return nodeID
}

var backuperNode = &BackuperNode{
	databases.GetDatabaseService(),
	encryption.GetFieldEncryptor(),
	workspaces_services.GetWorkspaceService(),
	backupRepository,
	backups_config.GetBackupConfigService(),
	storages.GetStorageService(),
	notifiers.GetNotifierService(),
	backupCancelManager,
	nodesRegistry,
	logger.GetLogger(),
	usecases.GetCreateBackupUsecase(),
	getNodeID(),
	time.Time{},
}

var backupsScheduler = &BackupsScheduler{
	backupRepository,
	backups_config.GetBackupConfigService(),
	storages.GetStorageService(),
	backupCancelManager,
	nodesRegistry,
	time.Now().UTC(),
	logger.GetLogger(),
	make(map[uuid.UUID]BackupToNodeRelation),
	backuperNode,
}

func GetBackupsScheduler() *BackupsScheduler {
	return backupsScheduler
}

func GetBackuperNode() *BackuperNode {
	return backuperNode
}
