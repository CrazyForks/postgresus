package backuping

import (
	backups_core "databasus-backend/internal/features/backups/backups/core"
	"databasus-backend/internal/features/backups/backups/usecases"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/notifiers"
	"databasus-backend/internal/features/storages"
	tasks_cancellation "databasus-backend/internal/features/tasks/cancellation"
	workspaces_services "databasus-backend/internal/features/workspaces/services"
	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
	"time"

	"github.com/google/uuid"
)

var backupRepository = &backups_core.BackupRepository{}

var taskCancelManager = tasks_cancellation.GetTaskCancelManager()

var backupNodesRegistry = &BackupNodesRegistry{
	cache_utils.GetValkeyClient(),
	logger.GetLogger(),
	cache_utils.DefaultCacheTimeout,
	cache_utils.NewPubSubManager(),
	cache_utils.NewPubSubManager(),
}

func getNodeID() uuid.UUID {
	return uuid.New()
}

var backuperNode = &BackuperNode{
	databases.GetDatabaseService(),
	encryption.GetFieldEncryptor(),
	workspaces_services.GetWorkspaceService(),
	backupRepository,
	backups_config.GetBackupConfigService(),
	storages.GetStorageService(),
	notifiers.GetNotifierService(),
	taskCancelManager,
	backupNodesRegistry,
	logger.GetLogger(),
	usecases.GetCreateBackupUsecase(),
	getNodeID(),
	time.Time{},
}

var backupsScheduler = &BackupsScheduler{
	backupRepository:      backupRepository,
	backupConfigService:   backups_config.GetBackupConfigService(),
	storageService:        storages.GetStorageService(),
	taskCancelManager:     taskCancelManager,
	backupNodesRegistry:   backupNodesRegistry,
	lastBackupTime:        time.Now().UTC(),
	logger:                logger.GetLogger(),
	backupToNodeRelations: make(map[uuid.UUID]BackupToNodeRelation),
	backuperNode:          backuperNode,
}

func GetBackupsScheduler() *BackupsScheduler {
	return backupsScheduler
}

func GetBackuperNode() *BackuperNode {
	return backuperNode
}

func GetBackupNodesRegistry() *BackupNodesRegistry {
	return backupNodesRegistry
}
