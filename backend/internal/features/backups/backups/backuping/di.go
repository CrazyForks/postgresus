package backuping

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

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
)

var backupRepository = &backups_core.BackupRepository{}

var taskCancelManager = tasks_cancellation.GetTaskCancelManager()

var backupNodesRegistry = &BackupNodesRegistry{
	client:            cache_utils.GetValkeyClient(),
	logger:            logger.GetLogger(),
	timeout:           cache_utils.DefaultCacheTimeout,
	pubsubBackups:     cache_utils.NewPubSubManager(),
	pubsubCompletions: cache_utils.NewPubSubManager(),
	runOnce:           sync.Once{},
	hasRun:            atomic.Bool{},
}

func getNodeID() uuid.UUID {
	return uuid.New()
}

var backuperNode = &BackuperNode{
	databaseService:     databases.GetDatabaseService(),
	fieldEncryptor:      encryption.GetFieldEncryptor(),
	workspaceService:    workspaces_services.GetWorkspaceService(),
	backupRepository:    backupRepository,
	backupConfigService: backups_config.GetBackupConfigService(),
	storageService:      storages.GetStorageService(),
	notificationSender:  notifiers.GetNotifierService(),
	backupCancelManager: taskCancelManager,
	backupNodesRegistry: backupNodesRegistry,
	logger:              logger.GetLogger(),
	createBackupUseCase: usecases.GetCreateBackupUsecase(),
	nodeID:              getNodeID(),
	lastHeartbeat:       time.Time{},
	runOnce:             sync.Once{},
	hasRun:              atomic.Bool{},
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
	runOnce:               sync.Once{},
	hasRun:                atomic.Bool{},
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
