package backups

import (
	audit_logs "databasus-backend/internal/features/audit_logs"
	"databasus-backend/internal/features/backups/backups/backuping"
	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_download "databasus-backend/internal/features/backups/backups/download"
	"databasus-backend/internal/features/backups/backups/usecases"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	encryption_secrets "databasus-backend/internal/features/encryption/secrets"
	"databasus-backend/internal/features/notifiers"
	"databasus-backend/internal/features/storages"
	task_cancellation "databasus-backend/internal/features/tasks/cancellation"
	workspaces_services "databasus-backend/internal/features/workspaces/services"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
)

var backupRepository = &backups_core.BackupRepository{}

var taskCancelManager = task_cancellation.GetTaskCancelManager()

var backupService = &BackupService{
	databaseService:        databases.GetDatabaseService(),
	storageService:         storages.GetStorageService(),
	backupRepository:       backupRepository,
	notifierService:        notifiers.GetNotifierService(),
	notificationSender:     notifiers.GetNotifierService(),
	backupConfigService:    backups_config.GetBackupConfigService(),
	secretKeyService:       encryption_secrets.GetSecretKeyService(),
	fieldEncryptor:         encryption.GetFieldEncryptor(),
	createBackupUseCase:    usecases.GetCreateBackupUsecase(),
	logger:                 logger.GetLogger(),
	backupRemoveListeners:  []backups_core.BackupRemoveListener{},
	workspaceService:       workspaces_services.GetWorkspaceService(),
	auditLogService:        audit_logs.GetAuditLogService(),
	taskCancelManager:      taskCancelManager,
	downloadTokenService:   backups_download.GetDownloadTokenService(),
	backupSchedulerService: backuping.GetBackupsScheduler(),
}

var backupController = &BackupController{
	backupService: backupService,
}

func GetBackupService() *BackupService {
	return backupService
}

func GetBackupController() *BackupController {
	return backupController
}

func SetupDependencies() {
	backups_config.
		GetBackupConfigService().
		SetDatabaseStorageChangeListener(backupService)

	databases.GetDatabaseService().AddDbRemoveListener(backupService)
	databases.GetDatabaseService().AddDbCopyListener(backups_config.GetBackupConfigService())
}
