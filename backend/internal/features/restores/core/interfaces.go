package restores_core

import (
	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/storages"
)

type RestoreBackupUsecase interface {
	Execute(
		backupConfig *backups_config.BackupConfig,
		restore Restore,
		originalDB *databases.Database,
		restoringToDB *databases.Database,
		backup *backups_core.Backup,
		storage *storages.Storage,
		isExcludeExtensions bool,
	) error
}
