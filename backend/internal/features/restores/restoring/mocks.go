package restoring

import (
	"errors"

	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	restores_core "databasus-backend/internal/features/restores/core"
	"databasus-backend/internal/features/storages"
)

type MockSuccessRestoreUsecase struct{}

func (uc *MockSuccessRestoreUsecase) Execute(
	backupConfig *backups_config.BackupConfig,
	restore restores_core.Restore,
	originalDB *databases.Database,
	restoringToDB *databases.Database,
	backup *backups_core.Backup,
	storage *storages.Storage,
	isExcludeExtensions bool,
) error {
	return nil
}

type MockFailedRestoreUsecase struct{}

func (uc *MockFailedRestoreUsecase) Execute(
	backupConfig *backups_config.BackupConfig,
	restore restores_core.Restore,
	originalDB *databases.Database,
	restoringToDB *databases.Database,
	backup *backups_core.Backup,
	storage *storages.Storage,
	isExcludeExtensions bool,
) error {
	return errors.New("restore failed")
}

type MockCaptureCredentialsRestoreUsecase struct {
	CalledChan    chan *databases.Database
	ShouldSucceed bool
}

func (uc *MockCaptureCredentialsRestoreUsecase) Execute(
	backupConfig *backups_config.BackupConfig,
	restore restores_core.Restore,
	originalDB *databases.Database,
	restoringToDB *databases.Database,
	backup *backups_core.Backup,
	storage *storages.Storage,
	isExcludeExtensions bool,
) error {
	uc.CalledChan <- restoringToDB

	if uc.ShouldSucceed {
		return nil
	}
	return errors.New("mock restore failed")
}
