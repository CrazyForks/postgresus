package backups_config

import (
	"postgresus-backend/internal/features/databases"
	"postgresus-backend/internal/features/intervals"
	"postgresus-backend/internal/features/storages"
	users_models "postgresus-backend/internal/features/users/models"
	"postgresus-backend/internal/util/period"

	"github.com/google/uuid"
)

type BackupConfigService struct {
	backupConfigRepository *BackupConfigRepository
	databaseService        *databases.DatabaseService
	storageService         *storages.StorageService

	dbStorageChangeListener BackupConfigStorageChangeListener
}

func (s *BackupConfigService) SetDatabaseStorageChangeListener(
	dbStorageChangeListener BackupConfigStorageChangeListener,
) {
	s.dbStorageChangeListener = dbStorageChangeListener
}

func (s *BackupConfigService) SaveBackupConfigWithAuth(
	user *users_models.User,
	backupConfig *BackupConfig,
) (*BackupConfig, error) {
	if err := backupConfig.Validate(); err != nil {
		return nil, err
	}

	_, err := s.databaseService.GetDatabase(user, backupConfig.DatabaseID)
	if err != nil {
		return nil, err
	}

	return s.SaveBackupConfig(backupConfig)
}

func (s *BackupConfigService) SaveBackupConfig(
	backupConfig *BackupConfig,
) (*BackupConfig, error) {
	if err := backupConfig.Validate(); err != nil {
		return nil, err
	}

	// Check if there's an existing backup config for this database
	existingConfig, err := s.GetBackupConfigByDbId(backupConfig.DatabaseID)
	if err != nil {
		return nil, err
	}

	if existingConfig != nil {
		// If storage is changing, notify the listener
		if s.dbStorageChangeListener != nil &&
			backupConfig.Storage != nil &&
			!storageIDsEqual(existingConfig.StorageID, &backupConfig.Storage.ID) {
			if err := s.dbStorageChangeListener.OnBeforeBackupsStorageChange(
				backupConfig.DatabaseID,
			); err != nil {
				return nil, err
			}
		}
	}

	if !backupConfig.IsBackupsEnabled && existingConfig.StorageID != nil {
		if err := s.dbStorageChangeListener.OnBeforeBackupsStorageChange(
			backupConfig.DatabaseID,
		); err != nil {
			return nil, err
		}

		// we clear storage for disabled backups to allow
		// storage removal for unused storages
		backupConfig.Storage = nil
		backupConfig.StorageID = nil
	}

	return s.backupConfigRepository.Save(backupConfig)
}

func (s *BackupConfigService) GetBackupConfigByDbIdWithAuth(
	user *users_models.User,
	databaseID uuid.UUID,
) (*BackupConfig, error) {
	_, err := s.databaseService.GetDatabase(user, databaseID)
	if err != nil {
		return nil, err
	}

	return s.GetBackupConfigByDbId(databaseID)
}

func (s *BackupConfigService) GetBackupConfigByDbId(
	databaseID uuid.UUID,
) (*BackupConfig, error) {
	config, err := s.backupConfigRepository.FindByDatabaseID(databaseID)
	if err != nil {
		return nil, err
	}

	if config == nil {
		err = s.initializeDefaultConfig(databaseID)
		if err != nil {
			return nil, err
		}

		return s.backupConfigRepository.FindByDatabaseID(databaseID)
	}

	return config, nil
}

func (s *BackupConfigService) IsStorageUsing(
	user *users_models.User,
	storageID uuid.UUID,
) (bool, error) {
	_, err := s.storageService.GetStorage(user, storageID)
	if err != nil {
		return false, err
	}

	return s.backupConfigRepository.IsStorageUsing(storageID)
}

func (s *BackupConfigService) GetBackupConfigsWithEnabledBackups() ([]*BackupConfig, error) {
	return s.backupConfigRepository.GetWithEnabledBackups()
}

func (s *BackupConfigService) initializeDefaultConfig(
	databaseID uuid.UUID,
) error {
	timeOfDay := "04:00"

	_, err := s.backupConfigRepository.Save(&BackupConfig{
		DatabaseID:       databaseID,
		IsBackupsEnabled: false,
		StorePeriod:      period.PeriodWeek,
		BackupInterval: &intervals.Interval{
			Interval:  intervals.IntervalDaily,
			TimeOfDay: &timeOfDay,
		},
		SendNotificationsOn: []BackupNotificationType{
			NotificationBackupFailed,
			NotificationBackupSuccess,
		},
		CpuCount:            1,
		IsRetryIfFailed:     true,
		MaxFailedTriesCount: 3,
	})

	return err
}

func storageIDsEqual(id1, id2 *uuid.UUID) bool {
	if id1 == nil && id2 == nil {
		return true
	}
	if id1 == nil || id2 == nil {
		return false
	}
	return *id1 == *id2
}

func (s *BackupConfigService) OnDatabaseCopied(originalDatabaseID, newDatabaseID uuid.UUID) {
	originalConfig, err := s.GetBackupConfigByDbId(originalDatabaseID)
	if err != nil {
		return
	}

	newConfig := &BackupConfig{
		DatabaseID:          newDatabaseID,
		IsBackupsEnabled:    originalConfig.IsBackupsEnabled,
		StorePeriod:         originalConfig.StorePeriod,
		BackupIntervalID:    originalConfig.BackupIntervalID,
		StorageID:           originalConfig.StorageID,
		SendNotificationsOn: originalConfig.SendNotificationsOn,
		IsRetryIfFailed:     originalConfig.IsRetryIfFailed,
		MaxFailedTriesCount: originalConfig.MaxFailedTriesCount,
		CpuCount:            originalConfig.CpuCount,
	}

	_, err = s.SaveBackupConfig(newConfig)
	if err != nil {
		return
	}
}
