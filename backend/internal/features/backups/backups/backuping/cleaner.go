package backuping

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/storages"
	util_encryption "databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/period"
)

const (
	cleanerTickerInterval = 1 * time.Minute
)

type BackupCleaner struct {
	backupRepository      *backups_core.BackupRepository
	storageService        *storages.StorageService
	backupConfigService   *backups_config.BackupConfigService
	fieldEncryptor        util_encryption.FieldEncryptor
	logger                *slog.Logger
	backupRemoveListeners []backups_core.BackupRemoveListener

	runOnce sync.Once
	hasRun  atomic.Bool
}

func (c *BackupCleaner) Run(ctx context.Context) {
	wasAlreadyRun := c.hasRun.Load()

	c.runOnce.Do(func() {
		c.hasRun.Store(true)

		if ctx.Err() != nil {
			return
		}

		ticker := time.NewTicker(cleanerTickerInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.cleanOldBackups(); err != nil {
					c.logger.Error("Failed to clean old backups", "error", err)
				}

				if err := c.cleanExceededBackups(); err != nil {
					c.logger.Error("Failed to clean exceeded backups", "error", err)
				}
			}
		}
	})

	if wasAlreadyRun {
		panic(fmt.Sprintf("%T.Run() called multiple times", c))
	}
}

func (c *BackupCleaner) DeleteBackup(backup *backups_core.Backup) error {
	for _, listener := range c.backupRemoveListeners {
		if err := listener.OnBeforeBackupRemove(backup); err != nil {
			return err
		}
	}

	storage, err := c.storageService.GetStorageByID(backup.StorageID)
	if err != nil {
		return err
	}

	err = storage.DeleteFile(c.fieldEncryptor, backup.ID)
	if err != nil {
		// we do not return error here, because sometimes clean up performed
		// before unavailable storage removal or change - therefore we should
		// proceed even in case of error. It's possible that some S3 or
		// storage is not available yet, it should not block us
		c.logger.Error("Failed to delete backup file", "error", err)
	}

	return c.backupRepository.DeleteByID(backup.ID)
}

func (c *BackupCleaner) AddBackupRemoveListener(listener backups_core.BackupRemoveListener) {
	c.backupRemoveListeners = append(c.backupRemoveListeners, listener)
}

func (c *BackupCleaner) cleanOldBackups() error {
	enabledBackupConfigs, err := c.backupConfigService.GetBackupConfigsWithEnabledBackups()
	if err != nil {
		return err
	}

	for _, backupConfig := range enabledBackupConfigs {
		backupStorePeriod := backupConfig.StorePeriod

		if backupStorePeriod == period.PeriodForever {
			continue
		}

		storeDuration := backupStorePeriod.ToDuration()
		dateBeforeBackupsShouldBeDeleted := time.Now().UTC().Add(-storeDuration)

		oldBackups, err := c.backupRepository.FindBackupsBeforeDate(
			backupConfig.DatabaseID,
			dateBeforeBackupsShouldBeDeleted,
		)
		if err != nil {
			c.logger.Error(
				"Failed to find old backups for database",
				"databaseId",
				backupConfig.DatabaseID,
				"error",
				err,
			)
			continue
		}

		for _, backup := range oldBackups {
			if err := c.DeleteBackup(backup); err != nil {
				c.logger.Error("Failed to delete old backup", "backupId", backup.ID, "error", err)
				continue
			}

			c.logger.Info(
				"Deleted old backup",
				"backupId",
				backup.ID,
				"databaseId",
				backupConfig.DatabaseID,
			)
		}
	}

	return nil
}

func (c *BackupCleaner) cleanExceededBackups() error {
	enabledBackupConfigs, err := c.backupConfigService.GetBackupConfigsWithEnabledBackups()
	if err != nil {
		return err
	}

	for _, backupConfig := range enabledBackupConfigs {
		if backupConfig.MaxBackupsTotalSizeMB <= 0 {
			continue
		}

		if err := c.cleanExceededBackupsForDatabase(
			backupConfig.DatabaseID,
			backupConfig.MaxBackupsTotalSizeMB,
		); err != nil {
			c.logger.Error(
				"Failed to clean exceeded backups for database",
				"databaseId",
				backupConfig.DatabaseID,
				"error",
				err,
			)
			continue
		}
	}

	return nil
}

func (c *BackupCleaner) cleanExceededBackupsForDatabase(
	databaseID uuid.UUID,
	limitperDbMB int64,
) error {
	for {
		backupsTotalSizeMB, err := c.backupRepository.GetTotalSizeByDatabase(databaseID)
		if err != nil {
			return err
		}

		if backupsTotalSizeMB <= float64(limitperDbMB) {
			break
		}

		oldestBackups, err := c.backupRepository.FindOldestByDatabaseExcludingInProgress(
			databaseID,
			1,
		)
		if err != nil {
			return err
		}

		if len(oldestBackups) == 0 {
			c.logger.Warn(
				"No backups to delete but still over limit",
				"databaseId",
				databaseID,
				"totalSizeMB",
				backupsTotalSizeMB,
				"limitMB",
				limitperDbMB,
			)
			break
		}

		backup := oldestBackups[0]
		if err := c.DeleteBackup(backup); err != nil {
			c.logger.Error(
				"Failed to delete exceeded backup",
				"backupId",
				backup.ID,
				"databaseId",
				databaseID,
				"error",
				err,
			)
			return err
		}

		c.logger.Info(
			"Deleted exceeded backup",
			"backupId",
			backup.ID,
			"databaseId",
			databaseID,
			"backupSizeMB",
			backup.BackupSizeMb,
			"totalSizeMB",
			backupsTotalSizeMB,
			"limitMB",
			limitperDbMB,
		)
	}

	return nil
}
