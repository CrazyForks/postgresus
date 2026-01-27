package backuping

import (
	"testing"
	"time"

	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/intervals"
	"databasus-backend/internal/features/notifiers"
	"databasus-backend/internal/features/storages"
	users_enums "databasus-backend/internal/features/users/enums"
	users_testing "databasus-backend/internal/features/users/testing"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
	"databasus-backend/internal/storage"
	"databasus-backend/internal/util/period"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_CleanOldBackups_DeletesBackupsOlderThanStorePeriod(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(100 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	// Create backup interval
	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:       database.ID,
		IsBackupsEnabled: true,
		StorePeriod:      period.PeriodWeek,
		StorageID:        &storage.ID,
		BackupIntervalID: interval.ID,
		BackupInterval:   interval,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	// Create backups with different ages
	now := time.Now().UTC()
	oldBackup1 := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    now.Add(-10 * 24 * time.Hour), // 10 days old
	}
	oldBackup2 := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    now.Add(-8 * 24 * time.Hour), // 8 days old
	}
	recentBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    now.Add(-3 * 24 * time.Hour), // 3 days old
	}

	err = backupRepository.Save(oldBackup1)
	assert.NoError(t, err)
	err = backupRepository.Save(oldBackup2)
	assert.NoError(t, err)
	err = backupRepository.Save(recentBackup)
	assert.NoError(t, err)

	// Run cleanup
	cleaner := GetBackupCleaner()
	err = cleaner.cleanOldBackups()
	assert.NoError(t, err)

	// Verify old backups deleted, recent backup remains
	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(remainingBackups))
	assert.Equal(t, recentBackup.ID, remainingBackups[0].ID)
}

func Test_CleanOldBackups_SkipsDatabaseWithForeverStorePeriod(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(100 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	// Create backup interval
	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:       database.ID,
		IsBackupsEnabled: true,
		StorePeriod:      period.PeriodForever,
		StorageID:        &storage.ID,
		BackupIntervalID: interval.ID,
		BackupInterval:   interval,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	// Create very old backup
	oldBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    time.Now().UTC().Add(-365 * 24 * time.Hour), // 1 year old
	}
	err = backupRepository.Save(oldBackup)
	assert.NoError(t, err)

	// Run cleanup
	cleaner := GetBackupCleaner()
	err = cleaner.cleanOldBackups()
	assert.NoError(t, err)

	// Verify backup still exists
	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(remainingBackups))
	assert.Equal(t, oldBackup.ID, remainingBackups[0].ID)
}

func Test_CleanExceededBackups_WhenUnderLimit_NoBackupsDeleted(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(100 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	// Create backup interval
	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:            database.ID,
		IsBackupsEnabled:      true,
		StorePeriod:           period.PeriodForever,
		StorageID:             &storage.ID,
		MaxBackupsTotalSizeMB: 100, // 100 MB limit
		BackupIntervalID:      interval.ID,
		BackupInterval:        interval,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	// Create 3 backups totaling 50MB (under limit)
	for i := 0; i < 3; i++ {
		backup := &backups_core.Backup{
			ID:           uuid.New(),
			DatabaseID:   database.ID,
			StorageID:    storage.ID,
			Status:       backups_core.BackupStatusCompleted,
			BackupSizeMb: 16.67,
			CreatedAt:    time.Now().UTC().Add(-time.Duration(i) * time.Hour),
		}
		err = backupRepository.Save(backup)
		assert.NoError(t, err)
	}

	// Run cleanup
	cleaner := GetBackupCleaner()
	err = cleaner.cleanExceededBackups()
	assert.NoError(t, err)

	// Verify all backups remain
	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(remainingBackups))
}

func Test_CleanExceededBackups_WhenOverLimit_DeletesOldestBackups(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(100 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	// Create backup interval
	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:            database.ID,
		IsBackupsEnabled:      true,
		StorePeriod:           period.PeriodForever,
		StorageID:             &storage.ID,
		MaxBackupsTotalSizeMB: 30, // 30 MB limit
		BackupIntervalID:      interval.ID,
		BackupInterval:        interval,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	// Create 5 backups of 10MB each (total 50MB, over 30MB limit)
	now := time.Now().UTC()
	var backupIDs []uuid.UUID
	for i := 0; i < 5; i++ {
		backup := &backups_core.Backup{
			ID:           uuid.New(),
			DatabaseID:   database.ID,
			StorageID:    storage.ID,
			Status:       backups_core.BackupStatusCompleted,
			BackupSizeMb: 10,
			CreatedAt:    now.Add(-time.Duration(4-i) * time.Hour), // Oldest first
		}
		err = backupRepository.Save(backup)
		assert.NoError(t, err)
		backupIDs = append(backupIDs, backup.ID)
	}

	// Run cleanup
	cleaner := GetBackupCleaner()
	err = cleaner.cleanExceededBackups()
	assert.NoError(t, err)

	// Verify 2 oldest backups deleted, 3 newest remain
	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(remainingBackups))

	// Check that the newest 3 backups remain
	remainingIDs := make(map[uuid.UUID]bool)
	for _, backup := range remainingBackups {
		remainingIDs[backup.ID] = true
	}
	assert.False(t, remainingIDs[backupIDs[0]]) // Oldest deleted
	assert.False(t, remainingIDs[backupIDs[1]]) // 2nd oldest deleted
	assert.True(t, remainingIDs[backupIDs[2]])  // 3rd remains
	assert.True(t, remainingIDs[backupIDs[3]])  // 4th remains
	assert.True(t, remainingIDs[backupIDs[4]])  // Newest remains
}

func Test_CleanExceededBackups_SkipsInProgressBackups(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(100 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	// Create backup interval
	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:            database.ID,
		IsBackupsEnabled:      true,
		StorePeriod:           period.PeriodForever,
		StorageID:             &storage.ID,
		MaxBackupsTotalSizeMB: 50, // 50 MB limit
		BackupIntervalID:      interval.ID,
		BackupInterval:        interval,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	now := time.Now().UTC()

	// Create 3 completed backups of 30MB each
	completedBackups := make([]*backups_core.Backup, 3)
	for i := 0; i < 3; i++ {
		backup := &backups_core.Backup{
			ID:           uuid.New(),
			DatabaseID:   database.ID,
			StorageID:    storage.ID,
			Status:       backups_core.BackupStatusCompleted,
			BackupSizeMb: 30,
			CreatedAt:    now.Add(-time.Duration(3-i) * time.Hour),
		}
		err = backupRepository.Save(backup)
		assert.NoError(t, err)
		completedBackups[i] = backup
	}

	// Create 1 in-progress backup (should be excluded from size calculation and deletion)
	inProgressBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusInProgress,
		BackupSizeMb: 10,
		CreatedAt:    now,
	}
	err = backupRepository.Save(inProgressBackup)
	assert.NoError(t, err)

	// Run cleanup
	cleaner := GetBackupCleaner()
	err = cleaner.cleanExceededBackups()
	assert.NoError(t, err)

	// Verify: only completed backups deleted, in-progress remains
	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)

	// Should have in-progress + 1 completed (total 40MB completed + 10MB in-progress)
	assert.GreaterOrEqual(t, len(remainingBackups), 2)

	// Verify in-progress backup still exists
	var inProgressFound bool
	for _, backup := range remainingBackups {
		if backup.ID == inProgressBackup.ID {
			inProgressFound = true
			assert.Equal(t, backups_core.BackupStatusInProgress, backup.Status)
		}
	}
	assert.True(t, inProgressFound, "In-progress backup should not be deleted")
}

func Test_CleanExceededBackups_WithZeroLimit_SkipsDatabase(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(100 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	// Create backup interval
	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:            database.ID,
		IsBackupsEnabled:      true,
		StorePeriod:           period.PeriodForever,
		StorageID:             &storage.ID,
		MaxBackupsTotalSizeMB: 0, // No size limit
		BackupIntervalID:      interval.ID,
		BackupInterval:        interval,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	// Create large backups
	for i := 0; i < 10; i++ {
		backup := &backups_core.Backup{
			ID:           uuid.New(),
			DatabaseID:   database.ID,
			StorageID:    storage.ID,
			Status:       backups_core.BackupStatusCompleted,
			BackupSizeMb: 100,
			CreatedAt:    time.Now().UTC().Add(-time.Duration(i) * time.Hour),
		}
		err = backupRepository.Save(backup)
		assert.NoError(t, err)
	}

	// Run cleanup
	cleaner := GetBackupCleaner()
	err = cleaner.cleanExceededBackups()
	assert.NoError(t, err)

	// Verify all backups remain
	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(remainingBackups))
}

func Test_GetTotalSizeByDatabase_CalculatesCorrectly(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(100 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	// Create completed backups
	completedBackup1 := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10.5,
		CreatedAt:    time.Now().UTC(),
	}
	completedBackup2 := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 20.3,
		CreatedAt:    time.Now().UTC(),
	}
	// Create failed backup (should be included)
	failedBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusFailed,
		BackupSizeMb: 5.2,
		CreatedAt:    time.Now().UTC(),
	}
	// Create in-progress backup (should be excluded)
	inProgressBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusInProgress,
		BackupSizeMb: 100,
		CreatedAt:    time.Now().UTC(),
	}

	err := backupRepository.Save(completedBackup1)
	assert.NoError(t, err)
	err = backupRepository.Save(completedBackup2)
	assert.NoError(t, err)
	err = backupRepository.Save(failedBackup)
	assert.NoError(t, err)
	err = backupRepository.Save(inProgressBackup)
	assert.NoError(t, err)

	// Calculate total size
	totalSize, err := backupRepository.GetTotalSizeByDatabase(database.ID)
	assert.NoError(t, err)

	// Should be 10.5 + 20.3 + 5.2 = 36.0 (excluding in-progress 100)
	assert.InDelta(t, 36.0, totalSize, 0.1)
}

// Mock listener for testing
type mockBackupRemoveListener struct {
	onBeforeBackupRemove func(*backups_core.Backup) error
}

func (m *mockBackupRemoveListener) OnBeforeBackupRemove(backup *backups_core.Backup) error {
	if m.onBeforeBackupRemove != nil {
		return m.onBeforeBackupRemove(backup)
	}

	return nil
}

// Test_DeleteBackup_WhenStorageDeleteFails_BackupStillRemovedFromDatabase verifies resilience
// when storage becomes unavailable. Even if storage.DeleteFile fails (e.g., storage is offline,
// credentials changed, or storage was deleted), the backup record should still be removed from
// the database. This prevents orphaned backup records when storage is no longer accessible.
func Test_DeleteBackup_WhenStorageDeleteFails_BackupStillRemovedFromDatabase(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	testStorage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, testStorage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(100 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(testStorage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	backup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    testStorage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    time.Now().UTC(),
	}
	err := backupRepository.Save(backup)
	assert.NoError(t, err)

	cleaner := GetBackupCleaner()

	err = cleaner.DeleteBackup(backup)
	assert.NoError(t, err, "DeleteBackup should succeed even when storage file doesn't exist")

	deletedBackup, err := backupRepository.FindByID(backup.ID)
	assert.Error(t, err, "Backup should not exist in database")
	assert.Nil(t, deletedBackup)
}

func createTestInterval() *intervals.Interval {
	timeOfDay := "04:00"
	interval := &intervals.Interval{
		Interval:  intervals.IntervalDaily,
		TimeOfDay: &timeOfDay,
	}

	err := storage.GetDb().Create(interval).Error
	if err != nil {
		panic(err)
	}

	return interval
}
