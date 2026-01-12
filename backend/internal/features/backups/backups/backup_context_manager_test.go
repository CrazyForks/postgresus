package backups

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_RegisterBackup_BackupRegisteredSuccessfully(t *testing.T) {
	manager := NewBackupContextManager()

	backupID := uuid.New()
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.RegisterBackup(backupID, cancel)

	manager.mu.RLock()
	_, exists := manager.cancelFuncs[backupID]
	manager.mu.RUnlock()
	assert.True(t, exists, "Backup should be registered")
}

func Test_UnregisterBackup_BackupUnregisteredSuccessfully(t *testing.T) {
	manager := NewBackupContextManager()

	backupID := uuid.New()
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.RegisterBackup(backupID, cancel)
	manager.UnregisterBackup(backupID)

	manager.mu.RLock()
	_, exists := manager.cancelFuncs[backupID]
	manager.mu.RUnlock()
	assert.False(t, exists, "Backup should be unregistered")
}

func Test_CancelBackup_OnSameInstance_BackupCancelledViaPubSub(t *testing.T) {
	manager := NewBackupContextManager()

	backupID := uuid.New()
	ctx, cancel := context.WithCancel(context.Background())

	cancelled := false
	var mu sync.Mutex

	wrappedCancel := func() {
		mu.Lock()
		cancelled = true
		mu.Unlock()
		cancel()
	}

	manager.RegisterBackup(backupID, wrappedCancel)
	manager.StartSubscription()
	time.Sleep(100 * time.Millisecond)

	err := manager.CancelBackup(backupID)
	assert.NoError(t, err, "Cancel should not return error")

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	wasCancelled := cancelled
	mu.Unlock()

	assert.True(t, wasCancelled, "Cancel function should have been called")
	assert.Error(t, ctx.Err(), "Context should be cancelled")
}

func Test_CancelBackup_FromDifferentInstance_BackupCancelledOnRunningInstance(t *testing.T) {
	manager1 := NewBackupContextManager()
	manager2 := NewBackupContextManager()

	backupID := uuid.New()
	ctx, cancel := context.WithCancel(context.Background())

	cancelled := false
	var mu sync.Mutex

	wrappedCancel := func() {
		mu.Lock()
		cancelled = true
		mu.Unlock()
		cancel()
	}

	manager1.RegisterBackup(backupID, wrappedCancel)

	manager1.StartSubscription()
	manager2.StartSubscription()
	time.Sleep(100 * time.Millisecond)

	err := manager2.CancelBackup(backupID)
	assert.NoError(t, err, "Cancel should not return error")

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	wasCancelled := cancelled
	mu.Unlock()

	assert.True(t, wasCancelled, "Cancel function should have been called on instance 1")
	assert.Error(t, ctx.Err(), "Context should be cancelled")
}

func Test_CancelBackup_WhenBackupDoesNotExist_NoErrorReturned(t *testing.T) {
	manager := NewBackupContextManager()

	manager.StartSubscription()
	time.Sleep(100 * time.Millisecond)

	nonExistentID := uuid.New()
	err := manager.CancelBackup(nonExistentID)
	assert.NoError(t, err, "Cancelling non-existent backup should not error")
}

func Test_CancelBackup_WithMultipleBackups_AllBackupsCancelled(t *testing.T) {
	manager := NewBackupContextManager()

	numBackups := 5
	backupIDs := make([]uuid.UUID, numBackups)
	contexts := make([]context.Context, numBackups)
	cancels := make([]context.CancelFunc, numBackups)
	cancelledFlags := make([]bool, numBackups)
	var mu sync.Mutex

	for i := 0; i < numBackups; i++ {
		backupIDs[i] = uuid.New()
		contexts[i], cancels[i] = context.WithCancel(context.Background())

		idx := i
		wrappedCancel := func() {
			mu.Lock()
			cancelledFlags[idx] = true
			mu.Unlock()
			cancels[idx]()
		}

		manager.RegisterBackup(backupIDs[i], wrappedCancel)
	}

	manager.StartSubscription()
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < numBackups; i++ {
		err := manager.CancelBackup(backupIDs[i])
		assert.NoError(t, err, "Cancel should not return error")
	}

	time.Sleep(1 * time.Second)

	mu.Lock()
	for i := 0; i < numBackups; i++ {
		assert.True(t, cancelledFlags[i], "Backup %d should be cancelled", i)
		assert.Error(t, contexts[i].Err(), "Context %d should be cancelled", i)
	}
	mu.Unlock()
}

func Test_CancelBackup_AfterUnregister_BackupNotCancelled(t *testing.T) {
	manager := NewBackupContextManager()

	backupID := uuid.New()
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancelled := false
	var mu sync.Mutex

	wrappedCancel := func() {
		mu.Lock()
		cancelled = true
		mu.Unlock()
		cancel()
	}

	manager.RegisterBackup(backupID, wrappedCancel)
	manager.StartSubscription()
	time.Sleep(100 * time.Millisecond)

	manager.UnregisterBackup(backupID)

	err := manager.CancelBackup(backupID)
	assert.NoError(t, err, "Cancel should not return error")

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	wasCancelled := cancelled
	mu.Unlock()

	assert.False(t, wasCancelled, "Cancel function should not be called after unregister")
}
