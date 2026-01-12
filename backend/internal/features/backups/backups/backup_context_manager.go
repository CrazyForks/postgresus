package backups

import (
	"context"
	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/logger"
	"log/slog"
	"sync"

	"github.com/google/uuid"
)

const backupCancelChannel = "backup:cancel"

type BackupContextManager struct {
	mu          sync.RWMutex
	cancelFuncs map[uuid.UUID]context.CancelFunc
	pubsub      *cache_utils.PubSubManager
	logger      *slog.Logger
}

func NewBackupContextManager() *BackupContextManager {
	return &BackupContextManager{
		cancelFuncs: make(map[uuid.UUID]context.CancelFunc),
		pubsub:      cache_utils.NewPubSubManager(),
		logger:      logger.GetLogger(),
	}
}

func (m *BackupContextManager) StartSubscription() {
	ctx := context.Background()

	handler := func(message string) {
		backupID, err := uuid.Parse(message)
		if err != nil {
			m.logger.Error("Invalid backup ID in cancel message", "message", message, "error", err)
			return
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		cancelFunc, exists := m.cancelFuncs[backupID]
		if exists {
			cancelFunc()
			delete(m.cancelFuncs, backupID)
			m.logger.Info("Cancelled backup via Pub/Sub", "backupID", backupID)
		}
	}

	err := m.pubsub.Subscribe(ctx, backupCancelChannel, handler)
	if err != nil {
		m.logger.Error("Failed to subscribe to backup cancel channel", "error", err)
	} else {
		m.logger.Info("Successfully subscribed to backup cancel channel")
	}
}

func (m *BackupContextManager) RegisterBackup(backupID uuid.UUID, cancelFunc context.CancelFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cancelFuncs[backupID] = cancelFunc
	m.logger.Debug("Registered backup", "backupID", backupID)
}

func (m *BackupContextManager) CancelBackup(backupID uuid.UUID) error {
	ctx := context.Background()

	err := m.pubsub.Publish(ctx, backupCancelChannel, backupID.String())
	if err != nil {
		m.logger.Error("Failed to publish cancel message", "backupID", backupID, "error", err)
		return err
	}

	m.logger.Info("Published backup cancel message", "backupID", backupID)
	return nil
}

func (m *BackupContextManager) UnregisterBackup(backupID uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.cancelFuncs, backupID)
	m.logger.Debug("Unregistered backup", "backupID", backupID)
}
