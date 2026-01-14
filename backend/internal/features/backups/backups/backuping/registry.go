package backuping

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	cache_utils "databasus-backend/internal/util/cache"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"
)

const (
	nodeInfoKeyPrefix       = "node:"
	nodeInfoKeySuffix       = ":info"
	nodeActiveBackupsPrefix = "node:"
	nodeActiveBackupsSuffix = ":active_backups"
	backupSubmitChannel     = "backup:submit"
	backupCompletionChannel = "backup:completion"
)

// BackupNodesRegistry helps to sync backups scheduler and backup nodes.
// Features:
// - Track node availability and load level
// - Assign from scheduler to node backups needed to be processed
// - Notify scheduler from node about backup completion
type BackupNodesRegistry struct {
	client            valkey.Client
	logger            *slog.Logger
	timeout           time.Duration
	pubsubBackups     *cache_utils.PubSubManager
	pubsubCompletions *cache_utils.PubSubManager
}

func (r *BackupNodesRegistry) GetAvailableNodes() ([]BackupNode, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	var allKeys []string
	cursor := uint64(0)
	pattern := nodeInfoKeyPrefix + "*" + nodeInfoKeySuffix

	for {
		result := r.client.Do(
			ctx,
			r.client.B().Scan().Cursor(cursor).Match(pattern).Count(1_000).Build(),
		)

		if result.Error() != nil {
			return nil, fmt.Errorf("failed to scan node keys: %w", result.Error())
		}

		scanResult, err := result.AsScanEntry()
		if err != nil {
			return nil, fmt.Errorf("failed to parse scan result: %w", err)
		}

		allKeys = append(allKeys, scanResult.Elements...)

		cursor = scanResult.Cursor
		if cursor == 0 {
			break
		}
	}

	if len(allKeys) == 0 {
		return []BackupNode{}, nil
	}

	keyDataMap, err := r.pipelineGetKeys(allKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to pipeline get node keys: %w", err)
	}

	var nodes []BackupNode
	for key, data := range keyDataMap {
		var node BackupNode
		if err := json.Unmarshal(data, &node); err != nil {
			r.logger.Warn("Failed to unmarshal node data", "key", key, "error", err)
			continue
		}
		nodes = append(nodes, node)
	}

	return nodes, nil
}

func (r *BackupNodesRegistry) GetBackupNodesStats() ([]BackupNodeStats, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	var allKeys []string
	cursor := uint64(0)
	pattern := nodeActiveBackupsPrefix + "*" + nodeActiveBackupsSuffix

	for {
		result := r.client.Do(
			ctx,
			r.client.B().Scan().Cursor(cursor).Match(pattern).Count(100).Build(),
		)

		if result.Error() != nil {
			return nil, fmt.Errorf("failed to scan active backups keys: %w", result.Error())
		}

		scanResult, err := result.AsScanEntry()
		if err != nil {
			return nil, fmt.Errorf("failed to parse scan result: %w", err)
		}

		allKeys = append(allKeys, scanResult.Elements...)

		cursor = scanResult.Cursor
		if cursor == 0 {
			break
		}
	}

	if len(allKeys) == 0 {
		return []BackupNodeStats{}, nil
	}

	keyDataMap, err := r.pipelineGetKeys(allKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to pipeline get active backups keys: %w", err)
	}

	var stats []BackupNodeStats
	for key, data := range keyDataMap {
		nodeID := r.extractNodeIDFromKey(key, nodeActiveBackupsPrefix, nodeActiveBackupsSuffix)

		count, err := r.parseIntFromBytes(data)
		if err != nil {
			r.logger.Warn("Failed to parse active backups count", "key", key, "error", err)
			continue
		}

		stat := BackupNodeStats{
			ID:            nodeID,
			ActiveBackups: int(count),
		}
		stats = append(stats, stat)
	}

	return stats, nil
}

func (r *BackupNodesRegistry) IncrementBackupsInProgress(nodeID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	key := fmt.Sprintf("%s%s%s", nodeActiveBackupsPrefix, nodeID, nodeActiveBackupsSuffix)
	result := r.client.Do(ctx, r.client.B().Incr().Key(key).Build())

	if result.Error() != nil {
		return fmt.Errorf(
			"failed to increment backups in progress for node %s: %w",
			nodeID,
			result.Error(),
		)
	}

	return nil
}

func (r *BackupNodesRegistry) DecrementBackupsInProgress(nodeID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	key := fmt.Sprintf("%s%s%s", nodeActiveBackupsPrefix, nodeID, nodeActiveBackupsSuffix)
	result := r.client.Do(ctx, r.client.B().Decr().Key(key).Build())

	if result.Error() != nil {
		return fmt.Errorf(
			"failed to decrement backups in progress for node %s: %w",
			nodeID,
			result.Error(),
		)
	}

	newValue, err := result.AsInt64()
	if err != nil {
		return fmt.Errorf("failed to parse decremented value for node %s: %w", nodeID, err)
	}

	if newValue < 0 {
		setCtx, setCancel := context.WithTimeout(context.Background(), r.timeout)
		r.client.Do(setCtx, r.client.B().Set().Key(key).Value("0").Build())
		setCancel()
		r.logger.Warn("Active backups counter went below 0, reset to 0", "nodeID", nodeID)
	}

	return nil
}

func (r *BackupNodesRegistry) HearthbeatNodeInRegistry(now time.Time, backupNode BackupNode) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	backupNode.LastHeartbeat = now

	data, err := json.Marshal(backupNode)
	if err != nil {
		return fmt.Errorf("failed to marshal backup node: %w", err)
	}

	key := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, backupNode.ID.String(), nodeInfoKeySuffix)
	result := r.client.Do(
		ctx,
		r.client.B().Set().Key(key).Value(string(data)).Build(),
	)

	if result.Error() != nil {
		return fmt.Errorf("failed to register node %s: %w", backupNode.ID, result.Error())
	}

	return nil
}

func (r *BackupNodesRegistry) UnregisterNodeFromRegistry(backupNode BackupNode) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	infoKey := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, backupNode.ID.String(), nodeInfoKeySuffix)
	counterKey := fmt.Sprintf(
		"%s%s%s",
		nodeActiveBackupsPrefix,
		backupNode.ID.String(),
		nodeActiveBackupsSuffix,
	)

	result := r.client.Do(
		ctx,
		r.client.B().Del().Key(infoKey, counterKey).Build(),
	)

	if result.Error() != nil {
		return fmt.Errorf("failed to unregister node %s: %w", backupNode.ID, result.Error())
	}

	r.logger.Info("Unregistered node from registry", "nodeID", backupNode.ID)
	return nil
}

func (r *BackupNodesRegistry) AssignBackupToNode(
	targetNodeID string,
	backupID uuid.UUID,
	isCallNotifier bool,
) error {
	ctx := context.Background()

	message := BackupSubmitMessage{
		NodeID:         targetNodeID,
		BackupID:       backupID.String(),
		IsCallNotifier: isCallNotifier,
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal backup submit message: %w", err)
	}

	err = r.pubsubBackups.Publish(ctx, backupSubmitChannel, string(messageJSON))
	if err != nil {
		return fmt.Errorf("failed to publish backup submit message: %w", err)
	}

	return nil
}

func (r *BackupNodesRegistry) SubscribeNodeForBackupsAssignment(
	nodeID string,
	handler func(backupID uuid.UUID, isCallNotifier bool),
) error {
	ctx := context.Background()

	wrappedHandler := func(message string) {
		var msg BackupSubmitMessage
		if err := json.Unmarshal([]byte(message), &msg); err != nil {
			r.logger.Warn("Failed to unmarshal backup submit message", "error", err)
			return
		}

		if msg.NodeID != nodeID {
			return
		}

		backupID, err := uuid.Parse(msg.BackupID)
		if err != nil {
			r.logger.Warn(
				"Failed to parse backup ID from message",
				"backupId",
				msg.BackupID,
				"error",
				err,
			)
			return
		}

		handler(backupID, msg.IsCallNotifier)
	}

	err := r.pubsubBackups.Subscribe(ctx, backupSubmitChannel, wrappedHandler)
	if err != nil {
		return fmt.Errorf("failed to subscribe to backup submit channel: %w", err)
	}

	r.logger.Info("Subscribed to backup submit channel", "nodeID", nodeID)
	return nil
}

func (r *BackupNodesRegistry) UnsubscribeNodeForBackupsAssignments() error {
	err := r.pubsubBackups.Close()
	if err != nil {
		return fmt.Errorf("failed to unsubscribe from backup submit channel: %w", err)
	}

	r.logger.Info("Unsubscribed from backup submit channel")
	return nil
}

func (r *BackupNodesRegistry) PublishBackupCompletion(nodeID string, backupID uuid.UUID) error {
	ctx := context.Background()

	message := BackupCompletionMessage{
		NodeID:   nodeID,
		BackupID: backupID.String(),
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal backup completion message: %w", err)
	}

	err = r.pubsubCompletions.Publish(ctx, backupCompletionChannel, string(messageJSON))
	if err != nil {
		return fmt.Errorf("failed to publish backup completion message: %w", err)
	}

	return nil
}

func (r *BackupNodesRegistry) SubscribeForBackupsCompletions(
	handler func(nodeID string, backupID uuid.UUID),
) error {
	ctx := context.Background()

	wrappedHandler := func(message string) {
		var msg BackupCompletionMessage
		if err := json.Unmarshal([]byte(message), &msg); err != nil {
			r.logger.Warn("Failed to unmarshal backup completion message", "error", err)
			return
		}

		backupID, err := uuid.Parse(msg.BackupID)
		if err != nil {
			r.logger.Warn(
				"Failed to parse backup ID from completion message",
				"backupId",
				msg.BackupID,
				"error",
				err,
			)
			return
		}

		handler(msg.NodeID, backupID)
	}

	err := r.pubsubCompletions.Subscribe(ctx, backupCompletionChannel, wrappedHandler)
	if err != nil {
		return fmt.Errorf("failed to subscribe to backup completion channel: %w", err)
	}

	r.logger.Info("Subscribed to backup completion channel")
	return nil
}

func (r *BackupNodesRegistry) UnsubscribeForBackupsCompletions() error {
	err := r.pubsubCompletions.Close()
	if err != nil {
		return fmt.Errorf("failed to unsubscribe from backup completion channel: %w", err)
	}

	r.logger.Info("Unsubscribed from backup completion channel")
	return nil
}

func (r *BackupNodesRegistry) extractNodeIDFromKey(key, prefix, suffix string) uuid.UUID {
	nodeIDStr := strings.TrimPrefix(key, prefix)
	nodeIDStr = strings.TrimSuffix(nodeIDStr, suffix)

	nodeID, err := uuid.Parse(nodeIDStr)
	if err != nil {
		r.logger.Warn("Failed to parse node ID from key", "key", key, "error", err)
		return uuid.Nil
	}

	return nodeID
}

func (r *BackupNodesRegistry) pipelineGetKeys(keys []string) (map[string][]byte, error) {
	if len(keys) == 0 {
		return make(map[string][]byte), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	commands := make([]valkey.Completed, 0, len(keys))
	for _, key := range keys {
		commands = append(commands, r.client.B().Get().Key(key).Build())
	}

	results := r.client.DoMulti(ctx, commands...)

	keyDataMap := make(map[string][]byte, len(keys))
	for i, result := range results {
		if result.Error() != nil {
			r.logger.Warn("Failed to get key in pipeline", "key", keys[i], "error", result.Error())
			continue
		}

		data, err := result.AsBytes()
		if err != nil {
			r.logger.Warn("Failed to parse key data in pipeline", "key", keys[i], "error", err)
			continue
		}

		keyDataMap[keys[i]] = data
	}

	return keyDataMap, nil
}

func (r *BackupNodesRegistry) parseIntFromBytes(data []byte) (int64, error) {
	str := string(data)
	var count int64
	_, err := fmt.Sscanf(str, "%d", &count)
	if err != nil {
		return 0, fmt.Errorf("failed to parse integer from bytes: %w", err)
	}
	return count, nil
}
