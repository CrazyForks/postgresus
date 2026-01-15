package task_registry

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
	nodeInfoKeyPrefix     = "node:"
	nodeInfoKeySuffix     = ":info"
	nodeActiveTasksPrefix = "node:"
	nodeActiveTasksSuffix = ":active_tasks"
	taskSubmitChannel     = "task:submit"
	taskCompletionChannel = "task:completion"

	deadNodeThreshold     = 2 * time.Minute
	cleanupTickerInterval = 1 * time.Second
)

// TaskNodesRegistry helps to sync tasks scheduler (backuping or restoring)
// and task nodes which are used for network-intensive tasks processing
//
// Features:
// - Track node availability and load level
// - Assign from scheduler to node tasks needed to be processed
// - Notify scheduler from node about task completion
//
// Important things to remember:
//   - Node can contain different tasks types so when task is assigned
//     or node's tasks cleaned - should be performed DB check in DB
//     that task with this ID exists for this task type at all
//   - Nodes without heathbeat for more than 2 minutes are not included
//     in available nodes list and stats
//
// Cleanup dead nodes performed on 2 levels:
//   - List and stats functions do not return dead nodes
//   - Periodically dead nodes are cleaned up in cache (to not
//     accumulate too many dead nodes in cache)
type TaskNodesRegistry struct {
	client            valkey.Client
	logger            *slog.Logger
	timeout           time.Duration
	pubsubTasks       *cache_utils.PubSubManager
	pubsubCompletions *cache_utils.PubSubManager
}

func (r *TaskNodesRegistry) Run(ctx context.Context) {
	if err := r.cleanupDeadNodes(); err != nil {
		r.logger.Error("Failed to cleanup dead nodes on startup", "error", err)
	}

	ticker := time.NewTicker(cleanupTickerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.cleanupDeadNodes(); err != nil {
				r.logger.Error("Failed to cleanup dead nodes", "error", err)
			}
		}
	}
}

func (r *TaskNodesRegistry) GetAvailableNodes() ([]TaskNode, error) {
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
		return []TaskNode{}, nil
	}

	keyDataMap, err := r.pipelineGetKeys(allKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to pipeline get node keys: %w", err)
	}

	threshold := time.Now().UTC().Add(-deadNodeThreshold)
	var nodes []TaskNode
	for key, data := range keyDataMap {
		// Skip if the key doesn't exist (data is empty)
		if len(data) == 0 {
			continue
		}

		var node TaskNode
		if err := json.Unmarshal(data, &node); err != nil {
			r.logger.Warn("Failed to unmarshal node data", "key", key, "error", err)
			continue
		}

		// Skip nodes with zero/uninitialized heartbeat
		if node.LastHeartbeat.IsZero() {
			continue
		}

		if node.LastHeartbeat.Before(threshold) {
			continue
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

func (r *TaskNodesRegistry) GetNodesStats() ([]TaskNodeStats, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	var allKeys []string
	cursor := uint64(0)
	pattern := nodeActiveTasksPrefix + "*" + nodeActiveTasksSuffix

	for {
		result := r.client.Do(
			ctx,
			r.client.B().Scan().Cursor(cursor).Match(pattern).Count(100).Build(),
		)

		if result.Error() != nil {
			return nil, fmt.Errorf("failed to scan active tasks keys: %w", result.Error())
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
		return []TaskNodeStats{}, nil
	}

	keyDataMap, err := r.pipelineGetKeys(allKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to pipeline get active tasks keys: %w", err)
	}

	var nodeInfoKeys []string
	nodeIDToStatsKey := make(map[string]string)
	for key := range keyDataMap {
		nodeID := r.extractNodeIDFromKey(key, nodeActiveTasksPrefix, nodeActiveTasksSuffix)
		nodeIDStr := nodeID.String()
		infoKey := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, nodeIDStr, nodeInfoKeySuffix)
		nodeInfoKeys = append(nodeInfoKeys, infoKey)
		nodeIDToStatsKey[infoKey] = key
	}

	nodeInfoMap, err := r.pipelineGetKeys(nodeInfoKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to pipeline get node info keys: %w", err)
	}

	threshold := time.Now().UTC().Add(-deadNodeThreshold)
	var stats []TaskNodeStats
	for infoKey, nodeData := range nodeInfoMap {
		// Skip if the info key doesn't exist (nodeData is empty)
		if len(nodeData) == 0 {
			continue
		}

		var node TaskNode
		if err := json.Unmarshal(nodeData, &node); err != nil {
			r.logger.Warn("Failed to unmarshal node data", "key", infoKey, "error", err)
			continue
		}

		// Skip nodes with zero/uninitialized heartbeat
		if node.LastHeartbeat.IsZero() {
			continue
		}

		if node.LastHeartbeat.Before(threshold) {
			continue
		}

		statsKey := nodeIDToStatsKey[infoKey]
		tasksData := keyDataMap[statsKey]
		count, err := r.parseIntFromBytes(tasksData)
		if err != nil {
			r.logger.Warn("Failed to parse active tasks count", "key", statsKey, "error", err)
			continue
		}

		stat := TaskNodeStats{
			ID:          node.ID,
			ActiveTasks: int(count),
		}
		stats = append(stats, stat)
	}

	return stats, nil
}

func (r *TaskNodesRegistry) IncrementTasksInProgress(nodeID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	key := fmt.Sprintf("%s%s%s", nodeActiveTasksPrefix, nodeID, nodeActiveTasksSuffix)
	result := r.client.Do(ctx, r.client.B().Incr().Key(key).Build())

	if result.Error() != nil {
		return fmt.Errorf(
			"failed to increment tasks in progress for node %s: %w",
			nodeID,
			result.Error(),
		)
	}

	return nil
}

func (r *TaskNodesRegistry) DecrementTasksInProgress(nodeID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	key := fmt.Sprintf("%s%s%s", nodeActiveTasksPrefix, nodeID, nodeActiveTasksSuffix)
	result := r.client.Do(ctx, r.client.B().Decr().Key(key).Build())

	if result.Error() != nil {
		return fmt.Errorf(
			"failed to decrement tasks in progress for node %s: %w",
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
		r.logger.Warn("Active tasks counter went below 0, reset to 0", "nodeID", nodeID)
	}

	return nil
}

func (r *TaskNodesRegistry) HearthbeatNodeInRegistry(now time.Time, node TaskNode) error {
	if now.IsZero() {
		return fmt.Errorf("cannot register node with zero heartbeat timestamp")
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	node.LastHeartbeat = now

	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node: %w", err)
	}

	key := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, node.ID.String(), nodeInfoKeySuffix)
	result := r.client.Do(
		ctx,
		r.client.B().Set().Key(key).Value(string(data)).Build(),
	)

	if result.Error() != nil {
		return fmt.Errorf("failed to register node %s: %w", node.ID, result.Error())
	}

	return nil
}

func (r *TaskNodesRegistry) UnregisterNodeFromRegistry(node TaskNode) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	infoKey := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, node.ID.String(), nodeInfoKeySuffix)
	counterKey := fmt.Sprintf(
		"%s%s%s",
		nodeActiveTasksPrefix,
		node.ID.String(),
		nodeActiveTasksSuffix,
	)

	result := r.client.Do(
		ctx,
		r.client.B().Del().Key(infoKey, counterKey).Build(),
	)

	if result.Error() != nil {
		return fmt.Errorf("failed to unregister node %s: %w", node.ID, result.Error())
	}

	r.logger.Info("Unregistered node from registry", "nodeID", node.ID)
	return nil
}

func (r *TaskNodesRegistry) AssignTaskToNode(
	targetNodeID string,
	taskID uuid.UUID,
	isCallNotifier bool,
) error {
	ctx := context.Background()

	message := TaskSubmitMessage{
		NodeID:         targetNodeID,
		TaskID:         taskID.String(),
		IsCallNotifier: isCallNotifier,
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal task submit message: %w", err)
	}

	err = r.pubsubTasks.Publish(ctx, taskSubmitChannel, string(messageJSON))
	if err != nil {
		return fmt.Errorf("failed to publish task submit message: %w", err)
	}

	return nil
}

func (r *TaskNodesRegistry) SubscribeNodeForTasksAssignment(
	nodeID string,
	handler func(taskID uuid.UUID, isCallNotifier bool),
) error {
	ctx := context.Background()

	wrappedHandler := func(message string) {
		var msg TaskSubmitMessage
		if err := json.Unmarshal([]byte(message), &msg); err != nil {
			r.logger.Warn("Failed to unmarshal task submit message", "error", err)
			return
		}

		if msg.NodeID != nodeID {
			return
		}

		taskID, err := uuid.Parse(msg.TaskID)
		if err != nil {
			r.logger.Warn(
				"Failed to parse task ID from message",
				"taskId",
				msg.TaskID,
				"error",
				err,
			)
			return
		}

		handler(taskID, msg.IsCallNotifier)
	}

	err := r.pubsubTasks.Subscribe(ctx, taskSubmitChannel, wrappedHandler)
	if err != nil {
		return fmt.Errorf("failed to subscribe to task submit channel: %w", err)
	}

	r.logger.Info("Subscribed to task submit channel", "nodeID", nodeID)
	return nil
}

func (r *TaskNodesRegistry) UnsubscribeNodeForTasksAssignments() error {
	err := r.pubsubTasks.Close()
	if err != nil {
		return fmt.Errorf("failed to unsubscribe from task submit channel: %w", err)
	}

	r.logger.Info("Unsubscribed from task submit channel")
	return nil
}

func (r *TaskNodesRegistry) PublishTaskCompletion(nodeID string, taskID uuid.UUID) error {
	ctx := context.Background()

	message := TaskCompletionMessage{
		NodeID: nodeID,
		TaskID: taskID.String(),
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal task completion message: %w", err)
	}

	err = r.pubsubCompletions.Publish(ctx, taskCompletionChannel, string(messageJSON))
	if err != nil {
		return fmt.Errorf("failed to publish task completion message: %w", err)
	}

	return nil
}

func (r *TaskNodesRegistry) SubscribeForTasksCompletions(
	handler func(nodeID string, taskID uuid.UUID),
) error {
	ctx := context.Background()

	wrappedHandler := func(message string) {
		var msg TaskCompletionMessage
		if err := json.Unmarshal([]byte(message), &msg); err != nil {
			r.logger.Warn("Failed to unmarshal task completion message", "error", err)
			return
		}

		taskID, err := uuid.Parse(msg.TaskID)
		if err != nil {
			r.logger.Warn(
				"Failed to parse task ID from completion message",
				"taskId",
				msg.TaskID,
				"error",
				err,
			)
			return
		}

		handler(msg.NodeID, taskID)
	}

	err := r.pubsubCompletions.Subscribe(ctx, taskCompletionChannel, wrappedHandler)
	if err != nil {
		return fmt.Errorf("failed to subscribe to task completion channel: %w", err)
	}

	r.logger.Info("Subscribed to task completion channel")
	return nil
}

func (r *TaskNodesRegistry) UnsubscribeForTasksCompletions() error {
	err := r.pubsubCompletions.Close()
	if err != nil {
		return fmt.Errorf("failed to unsubscribe from task completion channel: %w", err)
	}

	r.logger.Info("Unsubscribed from task completion channel")
	return nil
}

func (r *TaskNodesRegistry) extractNodeIDFromKey(key, prefix, suffix string) uuid.UUID {
	nodeIDStr := strings.TrimPrefix(key, prefix)
	nodeIDStr = strings.TrimSuffix(nodeIDStr, suffix)

	nodeID, err := uuid.Parse(nodeIDStr)
	if err != nil {
		r.logger.Warn("Failed to parse node ID from key", "key", key, "error", err)
		return uuid.Nil
	}

	return nodeID
}

func (r *TaskNodesRegistry) pipelineGetKeys(keys []string) (map[string][]byte, error) {
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

func (r *TaskNodesRegistry) parseIntFromBytes(data []byte) (int64, error) {
	str := string(data)
	var count int64
	_, err := fmt.Sscanf(str, "%d", &count)
	if err != nil {
		return 0, fmt.Errorf("failed to parse integer from bytes: %w", err)
	}
	return count, nil
}

func (r *TaskNodesRegistry) cleanupDeadNodes() error {
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
			return fmt.Errorf("failed to scan node keys: %w", result.Error())
		}

		scanResult, err := result.AsScanEntry()
		if err != nil {
			return fmt.Errorf("failed to parse scan result: %w", err)
		}

		allKeys = append(allKeys, scanResult.Elements...)

		cursor = scanResult.Cursor
		if cursor == 0 {
			break
		}
	}

	if len(allKeys) == 0 {
		return nil
	}

	keyDataMap, err := r.pipelineGetKeys(allKeys)
	if err != nil {
		return fmt.Errorf("failed to pipeline get node keys: %w", err)
	}

	threshold := time.Now().UTC().Add(-deadNodeThreshold)
	var deadNodeKeys []string

	for key, data := range keyDataMap {

		// Skip if the key doesn't exist (data is empty)
		if len(data) == 0 {
			continue
		}

		var node TaskNode
		if err := json.Unmarshal(data, &node); err != nil {
			r.logger.Warn("Failed to unmarshal node data during cleanup", "key", key, "error", err)
			continue
		}

		// Skip nodes with zero/uninitialized heartbeat
		if node.LastHeartbeat.IsZero() {
			continue
		}

		if node.LastHeartbeat.Before(threshold) {
			nodeID := node.ID.String()
			infoKey := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, nodeID, nodeInfoKeySuffix)
			statsKey := fmt.Sprintf("%s%s%s", nodeActiveTasksPrefix, nodeID, nodeActiveTasksSuffix)

			deadNodeKeys = append(deadNodeKeys, infoKey, statsKey)
			r.logger.Info(
				"Marking node for cleanup",
				"nodeID", nodeID,
				"lastHeartbeat", node.LastHeartbeat,
				"threshold", threshold,
			)
		}
	}

	if len(deadNodeKeys) == 0 {
		return nil
	}

	delCtx, delCancel := context.WithTimeout(context.Background(), r.timeout)
	defer delCancel()

	result := r.client.Do(
		delCtx,
		r.client.B().Del().Key(deadNodeKeys...).Build(),
	)

	if result.Error() != nil {
		return fmt.Errorf("failed to delete dead node keys: %w", result.Error())
	}

	deletedCount, err := result.AsInt64()
	if err != nil {
		return fmt.Errorf("failed to parse deleted count: %w", err)
	}

	r.logger.Info("Cleaned up dead nodes", "deletedKeysCount", deletedCount)
	return nil
}
