package task_registry

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/logger"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_HearthbeatNodeInRegistry_RegistersNodeWithTTL(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	defer cleanupTestNode(registry, node)
	defer cache_utils.ClearAllCache()

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Equal(t, node.ID, nodes[0].ID)
	assert.Equal(t, node.ThroughputMBs, nodes[0].ThroughputMBs)
}

func Test_UnregisterNodeFromRegistry_RemovesNodeAndCounter(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	err = registry.IncrementTasksInProgress(node.ID.String())
	assert.NoError(t, err)

	err = registry.UnregisterNodeFromRegistry(node)
	assert.NoError(t, err)

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Empty(t, nodes)

	stats, err := registry.GetNodesStats()
	assert.NoError(t, err)
	assert.Empty(t, stats)
}

func Test_GetAvailableNodes_ReturnsAllRegisteredNodes(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestTaskNode()
	node2 := createTestTaskNode()
	node3 := createTestTaskNode()
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)
	defer cleanupTestNode(registry, node3)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node3)
	assert.NoError(t, err)

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 3)

	nodeIDs := make(map[uuid.UUID]bool)
	for _, node := range nodes {
		nodeIDs[node.ID] = true
	}
	assert.True(t, nodeIDs[node1.ID])
	assert.True(t, nodeIDs[node2.ID])
	assert.True(t, nodeIDs[node3.ID])
}

func Test_GetAvailableNodes_WhenNoNodesExist_ReturnsEmptySlice(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.NotNil(t, nodes)
	assert.Empty(t, nodes)
}

func Test_IncrementTasksInProgress_IncrementsCounter(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	defer cleanupTestNode(registry, node)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	err = registry.IncrementTasksInProgress(node.ID.String())
	assert.NoError(t, err)

	stats, err := registry.GetNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 1)
	assert.Equal(t, node.ID, stats[0].ID)
	assert.Equal(t, 1, stats[0].ActiveTasks)

	err = registry.IncrementTasksInProgress(node.ID.String())
	assert.NoError(t, err)

	stats, err = registry.GetNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 1)
	assert.Equal(t, 2, stats[0].ActiveTasks)
}

func Test_DecrementTasksInProgress_DecrementsCounter(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	defer cleanupTestNode(registry, node)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	err = registry.IncrementTasksInProgress(node.ID.String())
	assert.NoError(t, err)
	err = registry.IncrementTasksInProgress(node.ID.String())
	assert.NoError(t, err)
	err = registry.IncrementTasksInProgress(node.ID.String())
	assert.NoError(t, err)

	stats, err := registry.GetNodesStats()
	assert.NoError(t, err)
	assert.Equal(t, 3, stats[0].ActiveTasks)

	err = registry.DecrementTasksInProgress(node.ID.String())
	assert.NoError(t, err)

	stats, err = registry.GetNodesStats()
	assert.NoError(t, err)
	assert.Equal(t, 2, stats[0].ActiveTasks)

	err = registry.DecrementTasksInProgress(node.ID.String())
	assert.NoError(t, err)

	stats, err = registry.GetNodesStats()
	assert.NoError(t, err)
	assert.Equal(t, 1, stats[0].ActiveTasks)
}

func Test_DecrementTasksInProgress_WhenNegative_ResetsToZero(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	defer cleanupTestNode(registry, node)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	err = registry.DecrementTasksInProgress(node.ID.String())
	assert.NoError(t, err)

	stats, err := registry.GetNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 1)
	assert.Equal(t, 0, stats[0].ActiveTasks)
}

func Test_GetTaskNodesStats_ReturnsStatsForAllNodes(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestTaskNode()
	node2 := createTestTaskNode()
	node3 := createTestTaskNode()
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)
	defer cleanupTestNode(registry, node3)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node3)
	assert.NoError(t, err)

	err = registry.IncrementTasksInProgress(node1.ID.String())
	assert.NoError(t, err)

	err = registry.IncrementTasksInProgress(node2.ID.String())
	assert.NoError(t, err)
	err = registry.IncrementTasksInProgress(node2.ID.String())
	assert.NoError(t, err)

	err = registry.IncrementTasksInProgress(node3.ID.String())
	assert.NoError(t, err)
	err = registry.IncrementTasksInProgress(node3.ID.String())
	assert.NoError(t, err)
	err = registry.IncrementTasksInProgress(node3.ID.String())
	assert.NoError(t, err)

	stats, err := registry.GetNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 3)

	statsMap := make(map[uuid.UUID]int)
	for _, stat := range stats {
		statsMap[stat.ID] = stat.ActiveTasks
	}

	assert.Equal(t, 1, statsMap[node1.ID])
	assert.Equal(t, 2, statsMap[node2.ID])
	assert.Equal(t, 3, statsMap[node3.ID])
}

func Test_GetTaskNodesStats_WhenNoStats_ReturnsEmptySlice(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()

	stats, err := registry.GetNodesStats()
	assert.NoError(t, err)
	assert.NotNil(t, stats)
	assert.Empty(t, stats)
}

func Test_MultipleNodes_RegisteredAndQueriedCorrectly(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestTaskNode()
	node1.ThroughputMBs = 50
	node2 := createTestTaskNode()
	node2.ThroughputMBs = 100
	node3 := createTestTaskNode()
	node3.ThroughputMBs = 150
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)
	defer cleanupTestNode(registry, node3)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node3)
	assert.NoError(t, err)

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 3)

	nodeMap := make(map[uuid.UUID]TaskNode)
	for _, node := range nodes {
		nodeMap[node.ID] = node
	}

	assert.Equal(t, 50, nodeMap[node1.ID].ThroughputMBs)
	assert.Equal(t, 100, nodeMap[node2.ID].ThroughputMBs)
	assert.Equal(t, 150, nodeMap[node3.ID].ThroughputMBs)
}

func Test_TaskCounters_TrackedSeparatelyPerNode(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestTaskNode()
	node2 := createTestTaskNode()
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)

	err = registry.IncrementTasksInProgress(node1.ID.String())
	assert.NoError(t, err)
	err = registry.IncrementTasksInProgress(node1.ID.String())
	assert.NoError(t, err)

	err = registry.IncrementTasksInProgress(node2.ID.String())
	assert.NoError(t, err)

	stats, err := registry.GetNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 2)

	statsMap := make(map[uuid.UUID]int)
	for _, stat := range stats {
		statsMap[stat.ID] = stat.ActiveTasks
	}

	assert.Equal(t, 2, statsMap[node1.ID])
	assert.Equal(t, 1, statsMap[node2.ID])

	err = registry.DecrementTasksInProgress(node1.ID.String())
	assert.NoError(t, err)

	stats, err = registry.GetNodesStats()
	assert.NoError(t, err)

	statsMap = make(map[uuid.UUID]int)
	for _, stat := range stats {
		statsMap[stat.ID] = stat.ActiveTasks
	}

	assert.Equal(t, 1, statsMap[node1.ID])
	assert.Equal(t, 1, statsMap[node2.ID])
}

func Test_GetAvailableNodes_SkipsInvalidJsonData(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	defer cleanupTestNode(registry, node)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), registry.timeout)
	defer cancel()

	invalidKey := nodeInfoKeyPrefix + uuid.New().String() + nodeInfoKeySuffix
	registry.client.Do(
		ctx,
		registry.client.B().Set().Key(invalidKey).Value("invalid json data").Build(),
	)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), registry.timeout)
		defer cleanupCancel()
		registry.client.Do(cleanupCtx, registry.client.B().Del().Key(invalidKey).Build())
	}()

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Equal(t, node.ID, nodes[0].ID)
}

func Test_PipelineGetKeys_HandlesEmptyKeysList(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()

	keyDataMap, err := registry.pipelineGetKeys([]string{})
	assert.NoError(t, err)
	assert.NotNil(t, keyDataMap)
	assert.Empty(t, keyDataMap)
}

func Test_HearthbeatNodeInRegistry_UpdatesLastHeartbeat(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	originalHeartbeat := node.LastHeartbeat
	defer cleanupTestNode(registry, node)

	time.Sleep(10 * time.Millisecond)

	node.LastHeartbeat = time.Now().UTC()
	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.True(t, nodes[0].LastHeartbeat.After(originalHeartbeat))
}

func Test_HearthbeatNodeInRegistry_RejectsZeroTimestamp(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()

	err := registry.HearthbeatNodeInRegistry(time.Time{}, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "zero heartbeat timestamp")

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 0)
}

func createTestRegistry() *TaskNodesRegistry {
	return &TaskNodesRegistry{
		cache_utils.GetValkeyClient(),
		logger.GetLogger(),
		cache_utils.DefaultCacheTimeout,
		cache_utils.NewPubSubManager(),
		cache_utils.NewPubSubManager(),
	}
}

func createTestTaskNode() TaskNode {
	return TaskNode{
		ID:            uuid.New(),
		ThroughputMBs: 100,
		LastHeartbeat: time.Now().UTC(),
	}
}

func cleanupTestNode(registry *TaskNodesRegistry, node TaskNode) {
	registry.UnregisterNodeFromRegistry(node)
}

func Test_AssignTaskToNode_PublishesJsonMessageToChannel(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	taskID := uuid.New()

	err := registry.AssignTaskToNode(node.ID.String(), taskID, true)
	assert.NoError(t, err)
}

func Test_SubscribeNodeForTasksAssignment_ReceivesSubmittedTasksForMatchingNode(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	taskID := uuid.New()
	defer registry.UnsubscribeNodeForTasksAssignments()

	receivedTaskID := make(chan uuid.UUID, 1)
	handler := func(id uuid.UUID, isCallNotifier bool) {
		receivedTaskID <- id
	}

	err := registry.SubscribeNodeForTasksAssignment(node.ID.String(), handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.AssignTaskToNode(node.ID.String(), taskID, true)
	assert.NoError(t, err)

	select {
	case received := <-receivedTaskID:
		assert.Equal(t, taskID, received)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for task message")
	}
}

func Test_SubscribeNodeForTasksAssignment_FiltersOutTasksForDifferentNode(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestTaskNode()
	node2 := createTestTaskNode()
	taskID := uuid.New()
	defer registry.UnsubscribeNodeForTasksAssignments()

	receivedTaskID := make(chan uuid.UUID, 1)
	handler := func(id uuid.UUID, isCallNotifier bool) {
		receivedTaskID <- id
	}

	err := registry.SubscribeNodeForTasksAssignment(node1.ID.String(), handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.AssignTaskToNode(node2.ID.String(), taskID, false)
	assert.NoError(t, err)

	select {
	case <-receivedTaskID:
		t.Fatal("Should not receive task for different node")
	case <-time.After(500 * time.Millisecond):
	}
}

func Test_SubscribeNodeForTasksAssignment_ParsesJsonAndTaskIdCorrectly(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	taskID1 := uuid.New()
	taskID2 := uuid.New()
	defer registry.UnsubscribeNodeForTasksAssignments()

	receivedTasks := make(chan uuid.UUID, 2)
	handler := func(id uuid.UUID, isCallNotifier bool) {
		receivedTasks <- id
	}

	err := registry.SubscribeNodeForTasksAssignment(node.ID.String(), handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.AssignTaskToNode(node.ID.String(), taskID1, true)
	assert.NoError(t, err)

	err = registry.AssignTaskToNode(node.ID.String(), taskID2, false)
	assert.NoError(t, err)

	received1 := <-receivedTasks
	received2 := <-receivedTasks

	receivedIDs := []uuid.UUID{received1, received2}
	assert.Contains(t, receivedIDs, taskID1)
	assert.Contains(t, receivedIDs, taskID2)
}

func Test_SubscribeNodeForTasksAssignment_HandlesInvalidJson(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	defer registry.UnsubscribeNodeForTasksAssignments()

	receivedTaskID := make(chan uuid.UUID, 1)
	handler := func(id uuid.UUID, isCallNotifier bool) {
		receivedTaskID <- id
	}

	err := registry.SubscribeNodeForTasksAssignment(node.ID.String(), handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	err = registry.pubsubTasks.Publish(ctx, "backup:submit", "invalid json")
	assert.NoError(t, err)

	select {
	case <-receivedTaskID:
		t.Fatal("Should not receive task for invalid JSON")
	case <-time.After(500 * time.Millisecond):
	}
}

func Test_UnsubscribeNodeForTasksAssignments_StopsReceivingMessages(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	taskID1 := uuid.New()
	taskID2 := uuid.New()

	receivedTaskID := make(chan uuid.UUID, 2)
	handler := func(id uuid.UUID, isCallNotifier bool) {
		receivedTaskID <- id
	}

	err := registry.SubscribeNodeForTasksAssignment(node.ID.String(), handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.AssignTaskToNode(node.ID.String(), taskID1, true)
	assert.NoError(t, err)

	received := <-receivedTaskID
	assert.Equal(t, taskID1, received)

	err = registry.UnsubscribeNodeForTasksAssignments()
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.AssignTaskToNode(node.ID.String(), taskID2, false)
	assert.NoError(t, err)

	select {
	case <-receivedTaskID:
		t.Fatal("Should not receive task after unsubscribe")
	case <-time.After(500 * time.Millisecond):
	}
}

func Test_SubscribeNodeForTasksAssignment_WhenAlreadySubscribed_ReturnsError(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	defer registry.UnsubscribeNodeForTasksAssignments()

	handler := func(id uuid.UUID, isCallNotifier bool) {}

	err := registry.SubscribeNodeForTasksAssignment(node.ID.String(), handler)
	assert.NoError(t, err)

	err = registry.SubscribeNodeForTasksAssignment(node.ID.String(), handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already subscribed")
}

func Test_MultipleNodes_EachReceivesOnlyTheirTasks(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry1 := createTestRegistry()
	registry2 := createTestRegistry()
	registry3 := createTestRegistry()

	node1 := createTestTaskNode()
	node2 := createTestTaskNode()
	node3 := createTestTaskNode()

	taskID1 := uuid.New()
	taskID2 := uuid.New()
	taskID3 := uuid.New()

	defer registry1.UnsubscribeNodeForTasksAssignments()
	defer registry2.UnsubscribeNodeForTasksAssignments()
	defer registry3.UnsubscribeNodeForTasksAssignments()
	defer cleanupTestNode(registry1, node1)
	defer cleanupTestNode(registry1, node2)
	defer cleanupTestNode(registry1, node3)

	receivedTasks1 := make(chan uuid.UUID, 3)
	receivedTasks2 := make(chan uuid.UUID, 3)
	receivedTasks3 := make(chan uuid.UUID, 3)

	handler1 := func(id uuid.UUID, isCallNotifier bool) { receivedTasks1 <- id }
	handler2 := func(id uuid.UUID, isCallNotifier bool) { receivedTasks2 <- id }
	handler3 := func(id uuid.UUID, isCallNotifier bool) { receivedTasks3 <- id }

	err := registry1.SubscribeNodeForTasksAssignment(node1.ID.String(), handler1)
	assert.NoError(t, err)

	err = registry2.SubscribeNodeForTasksAssignment(node2.ID.String(), handler2)
	assert.NoError(t, err)

	err = registry3.SubscribeNodeForTasksAssignment(node3.ID.String(), handler3)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	submitRegistry := createTestRegistry()
	err = submitRegistry.AssignTaskToNode(node1.ID.String(), taskID1, true)
	assert.NoError(t, err)

	err = submitRegistry.AssignTaskToNode(node2.ID.String(), taskID2, false)
	assert.NoError(t, err)

	err = submitRegistry.AssignTaskToNode(node3.ID.String(), taskID3, true)
	assert.NoError(t, err)

	select {
	case received := <-receivedTasks1:
		assert.Equal(t, taskID1, received)
	case <-time.After(2 * time.Second):
		t.Fatal("Node 1 timeout waiting for task message")
	}

	select {
	case received := <-receivedTasks2:
		assert.Equal(t, taskID2, received)
	case <-time.After(2 * time.Second):
		t.Fatal("Node 2 timeout waiting for task message")
	}

	select {
	case received := <-receivedTasks3:
		assert.Equal(t, taskID3, received)
	case <-time.After(2 * time.Second):
		t.Fatal("Node 3 timeout waiting for task message")
	}

	select {
	case <-receivedTasks1:
		t.Fatal("Node 1 should not receive additional tasks")
	case <-time.After(300 * time.Millisecond):
	}

	select {
	case <-receivedTasks2:
		t.Fatal("Node 2 should not receive additional tasks")
	case <-time.After(300 * time.Millisecond):
	}

	select {
	case <-receivedTasks3:
		t.Fatal("Node 3 should not receive additional tasks")
	case <-time.After(300 * time.Millisecond):
	}
}

func Test_PublishTaskCompletion_PublishesMessageToChannel(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	taskID := uuid.New()

	err := registry.PublishTaskCompletion(node.ID.String(), taskID)
	assert.NoError(t, err)
}

func Test_SubscribeForTasksCompletions_ReceivesCompletedTasks(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	taskID := uuid.New()
	defer registry.UnsubscribeForTasksCompletions()

	receivedTaskID := make(chan uuid.UUID, 1)
	receivedNodeID := make(chan string, 1)
	handler := func(nodeID string, taskID uuid.UUID) {
		receivedNodeID <- nodeID
		receivedTaskID <- taskID
	}

	err := registry.SubscribeForTasksCompletions(handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.PublishTaskCompletion(node.ID.String(), taskID)
	assert.NoError(t, err)

	select {
	case receivedNode := <-receivedNodeID:
		assert.Equal(t, node.ID.String(), receivedNode)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for node ID")
	}

	select {
	case received := <-receivedTaskID:
		assert.Equal(t, taskID, received)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for task completion message")
	}
}

func Test_SubscribeForTasksCompletions_ParsesJsonCorrectly(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	taskID1 := uuid.New()
	taskID2 := uuid.New()
	defer registry.UnsubscribeForTasksCompletions()

	receivedTasks := make(chan uuid.UUID, 2)
	handler := func(nodeID string, taskID uuid.UUID) {
		receivedTasks <- taskID
	}

	err := registry.SubscribeForTasksCompletions(handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.PublishTaskCompletion(node.ID.String(), taskID1)
	assert.NoError(t, err)

	err = registry.PublishTaskCompletion(node.ID.String(), taskID2)
	assert.NoError(t, err)

	received1 := <-receivedTasks
	received2 := <-receivedTasks

	receivedIDs := []uuid.UUID{received1, received2}
	assert.Contains(t, receivedIDs, taskID1)
	assert.Contains(t, receivedIDs, taskID2)
}

func Test_SubscribeForTasksCompletions_HandlesInvalidJson(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	defer registry.UnsubscribeForTasksCompletions()

	receivedTaskID := make(chan uuid.UUID, 1)
	handler := func(nodeID string, taskID uuid.UUID) {
		receivedTaskID <- taskID
	}

	err := registry.SubscribeForTasksCompletions(handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	err = registry.pubsubCompletions.Publish(ctx, "backup:completion", "invalid json")
	assert.NoError(t, err)

	select {
	case <-receivedTaskID:
		t.Fatal("Should not receive task for invalid JSON")
	case <-time.After(500 * time.Millisecond):
	}
}

func Test_UnsubscribeForTasksCompletions_StopsReceivingMessages(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestTaskNode()
	taskID1 := uuid.New()
	taskID2 := uuid.New()

	receivedTaskID := make(chan uuid.UUID, 2)
	handler := func(nodeID string, taskID uuid.UUID) {
		receivedTaskID <- taskID
	}

	err := registry.SubscribeForTasksCompletions(handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.PublishTaskCompletion(node.ID.String(), taskID1)
	assert.NoError(t, err)

	received := <-receivedTaskID
	assert.Equal(t, taskID1, received)

	err = registry.UnsubscribeForTasksCompletions()
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.PublishTaskCompletion(node.ID.String(), taskID2)
	assert.NoError(t, err)

	select {
	case <-receivedTaskID:
		t.Fatal("Should not receive task after unsubscribe")
	case <-time.After(500 * time.Millisecond):
	}
}

func Test_SubscribeForTasksCompletions_WhenAlreadySubscribed_ReturnsError(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	defer registry.UnsubscribeForTasksCompletions()

	handler := func(nodeID string, taskID uuid.UUID) {}

	err := registry.SubscribeForTasksCompletions(handler)
	assert.NoError(t, err)

	err = registry.SubscribeForTasksCompletions(handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already subscribed")
}

func Test_MultipleSubscribers_EachReceivesCompletionMessages(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry1 := createTestRegistry()
	registry2 := createTestRegistry()
	registry3 := createTestRegistry()

	node1 := createTestTaskNode()
	node2 := createTestTaskNode()
	node3 := createTestTaskNode()

	taskID1 := uuid.New()
	taskID2 := uuid.New()
	taskID3 := uuid.New()

	defer registry1.UnsubscribeForTasksCompletions()
	defer registry2.UnsubscribeForTasksCompletions()
	defer registry3.UnsubscribeForTasksCompletions()
	defer cleanupTestNode(registry1, node1)
	defer cleanupTestNode(registry1, node2)
	defer cleanupTestNode(registry1, node3)

	receivedTasks1 := make(chan uuid.UUID, 3)
	receivedTasks2 := make(chan uuid.UUID, 3)
	receivedTasks3 := make(chan uuid.UUID, 3)

	handler1 := func(nodeID string, taskID uuid.UUID) { receivedTasks1 <- taskID }
	handler2 := func(nodeID string, taskID uuid.UUID) { receivedTasks2 <- taskID }
	handler3 := func(nodeID string, taskID uuid.UUID) { receivedTasks3 <- taskID }

	err := registry1.SubscribeForTasksCompletions(handler1)
	assert.NoError(t, err)

	err = registry2.SubscribeForTasksCompletions(handler2)
	assert.NoError(t, err)

	err = registry3.SubscribeForTasksCompletions(handler3)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	publishRegistry := createTestRegistry()
	err = publishRegistry.PublishTaskCompletion(node1.ID.String(), taskID1)
	assert.NoError(t, err)

	err = publishRegistry.PublishTaskCompletion(node2.ID.String(), taskID2)
	assert.NoError(t, err)

	err = publishRegistry.PublishTaskCompletion(node3.ID.String(), taskID3)
	assert.NoError(t, err)

	receivedAll1 := []uuid.UUID{}
	receivedAll2 := []uuid.UUID{}
	receivedAll3 := []uuid.UUID{}

	for i := 0; i < 3; i++ {
		select {
		case received := <-receivedTasks1:
			receivedAll1 = append(receivedAll1, received)
		case <-time.After(2 * time.Second):
			t.Fatal("Subscriber 1 timeout waiting for completion message")
		}
	}

	for i := 0; i < 3; i++ {
		select {
		case received := <-receivedTasks2:
			receivedAll2 = append(receivedAll2, received)
		case <-time.After(2 * time.Second):
			t.Fatal("Subscriber 2 timeout waiting for completion message")
		}
	}

	for i := 0; i < 3; i++ {
		select {
		case received := <-receivedTasks3:
			receivedAll3 = append(receivedAll3, received)
		case <-time.After(2 * time.Second):
			t.Fatal("Subscriber 3 timeout waiting for completion message")
		}
	}

	assert.Contains(t, receivedAll1, taskID1)
	assert.Contains(t, receivedAll1, taskID2)
	assert.Contains(t, receivedAll1, taskID3)

	assert.Contains(t, receivedAll2, taskID1)
	assert.Contains(t, receivedAll2, taskID2)
	assert.Contains(t, receivedAll2, taskID3)

	assert.Contains(t, receivedAll3, taskID1)
	assert.Contains(t, receivedAll3, taskID2)
	assert.Contains(t, receivedAll3, taskID3)
}

func Test_GetAvailableNodes_ExcludesStaleNodesFromCache(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestTaskNode()
	node2 := createTestTaskNode()
	node3 := createTestTaskNode()
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)
	defer cleanupTestNode(registry, node3)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node3)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), registry.timeout)
	defer cancel()

	key := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, node2.ID.String(), nodeInfoKeySuffix)
	result := registry.client.Do(ctx, registry.client.B().Get().Key(key).Build())
	assert.NoError(t, result.Error())

	data, err := result.AsBytes()
	assert.NoError(t, err)

	var node TaskNode
	err = json.Unmarshal(data, &node)
	assert.NoError(t, err)

	node.LastHeartbeat = time.Now().UTC().Add(-3 * time.Minute)
	modifiedData, err := json.Marshal(node)
	assert.NoError(t, err)

	setCtx, setCancel := context.WithTimeout(context.Background(), registry.timeout)
	defer setCancel()
	setResult := registry.client.Do(
		setCtx,
		registry.client.B().Set().Key(key).Value(string(modifiedData)).Build(),
	)
	assert.NoError(t, setResult.Error())

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 2)

	nodeIDs := make(map[uuid.UUID]bool)
	for _, n := range nodes {
		nodeIDs[n.ID] = true
	}
	assert.True(t, nodeIDs[node1.ID])
	assert.False(t, nodeIDs[node2.ID])
	assert.True(t, nodeIDs[node3.ID])
}

func Test_GetNodesStats_ExcludesStaleNodesFromCache(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestTaskNode()
	node2 := createTestTaskNode()
	node3 := createTestTaskNode()
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)
	defer cleanupTestNode(registry, node3)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node3)
	assert.NoError(t, err)

	err = registry.IncrementTasksInProgress(node1.ID.String())
	assert.NoError(t, err)
	err = registry.IncrementTasksInProgress(node2.ID.String())
	assert.NoError(t, err)
	err = registry.IncrementTasksInProgress(node3.ID.String())
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), registry.timeout)
	defer cancel()

	key := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, node2.ID.String(), nodeInfoKeySuffix)
	result := registry.client.Do(ctx, registry.client.B().Get().Key(key).Build())
	assert.NoError(t, result.Error())

	data, err := result.AsBytes()
	assert.NoError(t, err)

	var node TaskNode
	err = json.Unmarshal(data, &node)
	assert.NoError(t, err)

	node.LastHeartbeat = time.Now().UTC().Add(-3 * time.Minute)
	modifiedData, err := json.Marshal(node)
	assert.NoError(t, err)

	setCtx, setCancel := context.WithTimeout(context.Background(), registry.timeout)
	defer setCancel()
	setResult := registry.client.Do(
		setCtx,
		registry.client.B().Set().Key(key).Value(string(modifiedData)).Build(),
	)
	assert.NoError(t, setResult.Error())

	stats, err := registry.GetNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 2)

	statsMap := make(map[uuid.UUID]int)
	for _, stat := range stats {
		statsMap[stat.ID] = stat.ActiveTasks
	}

	assert.Equal(t, 1, statsMap[node1.ID])
	_, hasNode2 := statsMap[node2.ID]
	assert.False(t, hasNode2)
	assert.Equal(t, 1, statsMap[node3.ID])
}

func Test_CleanupDeadNodes_RemovesNodeInfoAndCounter(t *testing.T) {
	cache_utils.ClearAllCache()
	defer cache_utils.ClearAllCache()

	registry := createTestRegistry()
	node1 := createTestTaskNode()
	node2 := createTestTaskNode()
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)

	err = registry.IncrementTasksInProgress(node1.ID.String())
	assert.NoError(t, err)
	err = registry.IncrementTasksInProgress(node2.ID.String())
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), registry.timeout)
	defer cancel()

	key := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, node2.ID.String(), nodeInfoKeySuffix)
	result := registry.client.Do(ctx, registry.client.B().Get().Key(key).Build())
	assert.NoError(t, result.Error())

	data, err := result.AsBytes()
	assert.NoError(t, err)

	var node TaskNode
	err = json.Unmarshal(data, &node)
	assert.NoError(t, err)

	node.LastHeartbeat = time.Now().UTC().Add(-3 * time.Minute)
	modifiedData, err := json.Marshal(node)
	assert.NoError(t, err)

	setCtx, setCancel := context.WithTimeout(context.Background(), registry.timeout)
	defer setCancel()
	setResult := registry.client.Do(
		setCtx,
		registry.client.B().Set().Key(key).Value(string(modifiedData)).Build(),
	)
	assert.NoError(t, setResult.Error())

	err = registry.cleanupDeadNodes()
	assert.NoError(t, err)

	checkCtx, checkCancel := context.WithTimeout(context.Background(), registry.timeout)
	defer checkCancel()

	infoKey := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, node2.ID.String(), nodeInfoKeySuffix)
	infoResult := registry.client.Do(checkCtx, registry.client.B().Get().Key(infoKey).Build())
	assert.Error(t, infoResult.Error())

	counterKey := fmt.Sprintf(
		"%s%s%s",
		nodeActiveTasksPrefix,
		node2.ID.String(),
		nodeActiveTasksSuffix,
	)
	counterCtx, counterCancel := context.WithTimeout(context.Background(), registry.timeout)
	defer counterCancel()
	counterResult := registry.client.Do(
		counterCtx,
		registry.client.B().Get().Key(counterKey).Build(),
	)
	assert.Error(t, counterResult.Error())

	activeInfoKey := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, node1.ID.String(), nodeInfoKeySuffix)
	activeCtx, activeCancel := context.WithTimeout(context.Background(), registry.timeout)
	defer activeCancel()
	activeResult := registry.client.Do(
		activeCtx,
		registry.client.B().Get().Key(activeInfoKey).Build(),
	)
	assert.NoError(t, activeResult.Error())

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Equal(t, node1.ID, nodes[0].ID)

	stats, err := registry.GetNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 1)
	assert.Equal(t, node1.ID, stats[0].ID)
}
