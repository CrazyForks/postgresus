package task_registry

import (
	"time"

	"github.com/google/uuid"
)

type TaskNode struct {
	ID            uuid.UUID `json:"id"`
	ThroughputMBs int       `json:"throughputMBs"`
	LastHeartbeat time.Time `json:"lastHeartbeat"`
}

type TaskNodeStats struct {
	ID          uuid.UUID `json:"id"`
	ActiveTasks int       `json:"activeTasks"`
}

type TaskSubmitMessage struct {
	NodeID         string `json:"nodeId"`
	TaskID         string `json:"taskId"`
	IsCallNotifier bool   `json:"isCallNotifier"`
}

type TaskCompletionMessage struct {
	NodeID string `json:"nodeId"`
	TaskID string `json:"taskId"`
}
