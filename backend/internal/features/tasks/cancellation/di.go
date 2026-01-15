package task_cancellation

import (
	"context"
	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/logger"
	"sync"

	"github.com/google/uuid"
)

var taskCancelManager = &TaskCancelManager{
	sync.RWMutex{},
	make(map[uuid.UUID]context.CancelFunc),
	cache_utils.NewPubSubManager(),
	logger.GetLogger(),
}

func GetTaskCancelManager() *TaskCancelManager {
	return taskCancelManager
}

func SetupDependencies() {
	taskCancelManager.StartSubscription()
}
