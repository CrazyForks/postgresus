package task_registry

import (
	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/logger"
)

var taskNodesRegistry = &TaskNodesRegistry{
	cache_utils.GetValkeyClient(),
	logger.GetLogger(),
	cache_utils.DefaultCacheTimeout,
	cache_utils.NewPubSubManager(),
	cache_utils.NewPubSubManager(),
}

func GetTaskNodesRegistry() *TaskNodesRegistry {
	return taskNodesRegistry
}
