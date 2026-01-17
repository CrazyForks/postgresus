package restores

import (
	"context"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"databasus-backend/internal/features/backups/backups"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/restores/restoring"
	workspaces_controllers "databasus-backend/internal/features/workspaces/controllers"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
)

func CreateTestRouter() *gin.Engine {
	router := workspaces_testing.CreateTestRouter(
		workspaces_controllers.GetWorkspaceController(),
		workspaces_controllers.GetMembershipController(),
		databases.GetDatabaseController(),
		backups_config.GetBackupConfigController(),
		backups.GetBackupController(),
		GetRestoreController(),
	)

	v1 := router.Group("/api/v1")
	backups.GetBackupController().RegisterPublicRoutes(v1)

	return router
}

func SetupMockRestoreNode(t *testing.T) (uuid.UUID, context.CancelFunc) {
	nodeID := uuid.New()
	err := restoring.CreateMockNodeInRegistry(
		nodeID,
		100,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("Failed to create mock node: %v", err)
	}

	cleanup := func() {
		// Node will expire naturally from registry
	}

	return nodeID, cleanup
}
