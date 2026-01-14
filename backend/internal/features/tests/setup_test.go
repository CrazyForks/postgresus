package tests

import (
	"os"
	"testing"

	"databasus-backend/internal/features/backups/backups/backuping"
	cache_utils "databasus-backend/internal/util/cache"
)

func TestMain(m *testing.M) {
	cache_utils.ClearAllCache()

	backuperNode := backuping.CreateTestBackuperNode()
	cancel := backuping.StartBackuperNodeForTest(&testing.T{}, backuperNode)

	exitCode := m.Run()

	backuping.StopBackuperNodeForTest(&testing.T{}, cancel, backuperNode)

	os.Exit(exitCode)
}
