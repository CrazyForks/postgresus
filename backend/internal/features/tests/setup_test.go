package tests

import (
	"os"
	"testing"

	"databasus-backend/internal/features/backups/backups/backuping"
	"databasus-backend/internal/features/restores/restoring"
	cache_utils "databasus-backend/internal/util/cache"
)

func TestMain(m *testing.M) {
	cache_utils.ClearAllCache()

	backuperNode := backuping.CreateTestBackuperNode()
	cancelBackup := backuping.StartBackuperNodeForTest(&testing.T{}, backuperNode)

	restorerNode := restoring.CreateTestRestorerNode()
	cancelRestore := restoring.StartRestorerNodeForTest(&testing.T{}, restorerNode)

	exitCode := m.Run()

	backuping.StopBackuperNodeForTest(&testing.T{}, cancelBackup, backuperNode)
	restoring.StopRestorerNodeForTest(&testing.T{}, cancelRestore, restorerNode)

	os.Exit(exitCode)
}
