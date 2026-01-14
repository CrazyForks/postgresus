package system_healthcheck

import (
	"databasus-backend/internal/config"
	"databasus-backend/internal/features/backups/backups/backuping"
	"databasus-backend/internal/features/disk"
	"databasus-backend/internal/storage"
	"errors"
)

type HealthcheckService struct {
	diskService             *disk.DiskService
	backupBackgroundService *backuping.BackupsScheduler
	backuperNode            *backuping.BackuperNode
}

func (s *HealthcheckService) IsHealthy() error {
	diskUsage, err := s.diskService.GetDiskUsage()
	if err != nil {
		return errors.New("cannot get disk usage")
	}

	if float64(diskUsage.UsedSpaceBytes) >= float64(diskUsage.TotalSpaceBytes)*0.95 {
		return errors.New("more than 95% of the disk is used")
	}

	db := storage.GetDb()
	err = db.Raw("SELECT 1").Error

	if err != nil {
		return errors.New("cannot connect to the database")
	}

	if config.GetEnv().IsPrimaryNode {
		if !s.backupBackgroundService.IsSchedulerRunning() {
			return errors.New("backups are not running for more than 5 minutes")
		}
	}

	if config.GetEnv().IsBackupNode {
		if !s.backuperNode.IsBackuperRunning() {
			return errors.New("backuper node is not running for more than 5 minutes")
		}
	}

	return nil
}
