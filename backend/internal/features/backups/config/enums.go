package backups_config

type BackupNotificationType string

const (
	NotificationBackupFailed  BackupNotificationType = "BACKUP_FAILED"
	NotificationBackupSuccess BackupNotificationType = "BACKUP_SUCCESS"
)
