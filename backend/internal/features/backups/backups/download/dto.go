package backups_download

import "github.com/google/uuid"

type GenerateDownloadTokenResponse struct {
	Token    string    `json:"token"`
	Filename string    `json:"filename"`
	BackupID uuid.UUID `json:"backupId"`
}
