package backups

import (
	backups_core "databasus-backend/internal/features/backups/backups/core"
	"databasus-backend/internal/features/backups/backups/encryption"
	"io"
)

type GetBackupsRequest struct {
	DatabaseID string `form:"database_id" binding:"required"`
	Limit      int    `form:"limit"`
	Offset     int    `form:"offset"`
}

type GetBackupsResponse struct {
	Backups []*backups_core.Backup `json:"backups"`
	Total   int64                  `json:"total"`
	Limit   int                    `json:"limit"`
	Offset  int                    `json:"offset"`
}

type DecryptionReaderCloser struct {
	*encryption.DecryptionReader
	BaseReader io.ReadCloser
}

func (r *DecryptionReaderCloser) Close() error {
	return r.BaseReader.Close()
}
