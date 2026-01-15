package backuping

import "github.com/google/uuid"

type BackupToNodeRelation struct {
	NodeID     uuid.UUID   `json:"nodeId"`
	BackupsIDs []uuid.UUID `json:"backupsIds"`
}
