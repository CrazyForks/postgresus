package postgres_monitoring_metrics

import (
	"time"

	"github.com/google/uuid"
)

type PostgresMonitoringMetric struct {
	ID         uuid.UUID                         `json:"id"         gorm:"column:id;primaryKey;type:uuid;default:gen_random_uuid()"`
	DatabaseID uuid.UUID                         `json:"databaseId" gorm:"column:database_id;not null;type:uuid"`
	Metric     PostgresMonitoringMetricType      `json:"metric"     gorm:"column:metric;not null"`
	ValueType  PostgresMonitoringMetricValueType `json:"valueType"  gorm:"column:value_type;not null"`
	Value      float64                           `json:"value"      gorm:"column:value;not null"`
	CreatedAt  time.Time                         `json:"createdAt"  gorm:"column:created_at;not null"`
}

func (p *PostgresMonitoringMetric) TableName() string {
	return "postgres_monitoring_metrics"
}
