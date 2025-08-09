package postgres_monitoring_metrics

type PostgresMonitoringMetricType string

const (
	// system resources (need extensions)
	MetricsTypeSystemCPU PostgresMonitoringMetricType = "SYSTEM_CPU_USAGE"
	MetricsTypeSystemRAM PostgresMonitoringMetricType = "SYSTEM_RAM_USAGE"
	MetricsTypeSystemROM PostgresMonitoringMetricType = "SYSTEM_ROM_USAGE"
	MetricsTypeSystemIO  PostgresMonitoringMetricType = "SYSTEM_IO_USAGE"
	// db resources (don't need extensions)
	MetricsTypeDbRAM PostgresMonitoringMetricType = "DB_RAM_USAGE"
	MetricsTypeDbROM PostgresMonitoringMetricType = "DB_ROM_USAGE"
	MetricsTypeDbIO  PostgresMonitoringMetricType = "DB_IO_USAGE"
)

type PostgresMonitoringMetricValueType string

const (
	MetricsValueTypeByte    PostgresMonitoringMetricValueType = "BYTE"
	MetricsValueTypePercent PostgresMonitoringMetricValueType = "PERCENT"
)
