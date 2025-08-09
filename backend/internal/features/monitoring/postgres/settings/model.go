package postgres_monitoring_settings

import (
	"postgresus-backend/internal/features/databases"
	"postgresus-backend/internal/util/tools"
	"strings"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type PostgresMonitoringSettings struct {
	DatabaseID uuid.UUID           `json:"databaseId" gorm:"column:database_id;not null"`
	Database   *databases.Database `json:"database" gorm:"foreignKey:DatabaseID"`

	IsSystemResourcesMonitoringEnabled bool  `json:"isSystemResourcesMonitoringEnabled" gorm:"column:is_system_resources_monitoring_enabled;not null"`
	IsDbResourcesMonitoringEnabled     bool  `json:"isDbResourcesMonitoringEnabled"     gorm:"column:is_db_resources_monitoring_enabled;not null"`
	MonitoringIntervalSeconds          int64 `json:"monitoringIntervalSeconds"          gorm:"column:monitoring_interval_seconds;not null"`

	InstalledExtensions    []tools.PostgresqlExtension `json:"installedExtensions"    gorm:"-"`
	InstalledExtensionsRaw string                      `json:"-" gorm:"column:installed_extensions_raw"`
}

func (p *PostgresMonitoringSettings) TableName() string {
	return "postgres_monitoring_settings"
}

func (p *PostgresMonitoringSettings) AfterFind(tx *gorm.DB) error {
	if p.InstalledExtensionsRaw != "" {
		rawExtensions := strings.Split(p.InstalledExtensionsRaw, ",")

		p.InstalledExtensions = make([]tools.PostgresqlExtension, len(rawExtensions))

		for i, ext := range rawExtensions {
			p.InstalledExtensions[i] = tools.PostgresqlExtension(ext)
		}
	} else {
		p.InstalledExtensions = []tools.PostgresqlExtension{}
	}

	return nil
}

func (p *PostgresMonitoringSettings) BeforeSave(tx *gorm.DB) error {
	extensions := make([]string, len(p.InstalledExtensions))

	for i, ext := range p.InstalledExtensions {
		extensions[i] = string(ext)
	}

	p.InstalledExtensionsRaw = strings.Join(extensions, ",")

	return nil
}

func (p *PostgresMonitoringSettings) AddInstalledExtensions(extensions []tools.PostgresqlExtension) {
	for _, ext := range extensions {
		exists := false

		for _, existing := range p.InstalledExtensions {
			if existing == ext {
				exists = true
				break
			}
		}

		if !exists {
			p.InstalledExtensions = append(p.InstalledExtensions, ext)
		}
	}
}
