package postgres_monitoring_settings

import (
	"net/http"
	"postgresus-backend/internal/features/users"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type PostgresMonitoringSettingsController struct {
	postgresMonitoringSettingsService *PostgresMonitoringSettingsService
	userService                       *users.UserService
}

func (c *PostgresMonitoringSettingsController) RegisterRoutes(router *gin.RouterGroup) {
	router.POST("/postgres-monitoring-settings/save", c.SaveSettings)
	router.GET("/postgres-monitoring-settings/database/:id", c.GetSettingsByDbID)
}

// SaveSettings
// @Summary Save postgres monitoring settings
// @Description Save or update postgres monitoring settings for a database
// @Tags postgres-monitoring-settings
// @Accept json
// @Produce json
// @Param request body PostgresMonitoringSettings true "Postgres monitoring settings data"
// @Success 200 {object} PostgresMonitoringSettings
// @Failure 400
// @Failure 401
// @Router /postgres-monitoring-settings/save [post]
func (c *PostgresMonitoringSettingsController) SaveSettings(ctx *gin.Context) {
	var requestDTO PostgresMonitoringSettings
	if err := ctx.ShouldBindJSON(&requestDTO); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	authorizationHeader := ctx.GetHeader("Authorization")
	if authorizationHeader == "" {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "authorization header is required"})
		return
	}

	user, err := c.userService.GetUserFromToken(authorizationHeader)
	if err != nil {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
		return
	}

	err = c.postgresMonitoringSettingsService.Save(user, &requestDTO)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, requestDTO)
}

// GetSettingsByDbID
// @Summary Get postgres monitoring settings by database ID
// @Description Get postgres monitoring settings for a specific database
// @Tags postgres-monitoring-settings
// @Produce json
// @Param id path string true "Database ID"
// @Success 200 {object} PostgresMonitoringSettings
// @Failure 400
// @Failure 401
// @Failure 404
// @Router /postgres-monitoring-settings/database/{id} [get]
func (c *PostgresMonitoringSettingsController) GetSettingsByDbID(ctx *gin.Context) {
	dbID := ctx.Param("id")
	if dbID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "database ID is required"})
		return
	}

	authorizationHeader := ctx.GetHeader("Authorization")
	if authorizationHeader == "" {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "authorization header is required"})
		return
	}

	user, err := c.userService.GetUserFromToken(authorizationHeader)
	if err != nil {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
		return
	}

	settings, err := c.postgresMonitoringSettingsService.GetByDbID(user, uuid.MustParse(dbID))
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "postgres monitoring settings not found"})
		return
	}

	ctx.JSON(http.StatusOK, settings)
}
