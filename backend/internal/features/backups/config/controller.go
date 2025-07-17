package backups_config

import (
	"net/http"
	"postgresus-backend/internal/features/users"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type BackupConfigController struct {
	backupConfigService *BackupConfigService
	userService         *users.UserService
}

func (c *BackupConfigController) RegisterRoutes(router *gin.RouterGroup) {
	router.POST("/backup-configs/save", c.SaveBackupConfig)
	router.GET("/backup-configs/database/:id", c.GetBackupConfigByDbID)
	router.GET("/backup-configs/storage/:id/is-using", c.IsStorageUsing)
}

// SaveBackupConfig
// @Summary Save backup configuration
// @Description Save or update backup configuration for a database
// @Tags backup-configs
// @Accept json
// @Produce json
// @Param request body BackupConfig true "Backup configuration data"
// @Success 200 {object} BackupConfig
// @Failure 400
// @Failure 401
// @Failure 500
// @Router /backup-configs/save [post]
func (c *BackupConfigController) SaveBackupConfig(ctx *gin.Context) {
	var requestDTO BackupConfig
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

	// make sure we rely on full .Storage object
	requestDTO.StorageID = nil

	savedConfig, err := c.backupConfigService.SaveBackupConfigWithAuth(user, &requestDTO)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, savedConfig)
}

// GetBackupConfigByDbID
// @Summary Get backup configuration by database ID
// @Description Get backup configuration for a specific database
// @Tags backup-configs
// @Produce json
// @Param id path string true "Database ID"
// @Success 200 {object} BackupConfig
// @Failure 400
// @Failure 401
// @Failure 404
// @Router /backup-configs/database/{id} [get]
func (c *BackupConfigController) GetBackupConfigByDbID(ctx *gin.Context) {
	id, err := uuid.Parse(ctx.Param("id"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid database ID"})
		return
	}

	authorizationHeader := ctx.GetHeader("Authorization")
	if authorizationHeader == "" {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "authorization header is required"})
		return
	}

	_, err = c.userService.GetUserFromToken(authorizationHeader)
	if err != nil {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
		return
	}

	user, err := c.userService.GetUserFromToken(authorizationHeader)
	if err != nil {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
		return
	}

	backupConfig, err := c.backupConfigService.GetBackupConfigByDbIdWithAuth(user, id)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "backup configuration not found"})
		return
	}

	ctx.JSON(http.StatusOK, backupConfig)
}

// IsStorageUsing
// @Summary Check if storage is being used
// @Description Check if a storage is currently being used by any backup configuration
// @Tags backup-configs
// @Produce json
// @Param id path string true "Storage ID"
// @Success 200 {object} map[string]bool
// @Failure 400
// @Failure 401
// @Failure 500
// @Router /backup-configs/storage/{id}/is-using [get]
func (c *BackupConfigController) IsStorageUsing(ctx *gin.Context) {
	id, err := uuid.Parse(ctx.Param("id"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid storage ID"})
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

	isUsing, err := c.backupConfigService.IsStorageUsing(user, id)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"isUsing": isUsing})
}
