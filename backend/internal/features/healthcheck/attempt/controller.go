package healthcheck_attempt

import (
	"net/http"
	"postgresus-backend/internal/features/users"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type HealthcheckAttemptController struct {
	healthcheckAttemptService *HealthcheckAttemptService
	userService               *users.UserService
}

func (c *HealthcheckAttemptController) RegisterRoutes(router *gin.RouterGroup) {
	router.GET("/healthcheck-attempts/:databaseId", c.GetAttemptsByDatabase)
}

// GetAttemptsByDatabase
// @Summary Get healthcheck attempts by database
// @Description Get healthcheck attempts for a specific database with optional before date filter
// @Tags healthcheck-attempts
// @Produce json
// @Param Authorization header string true "JWT token"
// @Param databaseId path string true "Database ID"
// @Param afterDate query string false "After date (RFC3339 format)"
// @Success 200 {array} HealthcheckAttempt
// @Failure 400
// @Failure 401
// @Router /healthcheck-attempts/{databaseId} [get]
func (c *HealthcheckAttemptController) GetAttemptsByDatabase(ctx *gin.Context) {
	user, err := c.userService.GetUserFromToken(ctx.GetHeader("Authorization"))
	if err != nil {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}

	databaseID, err := uuid.Parse(ctx.Param("databaseId"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid database ID"})
		return
	}

	afterDate := time.Now().UTC()
	if afterDateStr := ctx.Query("afterDate"); afterDateStr != "" {
		parsedDate, err := time.Parse(time.RFC3339, afterDateStr)
		if err != nil {
			ctx.JSON(
				http.StatusBadRequest,
				gin.H{"error": "invalid afterDate format, use RFC3339"},
			)
			return
		}
		afterDate = parsedDate
	}

	attempts, err := c.healthcheckAttemptService.GetAttemptsByDatabase(
		*user,
		databaseID,
		afterDate,
	)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, attempts)
}
