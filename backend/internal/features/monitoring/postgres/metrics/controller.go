package postgres_monitoring_metrics

import (
	"net/http"
	"postgresus-backend/internal/features/users"

	"github.com/gin-gonic/gin"
)

type PostgresMonitoringMetricsController struct {
	metricsService *PostgresMonitoringMetricService
	userService    *users.UserService
}

func (c *PostgresMonitoringMetricsController) RegisterRoutes(router *gin.RouterGroup) {
	router.POST("/postgres-monitoring-metrics/get", c.GetMetrics)
}

// GetMetrics
// @Summary Get postgres monitoring metrics
// @Description Get postgres monitoring metrics for a database within a time range
// @Tags postgres-monitoring-metrics
// @Accept json
// @Produce json
// @Param request body GetMetricsRequest true "Metrics request data"
// @Success 200 {object} []PostgresMonitoringMetric
// @Failure 400
// @Failure 401
// @Router /postgres-monitoring-metrics/get [post]
func (c *PostgresMonitoringMetricsController) GetMetrics(ctx *gin.Context) {
	var requestDTO GetMetricsRequest
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

	metrics, err := c.metricsService.GetMetrics(
		user,
		requestDTO.DatabaseID,
		requestDTO.MetricType,
		requestDTO.From,
		requestDTO.To,
	)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, metrics)
}
