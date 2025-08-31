package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Checkinhg Health
type HealthResponse struct {
	Status    string                 `json:"status"`
	Timestamp time.Time              `json:"timestamp"`
	Version   string                 `json:"version"`
	Uptime    string                 `json:"uptime"`
	Services  map[string]interface{} `json:"services"`
	RequestID string                 `json:"request_id"`
}

type ReadyResponse struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Ready     bool      `json:"ready"`
	RequestID string    `json:"request_id"`
}

var startTime = time.Now()

// Healthy and chubby handler to check things up
func (s *Server) healthHandler(c gin.Context) {
	ctx := c.Request.Context()
	requestID := c.GetString("request_id")
	dbHealth := s.db.Health(ctx)
	queueHealth := s.queue.Health(ctx)

	status := "healthy"
	if dbHealth["status"] != "healthy" || queueHealth["status"] != "healthy" {
		status = "unhealthy"
	}
	response := HealthResponse{
		Status:    status,
		Timestamp: time.Now().UTC(),
		Version:   "1.0.0", // TODO: Get from build info
		Uptime:    time.Since(startTime).String(),
		RequestID: requestID,
		Services: map[string]interface{}{
			"database": dbHealth,
			"queue":    queueHealth,
		},
	}
	statusCode := http.StatusOK
	if status == "unhealthy" {
		statusCode = http.StatusServiceUnavailable
	}

	c.JSON(statusCode, response)
}
