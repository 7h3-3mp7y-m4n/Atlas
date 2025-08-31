package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/7h3-3mp7y-m4n/atlas/internal/db/jobs"
	"github.com/7h3-3mp7y-m4n/atlas/internal/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type ErrorResponse struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
		Details   string `json:"details,omitempty"`
	} `json:"error"`
}

func (s *Server) createJobHandler(ctx *gin.Context) {
	requestID := ctx.GetString("request_id")
	log := s.logger.WithRequestID(requestID)
	var req models.JobCreateRequest
	if err := ctx.ShouldBindBodyWithJSON(&req); err != nil {
		log.Error("Invalid request body", zap.Error(err))
		s.respondWithError(ctx, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request body", err.Error())
		return
	}
	//create job now !
	job, err := s.createJobFromRequest(&req, requestID)
	if err != nil {
		log.Error("Failed to create job from request", zap.Error(err))
		s.respondWithError(ctx, http.StatusBadRequest, "INVALID_JOB_DATA", "Failed to create job", err.Error())
		return
	}
	// Save job to database
	jobRepo := jobs.NewJob(s.db.DB)
	if err := jobRepo.Create(ctx.Request.Context(), job); err != nil {
		log.Error("Failed to save job to database", zap.Error(err))
		s.respondWithError(ctx, http.StatusInternalServerError, "DATABASE_ERROR",
			"Failed to save job", "")
		return
	}
	if job.ScheduledAt == nil || job.ScheduledAt.Before(time.Now()) {
		if err := s.queue.Enqueue(ctx.Request.Context(), job); err != nil {
			log.Error("Failed to enqueue job",
				zap.String("job_id", job.ID.String()),
				zap.Error(err))
			// Don't fail the request, job can be retried later
		}
	}
	log.Info("Job created successfully", zap.String("job_id", job.ID.String()),
		zap.String("job_type", string(job.Type)),
		zap.String("status", string(job.Status)))

	ctx.JSON(http.StatusCreated, job.ToResponse())

}
func (s *Server) respondWithError(c *gin.Context, statusCode int, code, message, details string) {
	requestID := c.GetString("request_id")
	response := ErrorResponse{}
	response.Error.Code = code
	response.Error.Message = message
	response.Error.RequestID = requestID
	if details != "" {
		response.Error.Details = details
	}

	c.JSON(statusCode, response)
}

func (s *Server) createJobFromRequest(req *models.JobCreateRequest, requestID string) (*models.Job, error) {
	job := &models.Job{
		Type:      req.Type,
		Payload:   req.Payload,
		Status:    models.JobStatusPending,
		CreatedBy: req.CreatedBy,
		QueueName: req.QueueName,
	}
	if req.Priority != nil {
		job.Priority = *req.Priority
	} else {
		job.Priority = models.PriorityNormal
	}
	if req.MaxRetries != nil {
		job.MaxRetries = *req.MaxRetries
	} else {
		job.MaxRetries = 3
	}
	if req.RetryDelay != nil {
		duration, err := time.ParseDuration(*req.RetryDelay)
		if err != nil {
			return nil, fmt.Errorf("invalid retry_delay format: %w", err)
		}
		job.RetryDelay = duration
	} else {
		job.RetryDelay = 5 * time.Minute
	}
	if req.Timeout != nil {
		duration, err := time.ParseDuration(*req.Timeout)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout format: %w", err)
		}
		job.Timeout = duration
	} else {
		job.Timeout = 30 * time.Minute
	}
	if req.ScheduledAt != nil {
		job.ScheduledAt = req.ScheduledAt
	}
	if req.Tags != nil {
		job.Tags = models.StringArray(req.Tags)
	}
	if job.QueueName == "" {
		job.QueueName = "default"
	}
	return job, nil
}

func (s *Server) getJobStatsHandler(c *gin.Context) {
	requestID := c.GetString("request_id")
	log := s.logger.WithRequestID(requestID)
	// Get job statistics from database
	jobRepo := jobs.NewJob(s.db.DB)
	stats, err := jobRepo.GetStats(c.Request.Context())
	if err != nil {
		log.Error("Failed to get job statistics", zap.Error(err))
		s.respondWithError(c, http.StatusInternalServerError, "DATABASE_ERROR",
			"Failed to retrieve job statistics", "")
		return
	}
	c.JSON(http.StatusOK, stats)
}

func (s *Server) cancelJobHandler(c *gin.Context) {
	requestID := c.GetString("request_id")
	log := s.logger.WithRequestID(requestID)

	jobIDStr := c.Param("id")
	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		log.Error("Invalid job ID format", zap.String("job_id", jobIDStr))
		s.respondWithError(c, http.StatusBadRequest, "INVALID_JOB_ID",
			"Invalid job ID format", "")
		return
	}

	// Get job from database
	jobRepo := jobs.NewJob(s.db.DB)
	job, err := jobRepo.GetJobById(c.Request.Context(), jobID)
	if err != nil {
		if err == jobs.ErrJobNotFound { // TODO: Can i add this as More Absactraction < ??
			s.respondWithError(c, http.StatusNotFound, "JOB_NOT_FOUND",
				"Job not found", "")
			return
		}

		log.Error("Failed to get job for cancellation",
			zap.String("job_id", jobID.String()), zap.Error(err))
		s.respondWithError(c, http.StatusInternalServerError, "DATABASE_ERROR",
			"Failed to retrieve job", "")
		return
	}

	// Check if job can be cancelled ????
	if job.IsTerminalState() {
		s.respondWithError(c, http.StatusBadRequest, "JOB_NOT_CANCELLABLE",
			fmt.Sprintf("Job is already in terminal state: %s", job.Status), "")
		return
	}

	// Update job status to cancelled
	job.Status = models.JobStatusCancelled
	job.CompletedAt = &[]time.Time{time.Now()}[0]

	if err := jobRepo.UpdateJob(c.Request.Context(), job); err != nil {
		log.Error("Failed to update job status",
			zap.String("job_id", jobID.String()), zap.Error(err))
		s.respondWithError(c, http.StatusInternalServerError, "DATABASE_ERROR",
			"Failed to cancel job", "")
		return
	}
	log.Info("Job cancelled successfully",
		zap.String("job_id", job.ID.String()),
		zap.String("previous_status", string(job.Status)))
	c.JSON(http.StatusOK, job.ToResponse())
}

func (s *Server) listJobsHandler(c *gin.Context) {
	fmt.Println("soon work on this") //TODO:
}

func (s *Server) getJobHandler(c *gin.Context) {
	fmt.Println("soon work on this") //TODO:
}
