package service

import (
	"context"
	"fmt"
	"time"

	"github.com/7h3-3mp7y-m4n/atlas/internal/db/jobs"
	"github.com/7h3-3mp7y-m4n/atlas/internal/models"
	"github.com/7h3-3mp7y-m4n/atlas/internal/queue"
	"github.com/7h3-3mp7y-m4n/atlas/pkg/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type JobService struct {
	job    *jobs.Job
	queue  *queue.RedisQueue
	logger *logger.Logger
}

func NewJobService(job *jobs.Job, queue *queue.RedisQueue, logger *logger.Logger) *JobService {
	return &JobService{
		job:    job,
		queue:  queue,
		logger: logger,
	}
}

func (j *JobService) CreateJob(ctx context.Context, req *models.JobCreateRequest) (*models.Job, error) {
	job, err := j.createJobFromRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to create job from request: %w", err)
	}
	if err := j.job.Create(ctx, job); err != nil {
		return nil, fmt.Errorf("failed to save job to database: %w", err)
	}
	//enqueue it
	if err := j.queue.Enqueue(ctx, job); err != nil {
		j.job.UpdateError(ctx, job.ID, err.Error(), models.JobStatusFailed)
		return nil, fmt.Errorf("failed to enqueue job: %w", err)
	}
	j.logger.Info("Job created and enqueued",
		zap.String("job_id", job.ID.String()),
		zap.String("job_type", string(job.Type)),
		zap.String("status", string(job.Status)))
	return job, nil
}

func (j *JobService) GetJob(ctx context.Context, id uuid.UUID) (*models.Job, error) {
	job, err := j.job.GetJobById(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}
	return job, nil
}

func (j *JobService) ListJob(ctx context.Context, filter *jobs.JobFilter) (*models.JobListResponse, error) {
	jobs, total, err := j.job.List(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}
	jobResponse := make([]models.JobResponse, len(jobs))
	for i, job := range jobs {
		jobResponse[i] = job.ToResponse()
	}
	page := 1
	limit := filter.Limit
	if limit == 0 {
		limit = 22 //default value
	}
	if filter.Offset > 0 {
		page = (filter.Offset / limit) + 1
	}
	pages := int(total) / limit
	if int(total)%limit > 0 {
		pages++
	}
	return &models.JobListResponse{
		Jobs:  jobResponse,
		Total: total,
		Page:  page,
		Limit: limit,
		Pages: pages,
	}, nil
}
func (j *JobService) CancelJob(ctx context.Context, id uuid.UUID) error {
	job, err := j.job.GetJobById(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}
	if job.Status != models.JobStatusPending {
		return fmt.Errorf("job cannot be cancelled in status: %s", job.Status)
	}
	if err := j.job.UpdateStaus(ctx, id, models.JobStatusCancelled); err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}
	j.logger.Info("Job cancelled",
		zap.String("job_id", id.String()),
	)
	return nil
}

func (j *JobService) createJobFromRequest(req *models.JobCreateRequest) (*models.Job, error) {
	job := &models.Job{
		ID:         uuid.New(),
		Type:       req.Type,
		Status:     models.JobStatusPending,
		Payload:    req.Payload,
		Priority:   models.PriorityNormal,
		MaxRetries: 3,
		RetryDelay: 5 * time.Minute,
		Timeout:    30 * time.Minute,
		QueueName:  "default",
	}

	// Overriding defaults with request values
	if req.Priority != nil {
		job.Priority = *req.Priority
	}
	if req.MaxRetries != nil {
		job.MaxRetries = *req.MaxRetries
	}
	if req.RetryDelay != nil {
		if duration, err := time.ParseDuration(*req.RetryDelay); err == nil {
			job.RetryDelay = duration
		}
	}
	if req.Timeout != nil {
		if duration, err := time.ParseDuration(*req.Timeout); err == nil {
			job.Timeout = duration
		}
	}
	if req.QueueName != "" {
		job.QueueName = req.QueueName
	}
	if req.ScheduledAt != nil {
		job.ScheduledAt = req.ScheduledAt
	}
	if req.CreatedBy != "" {
		job.CreatedBy = req.CreatedBy
	}
	if len(req.Tags) > 0 {
		job.Tags = models.StringArray(req.Tags)
	}

	return job, nil
}
