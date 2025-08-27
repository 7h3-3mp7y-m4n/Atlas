package service

import (
	"context"
	"encoding/json"
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

func (j *JobService) RetryJob(ctx context.Context, id uuid.UUID) error {
	job, err := j.job.GetJobById(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}
	if !job.CanRetry() {
		return fmt.Errorf("job cannot be retried: status=%s, retries=%d/%d",
			job.Status, job.RetryCount, job.MaxRetries)
	}
	//Reset the job for retry
	job.Status = models.JobStatusPending
	job.Error = ""
	job.StartedAt = nil
	job.CompletedAt = nil
	if err := j.job.UpdateJob(ctx, job); err != nil {
		return fmt.Errorf("failed to update job for retry: %w", err)
	}
	if err := j.queue.Enqueue(ctx, job); err != nil {
		return fmt.Errorf("failed to enqueue job for retry: %w", err)
	}
	j.logger.Info("Job queued for retry",
		zap.String("job_id", id.String()),
		zap.Int("retry_count", job.RetryCount),
	)
	return nil
}

// mark that shit ;)
func (j *JobService) ProcessJob(ctx context.Context, message *queue.QueueMessage) error {
	jobId := message.JobID
	logger := j.logger.WithJobID(jobId.String())
	if err := j.job.UpdateStaus(ctx, jobId, models.JobStatusRunning); err != nil {
		logger.Error("Failed to mark job as running", zap.Error(err))
		return err
	}
	if err := j.queue.MarkProcessing(ctx, message); err != nil {
		logger.Warn("Failed to mark job in processing queue", zap.Error(err))
	}
	logger.Info("Job processing started",
		zap.String("job_type", string(message.Type)),
		zap.Int("retry_count", message.Retry),
	)
	return nil
}

func (j *JobService) CompleteJob(ctx context.Context, jobID uuid.UUID, result interface{}) error {
	logger := j.logger.WithJobID(jobID.String())
	// Serialize result
	var resultBytes []byte
	if result != nil {
		var err error
		resultBytes, err = json.Marshal(result)
		if err != nil {
			return fmt.Errorf("failed to marshal job result: %w", err)
		}
	}
	// Update job with result
	if err := j.job.UpdateResult(ctx, jobID, resultBytes, models.JobStatusCompleted); err != nil {
		logger.Error("Failed to update job result", zap.Error(err))
		return err
	}
	if err := j.queue.RemoveProcessing(ctx, jobID); err != nil {
		logger.Warn("Failed to remove job from processing queue", zap.Error(err))
	}
	logger.Info("Job completed successfully")
	return nil
}

func (j *JobService) FailJob(ctx context.Context, jobID uuid.UUID, jobError error) error {
	logger := j.logger.WithJobID(jobID.String())

	job, err := j.job.GetJobById(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to get job for failure: %w", err)
	}

	// Remove from processing queue
	if err := j.queue.RemoveProcessing(ctx, jobID); err != nil {
		logger.Warn("Failed to remove job from processing queue", zap.Error(err))
	}
	if job.CanRetry() {
		if err := j.job.RetryIncerment(ctx, jobID); err != nil {
			logger.Error("Failed to increment retry count", zap.Error(err))
			return err
		}
		backoffDelay := j.calculateBackoffDelay(job.RetryCount+1, job.RetryDelay)
		// Create queue message for retry
		message := &queue.QueueMessage{
			JobID:     job.ID,
			Type:      job.Type,
			Payload:   job.Payload,
			Priority:  job.Priority,
			Retry:     job.RetryCount + 1,
			MaxRetry:  job.MaxRetries,
			CreatedAt: time.Now().UTC(),
		}

		// Enqueue for retry with delay
		if err := j.queue.EnqueueRetry(ctx, message, backoffDelay); err != nil {
			logger.Error("Failed to enqueue job for retry", zap.Error(err))
			j.job.UpdateError(ctx, jobID, jobError.Error(), models.JobStatusFailed)
			return err
		}
		// Update status to retrying
		if err := j.job.UpdateError(ctx, jobID, jobError.Error(), models.JobStatusRetry); err != nil {
			logger.Error("Failed to update job status to retrying", zap.Error(err))
			return err
		}

		logger.Info("Job queued for retry",
			zap.Int("retry_count", job.RetryCount+1),
			zap.Int("max_retries", job.MaxRetries),
			zap.Duration("backoff_delay", backoffDelay),
		)
	} else {
		// No more retries, mark as permanently failed kill that shit
		if err := j.job.UpdateError(ctx, jobID, jobError.Error(), models.JobStatusFailed); err != nil {
			logger.Error("Failed to mark job as failed", zap.Error(err))
			return err
		}
		logger.Error("Job failed permanently",
			zap.Error(jobError),
			zap.Int("retry_count", job.RetryCount),
			zap.Int("max_retries", job.MaxRetries),
		)
	}
	return nil
}

func (j *JobService) GetJobStats(ctx context.Context) (*models.JobStats, error) {
	return j.job.GetStats(ctx)
}

// GetqueueStats retrieves queue statistics
func (j *JobService) GetQueueStats(ctx context.Context) (*queue.QueueStats, error) {
	return j.queue.GetQueueStats(ctx)
}

// ProcessdelayedJobs processes delayed jobs (called by scheduler)
func (j *JobService) ProcessDelayedJobs(ctx context.Context) error {
	return j.queue.ProcessDelayedJobs(ctx)
}

func (j *JobService) enqueueJob(ctx context.Context, job *models.Job) error {
	if job.ScheduledAt != nil && job.ScheduledAt.After(time.Now()) {
		delay := time.Until(*job.ScheduledAt)
		return j.queue.EnqueueDelayed(ctx, job, delay)
	} else {
		// Immediate job
		return j.queue.Enqueue(ctx, job)
	}
}

// calculateBackoffDelay calculates exponential backoff delay
func (j *JobService) calculateBackoffDelay(retryCount int, baseDelay time.Duration) time.Duration {
	// baseDelay * 2^(retryCount-1)
	multiplier := 1
	for i := 1; i < retryCount; i++ {
		multiplier *= 2
	}
	delay := time.Duration(multiplier) * baseDelay

	// Cap at maximum delay of 1 hour
	maxDelay := 1 * time.Hour
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}

func (j *JobService) CleanupOldJobs(ctx context.Context, olderThan time.Time) (int64, error) {
	statuses := []models.JobStatus{
		models.JobStatusCompleted,
		models.JobStatusFailed,
		models.JobStatusCancelled,
	}
	count, err := j.job.CleanUp(ctx, olderThan, statuses)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup old jobs: %w", err)
	}
	j.logger.Info("Old jobs cleaned up",
		zap.Int64("count", count),
		zap.Time("older_than", olderThan),
	)
	return count, nil
}
