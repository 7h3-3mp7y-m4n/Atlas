package jobs

import (
	"context"
	"fmt"
	"time"

	"github.com/7h3-3mp7y-m4n/atlas/internal/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Job struct {
	db *gorm.DB
}

type JobFilter struct {
	Status    []models.JobStatus `json:"status,omitempty"`
	Type      []models.JobType   `json:"type,omitempty"`
	Priority  int                `json:"priority,omitempty"`
	QueueName string             `json:"queue_name,omitempty"`
	CreatedBy string             `json:"created_by,omitempty"`
	Tags      []string           `json:"tags,omitempty"`

	CreatedAfter  *time.Time `json:"created_after,omitempty"`
	CreatedBefore *time.Time `json:"created_before,omitempty"`

	// Pagination , not sure about it yet!!!
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`

	// Sorting
	SortBy    string `json:"sort_by,omitempty"`    // priority
	SortOrder string `json:"sort_order,omitempty"` // asc, desc

}

func NewJob(db *gorm.DB) *Job {
	return &Job{db: db}
}

func (j *Job) Create(ctx context.Context, job *models.Job) error {
	if err := j.db.WithContext(ctx).Create(job).Error; err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}
	return nil
}

func (j *Job) GetJobById(ctx context.Context, id uuid.UUID) (*models.Job, error) {
	var job models.Job
	if err := j.db.WithContext(ctx).First(&job, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("job not found: %w", err)
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}
	return &job, nil
}

func (j *Job) UpdateJob(ctx context.Context, job *models.Job) error {
	if err := j.db.WithContext(ctx).Save(&job).Error; err != nil {
		return fmt.Errorf("failed to update job: %w", err)
	}
	return nil
}

func (j *Job) UpdateStaus(ctx context.Context, id uuid.UUID, status models.JobStatus) error {
	updates := map[string]interface{}{
		"status":     status,
		"updated_at": time.Now().UTC(),
	}
	switch status {
	case models.JobStatusRunning:
		updates["started_at"] = time.Now().UTC()
	case models.JobStatusCompleted, models.JobStatusFailed, models.JobStatusCancelled:
		updates["completed_at"] = time.Now().UTC()
	}
	result := j.db.WithContext(ctx).Model(&models.Job{}).Where("id = ?", id).Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("failed to update job status: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("job not found")
	}
	return nil
}

func (j *Job) UpdateResult(ctx context.Context, id uuid.UUID, result []byte, status models.JobStatus) error {
	updates := map[string]interface{}{
		"result":       result,
		"status":       status,
		"updated_at":   time.Now().UTC(),
		"completed_at": time.Now().UTC(),
	}
	dbResult := j.db.WithContext(ctx).Model(&models.Job{}).Where("id = ?", id).Updates(updates)
	if dbResult != nil {
		return fmt.Errorf("failed to update job result: %w", dbResult.Error)
	}
	if dbResult.RowsAffected == 0 {
		return fmt.Errorf("job not found")
	}
	return nil
}

func (j *Job) UpdateError(ctx context.Context, id uuid.UUID, errormsg string, status models.JobStatus) error {
	updates := map[string]interface{}{
		"error":      errormsg,
		"status":     status,
		"updated_at": time.Now().UTC(),
	}
	if status == models.JobStatusFailed {
		updates["completed_at"] = time.Now().UTC()
	}
	result := j.db.WithContext(ctx).Model(&models.Job{}).Where("id = ?", id).Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("failed to update job error: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("job not found")
	}
	return nil
}

func (j *Job) RetryIncerment(ctx context.Context, id uuid.UUID) error {
	result := j.db.WithContext(ctx).Model(&models.Job{}).Where("id = ?", id).Updates(map[string]interface{}{
		"retry_count": gorm.Expr("retry_count + 1"),
		"updated_at":  time.Now().UTC(),
	})
	if result.Error != nil {
		return fmt.Errorf("failed to increment retry count: %w", result.Error)
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("job not found")
	}
	return nil
}

func (j *Job) List(ctx context.Context, filter *JobFilter) ([]*models.Job, int64, error) {
	query := j.db.WithContext(ctx).Model(&models.Job{})
	query = j.applyFilters(query, filter)
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to count jobs: %w", err)
	}
	if filter.SortBy != "" {
		order := "DESC"
		if filter.SortOrder == "asc" {
			order = "ASC"
		}
		query = query.Order(fmt.Sprintf("%s %s", filter.SortBy, order))
	} else {
		query = query.Order("created_at DESC")
	}
	if filter.Limit > 0 {
		query = query.Limit(filter.Limit)
	}
	if filter.Offset > 0 {
		query = query.Offset(filter.Offset)
	}

	var jobs []*models.Job
	if err := query.Find(&jobs).Error; err != nil {
		return nil, 0, fmt.Errorf("failed to list jobs: %w", err)
	}
	return jobs, total, nil
}

func (j *Job) applyFilters(query *gorm.DB, filter *JobFilter) *gorm.DB {
	if filter == nil {
		return query
	}

	if len(filter.Status) > 0 {
		query = query.Where("status IN ?", filter.Status)
	}

	if len(filter.Type) > 0 {
		query = query.Where("type IN ?", filter.Type)
	}

	if filter.Priority > 0 {
		query = query.Where("priority = ?", filter.Priority)
	}

	if filter.QueueName != "" {
		query = query.Where("queue_name = ?", filter.QueueName)
	}

	if filter.CreatedBy != "" {
		query = query.Where("created_by = ?", filter.CreatedBy)
	}

	if len(filter.Tags) > 0 {
		query = query.Where("tags && ?", filter.Tags)
	}

	if filter.CreatedAfter != nil {
		query = query.Where("created_at >= ?", *filter.CreatedAfter)
	}

	if filter.CreatedBefore != nil {
		query = query.Where("created_at <= ?", *filter.CreatedBefore)
	}
	return query
}

func (j *Job) GetJobToRetry(ctx context.Context, limit int) ([]*models.Job, error) {
	var jobs []*models.Job
	err := j.db.WithContext(ctx).Where("status = ? AND retry_count < max_retries", models.JobStatusFailed).Limit(limit).Find(&jobs).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get jobs to retry: %w", err)
	}
	return jobs, nil
}

func (j *Job) GetScheduleJob(ctx context.Context, before time.Time) ([]*models.Job, error) {
	var jobs []*models.Job
	err := j.db.WithContext(ctx).
		Where("status = ? AND scheduled_at IS NOT NULL AND scheduled_at <= ?", models.JobStatusPending, before).Find(&jobs).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get scheduled jobs: %w", err)
	}
	return jobs, nil
}

func (j *Job) GetStaleJob(ctx context.Context, timeout time.Duration) ([]*models.Job, error) {
	var jobs []*models.Job
	err := j.db.WithContext(ctx).Where("status = ? AND started_at IS NOT NULL AND started_at < ?", models.JobStatusRunning, time.Now().UTC().Add(-timeout)).Find(&jobs).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get stale jobs: %w", err)
	}
	return jobs, nil
}

func (j *Job) GetStats(ctx context.Context) (*models.JobStats, error) {
	stats := &models.JobStats{}
	if err := j.db.WithContext(ctx).Model(&models.Job{}).Count(&stats.Total).Error; err != nil {
		return nil, fmt.Errorf("failed to count total jobs: %w", err)
	}
	statusCounts := make(map[models.JobStatus]int64)
	rows, err := j.db.WithContext(ctx).Model(&models.Job{}).Select("status, COUNT(*) as count").Group("status").Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to get status counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status models.JobStatus
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			continue
		}
		statusCounts[status] = count
	}
	stats.Pending = statusCounts[models.JobStatusPending]
	stats.Running = statusCounts[models.JobStatusRunning]
	stats.Completed = statusCounts[models.JobStatusCompleted]
	stats.Failed = statusCounts[models.JobStatusFailed]
	stats.Cancelled = statusCounts[models.JobStatusCancelled]
	stats.Retrying = statusCounts[models.JobStatusRetry]
	return stats, nil
}
