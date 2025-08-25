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
