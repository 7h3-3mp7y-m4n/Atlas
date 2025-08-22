package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type JobStatus string
type JobType string
type StringArray []string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "complete"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCancelled JobStatus = "cancelled"
	JobStatusRetry     JobStatus = "retryinh"
)

const (
	JobTypeEmail     JobType = "email"
	JobTypeReport    JobType = "report"
	JobTypeWebScrape JobType = "web_scrape"
	JobTypePayment   JobType = "payment"
	JobTypeMLInfer   JobType = "ml_inference"
	JobTypeCustom    JobType = "custom"
)

const (
	PriorityLow      int = 1
	PriorityNormal   int = 5
	PriorityHigh     int = 10
	PriorityCritical int = 15
)

type Job struct {
	ID       uuid.UUID       `json:"id" gorm:"type:uuid;primary_key;default:gen_random_uuid()"`
	Type     JobType         `json:"type" gorm:"not null;index"`
	Status   JobStatus       `json:"status" gorm:"not null;default:'pending';index"`
	Priority int             `json:"priority" gorm:"not null;default:5;index"`
	Payload  json.RawMessage `json:"payload" gorm:"type:jsonb"`
	Result   json.RawMessage `json:"result,omitempty" gorm:"type:jsonb"`
	Error    string          `json:"error,omitempty"`

	MaxRetries int           `json:"max_retries" gorm:"not null;default:3"`
	RetryCount int           `json:"retry_count" gorm:"not null;default:0"`
	RetryDelay time.Duration `json:"retry_delay" gorm:"not null;default:300000000000"` // 5 minutes in nanoseconds

	ScheduledAt *time.Time `json:"scheduled_at,omitempty" gorm:"index"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`

	CreatedBy string        `json:"created_by,omitempty"`
	Tags      StringArray   `json:"tags,omitempty" gorm:"type:text[]"`
	Timeout   time.Duration `json:"timeout" gorm:"default:1800000000000"` // 30 minutes in nanoseconds

	WorkerID  string `json:"worker_id,omitempty"`
	QueueName string `json:"queue_name" gorm:"not null;default:'default'"`

	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `json:"deleted_at,omitempty" gorm:"index"`
}

type JobCreateRequest struct {
	Type        JobType         `json:"type" binding:"required"`
	Payload     json.RawMessage `json:"payload" binding:"required"`
	Priority    *int            `json:"priority,omitempty"`
	MaxRetries  *int            `json:"max_retries,omitempty"`
	RetryDelay  *string         `json:"retry_delay,omitempty"` // Duration string like "5m"
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
	CreatedBy   string          `json:"created_by,omitempty"`
	Tags        []string        `json:"tags,omitempty"`
	Timeout     *string         `json:"timeout,omitempty"` // Duration string like "30m"
	QueueName   string          `json:"queue_name,omitempty"`
}
type JobResponse struct {
	ID          uuid.UUID       `json:"id"`
	Type        JobType         `json:"type"`
	Status      JobStatus       `json:"status"`
	Priority    int             `json:"priority"`
	Payload     json.RawMessage `json:"payload"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
	MaxRetries  int             `json:"max_retries"`
	RetryCount  int             `json:"retry_count"`
	RetryDelay  string          `json:"retry_delay"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	CreatedBy   string          `json:"created_by,omitempty"`
	Tags        []string        `json:"tags,omitempty"`
	Timeout     string          `json:"timeout"`
	WorkerID    string          `json:"worker_id,omitempty"`
	QueueName   string          `json:"queue_name"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}
type JobListResponse struct {
	Jobs  []JobResponse `json:"jobs"`
	Total int64         `json:"total"`
	Page  int           `json:"page"`
	Limit int           `json:"limit"`
	Pages int           `json:"pages"`
}
type JobStats struct {
	Total     int64 `json:"total"`
	Pending   int64 `json:"pending"`
	Running   int64 `json:"running"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Cancelled int64 `json:"cancelled"`
	Retrying  int64 `json:"retrying"`
}
type ScheduledJob struct {
	ID          uuid.UUID       `json:"id" gorm:"type:uuid;primary_key;default:gen_random_uuid()"`
	Name        string          `json:"name" gorm:"not null;unique"`
	Description string          `json:"description"`
	CronExpr    string          `json:"cron_expression" gorm:"not null"`
	JobType     JobType         `json:"job_type" gorm:"not null"`
	Payload     json.RawMessage `json:"payload" gorm:"type:jsonb"`
	Priority    int             `json:"priority" gorm:"not null;default:5"`
	MaxRetries  int             `json:"max_retries" gorm:"not null;default:3"`
	Timeout     time.Duration   `json:"timeout" gorm:"default:1800000000000"`
	QueueName   string          `json:"queue_name" gorm:"not null;default:'default'"`
	Tags        StringArray     `json:"tags,omitempty" gorm:"type:text[]"`
	Enabled     bool            `json:"enabled" gorm:"not null;default:true"`
	LastRun     *time.Time      `json:"last_run,omitempty"`
	NextRun     *time.Time      `json:"next_run,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	DeletedAt   gorm.DeletedAt  `json:"deleted_at,omitempty" gorm:"index"`
}

func (s *StringArray) Scan(value interface{}) error {
	if value == nil {
		*s = []string{}
		return nil
	}
	switch v := value.(type) {
	case string:
		return json.Unmarshal([]byte(v), s)
	case []byte:
		return json.Unmarshal(v, s)
	}
	return nil
}

func (j *Job) ToResponse() JobResponse {
	return JobResponse{
		ID:          j.ID,
		Type:        j.Type,
		Status:      j.Status,
		Priority:    j.Priority,
		Payload:     j.Payload,
		Result:      j.Result,
		Error:       j.Error,
		MaxRetries:  j.MaxRetries,
		RetryCount:  j.RetryCount,
		RetryDelay:  j.RetryDelay.String(),
		ScheduledAt: j.ScheduledAt,
		StartedAt:   j.StartedAt,
		CompletedAt: j.CompletedAt,
		CreatedBy:   j.CreatedBy,
		Tags:        []string(j.Tags),
		Timeout:     j.Timeout.String(),
		WorkerID:    j.WorkerID,
		QueueName:   j.QueueName,
		CreatedAt:   j.CreatedAt,
		UpdatedAt:   j.UpdatedAt,
	}
}

func (j *Job) IsTerminalState() bool {
	return j.Status == JobStatusCompleted ||
		j.Status == JobStatusFailed ||
		j.Status == JobStatusCancelled
}

func (j *Job) CanRetry() bool {
	return j.Status == JobStatusFailed && j.RetryCount < j.MaxRetries
}
