package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/7h3-3mp7y-m4n/atlas/internal/config"
	"github.com/7h3-3mp7y-m4n/atlas/internal/models"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type RedisQueue struct {
	client *redis.Client
	queue  string
}

type QueueMessage struct {
	JobID     uuid.UUID       `json:"job_id"`
	Type      models.JobType  `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Priority  int             `json:"priority"`
	Retry     int             `json:"retry"`
	MaxRetry  int             `json:"max_retry"`
	CreatedAt time.Time       `json:"created_at"`
}

func NewRedisQueue(cfg *config.RedisConfig, queue string) (*RedisQueue, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr(),
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdle,
		MaxRetries:   cfg.MaxRetries,
		DialTimeout:  cfg.DialTimeout,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	return &RedisQueue{
		client: rdb,
		queue:  queue,
	}, nil
}

func (q *RedisQueue) Enqueue(ctx context.Context, job *models.Job) error {
	message := QueueMessage{
		JobID:     job.ID,
		Type:      job.Type,
		Payload:   job.Payload,
		Priority:  job.Priority,
		Retry:     job.RetryCount,
		MaxRetry:  job.MaxRetries,
		CreatedAt: time.Now().UTC(),
	}
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal job message: %w", err)
	}
	// Using priority based queuing
	queueKey := q.getQueueKey()
	if job.Priority >= models.PriorityHigh {
		return q.client.LPush(ctx, queueKey, data).Err()
	} else {
		return q.client.RPush(ctx, queueKey, data).Err()
	}

}

func (q *RedisQueue) getQueueKey() string {
	return fmt.Sprintf("queue:%s", q.queue)
}
