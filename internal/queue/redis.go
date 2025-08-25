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

type QueueStats struct {
	Pending    int64  `json:"pending"`
	Delayed    int64  `json:"delayed"`
	Processing int64  `json:"processing"`
	QueueName  string `json:"queue_name"`
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

func (q *RedisQueue) Dqueue(ctx context.Context, timeout time.Duration) (*QueueMessage, error) {
	queueKey := q.getQueueKey()
	result, err := q.client.BLPop(ctx, timeout, queueKey).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to dequeue job: %w", err)
	}
	if len(result) < 2 {
		return nil, fmt.Errorf("invalid dequeue result")
	}
	var message QueueMessage
	if err := json.Unmarshal([]byte(result[1]), &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job message: %w", err)
	}
	return &message, nil
}

func (q *RedisQueue) EnqueueDelayed(ctx context.Context, job *models.Job, delay time.Duration) error {
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
		return fmt.Errorf("failed to marshal delayed job message: %w", err)
	}
	delayedKey := q.getDelayedQueueKey()
	score := float64(time.Now().Add(delay).Unix())
	return q.client.ZAdd(ctx, delayedKey, &redis.Z{
		Score:  score,
		Member: data,
	}).Err()
}

func (q *RedisQueue) ProcessDelayedJobs(ctx context.Context) error {
	delayedKey := q.getDelayedQueueKey()
	queueKey := q.getQueueKey()
	now := float64(time.Now().Unix())

	jobs, err := q.client.ZRangeByScore(ctx, delayedKey, &redis.ZRangeBy{
		Min: "0",
		Max: fmt.Sprintf("%f", now),
	}).Result()

	if err != nil {
		return fmt.Errorf("failed to get delayed jobs: %w", err)
	}

	if len(jobs) == 0 {
		return nil
	}

	pipe := q.client.TxPipeline()

	for _, jobData := range jobs {
		pipe.RPush(ctx, queueKey, jobData)
		pipe.ZRem(ctx, delayedKey, jobData)
	}

	_, err = pipe.Exec(ctx)
	return err
}

func (q *RedisQueue) EnqueueRetry(ctx context.Context, message *QueueMessage, delay time.Duration) error {
	message.Retry++
	message.CreatedAt = time.Now().UTC()

	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal retry job message: %w", err)
	}

	if delay > 0 {
		delayedKey := q.getDelayedQueueKey()
		score := float64(time.Now().Add(delay).Unix())

		return q.client.ZAdd(ctx, delayedKey, &redis.Z{
			Score:  score,
			Member: data,
		}).Err()
	} else {
		queueKey := q.getQueueKey()
		return q.client.RPush(ctx, queueKey, data).Err()
	}
}

func (q *RedisQueue) GetQueueStats(ctx context.Context) (*QueueStats, error) {
	pipe := q.client.Pipeline()

	queueKey := q.getQueueKey()
	delayedKey := q.getDelayedQueueKey()
	processingKey := q.getProcessingQueueKey()

	queueLen := pipe.LLen(ctx, queueKey)
	delayedLen := pipe.ZCard(ctx, delayedKey)
	processingLen := pipe.LLen(ctx, processingKey)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue stats: %w", err)
	}

	return &QueueStats{
		Pending:    queueLen.Val(),
		Delayed:    delayedLen.Val(),
		Processing: processingLen.Val(),
		QueueName:  q.queue,
	}, nil
}

func (q *RedisQueue) MarkProcessing(ctx context.Context, message *QueueMessage) error {
	processingKey := q.getProcessingQueueKey()

	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal processing job: %w", err)
	}

	return q.client.LPush(ctx, processingKey, data).Err()
}

func (q *RedisQueue) RemoveProcessing(ctx context.Context, jobID uuid.UUID) error {
	processingKey := q.getProcessingQueueKey()
	items, err := q.client.LRange(ctx, processingKey, 0, -1).Result()
	if err != nil {
		return err
	}
	for _, item := range items {
		var msg QueueMessage
		if err := json.Unmarshal([]byte(item), &msg); err != nil {
			continue
		}

		if msg.JobID == jobID {
			return q.client.LRem(ctx, processingKey, 1, item).Err()
		}
	}

	return nil
}

func (q *RedisQueue) PurgeQueue(ctx context.Context) error {
	keys := []string{
		q.getQueueKey(),
		q.getDelayedQueueKey(),
		q.getProcessingQueueKey(),
	}

	return q.client.Del(ctx, keys...).Err()
}

func (q *RedisQueue) Close() error {
	return q.client.Close()
}

func (q *RedisQueue) Health(ctx context.Context) map[string]interface{} {
	info := map[string]interface{}{
		"queue_name": q.queue,
		"status":     "healthy",
	}
	if err := q.client.Ping(ctx).Err(); err != nil {
		info["status"] = "unhealthy"
		info["error"] = err.Error()
		return info
	}
	if stats, err := q.GetQueueStats(ctx); err == nil {
		info["pending_jobs"] = stats.Pending
		info["delayed_jobs"] = stats.Delayed
		info["processing_jobs"] = stats.Processing
	}
	if redisInfo, err := q.client.Info(ctx, "memory").Result(); err == nil {
		info["redis_memory_info"] = redisInfo
	}

	return info
}

func (q *RedisQueue) getDelayedQueueKey() string {
	return fmt.Sprintf("queue:%s:delayed", q.queue)
}

func (q *RedisQueue) getProcessingQueueKey() string {
	return fmt.Sprintf("queue:%s:processing", q.queue)
}
