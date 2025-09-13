package worker

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/7h3-3mp7y-m4n/atlas/internal/db"
	"github.com/7h3-3mp7y-m4n/atlas/internal/queue"
	"github.com/7h3-3mp7y-m4n/atlas/pkg/logger"
	"go.uber.org/zap"
)

type Config struct {
	Concurrency     int
	MaxRetries      int
	RetryDelay      time.Duration
	PollInterval    time.Duration
	ShutdownTimeout time.Duration
	QueueName       string
}

type Worker struct {
	id        string
	pool      *Pool
	logger    *logger.Logger
	stopChan  chan struct{}
	isRunning bool
	mu        sync.RWMutex
}

type Pool struct {
	config      *Config
	db          *db.DB
	queue       *queue.RedisQueue
	logger      *logger.Logger
	executor    *Executor
	worker      []*Worker
	workerWg    sync.WaitGroup
	stopChan    chan struct{}
	stoppedChan chan struct{}
	isRunning   bool
	mu          sync.RWMutex
}

func NewPool(config *Config, database *db.DB, queue *queue.RedisQueue, logger *logger.Logger) *Pool {
	if config.Concurrency <= 0 {
		config.Concurrency = runtime.NumCPU()
	}
	if config.MaxRetries < 0 {
		config.MaxRetries = 3
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 5 * time.Second
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 1 * time.Second
	}
	if config.ShutdownTimeout <= 0 {
		config.ShutdownTimeout = 30 * time.Second
	}
	return &Pool{
		config:      config,
		db:          database,
		queue:       queue,
		logger:      logger,
		executor:    NewExecutor(logger),
		stopChan:    make(chan struct{}),
		stoppedChan: make(chan struct{}),
	}
}

func (p *Pool) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.isRunning {
		return fmt.Errorf("worker pool is already running")
	}
	p.logger.Info("Starting worker pool", zap.Int("concurrency", p.config.Concurrency), zap.String("queue", p.config.QueueName))
	p.worker = make([]*Worker, p.config.Concurrency)
	for i := 0; i < p.config.Concurrency; i++ {
		worker := &Worker{
			id:       fmt.Sprintf("worker-%d", i+1),
			pool:     p,
			logger:   p.logger.WithWorkerID(fmt.Sprintf("worker-%d", i+1)),
			stopChan: make(chan struct{}),
		}
		p.worker[i] = worker
	}
	return nil
}
