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
	for _, worker := range p.worker {
		p.workerWg.Add(1)
		go worker.run(ctx)
	}
	return nil
}

func (p *Pool) Stop(ctx context.Context) error {
	p.mu.Lock()
	if !p.isRunning {
		p.mu.Unlock()
		return nil
	}
	p.isRunning = false
	p.mu.Unlock()
	p.logger.Info("Stopping worker pool gracefully!!!!")
	// Signal all workers to stop
	close(p.stopChan)
	for _, worker := range p.worker {
		close(worker.stopChan)
	}
	done := make(chan struct{})
	go func() {
		p.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("All workers stopped gracefully!!!!!!")
	case <-ctx.Done():
		p.logger.Warn("Worker pool shutdown timeout reached, forcing stop!!!!!")
	}
	close(p.stoppedChan)
	return nil
}

func (w *Worker) run(ctx context.Context) {
	defer w.pool.workerWg.Done()

	w.mu.Lock()
	w.isRunning = true
	w.mu.Unlock()

	w.logger.Info("Worker started", zap.String("worker_id", w.id))

	ticker := time.NewTicker(w.pool.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopChan:
			w.logger.Info("Worker received stop signal", zap.String("worker_id", w.id))
			w.shutdown()
			return

		case <-w.pool.stopChan:
			w.logger.Info("Worker pool received stop signal", zap.String("worker_id", w.id))
			w.shutdown()
			return

		case <-ctx.Done():
			w.logger.Info("Context cancelled", zap.String("worker_id", w.id))
			w.shutdown()
			return

		case <-ticker.C:
			if err := w.processJob(ctx); err != nil {
				// Only log if its not a "no jobs available" error
				if err.Error() != "no jobs available" {
					w.logger.Error("Error processing job",
						zap.String("worker_id", w.id),
						zap.Error(err),
					)
				}
			}
		}
	}
}

func (p *Pool) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isRunning
}

func (w *Worker) shutdown() {
	w.mu.Lock()
	w.isRunning = false
	w.mu.Unlock()

	w.logger.Info("Worker shutdown completed", zap.String("worker_id", w.id))
}

func (w *Worker) IsRunning() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.isRunning
}

func (w *Worker) processJob(ctx context.Context) error {
	// Dequeue a job
	return nil
}
