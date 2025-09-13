package worker

import (
	"github.com/7h3-3mp7y-m4n/atlas/internal/models"
	"github.com/7h3-3mp7y-m4n/atlas/pkg/logger"
)

type Executor struct {
	logger   *logger.Logger
	handlers map[models.JobType]models.Job
}

func NewExecutor(logger *logger.Logger) *Executor {
	executor := &Executor{
		logger:   logger,
		handlers: make(map[models.JobType]models.Job),
	}
	return executor
}
