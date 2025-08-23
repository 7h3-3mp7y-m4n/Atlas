package logger

import (
	"os"

	"github.com/7h3-3mp7y-m4n/atlas/internal/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	*zap.Logger
}

var globalLogger *Logger

func New(cfg *config.LoggerConfig) (*Logger, error) {
	var zapConfig zap.Config
	if cfg.Format == "console" {
		zapConfig = zap.NewDevelopmentConfig()
		zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		zapConfig = zap.NewProductionConfig()
	}
	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		level = zapcore.InfoLevel
	}
	zapConfig.Level = zap.NewAtomicLevelAt(level)
	zapConfig.Development = false
	zapConfig.DisableCaller = false
	zapConfig.DisableStacktrace = level != zapcore.DebugLevel
	logger, err := zapConfig.Build(
		zap.AddCallerSkip(1),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)
	if err != nil {
		return nil, err
	}

	return &Logger{logger}, nil
}

func NewDefault() *Logger {
	logger, _ := zap.NewDevelopment()
	return &Logger{Logger: logger}
}
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	zapFields := make([]zap.Field, 0, len(fields))
	for key, value := range fields {
		zapFields = append(zapFields, zap.Any(key, value))
	}
	return &Logger{l.Logger.With(zapFields...)}
}

func (l *Logger) WithJobID(jobID string) *Logger {
	return &Logger{l.Logger.With(zap.String("job_id", jobID))}
}

func (l *Logger) WithWorkerID(workerID string) *Logger {
	return &Logger{l.Logger.With(zap.String("worker_id", workerID))}
}

func (l *Logger) WithRequestID(requestID string) *Logger {
	return &Logger{l.Logger.With(zap.String("request_id", requestID))}
}

func Init(cfg *config.LoggerConfig) error {
	logger, err := New(cfg)
	if err != nil {
		return err
	}
	globalLogger = logger
	return nil
}

func Get() *Logger {
	if globalLogger == nil {
		globalLogger = NewDefault()
	}
	return globalLogger
}

func Close() error {
	if globalLogger != nil {
		return globalLogger.Sync()
	}
	return nil
}

func Info(msg string, fields ...zap.Field) {
	Get().Info(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	Get().Error(msg, fields...)
}

func Debug(msg string, fields ...zap.Field) {
	Get().Debug(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	Get().Warn(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	Get().Fatal(msg, fields...)
	os.Exit(1)
}
