package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Redis    RedisConfig
	Worker   WorkerConfig
	Logger   LoggerConfig
}

type ServerConfig struct {
	Host         string
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

type DatabaseConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	DBName          string
	SSLMode         string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type RedisConfig struct {
	Host        string
	Port        int
	Password    string
	DB          int
	PoolSize    int
	MinIdle     int
	MaxRetries  int
	DialTimeout time.Duration
}

type WorkerConfig struct {
	Concurrency     int
	MaxRetries      int
	RetryDelay      time.Duration
	QueueName       string
	PollInterval    time.Duration
	ShutdownTimeout time.Duration
}

type LoggerConfig struct {
	Level  string
	Format string
}

func Load() (*Config, error) {
	cfg := &Config{
		Server: ServerConfig{
			Host:         getEnv("SERVER_HOST", "localhost"),
			Port:         getEnvInt("SERVER_PORT", 8080),
			ReadTimeout:  getEnvDuration("SERVER_READ_TIMEOUT", 15*time.Second),
			WriteTimeout: getEnvDuration("SERVER_WRITE_TIMEOUT", 15*time.Second),
			IdleTimeout:  getEnvDuration("SERVER_IDLE_TIMEOUT", 60*time.Second),
		},
		Database: DatabaseConfig{
			Host:            getEnv("DB_HOST", "localhost"),
			Port:            getEnvInt("DB_PORT", 5432),
			User:            getEnv("DB_USER", "postgres"),
			Password:        getEnv("DB_PASSWORD", "postgres"),
			DBName:          getEnv("DB_NAME", "job_scheduler"),
			SSLMode:         getEnv("DB_SSL_MODE", "disable"),
			MaxOpenConns:    getEnvInt("DB_MAX_OPEN_CONNS", 25),
			MaxIdleConns:    getEnvInt("DB_MAX_IDLE_CONNS", 5),
			ConnMaxLifetime: getEnvDuration("DB_CONN_MAX_LIFETIME", 5*time.Minute),
		},
		Redis: RedisConfig{
			Host:        getEnv("REDIS_HOST", "localhost"),
			Port:        getEnvInt("REDIS_PORT", 6379),
			Password:    getEnv("REDIS_PASSWORD", ""),
			DB:          getEnvInt("REDIS_DB", 0),
			PoolSize:    getEnvInt("REDIS_POOL_SIZE", 10),
			MinIdle:     getEnvInt("REDIS_MIN_IDLE", 2),
			MaxRetries:  getEnvInt("REDIS_MAX_RETRIES", 3),
			DialTimeout: getEnvDuration("REDIS_DIAL_TIMEOUT", 5*time.Second),
		},
		Worker: WorkerConfig{
			Concurrency:     getEnvInt("WORKER_CONCURRENCY", 10),
			MaxRetries:      getEnvInt("WORKER_MAX_RETRIES", 3),
			RetryDelay:      getEnvDuration("WORKER_RETRY_DELAY", 5*time.Second),
			QueueName:       getEnv("WORKER_QUEUE_NAME", "jobs"),
			PollInterval:    getEnvDuration("WORKER_POLL_INTERVAL", 1*time.Second),
			ShutdownTimeout: getEnvDuration("WORKER_SHUTDOWN_TIMEOUT", 30*time.Second),
		},
		Logger: LoggerConfig{
			Level:  getEnv("LOG_LEVEL", "info"),
			Format: getEnv("LOG_FORMAT", "json"),
		},
	}
	return cfg, nil
}
func (c *DatabaseConfig) DatabaseConn() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", c.Host, c.Port, c.User, c.Password, c.DBName, c.SSLMode)
}

func (c *RedisConfig) RedisAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func getEnv(key, defaultvalue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultvalue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
