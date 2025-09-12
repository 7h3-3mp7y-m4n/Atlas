package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/7h3-3mp7y-m4n/atlas/internal/config"
	"github.com/7h3-3mp7y-m4n/atlas/internal/db"
	"github.com/7h3-3mp7y-m4n/atlas/internal/handlers"
	"github.com/7h3-3mp7y-m4n/atlas/internal/queue"
	"github.com/7h3-3mp7y-m4n/atlas/pkg/logger"

	"go.uber.org/zap"
)

var (
	migrate = flag.Bool("migrate", false, "Run database migrations and exit")
	version = flag.Bool("version", false, "Print version and exit")
)

const (
	appVersion = "1.0.0"
	appName    = "atlas-api"
)

func main() {
	flag.Parse()

	if *version {
		fmt.Printf("%s version %s\n", appName, appVersion)
		os.Exit(0)
	}
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	if err := logger.Init(&cfg.Logger); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Close()

	appLogger := logger.Get()
	appLogger.Info("Starting Atlas Job Scheduler API",
		zap.String("version", appVersion),
		zap.String("log_level", cfg.Logger.Level),
		zap.String("server_address", fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)),
	)
	database, err := db.New(&cfg.Database)
	if err != nil {
		appLogger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer database.Close()

	appLogger.Info("Database connection established",
		zap.String("host", cfg.Database.Host),
		zap.Int("port", cfg.Database.Port),
		zap.String("database", cfg.Database.DBName),
	)
	if *migrate {
		appLogger.Info("Running database migrations")
		if err := database.Migrate(); err != nil {
			appLogger.Fatal("Failed to run database migrations", zap.Error(err))
		}
		appLogger.Info("Database migrations completed successfully")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := database.Ping(ctx); err != nil {
		appLogger.Fatal("Database health check failed", zap.Error(err))
	}
	jobQueue, err := queue.NewRedisQueue(&cfg.Redis, "default")
	if err != nil {
		appLogger.Fatal("Failed to initialize Redis queue", zap.Error(err))
	}
	defer jobQueue.Close()

	appLogger.Info("Redis connection established",
		zap.String("address", cfg.Redis.RedisAddr()),
		zap.Int("database", cfg.Redis.DB),
	)
	if err := jobQueue.Health(ctx); err != nil {
		fmt.Println(err)
	}
	server := handlers.NewServer(cfg, database, jobQueue, appLogger)
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() {
		appLogger.Info("Starting HTTP server",
			zap.String("address", fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)),
		)

		if err := server.Start(); err != nil {
			appLogger.Error("HTTP server error", zap.Error(err))
		}
	}()
	<-ctx.Done()
	appLogger.Info("Shutdown signal received, starting graceful shutdown")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		appLogger.Error("Error during server shutdown", zap.Error(err))
	} else {
		appLogger.Info("Server shutdown completed")
	}
}
