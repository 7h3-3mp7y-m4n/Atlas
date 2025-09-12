package handlers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/7h3-3mp7y-m4n/atlas/internal/config"
	"github.com/7h3-3mp7y-m4n/atlas/internal/db"
	"github.com/7h3-3mp7y-m4n/atlas/internal/queue"
	"github.com/7h3-3mp7y-m4n/atlas/pkg/logger"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Server struct {
	config     *config.Config
	db         *db.DB
	queue      queue.RedisQueue
	logger     *logger.Logger
	httpServer *http.Server
	router     *gin.Engine
}

func NewServer(cfg *config.Config, database *db.DB, q *queue.RedisQueue, log *logger.Logger) *Server {
	server := &Server{
		config: cfg,
		db:     database,
		queue:  *q,
		logger: log,
	}
	server.setupRouter()
	return server
}

func (s *Server) setupRouter() {
	// setting Gin mode based on environment
	if s.config.Logger.Level != "debug" {
		gin.SetMode(gin.ReleaseMode)
	}
	s.router = gin.New()
	//some middleware shits
	s.router.Use(s.requestIDMiddleware())
	s.router.Use(s.loggingMiddleware())
	s.router.Use(gin.Recovery())

	api := s.router.Group("/api/v1")
	{
		//Job management
		jobs := api.Group("/jobs")
		{
			jobs.POST("", s.createJobHandler)
			jobs.GET("", s.listJobsHandler)
			jobs.GET("/:id", s.getJobHandler)
			jobs.DELETE("/:id", s.cancelJobHandler)
			jobs.GET("/stats", s.getJobStatsHandler)
		}
	}

	//pichdi jati kei liye
	s.router.POST("/jobs", s.createJobHandler)
	s.router.GET("/jobs", s.listJobsHandler)
	s.router.GET("/jobs/:id", s.getJobHandler)
	s.router.DELETE("/jobs/:id", s.cancelJobHandler)
	s.router.GET("/jobs/stats", s.getJobStatsHandler)
}

func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.Server.Host, s.config.Server.Port)
	s.httpServer = &http.Server{
		Addr:        addr,
		Handler:     s.router,
		ReadTimeout: s.config.Server.ReadTimeout,
		IdleTimeout: s.config.Server.IdleTimeout,
	}
	s.logger.Info("Starting HTTP server",
		zap.String("address", addr),
		zap.Duration("read_timeout", s.config.Server.ReadTimeout),
		zap.Duration("write_timeout", s.config.Server.WriteTimeout))
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start server: %w", err)
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down HTTP server")
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

func (s *Server) requestIDMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		requestID := ctx.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}
		ctx.Header("X-Request-ID", requestID)
		ctx.Set("request_id", requestID)
		ctx.Next()
	}
}

func (s *Server) loggingMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		start := time.Now()
		path := ctx.Request.URL.Path
		raw := ctx.Request.URL.RawQuery
		ctx.Next()

		latency := time.Since(start)
		clientIP := ctx.ClientIP()
		method := ctx.Request.Method
		statusCode := ctx.Writer.Status()
		bodySize := ctx.Writer.Size()
		userAgent := ctx.Request.UserAgent()
		requestID := ctx.GetString("request_id")
		if raw != "" {
			path = path + "?" + raw
		}
		fields := []zap.Field{
			zap.String("request_id", requestID),
			zap.String("client_ip", clientIP),
			zap.String("method", method),
			zap.String("path", path),
			zap.Int("status", statusCode),
			zap.Int("body_size", bodySize),
			zap.Duration("latency", latency),
			zap.String("user_agent", userAgent),
		}
		if len(ctx.Errors) > 0 {
			fields = append(fields, zap.String("errors", ctx.Errors.String()))
		}
		switch {
		case statusCode >= 500:
			s.logger.Error("HTTP request", fields...)
		case statusCode >= 400:
			s.logger.Warn("HTTP request", fields...)
		default:
			s.logger.Info("HTTP request", fields...)
		}
	}
}

func (s *Server) corsMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		ctx.Header("Access-Control-Allow-Origin", "*")
		ctx.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		ctx.Header("Access-Control-Allow-Headers", "Origin, Authorization, Content-Type, X-Request-ID")
		ctx.Header("Access-Control-Expose-Headers", "X-Request-ID")
		if ctx.Request.Method == "OPTIONS" {
			ctx.AbortWithStatus(204)
			return
		}
		ctx.Next()
	}
}
func (s *Server) errorHandlingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				requestID := c.GetString("request_id")
				s.logger.Error("Panic recovered",
					zap.String("request_id", requestID),
					zap.Any("error", err),
					zap.String("path", c.Request.URL.Path),
				)
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": gin.H{
						"code":       "INTERNAL_ERROR",
						"message":    "Internal server error",
						"request_id": requestID,
					},
				})
				c.Abort()
			}
		}()
		c.Next()
	}
}
