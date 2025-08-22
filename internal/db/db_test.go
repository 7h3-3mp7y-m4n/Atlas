package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/7h3-3mp7y-m4n/atlas/internal/config"
	"github.com/7h3-3mp7y-m4n/atlas/internal/db"
)

func TestDBConnectionAndMigrate(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host: "localhost", Port: 5433,
		User: "test", Password: "test", DBName: "atlas_test",
		SSLMode:      "disable",
		MaxOpenConns: 5, MaxIdleConns: 2, ConnMaxLifetime: time.Minute,
	}

	database, err := db.New(cfg)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer database.Close()

	if err := database.Migrate(); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	ctx := context.Background()
	if err := database.Ping(ctx); err != nil {
		t.Fatalf("ping failed: %v", err)
	}

	health := database.Health(ctx)
	if health["status"] != "healthy" {
		t.Errorf("expected healthy db, got: %+v", health)
	}
}
