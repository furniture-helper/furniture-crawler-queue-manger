package database

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultPort = "5432"
const defaultHost = "localhost"
const defaultDatabase = "furniture_crawler"
const defaultUser = "furniture_crawler"
const defaultPassword = "Test@123"
const defaultSSLMode = "require"

func Connect(ctx context.Context) (*pgxpool.Pool, error) {
	dbHost := os.Getenv("PG_HOST")
	if dbHost == "" {
		dbHost = defaultHost
	}

	dbPort := os.Getenv("PG_PORT")
	if dbPort == "" {
		dbPort = defaultPort
	}

	dbUser := os.Getenv("PG_USER")
	if dbUser == "" {
		dbUser = defaultUser
	}

	dbPassword := os.Getenv("PG_PASSWORD")
	if dbPassword == "" {
		dbPassword = defaultPassword
	}

	dbName := os.Getenv("PG_DATABASE")
	if dbName == "" {
		dbName = defaultDatabase
	}

	sslMode := os.Getenv("PG_SSL_MODE")
	if sslMode == "" {
		sslMode = defaultSSLMode
	}

	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", dbUser, dbPassword, dbHost, dbPort, dbName, sslMode)

	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	cfg.MaxConns = 10
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

func Close(pool *pgxpool.Pool) {
	if pool != nil {
		pool.Close()
	}
}
