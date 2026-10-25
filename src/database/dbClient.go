package database

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	rdsauth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultPort = "5432"
const defaultHost = "localhost"
const defaultDatabase = "furniture_crawler"
const defaultUser = "furniture_crawler"
const defaultSSLMode = "require"
const rdsIAMTokenTTL = 15 * time.Minute
const rdsIAMRefreshBeforeExpiry = 2 * time.Minute

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

	dbName := os.Getenv("PG_DATABASE")
	if dbName == "" {
		dbName = defaultDatabase
	}

	sslMode := os.Getenv("PG_SSL_MODE")
	if sslMode == "" {
		sslMode = defaultSSLMode
	}

	connString := fmt.Sprintf("postgres://%s@%s:%s/%s?sslmode=%s", dbUser, dbHost, dbPort, dbName, sslMode)

	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration for RDS IAM auth: %w", err)
	}

	tokenProvider := &iamTokenProvider{
		host:        dbHost,
		port:        dbPort,
		user:        dbUser,
		region:      awsCfg.Region,
		credentials: awsCfg.Credentials,
	}

	cfg.BeforeConnect = func(ctx context.Context, connCfg *pgx.ConnConfig) error {
		token, err := tokenProvider.GetToken(ctx)
		if err != nil {
			return err
		}
		connCfg.Password = token
		return nil
	}

	cfg.MaxConns = 10
	cfg.HealthCheckPeriod = 30 * time.Second
	cfg.MaxConnLifetime = 12 * time.Minute
	cfg.MaxConnLifetimeJitter = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
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

type iamTokenProvider struct {
	host        string
	port        string
	user        string
	region      string
	credentials aws.CredentialsProvider

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

func (p *iamTokenProvider) GetToken(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	refreshAt := p.expiresAt.Add(-rdsIAMRefreshBeforeExpiry)
	if p.token != "" && time.Now().Before(refreshAt) {
		return p.token, nil
	}

	if p.region == "" {
		return "", fmt.Errorf("AWS region is required for RDS IAM authentication")
	}

	endpoint := net.JoinHostPort(p.host, p.port)
	token, err := rdsauth.BuildAuthToken(ctx, endpoint, p.region, p.user, p.credentials)
	if err != nil {
		return "", fmt.Errorf("failed to generate RDS IAM auth token: %w", err)
	}

	p.token = token
	p.expiresAt = time.Now().Add(rdsIAMTokenTTL)
	return p.token, nil
}
