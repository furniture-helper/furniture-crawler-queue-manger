package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type DatabaseSecrets struct {
	Username string `json:"username"`
	Password string `json:"password"`
	DBName   string `json:"database_name"`
}

var secretsClient *secretsmanager.Client

func init() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	secretsClient = secretsmanager.NewFromConfig(cfg)
}

func GetDatabaseCredentials(ctx context.Context) error {
	databaseCredentialsType := os.Getenv("DATABASE_CREDENTIALS_TYPE")
	if databaseCredentialsType != "secrets_manager" {
		fmt.Println("Skipping retrieval of database credentials from Secrets Manager.")
		return nil
	}

	secretName := os.Getenv("DATABASE_CREDENTIALS_SECRET_NAME")
	if secretName == "" {
		return fmt.Errorf("DATABASE_CREDENTIALS_SECRET_NAME must be set")
	}

	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	}

	result, err := secretsClient.GetSecretValue(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to retrieve secret: %w", err)
	}

	var dbSecrets DatabaseSecrets
	err = json.Unmarshal([]byte(*result.SecretString), &dbSecrets)
	if err != nil {
		return fmt.Errorf("failed to unmarshal secret: %w", err)
	}

	fmt.Println("Database credentials retrieved successfully from Secrets Manager.")

	err = os.Setenv("PG_USER", dbSecrets.Username)
	if err != nil {
		return fmt.Errorf("failed to set PG_USER environment variable: %w", err)
	}

	err = os.Setenv("PG_PASSWORD", dbSecrets.Password)
	if err != nil {
		return fmt.Errorf("failed to set PG_PASSWORD environment variable: %w", err)
	}

	err = os.Setenv("PG_DATABASE", dbSecrets.DBName)
	if err != nil {
		return fmt.Errorf("failed to set PG_DATABASE environment variable: %w", err)
	}

	fmt.Println("Database credentials set as environment variables.")
	return nil
}
