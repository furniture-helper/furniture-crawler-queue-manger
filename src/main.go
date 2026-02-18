package main

import (
	"context"
	"fmt"
	"furniture-crawler-queue-manager/database"
	"furniture-crawler-queue-manager/secrets"
	"furniture-crawler-queue-manager/sqs"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-lambda-go/lambda"
)

type Response struct {
	Message string `json:"message"`
	Status  int    `json:"status"`
}

const defaultFetchAmount = 10

func HandleRequest(ctx context.Context) (Response, error) {
	queueUrl := os.Getenv("SQS_QUEUE_URL")
	if queueUrl == "" {
		log.Fatal("SQS_QUEUE_URL environment variable is not set")
	}

	fmt.Printf("Using SQS Queue URL: %s\n", queueUrl)

	sqsClient, err := sqs.New(ctx, queueUrl)
	if err != nil {
		log.Fatal("Failed to create SQS client:", err)
	}

	isQueueBelowThreshold, err := sqsClient.IsQueueBelowThreshold(ctx)
	if err != nil {
		log.Fatal("Failed to check if SQS queue is empty:", err)
	}

	if !isQueueBelowThreshold {
		fmt.Println("SQS queue is above threshold. Exiting without adding new messages.")
		return Response{
			Message: "Queue not empty, no messages added",
			Status:  200,
		}, nil
	}

	//err = sqsClient.PurgeQueue(ctx)
	//if err != nil {
	//	log.Fatal("Failed to purge SQS queue:", err)
	//}
	//fmt.Println("SQS queue purged successfully")
	//
	//err = secrets.GetDatabaseCredentials(ctx)
	//if err != nil {
	//	log.Fatalf("failed to get database credentials: %v", err)
	//}

	conn, err := database.Connect(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	//deletionIntervalStr := os.Getenv("DELETION_INTERVAL_DAYS")
	//deletionInterval := 3
	//if deletionIntervalStr != "" {
	//	n, err := strconv.Atoi(deletionIntervalStr)
	//	if err != nil || n <= 0 {
	//		log.Fatalf("invalid DELETION_INTERVAL_DAYS: %q", deletionIntervalStr)
	//	}
	//	deletionInterval = n
	//}

	//fmt.Printf("Deleting inactive pages older than %d days\n", deletionInterval)
	//_, err = conn.Exec(ctx, `
	//	DELETE FROM pages WHERE updated_at < NOW() - $1 * INTERVAL '1 day' AND s3_key != 'NOT_CRAWLED'
	//`, deletionInterval)
	//
	//if err != nil {
	//	log.Fatal("Query failed:", err)
	//}

	//fmt.Printf("Deleting old pages that have not been crawled in %d days\n", deletionInterval)
	//_, err = conn.Exec(ctx, `
	//	DELETE FROM pages WHERE created_at < NOW() - $1 * INTERVAL '1 day' AND s3_key = 'NOT_CRAWLED'
	//`, deletionInterval)
	//
	//if err != nil {
	//	log.Fatal("Query failed:", err)
	//}

	amountStr := os.Getenv("FETCH_AMOUNT")
	amount := defaultFetchAmount
	if amountStr != "" {
		n, err := strconv.Atoi(amountStr)
		if err != nil || n <= 0 {
			log.Fatalf("invalid AMOUNT: %q", amountStr)
		}
		amount = n
	}

	rows, err := conn.Query(ctx, `
		SELECT url, domain
		FROM pages
		WHERE is_active = $1
		  AND updated_at < NOW() - INTERVAL '24 hours'
		ORDER BY RANDOM()
		LIMIT $2`, true, amount)

	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var url string
		var domain string
		if err := rows.Scan(&url, &domain); err != nil {
			log.Fatal("Scan failed:", err)
		}

		messageId, err := sqsClient.SendMessage(ctx, url, map[string]string{"domain": domain})
		if err != nil {
			log.Fatal("Failed to send message to SQS:", err)
		}
		fmt.Printf("Sent message to SQS with Message ID: %s\n", messageId)

		count++
		fmt.Printf("URL: %s, Domain: %s Count: %d\n", url, domain, count)
	}

	fmt.Printf("Total messages sent to SQS: %d\n", count)

	if err := rows.Err(); err != nil {
		log.Fatal("rows error:", err)
	}

	return Response{
		Message: "Success",
		Status:  200,
	}, nil
}

func main() {
	// If running locally (no Lambda env var), call handler directly
	if os.Getenv("AWS_LAMBDA_FUNCTION_NAME") == "" {
		resp, err := HandleRequest(context.Background())
		fmt.Printf("Response: %+v, Error: %v\n", resp, err)
	} else {
		lambda.Start(HandleRequest)
	}
}
