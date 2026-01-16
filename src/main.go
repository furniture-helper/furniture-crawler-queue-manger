package main

import (
	"context"
	"fmt"
	"furniture-crawler-queue-manager/database"
	"furniture-crawler-queue-manager/sqs"
	"log"
	"os"
	"strconv"
)

const defaultFetchAmount = 10

func main() {
	ctx := context.Background()
	conn, err := database.Connect(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	amountStr := os.Getenv("FETCH_AMOUNT")
	amount := defaultFetchAmount
	if amountStr != "" {
		n, err := strconv.Atoi(amountStr)
		if err != nil || n <= 0 {
			log.Fatalf("invalid AMOUNT: %q", amountStr)
		}
		amount = n
	}

	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT url, domain from pages WHERE is_active = 'true' ORDER BY updated_at DESC LIMIT %d", amount))
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	queueUrl := os.Getenv("SQS_QUEUE_URL")
	if queueUrl == "" {
		log.Fatal("SQS_QUEUE_URL environment variable is not set")
	}

	fmt.Printf("Using SQS Queue URL: %s\n", queueUrl)

	sqsClient, err := sqs.New(ctx, queueUrl)
	if err != nil {
		log.Fatal("Failed to create SQS client:", err)
	}

	count := 0
	for rows.Next() {
		var url string
		var domain string
		if err := rows.Scan(&url, &domain); err != nil {
			log.Fatal("Scan failed:", err)
		}
		fmt.Printf("URL: %s, Domain: %s\n", url, domain)

		messageId, err := sqsClient.SendMessage(ctx, url, map[string]string{"domain": domain})
		if err != nil {
			log.Fatal("Failed to send message to SQS:", err)
		}
		fmt.Printf("Sent message to SQS with Message ID: %s\n", messageId)
		count++
	}

	fmt.Printf("Total messages sent to SQS: %d\n", count)

	if err := rows.Err(); err != nil {
		log.Fatal("rows error:", err)
	}
}
