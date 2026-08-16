package main

import (
	"context"
	"fmt"
	"furniture-crawler-queue-manager/database"
	"furniture-crawler-queue-manager/secrets"
	"furniture-crawler-queue-manager/sqs"
	"hash/fnv"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/jackc/pgx/v5/pgxpool"
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

	err = secrets.GetDatabaseCredentials(ctx)
	if err != nil {
		log.Fatalf("failed to get database credentials: %v", err)
	}

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

	uncrawledUrls, err := getUncrawledPages(ctx, conn, amount)
	if err != nil {
		log.Fatal("Failed to get uncrawled pages:", err)
	}

	oldUrls, err := getOldPages(ctx, conn, amount)
	if err != nil {
		log.Fatal("Failed to get old pages:", err)
	}

	seen := make(map[string]struct{}, len(uncrawledUrls)+len(oldUrls))
	urls := make([]string, 0, len(uncrawledUrls)+len(oldUrls))

	for _, url := range append(uncrawledUrls, oldUrls...) {
		if _, ok := seen[url]; ok {
			continue
		}
		seen[url] = struct{}{}
		urls = append(urls, url)
	}

	count := 0
	for _, url := range urls {
		messageId, err := sqsClient.SendMessage(ctx, url)
		if err != nil {
			log.Fatal("Failed to send message to SQS:", err)
		}
		fmt.Printf("Sent message to SQS with Message ID: %s\n", messageId)

		count++
		fmt.Printf("URL: %s Count: %d\n", url, count)
	}

	fmt.Printf("Total messages sent to SQS: %d\n", count)

	return Response{
		Message: "Success",
		Status:  200,
	}, nil
}

func getUncrawledPages(ctx context.Context, conn *pgxpool.Pool, amount int) ([]string, error) {
	rows, err := conn.Query(ctx, `
		SELECT url
		FROM pages
		WHERE is_active = $1 AND s3_key = 'NOT_CRAWLED'
		ORDER BY RANDOM()
		LIMIT $2;`, true, amount)

	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var urls []string
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		urls = append(urls, url)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return urls, nil
}

func getOldPages(ctx context.Context, conn *pgxpool.Pool, amount int) ([]string, error) {
	crawlIntervalDays := os.Getenv("CRAWL_INTERVAL_DAYS")
	crawlIntervalDaysInt, err := strconv.Atoi(crawlIntervalDays)
	if err != nil {
		crawlIntervalDaysInt = 7
	}
	fetchIntervalHours := int(float64(crawlIntervalDaysInt*24) * 0.9)

	rows, err := conn.Query(ctx, `
		SELECT url
		FROM pages
		WHERE is_active = $1 AND s3_key != 'REDIRECT' AND last_crawled_at < NOW() - $2 * INTERVAL '1 hour'
		ORDER BY RANDOM()
		LIMIT $3;`, true, fetchIntervalHours, amount)

	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var urls []string
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		urlHash := getHashFromUrl(url)
		urlMod := urlHash % uint64(crawlIntervalDaysInt)

		now := time.Now().UTC()
		dayNum := now.Unix() / 86400
		dayMod := dayNum % int64(crawlIntervalDaysInt)

		if urlMod == uint64(dayMod) {
			urls = append(urls, url)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return urls, nil
}

func getHashFromUrl(url string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(url))
	return h.Sum64()
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
