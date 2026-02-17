package sqs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const defaultThreshold = 0

type Client struct {
	svc      *sqs.Client
	queueURL string
}

func New(ctx context.Context, queueURL string) (*Client, error) {
	if queueURL == "" {
		return nil, errors.New("queueURL is required")
	}
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return &Client{
		svc:      sqs.NewFromConfig(cfg),
		queueURL: queueURL,
	}, nil
}

func (c *Client) SendMessage(ctx context.Context, body string, attrs map[string]string) (string, error) {
	msgAttrs := map[string]types.MessageAttributeValue{}
	for k, v := range attrs {
		msgAttrs[k] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}

	out, err := c.svc.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:          aws.String(c.queueURL),
		MessageBody:       aws.String(body),
		MessageAttributes: msgAttrs,
	})
	if err != nil {
		return "", err
	}
	if out.MessageId == nil {
		return "", errors.New("no message id returned")
	}
	return *out.MessageId, nil
}

func (c *Client) IsQueueBelowThreshold(ctx context.Context) (bool, error) {
	thresholdStr := os.Getenv("SQS_QUEUE_THRESHOLD")
	threshold := defaultThreshold
	if thresholdStr != "" {
		n, err := strconv.Atoi(thresholdStr)
		if err != nil || n <= 0 {
			log.Fatalf("invalid SQS_QUEUE_THRESHOLD: %q", thresholdStr)
		}
		threshold = n
	}

	out, err := c.svc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(c.queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	if err != nil {
		return false, err
	}
	numMessagesStr, ok := out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
	if !ok {
		return false, errors.New("attribute ApproximateNumberOfMessages not found")
	}
	var numMessages int
	_, err = fmt.Sscanf(numMessagesStr, "%d", &numMessages)
	if err != nil {
		return false, fmt.Errorf("parsing number of messages: %w", err)
	}

	fmt.Printf("Number of messages in queue: %d\n", numMessages)

	return numMessages <= threshold, nil
}

func (c *Client) PurgeQueue(ctx context.Context) error {
	_, err := c.svc.PurgeQueue(ctx, &sqs.PurgeQueueInput{
		QueueUrl: aws.String(c.queueURL),
	})
	return err
}
