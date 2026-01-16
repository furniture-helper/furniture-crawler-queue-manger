package sqs

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

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
