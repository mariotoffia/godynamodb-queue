package dynamodbqueue

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// New creates a new Queue instance.
//
// By default, it creates a StandardQueue. Pass QueueFIFO as the third argument
// to create a FifoQueue instead.
//
// If ttl is zero, the default of 14 days is used.
//
// Examples:
//
//	// Create a standard queue (default)
//	queue := dynamodbqueue.New(cfg, 0)
//
//	// Explicitly create a standard queue
//	queue := dynamodbqueue.New(cfg, 0, dynamodbqueue.QueueStandard)
//
//	// Create a FIFO queue
//	queue := dynamodbqueue.New(cfg, 0, dynamodbqueue.QueueFIFO)
//	fifoQueue := queue.(dynamodbqueue.FifoQueue) // Access FIFO-specific methods
func New(cfg aws.Config, ttl time.Duration, queueType ...QueueType) Queue {
	client := dynamodb.NewFromConfig(cfg)

	qt := QueueStandard
	if len(queueType) > 0 {
		qt = queueType[0]
	}

	switch qt {
	case QueueFIFO:
		return NewFifoQueue(client, ttl)
	default:
		return NewStandardQueue(client, ttl)
	}
}

// NewWithClient creates a new Queue instance with an existing DynamoDB client.
//
// This is useful for testing or when you need custom client configuration.
func NewWithClient(client *dynamodb.Client, ttl time.Duration, queueType ...QueueType) Queue {
	qt := QueueStandard
	if len(queueType) > 0 {
		qt = queueType[0]
	}

	switch qt {
	case QueueFIFO:
		return NewFifoQueue(client, ttl)
	default:
		return NewStandardQueue(client, ttl)
	}
}
