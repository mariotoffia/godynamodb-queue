package dynamodbqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// baseQueue contains shared configuration and functionality
// for both StandardQueue and FifoQueue implementations.
type baseQueue struct {
	// client is the DynamoDB client
	client *dynamodb.Client
	// table is the name of the DynamoDB table to use
	table string
	// clientID the second part of the partition key (PK) for the queue.
	clientID string
	// queueName is the name of the queue. This is one component of the partition key.
	queueName string
	// ttl is the default time to live for messages in the queue.
	ttl time.Duration
	// randomDigits contains the number of random digits to append to the SK.
	randomDigits int
	// logging when set to true will log operations.
	logging bool
	// queueType identifies whether this is a standard or FIFO queue.
	queueType QueueType
}

// MinRandomDigits is the minimum number of random digits required for SK uniqueness.
// With 8 digits using 36 characters (a-z, 0-9), we have 36^8 = 2.8 trillion combinations,
// providing extremely low collision probability even at high write rates.
const MinRandomDigits = 8

// newBaseQueue creates a new baseQueue with the given configuration.
func newBaseQueue(client *dynamodb.Client, ttl time.Duration, queueType QueueType) *baseQueue {
	if ttl == 0 {
		ttl = 14 * 24 * time.Hour
	}

	return &baseQueue{
		client:       client,
		ttl:          ttl,
		randomDigits: MinRandomDigits,
		queueType:    queueType,
	}
}

// SetLogging enables or disables logging.
func (bq *baseQueue) SetLogging(enabled bool) {
	bq.logging = enabled
}

// Logging returns true if logging is enabled.
func (bq *baseQueue) Logging() bool {
	return bq.logging
}

// Table returns the DynamoDB table name.
func (bq *baseQueue) Table() string {
	return bq.table
}

// QueueName returns the queue name.
func (bq *baseQueue) QueueName() string {
	return bq.queueName
}

// ClientID returns the client ID.
func (bq *baseQueue) ClientID() string {
	return bq.clientID
}

// PartitionKey returns the combined partition key (queueName|clientID).
func (bq *baseQueue) PartitionKey() string {
	return bq.queueName + PartitionKeySeparator + bq.clientID
}

// RandomDigits returns the number of random digits appended to SK.
func (bq *baseQueue) RandomDigits() int {
	return bq.randomDigits
}

// Type returns the queue type.
func (bq *baseQueue) Type() QueueType {
	return bq.queueType
}

// DefaultTTL returns the default TTL for the queue.
func (bq *baseQueue) DefaultTTL() time.Duration {
	return bq.ttl
}

// Client returns the DynamoDB client.
func (bq *baseQueue) Client() *dynamodb.Client {
	return bq.client
}

// validateOperation ensures table, queueName and clientID are set.
func (bq *baseQueue) validateOperation() error {
	if bq.table == "" {
		return fmt.Errorf(TableNameNotSet)
	}

	if bq.queueName == "" {
		return fmt.Errorf(QueueNameNotSet)
	}

	if bq.clientID == "" {
		return fmt.Errorf(ClientIDNotSet)
	}

	return nil
}

// batchProcess processes write requests in batches of 25.
func (bq *baseQueue) batchProcess(ctx context.Context, writeReqs []types.WriteRequest) error {
	batches := ToBatches(writeReqs, 25)

	for _, batch := range batches {
		var left = batch

		for len(left) > 0 {
			output, err := bq.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					bq.table: left,
				},
			})

			if err != nil {
				return err
			}

			if len(output.UnprocessedItems) > 0 {
				left = output.UnprocessedItems[bq.table]
			} else {
				left = nil
			}
		}
	}

	return nil
}
