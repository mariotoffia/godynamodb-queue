package dynamodbqueue_test

import (
	"context"
	"testing"
	"time"

	"github.com/mariotoffia/godynamodb-queue/dynamodbqueue"
	"github.com/stretchr/testify/assert"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Factory Function Tests
//
// Tests for New() and NewWithClient() factory functions.
// ═══════════════════════════════════════════════════════════════════════════════

// TestNew_DefaultQueueType validates that New() creates StandardQueue by default.
func TestNew_DefaultQueueType(t *testing.T) {
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0)

	assert.NotNil(t, queue)
	assert.Equal(t, dynamodbqueue.QueueStandard, queue.Type(),
		"default queue type should be Standard")
}

// TestNew_ExplicitStandardQueue validates explicit StandardQueue creation.
func TestNew_ExplicitStandardQueue(t *testing.T) {
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueStandard)

	assert.NotNil(t, queue)
	assert.Equal(t, dynamodbqueue.QueueStandard, queue.Type())
}

// TestNew_FifoQueue validates FifoQueue creation.
func TestNew_FifoQueue(t *testing.T) {
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueFIFO)

	assert.NotNil(t, queue)
	assert.Equal(t, dynamodbqueue.QueueFIFO, queue.Type())

	// Should be castable to FifoQueue interface
	fifoQueue, ok := queue.(dynamodbqueue.FifoQueue)
	assert.True(t, ok, "FIFO queue should implement FifoQueue interface")
	assert.NotNil(t, fifoQueue)
}

// TestNew_DefaultTTL validates that zero TTL uses default 14 days.
func TestNew_DefaultTTL(t *testing.T) {
	// We can't directly access TTL, but we can verify the queue is created
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0)
	assert.NotNil(t, queue)
}

// TestNew_CustomTTL validates custom TTL is accepted.
func TestNew_CustomTTL(t *testing.T) {
	customTTL := 7 * 24 * time.Hour
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), customTTL)
	assert.NotNil(t, queue)
}

// TestNewWithClient_StandardQueue validates NewWithClient with StandardQueue.
func TestNewWithClient_StandardQueue(t *testing.T) {
	client := ddbLocal.DynamoDBClient()
	queue := dynamodbqueue.NewWithClient(client, 0, dynamodbqueue.QueueStandard)

	assert.NotNil(t, queue)
	assert.Equal(t, dynamodbqueue.QueueStandard, queue.Type())
}

// TestNewWithClient_FifoQueue validates NewWithClient with FifoQueue.
func TestNewWithClient_FifoQueue(t *testing.T) {
	client := ddbLocal.DynamoDBClient()
	queue := dynamodbqueue.NewWithClient(client, 0, dynamodbqueue.QueueFIFO)

	assert.NotNil(t, queue)
	assert.Equal(t, dynamodbqueue.QueueFIFO, queue.Type())
}

// TestNewWithClient_DefaultQueueType validates default is StandardQueue.
func TestNewWithClient_DefaultQueueType(t *testing.T) {
	client := ddbLocal.DynamoDBClient()
	queue := dynamodbqueue.NewWithClient(client, 0)

	assert.NotNil(t, queue)
	assert.Equal(t, dynamodbqueue.QueueStandard, queue.Type())
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fluent Configuration Tests
// ═══════════════════════════════════════════════════════════════════════════════

// TestFluentConfiguration validates chainable configuration methods.
func TestFluentConfiguration(t *testing.T) {
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0).
		UseTable("test-table").
		UseQueueName("test-queue").
		UseClientID("test-client")

	assert.Equal(t, "test-table", queue.Table())
	assert.Equal(t, "test-queue", queue.QueueName())
	assert.Equal(t, "test-client", queue.ClientID())
}

// TestFluentConfiguration_FifoQueue validates fluent config returns Queue interface.
func TestFluentConfiguration_FifoQueue(t *testing.T) {
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueFIFO).
		UseTable("fifo-table").
		UseQueueName("fifo-queue").
		UseClientID("fifo-client")

	assert.Equal(t, "fifo-table", queue.Table())
	assert.Equal(t, dynamodbqueue.QueueFIFO, queue.Type())

	// Can still cast to FifoQueue
	_, ok := queue.(dynamodbqueue.FifoQueue)
	assert.True(t, ok)
}

// TestLocalUseQueueName_InvalidReturnsErrorOnOperation validates that invalid queue names
// are accepted at setter time but return errors when operations are attempted.
// This is the deferred validation pattern - no panics in library code.
func TestLocalUseQueueName_InvalidReturnsErrorOnOperation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		queueName string
		wantErr   error
	}{
		{"empty queue name", "", dynamodbqueue.ErrQueueNameNotSet},
		{"queue name with pipes", "queue|with|pipes", dynamodbqueue.ErrInvalidQueueName},
		{"queue name with ampersands", "queue&with&ampersands", dynamodbqueue.ErrInvalidQueueName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0).
				UseTable(tableName).
				UseQueueName(tt.queueName).
				UseClientID("test-client")

			// Setters don't panic - validation happens at operation time
			_, err := queue.Count(ctx)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestLocalUseClientID_InvalidReturnsErrorOnOperation validates that invalid client IDs
// are accepted at setter time but return errors when operations are attempted.
func TestLocalUseClientID_InvalidReturnsErrorOnOperation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		clientID string
		wantErr  error
	}{
		{"empty client ID", "", dynamodbqueue.ErrClientIDNotSet},
		{"client ID with pipes", "client|with|pipes", dynamodbqueue.ErrInvalidClientID},
		{"client ID with ampersands", "client&with&ampersands", dynamodbqueue.ErrInvalidClientID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0).
				UseTable(tableName).
				UseQueueName("test-queue").
				UseClientID(tt.clientID)

			// Setters don't panic - validation happens at operation time
			_, err := queue.Count(ctx)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestLoggingConfiguration validates logging toggle.
func TestLoggingConfiguration(t *testing.T) {
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0)

	assert.False(t, queue.Logging(), "logging should be off by default")

	queue.SetLogging(true)
	assert.True(t, queue.Logging())

	queue.SetLogging(false)
	assert.False(t, queue.Logging())
}

// TestPartitionKey validates partition key format.
func TestPartitionKey(t *testing.T) {
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0).
		UseQueueName("orders").
		UseClientID("consumer-1")

	assert.Equal(t, "orders|consumer-1", queue.PartitionKey())
}
