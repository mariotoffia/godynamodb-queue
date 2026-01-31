package dynamodbqueue

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-lambda-go/events"
)

// DynamoDB column names.
const (
	// ColumnPK is the name of the partition key.
	ColumnPK = "PK"
	// ColumnSK is the name of the sort key.
	ColumnSK = "SK"
	// ColumnHiddenUntil is the name of the hidden_until attribute where the visibility timeout
	// while polling messages is stored and compared against.
	ColumnHiddenUntil = "hidden_until"
	// ColumnOwner is the name of the owner attribute where the clientID is stored.
	ColumnOwner = "owner"
	// ColumnTTL is the name of the TTL attribute where the time to live is stored.
	// This is used when the message is pushed to the queue and DynamoDB is configured to
	// automatically delete the message when the TTL is reached.
	ColumnTTL = "TTL"
	// ColumnEvent is the name of the event attribute where the `events.SQSMessage` is stored.
	ColumnEvent = "event"
	// ColumnMessageGroup is the name of the message_group attribute for FIFO queues.
	ColumnMessageGroup = "message_group"
)

// SQS message attribute names.
const (
	AttrSentTimestamp = "SentTimestamp"
)

// Separator constants.
const PartitionKeySeparator = "|"
const RecipientHandleSeparator = "&"

// Sentinel errors for validation failures.
var (
	ErrTableNameNotSet  = errors.New("table name not set")
	ErrQueueNameNotSet  = errors.New("queue name not set")
	ErrClientIDNotSet   = errors.New("client ID not set")
	ErrInvalidQueueName = errors.New("invalid queue name: must be 1-64 chars without | or & separators")
	ErrInvalidClientID  = errors.New("invalid client ID: must be 1-64 chars without | or & separators")
	ErrExpressionBuild  = errors.New("failed to build DynamoDB expression")
)

// Sentinel errors for message operations.
var (
	ErrTooManyMessages    = errors.New("maximum of 25 messages allowed")
	ErrMessageTooLarge    = errors.New("message body exceeds maximum size")
	ErrAttributeNotFound  = errors.New("attribute not found")
	ErrAttributeNotNumber = errors.New("attribute is not a number")
	ErrAttributeNotString = errors.New("attribute is not a string")
	ErrBatchWriteRetries  = errors.New("batch write failed after max retries")
)

// MaxMessageBodySize is the maximum allowed size for a message body (256KB).
// This is conservative compared to DynamoDB's 400KB item limit to leave room
// for message attributes and metadata.
const MaxMessageBodySize = 256 * 1024

// QueueAndClientID is used when `List` operation returns queue/clientID combinations.
type QueueAndClientID struct {
	QueueName string `json:"qn"`
	ClientID  string `json:"cid"`
}

// QueueType represents the type of queue to create.
type QueueType int

const (
	// QueueStandard is the default queue type with best-effort FIFO ordering,
	// high throughput, and at-least-once delivery.
	QueueStandard QueueType = iota

	// QueueFIFO provides strict FIFO ordering per message group with exactly
	// one message in-flight per group at a time.
	QueueFIFO
)

// DefaultMessageGroup is used when no message group is specified for FIFO queues.
const DefaultMessageGroup = "default"

// Queue is the common interface for all queue types.
// It provides methods for managing messages in a DynamoDB-backed queue.
type Queue interface {
	// UseTable sets the DynamoDB table name for the queue.
	UseTable(table string) Queue

	// UseQueueName sets the queue name (part of the partition key).
	UseQueueName(queueName string) Queue

	// UseClientID sets the client ID (part of the partition key).
	UseClientID(clientID string) Queue

	// SetLogging enables or disables logging for queue operations.
	SetLogging(enabled bool)

	// Logging returns true if logging is enabled.
	Logging() bool

	// Table returns the DynamoDB table name.
	Table() string

	// QueueName returns the queue name.
	QueueName() string

	// ClientID returns the client ID.
	ClientID() string

	// PartitionKey returns the combined partition key (queueName|clientID).
	PartitionKey() string

	// RandomDigits returns the number of random digits appended to SK.
	RandomDigits() int

	// PushMessages pushes one or more messages onto the queue.
	// Maximum of 25 messages allowed per call.
	// Returns any messages that failed to send along with an error.
	PushMessages(ctx context.Context, ttl time.Duration, messages ...events.SQSMessage) ([]events.SQSMessage, error)

	// PollMessages polls messages from the queue using SQS-compatible semantics.
	// timeout: Maximum time to wait for minMessages.
	// visibilityTimeout: How long messages are hidden after being returned.
	// minMessages: Minimum messages to collect before returning.
	// maxMessages: Maximum messages to return.
	PollMessages(ctx context.Context, timeout, visibilityTimeout time.Duration, minMessages, maxMessages int) ([]events.SQSMessage, error)

	// DeleteMessages deletes messages using their receipt handles.
	// Returns receipt handles that failed to delete.
	DeleteMessages(ctx context.Context, receiptHandles ...string) ([]string, error)

	// Count returns the number of messages in the queue.
	Count(ctx context.Context) (int32, error)

	// Purge deletes all messages from the queue.
	Purge(ctx context.Context) error

	// PurgeAll deletes all items from the DynamoDB table.
	PurgeAll(ctx context.Context) error

	// List returns all queue/clientID combinations in the table.
	List(ctx context.Context) ([]QueueAndClientID, error)

	// CreateQueueTable creates the DynamoDB table for the queue.
	// Returns true if created, false if it already exists.
	CreateQueueTable(ctx context.Context) (bool, error)

	// DropQueueTable drops the DynamoDB table.
	DropQueueTable(ctx context.Context) error

	// TableExists returns true if the table exists.
	TableExists(ctx context.Context) bool

	// Type returns the queue type (Standard or FIFO).
	Type() QueueType
}

// FifoQueue extends Queue with message group support for FIFO queues.
type FifoQueue interface {
	Queue

	// PushMessagesWithGroup pushes messages to a specific message group.
	// Messages in the same group are delivered in strict FIFO order.
	// Only one message per group can be in-flight at a time.
	PushMessagesWithGroup(ctx context.Context, ttl time.Duration, messageGroup string, messages ...events.SQSMessage) ([]events.SQSMessage, error)
}
