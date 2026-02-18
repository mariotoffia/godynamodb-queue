package dynamodbqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// FifoQueueImpl implements the FifoQueue interface with strict FIFO ordering
// per message group and exactly one message in-flight per group.
type FifoQueueImpl struct {
	*baseQueue
}

// Ensure FifoQueueImpl implements both Queue and FifoQueue interfaces.
var _ Queue = (*FifoQueueImpl)(nil)
var _ FifoQueue = (*FifoQueueImpl)(nil)

// NewFifoQueue creates a new FifoQueueImpl instance.
func NewFifoQueue(client *dynamodb.Client, ttl time.Duration) *FifoQueueImpl {
	return &FifoQueueImpl{
		baseQueue: newBaseQueue(client, ttl, QueueFIFO),
	}
}

// UseTable sets the DynamoDB table name.
func (fq *FifoQueueImpl) UseTable(table string) Queue {
	fq.table = table
	return fq
}

// UseQueueName sets the queue name.
// Validation occurs when queue operations are called.
func (fq *FifoQueueImpl) UseQueueName(queueName string) Queue {
	fq.queueName = queueName
	return fq
}

// UseClientID sets the client ID.
// Validation occurs when queue operations are called.
func (fq *FifoQueueImpl) UseClientID(clientID string) Queue {
	fq.clientID = clientID
	return fq
}

// UseSigningKey sets a persistent HMAC signing key for receipt handle verification.
// The key must be at least MinSigningKeyLength (32) bytes; validation occurs at operation time.
func (fq *FifoQueueImpl) UseSigningKey(key []byte) Queue {
	fq.useSigningKey(key)
	return fq
}

// PushMessages pushes messages to the default message group.
func (fq *FifoQueueImpl) PushMessages(
	ctx context.Context,
	ttl time.Duration,
	messages ...events.SQSMessage,
) ([]events.SQSMessage, error) {
	return pushMessagesInternal(fq.baseQueue, ctx, ttl, DefaultMessageGroup, messages...)
}

// PushMessagesWithGroup pushes messages to a specific message group.
func (fq *FifoQueueImpl) PushMessagesWithGroup(
	ctx context.Context,
	ttl time.Duration,
	messageGroup string,
	messages ...events.SQSMessage,
) ([]events.SQSMessage, error) {
	if messageGroup == "" {
		messageGroup = DefaultMessageGroup
	}
	return pushMessagesInternal(fq.baseQueue, ctx, ttl, messageGroup, messages...)
}

// PollMessages polls messages from the queue using FIFO semantics.
// Only one message per message group can be in-flight at a time.
// To guarantee strict FIFO ordering under concurrent access, this implementation
// processes messages one at a time with verification.
func (fq *FifoQueueImpl) PollMessages(
	ctx context.Context,
	timeout, visibilityTimeout time.Duration,
	minMessages, maxMessages int,
) ([]events.SQSMessage, error) {
	if err := fq.validateOperation(); err != nil {
		return nil, err
	}

	var allMessages []events.SQSMessage
	startTime := time.Now()

	for len(allMessages) < maxMessages {
		// Check context cancellation
		select {
		case <-ctx.Done():
			if len(allMessages) > 0 {
				return allMessages, nil
			}
			return nil, ctx.Err()
		default:
		}

		elapsed := time.Since(startTime)
		if elapsed > timeout && len(allMessages) >= minMessages {
			break
		}

		// Find and lock ONE message at a time to guarantee FIFO ordering
		msg, err := fq.pollSingleMessage(ctx, visibilityTimeout)
		if err != nil {
			return nil, err
		}

		if msg != nil {
			allMessages = append(allMessages, *msg)
		} else {
			// No message available
			if len(allMessages) >= minMessages || time.Since(startTime) > timeout {
				break
			}
			time.Sleep(time.Duration(RandomInt(50, 150)) * time.Millisecond)
		}
	}

	if len(allMessages) == 0 {
		return nil, nil
	}

	return allMessages, nil
}

// pollSingleMessage finds and locks a single message with guaranteed FIFO ordering.
// It queries the oldest visible message across all groups, verifies it's truly the
// oldest in its group, and locks it atomically.
func (fq *FifoQueueImpl) pollSingleMessage(
	ctx context.Context,
	visibilityTimeout time.Duration,
) (*events.SQSMessage, error) {
	// Get current in-flight groups
	inFlightGroups, err := fq.getInFlightGroups(ctx)
	if err != nil {
		return nil, err
	}

	// Query visible messages
	candidates, err := fq.queryVisibleMessages(ctx, 0)
	if err != nil {
		return nil, err
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	// Group by message_group and find oldest per group
	groupedMessages := make(map[string][]messageCandidate)
	for i := range candidates {
		msg := &candidates[i]
		groupedMessages[msg.messageGroup] = append(groupedMessages[msg.messageGroup], *msg)
	}

	// Try to lock one message from an eligible group
	for group, msgs := range groupedMessages {
		if inFlightGroups[group] {
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		// Select the oldest message in this group
		candidate := &msgs[0]

		// Try to lock it
		msg, locked := fq.tryLockMessage(ctx, candidate, visibilityTimeout)
		if locked {
			return msg, nil
		}
		// Lock failed, try next group
	}

	return nil, nil
}

// tryLockMessage attempts to lock a single message with FIFO verification.
func (fq *FifoQueueImpl) tryLockMessage(
	ctx context.Context,
	c *messageCandidate,
	visibilityTimeout time.Duration,
) (*events.SQSMessage, bool) {
	lockTime := time.Now().UnixMilli()
	newHiddenUntil := lockTime + visibilityTimeout.Milliseconds()

	// Build the lock expression
	expr, err := expression.NewBuilder().
		WithCondition(
			expression.Equal(
				expression.Name(ColumnHiddenUntil),
				expression.Value(c.hiddenUntil),
			),
		).
		WithUpdate(
			expression.Set(
				expression.Name(ColumnHiddenUntil),
				expression.Value(newHiddenUntil),
			).Set(
				expression.Name(ColumnOwner),
				expression.Value(fq.clientID),
			),
		).
		Build()

	if err != nil {
		return nil, false
	}

	// Attempt to lock
	_, err = fq.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &fq.table,
		Key: map[string]types.AttributeValue{
			ColumnPK: &types.AttributeValueMemberS{Value: fq.PartitionKey()},
			ColumnSK: &types.AttributeValueMemberS{Value: c.sk},
		},
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})

	if err != nil {
		return nil, false
	}

	// POST-LOCK VERIFICATION: Ensure this is still the oldest message in the group
	isOldest, verifyErr := fq.isOldestInGroup(ctx, c.messageGroup, c.sk)
	if verifyErr != nil || !isOldest {
		// Release the lock - we're not the oldest
		fq.releaseLock(ctx, c.sk, newHiddenUntil)
		return nil, false
	}

	// Build the return message
	msg := c.event
	unsignedHandle := fmt.Sprintf(
		"%s%s%d%s%s",
		msg.MessageId,
		RecipientHandleSeparator,
		newHiddenUntil,
		RecipientHandleSeparator,
		fq.clientID,
	)
	msg.ReceiptHandle = fq.signReceiptHandle(unsignedHandle)

	return &msg, true
}

// DeleteMessages deletes messages using their receipt handles.
func (fq *FifoQueueImpl) DeleteMessages(
	ctx context.Context,
	receiptHandles ...string,
) ([]string, error) {
	return deleteMessagesInternal(fq.baseQueue, ctx, receiptHandles...)
}

// Count returns the number of messages in the queue.
func (fq *FifoQueueImpl) Count(ctx context.Context) (int32, error) {
	return countMessagesInternal(fq.baseQueue, ctx)
}

// Purge deletes all messages from the queue.
func (fq *FifoQueueImpl) Purge(ctx context.Context) error {
	return purgeInternal(fq.baseQueue, ctx)
}

// PurgeAll deletes all items from the DynamoDB table.
func (fq *FifoQueueImpl) PurgeAll(ctx context.Context) error {
	return purgeAllInternal(fq.baseQueue, ctx)
}

// List returns all queue/clientID combinations in the table.
func (fq *FifoQueueImpl) List(ctx context.Context) ([]QueueAndClientID, error) {
	return listInternal(fq.baseQueue, ctx)
}

// CreateQueueTable creates the DynamoDB table for the queue.
func (fq *FifoQueueImpl) CreateQueueTable(ctx context.Context) (bool, error) {
	return createQueueTableInternal(fq.baseQueue, ctx)
}

// DropQueueTable drops the DynamoDB table.
func (fq *FifoQueueImpl) DropQueueTable(ctx context.Context) error {
	return dropQueueTableInternal(fq.baseQueue, ctx)
}

// TableExists returns true if the table exists.
func (fq *FifoQueueImpl) TableExists(ctx context.Context) bool {
	return tableExistsInternal(fq.baseQueue, ctx)
}

