package dynamodbqueue

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
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
func (fq *FifoQueueImpl) UseQueueName(queueName string) Queue {
	if !isValidQueueName(queueName) {
		panic("queueName must be set to a valid string")
	}
	fq.queueName = queueName
	return fq
}

// UseClientID sets the client ID.
func (fq *FifoQueueImpl) UseClientID(clientID string) Queue {
	if !isValidClientID(clientID) {
		panic("clientID must be set to a valid string")
	}
	fq.clientID = clientID
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

	for {
		if len(allMessages) >= maxMessages {
			break
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
	for _, msg := range candidates {
		groupedMessages[msg.messageGroup] = append(groupedMessages[msg.messageGroup], msg)
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
		candidate := msgs[0]

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
	c messageCandidate,
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
	msg.ReceiptHandle = fmt.Sprintf(
		"%s%s%d%s%s",
		msg.MessageId,
		RecipientHandleSeparator,
		newHiddenUntil,
		RecipientHandleSeparator,
		fq.clientID,
	)

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
func (fq *FifoQueueImpl) List(ctx context.Context) ([]QueueAndClientId, error) {
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

// getInFlightGroups returns a set of message groups that have in-flight messages.
func (fq *FifoQueueImpl) getInFlightGroups(ctx context.Context) (map[string]bool, error) {
	inFlightGroups := make(map[string]bool)
	var lastEvaluatedKey map[string]types.AttributeValue

	now := time.Now().UnixMilli()

	for {
		expr, err := expression.NewBuilder().
			WithKeyCondition(expression.Key("PK").Equal(expression.Value(fq.PartitionKey()))).
			WithFilter(
				expression.Name(ColumnHiddenUntil).GreaterThanEqual(expression.Value(now)),
			).
			WithProjection(
				expression.NamesList(
					expression.Name(ColumnMessageGroup),
				)).
			Build()

		if err != nil {
			return nil, fmt.Errorf("failed to build in-flight query expression: %w", err)
		}

		resp, err := fq.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &fq.table,
			KeyConditionExpression:    expr.KeyCondition(),
			ProjectionExpression:      expr.Projection(),
			FilterExpression:          expr.Filter(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         lastEvaluatedKey,
			ConsistentRead:            aws.Bool(true), // Use strongly consistent read for FIFO
		})

		if err != nil {
			return nil, err
		}

		for _, item := range resp.Items {
			if groupAttr, ok := item[ColumnMessageGroup]; ok {
				if val, ok := groupAttr.(*types.AttributeValueMemberS); ok {
					inFlightGroups[val.Value] = true
				}
			}
		}

		if resp.LastEvaluatedKey == nil {
			break
		}
		lastEvaluatedKey = resp.LastEvaluatedKey
	}

	if fq.logging {
		log.Printf("found %d groups with in-flight messages", len(inFlightGroups))
	}

	return inFlightGroups, nil
}

// queryVisibleMessages queries all visible messages from the queue.
func (fq *FifoQueueImpl) queryVisibleMessages(
	ctx context.Context,
	timeout time.Duration,
) ([]messageCandidate, error) {
	var candidates []messageCandidate
	var lastEvaluatedKey map[string]types.AttributeValue

	startTime := time.Now()
	now := time.Now().UnixMilli()

	for {
		expr, err := expression.NewBuilder().
			WithKeyCondition(expression.Key("PK").Equal(expression.Value(fq.PartitionKey()))).
			WithFilter(
				expression.Name(ColumnHiddenUntil).LessThan(expression.Value(now)),
			).
			WithProjection(
				expression.NamesList(
					expression.Name(ColumnSK),
					expression.Name(ColumnHiddenUntil),
					expression.Name(ColumnEvent),
					expression.Name(ColumnMessageGroup),
				)).
			Build()

		if err != nil {
			return nil, fmt.Errorf("failed to build query expression: %w", err)
		}

		resp, err := fq.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &fq.table,
			KeyConditionExpression:    expr.KeyCondition(),
			ProjectionExpression:      expr.Projection(),
			FilterExpression:          expr.Filter(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         lastEvaluatedKey,
			ScanIndexForward:          aws.Bool(true), // FIFO: oldest first
			ConsistentRead:            aws.Bool(true), // Use strongly consistent read for FIFO
		})

		if err != nil {
			return nil, err
		}

		for _, item := range resp.Items {
			sk, err := getSKValue(item)
			if err != nil {
				return nil, err
			}

			hiddenUntil, err := getDynamoDbAttributeNumber(ColumnHiddenUntil, item)
			if err != nil {
				return nil, err
			}

			var event events.SQSMessage
			if err := attributevalue.Unmarshal(item[ColumnEvent], &event); err != nil {
				return nil, fmt.Errorf("failed to unmarshal event: %w", err)
			}

			// Extract message group
			messageGroup := DefaultMessageGroup
			if groupAttr, ok := item[ColumnMessageGroup]; ok {
				if val, ok := groupAttr.(*types.AttributeValueMemberS); ok {
					messageGroup = val.Value
				}
			}

			candidates = append(candidates, messageCandidate{
				sk:           sk,
				hiddenUntil:  hiddenUntil,
				event:        event,
				messageGroup: messageGroup,
			})
		}

		if resp.LastEvaluatedKey == nil {
			break
		}
		lastEvaluatedKey = resp.LastEvaluatedKey

		if time.Since(startTime) > timeout {
			break
		}
	}

	return candidates, nil
}

// releaseLock releases a lock by resetting the hiddenUntil to make the message visible again.
func (fq *FifoQueueImpl) releaseLock(ctx context.Context, sk string, currentHiddenUntil int64) {
	// Reset hiddenUntil to 0 (immediately visible) with a condition to ensure we still own the lock
	expr, err := expression.NewBuilder().
		WithCondition(
			expression.Equal(
				expression.Name(ColumnHiddenUntil),
				expression.Value(currentHiddenUntil),
			),
		).
		WithUpdate(
			expression.Set(
				expression.Name(ColumnHiddenUntil),
				expression.Value(0), // Make immediately visible
			),
		).
		Build()

	if err != nil {
		return
	}

	_, _ = fq.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &fq.table,
		Key: map[string]types.AttributeValue{
			ColumnPK: &types.AttributeValueMemberS{Value: fq.PartitionKey()},
			ColumnSK: &types.AttributeValueMemberS{Value: sk},
		},
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
}

// isOldestInGroup checks if the candidate is the oldest message in the group.
// This prevents locking a newer message when an older one exists (visible or not).
func (fq *FifoQueueImpl) isOldestInGroup(
	ctx context.Context,
	messageGroup string,
	candidateSK string,
) (bool, error) {
	// Query for ANY message in this group with SK < candidateSK
	// If such a message exists, we should not lock the candidate
	//
	// IMPORTANT: We cannot use Limit with a filter because DynamoDB applies
	// Limit BEFORE the filter. This could cause false positives where messages
	// from other groups are selected by limit, then filtered out, returning 0
	// results even when older messages from the target group exist.
	// Instead, we paginate through results until we find one matching message
	// or exhaust all candidates with SK < candidateSK.

	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		expr, err := expression.NewBuilder().
			WithKeyCondition(
				expression.Key("PK").Equal(expression.Value(fq.PartitionKey())).
					And(expression.Key("SK").LessThan(expression.Value(candidateSK))),
			).
			WithFilter(
				expression.Name(ColumnMessageGroup).Equal(expression.Value(messageGroup)),
			).
			WithProjection(expression.NamesList(expression.Name(ColumnSK))).
			Build()

		if err != nil {
			return false, fmt.Errorf("failed to build oldest check expression: %w", err)
		}

		resp, err := fq.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &fq.table,
			KeyConditionExpression:    expr.KeyCondition(),
			FilterExpression:          expr.Filter(),
			ProjectionExpression:      expr.Projection(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         lastEvaluatedKey,
			ScanIndexForward:          aws.Bool(true),
			ConsistentRead:            aws.Bool(true), // CRITICAL: Use strongly consistent read
		})

		if err != nil {
			return false, err
		}

		// If we found any message from this group with older SK, candidate is not oldest
		if len(resp.Items) > 0 {
			return false, nil
		}

		// If no more pages, candidate is the oldest
		if resp.LastEvaluatedKey == nil {
			return true, nil
		}

		lastEvaluatedKey = resp.LastEvaluatedKey
	}
}
