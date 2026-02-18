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

// StandardQueue implements the Queue interface with best-effort FIFO ordering,
// high throughput, and at-least-once delivery semantics.
type StandardQueue struct {
	*baseQueue
}

// Ensure StandardQueue implements Queue interface.
var _ Queue = (*StandardQueue)(nil)

// NewStandardQueue creates a new StandardQueue instance.
func NewStandardQueue(client *dynamodb.Client, ttl time.Duration) *StandardQueue {
	return &StandardQueue{
		baseQueue: newBaseQueue(client, ttl, QueueStandard),
	}
}

// UseTable sets the DynamoDB table name.
func (sq *StandardQueue) UseTable(table string) Queue {
	sq.table = table
	return sq
}

// UseQueueName sets the queue name.
// Validation occurs when queue operations are called.
func (sq *StandardQueue) UseQueueName(queueName string) Queue {
	sq.queueName = queueName
	return sq
}

// UseClientID sets the client ID.
// Validation occurs when queue operations are called.
func (sq *StandardQueue) UseClientID(clientID string) Queue {
	sq.clientID = clientID
	return sq
}

// UseSigningKey sets a persistent HMAC signing key for receipt handle verification.
// The key must be at least MinSigningKeyLength (32) bytes; validation occurs at operation time.
func (sq *StandardQueue) UseSigningKey(key []byte) Queue {
	sq.useSigningKey(key)
	return sq
}

// PushMessages pushes messages to the queue.
func (sq *StandardQueue) PushMessages(
	ctx context.Context,
	ttl time.Duration,
	messages ...events.SQSMessage,
) ([]events.SQSMessage, error) {
	return pushMessagesInternal(sq.baseQueue, ctx, ttl, "", messages...)
}

// pollState holds state for the polling loop.
type pollState struct {
	startTime time.Time
	messages  []events.SQSMessage
}

// shouldBreak checks if the polling loop should exit.
func (ps *pollState) shouldBreak(minMessages, maxMessages int, timeout time.Duration) bool {
	if len(ps.messages) >= maxMessages {
		return true
	}
	if len(ps.messages) >= minMessages {
		return true
	}
	return time.Since(ps.startTime) > timeout
}

// PollMessages polls messages from the queue using SQS-compatible semantics.
func (sq *StandardQueue) PollMessages(
	ctx context.Context,
	timeout, visibilityTimeout time.Duration,
	minMessages, maxMessages int,
) ([]events.SQSMessage, error) {
	if err := sq.validateOperation(); err != nil {
		return nil, err
	}

	state := &pollState{startTime: time.Now()}

	for !state.shouldBreak(minMessages, maxMessages, timeout) {
		if err := ctx.Err(); err != nil {
			return sq.returnMessagesOrError(state.messages, err)
		}

		remaining := maxMessages - len(state.messages)
		remainingTimeout := sq.calcRemainingTimeout(timeout, state.startTime)

		candidates, err := sq.collectCandidates(ctx, remainingTimeout, minMessages-len(state.messages), remaining)
		if err != nil {
			return nil, err
		}

		if len(candidates) == 0 {
			time.Sleep(time.Duration(RandomInt(50, 150)) * time.Millisecond)
			continue
		}

		locked, err := sq.lockCandidates(ctx, candidates, visibilityTimeout)
		if err != nil {
			return nil, err
		}

		state.messages = append(state.messages, locked...)

		if len(locked) >= len(candidates) {
			time.Sleep(time.Duration(RandomInt(50, 150)) * time.Millisecond)
		}
	}

	if len(state.messages) == 0 {
		return nil, nil
	}

	return state.messages, nil
}

// returnMessagesOrError returns messages if any exist, otherwise returns the error.
func (sq *StandardQueue) returnMessagesOrError(messages []events.SQSMessage, err error) ([]events.SQSMessage, error) {
	if len(messages) > 0 {
		return messages, nil
	}
	return nil, err
}

// calcRemainingTimeout calculates the remaining timeout duration.
func (sq *StandardQueue) calcRemainingTimeout(timeout time.Duration, startTime time.Time) time.Duration {
	remaining := timeout - time.Since(startTime)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// DeleteMessages deletes messages using their receipt handles.
func (sq *StandardQueue) DeleteMessages(
	ctx context.Context,
	receiptHandles ...string,
) ([]string, error) {
	return deleteMessagesInternal(sq.baseQueue, ctx, receiptHandles...)
}

// Count returns the number of messages in the queue.
func (sq *StandardQueue) Count(ctx context.Context) (int32, error) {
	return countMessagesInternal(sq.baseQueue, ctx)
}

// Purge deletes all messages from the queue.
func (sq *StandardQueue) Purge(ctx context.Context) error {
	return purgeInternal(sq.baseQueue, ctx)
}

// PurgeAll deletes all items from the DynamoDB table.
func (sq *StandardQueue) PurgeAll(ctx context.Context) error {
	return purgeAllInternal(sq.baseQueue, ctx)
}

// List returns all queue/clientID combinations in the table.
func (sq *StandardQueue) List(ctx context.Context) ([]QueueAndClientID, error) {
	return listInternal(sq.baseQueue, ctx)
}

// CreateQueueTable creates the DynamoDB table for the queue.
func (sq *StandardQueue) CreateQueueTable(ctx context.Context) (bool, error) {
	return createQueueTableInternal(sq.baseQueue, ctx)
}

// DropQueueTable drops the DynamoDB table.
func (sq *StandardQueue) DropQueueTable(ctx context.Context) error {
	return dropQueueTableInternal(sq.baseQueue, ctx)
}

// TableExists returns true if the table exists.
func (sq *StandardQueue) TableExists(ctx context.Context) bool {
	return tableExistsInternal(sq.baseQueue, ctx)
}

// messageCandidate holds message data collected during the query phase before locking.
type messageCandidate struct {
	event        events.SQSMessage
	sk           string
	messageGroup string // Used by FIFO queue
	hiddenUntil  int64
}

// collectState holds state for the candidate collection loop.
type collectState struct { //nolint:govet // fieldalignment: internal state struct, not allocated in large quantities
	startTime        time.Time
	candidates       []messageCandidate
	lastQueriedSK    string
	lastEvaluatedKey map[string]types.AttributeValue
}

// shouldStopCollecting checks if we should stop collecting candidates.
func (cs *collectState) shouldStopCollecting(minMessages, maxMessages int, timeout time.Duration) bool {
	if len(cs.candidates) >= maxMessages {
		return true
	}
	if len(cs.candidates) >= minMessages {
		return true
	}
	return time.Since(cs.startTime) > timeout
}

// updatePaginationKey updates the pagination key for the next query.
func (cs *collectState) updatePaginationKey(pk string, respKey map[string]types.AttributeValue) {
	if respKey != nil {
		cs.lastEvaluatedKey = respKey
	} else if cs.lastQueriedSK != "" {
		cs.lastEvaluatedKey = map[string]types.AttributeValue{
			ColumnPK: &types.AttributeValueMemberS{Value: pk},
			ColumnSK: &types.AttributeValueMemberS{Value: cs.lastQueriedSK},
		}
	}
}

// collectCandidates queries for visible messages without locking them.
func (sq *StandardQueue) collectCandidates(
	ctx context.Context,
	timeout time.Duration,
	minMessages, maxMessages int,
) ([]messageCandidate, error) {
	state := &collectState{startTime: time.Now()}

	for !state.shouldStopCollecting(minMessages, maxMessages, timeout) {
		if err := ctx.Err(); err != nil {
			return sq.returnCandidatesOrError(state.candidates, err)
		}

		resp, err := sq.queryVisibleCandidates(ctx, maxMessages-len(state.candidates), state.lastEvaluatedKey)
		if err != nil {
			return nil, err
		}

		if sq.logging {
			log.Printf("query returned %d items", len(resp.Items))
		}

		if err := sq.processQueryItems(resp.Items, state, maxMessages); err != nil {
			return nil, err
		}

		state.updatePaginationKey(sq.PartitionKey(), resp.LastEvaluatedKey)
		time.Sleep(time.Duration(RandomInt(100, 500)) * time.Millisecond)
	}

	return state.candidates, nil
}

// returnCandidatesOrError returns candidates if any exist, otherwise returns the error.
func (sq *StandardQueue) returnCandidatesOrError(candidates []messageCandidate, err error) ([]messageCandidate, error) {
	if len(candidates) > 0 {
		return candidates, nil
	}
	return nil, err
}

// queryVisibleCandidates executes a query for visible messages.
func (sq *StandardQueue) queryVisibleCandidates(
	ctx context.Context,
	limit int,
	exclusiveStartKey map[string]types.AttributeValue,
) (*dynamodb.QueryOutput, error) {
	expr, err := expression.NewBuilder().
		WithKeyCondition(expression.Key("PK").Equal(expression.Value(sq.PartitionKey()))).
		WithFilter(
			expression.Name(ColumnHiddenUntil).
				LessThan(expression.Value(time.Now().UnixMilli())),
		).
		WithProjection(
			expression.NamesList(
				expression.Name(ColumnSK),
				expression.Name(ColumnHiddenUntil),
				expression.Name(ColumnEvent),
			)).
		Build()

	if err != nil {
		return nil, fmt.Errorf("failed to build query expression: %w", err)
	}

	return sq.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 &sq.table,
		KeyConditionExpression:    expr.KeyCondition(),
		ProjectionExpression:      expr.Projection(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		Limit:                     aws.Int32(int32(limit)), //nolint:gosec // G115: safe, limit is always small (<=25)
		ExclusiveStartKey:         exclusiveStartKey,
		ScanIndexForward:          aws.Bool(true),
	})
}

// processQueryItems processes query items and adds them to the collect state.
func (sq *StandardQueue) processQueryItems(
	items []map[string]types.AttributeValue,
	state *collectState,
	maxMessages int,
) error {
	for _, item := range items {
		candidate, err := sq.parseItemToCandidate(item)
		if err != nil {
			return err
		}

		state.candidates = append(state.candidates, candidate)
		state.lastQueriedSK = candidate.sk

		if len(state.candidates) >= maxMessages {
			break
		}
	}
	return nil
}

// parseItemToCandidate parses a DynamoDB item into a messageCandidate.
func (sq *StandardQueue) parseItemToCandidate(item map[string]types.AttributeValue) (messageCandidate, error) {
	sk, err := getSKValue(item)
	if err != nil {
		return messageCandidate{}, err
	}

	hiddenUntil, err := getDynamoDBAttributeNumber(ColumnHiddenUntil, item)
	if err != nil {
		return messageCandidate{}, err
	}

	var event events.SQSMessage
	if err := attributevalue.Unmarshal(item[ColumnEvent], &event); err != nil {
		return messageCandidate{}, fmt.Errorf("failed to unmarshal event: %w", err)
	}

	return messageCandidate{
		sk:          sk,
		hiddenUntil: hiddenUntil,
		event:       event,
	}, nil
}

// lockCandidates attempts to lock all candidates with a uniform visibility timeout.
func (sq *StandardQueue) lockCandidates(
	ctx context.Context,
	candidates []messageCandidate,
	visibilityTimeout time.Duration,
) ([]events.SQSMessage, error) {
	lockTime := time.Now().UnixMilli()
	newHiddenUntil := lockTime + visibilityTimeout.Milliseconds()

	var messages []events.SQSMessage

	for i := range candidates {
		c := &candidates[i]
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
					expression.Value(sq.clientID),
				),
			).
			Build()

		if err != nil {
			return nil, fmt.Errorf("failed to build lock expression: %w", err)
		}

		_, err = sq.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &sq.table,
			Key: map[string]types.AttributeValue{
				ColumnPK: &types.AttributeValueMemberS{Value: sq.PartitionKey()},
				ColumnSK: &types.AttributeValueMemberS{Value: c.sk},
			},
			UpdateExpression:          expr.Update(),
			ConditionExpression:       expr.Condition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		})

		if err != nil {
			if sq.logging {
				log.Printf("failed to lock message (likely claimed by another consumer): SK: %s", c.sk)
			}
			continue
		}

		msg := c.event
		unsignedHandle := fmt.Sprintf(
			"%s%s%d%s%s",
			msg.MessageId,
			RecipientHandleSeparator,
			newHiddenUntil,
			RecipientHandleSeparator,
			sq.clientID,
		)
		msg.ReceiptHandle = sq.signReceiptHandle(unsignedHandle)

		messages = append(messages, msg)
	}

	if sq.logging {
		log.Printf("locked %d of %d candidates", len(messages), len(candidates))
	}

	return messages, nil
}
