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
func (sq *StandardQueue) UseQueueName(queueName string) Queue {
	if !isValidQueueName(queueName) {
		panic("queueName must be set to a valid string")
	}
	sq.queueName = queueName
	return sq
}

// UseClientID sets the client ID.
func (sq *StandardQueue) UseClientID(clientID string) Queue {
	if !isValidClientID(clientID) {
		panic("clientID must be set to a valid string")
	}
	sq.clientID = clientID
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

// PollMessages polls messages from the queue using SQS-compatible semantics.
func (sq *StandardQueue) PollMessages(
	ctx context.Context,
	timeout, visibilityTimeout time.Duration,
	minMessages, maxMessages int,
) ([]events.SQSMessage, error) {
	if err := sq.validateOperation(); err != nil {
		return nil, err
	}

	var allMessages []events.SQSMessage
	startTime := time.Now()

	for {
		remaining := maxMessages - len(allMessages)
		if remaining <= 0 {
			break
		}

		elapsed := time.Since(startTime)
		remainingTimeout := timeout - elapsed
		if remainingTimeout < 0 {
			remainingTimeout = 0
		}

		candidates, err := sq.collectCandidates(ctx, remainingTimeout, minMessages-len(allMessages), remaining)
		if err != nil {
			return nil, err
		}

		if len(candidates) == 0 {
			if len(allMessages) >= minMessages || time.Since(startTime) > timeout {
				break
			}
			time.Sleep(time.Duration(RandomInt(50, 150)) * time.Millisecond)
			continue
		}

		locked, err := sq.lockCandidates(ctx, candidates, visibilityTimeout)
		if err != nil {
			return nil, err
		}

		allMessages = append(allMessages, locked...)

		if len(allMessages) >= minMessages {
			break
		}

		if time.Since(startTime) > timeout {
			break
		}

		if len(locked) >= len(candidates) {
			time.Sleep(time.Duration(RandomInt(50, 150)) * time.Millisecond)
		}
	}

	if len(allMessages) == 0 {
		return nil, nil
	}

	return allMessages, nil
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
func (sq *StandardQueue) List(ctx context.Context) ([]QueueAndClientId, error) {
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
	sk           string
	hiddenUntil  int64
	event        events.SQSMessage
	messageGroup string // Used by FIFO queue
}

// collectCandidates queries for visible messages without locking them.
func (sq *StandardQueue) collectCandidates(
	ctx context.Context,
	timeout time.Duration,
	minMessages, maxMessages int,
) ([]messageCandidate, error) {
	var candidates []messageCandidate
	var lastEvaluatedKey map[string]types.AttributeValue
	var lastQueriedSK string

	startTime := time.Now()

	for {
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

		resp, err := sq.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &sq.table,
			KeyConditionExpression:    expr.KeyCondition(),
			ProjectionExpression:      expr.Projection(),
			FilterExpression:          expr.Filter(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			Limit:                     aws.Int32(int32(maxMessages - len(candidates))),
			ExclusiveStartKey:         lastEvaluatedKey,
			ScanIndexForward:          aws.Bool(true),
		})

		if err != nil {
			return nil, err
		}

		if sq.logging {
			log.Printf("query returned %d items", len(resp.Items))
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

			candidates = append(candidates, messageCandidate{
				sk:          sk,
				hiddenUntil: hiddenUntil,
				event:       event,
			})

			lastQueriedSK = sk

			if len(candidates) >= maxMessages {
				break
			}
		}

		if len(candidates) >= minMessages {
			break
		}

		if time.Since(startTime) > timeout {
			break
		}

		if resp.LastEvaluatedKey != nil {
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else if lastQueriedSK != "" {
			lastEvaluatedKey = map[string]types.AttributeValue{
				ColumnPK: &types.AttributeValueMemberS{Value: sq.PartitionKey()},
				ColumnSK: &types.AttributeValueMemberS{Value: lastQueriedSK},
			}
		}

		time.Sleep(time.Duration(RandomInt(100, 500)) * time.Millisecond)
	}

	return candidates, nil
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

	for _, c := range candidates {
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
		msg.ReceiptHandle = fmt.Sprintf(
			"%s%s%d%s%s",
			msg.MessageId,
			RecipientHandleSeparator,
			newHiddenUntil,
			RecipientHandleSeparator,
			sq.clientID,
		)

		messages = append(messages, msg)
	}

	if sq.logging {
		log.Printf("locked %d of %d candidates", len(messages), len(candidates))
	}

	return messages, nil
}
