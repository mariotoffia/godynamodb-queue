package dynamodbqueue

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// pushMessagesInternal pushes messages with optional message group.
// If messageGroup is empty, no message_group attribute is added (Standard queue behavior).
// If messageGroup is non-empty, it's stored for FIFO queue semantics.
func pushMessagesInternal(
	bq *baseQueue,
	ctx context.Context,
	ttl time.Duration,
	messageGroup string,
	messages ...events.SQSMessage,
) ([]events.SQSMessage, error) {
	if err := bq.validateOperation(); err != nil {
		return nil, err
	}

	if len(messages) > 25 {
		return nil, fmt.Errorf("%w: got %d", ErrTooManyMessages, len(messages))
	}

	if len(messages) == 0 {
		return nil, nil
	}

	// Validate message sizes
	for i := range messages {
		if len(messages[i].Body) > MaxMessageBodySize {
			return nil, fmt.Errorf("%w: message %d has body size %d bytes (max %d)",
				ErrMessageTooLarge, i, len(messages[i].Body), MaxMessageBodySize)
		}
	}

	if ttl == 0 {
		ttl = bq.ttl
	}

	writeRequests, err := bq.prepareWriteRequests(ttl, messageGroup, messages)
	if err != nil {
		return nil, err
	}

	return bq.executeBatchWrite(ctx, writeRequests)
}

// prepareWriteRequests converts SQS messages into DynamoDB write requests.
func (bq *baseQueue) prepareWriteRequests(
	ttl time.Duration,
	messageGroup string,
	messages []events.SQSMessage,
) ([]types.WriteRequest, error) {
	writeRequests := make([]types.WriteRequest, 0, len(messages))

	for i := range messages {
		req, err := bq.prepareMessageWriteRequest(&messages[i], ttl, messageGroup)
		if err != nil {
			return nil, err
		}
		writeRequests = append(writeRequests, req)
	}

	return writeRequests, nil
}

// prepareMessageWriteRequest creates a DynamoDB write request for a single message.
// Optimized to call time.Now() once and use strconv for numeric conversions.
func (bq *baseQueue) prepareMessageWriteRequest(
	msg *events.SQSMessage,
	ttl time.Duration,
	messageGroup string,
) (types.WriteRequest, error) {
	// Call time.Now() once and derive all timestamps from it
	now := time.Now()
	nowNano := now.UnixNano()
	nowMilli := now.UnixMilli()
	ttlTimestamp := now.Add(ttl).Unix()

	// Build SK using strconv for efficiency
	sk := strconv.FormatInt(nowNano, 10) + "-" + RandomString(bq.randomDigits)

	// Build MessageId
	pk := bq.PartitionKey()
	msg.MessageId = pk + RecipientHandleSeparator + sk

	// Pre-allocate attributes map with expected capacity
	if msg.Attributes == nil {
		msg.Attributes = make(map[string]string, 1)
	}
	msg.Attributes[AttrSentTimestamp] = strconv.FormatInt(nowMilli, 10)

	// Build item data with pre-sized map
	// hiddenUntil = 0 makes message immediately visible (0 < any positive timestamp)
	itemData := make(map[string]any, 7)
	itemData[ColumnPK] = pk
	itemData[ColumnSK] = sk
	itemData[ColumnHiddenUntil] = 0
	itemData[ColumnOwner] = bq.clientID
	itemData[ColumnTTL] = ttlTimestamp
	itemData[ColumnEvent] = *msg

	if messageGroup != "" {
		itemData[ColumnMessageGroup] = messageGroup
	}

	item, err := attributevalue.MarshalMap(itemData)
	if err != nil {
		return types.WriteRequest{}, err
	}

	return types.WriteRequest{
		PutRequest: &types.PutRequest{Item: item},
	}, nil
}

// executeBatchWrite executes batch write requests with retry for unprocessed items.
// Includes context cancellation checks and retry limits to prevent infinite loops.
func (bq *baseQueue) executeBatchWrite(
	ctx context.Context,
	writeRequests []types.WriteRequest,
) ([]events.SQSMessage, error) {
	left := writeRequests
	retries := 0

	for len(left) > 0 {
		// Check context before each attempt
		if err := ctx.Err(); err != nil {
			return extractFailedMessages(left), err
		}

		// Prevent infinite retry loops
		if retries >= maxBatchRetries {
			return extractFailedMessages(left), fmt.Errorf("%w: %d retries", ErrBatchWriteRetries, retries)
		}

		output, err := bq.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				bq.table: left,
			},
		})

		if err != nil {
			return extractFailedMessages(left), err
		}

		if len(output.UnprocessedItems) > 0 {
			left = output.UnprocessedItems[bq.table]
			retries++

			// Exponential backoff with jitter before retry
			backoff := calcBackoff(retries)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return extractFailedMessages(left), ctx.Err()
			}
		} else {
			left = nil
		}
	}

	return nil, nil
}

// extractFailedMessages extracts SQS messages from failed write requests.
func extractFailedMessages(requests []types.WriteRequest) []events.SQSMessage {
	var failed []events.SQSMessage

	for i := range requests {
		req := &requests[i]
		if req.PutRequest == nil || req.PutRequest.Item == nil {
			continue
		}

		var sqsMsg events.SQSMessage
		if err := attributevalue.Unmarshal(req.PutRequest.Item[ColumnEvent], &sqsMsg); err != nil {
			continue
		}

		failed = append(failed, sqsMsg)
	}

	return failed
}

// deleteMessagesInternal deletes messages using their receipt handles.
func deleteMessagesInternal(
	bq *baseQueue,
	ctx context.Context,
	receiptHandles ...string,
) ([]string, error) {
	if err := bq.validateOperation(); err != nil {
		return nil, err
	}

	var invalid []string

	for _, receiptHandle := range receiptHandles {
		// Verify receipt handle signature
		unsignedHandle, valid := bq.verifyReceiptHandle(receiptHandle)
		if !valid {
			invalid = append(invalid, receiptHandle)
			continue
		}

		pk, sk, hiddenUntil, owner := decodeRecipientHandle(unsignedHandle)

		if bq.logging {
			// Log with truncated values to avoid exposing sensitive data
			log.Printf(
				"deleting message: SK: %s..., owner: %s...",
				truncateForLog(sk, 16), truncateForLog(owner, 8),
			)
		}

		if pk == "" || hiddenUntil == -1 {
			invalid = append(invalid, receiptHandle)
			continue
		}

		expr, err := expression.NewBuilder().
			WithCondition(
				expression.Equal(
					expression.Name(ColumnHiddenUntil), expression.Value(hiddenUntil)).
					And(
						expression.Equal(expression.Name(ColumnOwner), expression.Value(owner)),
					),
			).
			Build()

		if err != nil {
			// Expression build errors indicate programming bugs, log and skip this deletion
			invalid = append(invalid, receiptHandle)
			continue
		}

		_, err = bq.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: &bq.table,
			Key: map[string]types.AttributeValue{
				ColumnPK: &types.AttributeValueMemberS{Value: pk},
				ColumnSK: &types.AttributeValueMemberS{Value: sk}},
			ConditionExpression:       expr.Condition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		})

		if err != nil {
			invalid = append(invalid, receiptHandle)
		}
	}

	return invalid, nil
}

// countMessagesInternal returns the number of messages in the queue.
func countMessagesInternal(bq *baseQueue, ctx context.Context) (int32, error) {
	if err := bq.validateOperation(); err != nil {
		return 0, err
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String(bq.table),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: bq.PartitionKey()},
		},
		Select: types.SelectCount,
	}

	output, err := bq.client.Query(ctx, input)
	if err != nil {
		return 0, err
	}

	return output.Count, nil
}
