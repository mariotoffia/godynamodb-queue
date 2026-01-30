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
		return nil, fmt.Errorf("maximum of 25 messages allowed, got %d", len(messages))
	}

	if len(messages) == 0 {
		return nil, nil
	}

	if ttl == 0 {
		ttl = bq.ttl
	}

	var writeRequests []types.WriteRequest

	for _, msg := range messages {
		sk := fmt.Sprintf("%d-%s", time.Now().UnixNano(), RandomString(bq.randomDigits))

		msg.MessageId = fmt.Sprintf("%s%s%s", bq.PartitionKey(), RecipientHandleSeparator, sk)

		if msg.Attributes == nil {
			msg.Attributes = map[string]string{}
		}

		msg.Attributes[AttrSentTimestamp] = fmt.Sprintf("%d", time.Now().UTC().UnixMilli())

		itemData := map[string]interface{}{
			ColumnPK:          bq.PartitionKey(),
			ColumnSK:          sk,
			ColumnHiddenUntil: time.Now().UnixMilli(),
			ColumnOwner:       bq.clientID,
			ColumnTTL:         time.Now().Add(ttl).Unix(),
			ColumnEvent:       msg,
		}

		// Add message group for FIFO queues
		if messageGroup != "" {
			itemData[ColumnMessageGroup] = messageGroup
		}

		item, err := attributevalue.MarshalMap(itemData)
		if err != nil {
			return nil, err
		}

		writeRequests = append(writeRequests, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})
	}

	var left = writeRequests

	for len(left) > 0 {
		output, err := bq.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				bq.table: left,
			},
		})

		if err != nil {
			var failedMessages []events.SQSMessage

			for _, msg := range left {
				if msg.PutRequest == nil || msg.PutRequest.Item == nil {
					continue
				}

				var sqsMsg events.SQSMessage
				if err := attributevalue.Unmarshal(msg.PutRequest.Item[ColumnEvent], &sqsMsg); err != nil {
					continue
				}

				failedMessages = append(failedMessages, sqsMsg)
			}

			return failedMessages, err
		}

		if len(output.UnprocessedItems) > 0 {
			left = output.UnprocessedItems[bq.table]
		} else {
			left = nil
		}
	}

	return nil, nil
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
		pk, sk, hidden_until, owner := decodeRecipientHandle(receiptHandle)

		if bq.logging {
			log.Printf(
				"deleting message: PK: %s, SK: %s, hidden_until: %d, owner: %s",
				pk, sk, hidden_until, owner,
			)
		}

		if pk == "" || hidden_until == -1 {
			invalid = append(invalid, receiptHandle)
			continue
		}

		expr, err := expression.NewBuilder().
			WithCondition(
				expression.Equal(
					expression.Name(ColumnHiddenUntil), expression.Value(hidden_until)).
					And(
						expression.Equal(expression.Name(ColumnOwner), expression.Value(owner)),
					),
			).
			Build()

		if err != nil {
			panic(err)
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
