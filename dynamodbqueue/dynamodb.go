package dynamodbqueue

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	// ColumnPK is the name of the partition key
	ColumnPK = "PK"
	// ColumnSK is the name of the sort key
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
)

const (
	AttrSentTimestamp = "SentTimestamp"
)

const PartitionKeySeparator = "|"
const RecipientHandleSeparator = "&"

const (
	TableNameNotSet = "table name not set"
	QueueNameNotSet = "queue name not set"
	ClientIDNotSet  = "client ID not set"
)

// QueueType specifies how polling works, this may be dynamically changed for each
// poll. Default is _FIFO_.
type QueueType int

const (
	// QueueTypeFIFO is a FIFO queue. This affect sorting of messages.
	QueueTypeFIFO QueueType = 0
	// QueueTypeLIFO is a LIFO queue. This affect sorting of messages.
	QueueTypeLIFO QueueType = 1
)

// QueueAndClientId is used when `List` operation.
type QueueAndClientId struct {
	QueueName string `json:"qn"`
	ClientID  string `json:"cid"`
}

// DynamoDBQueue is a implementation of a queue but have
// DynamoDB table as the backing store.
//
// This implements a FIFO queue and allows for multiple
// "queues" to be present in the same table by concatenating
// the queue-name and consumer-id to the partition key.
//
// Each consumer Id is therefore also considered as a separate
// queue.
//
// Since multiple `DynamoDbQueue`/processes can simultaneously read/write/delete from the
// queue it will use a `visibilityTimeout` to ensure that only one consumer
// can process a message at a time.
//
// The DynamoDB Table have the following schema:
//
// |===
// |PK 								 |SK 																						|hidden_until						 	 |owner |TTL |event
// |queueName-clientID |{unix-64-bit-timestamp + "_" + random_string} |{now()+visibilityTimeout} |owner |ttl |{events.SQSMessage}
// |===
//
// The PK is separated using the `PartitionKeySeparator` of which is "+-+" by default.
//
// When the query is commenced, it will query for all messages within PK (oldest first) and that
// now() is greater than hidden_until.
//
// NOTE: The DynamoDB table must be configured with the TTL option on the TTL column in order for it to
// be deleted automatically.
type DynamoDBQueue struct {
	// client is the DynamoDB client
	client *dynamodb.Client
	// table is the name of the DynamoDB table to use
	table string
	// clientID the second part of the partition key (PK) for the queue. So it is possible
	// to have multiple consumers of the same queue that is isolated from each other.
	//
	// NOTE: Technically this each queueName, clientID pair is a separate queue.
	clientID string
	// queueName is the name of the queue. This is one component of the partition key.
	queueName string
	// ttl is the default time to live for messages in the queue. This is set on the TTL
	// attribute in DynamoDB. This is written in seconds.
	ttl time.Duration
	// randomDigits contains the number of random digits to append to the SK to ensure
	// uniqueness. This is set to 6 by default.
	randomDigits int
	// queueType is the type of queue to use. This affects the sorting of messages.
	// FIFO is the default.
	queueType QueueType
	// logging when set to `true` will log operations.
	logging bool
}

// New creates a new `DynamoDBQueue` instance.
//
// If the ttl is set to zero, it will use the default of 14 days.
func New(cfg aws.Config, ttl time.Duration) *DynamoDBQueue {
	if ttl == 0 {
		ttl = 14 * 24 * time.Hour
	}

	return &DynamoDBQueue{
		client:       dynamodb.NewFromConfig(cfg),
		ttl:          ttl,
		randomDigits: 6,
	}
}

func (dq *DynamoDBQueue) UseTable(table string) *DynamoDBQueue {
	dq.table = table

	return dq
}

// SetLogging will enable/disable logging.
func (dq *DynamoDBQueue) SetLogging(enabled bool) {
	dq.logging = enabled
}

// Logging returns `true` if logging is enabled.
func (dq *DynamoDBQueue) Logging() bool {
	return dq.logging
}

// UseClientID will change the clientID for the queue.
//
// CAUTION: The clientID *must* be set to a valid string otherwise it will panic!
func (dq *DynamoDBQueue) UseClientID(clientID string) *DynamoDBQueue {
	if !isValidClientID(clientID) {
		panic("clientID must be set to a valid string")
	}

	dq.clientID = clientID

	return dq
}

// UseQueueName will change the queueName for the queue.
//
// CAUTION: The queueName *must* be set to a valid string otherwise it will panic!
func (dq *DynamoDBQueue) UseQueueName(queueName string) *DynamoDBQueue {
	if !isValidQueueName(queueName) {
		panic("queueName must be set to a valid string")
	}

	dq.queueName = queueName

	return dq
}

// SetRandomDigits will set a new number of random digits to append to the SK to ensure
// uniqueness. This is set to 6 by default.
func (dp *DynamoDBQueue) SetRandomDigits(digits int) *DynamoDBQueue {
	if dp.randomDigits < 6 {
		panic("random digits must be at least 6")
	}

	dp.randomDigits = digits

	return dp
}

// AsQueueType will set the queue type to use. This affects the sorting of messages.
// FIFO is the default.
func (dp *DynamoDBQueue) AsQueueType(queueType QueueType) *DynamoDBQueue {
	dp.queueType = queueType
	return dp
}

func (dp *DynamoDBQueue) Table() string {
	return dp.table
}

func (dp *DynamoDBQueue) ClientID() string {
	return dp.clientID
}

func (dp *DynamoDBQueue) QueueName() string {
	return dp.queueName
}

// PartitionKey is the _queueName_ + '_' + _clientID_.
func (dq *DynamoDBQueue) PartitionKey() string {
	return dq.queueName + PartitionKeySeparator + dq.clientID
}

func (dq *DynamoDBQueue) RandomDigits() int {
	return dq.randomDigits
}

// PollMessages will poll messages from the queue up to either maxMessages or
// until the timeout is reached. It will query for all messages within PK
// (oldest first) and that now() is greater than hidden_until.
//
// If the message has been polled, it will be invisible to other consumers for
// the duration of the visibilityTimeout. The visibilityTimeout is set in milliseconds
// on the hidden_until attribute and hence cannot be any lower than that.
//
// NOTE: The visibilityTimeout is when a message been fetched. If the message
// was fetched early and the _timeout_ is great, it may have consumed all the
// visibility timeout before the message is returned. Make sure to set the
// _visibilityTimeout_ much higher than the _timeout_ and calculate the `actual
// visibility timeout = timeout - visibilityTimeout`.
//
// If the table is not set, it will return an error. If either of _queueName_ or
// _clientID_ is not set, it will return an error.
//
// The recipient handle is a combination of the PK and SK. It also adds the hidden_until
// and owner to the receipt handle to ensure that the message is not accidentally deleted
// by another consumer.
//
// It will poll until _timeout_ for _minMessages_, and will return at most _maxMessages_.
//
// If _timeout_ is set to zero, it will do one fetch and return even if _minMessages_ is not
// reached.
func (dq *DynamoDBQueue) PollMessages(
	ctx context.Context,
	timeout, visibilityTimeout time.Duration,
	minMessages, maxMessages int,
) ([]events.SQSMessage, error) {
	if err := dq.validateOperation(ctx); err != nil {
		return nil, err
	}

	var messages []events.SQSMessage
	var lastEvaluatedKey map[string]types.AttributeValue

	startTime := time.Now()

	for {
		// Calculate remaining visibility time for messages
		expr, err := expression.NewBuilder().
			// PK = queueName++-++clientID -> the queue
			WithKeyCondition(expression.Key("PK").Equal(expression.Value(dq.PartitionKey()))).
			WithFilter(
				// hidden_until < now() -> the message is visible
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
			panic(err)
		}

		// Query for messages
		resp, err := dq.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &dq.table,
			KeyConditionExpression:    expr.KeyCondition(),
			ProjectionExpression:      expr.Projection(),
			FilterExpression:          expr.Filter(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			Limit:                     aws.Int32(int32(maxMessages - len(messages))),
			ExclusiveStartKey:         lastEvaluatedKey,
			// true == oldest first -> FIFO
			ScanIndexForward: aws.Bool(dq.queueType == QueueTypeFIFO),
		})

		if err != nil {
			return nil, err
		}

		if dq.logging {
			log.Printf("found %d messages", len(resp.Items))
		}

		// Extract messages and add to results
		for _, item := range resp.Items {
			// Lock message
			hidden_until, he := getDynamoDbAttributeNumber(ctx, ColumnHiddenUntil, item)

			if he != nil {
				return nil, he
			}

			visibility_timeout := hidden_until + visibilityTimeout.Milliseconds()

			expr, err := expression.NewBuilder().
				// hidden_until ==  now() -> the message is not touched by another consumer
				WithCondition(
					expression.Equal(expression.Name(ColumnHiddenUntil),
						expression.Value(hidden_until)),
				).
				WithUpdate(
					// Set the hidden_until to now() + visibilityTimeout
					expression.Set(
						expression.Name(ColumnHiddenUntil),
						expression.Value(visibility_timeout),
					).
						// Set the owner to the clientID
						Set(expression.Name(ColumnOwner), expression.Value(dq.clientID)),
				).
				Build()

			if err != nil {
				panic(err)
			}

			_, err = dq.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: &dq.table,
				Key: map[string]types.AttributeValue{
					ColumnPK: &types.AttributeValueMemberS{Value: dq.PartitionKey()},
					ColumnSK: item[ColumnSK],
				},
				UpdateExpression:          expr.Update(),
				ConditionExpression:       expr.Condition(),
				ExpressionAttributeNames:  expr.Names(),
				ExpressionAttributeValues: expr.Values(),
			})

			if err != nil {
				// Failed, just skip the message
				if dq.logging {
					log.Printf(
						"failed to acquire message: PK: %s, SK: %v Owner: %s",
						dq.PartitionKey(), item[ColumnSK], dq.clientID)
				}

				continue
			}

			var msg events.SQSMessage
			if err := attributevalue.Unmarshal(item[ColumnEvent], &msg); err != nil {
				fmt.Println("FAILED: ", item[ColumnSK])
				return nil, err
			}

			// SEP=RecipientHandleSeparator
			// PK{SEP}SK{SEP}hidden_until{SEP}owner
			msg.ReceiptHandle = fmt.Sprintf(
				"%s%s%d%s%s",
				// it is already PK{SEP}SK
				msg.MessageId,
				RecipientHandleSeparator,
				visibility_timeout,
				RecipientHandleSeparator,
				dq.clientID,
			)

			messages = append(messages, msg)
		}

		// If there's no more data or we reached the desired number of messages, break out of the loop
		if len(messages) >= minMessages {
			break
		}

		// If we've exceeded the timeout, break out of the loop
		if time.Since(startTime) > timeout {
			break
		}

		// Set the last evaluated key for the next iteration
		lastEvaluatedKey = resp.LastEvaluatedKey

		// Try to spread out if multiple consumers and if not, wait so it doesn't hammer the database
		// (since we're querying the database until msgCount or timeout...)
		time.Sleep(time.Duration(RandomInt(100, 500)) * time.Millisecond)
	}

	return messages, nil
}

// PushMessages pushes one or more messages onto the database. It uses the unix nanosecond timestamp as SK.
// The primary key is the queueName{`PartitionKeySeparator`}clientID. If ttl is set to zero, it will use the default DynamoDBQueue.ttl.
//
// This function strives to create unique SKs by using the unix nanosecond timestamp plus a random number with n
// digits. The n is by default 6 but can be altered to be higher by `SetRandomDigits()` function.
//
// Maximum of 25 messages or 16MB is allowed.
//
// The function returns any message that was not sent it is accompanied by an error.
func (dq *DynamoDBQueue) PushMessages(
	ctx context.Context,
	ttl time.Duration,
	messages ...events.SQSMessage,
) ([]events.SQSMessage, error) {
	if err := dq.validateOperation(ctx); err != nil {
		return nil, err
	}

	if len(messages) > 25 {
		return nil, fmt.Errorf("maximum of 25 messages allowed, got %d", len(messages))
	}

	if len(messages) == 0 {
		return nil, nil
	}

	if ttl == 0 {
		ttl = dq.ttl
	}

	// Prepare the batch write input
	var writeRequests []types.WriteRequest

	for _, msg := range messages {
		// Generate a "unique" SK
		sk := fmt.Sprintf("%d-%s", time.Now().UnixNano(), RandomString(dq.randomDigits))

		// Must be unique (otherwise we have overwritten a message and thus this is now the message...)
		msg.MessageId = fmt.Sprintf("%s%s%s", dq.PartitionKey(), RecipientHandleSeparator, sk)

		// Put required attributes
		if msg.Attributes == nil {
			msg.Attributes = map[string]string{}
		}

		msg.Attributes[AttrSentTimestamp] = fmt.Sprintf("%d", time.Now().UTC().UnixMilli())

		item, err := attributevalue.MarshalMap(map[string]interface{}{
			ColumnPK:          dq.PartitionKey(),
			ColumnSK:          sk,
			ColumnHiddenUntil: time.Now().UnixMilli(),
			ColumnOwner:       dq.clientID,
			ColumnTTL:         time.Now().Add(ttl).Unix(),
			ColumnEvent:       msg,
		})

		if err != nil {
			return nil, err
		}

		writeRequests = append(writeRequests, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})
	}

	// Track the left over messages to re-send
	var left []types.WriteRequest = writeRequests

	for len(left) > 0 {
		// Perform the batch write operation
		output, err := dq.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				dq.table: left,
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
			left = output.UnprocessedItems[dq.table]
		} else {
			left = nil
		}
	}

	return nil, nil
}

// DeleteMessages will delete one or more messages from the queue.
//
// Use the `events.SQSMessage.ReceiptHandle` to delete the message.
//
// It will return those not deleted and an error if any.
func (dq *DynamoDBQueue) DeleteMessages(
	ctx context.Context,
	receiptHandles ...string,
) ([]string, error) {
	if err := dq.validateOperation(ctx); err != nil {
		return nil, err
	}

	var invalid []string

	for _, receiptHandle := range receiptHandles {
		pk, sk, hidden_until, owner := decodeRecipientHandle(receiptHandle)

		if dq.logging {
			log.Printf(
				"deleting message: PK: %s, SK: %s, hidden_until: %d, owner: %s",
				pk, sk, hidden_until, owner,
			)
		}

		if pk == "" || hidden_until == -1 {
			// Invalid receipt handle
			invalid = append(invalid, receiptHandle)
			continue
		}

		// Delete with condition that hidden_until == hidden_until and owner == owner, sk == sk and pk == pk
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

		_, err = dq.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: &dq.table,
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

// Count will return the number of messages in the queue. It will return an error if it fails.
//
// This will not count all messages in all queues in the table, just the one for
// current _queueName_ and _clientID_.
func (dq *DynamoDBQueue) Count(ctx context.Context) (int32, error) {
	if err := dq.validateOperation(ctx); err != nil {
		return 0, err
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String(dq.table),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: dq.PartitionKey()},
		},
		Select: types.SelectCount,
	}

	output, err := dq.client.Query(ctx, input)
	if err != nil {
		return 0, err
	}

	return output.Count, nil
}

// List will return all queues that exists in the system.
//
// CAUTION: This is a really expensive operation since it will perform a table scan.
// to get all partition keys.
func (dq *DynamoDBQueue) List(ctx context.Context) ([]QueueAndClientId, error) {
	if dq.table == "" {
		return nil, fmt.Errorf(TableNameNotSet)
	}

	var lastEvaluatedKey map[string]types.AttributeValue

	seen := make(map[string]bool)
	all := make([]QueueAndClientId, 0)

	expr, err := expression.NewBuilder().
		WithProjection(expression.NamesList(expression.Name(ColumnPK))).
		Build()

	if err != nil {
		panic(err)
	}

	// Continue scanning until all items are covered
	for {
		input := &dynamodb.ScanInput{
			TableName:                &dq.table,
			ProjectionExpression:     expr.Projection(),
			ExpressionAttributeNames: expr.Names(),
		}

		if lastEvaluatedKey != nil {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		// Execute the scan
		response, err := dq.client.Scan(ctx, input)

		if err != nil {
			return nil, err
		}

		for _, item := range response.Items {
			pk := ""
			err = attributevalue.Unmarshal(item[ColumnPK], &pk)

			if err != nil {
				return nil, err
			}

			if _, exists := seen[pk]; !exists {
				// New queue and clientID combination
				seen[pk] = true
				parts := strings.Split(pk, PartitionKeySeparator)

				if len(parts) == 2 {
					all = append(all, QueueAndClientId{QueueName: parts[0], ClientID: parts[1]})
				}
			}
		}

		if response.LastEvaluatedKey == nil {
			break
		}

		lastEvaluatedKey = response.LastEvaluatedKey
	}

	return all, nil
}

// Purge deletes all messages from the queue based on the queueName and clientID.
//
// If it fails it will return an error.
func (dq *DynamoDBQueue) Purge(ctx context.Context) error {
	if err := dq.validateOperation(ctx); err != nil {
		return err
	}

	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		expr, err := expression.NewBuilder().
			WithKeyCondition(expression.Key("PK").Equal(expression.Value(dq.PartitionKey()))).
			Build()

		if err != nil {
			panic(err)
		}

		// Query for all messages related to the queueName and clientID
		resp, err := dq.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &dq.table,
			KeyConditionExpression:    expr.KeyCondition(),
			ProjectionExpression:      expr.Projection(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			Limit:                     aws.Int32(1000),
			ExclusiveStartKey:         lastEvaluatedKey,
		})

		if err != nil {
			return err
		}

		// If there's no more data, break out of the loop
		if len(resp.Items) == 0 {
			break
		}

		// Batch delete the retrieved items
		writeReqs := make([]types.WriteRequest, len(resp.Items))

		for i, item := range resp.Items {
			writeReqs[i] = types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: map[string]types.AttributeValue{
						ColumnPK: &types.AttributeValueMemberS{Value: dq.PartitionKey()},
						ColumnSK: item[ColumnSK],
					},
				},
			}
		}

		err = dq.batchProcess(ctx, writeReqs)

		if err != nil {
			return err
		}

		// Set the last evaluated key for the next iteration
		lastEvaluatedKey = resp.LastEvaluatedKey

		if lastEvaluatedKey == nil {
			break
		}
	}

	return nil
}

// PurgeAll deletes all items from the DynamoDB table.
//
// If it fails, it will return an error.
func (dq *DynamoDBQueue) PurgeAll(ctx context.Context) error {
	if dq.table == "" {
		return fmt.Errorf(TableNameNotSet)
	}

	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		// Scan the table for all items
		resp, err := dq.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         &dq.table,
			ExclusiveStartKey: lastEvaluatedKey,
		})

		if err != nil {
			return err
		}

		// If there's no more data, break out of the loop
		if len(resp.Items) == 0 {
			break
		}

		// Batch delete the retrieved items
		writeReqs := make([]types.WriteRequest, len(resp.Items))
		for i, item := range resp.Items {
			writeReqs[i] = types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: map[string]types.AttributeValue{
						ColumnPK: item[ColumnPK],
						ColumnSK: item[ColumnSK],
					},
				},
			}
		}

		err = dq.batchProcess(ctx, writeReqs)

		if err != nil {
			return err
		}

		// Set the last evaluated key for the next iteration
		lastEvaluatedKey = resp.LastEvaluatedKey

		if lastEvaluatedKey == nil {
			break
		}
	}

	return nil
}

// DropQueueTable will drop the queue table.
func (dq *DynamoDBQueue) DropQueueTable(ctx context.Context) error {
	if dq.table == "" {
		return fmt.Errorf(TableNameNotSet)
	}

	_, err := dq.client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: &dq.table,
	})

	if err != nil {
		return err
	}

	return nil
}

// TableExists will check if the table exist, if not, it will return
// `false`, otherwise it will return `true`.
func (dq *DynamoDBQueue) TableExists(ctx context.Context) bool {
	_, err := dq.client.DescribeTable(
		ctx,
		&dynamodb.DescribeTableInput{TableName: aws.String(dq.table)},
	)

	return err == nil
}

// CreateQueueTable will create the DynamoDB table for the queue in on-demand mode.
//
// It will have the _TTL_ enabled and therefore messages are automatically deleted.
//
// It will return `true` if created, `false` if it already exists and an error if it fails.
//
// NOTE: It may return an error *and* `true`, hence the table was created but there where
// some other issues, and the table needs to be managed.
func (dq *DynamoDBQueue) CreateQueueTable(ctx context.Context) (bool, error) {
	if dq.table == "" {
		return false, fmt.Errorf(TableNameNotSet)
	}

	// Do not create table if it already exists
	_, err := dq.client.DescribeTable(
		ctx,
		&dynamodb.DescribeTableInput{TableName: aws.String(dq.table)},
	)

	var created bool

	// No table -> Create table
	if err != nil {
		// Define table attributes and schema
		input := &dynamodb.CreateTableInput{
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String(ColumnPK),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String(ColumnSK),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String(ColumnPK),
					KeyType:       types.KeyTypeHash,
				},
				{
					AttributeName: aws.String(ColumnSK),
					KeyType:       types.KeyTypeRange,
				},
			},
			BillingMode: types.BillingModePayPerRequest,
			SSESpecification: &types.SSESpecification{
				Enabled: aws.Bool(true),
				SSEType: types.SSETypeKms,
			},
			TableName: &dq.table,
		}

		// Create table
		_, err := dq.client.CreateTable(ctx, input)

		if err != nil {
			return false, err
		}

		created = true

		time.Sleep(5 * time.Second)
	}

	// Wait until table is active
	for {
		resp, err := dq.client.DescribeTable(
			ctx,
			&dynamodb.DescribeTableInput{TableName: aws.String(dq.table)},
		)
		if err != nil {
			return created, err
		}

		if resp.Table.TableStatus == types.TableStatusActive {
			break
		}

		time.Sleep(2 * time.Second)
	}

	if created {
		// Update TTL if newly created
		ttlInput := &dynamodb.UpdateTimeToLiveInput{
			TableName: aws.String(dq.table),
			TimeToLiveSpecification: &types.TimeToLiveSpecification{
				AttributeName: aws.String(ColumnTTL),
				Enabled:       aws.Bool(true),
			},
		}
		_, err = dq.client.UpdateTimeToLive(ctx, ttlInput)

		if err != nil {
			return created, err
		}
	}

	return created, nil
}
