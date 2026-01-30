package dynamodbqueue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// listInternal returns all queues that exist in the system.
func listInternal(bq *baseQueue, ctx context.Context) ([]QueueAndClientId, error) {
	if bq.table == "" {
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

	for {
		input := &dynamodb.ScanInput{
			TableName:                &bq.table,
			ProjectionExpression:     expr.Projection(),
			ExpressionAttributeNames: expr.Names(),
		}

		if lastEvaluatedKey != nil {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		response, err := bq.client.Scan(ctx, input)

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

// purgeInternal deletes all messages from the queue based on queueName and clientID.
func purgeInternal(bq *baseQueue, ctx context.Context) error {
	if err := bq.validateOperation(); err != nil {
		return err
	}

	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		expr, err := expression.NewBuilder().
			WithKeyCondition(expression.Key("PK").Equal(expression.Value(bq.PartitionKey()))).
			Build()

		if err != nil {
			panic(err)
		}

		resp, err := bq.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &bq.table,
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

		if len(resp.Items) == 0 {
			break
		}

		writeReqs := make([]types.WriteRequest, len(resp.Items))

		for i, item := range resp.Items {
			writeReqs[i] = types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: map[string]types.AttributeValue{
						ColumnPK: &types.AttributeValueMemberS{Value: bq.PartitionKey()},
						ColumnSK: item[ColumnSK],
					},
				},
			}
		}

		err = bq.batchProcess(ctx, writeReqs)

		if err != nil {
			return err
		}

		lastEvaluatedKey = resp.LastEvaluatedKey

		if lastEvaluatedKey == nil {
			break
		}
	}

	return nil
}

// purgeAllInternal deletes all items from the DynamoDB table.
func purgeAllInternal(bq *baseQueue, ctx context.Context) error {
	if bq.table == "" {
		return fmt.Errorf(TableNameNotSet)
	}

	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		resp, err := bq.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         &bq.table,
			ExclusiveStartKey: lastEvaluatedKey,
		})

		if err != nil {
			return err
		}

		if len(resp.Items) == 0 {
			break
		}

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

		err = bq.batchProcess(ctx, writeReqs)

		if err != nil {
			return err
		}

		lastEvaluatedKey = resp.LastEvaluatedKey

		if lastEvaluatedKey == nil {
			break
		}
	}

	return nil
}

// dropQueueTableInternal drops the DynamoDB table.
func dropQueueTableInternal(bq *baseQueue, ctx context.Context) error {
	if bq.table == "" {
		return fmt.Errorf(TableNameNotSet)
	}

	_, err := bq.client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: &bq.table,
	})

	return err
}

// tableExistsInternal checks if the table exists.
func tableExistsInternal(bq *baseQueue, ctx context.Context) bool {
	_, err := bq.client.DescribeTable(
		ctx,
		&dynamodb.DescribeTableInput{TableName: aws.String(bq.table)},
	)

	return err == nil
}

// createQueueTableInternal creates the DynamoDB table for the queue.
func createQueueTableInternal(bq *baseQueue, ctx context.Context) (bool, error) {
	if bq.table == "" {
		return false, fmt.Errorf(TableNameNotSet)
	}

	_, err := bq.client.DescribeTable(
		ctx,
		&dynamodb.DescribeTableInput{TableName: aws.String(bq.table)},
	)

	var created bool

	if err != nil {
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
			TableName: &bq.table,
		}

		_, err := bq.client.CreateTable(ctx, input)

		if err != nil {
			return false, err
		}

		created = true

		time.Sleep(5 * time.Second)
	}

	for {
		resp, err := bq.client.DescribeTable(
			ctx,
			&dynamodb.DescribeTableInput{TableName: aws.String(bq.table)},
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
		ttlInput := &dynamodb.UpdateTimeToLiveInput{
			TableName: aws.String(bq.table),
			TimeToLiveSpecification: &types.TimeToLiveSpecification{
				AttributeName: aws.String(ColumnTTL),
				Enabled:       aws.Bool(true),
			},
		}
		_, err = bq.client.UpdateTimeToLive(ctx, ttlInput)

		if err != nil {
			return created, err
		}
	}

	return created, nil
}
