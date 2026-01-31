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

// maxPaginationPages is the maximum number of pagination pages to iterate.
// This prevents unbounded queries from consuming excessive resources.
// At 1000 items per page, this allows up to 100,000 items to be processed.
const maxPaginationPages = 100

// queryPageLimit is the maximum items per query page for bounded queries.
const queryPageLimit = 1000

// getInFlightGroups returns a set of message groups that have in-flight messages.
// Limits pagination to maxPaginationPages to prevent unbounded queries.
func (fq *FifoQueueImpl) getInFlightGroups(ctx context.Context) (map[string]bool, error) {
	inFlightGroups := make(map[string]bool)
	var lastEvaluatedKey map[string]types.AttributeValue

	now := time.Now().UnixMilli()
	pageCount := 0

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Prevent unbounded pagination
		if pageCount >= maxPaginationPages {
			if fq.logging {
				log.Printf("reached pagination limit (%d pages) while scanning in-flight groups", maxPaginationPages)
			}
			break
		}

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
			Limit:                     aws.Int32(queryPageLimit),
			ConsistentRead:            aws.Bool(true), // Use strongly consistent read for FIFO
		})

		if err != nil {
			return nil, err
		}

		pageCount++

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
		log.Printf("found %d groups with in-flight messages (scanned %d pages)", len(inFlightGroups), pageCount)
	}

	return inFlightGroups, nil
}

// queryVisibleMessages queries all visible messages from the queue.
// Limits pagination to maxPaginationPages and respects timeout to prevent unbounded queries.
//
//nolint:gocyclo,cyclop,funlen // complexity from pagination, timeout, and error handling is necessary
func (fq *FifoQueueImpl) queryVisibleMessages(
	ctx context.Context,
	timeout time.Duration,
) ([]messageCandidate, error) {
	var candidates []messageCandidate
	var lastEvaluatedKey map[string]types.AttributeValue

	startTime := time.Now()
	now := time.Now().UnixMilli()
	pageCount := 0

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Prevent unbounded pagination
		if pageCount >= maxPaginationPages {
			if fq.logging {
				log.Printf("reached pagination limit (%d pages) while querying visible messages", maxPaginationPages)
			}
			break
		}

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
			Limit:                     aws.Int32(queryPageLimit),
			ScanIndexForward:          aws.Bool(true), // FIFO: oldest first
			ConsistentRead:            aws.Bool(true), // Use strongly consistent read for FIFO
		})

		if err != nil {
			return nil, err
		}

		pageCount++

		for _, item := range resp.Items {
			sk, err := getSKValue(item)
			if err != nil {
				return nil, err
			}

			hiddenUntil, err := getDynamoDBAttributeNumber(ColumnHiddenUntil, item)
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

		if timeout > 0 && time.Since(startTime) > timeout {
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

	//nolint:errcheck // best-effort release; failure means lock expires naturally via visibility timeout
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
// Limits pagination to maxPaginationPages to prevent unbounded queries.
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
	pageCount := 0

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		// Prevent unbounded pagination - if we hit the limit, conservatively
		// assume there might be an older message we haven't found yet
		if pageCount >= maxPaginationPages {
			if fq.logging {
				log.Printf("reached pagination limit (%d pages) in isOldestInGroup, assuming not oldest", maxPaginationPages)
			}
			return false, nil // Conservative: assume not oldest
		}

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
			Limit:                     aws.Int32(queryPageLimit),
			ScanIndexForward:          aws.Bool(true),
			ConsistentRead:            aws.Bool(true), // CRITICAL: Use strongly consistent read
		})

		if err != nil {
			return false, err
		}

		pageCount++

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
