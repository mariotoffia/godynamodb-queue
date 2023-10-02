package dynamodbqueue

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// batchProcess will process the batch of put/delete requests.
func (dq *DynamoDBQueue) batchProcess(
	ctx context.Context,
	writeReqs []types.WriteRequest,
) error {
	// Split the batch into chunks of 25 and process each separately
	chunks := ToBatches(writeReqs, 25)

	for _, chunk := range chunks {
		// Track the left over messages to re-send
		var left []types.WriteRequest = chunk

		for len(left) > 0 {
			// Send the batch
			resp, err := dq.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					dq.table: left,
				},
			})

			if err != nil {
				return err
			}

			if len(resp.UnprocessedItems) > 0 {
				// Pick up the unprocessed ones and try again
				left = resp.UnprocessedItems[dq.table]
			} else {
				left = nil
			}
		}
	}

	return nil
}
