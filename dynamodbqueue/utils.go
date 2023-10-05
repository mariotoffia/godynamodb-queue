package dynamodbqueue

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// getDynamoDbAttributeNumber will try to extract the _id_ dynamodb `types.AttributeValue` and see if it is
// a number. If it is, it will return the number, otherwise it will return an error.
func getDynamoDbAttributeNumber(
	ctx context.Context,
	id string,
	attrs map[string]types.AttributeValue,
) (int64, error) {
	if val, ok := attrs["hidden_until"].(*types.AttributeValueMemberN); ok {
		hu, err := strconv.ParseInt(val.Value, 10, 64)
		if err != nil {
			// Handle the error
			panic(err)
		}
		return hu, nil
	} else {
		return 0, fmt.Errorf("hidden_until is not a number")
	}
}

// isValidClientID will check if the clientID is valid.
func isValidClientID(clientID string) bool {
	return len(clientID) > 0 && len(clientID) <= 64 &&
		!strings.Contains(clientID, RecipientHandleSeparator) &&
		!strings.Contains(clientID, PartitionKeySeparator)
}

// isValidQueueName will check if the queueName is valid.
func isValidQueueName(queueName string) bool {
	return len(queueName) > 0 && len(queueName) <= 64 &&
		!strings.Contains(queueName, RecipientHandleSeparator) &&
		!strings.Contains(queueName, PartitionKeySeparator)
}

// decodeRecipientHandle will parse the recipient handle on the format: "pk#sk#hidden_until#owner"
//
// If it fails, it will return empty strings.
func decodeRecipientHandle(
	recipientHandle string,
) (pk, sk string, hidden_until int64, owner string) {
	parts := strings.Split(recipientHandle, "#")

	if len(parts) != 4 {
		return "", "", -1, ""
	}

	hidden_until, _ = strconv.ParseInt(parts[2], 10, 64)

	return parts[0], parts[1], hidden_until, parts[3]
}

// validateOperation will make sure the table, queueName and clientID is set.
func (dq *DynamoDBQueue) validateOperation(ctx context.Context) error {
	//
	if dq.table == "" {
		return fmt.Errorf(TableNameNotSet)
	}

	if dq.queueName == "" {
		return fmt.Errorf(QueueNameNotSet)
	}

	if dq.clientID == "" {
		return fmt.Errorf(ClientIDNotSet)
	}

	return nil
}

func ToBatches[T any](items []T, batchSize int) [][]T {
	if len(items) == 0 {
		return nil
	}

	var noBatches int

	itemLen := len(items)

	if itemLen%batchSize == 0 {
		noBatches = itemLen / batchSize
	} else {
		noBatches = itemLen/batchSize + 1
	}

	batches := make([][]T, noBatches)
	batchIndex := 0

	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize

		if end > len(items) {
			end = len(items)
		}

		batches[batchIndex] = items[i:end]
		batchIndex++
	}

	return batches
}

// ToReceiptHandles will extract the receipt handles from the SQS messages.
func ToReceiptHandles(msgs []events.SQSMessage) []string {
	if len(msgs) == 0 {
		return nil
	}

	receiptHandles := make([]string, len(msgs))

	for i, msg := range msgs {
		receiptHandles[i] = msg.ReceiptHandle
	}

	return receiptHandles
}
func RandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[RandomInt(0, len(letters))]
	}

	return string(s)
}

func RandomInt(min, max int) int {
	idx, err := rand.Int(rand.Reader, big.NewInt(int64(max-min)))
	if err != nil {
		panic(err)
	}

	return int(idx.Int64()) + min
}

// RandomPostfix postfixes the in param name with an random integer
func RandomPostfix(name string) string {
	return fmt.Sprintf("%s-%s", name, RandomString(6))
}
