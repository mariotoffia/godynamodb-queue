package dynamodbqueue

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// getDynamoDbAttributeNumber extracts a number attribute from a DynamoDB item.
func getDynamoDbAttributeNumber(
	id string,
	attrs map[string]types.AttributeValue,
) (int64, error) {
	attr, ok := attrs[id]
	if !ok {
		return 0, fmt.Errorf("%s attribute not found", id)
	}

	if val, ok := attr.(*types.AttributeValueMemberN); ok {
		hu, err := strconv.ParseInt(val.Value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse %s: %w", id, err)
		}
		return hu, nil
	}

	return 0, fmt.Errorf("%s is not a number", id)
}

// getSKValue extracts the SK (sort key) string value from the item attributes.
func getSKValue(attrs map[string]types.AttributeValue) (string, error) {
	skAttr, ok := attrs["SK"]
	if !ok {
		return "", fmt.Errorf("SK attribute not found")
	}

	if val, ok := skAttr.(*types.AttributeValueMemberS); ok {
		return val.Value, nil
	}

	return "", fmt.Errorf("SK is not a string")
}

// isValidClientID checks if the clientID is valid.
func isValidClientID(clientID string) bool {
	return len(clientID) > 0 && len(clientID) <= 64 &&
		!strings.Contains(clientID, RecipientHandleSeparator) &&
		!strings.Contains(clientID, PartitionKeySeparator)
}

// isValidQueueName checks if the queueName is valid.
func isValidQueueName(queueName string) bool {
	return len(queueName) > 0 && len(queueName) <= 64 &&
		!strings.Contains(queueName, RecipientHandleSeparator) &&
		!strings.Contains(queueName, PartitionKeySeparator)
}

// decodeRecipientHandle parses the recipient handle on the format:
// SEP={RecipientHandleSeparator}
// "pk{SEP}sk{SEP}hidden_until{SEP}owner"
//
// If it fails, it returns empty strings and -1 for hidden_until.
func decodeRecipientHandle(
	recipientHandle string,
) (pk, sk string, hidden_until int64, owner string) {
	parts := strings.Split(recipientHandle, RecipientHandleSeparator)

	if len(parts) != 4 {
		return "", "", -1, ""
	}

	hidden_until, _ = strconv.ParseInt(parts[2], 10, 64)

	return parts[0], parts[1], hidden_until, parts[3]
}

// ToBatches splits a slice into batches of the specified size.
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

// ToReceiptHandles extracts the receipt handles from the SQS messages.
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

// RandomString generates a random alphanumeric string of length n.
func RandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[RandomInt(0, len(letters))]
	}

	return string(s)
}

// RandomInt generates a random integer in the range [min, max).
func RandomInt(min, max int) int {
	idx, err := rand.Int(rand.Reader, big.NewInt(int64(max-min)))
	if err != nil {
		panic(err)
	}

	return int(idx.Int64()) + min
}

// RandomPostfix postfixes the name with a random string.
func RandomPostfix(name string) string {
	return fmt.Sprintf("%s-%s", name, RandomString(6))
}
