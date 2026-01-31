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

// getDynamoDBAttributeNumber extracts a number attribute from a DynamoDB item.
func getDynamoDBAttributeNumber(
	id string,
	attrs map[string]types.AttributeValue,
) (int64, error) {
	attr, ok := attrs[id]
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrAttributeNotFound, id)
	}

	if val, ok := attr.(*types.AttributeValueMemberN); ok {
		hu, err := strconv.ParseInt(val.Value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse %s: %w", id, err)
		}
		return hu, nil
	}

	return 0, fmt.Errorf("%w: %s", ErrAttributeNotNumber, id)
}

// getSKValue extracts the SK (sort key) string value from the item attributes.
func getSKValue(attrs map[string]types.AttributeValue) (string, error) {
	skAttr, ok := attrs[ColumnSK]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrAttributeNotFound, ColumnSK)
	}

	if val, ok := skAttr.(*types.AttributeValueMemberS); ok {
		return val.Value, nil
	}

	return "", fmt.Errorf("%w: %s", ErrAttributeNotString, ColumnSK)
}

// isValidClientID checks if the clientID is valid.
// Valid client IDs: 1-64 chars, alphanumeric plus dash and underscore only.
// Rejects separators, path traversal sequences, and non-ASCII characters.
func isValidClientID(clientID string) bool {
	if clientID == "" || len(clientID) > 64 {
		return false
	}
	// Whitelist: alphanumeric, dash, underscore only
	for _, r := range clientID {
		if !isAllowedNameChar(r) {
			return false
		}
	}
	// Reject path traversal
	if strings.Contains(clientID, "..") {
		return false
	}
	return true
}

// isValidQueueName checks if the queueName is valid.
// Valid queue names: 1-64 chars, alphanumeric plus dash and underscore only.
// Rejects separators, path traversal sequences, and non-ASCII characters.
func isValidQueueName(queueName string) bool {
	if queueName == "" || len(queueName) > 64 {
		return false
	}
	// Whitelist: alphanumeric, dash, underscore only
	for _, r := range queueName {
		if !isAllowedNameChar(r) {
			return false
		}
	}
	// Reject path traversal
	if strings.Contains(queueName, "..") {
		return false
	}
	return true
}

// isAllowedNameChar returns true if the character is allowed in queue names and client IDs.
// Allows: a-z, A-Z, 0-9, dash (-), underscore (_)
func isAllowedNameChar(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9') ||
		r == '-' || r == '_'
}

// getHiddenUntilFromHandle extracts only the hiddenUntil timestamp from a receipt handle.
// Returns -1 if the handle format is invalid.
func getHiddenUntilFromHandle(handle string) int64 {
	_, _, hiddenUntil, _ := decodeRecipientHandle(handle) //nolint:dogsled // only need hiddenUntil
	return hiddenUntil
}

// decodeRecipientHandle parses the recipient handle on the format:
// SEP={RecipientHandleSeparator}
// "pk{SEP}sk{SEP}hiddenUntil{SEP}owner"
//
// If it fails, it returns empty strings and -1 for hiddenUntil.
func decodeRecipientHandle(
	recipientHandle string,
) (pk, sk string, hiddenUntil int64, owner string) {
	parts := strings.Split(recipientHandle, RecipientHandleSeparator)

	if len(parts) != 4 {
		return "", "", -1, ""
	}

	var err error
	hiddenUntil, err = strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return "", "", -1, ""
	}

	return parts[0], parts[1], hiddenUntil, parts[3]
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
		end := min(i+batchSize, len(items))
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

	for i := range msgs {
		receiptHandles[i] = msgs[i].ReceiptHandle
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

// RandomInt generates a cryptographically secure random integer in the range [minVal, maxVal).
// If random number generation fails (extremely rare), returns minVal as a safe fallback
// rather than panicking. Library code should never panic in normal operation.
func RandomInt(minVal, maxVal int) int {
	if maxVal <= minVal {
		return minVal
	}

	idx, err := rand.Int(rand.Reader, big.NewInt(int64(maxVal-minVal)))
	if err != nil {
		// In the extremely unlikely event that crypto/rand fails,
		// return deterministic fallback rather than crashing the application.
		// This is acceptable for jitter/backoff calculations.
		return minVal
	}

	return int(idx.Int64()) + minVal
}

// RandomPostfix postfixes the name with a random string.
func RandomPostfix(name string) string {
	return fmt.Sprintf("%s-%s", name, RandomString(6))
}

// truncateForLog truncates a string for safe logging, preventing sensitive data exposure.
// Appends "..." if truncated. Returns empty string for empty input.
func truncateForLog(s string, maxLen int) string {
	if s == "" {
		return ""
	}
	if maxLen <= 0 {
		maxLen = 8
	}
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
