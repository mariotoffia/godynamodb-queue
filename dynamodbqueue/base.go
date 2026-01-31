package dynamodbqueue

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// baseQueue contains shared configuration and functionality
// for both StandardQueue and FifoQueue implementations.
type baseQueue struct { //nolint:govet // fieldalignment: struct created once per queue, not in hot path
	// signingKey is used to HMAC-sign receipt handles for tamper detection.
	signingKey []byte
	// table is the name of the DynamoDB table to use
	table string
	// clientID the second part of the partition key (PK) for the queue.
	clientID string
	// queueName is the name of the queue. This is one component of the partition key.
	queueName string
	// client is the DynamoDB client
	client *dynamodb.Client
	// ttl is the default time to live for messages in the queue.
	ttl time.Duration
	// randomDigits contains the number of random digits to append to the SK.
	randomDigits int
	// queueType identifies whether this is a standard or FIFO queue.
	queueType QueueType
	// logging when set to true will log operations.
	logging bool
}

// MinRandomDigits is the minimum number of random digits required for SK uniqueness.
// With 8 digits using 36 characters (a-z, 0-9), we have 36^8 = 2.8 trillion combinations,
// providing extremely low collision probability even at high write rates.
const MinRandomDigits = 8

// newBaseQueue creates a new baseQueue with the given configuration.
func newBaseQueue(client *dynamodb.Client, ttl time.Duration, queueType QueueType) *baseQueue {
	if ttl == 0 {
		ttl = 14 * 24 * time.Hour
	}

	// Generate a cryptographically secure random signing key for HMAC receipt handle signing.
	// Uses crypto/rand.Read() directly for proper entropy without modulo bias.
	signingKey := make([]byte, 32)
	if _, err := rand.Read(signingKey); err != nil {
		panic(fmt.Sprintf("failed to generate signing key: %v", err))
	}

	return &baseQueue{
		client:       client,
		ttl:          ttl,
		randomDigits: MinRandomDigits,
		queueType:    queueType,
		signingKey:   signingKey,
	}
}

// signReceiptHandle signs a receipt handle using HMAC-SHA256.
func (bq *baseQueue) signReceiptHandle(handle string) string {
	mac := hmac.New(sha256.New, bq.signingKey)
	mac.Write([]byte(handle))
	signature := base64.URLEncoding.EncodeToString(mac.Sum(nil))
	return handle + RecipientHandleSeparator + signature
}

// verifyReceiptHandle verifies a signed receipt handle.
// Returns the unsigned handle if valid and not expired, or empty string if invalid.
// Receipt handles expire when their visibility timeout (hiddenUntil) has passed.
func (bq *baseQueue) verifyReceiptHandle(signedHandle string) (string, bool) {
	// Find the last separator to split handle from signature using optimized search
	lastSep := strings.LastIndexByte(signedHandle, RecipientHandleSeparator[0])
	if lastSep == -1 {
		return "", false
	}

	handle := signedHandle[:lastSep]
	providedSig := signedHandle[lastSep+1:]

	// Validate signature length before comparison to prevent timing attacks
	if providedSig == "" {
		return "", false
	}

	// Compute expected signature
	mac := hmac.New(sha256.New, bq.signingKey)
	mac.Write([]byte(handle))
	expectedSig := base64.URLEncoding.EncodeToString(mac.Sum(nil))

	// Constant-time comparison to prevent timing attacks on signature verification
	if !hmac.Equal([]byte(expectedSig), []byte(providedSig)) {
		return "", false
	}

	// Verify receipt handle hasn't expired (visibility timeout check)
	hiddenUntil := getHiddenUntilFromHandle(handle)
	if hiddenUntil == -1 {
		return "", false
	}

	// Receipt handle expires when visibility timeout ends
	if time.Now().UnixMilli() > hiddenUntil {
		return "", false
	}

	return handle, true
}

// SetLogging enables or disables logging.
func (bq *baseQueue) SetLogging(enabled bool) {
	bq.logging = enabled
}

// Logging returns true if logging is enabled.
func (bq *baseQueue) Logging() bool {
	return bq.logging
}

// Table returns the DynamoDB table name.
func (bq *baseQueue) Table() string {
	return bq.table
}

// QueueName returns the queue name.
func (bq *baseQueue) QueueName() string {
	return bq.queueName
}

// ClientID returns the client ID.
func (bq *baseQueue) ClientID() string {
	return bq.clientID
}

// PartitionKey returns the combined partition key (queueName|clientID).
func (bq *baseQueue) PartitionKey() string {
	return bq.queueName + PartitionKeySeparator + bq.clientID
}

// RandomDigits returns the number of random digits appended to SK.
func (bq *baseQueue) RandomDigits() int {
	return bq.randomDigits
}

// Type returns the queue type.
func (bq *baseQueue) Type() QueueType {
	return bq.queueType
}

// DefaultTTL returns the default TTL for the queue.
func (bq *baseQueue) DefaultTTL() time.Duration {
	return bq.ttl
}

// Client returns the DynamoDB client.
func (bq *baseQueue) Client() *dynamodb.Client {
	return bq.client
}

// validateOperation ensures table, queueName and clientID are set and valid.
func (bq *baseQueue) validateOperation() error {
	if bq.table == "" {
		return ErrTableNameNotSet
	}

	if bq.queueName == "" {
		return ErrQueueNameNotSet
	}

	if !isValidQueueName(bq.queueName) {
		return ErrInvalidQueueName
	}

	if bq.clientID == "" {
		return ErrClientIDNotSet
	}

	if !isValidClientID(bq.clientID) {
		return ErrInvalidClientID
	}

	return nil
}

// maxBatchRetries is the maximum number of retries for unprocessed items in batch operations.
const maxBatchRetries = 10

// batchProcess processes write requests in batches of 25 with context cancellation and retry limits.
func (bq *baseQueue) batchProcess(ctx context.Context, writeReqs []types.WriteRequest) error {
	batches := ToBatches(writeReqs, 25)

	for _, batch := range batches {
		// Check context before processing each batch
		if err := ctx.Err(); err != nil {
			return err
		}

		left := batch
		retries := 0

		for len(left) > 0 {
			// Check context before each retry
			if err := ctx.Err(); err != nil {
				return err
			}

			// Prevent infinite retry loops
			if retries >= maxBatchRetries {
				return fmt.Errorf("%w: %d retries, %d items remaining", ErrBatchWriteRetries, retries, len(left))
			}

			output, err := bq.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					bq.table: left,
				},
			})

			if err != nil {
				return err
			}

			if len(output.UnprocessedItems) > 0 {
				left = output.UnprocessedItems[bq.table]
				retries++

				// Exponential backoff with jitter before retry
				backoff := calcBackoff(retries)
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return ctx.Err()
				}
			} else {
				left = nil
			}
		}
	}

	return nil
}

// calcBackoff calculates exponential backoff with jitter for retry attempts.
// Returns duration between 50ms and ~3.2s (capped at exponent 6) based on attempt number.
// Negative attempts are treated as attempt 0.
func calcBackoff(attempt int) time.Duration {
	// Handle negative attempts gracefully
	if attempt < 0 {
		attempt = 0
	}

	// Exponential: 50ms * 2^attempt, capped at exponent 6 (3.2s base)
	base := 50 * time.Millisecond * (1 << min(attempt, 6))
	if base > 5*time.Second {
		base = 5 * time.Second
	}

	// Add jitter (±25%)
	jitter := base / 4
	jitterMs := int(jitter.Milliseconds())
	if jitterMs > 0 {
		base += time.Duration(RandomInt(-jitterMs, jitterMs)) * time.Millisecond
	}

	return base
}
