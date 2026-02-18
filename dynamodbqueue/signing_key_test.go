package dynamodbqueue

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ═══════════════════════════════════════════════════════════════════════════════
// User-Provided Signing Key Tests
//
// Tests for UseSigningKey which allows persistent HMAC keys that survive
// instance restarts and can be shared across queue instances.
//
// Key Lifecycle:
// ───────────────────────────────────────────────────────────────────────────────
//   Instance A: UseSigningKey(key) ──▶ sign(handle) ──▶ receipt handle
//   Instance B: UseSigningKey(key) ──▶ verify(handle) ──▶ valid
// ───────────────────────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// testSigningKey returns a deterministic 32-byte test key.
func testSigningKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	return key
}

// testFutureHandle returns a receipt handle with a future hiddenUntil timestamp.
func testFutureHandle() string {
	futureTime := time.Now().Add(time.Hour).UnixMilli()
	return "pk&sk&" + strconv.FormatInt(futureTime, 10) + "&owner"
}

// TestUseSigningKey_CrossInstanceVerification validates the core use case:
// two queue instances with the same key can sign and verify each other's handles.
func TestUseSigningKey_CrossInstanceVerification(t *testing.T) {
	key := testSigningKey()
	handle := testFutureHandle()

	bq1 := newBaseQueue(nil, 0, QueueStandard)
	bq1.useSigningKey(key)

	bq2 := newBaseQueue(nil, 0, QueueStandard)
	bq2.useSigningKey(key)

	// Sign with instance 1
	signed := bq1.signReceiptHandle(handle)

	// Verify with instance 2
	unsignedHandle, valid := bq2.verifyReceiptHandle(signed)
	assert.True(t, valid, "same key on different instances should verify")
	assert.Equal(t, handle, unsignedHandle)
}

// TestUseSigningKey_OverridesRandomKey validates that a user-provided key
// replaces the random default and produces different signatures.
func TestUseSigningKey_OverridesRandomKey(t *testing.T) {
	handle := testFutureHandle()

	// Create two instances: one with random key, one with user key
	bqRandom := newBaseQueue(nil, 0, QueueStandard)

	bqUser := newBaseQueue(nil, 0, QueueStandard)
	bqUser.useSigningKey(testSigningKey())

	signedRandom := bqRandom.signReceiptHandle(handle)
	signedUser := bqUser.signReceiptHandle(handle)

	assert.NotEqual(t, signedRandom, signedUser,
		"user-provided key should produce different signature than random key")

	// Random key instance cannot verify user-signed handle
	_, valid := bqRandom.verifyReceiptHandle(signedUser)
	assert.False(t, valid)

	// User key instance cannot verify random-signed handle
	_, valid = bqUser.verifyReceiptHandle(signedRandom)
	assert.False(t, valid)
}

// TestUseSigningKey_DefensiveCopy validates that mutating the original key slice
// after calling useSigningKey does not affect the stored key.
func TestUseSigningKey_DefensiveCopy(t *testing.T) {
	key := testSigningKey()
	handle := testFutureHandle()

	bq := newBaseQueue(nil, 0, QueueStandard)
	bq.useSigningKey(key)

	signed := bq.signReceiptHandle(handle)

	// Mutate the original key
	key[0] = 0xFF
	key[15] = 0xFF
	key[31] = 0xFF

	// Verification should still work (key was defensively copied)
	unsignedHandle, valid := bq.verifyReceiptHandle(signed)
	assert.True(t, valid, "defensive copy should protect against external mutation")
	assert.Equal(t, handle, unsignedHandle)
}

// TestUseSigningKey_TooShort_FailsValidation validates that keys shorter than
// MinSigningKeyLength cause validateOperation to return ErrSigningKeyTooShort.
func TestUseSigningKey_TooShort_FailsValidation(t *testing.T) {
	tests := []struct {
		name   string
		keyLen int
	}{
		{"empty key", 0},
		{"1 byte", 1},
		{"16 bytes", 16},
		{"31 bytes", 31},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := newBaseQueue(nil, 0, QueueStandard)
			bq.useSigningKey(make([]byte, tt.keyLen))
			bq.table = "test"
			bq.queueName = "test"
			bq.clientID = "test"

			err := bq.validateOperation()
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrSigningKeyTooShort)
		})
	}
}

// TestUseSigningKey_ExactMinLength_Succeeds validates that a key of exactly
// MinSigningKeyLength bytes passes validation.
func TestUseSigningKey_ExactMinLength_Succeeds(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)
	bq.useSigningKey(make([]byte, MinSigningKeyLength))
	bq.table = "test"
	bq.queueName = "test"
	bq.clientID = "test"

	err := bq.validateOperation()
	assert.NoError(t, err)
}

// TestUseSigningKey_LongerKey_Succeeds validates that keys longer than
// MinSigningKeyLength are accepted (HMAC supports arbitrary-length keys).
func TestUseSigningKey_LongerKey_Succeeds(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)
	bq.useSigningKey(make([]byte, 64))
	bq.table = "test"
	bq.queueName = "test"
	bq.clientID = "test"

	err := bq.validateOperation()
	assert.NoError(t, err)
}

// TestUseSigningKey_NilKey_FailsValidation validates that passing nil
// causes validation failure.
func TestUseSigningKey_NilKey_FailsValidation(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)
	bq.useSigningKey(nil)
	bq.table = "test"
	bq.queueName = "test"
	bq.clientID = "test"

	err := bq.validateOperation()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSigningKeyTooShort)
}

// TestUseSigningKey_DifferentUserKeys_CannotCrossVerify validates that
// two instances with different user-provided keys reject each other's handles.
func TestUseSigningKey_DifferentUserKeys_CannotCrossVerify(t *testing.T) {
	handle := testFutureHandle()

	key1 := make([]byte, 32)
	for i := range key1 {
		key1[i] = byte(i)
	}

	key2 := make([]byte, 32)
	for i := range key2 {
		key2[i] = byte(i + 100)
	}

	bq1 := newBaseQueue(nil, 0, QueueStandard)
	bq1.useSigningKey(key1)

	bq2 := newBaseQueue(nil, 0, QueueStandard)
	bq2.useSigningKey(key2)

	signed1 := bq1.signReceiptHandle(handle)
	signed2 := bq2.signReceiptHandle(handle)

	// Cross-verification should fail
	_, valid := bq1.verifyReceiptHandle(signed2)
	assert.False(t, valid, "different keys should not cross-verify")

	_, valid = bq2.verifyReceiptHandle(signed1)
	assert.False(t, valid, "different keys should not cross-verify")
}

// TestUseSigningKey_Idempotent validates that calling useSigningKey with
// the same key multiple times produces consistent behavior.
func TestUseSigningKey_Idempotent(t *testing.T) {
	key := testSigningKey()
	handle := testFutureHandle()

	bq := newBaseQueue(nil, 0, QueueStandard)

	// Set the same key multiple times
	bq.useSigningKey(key)
	signed1 := bq.signReceiptHandle(handle)

	bq.useSigningKey(key)
	signed2 := bq.signReceiptHandle(handle)

	assert.Equal(t, signed1, signed2,
		"setting same key multiple times should produce identical signatures")
}
