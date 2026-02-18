package dynamodbqueue

import (
	"encoding/base64"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test constants for receipt handle testing.
const testSimpleReceiptHandle = "pk&sk&123&owner"

// ═══════════════════════════════════════════════════════════════════════════════
// HMAC-SHA256 Receipt Handle Signing Tests
//
// Tests for signReceiptHandle which signs receipt handles using HMAC-SHA256
// with base64 URL encoding to prevent tampering.
//
// Signature Flow:
// ───────────────────────────────────────────────────────────────────────────────
//   handle ──▶ HMAC-SHA256(signingKey) ──▶ base64URL ──▶ handle & signature
// ───────────────────────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// TestSignReceiptHandle_ValidHandle validates signing produces expected format.
func TestSignReceiptHandle_ValidHandle(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)
	handle := "queue|client&1234567890-abc&1700000000000&owner"

	signed := bq.signReceiptHandle(handle)

	// Should contain original handle
	assert.True(t, strings.HasPrefix(signed, handle),
		"signed handle should start with original handle")

	// Should have separator between handle and signature
	assert.Contains(t, signed, RecipientHandleSeparator)

	// Should be longer than original (handle + separator + signature)
	assert.Greater(t, len(signed), len(handle)+1)
}

// TestSignReceiptHandle_Format validates signed handle has correct separator.
func TestSignReceiptHandle_Format(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)
	handle := testSimpleReceiptHandle

	signed := bq.signReceiptHandle(handle)

	// Count separators - should have one more than original
	originalSeps := strings.Count(handle, RecipientHandleSeparator)
	signedSeps := strings.Count(signed, RecipientHandleSeparator)
	assert.Equal(t, originalSeps+1, signedSeps,
		"signed handle should have one additional separator for signature")
}

// TestSignReceiptHandle_SignatureIsBase64URLEncoded validates signature encoding.
func TestSignReceiptHandle_SignatureIsBase64URLEncoded(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)
	handle := testSimpleReceiptHandle

	signed := bq.signReceiptHandle(handle)

	// Extract signature (after last separator)
	lastSep := strings.LastIndex(signed, RecipientHandleSeparator)
	require.Greater(t, lastSep, 0, "should find separator")

	signature := signed[lastSep+1:]

	// Signature should be valid base64 URL encoding
	_, err := base64.URLEncoding.DecodeString(signature)
	assert.NoError(t, err, "signature should be valid base64 URL encoded")

	// HMAC-SHA256 produces 32 bytes, base64 encoded = 44 chars (with padding)
	assert.Equal(t, 44, len(signature), "HMAC-SHA256 base64 signature should be 44 chars")
}

// TestSignReceiptHandle_DefaultKey_ProducesSameSignatureAcrossInstances validates
// that the deterministic default key produces identical signatures across instances.
func TestSignReceiptHandle_DefaultKey_ProducesSameSignatureAcrossInstances(t *testing.T) {
	bq1 := newBaseQueue(nil, 0, QueueStandard)
	bq2 := newBaseQueue(nil, 0, QueueStandard)
	handle := testSimpleReceiptHandle

	signed1 := bq1.signReceiptHandle(handle)
	signed2 := bq2.signReceiptHandle(handle)

	// Default deterministic key should produce same signatures across instances
	assert.Equal(t, signed1, signed2,
		"default key should produce same signatures across instances")
}

// TestSignReceiptHandle_DifferentKeys_ProduceDifferentSignatures validates key isolation
// when explicit keys are set via UseSigningKey.
func TestSignReceiptHandle_DifferentKeys_ProduceDifferentSignatures(t *testing.T) {
	bq1 := newBaseQueue(nil, 0, QueueStandard)
	bq1.useSigningKey([]byte("key-one-key-one-key-one-key-one!"))

	bq2 := newBaseQueue(nil, 0, QueueStandard)
	bq2.useSigningKey([]byte("key-two-key-two-key-two-key-two!"))

	handle := testSimpleReceiptHandle

	signed1 := bq1.signReceiptHandle(handle)
	signed2 := bq2.signReceiptHandle(handle)

	assert.NotEqual(t, signed1, signed2,
		"different signing keys should produce different signatures")
}

// TestSignReceiptHandle_SameKeyAndHandle_ProducesIdenticalSignature validates idempotency.
func TestSignReceiptHandle_SameKeyAndHandle_ProducesIdenticalSignature(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)
	handle := testSimpleReceiptHandle

	signed1 := bq.signReceiptHandle(handle)
	signed2 := bq.signReceiptHandle(handle)

	assert.Equal(t, signed1, signed2,
		"same key and handle should produce identical signatures")
}

// TestSignReceiptHandle_EdgeCases validates handling of edge case inputs.
func TestSignReceiptHandle_EdgeCases(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)

	tests := []struct {
		name   string
		handle string
	}{
		{"empty handle", ""},
		{"single char", "x"},
		{"long handle (1000 chars)", strings.Repeat("a", 1000)},
		{"special chars", "handle-with_special.chars!@#$%"},
		{"unicode", "handle-日本語-emoji-🎉"}, //nolint:gosmopolitan // intentional UTF-8 test
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signed := bq.signReceiptHandle(tt.handle)

			// Should always produce valid signed output
			assert.True(t, strings.HasPrefix(signed, tt.handle))
			assert.Greater(t, len(signed), len(tt.handle))
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Receipt Handle Verification Tests
//
// Tests for verifyReceiptHandle which validates HMAC signatures and expiration.
//
// Verification Flow:
// ───────────────────────────────────────────────────────────────────────────────
//   signed ──▶ split(handle, sig) ──▶ HMAC verify ──▶ expiration check ──▶ result
// ───────────────────────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// TestVerifyReceiptHandle_ValidSignature validates successful verification.
func TestVerifyReceiptHandle_ValidSignature(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)

	// Create a handle with future expiration
	futureTime := time.Now().Add(time.Hour).UnixMilli()
	handle := "pk&sk&" + strconv.FormatInt(futureTime, 10) + "&owner"

	signed := bq.signReceiptHandle(handle)
	unsignedHandle, valid := bq.verifyReceiptHandle(signed)

	assert.True(t, valid, "valid signature should verify")
	assert.Equal(t, handle, unsignedHandle, "should return original handle")
}

// TestVerifyReceiptHandle_InvalidSignature_ReturnsFalse validates tampering detection.
func TestVerifyReceiptHandle_InvalidSignature_ReturnsFalse(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)

	futureTime := time.Now().Add(time.Hour).UnixMilli()
	handle := "pk&sk&" + strconv.FormatInt(futureTime, 10) + "&owner"
	signed := bq.signReceiptHandle(handle)

	tests := []struct {
		name     string
		tampered string
	}{
		{"single bit flip in signature", signed[:len(signed)-1] + "X"},
		{"truncated signature", signed[:len(signed)-5]},
		{"appended chars", signed + "extra"},
		{"modified handle", "tampered" + signed[8:]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unsignedHandle, valid := bq.verifyReceiptHandle(tt.tampered)
			assert.False(t, valid, "tampered signature should not verify")
			assert.Empty(t, unsignedHandle, "should return empty handle")
		})
	}
}

// TestVerifyReceiptHandle_MissingSignature_ReturnsFalse validates missing signature rejection.
func TestVerifyReceiptHandle_MissingSignature_ReturnsFalse(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)

	tests := []struct {
		name   string
		handle string
	}{
		{"no separator at all", "pkskowner"},
		{"handle without signature part", testSimpleReceiptHandle},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// For handles without proper signature, verification should fail
			// because the "signature" part won't match HMAC
			unsignedHandle, valid := bq.verifyReceiptHandle(tt.handle)
			assert.False(t, valid)
			assert.Empty(t, unsignedHandle)
		})
	}
}

// TestVerifyReceiptHandle_EmptySignature_ReturnsFalse validates empty signature rejection.
func TestVerifyReceiptHandle_EmptySignature_ReturnsFalse(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)

	// Handle ending with separator (empty signature)
	handle := testSimpleReceiptHandle + "&"

	unsignedHandle, valid := bq.verifyReceiptHandle(handle)
	assert.False(t, valid, "empty signature should not verify")
	assert.Empty(t, unsignedHandle)
}

// TestVerifyReceiptHandle_WrongKey_ReturnsFalse validates cross-key rejection.
func TestVerifyReceiptHandle_WrongKey_ReturnsFalse(t *testing.T) {
	bq1 := newBaseQueue(nil, 0, QueueStandard)
	bq1.useSigningKey([]byte("key-one-key-one-key-one-key-one!"))

	bq2 := newBaseQueue(nil, 0, QueueStandard)
	bq2.useSigningKey([]byte("key-two-key-two-key-two-key-two!"))

	futureTime := time.Now().Add(time.Hour).UnixMilli()
	handle := "pk&sk&" + strconv.FormatInt(futureTime, 10) + "&owner"

	// Sign with bq1's key
	signed := bq1.signReceiptHandle(handle)

	// Try to verify with bq2's key
	unsignedHandle, valid := bq2.verifyReceiptHandle(signed)
	assert.False(t, valid, "different key should not verify")
	assert.Empty(t, unsignedHandle)
}

// TestVerifyReceiptHandle_ExpiredHandle_ReturnsFalse validates expiration check.
//
// Timeline:
// ───────────────────────────────────────────────────────────────────────────────
//   hiddenUntil (past) ────────── now ────────── future
//        ↓                         ↓
//   handle expired            verification fails
// ───────────────────────────────────────────────────────────────────────────────
func TestVerifyReceiptHandle_ExpiredHandle_ReturnsFalse(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)

	// Create a handle with past expiration
	pastTime := time.Now().Add(-time.Hour).UnixMilli()
	handle := "pk&sk&" + strconv.FormatInt(pastTime, 10) + "&owner"

	signed := bq.signReceiptHandle(handle)
	unsignedHandle, valid := bq.verifyReceiptHandle(signed)

	assert.False(t, valid, "expired handle should not verify")
	assert.Empty(t, unsignedHandle)
}

// TestVerifyReceiptHandle_NonExpiredHandle_ReturnsValid validates future expiration acceptance.
func TestVerifyReceiptHandle_NonExpiredHandle_ReturnsValid(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)

	tests := []struct {
		name   string
		offset time.Duration
	}{
		{"1 second in future", time.Second},
		{"1 minute in future", time.Minute},
		{"1 hour in future", time.Hour},
		{"24 hours in future", 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			futureTime := time.Now().Add(tt.offset).UnixMilli()
			handle := "pk&sk&" + strconv.FormatInt(futureTime, 10) + "&owner"

			signed := bq.signReceiptHandle(handle)
			unsignedHandle, valid := bq.verifyReceiptHandle(signed)

			assert.True(t, valid, "non-expired handle should verify")
			assert.Equal(t, handle, unsignedHandle)
		})
	}
}

// TestVerifyReceiptHandle_InvalidHandleFormat_ReturnsFalse validates format rejection.
func TestVerifyReceiptHandle_InvalidHandleFormat_ReturnsFalse(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)

	tests := []struct {
		name   string
		handle string
	}{
		{"only 2 parts", "pk&sk"},
		{"only 3 parts", "pk&sk&123"},
		{"non-numeric hiddenUntil", "pk&sk&notanumber&owner"},
		{"empty parts", "&&&&"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Sign the malformed handle
			signed := bq.signReceiptHandle(tt.handle)

			// Even with valid signature, malformed handle should fail
			unsignedHandle, valid := bq.verifyReceiptHandle(signed)
			assert.False(t, valid, "malformed handle format should not verify")
			assert.Empty(t, unsignedHandle)
		})
	}
}

// TestVerifyReceiptHandle_MultipleVerifications_Idempotent validates consistent verification.
func TestVerifyReceiptHandle_MultipleVerifications_Idempotent(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)

	futureTime := time.Now().Add(time.Hour).UnixMilli()
	handle := "pk&sk&" + strconv.FormatInt(futureTime, 10) + "&owner"
	signed := bq.signReceiptHandle(handle)

	// Verify multiple times
	for i := 0; i < 10; i++ {
		unsignedHandle, valid := bq.verifyReceiptHandle(signed)
		assert.True(t, valid, "verification %d should succeed", i)
		assert.Equal(t, handle, unsignedHandle)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Default Signing Key Tests
//
// Tests for newBaseQueue deterministic default signing key.
// ═══════════════════════════════════════════════════════════════════════════════

// TestNewBaseQueue_SigningKey_IsDefaultKey validates the default key is used.
func TestNewBaseQueue_SigningKey_IsDefaultKey(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)

	assert.Equal(t, defaultSigningKey, bq.signingKey,
		"new queue should use the deterministic default signing key")
	assert.Len(t, bq.signingKey, 32, "signing key should be 32 bytes (256 bits)")
}

// TestNewBaseQueue_SigningKey_DeterministicAcrossInstances validates all instances
// share the same default key for cross-instance receipt handle verification.
func TestNewBaseQueue_SigningKey_DeterministicAcrossInstances(t *testing.T) {
	for range 100 {
		bq := newBaseQueue(nil, 0, QueueStandard)
		assert.Equal(t, defaultSigningKey, bq.signingKey,
			"all instances should share the same default signing key")
	}
}

// TestNewBaseQueue_SigningKey_NotZero validates key is not all zeros.
func TestNewBaseQueue_SigningKey_NotZero(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)

	allZero := true
	for _, b := range bq.signingKey {
		if b != 0 {
			allZero = false
			break
		}
	}

	assert.False(t, allZero, "signing key should not be all zeros")
}
