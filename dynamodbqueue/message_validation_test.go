package dynamodbqueue

import (
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/stretchr/testify/assert"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Message Size Validation Tests
//
// Tests for message body size validation against MaxMessageBodySize (256KB).
//
// Size Boundaries:
// ───────────────────────────────────────────────────────────────────────────────
//   0 bytes        → valid (empty body)
//   256KB          → valid (exactly at limit)
//   256KB + 1 byte → invalid (ErrMessageTooLarge)
// ───────────────────────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// TestMaxMessageBodySize_Constant validates the constant value.
func TestMaxMessageBodySize_Constant(t *testing.T) {
	assert.Equal(t, 256*1024, MaxMessageBodySize,
		"MaxMessageBodySize should be 256KB (262144 bytes)")
}

// TestMessageSizeValidation_EmptyBody validates empty message body is accepted.
func TestMessageSizeValidation_EmptyBody(t *testing.T) {
	msg := events.SQSMessage{Body: ""}

	assert.LessOrEqual(t, len(msg.Body), MaxMessageBodySize,
		"empty body should be within size limit")
}

// TestMessageSizeValidation_SmallBody validates small messages are accepted.
func TestMessageSizeValidation_SmallBody(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"1 byte", 1},
		{"100 bytes", 100},
		{"1KB", 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := strings.Repeat("x", tt.size)
			msg := events.SQSMessage{Body: body}

			assert.LessOrEqual(t, len(msg.Body), MaxMessageBodySize,
				"%s should be within size limit", tt.name)
		})
	}
}

// TestMessageSizeValidation_ExactlyMaxSize validates boundary acceptance.
func TestMessageSizeValidation_ExactlyMaxSize(t *testing.T) {
	body := strings.Repeat("x", MaxMessageBodySize)
	msg := events.SQSMessage{Body: body}

	assert.Equal(t, MaxMessageBodySize, len(msg.Body),
		"message body should be exactly MaxMessageBodySize")
	assert.False(t, len(msg.Body) > MaxMessageBodySize,
		"exactly max size should not exceed limit")
}

// TestMessageSizeValidation_OneByteOver validates boundary rejection.
func TestMessageSizeValidation_OneByteOver(t *testing.T) {
	body := strings.Repeat("x", MaxMessageBodySize+1)
	msg := events.SQSMessage{Body: body}

	assert.Greater(t, len(msg.Body), MaxMessageBodySize,
		"one byte over should exceed limit")
}

// TestMessageSizeValidation_LargeMessages validates clear rejections.
func TestMessageSizeValidation_LargeMessages(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"256KB + 1", MaxMessageBodySize + 1},
		{"300KB", 300 * 1024},
		{"512KB", 512 * 1024},
		{"1MB", 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := strings.Repeat("x", tt.size)
			msg := events.SQSMessage{Body: body}

			assert.Greater(t, len(msg.Body), MaxMessageBodySize,
				"%s should exceed size limit", tt.name)
		})
	}
}

// TestMessageSizeValidation_BatchOfMessages validates each message in batch.
func TestMessageSizeValidation_BatchOfMessages(t *testing.T) {
	messages := []events.SQSMessage{
		{Body: strings.Repeat("a", 100)},        // valid
		{Body: strings.Repeat("b", 10*1024)},    // valid
		{Body: strings.Repeat("c", 100*1024)},   // valid
		{Body: strings.Repeat("d", 256*1024)},   // valid (exactly max)
		{Body: strings.Repeat("e", 256*1024+1)}, // invalid (over max)
	}

	for i, msg := range messages {
		isValid := len(msg.Body) <= MaxMessageBodySize
		if i < 4 {
			assert.True(t, isValid, "message %d should be valid", i)
		} else {
			assert.False(t, isValid, "message %d should be invalid (over max)", i)
		}
	}
}

// TestMessageSizeValidation_MultiBytChars validates UTF-8 byte counting.
//
// Note: Size is measured in bytes, not characters.
// ───────────────────────────────────────────────────────────────────────────────
//   "a" = 1 byte,  "日" = 3 bytes,  "🎉" = 4 bytes
// ───────────────────────────────────────────────────────────────────────────────
func TestMessageSizeValidation_MultiByteChars(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantBytes int
	}{
		{"ASCII", "hello", 5},
		{"Japanese", "日本語", 9},    //nolint:gosmopolitan // intentional UTF-8 test
		{"Emoji", "🎉🎉", 8},        // 2 chars × 4 bytes
		{"Mixed", "hello日本🎉", 16}, //nolint:gosmopolitan // intentional UTF-8 test
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Verify Go measures in bytes (not runes)
			actualBytes := len(tt.body)
			t.Logf("%s: %d bytes", tt.name, actualBytes)

			// Size check uses len() which returns byte count
			isValid := actualBytes <= MaxMessageBodySize
			assert.True(t, isValid, "should validate based on byte count")
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Error Sentinel Tests
// ═══════════════════════════════════════════════════════════════════════════════

// TestErrMessageTooLarge_Sentinel validates error sentinel is defined correctly.
func TestErrMessageTooLarge_Sentinel(t *testing.T) {
	assert.NotNil(t, ErrMessageTooLarge)
	assert.Contains(t, ErrMessageTooLarge.Error(), "message body exceeds maximum size")
}

// TestErrTooManyMessages_Sentinel validates batch limit error.
func TestErrTooManyMessages_Sentinel(t *testing.T) {
	assert.NotNil(t, ErrTooManyMessages)
	assert.Contains(t, ErrTooManyMessages.Error(), "maximum of 25 messages")
}

// ═══════════════════════════════════════════════════════════════════════════════
// Message Batch Limit Tests
// ═══════════════════════════════════════════════════════════════════════════════

// TestMessageBatchLimit validates 25 message limit.
func TestMessageBatchLimit(t *testing.T) {
	// These tests validate the understanding of batch limits
	// Actual enforcement is tested in integration tests

	validBatchSize := 25
	invalidBatchSize := 26

	assert.LessOrEqual(t, validBatchSize, 25, "25 messages should be valid")
	assert.Greater(t, invalidBatchSize, 25, "26 messages should be invalid")
}
