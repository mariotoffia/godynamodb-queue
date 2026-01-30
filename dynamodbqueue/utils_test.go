package dynamodbqueue

import (
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/stretchr/testify/assert"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Validation Function Tests
//
// Tests for isValidClientID and isValidQueueName validation functions.
// ═══════════════════════════════════════════════════════════════════════════════

// TestIsValidClientID validates clientID validation logic.
//
// Test Parameters:
//   - Valid: 1-64 chars, no separators (| or &)
//   - Invalid: empty, >64 chars, contains separators
func TestIsValidClientID(t *testing.T) {
	tests := []struct {
		name     string
		clientID string
		want     bool
	}{
		// Valid cases
		{"single char", "a", true},
		{"alphanumeric", "client123", true},
		{"with dashes", "my-client-id", true},
		{"with underscores", "my_client_id", true},
		{"mixed valid chars", "Client-123_test", true},
		{"exactly 64 chars", strings.Repeat("a", 64), true},
		{"numeric only", "12345", true},

		// Invalid cases
		{"empty string", "", false},
		{"65 chars (too long)", strings.Repeat("a", 65), false},
		{"contains pipe separator", "client|test", false},
		{"contains ampersand separator", "client&test", false},
		{"contains both separators", "client|test&id", false},
		{"only pipe", "|", false},
		{"only ampersand", "&", false},
		{"100 chars (way too long)", strings.Repeat("x", 100), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidClientID(tt.clientID)
			assert.Equal(t, tt.want, got, "isValidClientID(%q)", tt.clientID)
		})
	}
}

// TestIsValidQueueName validates queueName validation logic.
//
// Test Parameters:
//   - Valid: 1-64 chars, no separators (| or &)
//   - Invalid: empty, >64 chars, contains separators
func TestIsValidQueueName(t *testing.T) {
	tests := []struct {
		name      string
		queueName string
		want      bool
	}{
		// Valid cases
		{"single char", "q", true},
		{"simple name", "myQueue", true},
		{"with dashes", "my-queue-name", true},
		{"with underscores", "my_queue_name", true},
		{"mixed", "Queue-123_test", true},
		{"exactly 64 chars", strings.Repeat("q", 64), true},

		// Invalid cases
		{"empty string", "", false},
		{"65 chars (too long)", strings.Repeat("q", 65), false},
		{"contains pipe", "queue|name", false},
		{"contains ampersand", "queue&name", false},
		{"pipe at start", "|queue", false},
		{"ampersand at end", "queue&", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidQueueName(tt.queueName)
			assert.Equal(t, tt.want, got, "isValidQueueName(%q)", tt.queueName)
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Receipt Handle Tests
//
// Tests for decodeRecipientHandle parsing and validation.
// ═══════════════════════════════════════════════════════════════════════════════

// TestDecodeRecipientHandle validates receipt handle parsing.
//
// Receipt Handle Format:
// ───────────────────────────────────────────────────────────────────────────────
//   pk & sk & hidden_until & owner
//   ↓    ↓         ↓          ↓
//   partition key, sort key, timestamp (ms), clientID
// ───────────────────────────────────────────────────────────────────────────────
func TestDecodeRecipientHandle(t *testing.T) {
	tests := []struct {
		name              string
		handle            string
		wantPK            string
		wantSK            string
		wantHiddenUntil   int64
		wantOwner         string
		expectInvalidFlag bool // when true, hiddenUntil should be -1
	}{
		{
			name:            "valid handle",
			handle:          "queue|client&1234567890-abc123&1700000000000&myowner",
			wantPK:          "queue|client",
			wantSK:          "1234567890-abc123",
			wantHiddenUntil: 1700000000000,
			wantOwner:       "myowner",
		},
		{
			name:            "valid with simple values",
			handle:          "pk&sk&999&owner",
			wantPK:          "pk",
			wantSK:          "sk",
			wantHiddenUntil: 999,
			wantOwner:       "owner",
		},
		{
			name:            "zero hidden_until",
			handle:          "pk&sk&0&owner",
			wantPK:          "pk",
			wantSK:          "sk",
			wantHiddenUntil: 0,
			wantOwner:       "owner",
		},
		{
			name:              "missing parts (only 3)",
			handle:            "pk&sk&123",
			expectInvalidFlag: true,
		},
		{
			name:              "missing parts (only 2)",
			handle:            "pk&sk",
			expectInvalidFlag: true,
		},
		{
			name:              "empty string",
			handle:            "",
			expectInvalidFlag: true,
		},
		{
			name:              "too many parts",
			handle:            "pk&sk&123&owner&extra",
			expectInvalidFlag: true,
		},
		{
			name:            "non-numeric hidden_until",
			handle:          "pk&sk&notanumber&owner",
			wantPK:          "pk",
			wantSK:          "sk",
			wantHiddenUntil: 0, // ParseInt returns 0 on error
			wantOwner:       "owner",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pk, sk, hiddenUntil, owner := decodeRecipientHandle(tt.handle)

			if tt.expectInvalidFlag {
				assert.Equal(t, "", pk, "pk should be empty for invalid handle")
				assert.Equal(t, int64(-1), hiddenUntil, "hiddenUntil should be -1 for invalid handle")
			} else {
				assert.Equal(t, tt.wantPK, pk)
				assert.Equal(t, tt.wantSK, sk)
				assert.Equal(t, tt.wantHiddenUntil, hiddenUntil)
				assert.Equal(t, tt.wantOwner, owner)
			}
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Batch Utility Tests
//
// Tests for ToBatches generic batch splitting function.
// ═══════════════════════════════════════════════════════════════════════════════

// TestToBatches validates batch splitting logic.
//
// Scenarios:
// ───────────────────────────────────────────────────────────────────────────────
//   Items:  [1,2,3,4,5,6,7]  BatchSize: 3
//   Result: [[1,2,3], [4,5,6], [7]]
// ───────────────────────────────────────────────────────────────────────────────
func TestToBatches(t *testing.T) {
	tests := []struct {
		name      string
		items     []int
		batchSize int
		wantLen   int
		wantSizes []int // size of each batch
	}{
		{
			name:      "empty slice",
			items:     []int{},
			batchSize: 25,
			wantLen:   0,
			wantSizes: nil,
		},
		{
			name:      "nil slice",
			items:     nil,
			batchSize: 25,
			wantLen:   0,
			wantSizes: nil,
		},
		{
			name:      "single item",
			items:     []int{1},
			batchSize: 25,
			wantLen:   1,
			wantSizes: []int{1},
		},
		{
			name:      "exactly batch size",
			items:     makeIntSlice(25),
			batchSize: 25,
			wantLen:   1,
			wantSizes: []int{25},
		},
		{
			name:      "one over batch size",
			items:     makeIntSlice(26),
			batchSize: 25,
			wantLen:   2,
			wantSizes: []int{25, 1},
		},
		{
			name:      "two full batches",
			items:     makeIntSlice(50),
			batchSize: 25,
			wantLen:   2,
			wantSizes: []int{25, 25},
		},
		{
			name:      "two and partial",
			items:     makeIntSlice(51),
			batchSize: 25,
			wantLen:   3,
			wantSizes: []int{25, 25, 1},
		},
		{
			name:      "100 items",
			items:     makeIntSlice(100),
			batchSize: 25,
			wantLen:   4,
			wantSizes: []int{25, 25, 25, 25},
		},
		{
			name:      "7 items batch 3",
			items:     []int{1, 2, 3, 4, 5, 6, 7},
			batchSize: 3,
			wantLen:   3,
			wantSizes: []int{3, 3, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batches := ToBatches(tt.items, tt.batchSize)

			if tt.wantLen == 0 {
				assert.Nil(t, batches)
				return
			}

			assert.Len(t, batches, tt.wantLen)
			for i, wantSize := range tt.wantSizes {
				assert.Len(t, batches[i], wantSize, "batch %d size", i)
			}

			// Verify all items are preserved
			var allItems []int
			for _, batch := range batches {
				allItems = append(allItems, batch...)
			}
			assert.Equal(t, tt.items, allItems, "all items should be preserved")
		})
	}
}

// TestToBatches_WithStrings validates ToBatches works with string type.
func TestToBatches_WithStrings(t *testing.T) {
	items := []string{"a", "b", "c", "d", "e"}
	batches := ToBatches(items, 2)

	assert.Len(t, batches, 3)
	assert.Equal(t, []string{"a", "b"}, batches[0])
	assert.Equal(t, []string{"c", "d"}, batches[1])
	assert.Equal(t, []string{"e"}, batches[2])
}

// ═══════════════════════════════════════════════════════════════════════════════
// Receipt Handle Extraction Tests
// ═══════════════════════════════════════════════════════════════════════════════

// TestToReceiptHandles validates extraction of receipt handles from SQS messages.
func TestToReceiptHandles(t *testing.T) {
	tests := []struct {
		name     string
		msgs     []events.SQSMessage
		wantLen  int
		wantNil  bool
		wantVals []string
	}{
		{
			name:    "empty slice",
			msgs:    []events.SQSMessage{},
			wantLen: 0,
			wantNil: true,
		},
		{
			name:    "nil slice",
			msgs:    nil,
			wantLen: 0,
			wantNil: true,
		},
		{
			name: "single message",
			msgs: []events.SQSMessage{
				{ReceiptHandle: "handle-1"},
			},
			wantLen:  1,
			wantVals: []string{"handle-1"},
		},
		{
			name: "multiple messages",
			msgs: []events.SQSMessage{
				{ReceiptHandle: "handle-1"},
				{ReceiptHandle: "handle-2"},
				{ReceiptHandle: "handle-3"},
			},
			wantLen:  3,
			wantVals: []string{"handle-1", "handle-2", "handle-3"},
		},
		{
			name: "preserves order",
			msgs: []events.SQSMessage{
				{ReceiptHandle: "z"},
				{ReceiptHandle: "a"},
				{ReceiptHandle: "m"},
			},
			wantLen:  3,
			wantVals: []string{"z", "a", "m"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handles := ToReceiptHandles(tt.msgs)

			if tt.wantNil {
				assert.Nil(t, handles)
				return
			}

			assert.Len(t, handles, tt.wantLen)
			assert.Equal(t, tt.wantVals, handles)
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Random Generation Tests
//
// Tests verify properties of random functions (length, charset, range).
// Note: Cannot test exact values due to nondeterministic nature.
// ═══════════════════════════════════════════════════════════════════════════════

// TestRandomString validates random string generation properties.
func TestRandomString(t *testing.T) {
	tests := []struct {
		name   string
		length int
	}{
		{"length 1", 1},
		{"length 6 (default)", 6},
		{"length 10", 10},
		{"length 20", 20},
		{"length 100", 100},
	}

	validChars := "abcdefghijklmnopqrstuvwxyz0123456789"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RandomString(tt.length)

			// Verify length
			assert.Len(t, result, tt.length)

			// Verify all characters are valid
			for _, c := range result {
				assert.Contains(t, validChars, string(c),
					"character '%c' should be alphanumeric lowercase", c)
			}
		})
	}
}

// TestRandomString_Uniqueness validates that successive calls produce different values.
func TestRandomString_Uniqueness(t *testing.T) {
	seen := make(map[string]bool)
	iterations := 100

	for i := 0; i < iterations; i++ {
		s := RandomString(10)
		if seen[s] {
			t.Errorf("duplicate string generated: %s", s)
		}
		seen[s] = true
	}

	assert.Len(t, seen, iterations, "all strings should be unique")
}

// TestRandomInt validates random integer generation within bounds.
func TestRandomInt(t *testing.T) {
	tests := []struct {
		name string
		min  int
		max  int
	}{
		{"0 to 10", 0, 10},
		{"0 to 1", 0, 1},
		{"100 to 200", 100, 200},
		{"0 to 100", 0, 100},
		{"-10 to 10", -10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run multiple iterations to verify bounds
			for i := 0; i < 100; i++ {
				result := RandomInt(tt.min, tt.max)
				assert.GreaterOrEqual(t, result, tt.min, "should be >= min")
				assert.Less(t, result, tt.max, "should be < max")
			}
		})
	}
}

// TestRandomPostfix validates postfix generation.
func TestRandomPostfix(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
	}{
		{"empty prefix", ""},
		{"simple prefix", "test"},
		{"long prefix", "my-very-long-prefix-name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RandomPostfix(tt.prefix)

			// Should start with prefix
			assert.True(t, strings.HasPrefix(result, tt.prefix),
				"should start with prefix: %s", tt.prefix)

			// Should have dash separator
			if tt.prefix != "" {
				assert.Contains(t, result, tt.prefix+"-")
			}

			// Should be longer than prefix by at least 7 (dash + 6 random chars)
			assert.Greater(t, len(result), len(tt.prefix)+6)
		})
	}
}

// TestRandomPostfix_Uniqueness validates unique postfixes.
func TestRandomPostfix_Uniqueness(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 50; i++ {
		result := RandomPostfix("test")
		assert.False(t, seen[result], "should generate unique postfixes")
		seen[result] = true
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helper Functions
// ═══════════════════════════════════════════════════════════════════════════════

// makeIntSlice creates a slice of integers from 0 to n-1.
func makeIntSlice(n int) []int {
	result := make([]int, n)
	for i := 0; i < n; i++ {
		result[i] = i
	}
	return result
}
