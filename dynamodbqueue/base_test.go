package dynamodbqueue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Base Queue Tests
//
// Tests for baseQueue shared functionality used by both Standard and FIFO queues.
// ═══════════════════════════════════════════════════════════════════════════════

// TestNewBaseQueue_DefaultTTL validates default TTL is set when zero is provided.
func TestNewBaseQueue_DefaultTTL(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)

	expectedTTL := 14 * 24 * time.Hour // 14 days
	assert.Equal(t, expectedTTL, bq.ttl, "default TTL should be 14 days")
}

// TestNewBaseQueue_CustomTTL validates custom TTL is preserved.
func TestNewBaseQueue_CustomTTL(t *testing.T) {
	tests := []struct {
		name string
		ttl  time.Duration
	}{
		{"1 hour", time.Hour},
		{"24 hours", 24 * time.Hour},
		{"7 days", 7 * 24 * time.Hour},
		{"30 days", 30 * 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := newBaseQueue(nil, tt.ttl, QueueStandard)
			assert.Equal(t, tt.ttl, bq.ttl)
		})
	}
}

// TestNewBaseQueue_DefaultRandomDigits validates default random digits is MinRandomDigits (8).
func TestNewBaseQueue_DefaultRandomDigits(t *testing.T) {
	bq := newBaseQueue(nil, 0, QueueStandard)
	assert.Equal(t, MinRandomDigits, bq.randomDigits, "default random digits should be MinRandomDigits (8)")
}

// TestNewBaseQueue_QueueType validates queue type is set correctly.
func TestNewBaseQueue_QueueType(t *testing.T) {
	tests := []struct {
		name      string
		queueType QueueType
	}{
		{"standard", QueueStandard},
		{"fifo", QueueFIFO},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := newBaseQueue(nil, 0, tt.queueType)
			assert.Equal(t, tt.queueType, bq.queueType)
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Validation Tests
// ═══════════════════════════════════════════════════════════════════════════════

// TestValidateOperation validates operation validation logic.
//
// Validation Rules:
// ───────────────────────────────────────────────────────────────────────────────
//   table     → required → ErrTableNameNotSet
//   queueName → required → ErrQueueNameNotSet
//   clientID  → required → ErrClientIDNotSet
// ───────────────────────────────────────────────────────────────────────────────
func TestValidateOperation(t *testing.T) {
	tests := []struct {
		name      string
		table     string
		queueName string
		clientID  string
		wantErr   error
	}{
		{
			name:      "all fields set",
			table:     "my-table",
			queueName: "my-queue",
			clientID:  "my-client",
			wantErr:   nil,
		},
		{
			name:      "missing table",
			table:     "",
			queueName: "my-queue",
			clientID:  "my-client",
			wantErr:   ErrTableNameNotSet,
		},
		{
			name:      "missing queue name",
			table:     "my-table",
			queueName: "",
			clientID:  "my-client",
			wantErr:   ErrQueueNameNotSet,
		},
		{
			name:      "missing client ID",
			table:     "my-table",
			queueName: "my-queue",
			clientID:  "",
			wantErr:   ErrClientIDNotSet,
		},
		{
			name:      "all missing (table checked first)",
			table:     "",
			queueName: "",
			clientID:  "",
			wantErr:   ErrTableNameNotSet,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := &baseQueue{
				table:     tt.table,
				queueName: tt.queueName,
				clientID:  tt.clientID,
			}

			err := bq.validateOperation()

			if tt.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Partition Key Tests
// ═══════════════════════════════════════════════════════════════════════════════

// TestPartitionKey validates partition key format.
//
// Format:
// ───────────────────────────────────────────────────────────────────────────────
//   queueName | clientID
//   e.g., "orders|consumer-1"
// ───────────────────────────────────────────────────────────────────────────────
func TestPartitionKey(t *testing.T) {
	tests := []struct {
		name      string
		queueName string
		clientID  string
		want      string
	}{
		{
			name:      "simple values",
			queueName: "orders",
			clientID:  "consumer-1",
			want:      "orders|consumer-1",
		},
		{
			name:      "with dashes",
			queueName: "my-queue-name",
			clientID:  "my-client-id",
			want:      "my-queue-name|my-client-id",
		},
		{
			name:      "numeric values",
			queueName: "queue123",
			clientID:  "client456",
			want:      "queue123|client456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := &baseQueue{
				queueName: tt.queueName,
				clientID:  tt.clientID,
			}

			assert.Equal(t, tt.want, bq.PartitionKey())
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Getter Tests
// ═══════════════════════════════════════════════════════════════════════════════

// TestBaseQueueGetters validates all getter methods return expected values.
func TestBaseQueueGetters(t *testing.T) {
	bq := &baseQueue{
		table:        "test-table",
		queueName:    "test-queue",
		clientID:     "test-client",
		ttl:          time.Hour * 24,
		randomDigits: 8,
		logging:      true,
		queueType:    QueueFIFO,
	}

	assert.Equal(t, "test-table", bq.Table())
	assert.Equal(t, "test-queue", bq.QueueName())
	assert.Equal(t, "test-client", bq.ClientID())
	assert.Equal(t, time.Hour*24, bq.DefaultTTL())
	assert.Equal(t, 8, bq.RandomDigits())
	assert.True(t, bq.Logging())
	assert.Equal(t, QueueFIFO, bq.Type())
}

// TestSetLogging validates logging toggle.
func TestSetLogging(t *testing.T) {
	bq := &baseQueue{}

	assert.False(t, bq.Logging())

	bq.SetLogging(true)
	assert.True(t, bq.Logging())

	bq.SetLogging(false)
	assert.False(t, bq.Logging())
}

// ═══════════════════════════════════════════════════════════════════════════════
// Deferred Validation Pattern Tests
//
// Tests validating that invalid names/IDs are accepted at setter time but
// rejected at operation time (deferred validation pattern).
//
// Pattern:
// ───────────────────────────────────────────────────────────────────────────────
//   UseQueueName("invalid|name")  →  no error (accepted)
//   queue.Count(ctx)              →  ErrInvalidQueueName (rejected)
// ───────────────────────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// TestValidateOperation_InvalidQueueName_ReturnsError validates queue name format validation.
func TestValidateOperation_InvalidQueueName_ReturnsError(t *testing.T) {
	tests := []struct {
		name      string
		queueName string
		wantErr   error
	}{
		{"contains pipe", "queue|name", ErrInvalidQueueName},
		{"contains ampersand", "queue&name", ErrInvalidQueueName},
		{"contains both", "queue|name&test", ErrInvalidQueueName},
		{"path traversal", "queue..name", ErrInvalidQueueName},
		{"65 chars (too long)", string(make([]byte, 65)), ErrInvalidQueueName},
		{"contains space", "queue name", ErrInvalidQueueName},
		{"contains slash", "queue/name", ErrInvalidQueueName},
		{"contains dot", "queue.name", ErrInvalidQueueName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := &baseQueue{
				table:     "my-table",
				queueName: tt.queueName,
				clientID:  "valid-client",
			}

			err := bq.validateOperation()
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestValidateOperation_InvalidClientID_ReturnsError validates client ID format validation.
func TestValidateOperation_InvalidClientID_ReturnsError(t *testing.T) {
	tests := []struct {
		name     string
		clientID string
		wantErr  error
	}{
		{"contains pipe", "client|id", ErrInvalidClientID},
		{"contains ampersand", "client&id", ErrInvalidClientID},
		{"contains both", "client|id&test", ErrInvalidClientID},
		{"path traversal", "client..id", ErrInvalidClientID},
		{"65 chars (too long)", string(make([]byte, 65)), ErrInvalidClientID},
		{"contains space", "client id", ErrInvalidClientID},
		{"contains slash", "client/id", ErrInvalidClientID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := &baseQueue{
				table:     "my-table",
				queueName: "valid-queue",
				clientID:  tt.clientID,
			}

			err := bq.validateOperation()
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestValidateOperation_ValidationOrder validates validation happens in correct order.
//
// Order:
// ───────────────────────────────────────────────────────────────────────────────
//   1. table empty      →  ErrTableNameNotSet
//   2. queueName empty  →  ErrQueueNameNotSet
//   3. queueName invalid→  ErrInvalidQueueName
//   4. clientID empty   →  ErrClientIDNotSet
//   5. clientID invalid →  ErrInvalidClientID
// ───────────────────────────────────────────────────────────────────────────────
func TestValidateOperation_ValidationOrder(t *testing.T) {
	t.Run("table checked first", func(t *testing.T) {
		bq := &baseQueue{
			table:     "", // Invalid
			queueName: "", // Also invalid
			clientID:  "", // Also invalid
		}
		err := bq.validateOperation()
		assert.ErrorIs(t, err, ErrTableNameNotSet, "table should be checked first")
	})

	t.Run("queueName empty checked second", func(t *testing.T) {
		bq := &baseQueue{
			table:     "my-table",
			queueName: "", // Empty
			clientID:  "", // Also invalid
		}
		err := bq.validateOperation()
		assert.ErrorIs(t, err, ErrQueueNameNotSet, "queueName empty should be checked second")
	})

	t.Run("queueName invalid checked third", func(t *testing.T) {
		bq := &baseQueue{
			table:     "my-table",
			queueName: "invalid|queue", // Invalid format
			clientID:  "",              // Also invalid
		}
		err := bq.validateOperation()
		assert.ErrorIs(t, err, ErrInvalidQueueName, "queueName format should be checked third")
	})

	t.Run("clientID empty checked fourth", func(t *testing.T) {
		bq := &baseQueue{
			table:     "my-table",
			queueName: "valid-queue",
			clientID:  "", // Empty
		}
		err := bq.validateOperation()
		assert.ErrorIs(t, err, ErrClientIDNotSet, "clientID empty should be checked fourth")
	})

	t.Run("clientID invalid checked fifth", func(t *testing.T) {
		bq := &baseQueue{
			table:     "my-table",
			queueName: "valid-queue",
			clientID:  "invalid|client", // Invalid format
		}
		err := bq.validateOperation()
		assert.ErrorIs(t, err, ErrInvalidClientID, "clientID format should be checked last")
	})
}

// TestValidateOperation_ValidInputs_NoError validates all valid inputs pass.
func TestValidateOperation_ValidInputs_NoError(t *testing.T) {
	tests := []struct {
		name      string
		table     string
		queueName string
		clientID  string
	}{
		{"simple names", "table", "queue", "client"},
		{"with dashes", "my-table", "my-queue", "my-client"},
		{"with underscores", "my_table", "my_queue", "my_client"},
		{"alphanumeric", "table123", "queue456", "client789"},
		{"mixed", "My-Table_1", "My-Queue_2", "My-Client_3"},
		{"64 chars queue", "table", string(make([]byte, 64)), "client"},
		{"64 chars client", "table", "queue", string(make([]byte, 64))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Fill arrays with valid chars
			queueName := tt.queueName
			clientID := tt.clientID
			if len(queueName) == 64 {
				queueName = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			}
			if len(clientID) == 64 {
				clientID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			}

			bq := &baseQueue{
				table:     tt.table,
				queueName: queueName,
				clientID:  clientID,
			}

			err := bq.validateOperation()
			assert.NoError(t, err)
		})
	}
}
