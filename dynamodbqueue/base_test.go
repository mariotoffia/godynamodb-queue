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
//   table     → required → "table name not set"
//   queueName → required → "queue name not set"
//   clientID  → required → "client ID not set"
// ───────────────────────────────────────────────────────────────────────────────
func TestValidateOperation(t *testing.T) {
	tests := []struct {
		name      string
		table     string
		queueName string
		clientID  string
		wantErr   string
	}{
		{
			name:      "all fields set",
			table:     "my-table",
			queueName: "my-queue",
			clientID:  "my-client",
			wantErr:   "",
		},
		{
			name:      "missing table",
			table:     "",
			queueName: "my-queue",
			clientID:  "my-client",
			wantErr:   TableNameNotSet,
		},
		{
			name:      "missing queue name",
			table:     "my-table",
			queueName: "",
			clientID:  "my-client",
			wantErr:   QueueNameNotSet,
		},
		{
			name:      "missing client ID",
			table:     "my-table",
			queueName: "my-queue",
			clientID:  "",
			wantErr:   ClientIDNotSet,
		},
		{
			name:      "all missing (table checked first)",
			table:     "",
			queueName: "",
			clientID:  "",
			wantErr:   TableNameNotSet,
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

			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.wantErr)
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
