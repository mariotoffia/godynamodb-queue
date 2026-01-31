package dynamodbqueue_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/mariotoffia/godynamodb-queue/dynamodbqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ═══════════════════════════════════════════════════════════════════════════════
// FIFO Concurrent Consumer Simulations
// ═══════════════════════════════════════════════════════════════════════════════

// TestSimulation_FIFO_ConcurrentConsumers_NoOrderViolation verifies concurrent consumers.
//
// Scenario:
// ───────────────────────────────────────────────────────────────────────────────
//
//	5 groups, 100 messages each (500 total)
//	3 concurrent consumers
//	Verify: Each group still maintains strict FIFO order
//	Note: FIFO guarantees only ONE message in-flight per group at a time,
//	      so messages within a group must be processed sequentially.
//
// ───────────────────────────────────────────────────────────────────────────────
func TestSimulation_FIFO_ConcurrentConsumers_NoOrderViolation(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("sim-fifo-conc-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("fifo-conc")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numGroups = 5
	const messagesPerGroup = 100
	const numConsumers = 3
	const totalMessages = numGroups * messagesPerGroup

	// Create groups
	groups := make([]string, numGroups)
	for i := range groups {
		groups[i] = fmt.Sprintf("cg%d", i)
	}

	// Push messages with sufficient delay to ensure timestamp ordering
	// The SK uses nanosecond timestamp, so 10ms delay ensures distinct timestamps
	t.Logf("Pushing %d messages to %d groups...", totalMessages, numGroups)
	for _, group := range groups {
		for i := 0; i < messagesPerGroup; i++ {
			_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
				Body: fmt.Sprintf("%s|%03d", group, i),
			})
			require.NoError(t, err)
			// Use 10ms delay to ensure distinct timestamps under any system load
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Track ALL received messages per group to verify order post-hoc
	groupMessages := make(map[string][]int)
	var groupMu sync.Mutex
	var totalReceived int64

	// Use context with timeout instead of separate goroutine to avoid leaks
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	t.Logf("Starting %d consumers...", numConsumers)

	for c := 0; c < numConsumers; c++ {
		go func(consumerID int) {
			defer wg.Done()

			emptyPolls := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if atomic.LoadInt64(&totalReceived) >= totalMessages {
					return
				}

				msgs, err := fifo.PollMessages(ctx, time.Millisecond*200, time.Minute*5, 1, numGroups)
				if err != nil {
					// Context cancellation is expected
					if ctx.Err() != nil {
						return
					}
					return
				}

				if len(msgs) == 0 {
					emptyPolls++
					// With FIFO, all groups might be temporarily blocked
					if emptyPolls > 20 && atomic.LoadInt64(&totalReceived) >= totalMessages {
						return
					}
					time.Sleep(time.Millisecond * 100)
					continue
				}

				emptyPolls = 0

				for _, msg := range msgs {
					parts := strings.Split(msg.Body, "|")
					group := parts[0]
					msgNum, _ := strconv.Atoi(parts[1])

					// Record message for post-hoc analysis
					groupMu.Lock()
					groupMessages[group] = append(groupMessages[group], msgNum)
					groupMu.Unlock()

					// Delete BEFORE incrementing counter to ensure proper ordering
					_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)

					atomic.AddInt64(&totalReceived, 1)
				}
			}
		}(c)
	}

	wg.Wait()

	// Post-hoc verification: check ordering within each group
	violations := 0
	for group, msgs := range groupMessages {
		for i := 1; i < len(msgs); i++ {
			if msgs[i] <= msgs[i-1] {
				violations++
				t.Errorf("ORDERING VIOLATION in group %s: message %d came after %d (position %d)",
					group, msgs[i], msgs[i-1], i)
			}
		}
	}

	t.Logf("Completed: %d messages received, %d violations", totalReceived, violations)

	assert.Equal(t, 0, violations, "should have no ordering violations")
	assert.Equal(t, int64(totalMessages), totalReceived, "all messages should be consumed")

	// Verify all messages were received for each group
	for _, group := range groups {
		receivedCount := len(groupMessages[group])
		assert.Equal(t, messagesPerGroup, receivedCount,
			"group %s should have received all %d messages", group, messagesPerGroup)
	}
}
