package dynamodbqueue_test

import (
	"context"
	"fmt"
	"strconv"
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
// FIFO Race Condition Tests - Ordering Verification
//
// These tests verify FIFO ordering is maintained under various race conditions.
// ═══════════════════════════════════════════════════════════════════════════════

// TestFIFORace_RapidDeleteAndPoll tests race between delete and next poll.
//
// Strategy: Two concurrent goroutines process messages from the same group,
// testing that FIFO ordering is maintained despite rapid delete/poll cycles.
func TestFIFORace_RapidDeleteAndPoll(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("race-delpoll-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-delpoll")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numMessages = 50
	const group = "delpoll-group"

	// Push messages with adequate delay to ensure unique timestamps
	for i := 0; i < numMessages; i++ {
		_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
			Body: fmt.Sprintf("%d", i),
		})
		require.NoError(t, err)
		time.Sleep(time.Millisecond) // Use 1ms for reliable timestamp ordering
	}

	receivedOrder := make([]int, 0, numMessages)
	var mu sync.Mutex
	var totalReceived int64

	done := make(chan struct{})
	var doneOnce sync.Once

	// Two consumers racing to poll/delete from same group
	var wg sync.WaitGroup
	wg.Add(2)

	for c := 0; c < 2; c++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}

				msgs, err := fifo.PollMessages(ctx, time.Millisecond*50, time.Second*5, 1, 1)
				if err != nil {
					return
				}

				if len(msgs) == 0 {
					continue
				}

				msgNum, _ := strconv.Atoi(msgs[0].Body)

				mu.Lock()
				receivedOrder = append(receivedOrder, msgNum)
				mu.Unlock()

				// Delete immediately
				_, _ = fifo.DeleteMessages(ctx, msgs[0].ReceiptHandle)

				if atomic.AddInt64(&totalReceived, 1) >= numMessages {
					doneOnce.Do(func() { close(done) })
					return
				}
			}
		}()
	}

	// Timeout
	go func() {
		time.Sleep(30 * time.Second)
		doneOnce.Do(func() { close(done) })
	}()

	wg.Wait()

	// Check ordering
	violations := 0
	for i := 1; i < len(receivedOrder); i++ {
		if receivedOrder[i] <= receivedOrder[i-1] {
			violations++
			t.Errorf("DELETE/POLL RACE: message %d came after %d",
				receivedOrder[i], receivedOrder[i-1])
		}
	}

	t.Logf("Results: %d messages, %d violations", len(receivedOrder), violations)
	assert.Equal(t, 0, violations, "Race between delete and poll detected")
	assert.Equal(t, int64(numMessages), totalReceived, "all messages received")
}

// TestFIFORace_VerifyDeliveryOrder directly checks if messages are delivered in SK order.
//
// This test isolates the ordering issue by using sequential consumption
// and checking exact delivery order against push order.
func TestFIFORace_VerifyDeliveryOrder(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("race-order-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-order")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numMessages = 50
	const group = "order-group"

	// Push messages with MINIMAL delay - maximize timestamp proximity
	t.Log("Pushing messages with 50 microsecond delay...")
	for i := 0; i < numMessages; i++ {
		_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
			Body: fmt.Sprintf("%d", i),
		})
		require.NoError(t, err)
		time.Sleep(50 * time.Microsecond) // Very tight - 50,000 nanoseconds
	}

	// Use MANY concurrent consumers to maximize race potential
	receivedOrder := make([]int, 0, numMessages)
	var mu sync.Mutex
	var received int64

	done := make(chan struct{})
	var doneOnce sync.Once

	const numConsumers = 12 // High number of consumers
	var wg sync.WaitGroup
	wg.Add(numConsumers)

	t.Logf("Starting %d aggressive consumers on single group...", numConsumers)
	for c := 0; c < numConsumers; c++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}

				// Zero timeout, poll aggressively
				msgs, err := fifo.PollMessages(ctx, 0, time.Minute, 1, 1)
				if err != nil {
					return
				}

				for _, msg := range msgs {
					msgNum, _ := strconv.Atoi(msg.Body)

					mu.Lock()
					receivedOrder = append(receivedOrder, msgNum)
					mu.Unlock()

					_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)

					if atomic.AddInt64(&received, 1) >= numMessages {
						doneOnce.Do(func() { close(done) })
						return
					}
				}
				// No sleep - burst poll
			}
		}()
	}

	go func() {
		time.Sleep(30 * time.Second)
		doneOnce.Do(func() { close(done) })
	}()

	wg.Wait()

	// Check for violations
	violations := 0
	for i := 1; i < len(receivedOrder); i++ {
		if receivedOrder[i] <= receivedOrder[i-1] {
			violations++
			t.Errorf("ORDER VIOLATION: message %d came after %d (positions %d, %d)",
				receivedOrder[i], receivedOrder[i-1], i-1, i)
		}
	}

	t.Logf("Received %d messages, %d violations", len(receivedOrder), violations)
	t.Logf("Order: %v", receivedOrder)
	assert.Equal(t, 0, violations, "FIFO delivery order violated")
}

// TestFIFORace_TimestampCollision tests what happens when timestamps might collide.
//
// This test pushes messages with NO delay to maximize timestamp collision chance.
func TestFIFORace_TimestampCollision(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("race-ts-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.NewWithClient(ddbLocal.DynamoDBClient(), 0, dynamodbqueue.QueueFIFO).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-ts")

	fifo := queue.(dynamodbqueue.FifoQueue)
	require.NoError(t, fifo.Purge(ctx))

	const numMessages = 20
	const group = "ts-group"

	// Push messages with ZERO delay - maximizes timestamp collision chance
	t.Log("Pushing messages with ZERO delay (timestamp collision test)...")
	for i := 0; i < numMessages; i++ {
		_, err := fifo.PushMessagesWithGroup(ctx, 0, group, events.SQSMessage{
			Body: fmt.Sprintf("%d", i),
		})
		require.NoError(t, err)
		// NO SLEEP - push as fast as possible
	}

	// Single consumer to eliminate race condition - just test timestamp ordering
	receivedOrder := make([]int, 0, numMessages)

	for len(receivedOrder) < numMessages {
		msgs, err := fifo.PollMessages(ctx, time.Second, time.Minute, 1, 1)
		require.NoError(t, err)

		for _, msg := range msgs {
			msgNum, _ := strconv.Atoi(msg.Body)
			receivedOrder = append(receivedOrder, msgNum)
			_, _ = fifo.DeleteMessages(ctx, msg.ReceiptHandle)
		}
	}

	// Check for violations
	violations := 0
	for i := 1; i < len(receivedOrder); i++ {
		if receivedOrder[i] <= receivedOrder[i-1] {
			violations++
			t.Errorf("TIMESTAMP COLLISION: message %d came after %d",
				receivedOrder[i], receivedOrder[i-1])
		}
	}

	t.Logf("Single consumer received: %v", receivedOrder)
	t.Logf("Violations: %d", violations)

	// Note: violations here indicate timestamp collision bug
	// (random suffix causing wrong order when timestamps are equal)
	assert.Equal(t, 0, violations, "Timestamp collision caused wrong ordering")
}
