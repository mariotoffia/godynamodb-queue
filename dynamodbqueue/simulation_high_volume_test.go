package dynamodbqueue_test

import (
	"context"
	"fmt"
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
// High Volume Exactly-Once Delivery Simulations
//
// These tests verify that large volumes of messages are delivered exactly once.
// ═══════════════════════════════════════════════════════════════════════════════

// TestSimulation_1kMessages_ExactlyOnceDelivery verifies 1000 messages delivered exactly once.
//
// Scenario:
// ───────────────────────────────────────────────────────────────────────────────
//   Phase 1: Push 1000 messages in batches of 25
//   Phase 2: Poll and delete all messages
//   Verify:  Each message received exactly once, none missing
// ───────────────────────────────────────────────────────────────────────────────
func TestSimulation_1kMessages_ExactlyOnceDelivery(t *testing.T) {
	runExactlyOnceSimulation(t, 1000)
}

// TestSimulation_2kMessages_ExactlyOnceDelivery verifies 2000 messages delivered exactly once.
func TestSimulation_2kMessages_ExactlyOnceDelivery(t *testing.T) {
	runExactlyOnceSimulation(t, 2000)
}

// TestSimulation_3kMessages_ExactlyOnceDelivery verifies 3000 messages delivered exactly once.
func TestSimulation_3kMessages_ExactlyOnceDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 3k message simulation in short mode")
	}
	runExactlyOnceSimulation(t, 3000)
}

func runExactlyOnceSimulation(t *testing.T, totalMessages int) {
	ctx := context.Background()

	queueName := fmt.Sprintf("sim-exact-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueStandard).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("sim-client")

	// Clear
	require.NoError(t, queue.Purge(ctx))

	const batchSize = 25

	// PHASE 1: Push all messages
	t.Logf("Phase 1: Pushing %d messages...", totalMessages)
	pushStart := time.Now()

	for batch := 0; batch < totalMessages; batch += batchSize {
		remaining := totalMessages - batch
		if remaining > batchSize {
			remaining = batchSize
		}

		messages := make([]events.SQSMessage, remaining)
		for i := range messages {
			messages[i] = events.SQSMessage{
				Body: fmt.Sprintf("msg-%06d", batch+i),
			}
		}

		_, err := queue.PushMessages(ctx, 0, messages...)
		require.NoError(t, err)

		// Small delay to ensure unique timestamps across batches
		time.Sleep(time.Millisecond)

		if batch > 0 && batch%1000 == 0 {
			t.Logf("  Pushed %d messages...", batch)
		}
	}

	pushDuration := time.Since(pushStart)
	t.Logf("Phase 1 complete in %v (%.0f msg/s)", pushDuration,
		float64(totalMessages)/pushDuration.Seconds())

	// Verify count
	count, err := queue.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(totalMessages), count, "all messages should be in queue")

	// PHASE 2: Consume all messages
	t.Logf("Phase 2: Consuming all messages...")
	consumeStart := time.Now()

	received := make(map[string]bool)
	duplicates := 0
	missing := 0

	for len(received) < totalMessages {
		msgs, err := queue.PollMessages(ctx, time.Second*2, time.Minute*5, 10, 25)
		require.NoError(t, err)

		if len(msgs) == 0 {
			// Check if there's actually more
			remaining, _ := queue.Count(ctx)
			if remaining == 0 {
				break
			}
			continue
		}

		for _, msg := range msgs {
			if received[msg.MessageId] {
				duplicates++
				t.Errorf("DUPLICATE: %s (body: %s)", msg.MessageId, msg.Body)
			}
			received[msg.MessageId] = true

			_, err := queue.DeleteMessages(ctx, msg.ReceiptHandle)
			require.NoError(t, err)
		}

		if len(received)%1000 == 0 {
			t.Logf("  Consumed %d messages...", len(received))
		}
	}

	consumeDuration := time.Since(consumeStart)
	t.Logf("Phase 2 complete in %v (%.0f msg/s)", consumeDuration,
		float64(len(received))/consumeDuration.Seconds())

	// ASSERTIONS
	assert.Equal(t, 0, duplicates, "should have no duplicate messages")
	assert.Equal(t, totalMessages, len(received), "should receive all messages exactly once")

	// Verify queue is empty
	finalCount, err := queue.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(0), finalCount, "queue should be empty")

	if len(received) < totalMessages {
		missing = totalMessages - len(received)
		t.Errorf("MISSING: %d messages not received", missing)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Multi-Consumer Race Simulations
//
// Verifies no message duplication when multiple consumers race to poll.
// ═══════════════════════════════════════════════════════════════════════════════

// TestSimulation_5Consumers_500Messages_NoOverlap verifies no duplicates with 5 consumers.
//
// Scenario:
// ───────────────────────────────────────────────────────────────────────────────
//   5 consumers racing to poll 500 messages
//   Each message should be delivered to exactly one consumer
// ───────────────────────────────────────────────────────────────────────────────
func TestSimulation_5Consumers_500Messages_NoOverlap(t *testing.T) {
	runMultiConsumerSimulation(t, 5, 500)
}

// TestSimulation_10Consumers_1000Messages_NoOverlap verifies no duplicates with 10 consumers.
func TestSimulation_10Consumers_1000Messages_NoOverlap(t *testing.T) {
	runMultiConsumerSimulation(t, 10, 1000)
}

// TestSimulation_20Consumers_2000Messages_NoOverlap verifies no duplicates with 20 consumers.
func TestSimulation_20Consumers_2000Messages_NoOverlap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large consumer simulation in short mode")
	}
	runMultiConsumerSimulation(t, 20, 2000)
}

func runMultiConsumerSimulation(t *testing.T, numConsumers, numMessages int) {
	ctx := context.Background()

	queueName := fmt.Sprintf("sim-race-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueStandard).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("race-client")

	// Clear and populate
	require.NoError(t, queue.Purge(ctx))

	for i := 0; i < numMessages; i++ {
		_, err := queue.PushMessages(ctx, 0, events.SQSMessage{
			Body: fmt.Sprintf("race-msg-%d", i),
		})
		require.NoError(t, err)
	}

	// Verify initial count
	count, err := queue.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(numMessages), count)

	// Concurrent consumers
	var wg sync.WaitGroup
	wg.Add(numConsumers)

	allMessages := &sync.Map{}
	duplicates := &sync.Map{}
	var totalReceived int64

	t.Logf("Starting %d consumers racing for %d messages...", numConsumers, numMessages)

	for c := 0; c < numConsumers; c++ {
		go func(consumerID int) {
			defer wg.Done()

			for {
				msgs, err := queue.PollMessages(ctx, time.Millisecond*500, time.Minute*5, 1, 10)
				if err != nil {
					return
				}

				if len(msgs) == 0 {
					return
				}

				for _, msg := range msgs {
					if _, loaded := allMessages.LoadOrStore(msg.MessageId, consumerID); loaded {
						duplicates.Store(msg.MessageId, consumerID)
					}
					atomic.AddInt64(&totalReceived, 1)

					// Delete
					_, _ = queue.DeleteMessages(ctx, msg.ReceiptHandle)
				}
			}
		}(c)
	}

	wg.Wait()

	// Check for duplicates
	duplicateCount := 0
	duplicates.Range(func(key, value interface{}) bool {
		t.Errorf("DUPLICATE: Message %v received by multiple consumers", key)
		duplicateCount++
		return true
	})

	// Count unique messages received
	uniqueCount := 0
	allMessages.Range(func(key, value interface{}) bool {
		uniqueCount++
		return true
	})

	assert.Equal(t, 0, duplicateCount, "should have no duplicate messages")
	assert.Equal(t, numMessages, uniqueCount, "all messages should be consumed exactly once")

	t.Logf("Completed: %d unique messages received, %d duplicates", uniqueCount, duplicateCount)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Multi-Queue Isolation Simulations
//
// Verifies messages don't leak between different queues on same table.
// ═══════════════════════════════════════════════════════════════════════════════

// TestSimulation_MultiQueue_Isolation verifies queue isolation on shared table.
//
// Scenario:
// ───────────────────────────────────────────────────────────────────────────────
//   5 queues on same table, each receives 500 messages (2500 total)
//   Verify: Each queue only sees its own messages
// ───────────────────────────────────────────────────────────────────────────────
func TestSimulation_MultiQueue_Isolation(t *testing.T) {
	ctx := context.Background()

	suffix := dynamodbqueue.RandomString(4)
	queueNames := []string{
		fmt.Sprintf("iso-queue-A-%s", suffix),
		fmt.Sprintf("iso-queue-B-%s", suffix),
		fmt.Sprintf("iso-queue-C-%s", suffix),
		fmt.Sprintf("iso-queue-D-%s", suffix),
		fmt.Sprintf("iso-queue-E-%s", suffix),
	}

	const messagesPerQueue = 500

	queues := make([]dynamodbqueue.Queue, len(queueNames))
	for i, name := range queueNames {
		queues[i] = dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueStandard).
			UseTable(tableName).
			UseQueueName(name).
			UseClientID("iso-client")
		require.NoError(t, queues[i].Purge(ctx))
	}

	// Push messages to each queue with identifiable content
	for qi, queue := range queues {
		for i := 0; i < messagesPerQueue; i++ {
			_, err := queue.PushMessages(ctx, 0, events.SQSMessage{
				Body: fmt.Sprintf("queue-%d-msg-%d", qi, i),
			})
			require.NoError(t, err)
		}
	}

	// Verify counts
	for qi, queue := range queues {
		count, err := queue.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, int32(messagesPerQueue), count,
			"queue %d should have %d messages", qi, messagesPerQueue)
	}

	// Poll from each queue and verify isolation
	for qi, queue := range queues {
		received := make(map[string]bool)
		wrongQueueCount := 0

		for {
			msgs, err := queue.PollMessages(ctx, time.Millisecond*500, time.Minute, 10, 25)
			require.NoError(t, err)

			if len(msgs) == 0 {
				break
			}

			for _, msg := range msgs {
				received[msg.MessageId] = true

				// Verify body contains correct queue identifier
				expectedPrefix := fmt.Sprintf("queue-%d-", qi)
				if len(msg.Body) < len(expectedPrefix) ||
					msg.Body[:len(expectedPrefix)] != expectedPrefix {
					wrongQueueCount++
					t.Errorf("Queue %d received wrong message: %s", qi, msg.Body)
				}

				_, _ = queue.DeleteMessages(ctx, msg.ReceiptHandle)
			}
		}

		assert.Equal(t, messagesPerQueue, len(received),
			"queue %d should receive exactly %d messages", qi, messagesPerQueue)
		assert.Equal(t, 0, wrongQueueCount,
			"queue %d should not receive messages from other queues", qi)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Burst Traffic Simulations
//
// Verifies system handles sudden traffic spikes correctly.
// ═══════════════════════════════════════════════════════════════════════════════

// TestSimulation_BurstTraffic_5000Messages verifies handling of 5000 message burst.
//
// Scenario:
// ───────────────────────────────────────────────────────────────────────────────
//   Burst: Push 5000 messages as fast as possible
//   Drain: Consume all messages with 5 parallel consumers
//   Verify: All messages delivered exactly once
// ───────────────────────────────────────────────────────────────────────────────
func TestSimulation_BurstTraffic_5000Messages(t *testing.T) {
	ctx := context.Background()

	queueName := fmt.Sprintf("sim-burst-%s", dynamodbqueue.RandomString(4))
	queue := dynamodbqueue.New(ddbLocal.AWSConfig(), 0, dynamodbqueue.QueueStandard).
		UseTable(tableName).
		UseQueueName(queueName).
		UseClientID("burst-client")

	require.NoError(t, queue.Purge(ctx))

	const totalMessages = 5000
	const numConsumers = 5

	// BURST PHASE: Push as fast as possible
	t.Logf("Burst phase: pushing %d messages...", totalMessages)
	burstStart := time.Now()

	for batch := 0; batch < totalMessages; batch += 25 {
		remaining := totalMessages - batch
		if remaining > 25 {
			remaining = 25
		}

		messages := make([]events.SQSMessage, remaining)
		for i := range messages {
			messages[i] = events.SQSMessage{
				Body: fmt.Sprintf("burst-%d", batch+i),
			}
		}

		_, err := queue.PushMessages(ctx, 0, messages...)
		require.NoError(t, err)
	}

	burstDuration := time.Since(burstStart)
	t.Logf("Burst complete in %v (%.0f msg/s)", burstDuration,
		float64(totalMessages)/burstDuration.Seconds())

	// DRAIN PHASE: Consume with parallel consumers
	t.Logf("Drain phase: %d consumers processing...", numConsumers)
	drainStart := time.Now()

	allMessages := &sync.Map{}
	var totalReceived int64

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	for c := 0; c < numConsumers; c++ {
		go func() {
			defer wg.Done()

			for {
				msgs, err := queue.PollMessages(ctx, time.Millisecond*500, time.Minute*5, 10, 25)
				if err != nil {
					return
				}

				if len(msgs) == 0 {
					return
				}

				for _, msg := range msgs {
					allMessages.LoadOrStore(msg.MessageId, msg.Body)
					atomic.AddInt64(&totalReceived, 1)
					_, _ = queue.DeleteMessages(ctx, msg.ReceiptHandle)
				}
			}
		}()
	}

	wg.Wait()
	drainDuration := time.Since(drainStart)

	// Count unique messages
	uniqueCount := 0
	allMessages.Range(func(key, value interface{}) bool {
		uniqueCount++
		return true
	})

	t.Logf("Drain complete in %v (%.0f msg/s)", drainDuration,
		float64(uniqueCount)/drainDuration.Seconds())

	assert.Equal(t, totalMessages, uniqueCount, "all burst messages should be consumed")

	// Verify queue empty
	finalCount, err := queue.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(0), finalCount, "queue should be empty after drain")
}
